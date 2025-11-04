#!/usr/bin/env python3
"""
A/B Orchestrator: надійний збір /metrics для двох інстансів і авто‑звіт.

• Перевіряє доступність обох портів перед стартом.
• Робить перший зріз негайно (t0), далі — кожні interval_sec до duration_min.
• Зберігає сирі метрики у out_dir/A і out_dir/B як metrics_YYYYMMDD_HHMMSS.txt.
• Після завершення викликає генератор звіту (tools.ab_generate_report) і друкує шлях.

Приклад запуску:
  python -m tools.ab_orchestrator \
    --port-a 9108 --port-b 9118 \
    --symbol BTCUSDT --duration-min 91 --interval-sec 900 \
    --out-dir ab_runs \
    --label-a "A (ctx-weights off)" --label-b "B (ctx-weights on)"
"""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def _fetch_metrics(port: int, timeout: float = 5.0) -> str:
    url = f"http://127.0.0.1:{int(port)}/metrics"
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # nosec B310
        data = resp.read()
    try:
        return data.decode("utf-8", errors="replace")
    except Exception:
        return data.decode("latin-1", errors="replace")


def _ts_name() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _write(out_dir: Path, label: str, text: str) -> Path:
    (out_dir / label).mkdir(parents=True, exist_ok=True)
    p = out_dir / label / f"metrics_{_ts_name()}.txt"
    p.write_text(text, encoding="utf-8")
    return p


def _preflight(port_a: int, port_b: int) -> None:
    ok = True
    for label, port in (("A", port_a), ("B", port_b)):
        try:
            text = _fetch_metrics(port, timeout=3.0)
            if "htf_strength" not in text and "presence" not in text:
                print(
                    f"[WARN] Префлайт {label}: відповідь без очікуваних метрик (довжина={len(text)})"
                )
            else:
                print(f"[OK] Префлайт {label}: metrics доступні (довжина={len(text)})")
        except Exception as e:  # pragma: no cover - мережеві помилки
            print(f"[ERR] Префлайт {label}: {e}")
            ok = False
    if not ok:
        raise SystemExit(2)


def _compute_iterations(duration_min: float, interval_sec: float) -> int:
    total_sec = max(0.0, float(duration_min) * 60.0)
    if interval_sec <= 0:
        return 1
    # знімки: t0, t+interval, ..., <= duration
    return int(total_sec // float(interval_sec)) + 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="A/B Orchestrator for /metrics collection")
    # Сумісні параметри (старі назви)
    ap.add_argument("--port-a", type=int, help="Port for instance A")
    ap.add_argument("--port-b", type=int, help="Port for instance B")
    ap.add_argument("--symbol", type=str, help="Single symbol for report (fallback)")
    ap.add_argument(
        "--duration-min", type=float, default=91.0, help="Duration in minutes"
    )
    ap.add_argument(
        "--interval-sec", type=float, default=900.0, help="Snapshot interval in seconds"
    )
    ap.add_argument("--out-dir", type=str, default="ab_runs")
    # Нові короткі параметри (зручні для скриптів)
    ap.add_argument("--ports", type=str, help="Comma-separated ports e.g. 9108,9118")
    ap.add_argument(
        "--snap-every",
        type=float,
        help="Snapshot interval in seconds (alias for --interval-sec)",
    )
    ap.add_argument("--duration", type=float, help="Duration in seconds (alias)")
    ap.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated symbols for which to render reports (default: use --symbol)",
    )
    ap.add_argument("--label-a", type=str, default="A")
    ap.add_argument("--label-b", type=str, default="B")
    args = ap.parse_args(argv)

    # Узгодження аліасів аргументів
    port_a = args.port_a
    port_b = args.port_b
    if args.ports and (not port_a or not port_b):
        try:
            p1, p2 = (int(x.strip()) for x in str(args.ports).split(",", 1))
            port_a, port_b = p1, p2
        except Exception as exc:
            raise SystemExit("--ports має бути у форматі '9108,9118'") from exc
    if not port_a or not port_b:
        raise SystemExit("Потрібно вказати --port-a/--port-b або --ports")

    if args.snap_every is not None:
        args.interval_sec = float(args.snap_every)
    if args.duration is not None:
        # seconds → minutes для внутрішньої змінної
        args.duration_min = float(args.duration) / 60.0

    # Підтримка множинних символів (за замовчуванням використовуємо один --symbol)
    symbols: list[str]
    if args.symbols:
        symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    elif args.symbol:
        symbols = [args.symbol.strip().upper()]
    else:
        raise SystemExit("Потрібно вказати --symbol або --symbols")

    out_dir = Path(args.out_dir)
    (out_dir / "A").mkdir(parents=True, exist_ok=True)
    (out_dir / "B").mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Префлайт перевірка портів A={port_a} B={port_b}...")
    _preflight(port_a, port_b)

    count = _compute_iterations(args.duration_min, args.interval_sec)
    print(
        f"[INFO] План: зрізів={count}, інтервал={args.interval_sec:.0f}s, тривалість≈{args.duration_min:.1f} хв"
    )

    # орієнтація на стійкий інтервал за годинником
    for i in range(count):
        t0 = time.monotonic()
        ts = _ts_name()
        print(f"[SNAP {i+1}/{count}] {ts} — знімаю /metrics A та B...")
        try:
            text_a = _fetch_metrics(port_a)
            path_a = _write(out_dir, "A", text_a)
            print(f"  A → {path_a.name} ({len(text_a)} байт)")
        except Exception as e:
            print(f"  [ERR] A fetch: {e}")
        try:
            text_b = _fetch_metrics(port_b)
            path_b = _write(out_dir, "B", text_b)
            print(f"  B → {path_b.name} ({len(text_b)} байт)")
        except Exception as e:
            print(f"  [ERR] B fetch: {e}")
        # сон до наступного періоду, але не після останнього
        if i + 1 < count:
            elapsed = time.monotonic() - t0
            sleep_s = max(0.0, float(args.interval_sec) - elapsed)
            mins = int(sleep_s // 60)
            secs = int(sleep_s % 60)
            print(f"[WAIT] До наступного зрізу: {mins:02d}:{secs:02d}")
            time.sleep(sleep_s)

    # Авто-звіт (детальний) + короткий A/B зріз
    print("[INFO] Генерую звіти...")
    try:
        for sym in symbols:
            report_path = out_dir / f"report_{sym}.md"
            short_report_path = out_dir / f"report_{sym}_short.md"
            cmd = [
                sys.executable,
                "-m",
                "tools.ab_generate_report",
                "--dir-a",
                str(out_dir / "A"),
                "--dir-b",
                str(out_dir / "B"),
                "--symbol",
                sym,
                "--out",
                str(report_path),
                "--label-a",
                args.label_a,
                "--label-b",
                args.label_b,
            ]
            subprocess.check_call(cmd)
            print(f"[DONE] Звіт збережено: {report_path}")
            # Короткий A/B зріз
            try:
                cmd_short = [
                    sys.executable,
                    "-m",
                    "tools.ab_compare",
                    "--a-dir",
                    str(out_dir / "A"),
                    "--b-dir",
                    str(out_dir / "B"),
                    "--symbol",
                    sym,
                    "--out",
                    str(short_report_path),
                ]
                subprocess.check_call(cmd_short)
                print(f"[DONE] Короткий зріз збережено: {short_report_path}")
            except subprocess.CalledProcessError as e:
                print(
                    f"[WARN] ab_compare повернув {e.returncode} для {sym}, короткий звіт пропущено"
                )
        return 0
    except subprocess.CalledProcessError as e:
        print(f"[ERR] ab_generate_report повернув {e.returncode}")
        return e.returncode


if __name__ == "__main__":
    raise SystemExit(main())
