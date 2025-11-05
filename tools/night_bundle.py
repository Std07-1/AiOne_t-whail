"""
Оркестратор нічного комплекту: запускає live‑вікно, паралельно скрейпить /metrics,
а по завершенні формує пакет артефактів (quality, forward, summary, episodes index).

Приклад запуску:
  python -m tools.night_bundle --duration 28800 --prom-port 9125 \
    --namespace ai_one_night --out-dir reports/night_pack
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import threading
import urllib.request
from pathlib import Path


def _append_metrics(
    url: str, out_path: Path, stop_flag: threading.Event, interval_s: int = 15
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    while not stop_flag.is_set():
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = resp.read().decode("utf-8", errors="ignore")
                with out_path.open("a", encoding="utf-8") as fh:
                    fh.write(data)
                    if not data.endswith("\n"):
                        fh.write("\n")
        except Exception:
            # best-effort: просто пропускаємо і чекаємо
            pass
        # Неблокуючі sleep‑ітерації
        stop_flag.wait(interval_s)


def _run_py_module(module: str, args: list[str], cwd: Path | None = None) -> int:
    cmd = [sys.executable, "-m", module] + args
    try:
        res = subprocess.run(cmd, cwd=str(cwd) if cwd else None)
        return int(res.returncode or 0)
    except Exception:
        return 1


def main() -> int:
    ap = argparse.ArgumentParser(description="Night bundle orchestrator")
    ap.add_argument("--duration", type=int, default=28800)
    ap.add_argument("--prom-port", type=int, default=9125)
    ap.add_argument("--namespace", default="ai_one_night")
    ap.add_argument("--out-dir", default="reports/night_pack")
    ap.add_argument("--episodes-root", default="replay_bench/last48h")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "run_night.log"
    metrics_path = out_dir / "metrics_night.txt"

    # 1) Запуск live‑вікна
    run_args = [
        "-m",
        "tools.run_window",
        "--duration",
        str(int(args.duration)),
        "--set",
        f"STATE_NAMESPACE={args.namespace}",
        "--set",
        "PROFILE_ENGINE_ENABLED=true",
        "--set",
        "HTF_CONTEXT_ENABLED=true",
        "--set",
        "PROM_GAUGES_ENABLED=true",
        "--set",
        f"PROM_HTTP_PORT={int(args.prom_port)}",
        "--log",
        str(log_path),
    ]
    proc = subprocess.Popen([sys.executable] + run_args)

    # 2) Паралельний скрейп /metrics
    stop_ev = threading.Event()
    thr = threading.Thread(
        target=_append_metrics,
        args=(
            f"http://localhost:{int(args.prom_port)}/metrics",
            metrics_path,
            stop_ev,
            15,
        ),
        daemon=True,
    )
    thr.start()

    # 3) Очікуємо завершення live‑процесу
    try:
        proc.wait()
    finally:
        stop_ev.set()
        thr.join(timeout=5)

    # 4) Пост‑кроки: якість, snapshot, forward, summary
    # scenario_quality_report
    _run_py_module(
        "tools.scenario_quality_report",
        [
            "--logs",
            str(
                log_path.name
            ),  # модуль сам шукає у поточному каталозі logs, якщо не абсолютний
            "--out",
            str(out_dir / "quality_night.csv"),
        ],
    )
    # quality_snapshot (включно з forward‑K)
    _run_py_module(
        "tools.quality_snapshot",
        [
            "--csv",
            str(out_dir / "quality_night.csv"),
            "--metrics-dir",
            str(out_dir),
            "--out",
            str(out_dir / "quality_snapshot_night.md"),
            "--forward-k",
            "3,5,10,20,30",
            "--forward-enable-long",
            "true",
        ],
    )
    # forward_from_log (строгі пороги за замовч.)
    _run_py_module(
        "tools.forward_from_log",
        [
            "--log",
            str(log_path),
            "--out",
            str(out_dir / "forward_night.md"),
            "--k",
            "3",
            "--k",
            "5",
            "--k",
            "10",
            "--k",
            "20",
            "--k",
            "30",
            "--presence-min",
            "0.75",
            "--bias-abs-min",
            "0.6",
        ],
    )
    # night_post summary (авто‑GO/NO‑GO)
    _run_py_module(
        "tools.night_post",
        [
            "--metrics",
            str(metrics_path),
            "--out",
            str(out_dir / "night_summary.md"),
            "--quality",
            str(out_dir / "quality_night.csv"),
        ],
    )
    # episodes_index (опційно)
    ep_root = Path(args.episodes_root)
    if ep_root.exists():
        _run_py_module(
            "tools.episodes_index",
            [
                "--root",
                str(ep_root),
                "--out",
                str(out_dir / "episodes_index.csv"),
            ],
        )

    # 5) README.md: посилання на артефакти
    readme = [
        "# Night Pack\n",
        "\n",
        f"- Log: [{log_path.name}]({log_path.name})\n",
        f"- Metrics: [{metrics_path.name}]({metrics_path.name})\n",
        "- Quality CSV: [quality_night.csv](quality_night.csv)\n",
        "- Quality Snapshot: [quality_snapshot_night.md](quality_snapshot_night.md)\n",
        "- Forward: [forward_night.md](forward_night.md)\n",
        "- Summary: [night_summary.md](night_summary.md)\n",
    ]
    if (out_dir / "episodes_index.csv").exists():
        readme.append("- Episodes Index: [episodes_index.csv](episodes_index.csv)\n")
    (out_dir / "README.md").write_text("".join(readme), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
