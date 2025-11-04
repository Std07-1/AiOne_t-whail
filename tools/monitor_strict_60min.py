"""Строгий моніторинг у реальному часі (≈60 хв).

Призначення:
- Періодично знімати метрики Prometheus локального застосунку (порт за замовчуванням 9108),
- Знімати знімки стану Redis для заданих символів,
- Після завершення сесії узагальнювати SCENARIO_TRACE і писати артефакти у папку запуску.

Особливості:
- Максимально простий і надійний інструмент для оперативної перевірки «строгого режиму» (STRICT).
- Докстрінги, коментарі та логи українською, як вимагає репозиторій.
- Логування робимо через print() із префіксами [STRICT_SANITY]/[MONITOR], щоб легко grep-ити.

Застереження:
- Інструмент best-effort, не змінює контракти Stage1/2/3, не впливає на основний пайплайн.
- У разі відсутності Redis або метрик — продовжує роботу, але залишає пояснювальні повідомлення.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import time
from pathlib import Path
from typing import Any

from tools.canary_collect import fetch_metrics as _fetch_metrics
from tools.canary_collect import summarize_trace as _summarize_trace

try:
    from redis.asyncio import Redis
except Exception:  # pragma: no cover
    Redis = None  # type: ignore

REPO_ROOT = Path(__file__).resolve().parents[1]


async def _redis_hgetall(r: Any, key: str) -> dict[str, Any]:
    """Безпечне читання всього hash за ключем Redis.

    Повертає словник (може бути порожнім). У разі помилок — не кидає виключення,
    а повертає {} і друкує коротке повідомлення для діагностики.
    """
    try:
        res = await r.hgetall(key)
        return dict(res) if isinstance(res, dict) else {}
    except Exception as e:
        print(f"[STRICT_SANITY] [REDIS] hgetall failed key={key}: {e}")
        return {}


async def snapshot_state(symbols: list[str], out_dir: Path) -> None:
    """Зняти знімок Redis state для кожного символу в окремий файл.

    Формат файлу: state_{SYMBOL}.txt із парами ключ=значення (відсортовано).
    У разі відсутності клієнта Redis залишає службову замітку state_info.txt.
    """
    # Параметри з налаштувань застосунку (якщо доступні)
    try:
        from app.settings import settings  # type: ignore

        host, port = settings.redis_host, settings.redis_port
    except Exception:
        host, port = "localhost", 6379

    if Redis is None:
        msg = "redis-py not available; skipping Redis snapshots"
        print(f"[STRICT_SANITY] [REDIS] {msg}")
        (out_dir / "state_info.txt").write_text(msg, encoding="utf-8")
        return

    print(f"[STRICT_SANITY] [REDIS] Connecting host={host} port={port}")
    r = Redis(host=host, port=port, decode_responses=True, encoding="utf-8")
    try:
        for sym in symbols:
            upper = sym.upper()
            lower = sym.lower()
            keys = [f"ai_one:state:{lower}", f"ai_one:state:{upper}"]
            data: dict[str, Any] = {}
            for k in keys:
                got = await _redis_hgetall(r, k)
                print(
                    f"[STRICT_SANITY] [REDIS] hgetall key={k} size={len(got) if got else 0}"
                )
                if got:
                    data = got
                    break
            out = out_dir / f"state_{upper}.txt"
            with out.open("w", encoding="utf-8") as f:
                for k, v in sorted(data.items()):
                    f.write(f"{k}={v}\n")
            print(
                f"[STRICT_SANITY] [REDIS] snapshot written file={out} items={len(data)}"
            )
    finally:
        try:
            await r.close()
            print("[STRICT_SANITY] [REDIS] connection closed")
        except Exception:
            pass


def fetch_metrics(out_dir: Path, port: int = 9108) -> None:
    """Зібрати метрики з локального HTTP-ендпойнту застосунку та скопіювати у папку запуску.

    Використовує існуючий інструмент `tools.canary_collect.fetch_metrics` для збору,
    після чого переносить агреговані файли у поточну теку ітерації моніторингу.
    """
    print(f"[STRICT_SANITY] [METRICS] collecting from http://localhost:{port}/metrics")
    try:
        _fetch_metrics(port)
    except Exception as e:
        print(f"[STRICT_SANITY] [METRICS] fetch failed: {e}")
    for name in (
        "metrics.txt",
        "metrics_scenario.txt",
        "metrics_htf.txt",
        "metrics_phase.txt",
    ):
        src = REPO_ROOT / name
        dst = out_dir / name
        try:
            if src.exists():
                dst.write_text(
                    src.read_text(encoding="utf-8", errors="ignore"), encoding="utf-8"
                )
                print(f"[STRICT_SANITY] [METRICS] saved {dst}")
            else:
                print(f"[STRICT_SANITY] [METRICS] source not found: {src}")
        except Exception as e:
            print(f"[STRICT_SANITY] [METRICS] write failed {dst}: {e}")


async def run_monitor(
    symbols: list[str], duration_min: int, interval_min: int, prom_port: int
) -> Path:
    """Основний цикл моніторингу.

    - Створює ізольовану теку запуску з часовою міткою.
    - Кожну ітерацію: збирає метрики та знімає Redis-стани для символів.
    - Після завершення узагальнює трасу сценаріїв і пише артефакти прийнятності.
    """
    start = dt.datetime.utcnow().replace(tzinfo=dt.UTC)
    run_id = start.strftime("rt_%Y%m%d_%H%M%S")
    base_dir = REPO_ROOT / "monitoring" / run_id
    base_dir.mkdir(parents=True, exist_ok=True)

    total_sec = max(1, int(duration_min * 60))
    step_sec = max(60, int(interval_min * 60))
    end_ts = start.timestamp() + total_sec

    print(
        "[STRICT_SANITY] [MONITOR] start",
        f"symbols={len(symbols)}",
        f"duration_min={duration_min}",
        f"interval_min={interval_min}",
        f"prom_port={prom_port}",
        f"out={base_dir}",
    )

    i = 0
    while dt.datetime.utcnow().timestamp() < end_ts:
        i += 1
        loop_t0 = time.perf_counter()
        ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        iter_dir = base_dir / f"t_{ts}"
        iter_dir.mkdir(parents=True, exist_ok=True)

        print(f"[STRICT_SANITY] [MONITOR] iter={i} ts={ts} dir={iter_dir}")

        # 1) metrics
        fetch_metrics(iter_dir, prom_port)
        # 2) redis states
        await snapshot_state(symbols, iter_dir)

        loop_dt = (time.perf_counter() - loop_t0) * 1000.0
        remain = max(0.0, end_ts - dt.datetime.utcnow().timestamp())
        print(
            f"[STRICT_SANITY] [MONITOR] iter={i} done in {loop_dt:.0f} ms; remaining={remain:.0f} s"
        )

        # next
        await asyncio.sleep(step_sec)

    # Після завершення: узагальнити SCENARIO_TRACE у теку запуску
    try:
        summary = _summarize_trace()
    except Exception as e:
        summary = {}
        print(f"[STRICT_SANITY] [MONITOR] summarize_trace failed: {e}")
    (base_dir / "scenario_trace_summary.txt").write_text(
        "\n".join(
            ["category,percent"]
            + [f"{k},{v:.1f}" for k, v in sorted(summary.items(), key=lambda x: -x[1])]
        ),
        encoding="utf-8",
    )
    print(
        f"[STRICT_SANITY] [MONITOR] scenario_trace_summary written: {base_dir / 'scenario_trace_summary.txt'}"
    )

    # Acceptance marker
    acceptance_txt = (
        'Acceptance: expect ai_one_scenario{scenario!="none"} only on stabilized scenarios; '
        "state_* should include scenario_detected/confidence when stabilized.\n"
    )
    (base_dir / "ACCEPTANCE.txt").write_text(acceptance_txt, encoding="utf-8")
    print(
        f"[STRICT_SANITY] [MONITOR] acceptance written: {base_dir / 'ACCEPTANCE.txt'}"
    )
    return base_dir


def main(argv: list[str] | None = None) -> int:
    """CLI-вхідна точка для моніторингу у строгому режимі.

    Параметри:
    - symbols: список символів через кому (напр., BTCUSDT,TONUSDT,SNXUSDT)
    - duration-min: загальна тривалість у хвилинах
    - interval-min: крок між знімками у хвилинах (не менше 1 хв)
    - prom-port: порт HTTP-ендпойнту метрик застосунку
    """
    p = argparse.ArgumentParser(
        description="Строгий моніторинг сигналів сценаріїв у реальному часі"
    )
    p.add_argument(
        "--symbols",
        default="BTCUSDT,TONUSDT,SNXUSDT",
        help="Символи через кому для відстеження",
    )
    p.add_argument(
        "--duration-min", type=int, default=60, help="Загальна тривалість у хвилинах"
    )
    p.add_argument(
        "--interval-min", type=int, default=5, help="Інтервал вибірки у хвилинах"
    )
    p.add_argument(
        "--prom-port",
        type=int,
        default=int(os.environ.get("PROM_HTTP_PORT", "9108")),
        help="HTTP‑порт Prometheus‑метрик локального застосунку",
    )
    args = p.parse_args(argv)

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    try:
        out_dir = asyncio.run(
            run_monitor(symbols, args.duration_min, args.interval_min, args.prom_port)
        )
        print(f"[STRICT_SANITY] Monitor run completed; artifacts in: {out_dir}")
        return 0
    except KeyboardInterrupt:
        print("[STRICT_SANITY] Interrupted by user")
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
