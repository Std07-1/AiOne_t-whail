"""Псевдо-стрім бенчмарк: 300 барів × 3 символи.

Мета:
- Прогнати tools.replay_stream.run_replay для 3 символів по 300 барів (snapshot).
- Заміряти часові метрики та перевірити, що середній час < 200 мс/бар.
- Оцінити «стабільність» stage2_hint як частку фліпів напрямку (UP/DOWN) у phase_detected.
- Зафіксувати результат у JSON/Markdown.

Запуск:
    python -m tools.bench_pseudostream --symbols BTCUSDT,ETHUSDT,TRXUSDT --limit 300 --out-dir ./replay_bench
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import config.config as cfg
except Exception:  # pragma: no cover - fallback for direct script execution
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    import config.config as cfg  # type: ignore  # noqa: E402
from tools.replay_stream import ReplayConfig, run_replay


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        rows.append(obj)
                except Exception:
                    continue
    except FileNotFoundError:
        return []
    return rows


def _hint_stability_from_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    # Візьмемо phase_detected події, з яких дістаємо payload.stage2_hint.dir
    ordered = [e for e in events if str(e.get("event")) == "phase_detected"]
    # Сортування за часом якщо можливе (ts ISO), але рядки вже в порядку запису
    dirs: list[str] = []
    for ev in ordered:
        payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}
        hint = payload.get("stage2_hint") if isinstance(payload, dict) else None
        d = hint.get("dir") if isinstance(hint, dict) else None
        if isinstance(d, str) and d:
            d_up = d.strip().upper()
            if d_up in {"UP", "DOWN"}:
                dirs.append(d_up)
    flips = 0
    for i in range(1, len(dirs)):
        if dirs[i] != dirs[i - 1]:
            flips += 1
    stability = None
    if len(dirs) > 1:
        stability = 1.0 - (flips / (len(dirs) - 1))
    return {
        "events": len(ordered),
        "dirs": len(dirs),
        "flips": flips,
        "stability": stability,
    }


@dataclass
class BenchResult:
    symbol: str
    bars: int
    processed: int
    total_sec: float
    avg_ms_per_bar: float
    hint: dict[str, Any]
    dump_dir: str


async def _run_for_symbol(
    symbol: str, *, limit: int, out_dir: Path, interval: str = "1m"
) -> BenchResult:
    dump_dir = out_dir / f"dump_{symbol.lower()}_{interval}_{limit}"
    cfg = ReplayConfig(
        symbol=symbol,
        interval=interval,
        source="snapshot",
        limit=limit,
        dump_dir=dump_dir,
    )
    t0 = time.perf_counter()
    try:
        stats = await run_replay(cfg)
    except FileNotFoundError:
        # Фолбек на live Binance, якщо немає snapshot
        cfg.source = "binance"
        stats = await run_replay(cfg)
    dt = time.perf_counter() - t0
    # зчитати stage1_events.jsonl для оцінки підказок
    s1e = _read_jsonl(dump_dir / "stage1_events.jsonl")
    hint = _hint_stability_from_events(s1e)
    bars = stats.bars
    processed = stats.processed
    avg_ms = (dt / processed) * 1000.0 if processed else None
    return BenchResult(
        symbol=symbol.upper(),
        bars=bars,
        processed=processed,
        total_sec=dt,
        avg_ms_per_bar=float(avg_ms or 0.0),
        hint=hint,
        dump_dir=str(dump_dir.resolve()),
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Псевдо-стрім бенчмарк 300×3")
    p.add_argument(
        "--symbols",
        default="BTCUSDT,ETHUSDT,TRXUSDT",
        help="Список символів через кому",
    )
    p.add_argument("--limit", type=int, default=300, help="Кількість барів на символ")
    p.add_argument("--interval", default="1m", help="Інтервал")
    p.add_argument(
        "--out-dir", default="./replay_bench", help="Каталог для дампів/звіту"
    )
    p.add_argument(
        "--source",
        default="snapshot",
        choices=["snapshot", "binance"],
        help="Джерело даних",
    )
    p.add_argument("--json", default=None, help="Куди зберегти JSON звіт (опційно)")
    p.add_argument("--md", default=None, help="Куди зберегти Markdown звіт (опційно)")
    # A/B перемикачі (опційно)
    p.add_argument("--enable-accum-caps", action="store_true")
    p.add_argument("--disable-accum-caps", action="store_true")
    p.add_argument("--enable-htf-gray", action="store_true")
    p.add_argument("--disable-htf-gray", action="store_true")
    p.add_argument("--enable-low-atr-spike-override", action="store_true")
    p.add_argument("--disable-low-atr-spike-override", action="store_true")
    p.add_argument("--prometheus", action="store_true")
    return p


def _format_md(results: list[BenchResult]) -> str:
    lines: list[str] = []
    lines.append("# Псевдо-стрім бенчмарк (300×3)\n")
    total_bars = sum(r.processed for r in results)
    total_sec = sum(r.total_sec for r in results)
    avg_ms = (total_sec / total_bars) * 1000.0 if total_bars else 0.0
    lines.append(
        f"Сумарно: bars={total_bars} time={total_sec:.2f}s avg={avg_ms:.2f}ms/бар\n"
    )
    for r in results:
        lines.append(f"## {r.symbol}")
        lines.append(
            f"bars={r.bars} processed={r.processed} time={r.total_sec:.2f}s avg={r.avg_ms_per_bar:.2f}ms | dump={r.dump_dir}"
        )
        h = r.hint
        lines.append(
            f"hint: events={h.get('events')} dirs={h.get('dirs')} flips={h.get('flips')} stability={h.get('stability')}\n"
        )
    return "\n".join(lines) + "\n"


async def _amain(args: argparse.Namespace) -> int:
    # Застосовуємо CLI-прапори до конфігу (rollback‑дружньо)
    if args.enable_accum_caps:
        cfg.STRICT_ACCUM_CAPS_ENABLED = True
    if args.disable_accum_caps:
        cfg.STRICT_ACCUM_CAPS_ENABLED = False
    if args.enable_htf_gray:
        cfg.STRICT_HTF_GRAY_GATE_ENABLED = True
    if args.disable_htf_gray:
        cfg.STRICT_HTF_GRAY_GATE_ENABLED = False
    if args.enable_low_atr_spike_override:
        cfg.STRICT_LOW_ATR_OVERRIDE_ON_SPIKE = True
    if args.disable_low_atr_spike_override:
        cfg.STRICT_LOW_ATR_OVERRIDE_ON_SPIKE = False
    if args.prometheus:
        # Дозволяємо виставлення гейджів, але HTTP‑експортер не стартуємо тут
        cfg.PROM_GAUGES_ENABLED = True

    def _norm(s: str) -> str:
        s = s.strip().upper()
        return s if s.endswith("USDT") else f"{s}USDT"

    symbols = [_norm(s) for s in str(args.symbols).split(",") if s.strip()]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    results: list[BenchResult] = []
    for s in symbols:
        res = await _run_for_symbol(
            s, limit=int(args.limit), out_dir=out_dir, interval=str(args.interval)
        )
        results.append(res)
    # Загальний JSON
    payload = {
        "total_bars": sum(r.processed for r in results),
        "total_sec": sum(r.total_sec for r in results),
        "avg_ms_per_bar": (
            (
                sum(r.total_sec for r in results)
                / max(1, sum(r.processed for r in results))
            )
            * 1000.0
        ),
        "results": [
            {
                "symbol": r.symbol,
                "bars": r.bars,
                "processed": r.processed,
                "total_sec": r.total_sec,
                "avg_ms_per_bar": r.avg_ms_per_bar,
                "hint": r.hint,
                "dump_dir": r.dump_dir,
            }
            for r in results
        ],
    }
    if args.json:
        Path(args.json).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    if args.md:
        Path(args.md).write_text(_format_md(results), encoding="utf-8")
    # простий вихідний код: перевірка бюджету часу
    avg_ms = payload["avg_ms_per_bar"]
    return 0 if avg_ms < 200.0 else 2


def main(argv: Iterable[str] | None = None) -> int:
    p = _build_parser()
    args = p.parse_args(argv)
    return asyncio.run(_amain(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
