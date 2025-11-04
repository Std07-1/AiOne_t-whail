"""
Оцінка якості сигналів Policy v2 (TP/FP/UNDECIDED) та короткі KPI.

Правила (T0-реалізація на 1m-хвості без точного time-align):
    - IMPULSE (вікно 10 хв): TP якщо рух у бік ≥ 1.0×ATR і adverse ≤ 0.5×ATR; FP якщо спершу adverse ≥ 0.6×ATR або vol5m < 1.2×median20.
    - ACCUM_BREAKOUT (вікно 60 хв): TP якщо закріплення за рівнем ±0.2×ATR ≤15 хв при vol5m ≥ 1.5×median20 і без повернення >50% ширини band у наступні 45 хв; FP — зворотнє.
    - MEAN_REVERT (вікно 20 хв): TP якщо рух до протилежного краю ≥ 0.5×ATR без adverse > 0.3×ATR; FP якщо продовження імпульсу ≥ 0.8×ATR.

CLI:
    python -m tools.signals_truth --last-hours 4 --logs run_*.log --ds-dir datastore --out reports/signals_qa.csv [--emit-prom=true]
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler

from telemetry import prom_gauges as prom
from utils.utils import safe_float

# Підтримуємо новий формат з ts_ms=... та старий бекап-формат без нього
LOG_RE_TS = re.compile(
    r"\[SIGNAL_V2\]\s+ts_ms=(?P<ts>\d+)\s+symbol=(?P<symbol>\S+)\s+class=(?P<class>\S+)\s+side=(?P<side>\S+)\s+p=(?P<p>[0-9.]+)"
)
LOG_RE_OLD = re.compile(
    r"\[SIGNAL_V2\]\s+symbol=(?P<symbol>\S+)\s+class=(?P<class>\S+)\s+side=(?P<side>\S+)\s+p=(?P<p>[0-9.]+)"
)


# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("signals_truth")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.DEBUG)
    # Пишемо у stdout, щоб tee у tools.run_window коректно дзеркалив у файл UTF-8
    logger.addHandler(RichHandler(console=Console(file=sys.stdout), show_path=True))
    logger.propagate = False  # Не пропагувати далі, щоб уникнути дублювання логів


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    low: float
    c: float
    v: float


def _iter_signal_lines(
    paths: Iterable[Path],
) -> Iterator[tuple[int, str, str, str, float]]:
    for p in paths:
        try:
            for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
                m = LOG_RE_TS.search(line)
                if m:
                    yield (
                        int(m.group("ts")),
                        m.group("symbol"),
                        m.group("class"),
                        m.group("side"),
                        float(m.group("p")),
                    )
                    continue
                m2 = LOG_RE_OLD.search(line)
                if m2:
                    # ts_ms відсутній — вважатимемо 0 (fallback на проксі-оцінку)
                    yield (
                        0,
                        m2.group("symbol"),
                        m2.group("class"),
                        m2.group("side"),
                        float(m2.group("p")),
                    )
        except Exception:
            continue


def _symbols_path(ds_dir: Path, symbol: str) -> Path:
    return ds_dir / f"{symbol.lower()}_bars_1m_snapshot.jsonl"


def load_tail(symbol: str, minutes: int, ds_dir: Path) -> list[Bar]:
    """Читає хвіст 1m-барів із snapshot JSONL (best-effort)."""
    path = _symbols_path(ds_dir, symbol)
    if not path.exists():
        return []
    lines: list[str]
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []
    lines = text.strip().splitlines()
    # беремо із запасом: вікно + 60 барів на контекст
    need = max(0, minutes + 60)
    chunk = lines[-need:]
    out: list[Bar] = []
    for ln in chunk:
        try:
            rec = json.loads(ln)
            ts = int(rec.get("ts") or rec.get("time") or 0)
            o = safe_float(rec.get("o") or rec.get("open"))
            h = safe_float(rec.get("h") or rec.get("high"))
            low_v = safe_float(rec.get("l") or rec.get("low"))
            c = safe_float(rec.get("c") or rec.get("close"))
            v = safe_float(rec.get("v") or rec.get("volume"))
            out.append(Bar(ts, o, h, low_v, c, v))
        except Exception:
            continue
    return out


def atr_1m(bars: list[Bar], n: int = 14) -> float:
    if not bars or len(bars) < n + 1:
        return 0.0
    trs: list[float] = []
    prev_c = bars[-(n + 1)].c
    for b in bars[-n:]:
        tr = max(b.h - b.low, abs(b.h - prev_c), abs(b.low - prev_c))
        trs.append(tr)
        prev_c = b.c
    if not trs:
        return 0.0
    return sum(trs) / len(trs)


def _band_levels(pre: list[Bar]) -> tuple[float | None, float | None, float]:
    if not pre:
        return (None, None, 0.0)
    # 30-хв діапазон на префіксі
    span = pre[-30:] if len(pre) >= 30 else pre
    lows = [b.low for b in span]
    highs = [b.h for b in span]
    lo = min(lows) if lows else None
    hi = max(highs) if highs else None
    width = (hi - lo) if (hi is not None and lo is not None) else 0.0
    return (lo, hi, width)


def _window(bars: list[Bar], minutes: int) -> list[Bar]:
    return bars[-minutes:] if len(bars) >= minutes else bars[:]


def evaluate_impulse(
    signal: tuple[int, str, str, str, float], ds_dir: Path
) -> tuple[str, float, float, float]:
    """IMPULSE: точний time-align за ts_ms, вікно 10 хв.

    Повертає: (verdict, lead_time_s, max_favorable_atr, max_adverse_atr)
    """
    ts_ms, symbol, _cls, side, _p = signal
    bars = load_tail(symbol, 10 + 40, ds_dir)
    if len(bars) < 24:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    if ts_ms > 0:
        pre = [b for b in bars if b.ts <= ts_ms]
        win = [b for b in bars if b.ts > ts_ms][:10]
    else:
        pre = bars[:-10]
        win = _window(bars, 10)
    if not pre or not win:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    atr = atr_1m(pre, n=14)
    if atr <= 0:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    entry = win[0].c
    highs = [b.h for b in win]
    lows = [b.low for b in win]
    mfe = (max(highs) - entry) if side == "long" else (entry - min(lows))
    mae = (entry - min(lows)) if side == "long" else (max(highs) - entry)
    vol5 = sum([b.v for b in win[:5]])
    med20 = 0.0
    pre20 = pre[-20:] if len(pre) >= 20 else pre
    if pre20:
        vols = sorted(b.v for b in pre20)
        med20 = vols[len(vols) // 2]
    verdict = "UNDECIDED"
    if mfe >= 1.0 * atr and mae <= 0.5 * atr:
        verdict = "TP"
    elif mae >= 0.6 * atr:
        verdict = "FP"
    elif med20 > 0 and vol5 < 1.2 * med20:
        verdict = "FP"
    # lead time до піка MFE/MAE (беремо MFE пік)
    peak_idx = 0
    if win:
        peak_idx = highs.index(max(highs)) if side == "long" else lows.index(min(lows))
    lead_s = float((win[peak_idx].ts - ts_ms) / 1000.0) if ts_ms > 0 and win else 0.0
    mfe_atr = float(mfe / atr) if atr else 0.0
    mae_atr = float(mae / atr) if atr else 0.0
    return (verdict, lead_s, mfe_atr, mae_atr)


def evaluate_accum_breakout(
    signal: tuple[int, str, str, str, float], ds_dir: Path
) -> tuple[str, float, float, float]:
    ts_ms, symbol, _cls, side, _p = signal
    bars = load_tail(symbol, 60 + 60, ds_dir)
    if len(bars) < 90:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    if ts_ms > 0:
        pre = [b for b in bars if b.ts <= ts_ms]
        win = [b for b in bars if b.ts > ts_ms][:60]
    else:
        pre = bars[:-60]
        win = _window(bars, 60)
    if not pre or not win:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    atr = atr_1m(pre, n=14)
    if atr <= 0:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    lo, hi, width = _band_levels(pre)
    if lo is None or hi is None or width <= 0:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    # закріплення за рівнем ±0.2*ATR у перші 15 хв
    first15 = win[:15]
    cond_fix = False
    if side == "long":
        thr = hi - 0.2 * atr
        cond_fix = all(b.c >= thr for b in first15[-3:]) or any(
            b.c >= hi for b in first15
        )
    else:
        thr = lo + 0.2 * atr
        cond_fix = all(b.c <= thr for b in first15[-3:]) or any(
            b.c <= lo for b in first15
        )
    vol5 = sum(b.v for b in first15[:5])
    pre20 = pre[-20:] if len(pre) >= 20 else pre
    med20 = 0.0
    if pre20:
        vols = sorted(b.v for b in pre20)
        med20 = vols[len(vols) // 2]
    verdict: str | None = None
    if not (cond_fix and (med20 == 0.0 or vol5 >= 1.5 * med20)):
        # швидке повернення
        verdict = "FP"
    # без повернення >50% ширини band у наступні 45 хв
    next45 = win[15:]
    if side == "long":
        min_after = min(b.c for b in next45) if next45 else win[-1].c
        if (hi - min_after) > 0.5 * width:
            verdict = "FP"
    else:
        max_after = max(b.c for b in next45) if next45 else win[-1].c
        if (max_after - lo) > 0.5 * width:
            verdict = "FP"
    if verdict is None:
        verdict = "TP"
    # Lead time до першого закріплення за рівнем (або піку за напрямком)
    idx = 0
    if side == "long":
        tgt = hi
        for i, b in enumerate(win):
            if b.c >= tgt:
                idx = i
                break
    else:
        tgt = lo
        for i, b in enumerate(win):
            if b.c <= tgt:
                idx = i
                break
    lead_s = float((win[idx].ts - ts_ms) / 1000.0) if ts_ms > 0 and win else 0.0
    entry = win[0].c
    highs = [b.h for b in win]
    lows = [b.low for b in win]
    mfe = (max(highs) - entry) if side == "long" else (entry - min(lows))
    mae = (entry - min(lows)) if side == "long" else (max(highs) - entry)
    mfe_atr = float(mfe / atr) if atr else 0.0
    mae_atr = float(mae / atr) if atr else 0.0
    return (verdict, lead_s, mfe_atr, mae_atr)


def evaluate_mean_revert(
    signal: tuple[int, str, str, str, float], ds_dir: Path
) -> tuple[str, float, float, float]:
    ts_ms, symbol, _cls, side, _p = signal
    bars = load_tail(symbol, 20 + 40, ds_dir)
    if len(bars) < 34:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    if ts_ms > 0:
        pre = [b for b in bars if b.ts <= ts_ms]
        win = [b for b in bars if b.ts > ts_ms][:20]
    else:
        pre = bars[:-20]
        win = _window(bars, 20)
    if not pre or not win:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    atr = atr_1m(pre, n=14)
    if atr <= 0:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    lo, hi, width = _band_levels(pre)
    if lo is None or hi is None or width <= 0:
        return ("UNDECIDED", 0.0, 0.0, 0.0)
    entry = win[0].c
    highs = [b.h for b in win]
    lows = [b.low for b in win]
    if side == "long":
        target_move = hi - entry
        mae = entry - min(lows)
        cont_impulse = max(highs) - entry
    else:
        target_move = entry - lo
        mae = max(highs) - entry
        cont_impulse = entry - min(lows)
    verdict = "UNDECIDED"
    if target_move >= 0.5 * atr and mae <= 0.3 * atr:
        verdict = "TP"
    elif cont_impulse >= 0.8 * atr:
        verdict = "FP"
    # Lead time до піку MFE
    peak_idx = highs.index(max(highs)) if side == "long" else lows.index(min(lows))
    lead_s = float((win[peak_idx].ts - ts_ms) / 1000.0) if ts_ms > 0 and win else 0.0
    mfe = (max(highs) - entry) if side == "long" else (entry - min(lows))
    mfe_atr = float(mfe / atr) if atr else 0.0
    mae_atr = float(mae / atr) if atr else 0.0
    return (verdict, lead_s, mfe_atr, mae_atr)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--last-hours", type=int, default=4)
    ap.add_argument("--logs", type=str, nargs="+", required=True)
    ap.add_argument("--ds-dir", type=str, required=True)
    ap.add_argument("--out", type=str, required=True)
    ap.add_argument("--emit-prom", type=str, default="false")
    args = ap.parse_args()

    log_paths = [Path(x) for x in args.logs]
    ds_dir = Path(args.ds_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    emit_prom = str(args.emit_prom).lower() in {"1", "true", "yes", "on"}
    rows: list[dict[str, str | float | int]] = []
    for sig in _iter_signal_lines(log_paths):
        ts_ms, symbol, cls, side, p = sig
        if cls == "IMPULSE":
            verdict, lead_s, mfe_atr, mae_atr = evaluate_impulse(sig, ds_dir)
        elif cls == "ACCUM_BREAKOUT":
            verdict, lead_s, mfe_atr, mae_atr = evaluate_accum_breakout(sig, ds_dir)
        elif cls == "MEAN_REVERT":
            verdict, lead_s, mfe_atr, mae_atr = evaluate_mean_revert(sig, ds_dir)
        else:
            verdict, lead_s, mfe_atr, mae_atr = ("UNDECIDED", 0.0, 0.0, 0.0)
        if emit_prom:
            try:
                if verdict == "TP":
                    prom.inc_tp(symbol, cls)
                elif verdict == "FP":
                    prom.inc_fp(symbol, cls)
            except Exception:
                pass
        rows.append(
            {
                "symbol": symbol,
                "class": cls,
                "side": side,
                "p_win": p,
                "verdict": verdict,
                "predicted_side": side,
                "ts": ts_ms,
                "lead_time_s": lead_s,
                "max_favorable_atr": mfe_atr,
                "max_adverse_atr": mae_atr,
            }
        )

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "symbol",
                "class",
                "side",
                "p_win",
                "verdict",
                "ts",
                "predicted_side",
                "lead_time_s",
                "max_favorable_atr",
                "max_adverse_atr",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Signals QA saved to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
