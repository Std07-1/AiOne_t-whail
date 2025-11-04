"""
Paper PnL Proxy: обчислює winrate/avg R/MFE/MAE/time_to_outcome для «паперових» угод.

Джерело: reports/paper_trades.jsonl (створюється run_stage3_canary/інтеграцією policy_v2→stage3).
Методика: точне вирівнювання по ts_ms з meta, ATR з 14 попер. 1m барів, вікно 60 хв.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from utils.utils import safe_float

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("paper_pnl_proxy")
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


def _symbols_path(ds_dir: Path, symbol: str) -> Path:
    return ds_dir / f"{symbol.lower()}_bars_1m_snapshot.jsonl"


def load_tail(symbol: str, minutes: int, ds_dir: Path) -> list[Bar]:
    path = _symbols_path(ds_dir, symbol)
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").strip().splitlines()
    except Exception:
        return []
    chunk = lines[-max(minutes + 60, 0) :]
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
    return (sum(trs) / len(trs)) if trs else 0.0


def evaluate_trade(
    symbol: str, ts_ms: int, direction: str, tp_atr: float, sl_atr: float, ds_dir: Path
) -> dict[str, float]:
    bars = load_tail(symbol, 60 + 40, ds_dir)
    if not bars:
        return {"r": 0.0, "mfe_atr": 0.0, "mae_atr": 0.0, "t_sec": 0.0}
    pre = [b for b in bars if b.ts <= ts_ms]
    win = [b for b in bars if b.ts > ts_ms][:60]
    if not pre or not win:
        return {"r": 0.0, "mfe_atr": 0.0, "mae_atr": 0.0, "t_sec": 0.0}
    atr = atr_1m(pre, 14)
    if atr <= 0:
        return {"r": 0.0, "mfe_atr": 0.0, "mae_atr": 0.0, "t_sec": 0.0}
    entry = win[0].c
    up = entry + float(tp_atr) * atr
    dn = entry - float(sl_atr) * atr
    mfe = 0.0
    mae = 0.0
    t_hit = 0.0
    outcome_r = 0.0
    for _idx, b in enumerate(win, 1):
        # update mfe/mae
        if direction.upper() == "BUY":
            mfe = max(mfe, b.h - entry)
            mae = max(mae, entry - b.low)
            # outcome check
            hit_tp = b.h >= up
            hit_sl = b.low <= dn
        else:
            mfe = max(mfe, entry - b.low)
            mae = max(mae, b.h - entry)
            hit_tp = b.low <= dn  # for SELL take profit at dn
            hit_sl = b.h >= up  # stop at up
        if hit_tp or hit_sl:
            t_hit = max(0.0, (b.ts - ts_ms) / 1000.0)
            outcome_r = (float(tp_atr) / float(sl_atr)) if hit_tp else -1.0
            break
    if t_hit == 0.0 and win:
        # proxy outcome at end-of-window
        last = win[-1]
        if direction.upper() == "BUY":
            px = last.c
            outcome_r = (px - entry) / (float(sl_atr) * atr)
        else:
            px = last.c
            outcome_r = (entry - px) / (float(sl_atr) * atr)
        t_hit = max(0.0, (last.ts - ts_ms) / 1000.0)
    return {
        "r": float(outcome_r),
        "mfe_atr": float(mfe / atr if atr else 0.0),
        "mae_atr": float(mae / atr if atr else 0.0),
        "t_sec": float(t_hit),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", type=str, default="reports/paper_trades.jsonl")
    ap.add_argument("--ds-dir", type=str, default="datastore")
    ap.add_argument("--out", type=str, default="reports/paper_pnl_proxy.md")
    args = ap.parse_args()

    trades_path = Path(args.trades)
    ds_dir = Path(args.ds_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    if trades_path.exists():
        for ln in trades_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            try:
                rec = json.loads(ln)
            except Exception:
                continue
            symbol = str(rec.get("symbol") or "").upper()
            scenario = str(rec.get("scenario") or "").upper()
            direction = str(rec.get("direction") or "").upper()
            meta = rec.get("meta") or {}
            ts_ms = int(meta.get("ts_ms") or 0)
            tp_atr = float(meta.get("tp_atr") or 0.0)
            sl_atr = float(meta.get("sl_atr") or 0.0)
            if not (symbol and direction and ts_ms and tp_atr and sl_atr):
                continue
            res = evaluate_trade(symbol, ts_ms, direction, tp_atr, sl_atr, ds_dir)
            rows.append(
                {
                    "symbol": symbol,
                    "scenario": scenario,
                    "direction": direction,
                    **res,
                }
            )

    # Агрегація
    agg: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "n": 0.0,
            "wins": 0.0,
            "losses": 0.0,
            "sum_r": 0.0,
            "sum_mfe": 0.0,
            "sum_mae": 0.0,
            "sum_t": 0.0,
        }
    )
    for r in rows:
        key = r.get("scenario") or "ALL"
        a = agg[key]
        a["n"] += 1.0
        a["wins"] += 1.0 if float(r["r"]) > 0 else 0.0
        a["losses"] += 1.0 if float(r["r"]) < 0 else 0.0
        a["sum_r"] += float(r["r"]) or 0.0
        a["sum_mfe"] += float(r["mfe_atr"]) or 0.0
        a["sum_mae"] += float(r["mae_atr"]) or 0.0
        a["sum_t"] += float(r["t_sec"]) or 0.0
    # Загальна стрічка
    if rows:
        a_all = {
            "n": 0.0,
            "wins": 0.0,
            "losses": 0.0,
            "sum_r": 0.0,
            "sum_mfe": 0.0,
            "sum_mae": 0.0,
            "sum_t": 0.0,
        }
        for r in rows:
            a_all["n"] += 1.0
            a_all["wins"] += 1.0 if float(r["r"]) > 0 else 0.0
            a_all["losses"] += 1.0 if float(r["r"]) < 0 else 0.0
            a_all["sum_r"] += float(r["r"]) or 0.0
            a_all["sum_mfe"] += float(r["mfe_atr"]) or 0.0
            a_all["sum_mae"] += float(r["mae_atr"]) or 0.0
            a_all["sum_t"] += float(r["t_sec"]) or 0.0
        agg["ALL"] = a_all

    # Рендер MD
    def _fmt(key: str, a: dict[str, float]) -> str:
        n = int(a["n"]) or 0
        if n <= 0:
            return f"### {key}\n\n—\n"
        winrate = 100.0 * (a["wins"] / a["n"]) if a["n"] else 0.0
        avg_r = a["sum_r"] / a["n"] if a["n"] else 0.0
        avg_mfe = a["sum_mfe"] / a["n"] if a["n"] else 0.0
        avg_mae = a["sum_mae"] / a["n"] if a["n"] else 0.0
        avg_t = a["sum_t"] / a["n"] if a["n"] else 0.0
        return (
            f"### {key}\n\n"
            f"- trades: {n}\n"
            f"- winrate: {winrate:.1f}%\n"
            f"- avg R: {avg_r:.2f}\n"
            f"- avg MFE(ATR): {avg_mfe:.2f}\n"
            f"- avg MAE(ATR): {avg_mae:.2f}\n"
            f"- avg time to outcome (s): {avg_t:.1f}\n"
        )

    sections: list[str] = ["# Paper PnL Proxy\n"]
    for key, a in agg.items():
        sections.append(_fmt(str(key), a))
        sections.append("\n")
    out_path.write_text("\n".join(sections), encoding="utf-8")
    logger.info(f"[PAPER_PNL] saved path={out_path}")

    # Опційні Prometheus‑метрики під прапорами
    prom_enabled = False
    paper_on = False
    try:
        from config.config import PROM_GAUGES_ENABLED, STAGE3_PAPER_ENABLED

        prom_enabled = bool(PROM_GAUGES_ENABLED)
        paper_on = bool(STAGE3_PAPER_ENABLED)
    except Exception:
        pass
    try:
        from telemetry import prom_gauges as prom
    except Exception:
        prom = None  # type: ignore[assignment]
    if prom is not None and prom_enabled and paper_on:
        # Не стартуємо HTTP сервер тут (див. примітку у tmp_emit_metrics.py)
        for key, a in agg.items():
            n = float(a.get("n") or 0.0)
            if n <= 0:
                continue
            winrate = 100.0 * (float(a.get("wins") or 0.0) / n)
            avg_r = float(a.get("sum_r") or 0.0) / n
            try:
                prom.set_paper_winrate(str(key), float(winrate))
                prom.set_paper_avg_r(str(key), float(avg_r))
                logger.info(
                    f"[PAPER_PNL] metrics scenario={key} winrate={winrate:.1f}% avg_r={avg_r:.3f}"
                )
            except Exception:
                pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
