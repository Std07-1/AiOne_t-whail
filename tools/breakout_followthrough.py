"""Аналіз «схожих на пробої» епізодів і фоллоутру за ціною.

- Читає дампи псевдостріму: stage1_events.jsonl і stage1_signals.jsonl.
- Витягає кандидатні події (phase_detected ∈ {pre_breakout, post_breakout, momentum, drift_trend}).
- Для кожної події визначає напрям (UP/DOWN) з ознак (near_edge + slope_atr, cd).
- Обчислює підтвердження за ціною в межах K барів: для UP — max приріст від ціни події ≥ поріг; для DOWN — max просідання ≤ -поріг.
- Рахує time-to-first (TTF) у барах і формує Markdown-звіт.

Запуск (приклад):
    python -m tools.breakout_followthrough \
      --dump-glob replay_bench/last300/dump_* \
      --k 3 5 10 20 30 \
      --thr 0.005 0.01 \
      --out reports/breakout_followthrough_last300.md
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


CANDIDATE_PHASES = {"pre_breakout", "post_breakout", "momentum", "drift_trend"}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
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
    return rows


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Breakout-like follow-through analyzer")
    p.add_argument(
        "--dump-glob",
        default="replay_bench/last300/dump_*",
        help="Глоб-шаблон директорій дампів (містять stage1_events.jsonl, stage1_signals.jsonl)",
    )
    p.add_argument(
        "--k", nargs="*", type=int, default=[3, 5, 10, 20, 30], help="Вікна K у барах"
    )
    p.add_argument(
        "--thr",
        nargs="*",
        type=float,
        default=[0.005, 0.01],
        help="Пороги підтвердження по ціні (частки: 0.005=0.5%, 0.01=1.0%)",
    )
    p.add_argument(
        "--out", default="reports/breakout_followthrough.md", help="MD вихідний файл"
    )
    return p.parse_args(argv)


@dataclass
class Candidate:
    symbol: str
    ts: str
    phase: str
    near_edge: str | None
    slope_atr: float | None
    cd: float | None
    dir: str  # "UP"|"DOWN"|"UNK"
    price: float
    idx: int  # індекс цього бара в послідовності stage1_signals


def _decide_dir(near_edge: str | None, slope_atr: float | None, cd: float | None) -> str:
    # Просте правило: якщо upper і (slope>=0 або cd>=0) → UP; якщо lower і (slope<=0 або cd<0) → DOWN.
    ne = (near_edge or "").strip().lower()
    s = float(slope_atr) if slope_atr is not None else 0.0
    c = float(cd) if cd is not None else 0.0
    if ne == "upper" and (s >= 0 or c >= 0):
        return "UP"
    if ne == "lower" and (s <= 0 or c < 0):
        return "DOWN"
    # fallback за знаком схилу/дельти
    if s > 0 or c > 0:
        return "UP"
    if s < 0 or c < 0:
        return "DOWN"
    return "UNK"


def _build_price_series(signals: list[dict[str, Any]]):
    # Повертає списки ts і цін, щоб індексувати події за часом
    ts_list: list[str] = []
    px_list: list[float] = []
    ts_to_idx: dict[str, int] = {}
    for i, row in enumerate(signals):
        ts = str(row.get("timestamp_iso"))
        px = float(row.get("current_price", 0.0))
        ts_list.append(ts)
        px_list.append(px)
        ts_to_idx[ts] = i
    return ts_list, px_list, ts_to_idx


def _find_candidates(events: list[dict[str, Any]], ts_to_idx: dict[str, int], px_list: list[float], symbol: str) -> list[Candidate]:
    out: list[Candidate] = []
    for ev in events:
        if str(ev.get("event")) != "phase_detected":
            continue
        phase = str(ev.get("phase") or "")
        if phase not in CANDIDATE_PHASES:
            continue
        ts = str(ev.get("ts"))
        if ts not in ts_to_idx:
            continue
        idx = ts_to_idx[ts]
        near_edge = ev.get("near_edge")
        slope_atr = ev.get("slope_atr")
        cd = ev.get("cd")
        direction = _decide_dir(near_edge if isinstance(near_edge, str) else None, slope_atr if isinstance(slope_atr, (int, float)) else None, cd if isinstance(cd, (int, float)) else None)
        price = px_list[idx]
        out.append(
            Candidate(
                symbol=symbol,
                ts=ts,
                phase=phase,
                near_edge=str(near_edge) if near_edge is not None else None,
                slope_atr=float(slope_atr) if isinstance(slope_atr, (int, float)) else None,
                cd=float(cd) if isinstance(cd, (int, float)) else None,
                dir=direction,
                price=price,
                idx=idx,
            )
        )
    return out


def _confirm_stats(px: list[float], i: int, direction: str, ks: list[int], thrs: list[float]) -> dict[str, Any]:
    base = px[i]
    res: dict[str, Any] = {}
    # Для кожного порогу і K знайдемо перший бар, де виконано умову
    for thr in thrs:
        key = f"thr_{int(thr*10000)/100.0:.2f}%"  # 0.50%, 1.00%
        sub: dict[str, Any] = {}
        for k in ks:
            jmax = min(len(px) - 1, i + k)
            window = px[i + 1 : jmax + 1]
            ttf = None
            hit = False
            if window:
                if direction == "UP":
                    for j, val in enumerate(window, start=1):
                        if (val - base) / base >= thr:
                            ttf = j
                            hit = True
                            break
                elif direction == "DOWN":
                    for j, val in enumerate(window, start=1):
                        if (val - base) / base <= -thr:
                            ttf = j
                            hit = True
                            break
            sub[str(k)] = {"hit": hit, "ttf": ttf}
        res[key] = sub
    return res


def _format_md(results: dict[str, list[dict[str, Any]]], ks: list[int], thrs: list[float]) -> str:
    lines: list[str] = []
    lines.append("# Breakout-like episodes: follow-through (price-based)\n")
    lines.append(f"K windows: {ks}, thresholds: {[f'{t*100:.2f}%' for t in thrs]}\n")
    for symbol, rows in results.items():
        lines.append(f"\n## {symbol}\n")
        if not rows:
            lines.append("(кандидатів не знайдено)\n")
            continue
        for r in rows:
            lines.append(
                f"- {r['ts']} phase={r['phase']} dir={r['dir']} near_edge={r['near_edge']} slope_atr={r['slope_atr']:.2f} cd={r['cd']:.2f} price={r['price']:.6g}"
            )
            conf: dict[str, Any] = r["confirm"]
            for thr, stats in conf.items():
                pairs = ", ".join(
                    f"K={k}: {'✓' if stats[str(k)]['hit'] else '—'}"
                    + (f" (ttf={stats[str(k)]['ttf']})" if stats[str(k)]["ttf"] is not None else "")
                    for k in ks
                )
                lines.append(f"  * {thr}: {pairs}")
        lines.append("")
    return "\n".join(lines) + "\n"


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    import glob

    dump_dirs = [Path(p) for p in glob.glob(args.dump_glob)]
    ks: list[int] = sorted(set(int(k) for k in args.k if int(k) > 0))
    thrs: list[float] = sorted(set(float(t) for t in args.thr if float(t) > 0))
    results: dict[str, list[dict[str, Any]]] = {}

    for d in sorted(dump_dirs):
        symbol = d.name.split("_")[1].upper() if "_" in d.name else d.name.upper()
        s1e_path = d / "stage1_events.jsonl"
        s1_path = d / "stage1_signals.jsonl"
        events = _read_jsonl(s1e_path)
        signals = _read_jsonl(s1_path)
        ts_list, px_list, ts_to_idx = _build_price_series(signals)
        cands = _find_candidates(events, ts_to_idx, px_list, symbol)
        rows: list[dict[str, Any]] = []
        for c in cands:
            conf = _confirm_stats(px_list, c.idx, c.dir, ks, thrs)
            rows.append(
                {
                    "symbol": c.symbol,
                    "ts": c.ts,
                    "phase": c.phase,
                    "dir": c.dir,
                    "near_edge": c.near_edge,
                    "slope_atr": float(c.slope_atr or 0.0),
                    "cd": float(c.cd or 0.0),
                    "price": c.price,
                    "confirm": conf,
                }
            )
        results[symbol] = rows

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_format_md(results, ks, thrs), encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
