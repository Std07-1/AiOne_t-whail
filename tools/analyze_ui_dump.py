"""Analyze UI snapshots JSONL (full or --light).

Usage:
  python -m tools.analyze_ui_dump --path ./replay_dump/ui_snapshots_light.jsonl [--symbol BTCUSDT]

Outputs a compact text summary: snapshots count, distinct symbols, SOFT_* counts,
BTCUSDT (or chosen symbol) band/confidence ranges and recent recommendation histogram.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Any


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--path", required=True, help="Path to JSONL with UI snapshots")
    p.add_argument(
        "--symbol", default="BTCUSDT", help="Focus symbol for per-asset stats"
    )
    p.add_argument(
        "--limit", type=int, default=5000, help="Max lines to process to avoid huge RAM"
    )
    return p.parse_args()


def coerce_dict(x: Any) -> dict[str, Any]:
    return x if isinstance(x, dict) else {}


def main() -> None:
    args = parse_args()

    snapshots = 0
    distinct_symbols: set[str] = set()
    soft_counts = Counter()

    per_symbol = args.symbol.upper()
    band_values: list[float] = []
    conf_values: list[float] = []
    reco_hist = Counter()

    with open(args.path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= args.limit:
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                # Підтримка формату-обгортки: {"raw": "<json>"}
                if (
                    isinstance(obj, dict)
                    and "assets" not in obj
                    and isinstance(obj.get("raw"), str)
                ):
                    try:
                        obj = json.loads(obj["raw"])  # розпарсити вкладений JSON
                    except Exception:
                        pass
            except Exception:
                continue

            assets = coerce_dict(obj).get("assets")
            if not isinstance(assets, list):
                continue

            snapshots += 1

            for a in assets:
                if not isinstance(a, dict):
                    continue
                sym = str(a.get("symbol", "")).upper()
                if sym:
                    distinct_symbols.add(sym)

                # recommendation histogram (SOFT_* vs others)
                rec = str(a.get("recommendation", "")).upper()
                if rec:
                    if rec.startswith("SOFT_"):
                        soft_counts[rec] += 1
                    reco_hist[rec] += 1

                if sym == per_symbol:
                    bp = a.get("band_pct")
                    cf = a.get("confidence")
                    try:
                        if isinstance(bp, (int, float)):
                            band_values.append(float(bp))
                        if isinstance(cf, (int, float)):
                            conf_values.append(float(cf))
                    except Exception:
                        pass

    def safe_stats(vals: list[float]) -> str:
        if not vals:
            return "-"
        try:
            vals_sorted = sorted(vals)
            n = len(vals_sorted)
            p50 = vals_sorted[n // 2]
            p90 = vals_sorted[int(n * 0.9) - 1 if n > 1 else 0]
            return f"min={vals_sorted[0]:.4f} p50={p50:.4f} p90={p90:.4f} max={vals_sorted[-1]:.4f} n={n}"
        except Exception:
            return f"n={len(vals)}"

    print("=== UI snapshots analysis ===")
    print(f"file: {args.path}")
    print(f"snapshots: {snapshots}")
    print(f"distinct_symbols: {len(distinct_symbols)}")
    if soft_counts:
        print("SOFT_* counts:")
        for k, v in soft_counts.most_common():
            print(f"  {k}: {v}")
    print("recommendation histogram (top 10):")
    for k, v in reco_hist.most_common(10):
        print(f"  {k or '-'}: {v}")
    print(f"{per_symbol} band_pct stats: {safe_stats(band_values)}")
    print(f"{per_symbol} confidence stats: {safe_stats(conf_values)}")


if __name__ == "__main__":
    main()
