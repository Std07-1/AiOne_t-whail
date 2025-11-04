from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Перевірка прийомки епізодів (без нових прогонів)"
    )
    ap.add_argument(
        "--in",
        dest="in_csv",
        required=True,
        help="Шлях до <out>/run/replay_summary.csv",
    )
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    if not in_path.exists():
        print(f"[ACCEPTANCE] input not found: {in_path}")
        return 2

    df = pd.read_csv(in_path)
    if df.empty:
        print("ACCEPTANCE FAILED: [] — порожній replay_summary.csv")
        return 5

    fails: list[tuple[str, str]] = []
    for eid, g in df.groupby("episode_id"):
        bars = int(len(g))
        strict_ok = int(g.get("strict_ok", 0).sum() if "strict_ok" in g else 0)
        nontrivial = (
            int(g.get("phase").fillna("NONE").ne("NORMAL").sum()) if "phase" in g else 0
        )
        pos_conf = (
            int((g.get("confidence", 0).fillna(0) > 0).sum())
            if "confidence" in g
            else 0
        )
        if strict_ok < bars:
            fails.append((str(eid), f"strict_ok {strict_ok}/{bars}"))
        if nontrivial == 0:
            fails.append((str(eid), "no nontrivial phases"))
        if pos_conf == 0:
            fails.append((str(eid), "no positive confidence"))

    if fails:
        print("ACCEPTANCE FAILED:", json.dumps(fails, ensure_ascii=False))
        return 5

    print("ACCEPTANCE OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
