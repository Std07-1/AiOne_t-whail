"""Quick analysis: print share of htf_ok=True grouped by volatility_regime.

Usage:
  python -m tools.analyze_summary [--csv path]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=Path("replay_dump_htf/summary.csv"))
    args = parser.parse_args(argv)
    if not args.csv.exists():
        print(f"CSV not found: {args.csv}")
        return 2
    df = pd.read_csv(args.csv)
    reg_col = "volatility_regime"
    ok_col = "htf_ok"
    if reg_col not in df.columns or ok_col not in df.columns:
        print("Missing columns. Columns present:", list(df.columns))
        return 3
    df[reg_col] = df[reg_col].fillna("unknown").astype(str)
    h = df[ok_col]
    if h.dtype == object:
        h = h.map({"True": True, "False": False}).fillna(False)
    else:
        h = h.fillna(False).astype(bool)
    grp = df.groupby(reg_col).apply(
        lambda g: pd.Series({"count": len(g), "ok_true": int(h.loc[g.index].sum())}),
        include_groups=False,
    )
    grp["share_htf_ok_true"] = (grp["ok_true"] / grp["count"]).round(3)
    # Stable order: by count desc then name
    grp = grp.sort_values(["count", grp.index.name], ascending=[False, True])
    print(grp.to_string())

    # Optional: htf_strength stats by regime
    strength_col = "htf_strength"
    if strength_col in df.columns:
        s = pd.to_numeric(df[strength_col], errors="coerce")

        def _agg(g: pd.DataFrame) -> pd.Series:
            vals = s.loc[g.index].dropna()
            if vals.empty:
                return pd.Series(
                    {
                        "count": len(g),
                        "mean": float("nan"),
                        "median": float("nan"),
                        "p25": float("nan"),
                        "p75": float("nan"),
                    }
                )
            return pd.Series(
                {
                    "count": len(g),
                    "mean": round(float(vals.mean()), 6),
                    "median": round(float(vals.median()), 6),
                    "p25": round(float(vals.quantile(0.25)), 6),
                    "p75": round(float(vals.quantile(0.75)), 6),
                }
            )

        print("\nHTF strength stats by volatility_regime:")
        print(
            df.groupby(reg_col)
            .apply(_agg, include_groups=False)
            .sort_values(
                [
                    "count",
                ],
                ascending=[False],
            )
            .to_string()
        )
    else:
        print(
            "\nNote: htf_strength column not found in CSV — rerun replay after wiring it into summary."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
