#!/usr/bin/env python3
"""Short A/B compare for BTCUSDT snapshots.
Usage:
  python -m tools.ab_compare --a-dir ab_runs/A --b-dir ab_runs/B --symbol BTCUSDT --out ab_runs/report_BTCUSDT.md
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from statistics import mean

from tools.ab_generate_report import parse_dir

LABELED_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][\w:]*)\{(?P<labels>[^}]*)\}\s+(?P<val>[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)$"
)


def _files(p: Path):
    return sorted([x for x in p.glob("metrics_*.txt") if x.is_file()])


def _label_map(s: str):
    m = {}
    for part in s.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            m[k.strip()] = v.strip().strip('"')
    return m


def scn_reject_rate(dirp: Path, sym: str) -> float:
    vals = []
    for f in _files(dirp):
        c = None
        for ln in f.read_text(encoding="utf-8", errors="replace").splitlines():
            m = LABELED_RE.match(ln.strip())
            if not m or m.group("name") != "ai_one_scn_reject_total":
                continue
            lb = _label_map(m.group("labels"))
            if (
                lb.get("symbol", "").upper() == sym
                and lb.get("reason") == "no_candidate"
            ):
                c = float(m.group("val"))
                break
        if c is not None:
            vals.append(c)
    if len(vals) < 2:
        return 0.0
    # Use number of intervals between samples (n-1) to compute average per-snapshot increase
    return max(0.0, (vals[-1] - vals[0]) / (len(vals) - 1))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--a-dir", required=True)
    ap.add_argument("--b-dir", required=True)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    sym = args.symbol.upper()
    a_rows = parse_dir(Path(args.a_dir), sym)
    b_rows = parse_dir(Path(args.b_dir), sym)

    # Mean helper: guard against empty rows and cases where all values are None
    def mh(rows, attr):
        if not rows:
            return None
        vals = [getattr(r, attr) for r in rows if getattr(r, attr) is not None]
        return mean(vals) if vals else None

    # Format helper: support both int and float; show en dash for None
    def fmt(x, n=4):
        return (
            f"{float(x):.{n}f}"
            if x is not None and isinstance(x, (int, float))
            else "–"
        )

    def occ(rows):
        return (
            (sum(1 for r in rows if (getattr(r, "bias", 0) or 0) != 0) / len(rows))
            if rows
            else 0.0
        )

    def act(rows):
        return sum(
            1 for r in rows if (getattr(r, "scn_name", "none") or "none") != "none"
        )

    sr_a = scn_reject_rate(Path(args.a_dir), sym)
    sr_b = scn_reject_rate(Path(args.b_dir), sym)
    md = []
    md.append(f"# A/B короткий зріз {sym}\n")
    md.append(f"- mean htf: A={fmt(mh(a_rows,'htf'))} B={fmt(mh(b_rows,'htf'))}")
    md.append(f"- mean presence: A={fmt(mh(a_rows,'pres'))} B={fmt(mh(b_rows,'pres'))}")
    md.append(f"- bias occupancy: A={fmt(occ(a_rows),2)} B={fmt(occ(b_rows),2)}")
    md.append(f"- scenario activations: A={act(a_rows)} B={act(b_rows)}")
    md.append(f"- no_candidate rate: A={fmt(sr_a,4)} B={fmt(sr_b,4)}\n")
    md.append(
        "| ts | A htf | B htf | A pres | B pres | A bias | B bias | A scn | B scn | A sweepU | B sweepU | A sweepL | B sweepL |"
    )
    md.append("|---|---:|---:|---:|---:|---:|---:|---|---|---:|---:|---:|---:|")
    n = min(len(a_rows), len(b_rows))
    for i in range(n):
        a, b = a_rows[-n + i], b_rows[-n + i]
        ts = a.ts.strftime("%Y-%m-%d %H:%M:%S")
        md.append(
            "| "
            + ts
            + " | "
            + fmt(a.htf)
            + " | "
            + fmt(b.htf)
            + " | "
            + fmt(a.pres)
            + " | "
            + fmt(b.pres)
            + " | "
            + fmt(a.bias, 0)
            + " | "
            + fmt(b.bias, 0)
            + " | "
            + (a.scn_name or "")
            + " | "
            + (b.scn_name or "")
            + " | "
            + fmt(a.lst_upper, 0)
            + " | "
            + fmt(b.lst_upper, 0)
            + " | "
            + fmt(a.lst_lower, 0)
            + " | "
            + fmt(b.lst_lower, 0)
            + " |"
        )
    Path(args.out).write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"Written: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
