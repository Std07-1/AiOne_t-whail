"""
Індексатор епізодів реплею: збирає короткий CSV з JSONL/логів під коренем.

Вхід:
  --root DIR   (каталог з *.jsonl, за замовчуванням 'replay_bench')
  --out  PATH  (шлях до CSV результату)

Вихідні колонки (best‑effort, заповнюються якщо знайдені):
  symbol, ts, kind, edge_hits, band_squeeze, wick_ratio, acceptance_ok,
  dominance_side, alt_confirms

ПРИМІТКА: Скрипт толерантний до відсутніх полів і неоднорідних рядків JSONL.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from collections.abc import Iterable
from typing import Any


def _iter_jsonl_files(root: str) -> Iterable[str]:
    patterns = [
        os.path.join(root, "**", "*.jsonl"),
    ]
    for pat in patterns:
        yield from glob.glob(pat, recursive=True)


def _extract_row(obj: dict[str, Any]) -> dict[str, Any]:
    sym = str(obj.get("symbol") or obj.get("sym") or obj.get("pair") or "").upper()
    ts = obj.get("ts") or obj.get("timestamp") or obj.get("time") or obj.get("t")
    kind = (
        obj.get("kind")
        or obj.get("scenario")
        or obj.get("profile")
        or obj.get("event")
        or ""
    )
    # Edge‑band / acceptance
    edge_hits = obj.get("edge_hits") or obj.get("edge_hits_upper")
    band_squeeze = obj.get("band_squeeze") or obj.get("band_squeeze_in")
    # Wick ratio: беремо max верх/низ, якщо присутні
    wu = obj.get("wick_upper_q") or obj.get("wick_u")
    wl = obj.get("wick_lower_q") or obj.get("wick_l")
    wick_ratio = (
        max(x for x in [wu, wl] if isinstance(x, (int, float)))
        if any(isinstance(x, (int, float)) for x in [wu, wl])
        else None
    )
    acceptance_ok = obj.get("acceptance_ok") or obj.get("accept_in_band")
    # Dominance side
    dom = obj.get("dominance") if isinstance(obj.get("dominance"), dict) else None
    dom_side = None
    if isinstance(dom, dict):
        if dom.get("buy"):
            dom_side = "buy"
        elif dom.get("sell"):
            dom_side = "sell"
    # Alt confirms (прапорці)
    alt = obj.get("alt_flags") if isinstance(obj.get("alt_flags"), dict) else None
    alt_cnt = None
    if isinstance(alt, dict):
        alt_cnt = sum(1 for v in alt.values() if bool(v))

    return {
        "symbol": sym,
        "ts": ts,
        "kind": kind,
        "edge_hits": edge_hits,
        "band_squeeze": band_squeeze,
        "wick_ratio": wick_ratio,
        "acceptance_ok": acceptance_ok,
        "dominance_side": dom_side,
        "alt_confirms": alt_cnt,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Індексатор епізодів реплею → CSV")
    ap.add_argument("--root", default="replay_bench", help="Корінь з JSONL")
    ap.add_argument("--out", required=True, help="Файл CSV для запису")
    args = ap.parse_args()

    rows: list[dict[str, Any]] = []
    for path in _iter_jsonl_files(args.root):
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            rows.append(_extract_row(obj))
                    except Exception:
                        continue
        except Exception:
            continue

    # Порядок колонок стабільний
    cols = [
        "symbol",
        "ts",
        "kind",
        "edge_hits",
        "band_squeeze",
        "wick_ratio",
        "acceptance_ok",
        "dominance_side",
        "alt_confirms",
    ]
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
