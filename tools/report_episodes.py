from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import mean, median
from typing import Any


def _to_float(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Звіт по епізодах з replay_summary.csv")
    ap.add_argument(
        "--in",
        dest="in_path",
        required=True,
        help="Шлях до <out>/run/replay_summary.csv",
    )
    args = ap.parse_args()

    in_path = Path(args.in_path)
    if not in_path.exists():
        print(f"[REPORT] input not found: {in_path}")
        return 1

    # Обчислюємо, куди писати episodes_report.csv
    base = in_path.parent
    out_dir = base.parent if base.name == "run" else base
    out_csv = out_dir / "episodes_report.csv"

    with in_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        groups: dict[str, list[dict[str, Any]]] = {}
        for r in reader:
            eid = r.get("episode_id") or ""
            if not eid:
                # fallback: з _source_csv не намагаємось відновлювати
                continue
            groups.setdefault(eid, []).append(dict(r))

    rows_out: list[dict[str, Any]] = []

    for eid, rows in groups.items():
        symbol = eid.split(":", 1)[0]
        scenario = rows[0].get("scenario_detected", "")
        strict_ok_sum = sum(int(r.get("strict_ok") or 0) for r in rows)
        bars = len(rows)
        key_phase = (
            "phase"
            if "phase" in rows[0]
            else ("signal" if "signal" in rows[0] else None)
        )
        phase_counts: dict[str, int] = {}
        if key_phase:
            for r in rows:
                v = str(r.get(key_phase) or "").strip()
                if not v:
                    continue
                phase_counts[v] = phase_counts.get(v, 0) + 1
            confidences = [
                x
                for x in (_to_float(r.get("confidence")) for r in rows)
                if x is not None
            ]
            confidence_mean = round(mean(confidences), 4) if confidences else ""
            if confidences:
                cs = sorted(confidences)
                idx = int(0.75 * (len(cs) - 1))
                confidence_p75 = round(cs[idx], 4)
            else:
                confidence_p75 = ""
        htf_vals = [
            x for x in (_to_float(r.get("htf_strength")) for r in rows) if x is not None
        ]
        htf_p50 = round(median(htf_vals), 4) if htf_vals else ""
        pres_vals = [
            x for x in (_to_float(r.get("presence")) for r in rows) if x is not None
        ]
        if pres_vals:
            pres_vals_sorted = sorted(pres_vals)
            idx = int(0.95 * (len(pres_vals_sorted) - 1))
            pres_p95 = round(pres_vals_sorted[idx], 4)
        else:
            pres_p95 = ""
        rows_out.append(
            {
                "episode_id": eid,
                "symbol": symbol,
                "scenario_detected": scenario,
                "strict_ok_sum": strict_ok_sum,
                "bars": bars,
                "phase_counts": json.dumps(phase_counts, ensure_ascii=False),
                "confidence_mean": confidence_mean,
                "confidence_p75": confidence_p75,
                "presence_p95": pres_p95,
                "htf_strength_p50": htf_p50,
            }
        )

    fieldnames = [
        "episode_id",
        "symbol",
        "scenario_detected",
        "strict_ok_sum",
        "bars",
        "phase_counts",
        "confidence_mean",
        "confidence_p75",
        "presence_p95",
        "htf_strength_p50",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_out)

    # Консольний вивід
    print(",".join(fieldnames))
    for r in rows_out:
        print(",".join(str(r[k]) for k in fieldnames))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
