from __future__ import annotations

import csv
from pathlib import Path

from tools.quality_snapshot import _load_quality_rows


def test_quality_snapshot_csv_parse(tmp_path: Path):
    # Підготуємо мінімальний quality.csv
    csv_path = tmp_path / "quality.csv"
    rows = [
        {
            "symbol": "BTCUSDT",
            "scenario": "pullback_continuation",
            "activations": 3,
            "p75_conf": 0.62,
            "mean_time_to_stabilize_s": 12.0,
            "reject_top1": "htf_weak",
            "reject_top1_rate": 0.5,
            "reject_top2": "presence_below_min",
            "reject_top2_rate": 0.3,
            "reject_top3": "",
            "reject_top3_rate": 0.0,
            "explain_coverage_rate": 0.7,
            "explain_latency_ms": 4000,
        }
    ]
    fieldnames = list(rows[0].keys())
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    parsed = _load_quality_rows(csv_path)
    assert isinstance(parsed, list) and len(parsed) == 1
    assert parsed[0]["symbol"].upper() == "BTCUSDT"
