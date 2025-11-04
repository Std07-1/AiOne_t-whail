from __future__ import annotations

import json
from pathlib import Path

from tools.analyze_telemetry import AnalyzeParams, TelemetryInput, analyze_directory


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def test_analyze_telemetry_happy(tmp_path: Path) -> None:
    base = tmp_path
    # stage1_events.jsonl
    _write_jsonl(
        base / "stage1_events.jsonl",
        [
            {
                "ts": "2025-01-01T00:00:00Z",
                "event": "stage1_signal",
                "symbol": "BTCUSDT",
                "signal": "ALERT",
                "trigger_reasons": ["volume_spike"],
            },
            {
                "ts": "2025-01-01T00:00:01Z",
                "event": "trap_eval",
                "symbol": "BTCUSDT",
                "score": 0.7,
                "fired": True,
            },
        ],
    )

    # stage1_signals.jsonl (monitor rejects)
    _write_jsonl(
        base / "stage1_signals.jsonl",
        [
            {
                "timestamp_iso": "2025-01-01T00:00:00Z",
                "symbol": "BTCUSDT",
                "event": "volume_spike_reject",
                "reject_reason": "below_z",
            }
        ],
    )

    # stage3_events.jsonl
    _write_jsonl(
        base / "stage3_events.jsonl",
        [
            {
                "ts": "2025-01-01T00:00:02Z",
                "event": "open_created",
                "symbol": "BTCUSDT",
                "payload": {"rr_ratio": 1.8},
            }
        ],
    )

    inp = TelemetryInput(base_dir=base)
    summary, md = analyze_directory(inp, AnalyzeParams())

    assert summary["stage1"]["events_total"] == 2
    assert summary["stage1"]["by_signal"].get("ALERT") == 1
    assert summary["stage1"]["trigger_reason_counts"].get("volume_spike") == 1
    assert summary["stage1"]["trap"]["fired"] == 1

    assert summary["stage1_signals"]["rows_total"] == 1
    assert summary["stage1_signals"]["rejects_by_reason"].get("below_z") == 1

    assert summary["stage3"]["events_total"] == 1
    assert summary["stage3"]["by_event"].get("open_created") == 1
    assert summary["stage3"]["rr_ratio"]["avg"] == 1.8

    # Markdown contains key sections
    assert "## Stage1" in md
    assert "## Stage3" in md
