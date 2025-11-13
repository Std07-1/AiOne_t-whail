from __future__ import annotations

from pathlib import Path

from tools.quality_snapshot import _render_md


def test_quality_snapshot_render_smoke(tmp_path: Path):
    # фікстурні рядки quality
    rows = [
        {
            "symbol": "BTCUSDT",
            "scenario": "pullback_continuation",
            "p75_conf": "0.66",
            "mean_time_to_stabilize_s": "12.0",
            "reject_top1": "htf_weak",
            "reject_top2": "",
            "reject_top3": "",
            "explain_coverage_rate": "0.75",
            "explain_latency_ms": "4000",
        },
        {
            "symbol": "TONUSDT",
            "scenario": "breakout_confirmation",
            "p75_conf": "0.55",
            "mean_time_to_stabilize_s": "9.5",
            "reject_top1": "presence_drop",
            "reject_top2": "",
            "reject_top3": "",
            "explain_coverage_rate": "0.65",
            "explain_latency_ms": "3000",
        },
        {
            "symbol": "SNXUSDT",
            "scenario": "pullback_continuation",
            "p75_conf": "0.40",
            "mean_time_to_stabilize_s": "15.2",
            "reject_top1": "no_candidate",
            "reject_top2": "",
            "reject_top3": "",
            "explain_coverage_rate": "0.20",
            "explain_latency_ms": "0",
        },
    ]
    md = _render_md(rows, metrics_note="metrics: dummy.txt@2025-01-01 00:00:00Z")
    assert "Quality Snapshot" in md
    assert "BTCUSDT" in md and "TONUSDT" in md and "SNXUSDT" in md
    # табличка рядків і короткі висновки
    assert "p75_conf" in md and "ExpCov" in md


def test_quality_snapshot_min_signal_block() -> None:
    rows: list[dict[str, str]] = []
    min_stats = {
        "BTCUSDT": {
            "candidates": 2,
            "open": 1,
            "exit_total": 1,
            "exit_breakdown": {"time_exit": 1},
        }
    }
    md = _render_md(rows, metrics_note="", min_signal_stats=min_stats)
    assert "Min-signal Paper KPI" in md
    assert "time_exit:1" in md
