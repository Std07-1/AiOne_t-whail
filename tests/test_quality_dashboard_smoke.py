from __future__ import annotations

from pathlib import Path

from tools.quality_dashboard import aggregate_by_scenario, render_md


def test_quality_dashboard_smoke(tmp_path: Path):
    # Синтетичні рядки quality.csv (по символах)
    rows = [
        {
            "symbol": "BTCUSDT",
            "scenario": "pullback_continuation",
            "activations": 4,
            "p75_conf": 0.6,
            "mean_time_to_stabilize_s": 10.0,
            "reject_top1": "htf_weak",
            "explain_coverage_rate": 0.7,
        },
        {
            "symbol": "TONUSDT",
            "scenario": "pullback_continuation",
            "activations": 1,
            "p75_conf": 0.5,
            "mean_time_to_stabilize_s": 8.0,
            "reject_top1": "presence_below_min",
            "explain_coverage_rate": 0.6,
        },
        {
            "symbol": "SNXUSDT",
            "scenario": "breakout_confirmation",
            "activations": 2,
            "p75_conf": 0.55,
            "mean_time_to_stabilize_s": 12.0,
            "reject_top1": "no_candidate",
            "explain_coverage_rate": 0.65,
        },
    ]
    agg = aggregate_by_scenario(rows)
    md = render_md(agg)

    out_path = tmp_path / "quality_dashboard.md"
    out_path.write_text(md, encoding="utf-8")

    assert out_path.exists()
    text = out_path.read_text(encoding="utf-8")
    assert "Scenario" in text
    # обидва сценарії присутні
    assert "pullback_continuation" in text
    assert "breakout_confirmation" in text
