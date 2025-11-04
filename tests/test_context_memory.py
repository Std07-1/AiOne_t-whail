from __future__ import annotations

from context.context_memory import ContextMemory


def test_context_memory_aggregates_basic() -> None:
    ctx = ContextMemory(n=5)
    # Add three points: near_edge twice, presence high twice, htf high once
    ctx.update(
        near_edge="upper", whale_presence=0.7, htf_strength=0.1, band_expand=0.01
    )
    ctx.update(near_edge=None, whale_presence=0.2, htf_strength=0.3, band_expand=0.02)
    ctx.update(
        near_edge="lower", whale_presence=0.8, htf_strength=0.1, band_expand=0.03
    )

    # near_edge_persist over last 3 should be 2/3
    assert abs(ctx.near_edge_persist(3) - (2.0 / 3.0)) < 1e-6
    # presence_sustain thr=0.6: 2/3
    assert abs(ctx.presence_sustain(3, 0.6) - (2.0 / 3.0)) < 1e-6
    # htf_sustain thr=0.2: 1/3
    assert abs(ctx.htf_sustain(3, 0.2) - (1.0 / 3.0)) < 1e-6
    # band_expand average over 3: (0.01+0.02+0.03)/3
    assert abs(ctx.band_expand_score(3) - (0.01 + 0.02 + 0.03) / 3.0) < 1e-9

    meta = ctx.export_meta(w=3)
    assert 0.0 <= meta["near_edge_persist"] <= 1.0
    assert 0.0 <= meta["presence_sustain"] <= 1.0
    assert 0.0 <= meta["htf_sustain"] <= 1.0
    assert meta["band_expand_score"] > 0.0
