from __future__ import annotations

from utils.context_frame import build_context_frame


def test_context_frame_persistence_and_compression() -> None:
    # Синтетика: 3 останні бари біля межі, band_expand>0, presence вище порогу
    stats = {
        "near_edge": True,
        "band_expand": 0.12,
        "whale": {"presence": 0.7},
        "htf_strength": 0.3,
    }
    ctx_meta = {
        "near_edge_persist": 1.0,
        "presence_sustain": 0.8,
        "band_expand_score": 0.1,
    }
    cf = build_context_frame(stats, ctx_meta)

    assert cf["compression"]["index"] > 0.0
    assert cf["persistence"]["near_edge_persist"] > 0.0
    assert cf["persistence"]["presence_sustain"] > 0.0
