from __future__ import annotations

import utils.phase_adapter as pa
from app import process_asset_batch as pab


def test_whale_default_presence_injection():
    stats = {"symbol": "AAAUSDT"}
    assert "whale" not in stats
    pab._ensure_whale_default(stats)
    assert isinstance(stats.get("whale"), dict)
    w = stats["whale"]
    assert w.get("stale") is True
    assert float(w.get("presence") or 0.0) == 0.0


def test_phase_adapter_htf_gray_penalty(monkeypatch):
    # Prepare fake detector result with high score
    class DummyRes:
        def __init__(self) -> None:
            self.phase = "momentum"
            self.score = 0.9
            self.reasons = ["dummy"]
            self.volz_source = "real"
            self.overlay = {}
            self.profile = "strict"
            self.threshold_tag = "t"
            self.strategy_hint = None

    monkeypatch.setattr(pa, "_detect_phase", lambda *_a, **_k: DummyRes())

    # Craft stats with htf_strength in gray band (between gray_low and ok)
    from config.config import HTF_GATES

    gray_low = float(HTF_GATES.get("gray_low", 0.10))
    ok_thr = float(HTF_GATES.get("ok", 0.20))
    penalty = float(HTF_GATES.get("gray_penalty", 0.20))
    mid = (gray_low + ok_thr) / 2.0

    stats = {"volume_z": 1.0, "htf_strength": mid}

    out = pa.detect_phase_from_stats(stats, symbol="ZZZUSDT")
    assert out is not None
    # Score should be penalized by gray_penalty (floored at 0)
    assert abs(out["score"] - max(0.0, 0.9 - penalty)) < 1e-6
