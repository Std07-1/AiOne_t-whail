from __future__ import annotations

from stage2.btc_regime import detect_btc_regime


def test_btc_regime_transitions() -> None:
    # ACCUM baseline
    st = {
        "band_expand": 0.005,
        "htf_strength": 0.05,
        "whale": {"presence": 0.2, "bias": 0.0},
    }
    ctx = {"band_expand_score": 0.005}
    s, reasons = detect_btc_regime(st, ctx)
    assert s == "ACCUM"

    # EXPANSION_SETUP when band_expand grows but bias ~0
    st2 = {
        "band_expand": 0.013,
        "htf_strength": 0.25,
        "whale": {"presence": 0.5, "bias": 0.0},
    }
    s2, _ = detect_btc_regime(st2, ctx)
    assert s2 in {"EXPANSION_SETUP", "BREAKOUT_UP", "BREAKOUT_DOWN"}

    # BREAKOUT_UP when bias positive with expansion
    st3 = {
        "band_expand": 0.013,
        "htf_strength": 0.3,
        "whale": {"presence": 0.6, "bias": 0.2},
    }
    s3, _ = detect_btc_regime(st3, ctx)
    assert s3 == "BREAKOUT_UP"

    # BREAKOUT_DOWN when bias negative with expansion
    st4 = {
        "band_expand": 0.013,
        "htf_strength": 0.3,
        "whale": {"presence": 0.6, "bias": -0.2},
    }
    s4, _ = detect_btc_regime(st4, ctx)
    assert s4 == "BREAKOUT_DOWN"
