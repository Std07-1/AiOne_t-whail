from stage2.manipulation_detector import (
    Features,
    attach_to_market_context,
    detect_manipulation,
)


def _base_features(**overrides):
    # Reasonable defaults; override per-test
    f = Features(
        symbol="TEST",
        o=100.0,
        h=101.0,
        low=99.0,
        c=100.5,
        atr=1.0,
        volume_z=None,
        range_z=None,
        trade_count_z=None,
        near_edge=None,
        band_pct=None,
        htf_strength=None,
        cumulative_delta=None,
        price_slope_atr=None,
        whale_presence=None,
        whale_bias=None,
        whale_stale=None,
    )
    return f.__class__(**{**f.__dict__, **overrides})


def test_detect_liquidity_grab_up():
    f = _base_features(
        o=100.0,
        h=110.0,
        low=99.0,
        c=101.0,
        atr=1.0,
        whale_presence=0.2,
        whale_bias=0.1,
    )
    ins = detect_manipulation(f)
    assert ins is not None
    assert ins.pattern == "liquidity_grab_up"
    assert ins.direction == "up"
    assert ins.recommendation in ("OBSERVE", "WAIT")


def test_detect_thin_liquidity_move_down():
    # Large range vs ATR with low volume_z/tick_z
    f = _base_features(
        o=100.0,
        h=101.0,
        low=94.0,
        c=95.0,
        atr=2.0,
        range_z=(101.0 - 94.0) / 2.0,  # 3.5
        volume_z=0.3,
        trade_count_z=0.4,
    )
    ins = detect_manipulation(f)
    assert ins is not None
    assert ins.pattern == "thin_liquidity_move"
    assert ins.direction == "down"


def test_detect_delta_divergence_up():
    f = _base_features(
        o=100.0,
        h=101.0,
        low=99.5,
        c=101.0,
        atr=1.0,
        cumulative_delta=-0.25,
    )
    ins = detect_manipulation(f)
    assert ins is not None
    assert ins.pattern == "delta_divergence_up"
    assert ins.direction == "up"


def test_attach_to_market_context_publishes_meta():
    f = _base_features(
        o=100.0,
        h=110.0,
        low=99.0,
        c=101.0,
        atr=1.0,
        whale_presence=0.2,
        whale_bias=0.1,
    )
    ins = detect_manipulation(f)
    market_context = {}
    attach_to_market_context(market_context, ins)
    meta = market_context.get("meta", {})
    assert "manipulation" in meta
    m = meta["manipulation"]
    assert m["direction"] in ("up", "down")
    assert isinstance(m["conf"], float)


def test_controlled_distribution_pattern():
    # BTC-like case: controlled sell pressure without spikes
    f = _base_features(
        o=100.0,
        h=100.6,
        low=99.7,
        c=99.8,  # slight down body
        atr=1.0,
        volume_z=1.28,  # in [1.0, 1.8]
        htf_strength=0.21,  # < 0.25
        whale_presence=0.55,  # >= 0.5
        whale_bias=-0.80,  # <= -0.6
        near_edge="lower",
        cumulative_delta=-0.2,  # same direction as price, avoid divergence
        range_z=(100.6 - 99.7) / 1.0,  # small range_z to avoid thin_liquidity trigger
    )
    ins = detect_manipulation(f)
    assert ins is not None
    assert ins.pattern == "controlled_distribution"
    assert ins.direction == "down"
    assert ins.recommendation == "OBSERVE"
