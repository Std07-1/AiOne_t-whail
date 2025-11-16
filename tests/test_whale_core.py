from whale.core import WhaleCore, WhaleInput


def test_whale_core_builds_stats_and_tags() -> None:
    core = WhaleCore()
    whale_input = WhaleInput(
        symbol="BTCUSDT",
        whale_snapshot={
            "presence": 1.2,
            "bias": -2.5,
            "vwap_dev": 0.4,
            "ts": 1_000,
            "ts_s": 1,
            "missing": False,
            "stale": True,
            "zones_summary": {"accum_cnt": 5, "dist_cnt": 1},
            "vol_regime": "high",
        },
        stats_slice={"current_price": 100.0, "vwap": 99.0},
        stage2_derived={},
        directional={"price_slope_atr": 1.5},
        time_context={"now_ts": 5.0},
    )

    telemetry = core.compute(whale_input)

    assert telemetry.stats_payload["presence"] == 1.0
    assert telemetry.stats_payload["bias"] == -1.0
    assert "whale_stale" in telemetry.tags
    assert "whale_soft_stale" in telemetry.tags
    assert telemetry.meta_payload.get("presence_score") is not None
