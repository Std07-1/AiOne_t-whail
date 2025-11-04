from app.process_asset_batch import _router_signal_v2
from config.config import STAGE2_PROFILE, STAGE2_SIGNAL_V2_PROFILES


def _base_stats():
    return {
        "band_pct": 0.015,
        "near_edge": "upper",
        "rsi": 50.0,
        "price_slope_atr": 0.6,
        "directional_volume_ratio": 0.7,
        "cumulative_delta": 0.3,
        "whale": {
            "stale": False,
            "presence": 0.7,
            "bias": 0.3,
            "vwap_dev": 0.006,
            "zones_summary": {"accum_cnt": 3, "dist_cnt": 0},
        },
    }


def test_router_stale_observe():
    stats = _base_stats()
    stats["whale"]["stale"] = True
    profile_cfg = (
        STAGE2_SIGNAL_V2_PROFILES.get(STAGE2_PROFILE)
        or STAGE2_SIGNAL_V2_PROFILES["strict"]
    )
    sig, conf, _ = _router_signal_v2(stats, profile_cfg)
    assert sig == "OBSERVE"
    assert conf == 0.0


def test_router_soft_buy_when_fresh():
    stats = _base_stats()
    stats["whale"]["stale"] = False
    profile_cfg = (
        STAGE2_SIGNAL_V2_PROFILES.get(STAGE2_PROFILE)
        or STAGE2_SIGNAL_V2_PROFILES["strict"]
    )
    sig, conf, _ = _router_signal_v2(stats, profile_cfg)
    assert sig in ("SOFT_BUY", "ALERT_BUY")
    assert 0.0 <= conf <= 1.0


def test_router_alert_sell_mirror_short():
    stats = _base_stats()
    # Mirror for short
    stats.update(
        {
            "near_edge": "lower",
            "price_slope_atr": -0.7,
            "cumulative_delta": -0.35,
        }
    )
    stats["whale"].update(
        {
            "bias": -0.4,
            "vwap_dev": -0.012,
            "zones_summary": {"accum_cnt": 0, "dist_cnt": 3},
            "stale": False,
        }
    )
    profile_cfg = (
        STAGE2_SIGNAL_V2_PROFILES.get(STAGE2_PROFILE)
        or STAGE2_SIGNAL_V2_PROFILES["strict"]
    )
    sig, conf, _ = _router_signal_v2(stats, profile_cfg)
    assert sig in ("SOFT_SELL", "ALERT_SELL")
    assert 0.0 <= conf <= 1.0
