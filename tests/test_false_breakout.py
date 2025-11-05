from __future__ import annotations

from whale.profile_selector import select_profile


def test_false_breakout_routes_to_range_fade():
    # Умови: chop-передпробійні + великій верхній гніт і відсутня акцептація → false_breakout
    stats = {
        "near_edge": "upper",
        "band_pct": 0.02,
        "acceptance_ok": False,
        "price_slope_atr": 0.7,
        "edge_hits_upper": 4,
        "edge_hits_window": 15,
        # проксі для false_breakout
        "wick_upper_q": 0.7,
        "vol_z": 1.6,
    }
    whale = {
        "presence": 0.72,
        "bias": 0.30,  # >0.2, тож grab_upper не спрацює
        "vwap_dev": 0.008,
        "dominance": {"buy": False, "sell": False},
    }
    prof, conf, reasons = select_profile(stats, whale, "TONUSDT")
    assert prof == "range_fade", reasons
    assert conf >= 0.45
    assert any("false_breakout" in r for r in reasons), reasons
