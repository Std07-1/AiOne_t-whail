from __future__ import annotations

import time

from whale.profile_selector import select_profile, apply_hysteresis


def test_probe_up_selection():
    stats = {"near_edge": "upper", "band_pct": 0.08}
    whale = {
        "bias": 0.6,
        "vwap_dev": 0.012,
        "presence": 0.72,
        "dominance": {"buy": True, "sell": False},
    }
    prof, conf, reasons = select_profile(stats, whale, "BTCUSDT")
    assert prof == "probe_up"
    assert conf > 0.6
    assert any("dom_buy" in r for r in reasons)


def test_grab_upper_selection():
    stats = {"wick_upper_q": 0.85, "acceptance_ok": False}
    whale = {"bias": 0.1, "vwap_dev": 0.0, "presence": 0.6, "dominance": {}}
    prof, conf, reasons = select_profile(stats, whale, "ETHUSDT")
    assert prof == "grab_upper"
    assert conf > 0.45
    assert "wick_u>=0.8" in reasons


def test_range_fade_selection():
    stats = {"band_pct": 0.04}
    whale = {"bias": 0.0, "vwap_dev": 0.0, "presence": 0.5, "dominance": {}}
    prof, conf, _ = select_profile(stats, whale, "TONUSDT")
    assert prof == "range_fade"
    assert 0.3 <= conf <= 1.0


def test_profile_hysteresis_holds_prev():
    prev = "range_fade"
    new = "probe_up"
    last_ts = time.time()
    chosen = apply_hysteresis(prev, new, last_ts, hysteresis_s=30)
    assert chosen == prev  # не дозволяє перемикатись раніше 30с


def test_priority_mutex_chop_over_probe_up():
    # Умови одночасно підходять під chop_pre_breakout_up та probe_up → має перемогти chop
    stats = {
        "near_edge": "upper",
        "band_pct": 0.02,
        "acceptance_ok": False,
        "price_slope_atr": 0.7,
        "edge_hits_upper": 4,
        "edge_hits_window": 15,
    }
    whale = {
        "presence": 0.75,
        "bias": 0.6,  # також робить кандидатом на probe_up
        "vwap_dev": 0.012,
        "dominance": {"buy": True, "sell": False},
    }
    prof, conf, reasons = select_profile(stats, whale, "BTCUSDT")
    assert prof == "chop_pre_breakout_up", reasons
    assert conf >= 0.6
