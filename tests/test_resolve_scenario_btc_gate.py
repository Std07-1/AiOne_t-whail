from __future__ import annotations

from utils.scenario_gate import apply_btc_gate_and_profiles


def test_btc_gate_suppresses_when_thresholds_fail() -> None:
    # pullback_continuation candidate but presence/htf too low under strengthened ACCUM gate
    symbol = "BTCUSDT"
    scn, conf = "pullback_continuation", 0.5
    stats = {"htf_strength": 0.18, "whale": {"presence": 0.55, "bias": 0.1}}
    name, new_conf, used = apply_btc_gate_and_profiles(
        symbol, scn, conf, stats, btc_regime="ACCUM"
    )
    assert name is None and new_conf == 0.0
    assert used["htf_min_eff"] >= 0.02 and used["presence_min_eff"] >= 0.30


def test_btc_gate_keeps_when_thresholds_pass() -> None:
    symbol = "BTCUSDT"
    scn, conf = "breakout_confirmation", 0.6
    stats = {
        "htf_strength": 0.25,
        "dvr": 0.5,
        "directional_volume_ratio": 0.5,
        "whale": {"bias": 0.2},
    }
    name, new_conf, used = apply_btc_gate_and_profiles(
        symbol, scn, conf, stats, btc_regime="BREAKOUT_UP"
    )
    assert name == scn and abs(new_conf - conf) < 1e-9
    assert used["dvr_min_eff"] > 0.0
