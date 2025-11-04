from __future__ import annotations

import math

from utils.btc_regime import apply_btc_gate


def test_btc_gate_penalty_for_alt_when_btc_flat_low_htf():
    # Альт, BTC flat і низький HTF → штраф 0.2 (conf *= 0.8)
    conf, applied = apply_btc_gate(
        symbol="TONUSDT",
        conf=0.5,
        regime="flat",
        btc_htf_strength=0.01,
        scen_htf_min=0.02,
        enabled=True,
    )
    assert applied is True
    assert math.isclose(conf, 0.5 * 0.8, rel_tol=1e-9)


essential_symbols = ["BTCUSDT", "btcusdt", "BtcUsdt"]


def test_btc_gate_not_applied_for_btc_symbol():
    for sym in essential_symbols:
        conf, applied = apply_btc_gate(
            symbol=sym,
            conf=0.5,
            regime="flat",
            btc_htf_strength=0.01,
            scen_htf_min=0.02,
            enabled=True,
        )
        assert applied is False
        assert math.isclose(conf, 0.5, rel_tol=1e-9)


def test_btc_gate_not_applied_on_trend_up():
    conf, applied = apply_btc_gate(
        symbol="SNXUSDT",
        conf=0.5,
        regime="trend_up",
        btc_htf_strength=0.50,
        scen_htf_min=0.02,
        enabled=True,
    )
    assert applied is False
    assert math.isclose(conf, 0.5, rel_tol=1e-9)
