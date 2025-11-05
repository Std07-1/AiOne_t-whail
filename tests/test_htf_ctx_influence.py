from __future__ import annotations

from context.htf_context import h1_ctx, d1_bias


def test_h1_ctx_up_confluence():
    last_h1 = {"open": 100.0, "close": 101.0, "ma": 100.2}
    trend, conf = h1_ctx(last_h1)
    assert trend == "up"
    assert conf is True


def test_h1_ctx_flat_no_conf():
    last_h1 = {"open": 100.0, "close": 100.05, "ma": 100.2}
    trend, conf = h1_ctx(last_h1)
    # close<ma → немає кон-флюенсу, і рух <0.2%
    assert conf is False


def test_d1_bias_basic():
    assert d1_bias({"open": 100.0, "close": 101.0}) == "pos"
    assert d1_bias({"open": 100.0, "close": 99.0}) == "neg"
    assert d1_bias({"open": 100.0, "close": 100.0}) == "flat"
