from __future__ import annotations

from context.htf_context import d1_bias, h1_ctx


def test_alt_confirm_min_reduces_for_btc_eth_only(monkeypatch):
    # Увімкнути прапор
    import config.flags as flags

    flags.HTF_CONTEXT_ENABLED = True
    from app.process_asset_batch import _htf_adjust_alt_min

    last_h1 = {"open": 100.0, "close": 101.0, "ma": 100.2}
    base_thr = {"alt_confirm_min": 3}

    # BTC: up+conf → зменшується до 2 (мінімум 1)
    new_thr_btc = _htf_adjust_alt_min("BTC", last_h1, base_thr, enabled=True)
    assert new_thr_btc.get("alt_confirm_min") == 2

    # ETH: так само
    new_thr_eth = _htf_adjust_alt_min("ETH", last_h1, base_thr, enabled=True)
    assert new_thr_eth.get("alt_confirm_min") == 2

    # ALTS: без змін
    new_thr_alt = _htf_adjust_alt_min("ALTS", last_h1, base_thr, enabled=True)
    assert new_thr_alt.get("alt_confirm_min") == 3

    # BTC, але без кон-флюенсу → без змін
    last_h1_weak = {"open": 100.0, "close": 100.1, "ma": 100.2}  # <0.2% і close<ma
    new_thr_btc_weak = _htf_adjust_alt_min("BTC", last_h1_weak, base_thr, enabled=True)
    assert new_thr_btc_weak.get("alt_confirm_min") == 3


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
