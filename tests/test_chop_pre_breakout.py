from __future__ import annotations

from whale.profile_selector import select_profile


def test_chop_pre_breakout_detection_neutral_profile():
    # Моделюємо «пиляння рівня»: багато торкань верхнього краю у вузькому діапазоні,
    # слабка акцептація, нахил достатній, присутність вище порогу, vwap_dev невеликий
    stats = {
        "near_edge": "upper",
        "band_pct": 0.02,
        "acceptance_ok": False,
        "price_slope_atr": 0.7,
        # Допоміжні підрахунки K/M з процесора батчів
        "edge_hits_upper": 4,
        "edge_hits_window": 15,
    }
    whale = {
        "presence": 0.70,
        "bias": 0.30,
        "vwap_dev": 0.008,
        "dominance": {"buy": False, "sell": False},
    }
    prof, conf, reasons = select_profile(stats, whale, "BTCUSDT")
    assert prof == "chop_pre_breakout_up"
    assert conf >= 0.6
    assert any("chop_pre_breakout" in r for r in reasons)
