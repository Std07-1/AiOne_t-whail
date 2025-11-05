from __future__ import annotations

from whale.profile_selector import select_profile


def test_selector_chop_band_triggers_with_band_features():
    # Згенеруємо 15 барів біля бази 100 з 4 торканнями hi і достатнім squeeze
    base = 100.0
    w = 1.0
    band = (base - w, base + w)
    bars = []
    # 6 повністю всередині band
    for _ in range(6):
        bars.append({"high": base + 0.5, "low": base - 0.5, "close": base})
    # 4 торкання верхньої межі
    for _ in range(4):
        bars.append({"high": base + 1.05, "low": base - 0.4, "close": base})
    # 5 інші
    for _ in range(5):
        bars.append({"high": base + 0.8, "low": base - 1.2, "close": base - 0.6})

    stats = {
        "atr_pct": 0.01,
        "tick": 0.1,
        "last_price": base,
        "bars": bars,
        "price_slope_atr": 0.7,
        "acceptance_ok": False,
    }
    whale = {
        "presence": 0.72,
        "bias": 0.30,
        "vwap_dev": 0.008,
        "dominance": {"buy": False, "sell": False},
    }
    prof, conf, reasons = select_profile(stats, whale, "BTCUSDT")
    assert prof == "chop_pre_breakout_up", reasons
    assert conf >= 0.6
    assert any(r.startswith("band=[") for r in reasons)
    assert any("edge_hits=" in r for r in reasons)
    assert any("band_squeeze_ok" in r for r in reasons)
