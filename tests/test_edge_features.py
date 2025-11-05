from __future__ import annotations

from levels.edge_band import band_width, edge_features


def test_edge_features_basic():
    # Створимо 15 барів навколо бази 100
    base = 100.0
    w = 1.0
    band = (base - w, base + w)
    bars = []
    # 6 повністю всередині band, 4 торкаються hi, інші змішані
    # Всередині: high<=101, low>=99
    for _ in range(6):
        bars.append({"high": base + 0.5, "low": base - 0.5, "close": base})
    # Торкання: high>=101
    for _ in range(4):
        bars.append({"high": base + 1.05, "low": base - 0.4, "close": base})
    # Зовні вниз
    for _ in range(3):
        bars.append({"high": base - 1.2, "low": base - 2.0, "close": base - 1.5})
    # Зовні вгору
    for _ in range(2):
        bars.append({"high": base + 2.0, "low": base + 1.2, "close": base + 1.6})

    ef = edge_features(bars, band)
    assert ef["edge_hits"] == 4.0
    # band_squeeze: 6/15 ~= 0.4
    assert 0.35 <= ef["band_squeeze"] <= 0.45
    # accept_in_band принаймні для 10/15 (6 всередині + 4 з close в середині) ≈ 0.66
    assert ef["accept_in_band"] >= 0.6
    # max_pullback_in_band ≤ w (бо close<=hi)
    assert 0.0 <= ef["max_pullback_in_band"] <= w


def test_band_width_rule():
    # tick домінує
    assert band_width(atr_pct=0.0, tick=0.1, k=0.5) == 0.30000000000000004
    # atr*k домінує
    assert band_width(atr_pct=0.02, tick=0.001, k=0.5) == 0.01
