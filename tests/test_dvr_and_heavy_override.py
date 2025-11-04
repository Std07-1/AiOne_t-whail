from __future__ import annotations

from app import process_asset_batch as pab
from config.config import K_DIRECTIONAL_VOLUME_RATIO


def test_dvr_normalize_cap_with_extremes():
    sym = "TESTUSDT"
    # push an extreme value; repeated calls should stay within cap
    out1 = pab._normalize_and_cap_dvr(sym, 123.0)
    assert -5.0 <= out1 <= 5.0
    out2 = pab._normalize_and_cap_dvr(sym, 9999.0)
    assert -5.0 <= out2 <= 5.0
    # NaN/Inf safe
    out3 = pab._normalize_and_cap_dvr(sym, float("nan"))
    assert -5.0 <= out3 <= 5.0


def test_heavy_compute_override_predicate_true():
    stats = {"near_edge": True, "band_pct": 0.03, K_DIRECTIONAL_VOLUME_RATIO: 0.7}
    assert pab._is_heavy_compute_override(stats) is True


def test_heavy_compute_override_predicate_false():
    stats = {"near_edge": True, "band_pct": 0.05, K_DIRECTIONAL_VOLUME_RATIO: 0.7}
    assert pab._is_heavy_compute_override(stats) is False
