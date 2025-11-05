from __future__ import annotations

import pytest

from app.process_asset_batch import _active_alt_keys, _score_scale_for_class


def test_alt_flags_deduplication_union():
    d1 = {"iceberg": True, "vol_spike": False, "zones_dist": True}
    d2 = {"iceberg": True, "vol_spike": True, "zones_dist": False}
    active = _active_alt_keys(d1, d2)
    # має бути лише унікальні ключі з True
    assert active == {"iceberg", "vol_spike", "zones_dist"}
    assert len(active) == 3


def test_score_scale_by_class():
    # BTC найвищий, ALTS найнижчий
    assert _score_scale_for_class("BTC") == pytest.approx(1.0)
    assert _score_scale_for_class("ETH") == pytest.approx(0.95)
    assert _score_scale_for_class("ALTS") == pytest.approx(0.85)
