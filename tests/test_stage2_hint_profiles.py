from __future__ import annotations

from app.process_asset_batch import (
    _compute_profile_hint_direction_and_score,
    _active_alt_keys,
)


def _thr(require_dom: bool = False, alt_min: int = 0):
    return {"require_dominance": require_dom, "alt_confirm_min": alt_min}


def test_require_dominance_blocks_without_dom():
    whale = {
        "presence": 0.8,
        "bias": 0.6,
        "vwap_dev": 0.02,
        "dominance": {"buy": False, "sell": False},
    }
    dir_hint, score, dom_ok, alt_ok = _compute_profile_hint_direction_and_score(
        whale,
        presence_min=0.7,
        bias_min=0.55,
        vdev_min=0.01,
        thr=_thr(require_dom=True, alt_min=0),
        alt_flags={"iceberg": False, "vol_spike": True, "zones_dist": True},
    )
    assert dom_ok is False
    assert dir_hint is None  # блокується
    assert 0.0 <= score <= 1.0


def test_alt_confirm_min_blocks_when_insufficient():
    whale = {
        "presence": 0.8,
        "bias": -0.6,
        "vwap_dev": -0.02,
        "dominance": {"buy": False, "sell": True},
    }
    alt1 = {"iceberg": True, "vol_spike": False, "zones_dist": False}
    alt2 = {"iceberg": False, "vol_spike": False, "zones_dist": False}
    # дедуп дає лише 1 активний прапорець
    active = _active_alt_keys(alt1, alt2)
    assert len(active) == 1
    dir_hint, score, dom_ok, alt_ok = _compute_profile_hint_direction_and_score(
        whale,
        presence_min=0.7,
        bias_min=0.55,
        vdev_min=0.01,
        thr=_thr(require_dom=False, alt_min=2),
        alt_flags={k: True for k in active},
    )
    assert dom_ok is True
    assert alt_ok is False
    assert dir_hint is None


def test_dir_emits_and_score_positive():
    whale = {
        "presence": 0.82,
        "bias": 0.62,
        "vwap_dev": 0.015,
        "dominance": {"buy": True, "sell": False},
    }
    dir_hint, score, dom_ok, alt_ok = _compute_profile_hint_direction_and_score(
        whale,
        presence_min=0.7,
        bias_min=0.55,
        vdev_min=0.01,
        thr=_thr(require_dom=True, alt_min=1),
        alt_flags={"iceberg": False, "vol_spike": True, "zones_dist": False},
    )
    assert dir_hint in ("long", "short")
    assert score > 0.5
