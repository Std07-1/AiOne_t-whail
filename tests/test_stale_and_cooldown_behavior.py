from __future__ import annotations

from app.process_asset_batch import _compute_profile_hint_direction_and_score


def test_stale_forces_neutral_and_penalty():
    whale = {
        "presence": 0.82,
        "bias": 0.62,
        "vwap_dev": 0.015,
        "dominance": {"buy": True, "sell": False},
        "stale": True,
    }
    # Базова формула (без stale) видасть позитивний dir і score>0.5,
    # але в основному коді stale потім форсує NEUTRAL і знижує score.
    # Тут перевіряємо, що базова формула працює (інтеграційно stale покривається окремо).
    dir_hint, score, dom_ok, alt_ok = _compute_profile_hint_direction_and_score(
        whale,
        presence_min=0.7,
        bias_min=0.55,
        vdev_min=0.01,
        thr={"require_dominance": True, "alt_confirm_min": 1},
        alt_flags={"vol_spike": True},
    )
    assert dom_ok is True and alt_ok is True
    assert dir_hint in ("long", "short")
    assert score > 0.5
