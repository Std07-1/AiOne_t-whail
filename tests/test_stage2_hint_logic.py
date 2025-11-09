from __future__ import annotations

import pytest

from config.config_whale import STAGE2_WHALE_TELEMETRY
from process_asset_batch.helpers import build_stage2_hint_from_whale


def test_build_stage2_hint_impulse_buy_direction() -> None:
    cfg = dict(STAGE2_WHALE_TELEMETRY)
    now_ms = 1_700_000_000_000
    whale_embedded = {
        "presence": 0.8,
        "bias": 0.7,
        "vwap_dev": 0.025,
        "features": {"slope_twap": 4.5},
        "dominance": {"buy": True, "sell": False},
        "dominance_meta": {"buy_candidate": True, "buy_confirmed": True},
        "zones_summary": {"dist_cnt": 4},
        "stale": False,
    }

    hint = build_stage2_hint_from_whale(whale_embedded, cfg, now_ms=now_ms)

    assert hint["dir"] == "UP"
    assert hint["ts"] == now_ms
    assert hint["cooldown_s"] == int(round(cfg["hint_cooldown"]["impulse"]))
    assert hint["score"] == pytest.approx(0.805, rel=1e-3)
    assert "vwap_dev↑" in hint["reasons"]
    assert "slope_twap↑" in hint["reasons"]
    assert "zones.dist=4" in hint["reasons"]


def test_build_stage2_hint_neutral_penalty_and_alt_confirm() -> None:
    cfg = dict(STAGE2_WHALE_TELEMETRY)
    now_ms = 1_700_000_100_000
    whale_embedded = {
        "presence": 0.5,
        "bias": -0.2,
        "vwap_dev": 0.015,
        "features": {"slope_twap": 3.6},
        "dominance": {"buy": True},
        "dominance_meta": {"buy_candidate": True, "buy_confirmed": False},
        "zones_summary": {"dist_cnt": 1},
        "stale": False,
    }

    hint = build_stage2_hint_from_whale(whale_embedded, cfg, now_ms=now_ms)

    assert hint["dir"] == "NEUTRAL"
    assert hint["cooldown_s"] == int(round(cfg["hint_cooldown"]["base"]))
    assert "alt_confirm=0" in hint["reasons"]
    assert hint["score"] == pytest.approx(0.267, rel=1e-3)


def test_build_stage2_hint_stale_penalty() -> None:
    cfg = dict(STAGE2_WHALE_TELEMETRY)
    now_ms = 1_700_000_200_000
    whale_embedded = {
        "presence": 0.9,
        "bias": 0.8,
        "vwap_dev": 0.02,
        "features": {"slope_twap": 5.0},
        "dominance": {"buy": True},
        "dominance_meta": {"buy_candidate": True, "buy_confirmed": True},
        "zones_summary": {"dist_cnt": 3},
        "stale": True,
    }

    hint = build_stage2_hint_from_whale(whale_embedded, cfg, now_ms=now_ms)

    assert hint["dir"] == "UP"
    assert "stale" in hint["reasons"]
    assert hint["score"] == pytest.approx(0.4425, rel=1e-3)
