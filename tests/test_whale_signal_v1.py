from __future__ import annotations

from pathlib import Path

import pytest

from stage3.trade_manager import MinSignalPaperPosition, TradeLifecycleManager
from stage3.whale_signal_telemetry import build_whale_signal_v1_payload
from stage3.whale_signal_v1 import WHALE_MAX_AGE_SEC, compute_whale_min_signal


def _base_view(**overrides: object) -> dict[str, object]:
    view: dict[str, object] = {
        "presence": 0.65,
        "bias": 0.2,
        "vwap_dev": 0.01,
        "dominance": {"buy": True, "sell": False},
        "vol_regime": "normal",
        "zones_summary": {"accum_cnt": 3, "dist_cnt": 1},
        "missing": False,
        "stale": False,
        "age_s": 30,
        "tags": [],
    }
    view.update(overrides)
    return view


@pytest.fixture
def trade_manager(tmp_path: Path) -> TradeLifecycleManager:
    return TradeLifecycleManager(
        log_file=str(tmp_path / "trade_log.jsonl"),
        summary_file=str(tmp_path / "summary_log.jsonl"),
    )


def _make_position(side: str = "long") -> MinSignalPaperPosition:
    return MinSignalPaperPosition(
        side=side,
        risk={},
        opened_ts=0.0,
        time_exit_s=0.0,
        last_snapshot={},
    )


def test_whale_signal_v1_strong_long() -> None:
    result = compute_whale_min_signal(_base_view())

    assert result["enabled"] is True
    assert result["direction"] == "long"
    assert result["profile"] == "strong"
    assert result["confidence"] > 0.8


def test_whale_signal_v1_strong_short_direction() -> None:
    view = _base_view(dominance={"buy": False, "sell": True})
    result = compute_whale_min_signal(view)

    assert result["enabled"] is True
    assert result["direction"] == "short"
    assert result["profile"] == "strong"


def test_whale_signal_v1_soft_profile_thresholds() -> None:
    view = _base_view(presence=0.5, bias=0.12)
    result = compute_whale_min_signal(view)

    assert result["enabled"] is True
    assert result["profile"] == "soft"
    assert 0.5 < result["confidence"] < 0.8


def test_whale_signal_v1_hard_deny_missing_or_age() -> None:
    stale_result = compute_whale_min_signal(_base_view(stale=True))
    assert stale_result["enabled"] is False
    assert stale_result["direction"] == "unknown"

    aged_result = compute_whale_min_signal(_base_view(age_s=WHALE_MAX_AGE_SEC + 5))
    assert aged_result["enabled"] is False
    assert aged_result["reasons"][-1] == "age_exceeded"


def test_whale_signal_v1_direction_unknown_disables() -> None:
    view = _base_view(dominance={"buy": False, "sell": False})
    result = compute_whale_min_signal(view)

    assert result["enabled"] is False
    assert result["direction"] == "unknown"
    assert "dominance_missing" in result["reasons"]


def test_build_whale_signal_v1_payload_includes_view_fields() -> None:
    stats = {
        "whale": _base_view(),
    }

    payload = build_whale_signal_v1_payload(stats)

    assert payload["enabled"] is True
    assert payload["profile"] == "strong"
    assert payload["presence"] == 0.65
    assert payload["zones_summary"] == {"accum_cnt": 3, "dist_cnt": 1}
    assert payload["dominance"] == {"buy": True, "sell": False}


def test_build_whale_signal_v1_payload_tracks_disabled_reasons() -> None:
    stats = {
        "whale": _base_view(dominance={"buy": False, "sell": False}),
    }

    payload = build_whale_signal_v1_payload(stats)

    assert payload["enabled"] is False
    assert payload["direction"] == "unknown"
    assert "dominance_missing" in payload["reasons"]


def test_whale_signal_v1_explain_only_for_volz_reason(
    trade_manager: TradeLifecycleManager,
) -> None:
    view = _base_view(
        presence=0.58,
        bias=0.16,
        vol_regime="hyper",
        zones_summary={"accum_cnt": 0, "dist_cnt": 0},
        phase_reason="volz_too_low",
    )

    result = compute_whale_min_signal(view)

    assert result["profile"] == "explain_only"
    assert result["enabled"] is False
    assert result["phase_reason"] == "volz_too_low"
    assert result["confidence"] == pytest.approx(0.45, rel=0.2)

    position = _make_position()
    position.last_snapshot = {"whale_signal_v1": result, "whale_view": view}

    outcome = trade_manager._enforce_whale_signal_v1("TONUSDT", position, None)

    assert outcome is True
    assert position.last_snapshot["whale_signal_profile"] == "explain_only"
    assert position.last_snapshot["whale_phase_reason"] == "volz_too_low"


def test_whale_signal_v1_explain_only_for_presence_cap_reason(
    trade_manager: TradeLifecycleManager,
) -> None:
    view = _base_view(
        presence=0.55,
        bias=0.13,
        vol_regime="hyper",
        zones_summary={"accum_cnt": 0, "dist_cnt": 0},
        phase_reason="presence_cap_no_bias_htf",
    )

    result = compute_whale_min_signal(view)

    assert result["profile"] == "explain_only"
    assert result["enabled"] is False
    assert result["phase_reason"] == "presence_cap_no_bias_htf"
    assert result["confidence"] < 0.5
