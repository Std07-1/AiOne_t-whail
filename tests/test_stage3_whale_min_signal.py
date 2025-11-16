"""Контрольні тести Stage3 мін-сигналу через whale_min_signal_view."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pytest

from config.config import K_STATS
from stage3.trade_manager import TradeLifecycleManager


@pytest.fixture(autouse=True)
def enable_min_signal_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Вмикаємо paper-режим і whale-view для всіх тестів."""
    monkeypatch.setattr("stage3.trade_manager.feature_flags.STAGE3_PAPER_ENABLED", True)
    monkeypatch.setattr("stage3.trade_manager.WHALE_MIN_SIGNAL_VIEW_ENABLED", True)


@pytest.fixture
def trade_manager(tmp_path: Path) -> TradeLifecycleManager:
    return TradeLifecycleManager(
        log_file=str(tmp_path / "trade_log.jsonl"),
        summary_file=str(tmp_path / "summary_log.jsonl"),
    )


def _whale_stats(
    *,
    presence: float,
    bias: float,
    vwap_dev: float,
    dvr: float,
    missing: bool = False,
    stale: bool = False,
    age_s: int = 15,
    dominance: Mapping[str, Any] | None = None,
    zones_summary: Mapping[str, Any] | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    dominance_block = dominance or {"buy": True, "sell": False}
    zones_block = zones_summary or {"accum_cnt": 1, "dist_cnt": 0}
    payload = {
        K_STATS: {
            "last_price": 100.0,
            "dvr": dvr,
            "whale": {
                "presence": presence,
                "bias": bias,
                "vwap_dev": vwap_dev,
                "dominance": dominance_block,
                "zones_summary": zones_block,
                "vol_regime": "normal",
                "missing": missing,
                "stale": stale,
                "age_s": age_s,
                "reasons": ["canary"],
            },
        }
    }
    if tags:
        payload[K_STATS]["tags"] = list(tags)
    return payload


def _seed_hint(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "kind": "min_signal",
        "side": "long",
        "risk": {"tp1_atr": 1.5, "sl_atr": 1.2, "time_exit_s": 900},
        "payload": {"snapshot": dict(snapshot)},
    }


def _open_position(
    manager: TradeLifecycleManager,
    symbol: str,
    snapshot: Mapping[str, Any],
    stats_payload: dict[str, Any],
    *,
    now_ts: float,
) -> dict[str, Any]:
    asset_state = {
        **stats_payload,
        "hints": [_seed_hint(snapshot)],
        "last_min_signal_accept": {"snapshot": dict(snapshot)},
    }
    manager.tick(symbol, asset_state, now_ts=now_ts)
    assert manager.has_min_signal_position(symbol)
    asset_state["hints"] = []
    return asset_state


def _force_stats_refresh(manager: TradeLifecycleManager, symbol: str) -> None:
    # Імітуємо ситуацію без snapshot, щоб Stage3 перечитав stats через view.
    manager._min_signal_positions[symbol].last_snapshot = {}


def test_min_signal_keeps_position_when_whale_view_strong(
    trade_manager: TradeLifecycleManager,
) -> None:
    symbol = "btcusdt"
    base_snapshot: dict[str, Any] = {}
    open_ts = 100.0
    stats_payload = _whale_stats(
        presence=0.62,
        bias=0.18,
        vwap_dev=0.9,
        dvr=1.25,
        tags=["whale_min_signal"],
    )
    asset_state = _open_position(
        trade_manager,
        symbol,
        base_snapshot,
        stats_payload,
        now_ts=open_ts,
    )

    _force_stats_refresh(trade_manager, symbol)
    trade_manager.tick(symbol, asset_state, now_ts=open_ts + 30)

    assert trade_manager.has_min_signal_position(symbol) is True
    position = trade_manager._min_signal_positions[symbol]
    whale_view = position.last_snapshot.get("whale_view", {})
    assert whale_view["presence"] == pytest.approx(0.62)
    assert whale_view["bias"] == pytest.approx(0.18)
    assert whale_view["dominance"] == {"buy": True, "sell": False}
    assert whale_view["tags"] == ["whale_min_signal"]


def test_min_signal_closes_on_presence_drop(
    mocker: pytest.MockFixture,
    trade_manager: TradeLifecycleManager,
) -> None:
    symbol = "btcusdt"
    base_snapshot: dict[str, Any] = {}
    open_ts = 200.0
    stats_payload = _whale_stats(
        presence=0.55,
        bias=0.16,
        vwap_dev=0.7,
        dvr=1.3,
    )
    asset_state = _open_position(
        trade_manager,
        symbol,
        base_snapshot,
        stats_payload,
        now_ts=open_ts,
    )

    mock_inc_exit = mocker.patch("stage3.trade_manager.inc_exit")
    weak_stats = _whale_stats(
        presence=0.2,
        bias=0.01,
        vwap_dev=0.05,
        dvr=1.1,
    )
    asset_state.update(weak_stats)

    _force_stats_refresh(trade_manager, symbol)
    trade_manager.tick(symbol, asset_state, now_ts=open_ts + 25)

    assert trade_manager.has_min_signal_position(symbol) is False
    mock_inc_exit.assert_called_once_with(symbol, "long", "presence_drop")


def test_min_signal_current_logic_ignores_missing_state(
    trade_manager: TradeLifecycleManager,
) -> None:
    symbol = "btcusdt"
    base_snapshot: dict[str, Any] = {}
    open_ts = 300.0
    stats_payload = _whale_stats(
        presence=0.6,
        bias=0.2,
        vwap_dev=0.85,
        dvr=1.35,
    )
    asset_state = _open_position(
        trade_manager,
        symbol,
        base_snapshot,
        stats_payload,
        now_ts=open_ts,
    )

    problematic_stats = _whale_stats(
        presence=0.58,
        bias=0.19,
        vwap_dev=0.82,
        dvr=1.2,
        missing=True,
        stale=True,
        age_s=240,
    )
    asset_state.update(problematic_stats)

    _force_stats_refresh(trade_manager, symbol)
    trade_manager.tick(symbol, asset_state, now_ts=open_ts + 45)

    assert trade_manager.has_min_signal_position(symbol) is True
    position = trade_manager._min_signal_positions[symbol]
    whale_view = position.last_snapshot.get("whale_view", {})
    assert whale_view["missing"] is True
    assert whale_view["stale"] is True
    assert whale_view["age_s"] == 240


"""Контрольні тести для поточної поведінки мін-сигналу Stage3 перед подальшим тюнінгом WhaleTelemetry."""
