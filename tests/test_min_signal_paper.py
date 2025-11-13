import time
from pathlib import Path

import pytest

from app.asset_state_manager import AssetStateManager
from app.min_signal_adapter import apply_min_signal_hint
from config.config import K_STATS
from stage3.trade_manager import TradeLifecycleManager


@pytest.fixture
def state_manager() -> AssetStateManager:
    manager = AssetStateManager(["btcusdt"])
    manager.update_asset("btcusdt", {K_STATS: {}})
    return manager


def test_hint_enrichment_and_attach(mocker, state_manager: AssetStateManager) -> None:
    snapshot = {
        "ts": 123,
        "presence": 0.6,
        "bias": 0.15,
        "dvr": 1.3,
        "vwap_dev": 0.7,
        "band_pct": 0.02,
        "near_edge": True,
        "retest_ok": False,
    }
    mocker.patch(
        "app.min_signal_adapter.minimal_signal",
        return_value={
            "side": "long",
            "sl_atr": 1.5,
            "tp1_atr": 1.5,
            "tp2_atr": 3.0,
            "time_exit_s": 1800,
            "reasons": ["ok"],
            "snapshot": snapshot,
        },
    )
    mocker.patch("metrics.whale_min_signal_metrics.inc_candidate")
    mocker.patch("metrics.whale_min_signal_metrics.set_state")

    asset_ctx = state_manager.get("btcusdt") or {}
    result = apply_min_signal_hint(
        symbol="btcusdt",
        ctx=asset_ctx,
        manager=state_manager,
        now_ts=time.time(),
    )

    assert result is True
    state = state_manager.get("btcusdt") or {}
    hints = state.get("hints", [])
    assert isinstance(hints, list)
    hint = hints[-1]
    assert hint["payload"]["snapshot"]["ts"] == 123
    last_accept = state.get("last_min_signal_accept")
    assert isinstance(last_accept, dict)
    assert last_accept.get("snapshot", {}).get("ts") == 123


def test_reject_no_hint(mocker, state_manager: AssetStateManager, caplog) -> None:
    caplog.set_level("DEBUG")
    mocker.patch(
        "app.min_signal_adapter.minimal_signal",
        return_value={
            "reject": {
                "reason": "bias_gate",
                "details": {
                    "failed_gate": "bias",
                    "value": 0.05,
                    "threshold": 0.12,
                },
            },
            "snapshot": {"ts": 123, "presence": 0.4},
        },
    )

    asset_ctx = state_manager.get("btcusdt") or {}
    result = apply_min_signal_hint(
        symbol="btcusdt",
        ctx=asset_ctx,
        manager=state_manager,
        now_ts=time.time(),
    )

    assert result is False
    state = state_manager.get("btcusdt") or {}
    hints = state.get("hints", [])
    assert hints == ["Очікування даних..."]
    assert "bias_gate" in caplog.text


@pytest.fixture
def trade_manager(tmp_path: Path) -> TradeLifecycleManager:
    return TradeLifecycleManager(
        log_file=str(tmp_path / "trade_log.jsonl"),
        summary_file=str(tmp_path / "summary_log.jsonl"),
    )


def test_paper_open_and_time_exit(mocker, trade_manager: TradeLifecycleManager) -> None:
    symbol = "btcusdt"
    hint = {
        "kind": "min_signal",
        "side": "long",
        "risk": {
            "sl_atr": 1.5,
            "tp1_atr": 1.5,
            "tp2_atr": 3.0,
            "time_exit_s": 2,
        },
        "payload": {"snapshot": {"ts": 100}},
    }
    asset_state = {"hints": [hint]}

    mocker.patch("stage3.trade_manager.feature_flags.STAGE3_PAPER_ENABLED", True)
    mock_inc_open = mocker.patch("stage3.trade_manager.inc_open")
    mock_inc_exit = mocker.patch("stage3.trade_manager.inc_exit")

    trade_manager.tick(symbol, asset_state, now_ts=100)
    assert trade_manager.has_min_signal_position(symbol) is True
    mock_inc_open.assert_called_once_with(symbol, "long")

    asset_state["hints"] = []
    trade_manager.tick(symbol, asset_state, now_ts=103)
    assert trade_manager.has_min_signal_position(symbol) is False
    mock_inc_exit.assert_called_once_with(symbol, "long", "time_exit")


def test_move_to_break_even_on_tp1(
    mocker, trade_manager: TradeLifecycleManager
) -> None:
    symbol = "btcusdt"
    snapshot = {
        "ts": 100,
        "presence": 0.6,
        "bias": 0.2,
        "dvr": 1.3,
        "vwap_dev": 0.8,
        "atr": 2.0,
    }
    hint = {
        "kind": "min_signal",
        "side": "long",
        "risk": {
            "sl_atr": 1.5,
            "tp1_atr": 1.5,
            "tp2_atr": 3.0,
            "time_exit_s": 600,
        },
        "payload": {"snapshot": dict(snapshot)},
    }
    asset_state = {
        "hints": [hint],
        "last_min_signal_accept": {"snapshot": dict(snapshot)},
        K_STATS: {
            "last_price": 100.0,
            "presence": 0.6,
            "bias": 0.2,
            "dvr": 1.3,
            "atr": 2.0,
        },
    }

    mocker.patch("stage3.trade_manager.feature_flags.STAGE3_PAPER_ENABLED", True)
    trade_manager.tick(symbol, asset_state, now_ts=100)
    position = trade_manager._min_signal_positions[symbol]
    assert position.entry_price == pytest.approx(100.0)
    assert position.atr_at_entry == pytest.approx(2.0)
    assert position.moved_to_be is False

    asset_state["hints"] = []
    asset_state[K_STATS]["last_price"] = 103.0
    trade_manager.tick(symbol, asset_state, now_ts=101)
    position = trade_manager._min_signal_positions[symbol]
    assert position.moved_to_be is True
    assert position.sl_price == pytest.approx(100.0)
