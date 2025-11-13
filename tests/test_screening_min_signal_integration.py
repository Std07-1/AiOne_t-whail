"""
Тест інтеграції minimal_signal у screening_producer: happy path.
"""

import time
from unittest.mock import patch

from app.asset_state_manager import AssetStateManager
from app.min_signal_adapter import apply_min_signal_hint
from config.config import K_STATS


def test_min_signal_hint_is_attached() -> None:
    symbol = "btcusdt"
    payload = {
        "side": "long",
        "sl_atr": 1.5,
        "tp1_atr": 1.5,
        "tp2_atr": 3.0,
        "time_exit_s": 1800,
        "reasons": ["ok"],
        "snapshot": {
            "presence": 0.61,
            "bias": 0.14,
            "dvr": 1.3,
            "vwap_dev": 0.6,
            "band_pct": 0.02,
            "near_edge": True,
            "retest_ok": False,
        },
    }

    manager = AssetStateManager([symbol])
    manager.update_asset(symbol, {K_STATS: {"presence": 0.61}})
    ctx = manager.state[symbol]

    with (
        patch("app.min_signal_adapter.minimal_signal", return_value=payload),
        patch("app.min_signal_adapter.inc_candidate") as mock_candidate,
        patch("app.min_signal_adapter.set_state"),
    ):
        apply_min_signal_hint(symbol, ctx, manager, now_ts=time.time())

    hints = manager.state[symbol].get("hints")
    assert isinstance(hints, list)
    assert hints[-1]["kind"] == "min_signal"
    mock_candidate.assert_called_once_with(symbol, "long")
