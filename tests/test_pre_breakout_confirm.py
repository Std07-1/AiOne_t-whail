from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config_whale import STAGE2_WHALE_TELEMETRY  # noqa: E402
from process_asset_batch.helpers import _should_confirm_pre_breakout  # noqa: E402


def _mid_dev_thr() -> float:
    return float(STAGE2_WHALE_TELEMETRY.get("mid_dev_thr", 0.0) or 0.0)


def test_confirm_requires_all_strict_gates() -> None:
    ready, flags = _should_confirm_pre_breakout(
        accept_ok=True,
        alt_confirms_count=3,
        thr_alt_min=2,
        dominance_buy=True,
        vwap_dev=_mid_dev_thr() + 1e-6,
        slope_atr=0.85,
    )
    assert ready is True
    assert flags["pre_breakout_dom"] is True
    assert flags["pre_breakout_vdev_mid"] is True
    assert flags["pre_breakout_slope_strict"] is True


def test_confirm_blocks_without_dom() -> None:
    ready, flags = _should_confirm_pre_breakout(
        accept_ok=True,
        alt_confirms_count=3,
        thr_alt_min=2,
        dominance_buy=False,
        vwap_dev=_mid_dev_thr() + 1e-6,
        slope_atr=0.9,
    )
    assert ready is False
    assert flags["pre_breakout_dom"] is False


def test_confirm_requires_alt_gate_even_with_acceptance() -> None:
    ready, flags = _should_confirm_pre_breakout(
        accept_ok=True,
        alt_confirms_count=1,
        thr_alt_min=3,
        dominance_buy=True,
        vwap_dev=_mid_dev_thr() + 1e-6,
        slope_atr=0.85,
    )
    assert ready is False
    assert flags["pre_breakout_alt_gate"] is False


def test_confirm_allows_alt_trigger_path() -> None:
    ready, flags = _should_confirm_pre_breakout(
        accept_ok=False,
        alt_confirms_count=3,
        thr_alt_min=2,
        dominance_buy=True,
        vwap_dev=_mid_dev_thr() + 5e-6,
        slope_atr=0.9,
    )
    assert ready is True
    assert flags["pre_breakout_alt_trigger"] is True
