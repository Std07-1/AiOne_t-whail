"""Адаптер для канарейкового мінімального китового сигналу."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from config.config import STRICT_PROFILE_ENABLED
from core.whale_processor import minimal_signal
from metrics.whale_min_signal_metrics import inc_candidate, set_state

if TYPE_CHECKING:  # pragma: no cover - типізація допоміжна
    from .asset_state_manager import AssetStateManager

logger = logging.getLogger("min_signal_adapter")
logger.setLevel(logging.DEBUG)

_MIN_SIGNAL_LOG_GAP = 10.0
_min_signal_last_log: dict[str, float] = {}


def apply_min_signal_hint(
    symbol: str,
    ctx: dict[str, Any],
    manager: AssetStateManager,
    *,
    now_ts: float,
) -> bool:
    try:
        ms = minimal_signal(symbol, ctx)
    except Exception as exc:  # pragma: no cover - best effort
        logger.warning("[STRICT_WHALE][min_signal] symbol=%s error=%s", symbol, exc)
        return False

    if not ms:
        if STRICT_PROFILE_ENABLED:
            logger.debug("[STRICT_WHALE][min_signal][reject] symbol=%s", symbol)
        return False

    if isinstance(ms, dict) and "reject" in ms:
        _min_signal_last_log[symbol] = now_ts
        reject_payload = ms.get("reject", {})
        snapshot_payload = ms.get("snapshot", {})
        logger.debug(
            "[STRICT_WHALE][min_signal][reject] symbol=%s reason=%s details=%s snap=%s",
            symbol,
            reject_payload.get("reason"),
            reject_payload.get("details", {}),
            {
                k: snapshot_payload.get(k)
                for k in (
                    "ts",
                    "presence",
                    "bias",
                    "dvr",
                    "vwap_dev",
                    "band_pct",
                    "near_edge",
                )
            },
        )
        return False

    side = ms.get("side")
    if not side:
        return False

    snapshot = ms.get("snapshot", {})
    risk_block = {
        "sl_atr": ms.get("sl_atr"),
        "tp1_atr": ms.get("tp1_atr"),
        "tp2_atr": ms.get("tp2_atr"),
        "time_exit_s": ms.get("time_exit_s"),
    }
    hint = {
        "kind": "min_signal",
        "side": side,
        "risk": risk_block,
        "payload": {"snapshot": dict(snapshot)},
        "source": "whale_canary",
        "reasons": ms.get("reasons", []),
    }

    inc_candidate(symbol, side)
    set_state(symbol, snapshot)
    manager.attach_hint(symbol, hint)
    manager.set_last_min_signal_accept(
        symbol,
        {
            "ts": snapshot.get("ts"),
            "side": side,
            "snapshot": dict(snapshot),
            "risk": dict(risk_block),
        },
    )

    if now_ts - _min_signal_last_log.get(symbol, 0.0) >= _MIN_SIGNAL_LOG_GAP:
        _min_signal_last_log[symbol] = now_ts
        logger.info(
            "[SCEN_EXPLAIN][min_signal] symbol=%s side=%s reasons=%s snapshot=%s",
            symbol,
            side,
            hint["reasons"],
            {
                k: snapshot.get(k)
                for k in (
                    "presence",
                    "bias",
                    "dvr",
                    "vwap_dev",
                    "band_pct",
                    "near_edge",
                    "retest_ok",
                )
            },
        )

    return True
