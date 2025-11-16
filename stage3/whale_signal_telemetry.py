"""Допоміжні утиліти телеметрії для whale min-signal v1."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from stage3.whale_min_signal_view import whale_min_signal_view
from stage3.whale_signal_v1 import compute_whale_min_signal
from utils.utils import safe_float


def _bool(value: Any) -> bool:
    return bool(value)


def _float(value: Any, default: float = 0.0) -> float:
    resolved = safe_float(value, default)
    if resolved is None:
        return default
    return float(resolved)


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def build_whale_signal_v1_payload(
    stats: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Формує payload для market_context.meta та телеметрії."""

    whale_view = whale_min_signal_view(stats)
    whale_signal = compute_whale_min_signal(whale_view)

    dominance = whale_view.get("dominance")
    if isinstance(dominance, Mapping):
        dom_payload = {
            "buy": _bool(dominance.get("buy")),
            "sell": _bool(dominance.get("sell")),
        }
    else:
        dom_payload = {"buy": False, "sell": False}

    zones = whale_view.get("zones_summary")
    if isinstance(zones, Mapping):
        zones_payload = {
            "accum_cnt": _int(zones.get("accum_cnt")),
            "dist_cnt": _int(zones.get("dist_cnt")),
        }
    else:
        zones_payload = {"accum_cnt": 0, "dist_cnt": 0}

    reasons_raw = whale_signal.get("reasons")
    if isinstance(reasons_raw, list):
        reasons = [str(reason) for reason in reasons_raw]
    else:
        reasons = []

    payload = {
        "enabled": _bool(whale_signal.get("enabled")),
        "direction": str(whale_signal.get("direction") or "unknown"),
        "profile": str(whale_signal.get("profile") or "none"),
        "confidence": _float(whale_signal.get("confidence"), 0.0),
        "phase_reason": (str(whale_signal.get("phase_reason") or "").strip() or None),
        "reasons": reasons,
        "presence": _float(whale_view.get("presence"), 0.0),
        "bias": _float(whale_view.get("bias"), 0.0),
        "vwap_dev": _float(whale_view.get("vwap_dev"), 0.0),
        "vol_regime": str(whale_view.get("vol_regime") or "unknown"),
        "age_s": _float(whale_view.get("age_s"), 0.0),
        "missing": _bool(whale_view.get("missing", False)),
        "stale": _bool(whale_view.get("stale", False)),
        "dominance": dom_payload,
        "zones_summary": zones_payload,
    }
    return payload


__all__ = ["build_whale_signal_v1_payload"]
