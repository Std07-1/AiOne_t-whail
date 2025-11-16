"""Prometheus-метрики для whale min-signal v1."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

try:  # прометей може бути відсутнім
    from prometheus_client import Counter, Gauge
except Exception:  # pragma: no cover
    Counter = Gauge = None  # type: ignore

logger = logging.getLogger("metrics.whale_signal_v1")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

if Counter is not None and Gauge is not None:  # pragma: no cover
    _CONFIDENCE = Gauge(
        "ai_one_whale_signal_v1_confidence",
        "Confidence whale min-signal v1",
        ["symbol"],
    )
    _ENABLED = Gauge(
        "ai_one_whale_signal_v1_enabled",
        "Enabled flag whale min-signal v1",
        ["symbol"],
    )
    _PROFILE = Counter(
        "ai_one_whale_signal_v1_profile_total",
        "Profile usages for whale min-signal v1",
        ["symbol", "profile"],
    )
    _DIRECTION = Counter(
        "ai_one_whale_signal_v1_direction_total",
        "Direction tally for whale min-signal v1",
        ["symbol", "direction"],
    )
    _DISABLED = Counter(
        "ai_one_whale_signal_v1_disabled_total",
        "Disabled reasons for whale min-signal v1",
        ["symbol", "reason"],
    )
else:  # pragma: no cover
    _CONFIDENCE = _ENABLED = None
    _PROFILE = _DIRECTION = _DISABLED = None


def _label(symbol: str) -> str:
    txt = str(symbol or "?").strip().upper()
    return txt or "?"


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _reason_label(reason: Any) -> str:
    if not reason:
        return "unknown"
    txt = str(reason).strip().lower().replace(" ", "_")
    return txt or "unknown"


def record_signal(symbol: str, payload: Mapping[str, Any]) -> None:
    """Оновлює гейджі/лічильники для whale min-signal v1."""

    if _CONFIDENCE is None or _ENABLED is None:
        return
    lbl = _label(symbol)
    try:
        _CONFIDENCE.labels(symbol=lbl).set(_safe_float(payload.get("confidence")))
    except Exception:
        logger.debug("[MIN_SIGNAL_V1_METRICS] confidence set failed", exc_info=True)
    try:
        _ENABLED.labels(symbol=lbl).set(1.0 if payload.get("enabled") else 0.0)
    except Exception:
        logger.debug("[MIN_SIGNAL_V1_METRICS] enabled set failed", exc_info=True)

    profile = str(payload.get("profile") or "none")
    if _PROFILE is not None:
        try:
            _PROFILE.labels(symbol=lbl, profile=profile).inc()
        except Exception:
            logger.debug("[MIN_SIGNAL_V1_METRICS] profile inc failed", exc_info=True)

    direction = str(payload.get("direction") or "unknown")
    if _DIRECTION is not None:
        try:
            _DIRECTION.labels(symbol=lbl, direction=direction).inc()
        except Exception:
            logger.debug("[MIN_SIGNAL_V1_METRICS] direction inc failed", exc_info=True)

    if _DISABLED is None or payload.get("enabled"):
        return
    reasons = payload.get("reasons")
    if not isinstance(reasons, list):
        return
    for reason in reasons:
        try:
            _DISABLED.labels(symbol=lbl, reason=_reason_label(reason)).inc()
        except Exception:
            logger.debug("[MIN_SIGNAL_V1_METRICS] disabled inc failed", exc_info=True)


__all__ = ["record_signal"]
