"""Prometheus-метрики стану китових снепшотів для Stage2."""

from __future__ import annotations

import logging

from rich.console import Console
from rich.logging import RichHandler

try:  # прометей може бути відсутнім у мінімальних середовищах
    from prometheus_client import Counter, Gauge
except Exception:  # pragma: no cover - прометей необов'язковий
    Counter = Gauge = None  # type: ignore

_logger = logging.getLogger("metrics.whale_states")
if not _logger.handlers:
    _logger.setLevel(logging.INFO)
    _logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    _logger.propagate = False

if Counter is not None and Gauge is not None:  # pragma: no cover
    _REDIS_MISS = Counter(
        "ai_one_whale_redis_miss_total",
        "Кількість пропусків whale-пейлоадів у Redis",
        ["symbol"],
    )
    _MISSING_STATE = Gauge(
        "ai_one_whale_missing_state",
        "Поточний стан missing для китового снепшота",
        ["symbol"],
    )
    _STALE_STATE = Gauge(
        "ai_one_whale_stale_state",
        "Поточний стан stale для китового снепшота",
        ["symbol"],
    )
else:  # pragma: no cover
    _REDIS_MISS = _MISSING_STATE = _STALE_STATE = None


def _label(symbol: str) -> str:
    txt = str(symbol or "?").strip().upper()
    return txt or "?"


def inc_redis_miss(symbol: str) -> None:
    """Інкрементує лічильник відсутності пейлоада в Redis."""

    if _REDIS_MISS is None:
        return
    try:
        _REDIS_MISS.labels(symbol=_label(symbol)).inc()
    except Exception:
        _logger.debug("[WHALE_STATES] redis_miss inc failed", exc_info=True)


def set_missing(symbol: str, value: bool) -> None:
    """Оновлює гейдж missing."""

    if _MISSING_STATE is None:
        return
    try:
        _MISSING_STATE.labels(symbol=_label(symbol)).set(1.0 if value else 0.0)
    except Exception:
        _logger.debug("[WHALE_STATES] missing gauge set failed", exc_info=True)


def set_stale(symbol: str, value: bool) -> None:
    """Оновлює гейдж stale."""

    if _STALE_STATE is None:
        return
    try:
        _STALE_STATE.labels(symbol=_label(symbol)).set(1.0 if value else 0.0)
    except Exception:
        _logger.debug("[WHALE_STATES] stale gauge set failed", exc_info=True)


__all__ = ["inc_redis_miss", "set_missing", "set_stale"]
