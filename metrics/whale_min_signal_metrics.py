"""Prometheus-метрики для мінімального китового сигналу."""

from __future__ import annotations

import logging
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

try:  # прометей може бути відсутнім у мінімальних середовищах
    from prometheus_client import Counter, Gauge
except Exception:  # pragma: no cover
    Counter = Gauge = None  # type: ignore

logger = logging.getLogger("metrics.whale_min_signal")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

# ── Реєстрація лічильників/гейджів ──────────────────────────────────────────
if Counter is not None and Gauge is not None:  # pragma: no cover - обхід без прометея
    _CANDIDATES = Counter(
        "ai_one_min_signal_candidates_total",
        "Кандидати мінімального whale-сигналу",
        ["symbol", "side"],
    )
    _OPEN = Counter(
        "ai_one_min_signal_open_total",
        "Відкриті позиції мінімального whale-сигналу",
        ["symbol", "side"],
    )
    _EXIT = Counter(
        "ai_one_min_signal_exit_total",
        "Виходи мінімального whale-сигналу",
        ["symbol", "side", "reason"],
    )
    _STATE_PRESENCE = Gauge(
        "ai_one_min_signal_state_presence",
        "Presence для мінімального whale-сигналу",
        ["symbol"],
    )
    _STATE_BIAS = Gauge(
        "ai_one_min_signal_state_bias",
        "Bias для мінімального whale-сигналу",
        ["symbol"],
    )
    _STATE_DVR = Gauge(
        "ai_one_min_signal_state_dvr",
        "DVR для мінімального whale-сигналу",
        ["symbol"],
    )
    _STATE_VWAP_DEV = Gauge(
        "ai_one_min_signal_state_vwap_dev",
        "VWAP deviation для мінімального whale-сигналу",
        ["symbol"],
    )
    _STATE_BAND = Gauge(
        "ai_one_min_signal_state_band_pct",
        "Band pct для мінімального whale-сигналу",
        ["symbol"],
    )
else:  # pragma: no cover
    _CANDIDATES = _OPEN = _EXIT = None
    _STATE_PRESENCE = _STATE_BIAS = _STATE_DVR = _STATE_VWAP_DEV = _STATE_BAND = None


# ── Допоміжні утиліти ───────────────────────────────────────────────────────
def _symbol_label(symbol: str) -> str:
    return str(symbol or "?").upper()


def _side_label(side: str) -> str:
    txt = str(side or "").strip().lower()
    return txt or "na"


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


# ── Публічні API ────────────────────────────────────────────────────────────
def inc_candidate(symbol: str, side: str) -> None:
    """Інкрементує лічильник кандидатів."""

    if _CANDIDATES is None:
        return
    try:
        _CANDIDATES.labels(
            symbol=_symbol_label(symbol),
            side=_side_label(side),
        ).inc()
    except Exception:
        logger.debug("[MIN_SIGNAL_METRICS] candidate inc failed", exc_info=True)


def inc_open(symbol: str, side: str) -> None:
    """Інкрементує лічильник відкриттів."""

    if _OPEN is None:
        return
    try:
        _OPEN.labels(
            symbol=_symbol_label(symbol),
            side=_side_label(side),
        ).inc()
    except Exception:
        logger.debug("[MIN_SIGNAL_METRICS] open inc failed", exc_info=True)


def inc_exit(symbol: str, side: str, reason: str) -> None:
    """Інкрементує лічильник виходів із причиною."""

    if _EXIT is None:
        return
    try:
        _EXIT.labels(
            symbol=_symbol_label(symbol),
            side=_side_label(side),
            reason=str(reason or "unknown"),
        ).inc()
    except Exception:
        logger.debug("[MIN_SIGNAL_METRICS] exit inc failed", exc_info=True)


def set_state(symbol: str, snapshot: dict[str, Any]) -> None:
    """Оновлює гейджі стану (presence, bias, dvr, vwap_dev, band_pct)."""

    if _STATE_PRESENCE is None:
        return
    try:
        lbl = _symbol_label(symbol)
        _STATE_PRESENCE.labels(symbol=lbl).set(_safe_float(snapshot.get("presence")))
        _STATE_BIAS.labels(symbol=lbl).set(_safe_float(snapshot.get("bias")))
        _STATE_DVR.labels(symbol=lbl).set(_safe_float(snapshot.get("dvr")))
        _STATE_VWAP_DEV.labels(symbol=lbl).set(_safe_float(snapshot.get("vwap_dev")))
        _STATE_BAND.labels(symbol=lbl).set(_safe_float(snapshot.get("band_pct")))
    except Exception:
        logger.debug("[MIN_SIGNAL_METRICS] state set failed", exc_info=True)


__all__ = ["inc_candidate", "inc_open", "inc_exit", "set_state"]
