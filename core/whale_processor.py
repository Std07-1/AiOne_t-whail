"""Мінімальна логіка китового сигналу для Stage2/Stage3 канарейки.

Модуль тримає чисту обробку статистик Stage1/whale й повертає компактний
план дій для Stage2Hint. Публічний API зведено до функції `minimal_signal`.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Final

from rich.console import Console
from rich.logging import RichHandler

from config.config_whale import STAGE2_WHALE_TELEMETRY
from config.constants import K_DIRECTIONAL_VOLUME_RATIO

logger = logging.getLogger("core.whale_processor")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

# ── Константи мінімального сигналу ───────────────────────────────────────────
_MIN_SIGNAL_PRESENCE: Final[float] = float(
    STAGE2_WHALE_TELEMETRY.get("MIN_SIGNAL_PRESENCE", 0.0) or 0.0
)
_MIN_SIGNAL_BIAS: Final[float] = float(
    STAGE2_WHALE_TELEMETRY.get("MIN_SIGNAL_BIAS", 0.0) or 0.0
)
_MIN_SIGNAL_DVR: Final[float] = float(
    STAGE2_WHALE_TELEMETRY.get("MIN_SIGNAL_DVR", 0.0) or 0.0
)
_MIN_SIGNAL_VWAP_DEV_ATR: Final[float] = float(
    STAGE2_WHALE_TELEMETRY.get("MIN_SIGNAL_VWAP_DEV_ATR", 0.0) or 0.0
)
_MAX_BAND_PCT: Final[float] = float(
    STAGE2_WHALE_TELEMETRY.get("MAX_BAND_PCT", 1.0) or 1.0
)
_RETEST_TTL_S: Final[float] = float(
    STAGE2_WHALE_TELEMETRY.get("RETEST_TTL_S", 600) or 600
)
_TIME_EXIT_S: Final[int] = int(STAGE2_WHALE_TELEMETRY.get("TIME_EXIT_S", 1800) or 1800)
_SL_ATR: Final[float] = float(STAGE2_WHALE_TELEMETRY.get("SL_ATR", 1.5) or 1.5)
_TP1_ATR: Final[float] = float(STAGE2_WHALE_TELEMETRY.get("TP1_ATR", 1.5) or 1.5)
_TP2_ATR: Final[float] = float(STAGE2_WHALE_TELEMETRY.get("TP2_ATR", 3.0) or 3.0)


# ── Допоміжні перетворення ──────────────────────────────────────────────────
def _as_float(value: Any, default: float = 0.0) -> float:
    """Безпечно конвертує значення у float, повертаючи default при хибному вводі."""

    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if math.isfinite(out):
        return out
    return default


def _resolve_ts_ms(raw: Any) -> int | None:
    """Конвертує різні форми timestamp у мілісекунди (або None при невдачі)."""

    if isinstance(raw, (int, float)) and math.isfinite(raw):
        val = int(raw)
        # Якщо схоже на секунди (<1e12) — конвертуємо у мс.
        if abs(val) < 1_000_000_000_000:
            val *= 1000
        return val
    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return None
        if txt.isdigit():
            return _resolve_ts_ms(int(txt))
        try:
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        except ValueError:
            return None
        return int(dt.timestamp() * 1000)
    return None


def _now_ts_ms(ctx: dict[str, Any], stats: dict[str, Any]) -> int:
    """Витягує timestamp «зараз» із контексту або повертає поточний час."""

    for container in (stats, ctx):
        for key in ("now_ts_ms", "ts_ms", "timestamp_ms", "ts", "timestamp"):
            ts = (
                _resolve_ts_ms(container.get(key))
                if isinstance(container, dict)
                else None
            )
            if ts is not None:
                return ts
    return int(time.time() * 1000)


def _normalize_direction(value: Any) -> float | None:
    """Зводить напрямок до {-1, +1}; повертає None при невизначеності."""

    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        val = float(value)
        if val > 0:
            return 1.0
        if val < 0:
            return -1.0
        return 0.0
    if isinstance(value, str):
        txt = value.strip().lower()
        if txt in {"long", "buy", "up", "upper"}:
            return 1.0
        if txt in {"short", "sell", "down", "lower"}:
            return -1.0
    return None


def _extract_stat_bool(stats: dict[str, Any], key: str) -> bool:
    """Дістає булеве значення з stats, терпляче обробляючи рядки/числа."""

    val = stats.get(key)
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        txt = val.strip().lower()
        if txt in {"1", "true", "yes", "on"}:
            return True
        if txt in {"0", "false", "no", "off"}:
            return False
    return False


def _log_reject(symbol: str, reason: str, snapshot: dict[str, Any]) -> None:
    """Легке debug-логування причини відсіву."""

    if not logger.isEnabledFor(logging.DEBUG):
        return
    try:
        logger.debug(
            "[MIN_SIGNAL] %s reject=%s presence=%.2f bias=%.2f dvr=%.2f vwap_dev=%.4f",
            str(symbol).upper(),
            reason,
            float(snapshot.get("presence", 0.0) or 0.0),
            float(snapshot.get("bias", 0.0) or 0.0),
            float(snapshot.get("dvr", 0.0) or 0.0),
            float(snapshot.get("vwap_dev", 0.0) or 0.0),
        )
    except Exception:
        pass


def _is_recent_retest(stats: dict[str, Any], ctx: dict[str, Any]) -> bool:
    """Перевіряє, чи retest_ok + sweep_then_breakout вкладаються у TTL."""

    if not (
        _extract_stat_bool(stats, "retest_ok")
        and _extract_stat_bool(stats, "sweep_then_breakout")
    ):
        return False

    # Якщо вже є готовий вік (секунди), використовуємо його напряму.
    for key in ("retest_ok_age_s", "retest_age_s", "sweep_then_breakout_age_s"):
        age_val = stats.get(key)
        if age_val is not None:
            age_s = _as_float(age_val, default=-1.0)
            if age_s >= 0:
                return age_s <= _RETEST_TTL_S

    # В іншому разі пробуємо timestamp події.
    event_ts = None
    for key in (
        "retest_ok_ts_ms",
        "retest_ok_ts",
        "retest_ok_detected_ts_ms",
        "retest_ok_detected_ts",
        "sweep_then_breakout_ts_ms",
        "sweep_then_breakout_ts",
    ):
        event_ts = _resolve_ts_ms(stats.get(key))
        if event_ts is not None:
            break

    if event_ts is None:
        return True  # Ліпше вважати свіжим, ніж втратити можливість входу.

    now_ts = _now_ts_ms(ctx, stats)
    return (now_ts - event_ts) <= int(_RETEST_TTL_S * 1000)


def minimal_signal(symbol: str, ctx: dict[str, Any]) -> dict[str, Any] | None:
    """Мінімальний вхід на базі whale-метрик."""

    if not isinstance(ctx, dict):  # оборона від зіпсованого контексту
        return None
    stats = ctx.get("stats")
    if not isinstance(stats, dict):
        return None

    whale_block = stats.get("whale") if isinstance(stats.get("whale"), dict) else {}
    presence = _as_float(stats.get("presence", whale_block.get("presence")))
    bias = _as_float(stats.get("bias", whale_block.get("bias")))
    dvr = _as_float(stats.get("dvr", stats.get(K_DIRECTIONAL_VOLUME_RATIO)))
    vwap_dev = _as_float(stats.get("vwap_dev", whale_block.get("vwap_dev")))
    atr = _as_float(stats.get("atr"))
    band_pct = _as_float(stats.get("band_pct"), default=1.0)
    near_edge_raw = stats.get("near_edge")
    stale_flag = bool(
        _extract_stat_bool(stats, "stale")
        or (isinstance(whale_block, dict) and bool(whale_block.get("stale")))
    )

    now_ts_seconds = int(round(_now_ts_ms(ctx, stats) / 1000))
    snapshot = {
        "presence": presence,
        "bias": bias,
        "direction": stats.get("direction"),
        "dvr": dvr,
        "vwap_dev": vwap_dev,
        "atr": atr,
        "band_pct": band_pct,
        "near_edge": near_edge_raw,
        "retest_ok": _extract_stat_bool(stats, "retest_ok"),
        "sweep_then_breakout": _extract_stat_bool(stats, "sweep_then_breakout"),
        "ts": now_ts_seconds,
    }

    def _reject(
        reason: str,
        gate: str | None = None,
        value: Any | None = None,
        threshold: Any | None = None,
    ) -> dict[str, Any]:
        details: dict[str, Any] = {}
        if gate:
            details["failed_gate"] = gate
        if value is not None:
            details["value"] = value
        if threshold is not None:
            details["threshold"] = threshold
        payload: dict[str, Any] = {"reason": reason}
        if details:
            payload["details"] = details
        snapshot_copy = dict(snapshot)
        _log_reject(symbol, reason, snapshot_copy)
        return {"reject": payload, "snapshot": snapshot_copy}

    if stale_flag:
        return _reject("stale", gate="stale_flag", value=True, threshold=False)

    direction_val = _normalize_direction(stats.get("direction"))
    if direction_val is None or direction_val == 0.0:
        # сприяємо лише визначеним напрямкам
        return _reject(
            "direction_unknown", gate="direction", value=stats.get("direction")
        )

    aligned_bias = bias * direction_val
    if presence < _MIN_SIGNAL_PRESENCE:
        return _reject(
            "presence_below_min",
            gate="presence",
            value=presence,
            threshold=_MIN_SIGNAL_PRESENCE,
        )
    if aligned_bias < _MIN_SIGNAL_BIAS:
        return _reject(
            "bias_direction_mismatch",
            gate="bias",
            value=aligned_bias,
            threshold=_MIN_SIGNAL_BIAS,
        )
    if dvr < _MIN_SIGNAL_DVR:
        return _reject(
            "dvr_below_min",
            gate="dvr",
            value=dvr,
            threshold=_MIN_SIGNAL_DVR,
        )
    if atr <= 0.0:
        return _reject("atr_missing", gate="atr", value=atr, threshold=0.0)
    if abs(vwap_dev) < (_MIN_SIGNAL_VWAP_DEV_ATR * atr):
        return _reject(
            "vwap_dev_insufficient",
            gate="vwap_dev",
            value=abs(vwap_dev),
            threshold=_MIN_SIGNAL_VWAP_DEV_ATR * atr,
        )

    near_edge_ok = bool(near_edge_raw in (True, "upper", "lower"))
    band_ok = band_pct <= _MAX_BAND_PCT
    if not (near_edge_ok or band_ok):
        return _reject(
            "edge_guard_failed",
            gate="edge",
            value={"near_edge": near_edge_raw, "band_pct": band_pct},
            threshold={"band_pct_max": _MAX_BAND_PCT},
        )

    side = "long" if direction_val > 0 else "short"
    accept_reasons = [
        "presence_ok",
        "bias_direction_ok",
        "dvr_ok",
        "vwap_dev_ok",
        "edge_guard_ok",
    ]

    if near_edge_ok:
        snapshot["near_edge"] = near_edge_raw if near_edge_raw is not None else True
    else:
        snapshot["near_edge"] = False

    if _is_recent_retest(stats, ctx):
        accept_reasons.append("retest_priority")

    result = {
        "symbol": str(symbol).upper(),
        "side": side,
        "sl_atr": _SL_ATR,
        "tp1_atr": _TP1_ATR,
        "tp2_atr": _TP2_ATR,
        "time_exit_s": _TIME_EXIT_S,
        "reasons": accept_reasons,
        "snapshot": dict(snapshot),
    }
    return result


__all__ = ["minimal_signal"]
