"""Stage3 мінімальний зріз китової телеметрії.

Посилається на контракт `whale.core.WhaleTelemetry`: повертає лише поля,
які Stage3 мін-сигнал очікує бачити, із безпечними дефолтами.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

_DEFAULT_ZONES = {"accum_cnt": 0, "dist_cnt": 0}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if not (num == num):  # NaN guard
        return default
    return num


def _extract_phase_reason(stats: Mapping[str, Any] | None) -> str | None:
    if not isinstance(stats, Mapping):
        return None
    reason_candidate: Any | None = None
    phase_debug = stats.get("phase_debug")
    if isinstance(phase_debug, Mapping):
        reason_candidate = phase_debug.get("reason")
    if reason_candidate is None:
        phase_state = stats.get("phase_state")
        if isinstance(phase_state, Mapping):
            reason_candidate = phase_state.get("last_reason")
    if reason_candidate is None:
        phase_payload = stats.get("phase")
        if isinstance(phase_payload, Mapping):
            hint = phase_payload.get("phase_state_hint")
            if isinstance(hint, Mapping):
                reason_candidate = hint.get("reason")
    if isinstance(reason_candidate, str):
        trimmed = reason_candidate.strip()
        return trimmed or None
    return None


def whale_min_signal_view(stats: Mapping[str, Any] | None) -> dict[str, Any]:
    """Повертає нормалізований view `stats.whale` для Stage3 мін-сигналу."""

    whale = stats.get("whale") if isinstance(stats, Mapping) else None
    whale_map = whale if isinstance(whale, Mapping) else {}

    presence = _as_float(whale_map.get("presence"), 0.0)
    bias = _as_float(whale_map.get("bias"), 0.0)
    vwap_dev = _as_float(whale_map.get("vwap_dev"), 0.0)

    dominance_raw = whale_map.get("dominance")
    if isinstance(dominance_raw, Mapping):
        dominance = {
            "buy": bool(dominance_raw.get("buy")),
            "sell": bool(dominance_raw.get("sell")),
        }
    else:
        dominance = {"buy": False, "sell": False}

    zones_raw = whale_map.get("zones_summary")
    if isinstance(zones_raw, Mapping):
        zones_summary = {
            "accum_cnt": int(zones_raw.get("accum_cnt", 0) or 0),
            "dist_cnt": int(zones_raw.get("dist_cnt", 0) or 0),
        }
    else:
        zones_summary = dict(_DEFAULT_ZONES)

    vol_regime = str(whale_map.get("vol_regime", "unknown") or "unknown")

    missing = bool(whale_map.get("missing", False))
    stale = bool(whale_map.get("stale", False))
    age_s = int(whale_map.get("age_s", 0) or 0)

    reasons_raw = whale_map.get("reasons")
    reasons = (
        [str(reason) for reason in reasons_raw] if isinstance(reasons_raw, list) else []
    )

    tags_candidate = stats.get("tags") if isinstance(stats, Mapping) else None
    if isinstance(tags_candidate, list):
        tags = [str(tag) for tag in tags_candidate]
    else:
        tags = []

    phase_reason = _extract_phase_reason(stats if isinstance(stats, Mapping) else None)

    return {
        "presence": presence,
        "bias": bias,
        "vwap_dev": vwap_dev,
        "dominance": dominance,
        "vol_regime": vol_regime,
        "zones_summary": zones_summary,
        "missing": missing,
        "stale": stale,
        "age_s": age_s,
        "tags": tags,
        "reasons": reasons,
        "phase_reason": phase_reason,
    }
