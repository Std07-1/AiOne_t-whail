"""Stage3 Whale MinSignal v1.

Цей модуль ізольовано обчислює мінімальний китовий сигнал на базі
нормалізованого `whale_min_signal_view`. Логіка під фіче-флагом
`WHALE_MINSIGNAL_V1_ENABLED` і може безпечно вимикатися.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from utils.utils import safe_float

# ── Параметри дизайну (див. docs/whale_contract.md) ─────────────────────────-
# Максимально дозволений вік снепшота whale-пейлоада перед hard-deny.
WHALE_MAX_AGE_SEC = 900.0  # 15 хвилин: достатньо для forward-аналізу, без флікеру.

# Пороги presence/bias для профілів. Значення зафіксовані для подальшого тюнінгу.
PROFILE_THRESHOLDS = (
    ("strong", {"presence": 0.60, "bias": 0.18, "confidence": 0.85}),
    ("soft", {"presence": 0.50, "bias": 0.12, "confidence": 0.65}),
)

# Бонуси/штрафи за волатильність (vol_regime) — м'які коригування впевненості.
VOL_REGIME_ADJUST = {
    "hyper": -0.20,
    "high": -0.08,
    "normal": 0.05,
}

# Перевага зон: акум/дист впливають на довіру до напрямку.
ZONES_SUPPORT_BONUS = 0.05

_DEFAULT_RESULT = {
    "direction": "unknown",
    "enabled": False,
    "reasons": ["no_data"],
    "profile": "none",
    "confidence": 0.0,
    "phase_reason": None,
}


def _float(value: Any, default: float = 0.0) -> float:
    resolved = safe_float(value, default)
    return default if resolved is None else resolved


def _resolve_direction(whale_view: Mapping[str, Any]) -> tuple[str, list[str]]:
    dominance = whale_view.get("dominance")
    reasons: list[str] = []
    if isinstance(dominance, Mapping):
        buy = bool(dominance.get("buy"))
        sell = bool(dominance.get("sell"))
        if buy and not sell:
            return "long", reasons
        if sell and not buy:
            return "short", reasons
        if buy and sell:
            reasons.append("dominance_conflict")
    reasons.append("dominance_missing")
    return "unknown", reasons


def _evaluate_profile(presence: float, bias_abs: float) -> tuple[str, float, list[str]]:
    reasons: list[str] = []
    for name, cfg in PROFILE_THRESHOLDS:
        if presence >= cfg["presence"] and bias_abs >= cfg["bias"]:
            reasons.append(f"profile:{name}")
            return name, cfg["confidence"], reasons
    reasons.append("profile:none")
    return "none", 0.0, reasons


def _apply_vol_regime(confidence: float, regime: str, reasons: list[str]) -> float:
    adjust = VOL_REGIME_ADJUST.get(regime.lower(), 0.0)
    if adjust != 0.0:
        tag = "bonus" if adjust > 0 else "penalty"
        reasons.append(f"vol_regime_{regime}:{tag}")
    return confidence + adjust


def _apply_zones_bias(
    confidence: float,
    direction: str,
    whale_view: Mapping[str, Any],
    reasons: list[str],
) -> float:
    zones = whale_view.get("zones_summary")
    if not isinstance(zones, Mapping):
        return confidence
    accum_cnt = int(zones.get("accum_cnt", 0) or 0)
    dist_cnt = int(zones.get("dist_cnt", 0) or 0)
    if accum_cnt == dist_cnt == 0:
        return confidence
    balance = accum_cnt - dist_cnt
    if direction == "long":
        if balance > 0:
            reasons.append("zones_support_long")
            return confidence + ZONES_SUPPORT_BONUS
        if balance < 0:
            reasons.append("zones_resist_long")
            return confidence - ZONES_SUPPORT_BONUS
    if direction == "short":
        if balance < 0:
            reasons.append("zones_support_short")
            return confidence + ZONES_SUPPORT_BONUS
        if balance > 0:
            reasons.append("zones_resist_short")
            return confidence - ZONES_SUPPORT_BONUS
    return confidence


def _clamp_confidence(value: float) -> float:
    return max(0.0, min(1.0, value))


def compute_whale_min_signal(whale_view: Mapping[str, Any] | None) -> dict[str, Any]:
    """Обчислює Stage3 мін-сигнал на базі нормалізованого whale view."""

    if not isinstance(whale_view, Mapping):
        return dict(_DEFAULT_RESULT)

    reasons: list[str] = []
    missing = bool(whale_view.get("missing"))
    stale = bool(whale_view.get("stale"))
    age_s = _float(whale_view.get("age_s"), 0.0)
    phase_reason_raw = whale_view.get("phase_reason")
    phase_reason = (
        str(phase_reason_raw).strip() if isinstance(phase_reason_raw, str) else None
    )
    phase_reason_norm = phase_reason.lower() if phase_reason else None
    if missing:
        reasons.append("missing_snapshot")
    if stale:
        reasons.append("stale_snapshot")
    if age_s > WHALE_MAX_AGE_SEC:
        reasons.append("age_exceeded")
    if missing or stale or age_s > WHALE_MAX_AGE_SEC:
        return {
            "direction": "unknown",
            "enabled": False,
            "reasons": reasons or ["stale_payload"],
            "profile": "none",
            "confidence": 0.0,
            "phase_reason": phase_reason,
        }

    presence = _float(whale_view.get("presence"), 0.0)
    bias = _float(whale_view.get("bias"), 0.0)
    bias_abs = abs(bias)

    profile, confidence, profile_reasons = _evaluate_profile(presence, bias_abs)
    reasons.extend(profile_reasons)

    direction, direction_reasons = _resolve_direction(whale_view)
    reasons.extend(direction_reasons)

    enabled = profile != "none" and direction in {"long", "short"}
    if not enabled:
        return {
            "direction": direction,
            "enabled": False,
            "reasons": reasons,
            "profile": profile,
            "confidence": 0.0,
            "phase_reason": phase_reason,
        }

    vol_regime = str(whale_view.get("vol_regime", "unknown") or "unknown")
    confidence = _apply_vol_regime(confidence, vol_regime, reasons)
    confidence = _apply_zones_bias(confidence, direction, whale_view, reasons)

    # Підрізаємо, щоб уникнути виходу за межі та надмірного оптимізму.
    confidence = _clamp_confidence(confidence)

    if confidence == 0.0:
        enabled = False
        reasons.append("confidence_zero")

    explain_only = False
    if phase_reason_norm == "volz_too_low":
        explain_only = True
        reasons.append("phase_reason_volz_too_low")
    elif phase_reason_norm == "presence_cap_no_bias_htf" and confidence < 0.5:
        explain_only = True
        reasons.append("phase_reason_presence_cap")

    if explain_only:
        profile = "explain_only"
        enabled = False

    return {
        "direction": direction,
        "enabled": enabled,
        "reasons": reasons,
        "profile": profile,
        "confidence": confidence,
        "phase_reason": phase_reason,
    }
