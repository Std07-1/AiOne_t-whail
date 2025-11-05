"""Профільні пороги для whale-hints (Stage1 телеметрія).

Контракти Stage1/Stage2/Stage3 НЕ змінюємо — профілі лише впливають на обчислення
stage2_hint і market_context.meta.profile (spying-only) під фіче-флагом.
"""

from __future__ import annotations

from typing import Any, TypedDict


class ProfileThresholds(TypedDict, total=False):
    """Пороги для профілю ринку (телеметрія‑only, Stage1).

    Поля:
        - presence_min: мінімальна присутність whale (0..1)
        - bias_abs_min: мінімальна |bias| (0..1)
        - vwap_dev_min: мінімальне |відхилення від VWAP|
        - alt_confirm_min: мінімальна кількість альтернативних підтверджень (цілісний >=0)
        - require_dominance: вимагати dominance.* у відповідному напрямку
        - hysteresis_s: мінімальна пауза між переключеннями профілю (антифлікер)
        - atr_scale: множники для порогів за vol_regime (low/mid/high/…)
        - k_range: рекомендований діапазон K для forward‑оцінки (довідково у meta)
        - cooldown_s: кулдаун на повторну емісію hint після UP/DOWN
    """

    presence_min: float
    bias_abs_min: float
    vwap_dev_min: float
    alt_confirm_min: int
    require_dominance: bool
    hysteresis_s: int
    atr_scale: dict[str, float]
    k_range: tuple[int, int]
    cooldown_s: int


# Базові пресети для маркет-класів
WHALE_PROFILES: dict[str, dict[str, ProfileThresholds]] = {
    "BTC": {
        "chop_pre_breakout_up": {
            "presence_min": 0.65,
            "bias_abs_min": 0.25,
            "vwap_dev_min": 0.008,
            "alt_confirm_min": 2,
            "require_dominance": False,
            "hysteresis_s": 45,
            "atr_scale": {"low": 1.0, "mid": 1.0, "high": 1.0, "hyper": 1.05},
            "k_range": (3, 10),
            "cooldown_s": 30,
            # band‑фічі (Stage B)
            "band_k": 0.5,
            "edge_hits_min": 3,
            "band_squeeze_min": 0.35,
            "max_pullback_in_band_ratio_max": 0.7,
        },
        "probe_up": {
            "presence_min": 0.70,
            "bias_abs_min": 0.52,
            "vwap_dev_min": 0.009,
            "alt_confirm_min": 1,
            "require_dominance": True,
            "hysteresis_s": 30,
            "atr_scale": {"low": 0.9, "mid": 1.0, "high": 1.1, "hyper": 1.2},
            "k_range": (5, 10),
            "cooldown_s": 40,
        },
        "grab_upper": {
            "presence_min": 0.60,
            "bias_abs_min": 0.00,
            "vwap_dev_min": 0.000,
            "alt_confirm_min": 1,
            "require_dominance": False,
            "hysteresis_s": 20,
            "atr_scale": {"low": 1.0, "mid": 1.0, "high": 1.05, "hyper": 1.10},
            "k_range": (3, 5),
            "cooldown_s": 20,
        },
        "pullback_up": {
            "presence_min": 0.68,
            "bias_abs_min": 0.00,
            "vwap_dev_min": 0.005,
            "alt_confirm_min": 0,
            "require_dominance": False,
            "hysteresis_s": 45,
            "atr_scale": {"low": 0.95, "mid": 1.0, "high": 1.05, "hyper": 1.10},
            "k_range": (10, 20),
            "cooldown_s": 90,
        },
        "range_fade": {
            "presence_min": 0.62,
            "bias_abs_min": 0.00,
            "vwap_dev_min": 0.010,
            "alt_confirm_min": 0,
            "require_dominance": False,
            "hysteresis_s": 30,
            "atr_scale": {"low": 1.0, "mid": 1.0, "high": 1.0, "hyper": 1.0},
            "k_range": (3, 10),
            "cooldown_s": 60,
        },
        "down_trend": {
            "presence_min": 0.70,
            "bias_abs_min": 0.55,
            "vwap_dev_min": 0.010,
            "alt_confirm_min": 1,
            "require_dominance": True,
            "hysteresis_s": 30,
            "atr_scale": {"low": 0.9, "mid": 1.0, "high": 1.1, "hyper": 1.2},
            "k_range": (5, 10),
            "cooldown_s": 40,
        },
        "grab_lower": {
            "presence_min": 0.60,
            "bias_abs_min": 0.00,
            "vwap_dev_min": 0.000,
            "alt_confirm_min": 1,
            "require_dominance": False,
            "hysteresis_s": 20,
            "atr_scale": {"low": 1.0, "mid": 1.0, "high": 1.05, "hyper": 1.10},
            "k_range": (3, 5),
            "cooldown_s": 20,
        },
    },
    "ETH": {
        "chop_pre_breakout_up": {
            "presence_min": 0.65,
            "bias_abs_min": 0.25,
            "vwap_dev_min": 0.008,
            "alt_confirm_min": 2,
            "require_dominance": False,
            "hysteresis_s": 45,
            "atr_scale": {"low": 1.0, "mid": 1.0, "high": 1.0, "hyper": 1.05},
            "k_range": (3, 10),
            "cooldown_s": 30,
            "band_k": 0.5,
            "edge_hits_min": 3,
            "band_squeeze_min": 0.35,
            "max_pullback_in_band_ratio_max": 0.7,
        },
        "probe_up": {
            "presence_min": 0.72,
            "bias_abs_min": 0.58,
            "vwap_dev_min": 0.010,
            "alt_confirm_min": 1,
            "require_dominance": True,
            "hysteresis_s": 30,
            "atr_scale": {"low": 0.9, "mid": 1.0, "high": 1.1, "hyper": 1.2},
            "k_range": (5, 10),
            "cooldown_s": 45,
        },
        "range_fade": {
            "presence_min": 0.64,
            "bias_abs_min": 0.00,
            "vwap_dev_min": 0.012,
            "alt_confirm_min": 0,
            "require_dominance": False,
            "hysteresis_s": 30,
            "atr_scale": {"low": 1.0, "mid": 1.0, "high": 1.0, "hyper": 1.0},
            "k_range": (3, 10),
            "cooldown_s": 60,
        },
    },
    "ALTS": {
        "chop_pre_breakout_up": {
            "presence_min": 0.65,
            "bias_abs_min": 0.25,
            "vwap_dev_min": 0.008,
            "alt_confirm_min": 3,
            "require_dominance": False,
            "hysteresis_s": 60,
            "atr_scale": {"low": 1.0, "mid": 1.0, "high": 1.0, "hyper": 1.05},
            "k_range": (3, 10),
            "cooldown_s": 30,
            "band_k": 0.6,
            "edge_hits_min": 4,
            "band_squeeze_min": 0.35,
            "max_pullback_in_band_ratio_max": 0.7,
        },
        "probe_up": {
            "presence_min": 0.78,
            "bias_abs_min": 0.60,
            "vwap_dev_min": 0.012,
            "alt_confirm_min": 2,
            "require_dominance": True,
            "hysteresis_s": 60,
            "atr_scale": {"low": 0.95, "mid": 1.0, "high": 1.05, "hyper": 1.10},
            "k_range": (3, 10),
            "cooldown_s": 60,
        },
        "range_fade": {
            "presence_min": 0.66,
            "bias_abs_min": 0.00,
            "vwap_dev_min": 0.012,
            "alt_confirm_min": 2,
            "require_dominance": False,
            "hysteresis_s": 45,
            "atr_scale": {"low": 1.0, "mid": 1.0, "high": 1.0, "hyper": 1.0},
            "k_range": (3, 10),
            "cooldown_s": 90,
        },
    },
}


def market_class_for_symbol(symbol: str) -> str:
    s = (symbol or "").upper()
    if s.startswith("BTC"):
        return "BTC"
    if s.startswith("ETH"):
        return "ETH"
    return "ALTS"


def get_profile_thresholds(market_class: str, profile: str) -> ProfileThresholds:
    return (WHALE_PROFILES.get(market_class) or {}).get(profile, {})


__all__ = [
    "ProfileThresholds",
    "WHALE_PROFILES",
    "market_class_for_symbol",
    "get_profile_thresholds",
]

# ── Пороги для false_breakout (Stage A) ─────────────────────────────────
# Значення: BTC/ETH — max_overrun до 0.0030 (0.30%), ALTS — до 0.0050 (0.50%).
FALSE_BREAKOUT: dict[str, dict[str, float | int]] = {
    "BTC": {
        "min_overrun": 0.0005,
        "max_overrun": 0.0030,
        "min_wick_ratio": 0.6,
        "min_vol_z": 1.5,
        "max_reject_bars": 3,
    },
    "ETH": {
        "min_overrun": 0.0005,
        "max_overrun": 0.0030,
        "min_wick_ratio": 0.6,
        "min_vol_z": 1.5,
        "max_reject_bars": 3,
    },
    "ALTS": {
        "min_overrun": 0.0005,
        "max_overrun": 0.0050,
        "min_wick_ratio": 0.6,
        "min_vol_z": 1.5,
        "max_reject_bars": 3,
    },
}


def get_false_breakout_cfg(market_class: str) -> dict[str, float | int]:
    mc = (market_class or "ALTS").upper()
    return dict(FALSE_BREAKOUT.get(mc) or FALSE_BREAKOUT["ALTS"])  # shallow copy


__all__.extend(["FALSE_BREAKOUT", "get_false_breakout_cfg"])
