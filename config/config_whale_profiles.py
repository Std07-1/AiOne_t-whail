"""
Конфігурація профільних порогів для whale-hints телеметрії.
Цей модуль містить налаштування порогових значень для різних ринкових профілів
у системі whale-hints (Stage1 телеметрія). Профілі впливають лише на обчислення
stage2_hint і market_context.meta.profile під фіче-флагом, не змінюючи основні
контракти Stage1/Stage2/Stage3.
Основні компоненти:
    - ProfileThresholds: TypedDict для типізації порогових значень профілю
    - WHALE_PROFILES: Словник конфігурацій для різних ринкових класів (BTC, ETH, ALTS)
    - FALSE_BREAKOUT: Налаштування для виявлення хибних пробоїв (Stage A)
Ринкові профілі:
    - chop_pre_breakout_up: Боковий рух перед висхідним пробоєм
    - probe_up: Розвідувальний висхідний рух
    - grab_upper/grab_lower: Захоплення верхніх/нижніх рівнів
    - pullback_up: Відкат у висхідному тренді
    - range_fade: Згасання в діапазоні
    - down_trend: Низхідний тренд
Функції:
    - market_class_for_symbol(): Визначає ринковий клас за символом
    - get_profile_thresholds(): Отримує порогові значення для профілю
    - get_false_breakout_cfg(): Отримує конфігурацію хибних пробоїв
Примітка: Всі налаштування призначені виключно для телеметрії та аналітики,
не впливають на торгову логіку.

Контракти Stage1/Stage2/Stage3 НЕ змінюємо — профілі лише впливають на обчислення
stage2_hint і market_context.meta.profile (spying-only) під фіче-флагом.
"""

from __future__ import annotations

from typing import Any, TypedDict, Tuple


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


# ── Символьні оверрайди порогів профілів (телеметрія‑only) ──────────────
# Формат:
#   SYMBOL_OVERRIDES = {
#       "TONUSDT": {
#           "probe_up": {
#               "presence_min_delta": -0.05,
#               "vwap_dev_min_delta": -0.002,
#               "alt_confirm_min_override": 1,
#           }
#       }
#   }
# Примітка:
#   - Використовуються лише для Stage1 телеметрії/підказок; жодного впливу на Stage2/Stage3.
#   - Мінімальна кардинальність: ключі лише symbol і profile.
#   - Делти застосовуються до базових порогів профілю ринкового класу.
SYMBOL_OVERRIDES: dict[str, dict[str, dict[str, float | int]]] = {
    # Постійний SNX override (телеметрія‑only)
    "snxusdt": {
        "probe_up": {"alt_confirm_min_override": 3, "presence_min_delta": +0.02}
    },
}


def apply_symbol_overrides(
    base: dict[str, Any] | None, symbol: str, profile: str
) -> Tuple[dict[str, Any], bool]:
    """Застосовує символьні оверрайди до базових порогів профілю.

    Підтримувані ключі у overrides[SYMBOL][profile]:
      - presence_min_delta: float (додається до presence_min, clamp ≥0)
      - vwap_dev_min_delta: float (додається до vwap_dev_min, clamp ≥0)
      - bias_abs_min_delta: float (додається до bias_abs_min, clamp ≥0)
      - alt_confirm_min_delta: int (додається до alt_confirm_min, clamp ≥0)
      - alt_confirm_min_override: int (жорстко задає alt_confirm_min, clamp ≥0)

    Повертає (оновлений_словник, чи_було_застосовано_оверрайд).
    Безпечний: при відсутності бази/оверрайдів повертає копію бази та False.
    """
    base_thr: dict[str, Any] = dict(base or {})
    if not symbol or not profile:
        return (base_thr, False)
    try:
        sym_up = str(symbol or "").upper()
        sym_lo = str(symbol or "").lower()
        prof_key = str(profile or "")
        # Підтримка ключів у будь-якому регістрі
        ov_sym = SYMBOL_OVERRIDES.get(sym_up) or SYMBOL_OVERRIDES.get(sym_lo)
        ov_prof = (ov_sym or {}).get(prof_key)
    except Exception:
        ov_prof = None
    if not isinstance(ov_prof, dict) or not ov_prof:
        return (base_thr, False)

    applied = False

    def _clamp_nonneg(x: float) -> float:
        try:
            return x if x >= 0.0 else 0.0
        except Exception:
            return 0.0

    # Делти для порогів
    if "presence_min_delta" in ov_prof:
        try:
            base_thr["presence_min"] = _clamp_nonneg(
                float(base_thr.get("presence_min", 0.0))
                + float(ov_prof.get("presence_min_delta") or 0.0)
            )
            applied = True
        except Exception:
            pass
    if "vwap_dev_min_delta" in ov_prof:
        try:
            base_thr["vwap_dev_min"] = _clamp_nonneg(
                float(base_thr.get("vwap_dev_min", 0.0))
                + float(ov_prof.get("vwap_dev_min_delta") or 0.0)
            )
            applied = True
        except Exception:
            pass
    if "bias_abs_min_delta" in ov_prof:
        try:
            base_thr["bias_abs_min"] = _clamp_nonneg(
                float(base_thr.get("bias_abs_min", 0.0))
                + float(ov_prof.get("bias_abs_min_delta") or 0.0)
            )
            applied = True
        except Exception:
            pass
    # Alt‑confirm: override має пріоритет над delta
    if "alt_confirm_min_override" in ov_prof:
        try:
            v = int(ov_prof.get("alt_confirm_min_override") or 0)
        except Exception:
            v = 0
        if v < 0:
            v = 0
        base_thr["alt_confirm_min"] = v
        applied = True
    elif "alt_confirm_min_delta" in ov_prof:
        try:
            cur = int(base_thr.get("alt_confirm_min", 0) or 0)
            dv = int(ov_prof.get("alt_confirm_min_delta") or 0)
            v = max(0, int(cur + dv))
            base_thr["alt_confirm_min"] = v
            applied = True
        except Exception:
            pass

    return (base_thr, applied)


def get_false_breakout_cfg(market_class: str) -> dict[str, float | int]:
    mc = (market_class or "ALTS").upper()
    return dict(FALSE_BREAKOUT.get(mc) or FALSE_BREAKOUT["ALTS"])  # shallow copy


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
    "FALSE_BREAKOUT",
    "get_false_breakout_cfg",
    "SYMBOL_OVERRIDES",
    "apply_symbol_overrides",
]
