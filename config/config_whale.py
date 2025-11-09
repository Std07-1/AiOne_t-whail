"""Конфігурація китових метрик (whale telemetry & dominance).

Мета:
        - Централізувати параметри нового методу аналізу whale-метрик.
        - Рознести налаштування з ``config.config`` у профільований модуль без бізнес-логіки.
        - Підтримувати легкий вибір профілю (legacy/aggressive/conservative) через YAML.

Структура:
        - :class:`WhaleTelemetryPresenceWeights` — ваги компонентів presence_score.
        - :class:`WhaleTelemetryDominanceConfig` — пороги для buy/sell dominance.
        - :class:`WhaleTelemetryEMAConfig` — опції EMA-згладжування presence.
        - :class:`WhaleTelemetryConfig` — верхньорівневий словник конфігурації.

Примітка:
    - Значення змінюються лише через PR із зазначенням впливу на PnL/latency.
    - Нові параметри для whale-логіки додавайте **лише** в цьому модулі, а не в ``config.config``.
    - В інструментах аналітики використовуйте ``dict(STAGE2_WHALE_TELEMETRY)`` для копії.

Операційна ціль:
    - Частка «застарілих» whale‑метрик (stale) в онлайні має бути < 15% від загальної кількості батчів.
      Будь ласка, відслідковуйте це через Prometheus/логи та оптимізуйте джерело даних при відхиленнях.
"""

from __future__ import annotations

from typing import Final, TypedDict


class WhaleTelemetryPresenceWeights(TypedDict):
    """Ваги компонентів presence_score (сума ≈ 1.0)."""

    vwap: float
    iceberg: float
    accum: float
    dist: float
    twap: float


class WhaleTelemetryDominanceAltConfirm(TypedDict, total=False):
    """Додаткові підтвердження для домінування (buy/sell)."""

    need_any: int
    vol_spike: bool
    iceberg: bool
    dist_zones_min: int


class WhaleTelemetryDominanceConfig(TypedDict, total=False):
    """Параметри buy/sell dominance flags."""

    vwap_dev_min: float
    slope_abs_min: float
    alt_confirm: WhaleTelemetryDominanceAltConfirm


class WhaleTelemetryEMAConfig(TypedDict, total=False):
    """Налаштування EMA-згладжування presence_score."""

    enabled: bool
    alpha: float


class WhaleTelemetryHintWeights(TypedDict, total=False):
    """Ваги компонентів для stage2_hint score."""

    presence: float
    bias: float
    vwap_dev: float


class WhaleTelemetryHintCooldown(TypedDict, total=False):
    """Налаштування cooldown для stage2_hint."""

    base: float
    impulse: float


class WhaleTelemetryConfig(TypedDict, total=False):
    """Базовий словник конфігурації китових метрик."""

    enabled: bool
    tail_window: int
    bias_confirm_thr: float
    bias_warn_thr: float
    bias_vwap_dev_thr: float
    iceberg_only_cap: float
    dominance: WhaleTelemetryDominanceConfig
    presence_weights: WhaleTelemetryPresenceWeights
    max_zones_contrib: int
    strong_dev_thr: float
    mid_dev_thr: float
    presence_proxy_cap: float
    vwap_flag_dev_thr: float
    dev_bonus: float  # legacy single-tier bonus (mapped to high)
    dev_bonus_low: float
    dev_bonus_high: float
    presence_zones_none_cap: float
    presence_accum_only_cap: float
    ema: WhaleTelemetryEMAConfig
    hint_weights: WhaleTelemetryHintWeights
    hint_cooldown: WhaleTelemetryHintCooldown
    stale_score_penalty: float
    neutral_score_penalty: float


STAGE2_WHALE_TELEMETRY: Final[WhaleTelemetryConfig] = {
    "enabled": True,
    "tail_window": 50,
    "bias_confirm_thr": 0.6,
    "bias_warn_thr": 0.4,
    "bias_vwap_dev_thr": 0.01,
    "iceberg_only_cap": 0.40,
    "dominance": {
        "vwap_dev_min": 0.01,
        "slope_abs_min": 3.0,
        "alt_confirm": {
            "need_any": 2,
            "vol_spike": True,
            "iceberg": True,
            "dist_zones_min": 3,
        },
    },
    "presence_weights": {
        "vwap": 0.25,
        "iceberg": 0.20,
        "accum": 0.10,
        "dist": 0.30,
        "twap": 0.15,
    },
    "max_zones_contrib": 4,
    "strong_dev_thr": 0.02,
    "mid_dev_thr": 0.012,
    "presence_proxy_cap": 0.85,
    "vwap_flag_dev_thr": 0.005,
    # Двоступеневі бонуси для presence за |vwap_dev|
    # Залишено legacy "dev_bonus" для сумісності (використовується як high, якщо задано через STRONG_REGIME)
    "dev_bonus": 0.08,
    "dev_bonus_low": 0.04,
    "dev_bonus_high": 0.08,
    # Денасящення presence за відсутності зон (zones=None)
    "presence_zones_none_cap": 0.30,
    "presence_accum_only_cap": 0.35,
    "ema": {
        "enabled": True,
        "alpha": 0.30,
    },
    "hint_weights": {
        "presence": 0.45,
        "bias": 0.35,
        "vwap_dev": 0.20,
    },
    "hint_cooldown": {
        "base": 120.0,
        "impulse": 45.0,
    },
    "stale_score_penalty": 0.5,
    "neutral_score_penalty": 0.6,
}

__all__ = ["WhaleTelemetryConfig", "STAGE2_WHALE_TELEMETRY"]
