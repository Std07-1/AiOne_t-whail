"""Stage2-конфігурація.

Мета:
- Винести Stage2-параметри у окремий модуль із чітким API.
- Створити основу для подальшої міграції фазових порогів і strict‑профілю (без історичного QDE).

Примітка:
- Публічні імена експортуються через ``config.config`` (re-export).
- Нові Stage2-параметри додавайте сюди або в YAML, а не в ``config/config.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final, Literal, NotRequired, TypedDict

# Телеметрійна директорія: деякі модулі (whale.insight_builder) очікують символ тут
try:  # не імпортуємо на верхньому рівні config.config у змінному середовищі
    from config.config import TELEMETRY_BASE_DIR as _TELEMETRY_BASE_DIR  # type: ignore

    TELEMETRY_BASE_DIR: Final[str] = str(_TELEMETRY_BASE_DIR)
except Exception:  # pragma: no cover - безпечний дефолт
    TELEMETRY_BASE_DIR = "./telemetry"


class Stage2InsightFallbackStability(TypedDict, total=False):
    """Параметри фільтра стабільності для телеметрійного fallback."""

    drift_max: float


class Stage2InsightFallbackVWAPBoost(TypedDict, total=False):
    """Налаштування бонусу за збіг знаку VWAP-відхилення з bias."""

    abs_min: float
    bonus: float


class Stage2InsightFallbackZones(TypedDict, total=False):
    """Вимоги до кількості зон для активації fallback."""

    accum_min: int
    dist_min: int
    policy: str


class Stage2InsightFallbackConfig(TypedDict, total=False):
    """Конфігурація телеметрійного fallback із китових метрик."""

    enabled: bool
    presence_min: float
    bias_min: float
    weight: float
    weight_cap_by_volregime: Mapping[str, float]
    stability_window: Stage2InsightFallbackStability
    vwap_dev_boost: Stage2InsightFallbackVWAPBoost
    zones_min_filter: Stage2InsightFallbackZones


class Stage2InsightConfig(TypedDict, total=False):
    """Конфігурація Market Insight (Phase 1.5, телеметрія-only)."""

    enabled: bool
    market_now_file: str
    asset_cards_file: str
    market_now_interval_sec: int
    fallback_from_whales: Stage2InsightFallbackConfig
    note_fallback_in_explain: bool


class StrongPreBreakoutAltThresholds(TypedDict):
    band_max: float
    slope_min: float
    dvr_min: float
    cd_min: float


class StrongPreBreakoutThresholds(TypedDict):
    band_max: float
    vol_z_min: float
    dvr_min: float
    cd_min: float
    htf_ok: bool
    alt: StrongPreBreakoutAltThresholds


class StrongPostBreakoutThresholds(TypedDict):
    near_edge: bool
    vol_z_max: float
    dvr_min: float


class StrongMomentumThresholds(TypedDict):
    slope_min: float
    cd_min: float
    rsi_lo: float
    rsi_hi: float
    vol_z_max: float


class StrongExhaustionThresholds(TypedDict):
    band_min: float
    vol_z_min: float
    rsi_hi: float
    atr_mult_low_gate: float
    deny_proxy_volz: bool


class StrongPhaseThresholds(TypedDict):
    pre_breakout: StrongPreBreakoutThresholds
    post_breakout: StrongPostBreakoutThresholds
    momentum: StrongMomentumThresholds
    exhaustion: StrongExhaustionThresholds


class StrongImpulseOverlay(TypedDict):
    slope_min: float
    vwap_dev_abs_min: float
    vol_z_max: float


class StrongInsightThresholds(TypedDict):
    rr_thresh: float
    atr_ratio_trade_min: float
    phase_ttl_sec: int


class StrongTrapThresholds(TypedDict):
    min_dist_to_edge_pct: float
    slope_significant: float
    acceleration_threshold: float
    volume_z_threshold: float
    atr_pct_spike_ratio: float


class StrongWhaleScoringConfig(TypedDict):
    vwap_flag: float
    twap: float
    iceberg: float
    zones_max: float
    zones_accum_cap: float
    zones_dist_cap: float
    dev_bonus: float
    dev_strong: float
    bias_base: float
    bias_dev_boost: float
    bias_iceberg_eps: float


class StrongRegimeConfig(TypedDict):
    phase_thresholds: StrongPhaseThresholds
    impulse_overlay: StrongImpulseOverlay
    insight: StrongInsightThresholds
    whale_scoring: StrongWhaleScoringConfig
    trap_thresholds: NotRequired[StrongTrapThresholds]


STAGE2_PROFILE: Final[Literal["strict", "legacy"]] = "strict"
STRICT_PROFILE_ENABLED: Final[bool] = True
WHALE_SCORING_V2_ENABLED: Final[bool] = True
INSIGHT_LAYER_ENABLED: Final[bool] = True
PROCESSOR_INSIGHT_WIREUP: Final[bool] = True
VOLZ_PROXY_ALLOWED_FOR_EXHAUSTION: Final[bool] = False

# Головний фіче‑флаг історичного QDE Core (залишено для зворотної сумісності)
STAGE2_QDE_ENABLED: Final[bool] = False
# Тег версії порогів Stage2
USED_THRESH_TAG: Final[str] = "strict:v1"

# ──────────────────────────────────────────────────────────────────────────────
# Stage2-lite: router (signal_v2) профілі порогів
# ──────────────────────────────────────────────────────────────────────────────


class RouterSideThresholds(TypedDict, total=False):
    band_max: float
    rsi_lo: float
    rsi_hi: float
    slope_min: float  # для long; для short використовується slope_abs_min
    slope_abs_min: float  # для short
    dvr_max: float
    cd_min: float  # для long; для short використовується cd_abs_min
    cd_abs_min: float  # для short
    presence_soft_min: float
    bias_soft_min: float  # sign-aware: ≥ для long, ≤ -.. для short
    vdev_soft_min: float  # abs for short, pos for long
    zones_accum_min: int
    zones_dist_min: int
    presence_alert_min: float
    vdev_alert_min: float  # abs for short, pos for long
    zones_accum_min_alert: int
    zones_dist_min_alert: int


class RouterDominanceThresholds(TypedDict, total=False):
    vdev_abs_min: float
    slope_atr_abs_min: float
    zones_accum_min: int
    zones_dist_min: int


class RouterProfile(TypedDict, total=False):
    stale_policy: str  # "observe_only" | "allow_soft"
    long: RouterSideThresholds
    short: RouterSideThresholds
    dominance: RouterDominanceThresholds


# === STAGE2_SIGNAL_V2_PROFILES ===
# Це словник профілів для Stage2-сигналів роутера.
# Кожен профіль (наприклад, "strict", "legacy") визначає набір порогів для long/short/домінування.
# Параметри впливають на фільтрацію, генерацію сигналів та поведінку системи.

# Консервативний strict‑профіль: відповідає поточній інлайн-логіці роутера
STAGE2_SIGNAL_V2_PROFILES: Final[dict[str, RouterProfile]] = {
    "strict": {
        # stale_policy: політика для "застарілих" сигналів
        # "observe_only" — лише спостерігати, не діяти на soft-сигнали
        "stale_policy": "allow_soft",
        "long": {
            # band_max: максимальна ширина цінового каналу (volatility band), фільтр для тренду
            "band_max": 0.02,
            # rsi_lo/rsi_hi: межі RSI для long-сигналу (відсікання слабких/перекуплених зон)
            "rsi_lo": 45.0,
            "rsi_hi": 65.0,
            # slope_min: мінімальний нахил тренду (градієнт), визначає силу руху
            "slope_min": 0.5,
            # dvr_max: максимальний коефіцієнт дивергенції (відхилення від тренду)
            "dvr_max": 0.8,
            # cd_min: мінімальна кумулятивна дельта (обʼємна перевага)
            "cd_min": 0.2,
            # SOFT-пороги — для "мʼяких" сигналів (менш впевнені)
            "presence_soft_min": 0.50,  # мінімальна присутність патерну
            "bias_soft_min": 0.25,  # мінімальний зсув (bias) у потрібний бік
            "vdev_soft_min": 0.005,  # мінімальна волатильність (volatility deviation)
            "zones_accum_min": 3,  # мінімум зон для накопичення (support/resistance)
            # ALERT-пороги — для "жорстких" сигналів (висока впевненість)
            "presence_alert_min": 0.65,  # підвищена присутність патерну
            "vdev_alert_min": 0.01,  # підвищена волатильність
            "zones_accum_min_alert": 3,  # мінімум зон для alert
        },
        "short": {
            # Аналогічні параметри, але для short-сигналу (дзеркально)
            "band_max": 0.02,
            "rsi_lo": 35.0,
            "rsi_hi": 55.0,
            "slope_abs_min": 0.5,  # абсолютний нахил (для падіння)
            "dvr_max": 0.8,
            "cd_abs_min": 0.2,  # абсолютна кумулятивна дельта (для short)
            # SOFT
            "presence_soft_min": 0.50,
            "bias_soft_min": 0.25,  # abs, напрямок −
            "vdev_soft_min": 0.005,  # abs, знак −
            "zones_dist_min": 3,  # мінімальна дистанція між зонами
            # ALERT
            "presence_alert_min": 0.65,
            "vdev_alert_min": 0.01,  # abs, знак −
            "zones_dist_min_alert": 3,
        },
        "dominance": {
            # Параметри для визначення домінування (сильний тренд)
            "vdev_abs_min": 0.01,  # мінімальна абсолютна волатильність
            "slope_atr_abs_min": 0.5,  # мінімальний нахил у ATR-одиницях
            "zones_accum_min": 3,  # мінімум зон для накопичення
            "zones_dist_min": 3,  # мінімум зон для дистанції
        },
    },
    "legacy": {
        # Більш "мʼякий" профіль, для сумісності або тестування
        "stale_policy": "allow_soft",  # дозволяє soft-сигнали
        "long": {
            "band_max": 0.03,
            "rsi_lo": 40.0,
            "rsi_hi": 70.0,
            "slope_min": 0.4,
            "dvr_max": 0.9,
            "cd_min": 0.1,
            # SOFT
            "presence_soft_min": 0.40,
            "bias_soft_min": 0.20,
            "vdev_soft_min": 0.003,
            "zones_accum_min": 2,
            # ALERT
            "presence_alert_min": 0.60,
            "vdev_alert_min": 0.008,
            "zones_accum_min_alert": 3,
        },
        "short": {
            "band_max": 0.03,
            "rsi_lo": 30.0,
            "rsi_hi": 60.0,
            "slope_abs_min": 0.4,
            "dvr_max": 0.9,
            "cd_abs_min": 0.1,
            # SOFT
            "presence_soft_min": 0.40,
            "bias_soft_min": 0.20,  # abs, напрямок −
            "vdev_soft_min": 0.003,  # abs, знак −
            "zones_dist_min": 2,
            # ALERT
            "presence_alert_min": 0.60,
            "vdev_alert_min": 0.008,  # abs, знак −
            "zones_dist_min_alert": 3,
        },
        "dominance": {
            "vdev_abs_min": 0.008,
            "slope_atr_abs_min": 0.4,
            "zones_accum_min": 2,
            "zones_dist_min": 2,
        },
    },
}

# === Додаткові нотатки ===
# - "long"/"short" — окремі пороги для лонг/шорт сценаріїв.
# - SOFT-пороги — для менш впевнених сигналів, ALERT — для сильних/критичних.
# - "dominance" — визначає, чи ринок знаходиться у фазі домінування тренду.
# - Значення параметрів підбираються емпірично для контролю якості сигналів.
# - Зміна цих порогів впливає на чутливість і частоту сигналів Stage2.
# - Не змінюйте структуру без оновлення контрактів та тестів!


# ── Stage2 (lite) — телеметрія/alert-hints (консервативно, за замовчуванням вимкнено)
# Використовує телеметрію китів для надання підказок/оцінок у Stage2 без зміни гейтів/рішень
# (потрібно явно увімкнути через STAGE2_INSIGHT.enabled у конфігурації)
# Телеметрія китів використовується для надання додаткової інформації про ринок
STAGE2_INSIGHT: Final[Stage2InsightConfig] = {
    "enabled": True,
    "market_now_file": "insight_market_now.jsonl",
    "asset_cards_file": "insight_asset_cards.jsonl",
    "market_now_interval_sec": 45,
    "fallback_from_whales": {
        "enabled": True,
        "presence_min": 0.18,
        "bias_min": 0.35,
        "weight": 0.80,
        "zones_min_filter": {"policy": "any", "accum_min": 3, "dist_min": 3},
        "stability_window": {"drift_max": 0.10},
        "weight_cap_by_volregime": {
            "normal": 0.8,
            "high": 0.65,
            "hyper": 0.5,
        },
    },
    "note_fallback_in_explain": True,
    # Пенальті для підказок/інсайтів, якщо whale-метрики застарілі (stale)
    # score *= stale_hint_penalty; score = min(score, stale_hint_cap)
    "stale_hint_penalty": 0.5,
    "stale_hint_cap": 0.35,
}

# Включення наративної телеметрії Stage2 (Market Insight narratives)
STAGE2_NARRATIVE_ENABLED: Final[bool] = False

# ──────────────────────────────────────────────────────────────────────────────
# Stage2: визначення фаз ринку та мапінг у стратегії
# ──────────────────────────────────────────────────────────────────────────────
#: Легкі пороги для фазо-детектора (без бізнес-логіки).
#: Значення є орієнтирами і можуть тюнитись незалежно від коду.
STAGE2_PHASE_THRESHOLDS: Final[dict[str, dict[str, float]]] = {
    "pre_breakout": {
        "band_max": 0.18,
        "vol_z_min": 1.4,
        "dvr_min": 0.6,
        "cd_min": 0.3,
    },
    "post_breakout": {
        "vol_z_max": 1.0,
        "dvr_min": 0.5,
    },
    "momentum": {
        "slope_min": 0.5,
        "rsi_lo": 45.0,
        "rsi_hi": 65.0,
        "cd_min": 0.2,
        "vol_z_max": 1.2,
    },
    "exhaustion": {
        "band_min": 0.45,
        "vol_z_min": 2.0,
        "rsi_hi": 72.0,
        "alt_atr_mult": 1.75,
    },
    "false_breakout": {
        "dvr_max": 0.4,
        "cd_against_thresh": 0.0,
    },
}

# Participation-light ("тихий режим") пороги для м'якої класифікації drift_trend
# Використовується лише при увімкненому прапорі FEATURE_PARTICIPATION_LIGHT.
PARTICIPATION_LIGHT_THRESHOLDS: Final[dict[str, float | bool]] = {
    # Мінімальний абсолютний volume z-score для розгляду м'якого поштовху
    "vol_z_min": 1.0,
    # Верхня межа DVR для збереження "легкої" участі (уникнення сильних пробоїв)
    "dvr_max": 0.8,
    # Дозволити трохи позитивний bias; якщо дані відсутні, трактувати як дозвільне
    "cd_min": 0.0,
    # Мінімальний нахил в ATR-одиницях, що вказує на дрейф тренду
    "slope_min": 0.4,
    # Смуга не повинна бути надмірно широкою (зберігати контекст консолідація-тренд)
    "band_max": 0.35,
    # Чи потрібне HTF підтвердження (False зберігає це як телеметрія-only і дозвільне)
    "htf_required": False,
}


# ── Stage2 volatility/crisis configuration ───────────────────────────────
# Пороги для класифікації режимів волатильності за atr_ratio
STAGE2_VOLATILITY_REGIME: Final[dict[str, float]] = {
    # atr_ratio ≥ hyper_threshold → "hyper_volatile"
    "hyper_threshold": 2.5,
    # atr_ratio ≥ high_threshold → "high_volatile" (інакше "normal")
    "high_threshold": 1.8,
    # atr_pct / median >= crisis_spike_ratio → кризовий режим незалежно від atr_ratio
    "crisis_spike_ratio": 3.0,
    # плавний перехід для spike (між 1.0 та crisis_spike_ratio)
    "crisis_spike_soft": 2.0,
}

# Кризовий режим. Безпечні дефолти і сумісність з детекторами.
STAGE2_CRISIS_MODE: Final[dict[str, object]] = {
    # Фаза 1: лише телеметрія, без впливу на гейти/рішення
    "telemetry_only": True,
    # Мінімальний інтервал повторної перевірки (секунди)
    "ttl_sec": 60.0,
    # Поріг для активації кризового режиму за сукупним severity каскаду
    "cascade_severity_min": 0.75,
    # Коригування risk/rr у кризі (множники ≤ 1 знижують вимоги)
    "rr_min_factor": 0.85,
    "min_atr_ratio_factor": 0.90,
    # Які волатильні режими активують кризу
    "activate_on_regimes": ["high_volatile", "hyper_volatile"],
    # Сумісність із LiquidationCascadeDetector (використовує ці ключі безпосередньо)
    "vol_spike_threshold": 2.5,
    "cascade_threshold": 3.0,
}

# Рантайм-параметри Stage2 (мінімальний набір для сумісності/телеметрії)
STAGE2_RUNTIME: Final[dict[str, object]] = {
    # Довжина історії для ATR% у телеметрії/оцінці режимів
    "atr_history_len": 120,
}

# Strong режим порогів для Stage2 phase_detector
# Визначення порогів для різних фаз ринку та інших параметрів Stage2.
# Використовується, коли STRICT_PROFILE_ENABLED встановлено в True.
STRONG_REGIME: Final[StrongRegimeConfig] = {
    "phase_thresholds": {
        "pre_breakout": {
            "band_max": 0.16,
            "vol_z_min": 1.8,
            "dvr_min": 0.8,
            "cd_min": 0.35,
            "htf_ok": True,
            "alt": {
                "band_max": 0.10,
                "slope_min": 1.2,
                "dvr_min": 0.8,
                "cd_min": 0.20,
            },
        },
        "post_breakout": {
            "near_edge": True,
            "vol_z_max": 1.0,
            "dvr_min": 0.6,
        },
        "momentum": {
            "slope_min": 0.8,
            "cd_min": 0.25,
            "rsi_lo": 45.0,
            "rsi_hi": 60.0,
            "vol_z_max": 1.4,
        },
        "exhaustion": {
            "band_min": 0.50,
            "vol_z_min": 2.2,
            "rsi_hi": 74.0,
            "atr_mult_low_gate": 1.9,
            "deny_proxy_volz": True,
        },
    },
    "impulse_overlay": {
        "slope_min": 1.5,
        "vwap_dev_abs_min": 0.01,
        "vol_z_max": 1.8,
    },
    "insight": {
        "rr_thresh": 1.2,
        "atr_ratio_trade_min": 1.0,
        "phase_ttl_sec": 300,
    },
    "whale_scoring": {
        "vwap_flag": 0.25,
        "twap": 0.15,
        "iceberg": 0.20,
        "zones_max": 0.30,
        "zones_accum_cap": 0.15,
        "zones_dist_cap": 0.15,
        "dev_bonus": 0.10,
        "dev_strong": 0.02,
        "bias_base": 0.60,
        "bias_dev_boost": 0.25,
        "bias_iceberg_eps": 0.02,
    },
    "trap_thresholds": {
        "min_dist_to_edge_pct": 0.03,
        "slope_significant": 0.75,
        "acceleration_threshold": 0.85,
        "volume_z_threshold": 3.00,
        "atr_pct_spike_ratio": 2.40,
    },
    # Додаткові суворі оверрайди/винятки та runtime-гейти
    "overrides": {
        # Дуже вузький HTF-override: дозволити htf_ok=True у чітких імпульсних умовах
        "htf_override": {
            "slope_atr_min": 1.6,
            "presence_min": 0.60,
            "bias_abs_min": 0.30,
            "vol_z_max": 1.6,
            "dvr_min": 0.30,
            "near_edge_required": True,
        },
        # Полегшення вимоги DVR для post_breakout ретесту за суворих умов
        "post_breakout_relief": {
            "dvr_min_base": 0.60,
            "dvr_min_relaxed": 0.50,
            "rsi_lo": 45.0,
            "rsi_hi": 62.0,
            "vol_z_max_relief": 1.2,
            "presence_min": 0.75,
            "bias_abs_min": 0.30,
        },
        # Runtime-гейт для важких локальних обчислень у продюсері (fallback whale)
        "heavy_compute_whitelist": {
            "slope_atr_min": 1.5,
            "near_edge_required": False,
            # за відсутності явної оцінки кризового режиму допускаємо обчислення
            "crisis_vol_score_max": 1.0,
        },
    },
}

# Суворі теги спостереження для Stage2 (використовуються у phase_detector та інших модулях)
# Визначають умови для активації спостереження за певними ринковими умовами
STRICT_WATCH_TAGS: Final[dict[str, object]] = {
    "momentum_overbought_watch": {
        "enabled": True,
        "slope_min": 0.80,
        "rsi_min": 65.0,
        "vol_z_max": 1.80,
        "band_max": 0.35,
        "dist_proximity_min": 0.80,
    },
    "momentum_cooling_box": {
        "enabled": True,
        "band_max": 0.20,
        "vol_z_max": 1.00,
        "dist_proximity_min": 0.80,
        "slope_abs_max": 0.80,
    },
    # Retest‑watch: біля краю з достатнім DVR. Детальна логіка у stage2/processor.py
    "retest_watch": {
        "enabled": True,
        "dvr_min": 0.60,
    },
}


def validate_stage2_config() -> list[str]:
    """Повертає список попереджень щодо можливих конфліктів конфігурації Stage2‑lite.

    Без побічних ефектів: нічого не змінює, лише аналізує поточні прапори/профілі.
    Мета — допомогти уникнути накладання профілів і джерел порогів.

    Перевірки (best‑effort):
    - Якщо STAGE2_PROFILE не "strict" при STAGE2_ENABLED=True — попередження.
    - Якщо ввімкнено YAML‑джерело порогів і профіль YAML != STAGE2_PROFILE — попередження.
    - Якщо PROFILE_ENGINE_ENABLED=True одночасно з YAML‑джерелом, але профілі не узгоджені — попередження.
    - Якщо історичний STAGE2_QDE_ENABLED=True — попередження.
    """
    hints: list[str] = []
    # Перевага config.config (run_window застосовує --set саме туди), фолбек — config.flags
    try:
        import config.config as _cfg

        _s2_on = bool(getattr(_cfg, "STAGE2_ENABLED", False))
        _s2_profile = str(getattr(_cfg, "STAGE2_PROFILE", "strict"))
        _yaml_on = bool(getattr(_cfg, "FEATURE_STAGE2_THRESHOLDS_YAML", False))
        _yaml_profile = str(
            getattr(_cfg, "STAGE2_THRESHOLDS_YAML_PROFILE", _s2_profile)
        )
        _prof_engine = bool(getattr(_cfg, "PROFILE_ENGINE_ENABLED", False))
        _qde_on = bool(getattr(_cfg, "STAGE2_QDE_ENABLED", False))
    except Exception:
        try:
            from config.flags import (
                FEATURE_STAGE2_THRESHOLDS_YAML,
                PROFILE_ENGINE_ENABLED,
                STAGE2_ENABLED,
                STAGE2_PROFILE,
                STAGE2_THRESHOLDS_YAML_PROFILE,
            )

            _yaml_on = FEATURE_STAGE2_THRESHOLDS_YAML
            _prof_engine = PROFILE_ENGINE_ENABLED
            _s2_on = STAGE2_ENABLED
            _s2_profile = STAGE2_PROFILE
            _yaml_profile = STAGE2_THRESHOLDS_YAML_PROFILE
            _qde_on = bool(globals().get("STAGE2_QDE_ENABLED", False))
        except Exception:
            # Якщо flags також недоступні у середовищі, повертаємо порожній список
            return hints

    # 1) Профіль strict як канон
    if bool(_s2_on) and str(_s2_profile).lower() != "strict":
        hints.append(
            "Stage2-lite увімкнено, але STAGE2_PROFILE!='strict'. Рекомендовано strict як канон для signal_v2."
        )

    # 2) YAML пороги vs активний профіль
    if bool(_yaml_on):
        if str(_yaml_profile).lower() != str(_s2_profile).lower():
            hints.append(
                "Увімкнено YAML-пороги, але STAGE2_THRESHOLDS_YAML_PROFILE не співпадає зі STAGE2_PROFILE — вирівняйте профілі."
            )

    # 3) Профільний двигун vs джерела порогів
    if bool(_prof_engine) and bool(_yaml_on):
        if str(_yaml_profile).lower() != str(_s2_profile).lower():
            hints.append(
                "PROFILE_ENGINE_ENABLED=True разом із YAML-порогами різних профілів — можливе накладання. Уніфікуйте на один профіль."
            )

    # 4) Історичний QDE флаг
    if bool(_qde_on):
        hints.append(
            "STAGE2_QDE_ENABLED=True — історичний режим; рекомендовано вимкнути."
        )

    return hints


__all__ = [
    "validate_stage2_config",
    "Stage2InsightConfig",
    "Stage2InsightFallbackConfig",
    "STAGE2_INSIGHT",
    "STAGE2_NARRATIVE_ENABLED",
    "STAGE2_PROFILE",
    "STRICT_PROFILE_ENABLED",
    "WHALE_SCORING_V2_ENABLED",
    "INSIGHT_LAYER_ENABLED",
    "PROCESSOR_INSIGHT_WIREUP",
    "VOLZ_PROXY_ALLOWED_FOR_EXHAUSTION",
    "STAGE2_QDE_ENABLED",
    "USED_THRESH_TAG",
    "STRONG_REGIME",
    "STRICT_WATCH_TAGS",
    "PARTICIPATION_LIGHT_THRESHOLDS",
    "STAGE2_SIGNAL_V2_PROFILES",
    "TELEMETRY_BASE_DIR",
]
