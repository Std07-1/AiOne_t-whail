"""
Архітектурні принципи:

Конфігурація — лише дані та легкі структури, без бізнес‑логіки і без гарячого I/O.
Розподіл відповідальності між:
constants.py — канонічні K_* ключі, стани, константи (re‑export у config).
flags.py — усі фіче‑флаги з докстрінгами; rollback‑дружні.
keys.py — єдиний шлях формування Redis ключів/каналів (KeyBuilder).
loaders.py — YAML‑лоадери з кешем для великих карт у config/data/*.
config_whale.py — вся whale‑телеметрія виноситься сюди (а не в config.py).
Контракти та Redis:

Контракти Stage1/Stage2/Stage3 незмінні; нові поля — тільки в market_context.meta.* або confidence_metrics.
Ключі будуються лише через keys.py, формат ai_one:{domain}:{symbol}:{granularity}; TTL — з INTERVAL_TTL_MAP.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, Final

from .config_stage2 import (  # noqa: F401 re-export for consumers
    STAGE2_CRISIS_MODE,
    STAGE2_INSIGHT,
    STAGE2_RUNTIME,
    STAGE2_SIGNAL_V2_PROFILES,
    STAGE2_VOLATILITY_REGIME,
)
from .config_whale import STAGE2_WHALE_TELEMETRY
from .constants import (
    ASSET_STATE,
    K_ANOMALY_DETECTION,
    K_ATR_PCT_STAGE2,
    K_ATR_VS_LOW_GATE_RATIO,
    K_CONFIDENCE_METRICS,
    K_CUMULATIVE_DELTA,
    K_DIRECTIONAL_CONFLICT,
    K_DIRECTIONAL_CONFLICT_STRENGTH,
    K_DIRECTIONAL_VOLUME_RATIO,
    K_LOW_VOLATILITY_FLAG,
    K_MARKET_CONTEXT,
    K_PRICE_SLOPE_ATR,
    K_RAW_TRIGGER_REASONS,
    K_RECOMMENDATION,
    K_RISK_PARAMETERS,
    K_SIGNAL,
    K_STATS,
    K_SYMBOL,
    K_THRESHOLDS,
    K_TRIGGER_REASONS,
    STAGE2_STATUS,
)
from .flags import (
    EXH_STRATEGY_HINT_ENABLED,
    HEAVY_COMPUTE_GATING_ENABLED,
    INSIGHT_LAYER_ENABLED,
    INSIGHT_TELEMETRY_ONLY,
    PACK_EXHAUSTION_REVERSAL_ENABLED,
    SOFT_RECO_SCORE_THR,
    SOFT_RECOMMENDATIONS_ENABLED,
    STAGE2_ENABLED,
    STAGE2_PROFILE,
    STRICT_LOG_MARKERS,
    STRICT_PROFILE_ENABLED,
    TRAP_COOLDOWN_OVERRIDE_ENABLED,
    UI_INSIGHT_PUBLISH_ENABLED,
    UI_PUBLISH_MIN_INTERVAL_MS,
    UI_SMART_PUBLISH_ENABLED,
    UI_WHALE_PUBLISH_ENABLED,
    VOLZ_PROXY_ALLOWED_FOR_EXHAUSTION,
    WHALE_SCORING_V2_ENABLED,
    WHALE_THROTTLE_SEC,
)


@dataclass(slots=True)
class FilterParams:
    """Lightweight replacement for the legacy Pydantic filter settings."""

    min_quote_volume: float = 1_200_000.0
    min_price_change: float = 3.0
    min_open_interest: float = 600_000.0
    min_orderbook_depth: float = 60_000.0
    min_atr_percent: float = 0.5
    max_symbols: int = 40
    dynamic: bool = False
    strict_validation: bool = True

    def dict(self) -> dict[str, Any]:
        """Return a plain dict copy of the filter settings."""

        return asdict(self)

    def copy(self, **override: Any) -> FilterParams:
        """Clone the params with optional overrides."""

        payload = self.dict()
        payload.update(override)
        return FilterParams(**payload)


@dataclass(slots=True)
class MetricResults:
    """Snapshot of the Stage1 filter metrics for debugging/visualisation."""

    initial_count: int
    prefiltered_count: int
    filtered_count: int
    result_count: int
    elapsed_time: float
    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.params, dict):
            self.params = dict(self.params)

    def dict(self) -> dict[str, Any]:
        """Return a serialisable view of the metrics."""

        payload = asdict(self)
        payload["params"] = dict(self.params)
        return payload


class SymbolInfo(Mapping[str, Any]):
    """Thin mapping wrapper for Binance ``exchangeInfo`` responses."""

    __slots__ = ("_data",)

    def __init__(self, **payload: Any) -> None:
        self._data = dict(payload)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __getattr__(self, item: str) -> Any:
        try:
            return self._data[item]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(item) from exc

    @property
    def symbol(self) -> str:
        return str(self._data.get("symbol", ""))

    def dict(self) -> dict[str, Any]:
        return dict(self._data)

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"SymbolInfo({self._data!r})"


# ── Core directories ─────────────────────────────────────────────────────
# Дозволяємо ізоляцію стану між інстансами через ENV "STATE_NAMESPACE".
# Значення нормалізується (обрізаємо двокрапки/пробіли). За замовчуванням "ai_one".
_ns_env = str(os.getenv("STATE_NAMESPACE", "")).strip().strip(":")
NAMESPACE: Final[str] = _ns_env or "ai_one"
DATASTORE_BASE_DIR: Final[str] = "./datastore"
# Директорія для локальних JSONL‑журналів телеметрії
TELEMETRY_BASE_DIR: Final[str] = "./telemetry"


# ── Redis keys / channels ─────────────────────────────────────────────────
REDIS_CHANNEL_ASSET_STATE: Final[str] = "asset_state"
REDIS_CHANNEL_UI_ASSET_STATE: Final[str] = "ui_asset_state"

REDIS_SNAPSHOT_KEY: Final[str] = "snapshot"
REDIS_SNAPSHOT_UI_KEY: Final[str] = "ui_snapshot"

REDIS_CACHE_TTL: Final[int] = 900
CACHE_TTL_DAYS: Final[int] = 3
NETWORK_DNS_CACHE_TTL_SEC: Final[int] = 900


# ── UI publishing / schema toggles ────────────────────────────────────────
# Мінімальний необхідний набір для роботи UI publish_full_state
UI_SNAPSHOT_TTL_SEC: Final[int] = 60
UI_PAYLOAD_SCHEMA_VERSION: Final[str] = "v2"
UI_TP_SL_FROM_STAGE3_ENABLED: Final[bool] = False
UI_USE_V2_NAMESPACE: Final[bool] = True
UI_DUAL_PUBLISH: Final[bool] = False
# Публікувати компактний snapshot market_context.meta у state‑HSET (для deep‑state моніторингу)
STATE_PUBLISH_META_ENABLED: Final[bool] = True

# ── Telemetry files / toggles ─────────────────────────────────────────────
# Stage3 події телеметрії, лог файл для логування подій Stage3 (наприклад, відкриття/закриття угод).
# Формат — JSONL (по одному запису на рядок).
STAGE3_EVENTS_LOG: Final[str] = "stage3_events.jsonl"
# Файл для фіксації причин пропуску Stage3 відкриття (best-effort JSONL).
STAGE3_SKIPS_LOG: Final[str] = "stage3_skips.jsonl"
# Файл телеметрії якості ALERT життєвого циклу (формат JSONL).
ALERTS_QUALITY_LOG: Final[str] = "alerts_quality.jsonl"
# Файл циклових latency Stage1 KPI (формат JSONL).
STAGE1_LATENCY_LOG: Final[str] = "stage1_latency.jsonl"
# Флаг, що вмикає або вимикає телеметрію для Stage3.
# Якщо False — події Stage3 не логуються.
STAGE3_TELEMETRY_ENABLED: Final[bool] = True
# Формат — JSONL (по одному запису на рядок).
STAGE1_EVENTS_LOG: Final[str] = "stage1_events.jsonl"
# Флаг для логування подій Stage1 (наприклад, спрацювання тригерів).
STAGE1_TELEMETRY_ENABLED: Final[bool] = True
# Чи зберігати snapshot (знімок стану) по кожному символу у Stage1.
STAGE1_SYMBOL_SNAPSHOT_ENABLED: Final[bool] = True
STAGE1_SYMBOL_SNAPSHOT_INTERVAL_SEC: Final[int] = (
    60  # Інтервал (у секундах) між збереженням snapshot для кожного символу.
)

# Опційні Prometheus‑метрики (легкий HTTP‑ендпоінт /metrics)
# За замовчуванням вимкнено; інструменти/процеси самі ініціалізують сервер, якщо прапор True
PROM_GAUGES_ENABLED: Final[bool] = True
# Порт для HTTP‑ендпоінту Prometheus (за замовчуванням 9108)
PROM_HTTP_PORT: Final[int] = int(os.getenv("PROM_HTTP_PORT", "9108"))

# ── Direction-only/Insight scaffolding (env-toggleable) ────────────────────
# Лише напрямок (up/down) та класи OBSERVE/AVOID/TRADEABLE без ордерів.
# За замовчуванням вимкнено; використовується у канарейкових перевірках.
INSIGHT_DIRECTION_ONLY: Final[bool] = str(
    os.getenv("INSIGHT_DIRECTION_ONLY", "false")
).strip().lower() in {"1", "true", "yes"}

# ── Zone windows (strict) для китових зон supply/demand ───────────────────
# При ввімкненні примушує використання фіксованих вікон сегментації,
# щоб уникнути "плоских" зон на низьких гістограмах.
ZONE_WINDOWS_STRICT_ENABLED: Final[bool] = str(
    os.getenv("ZONE_WINDOWS_STRICT_ENABLED", "false")
).strip().lower() in {"1", "true", "yes"}
ZONE_WINDOW_SHORT: Final[int] = int(os.getenv("ZONE_WINDOW_SHORT", "50"))
ZONE_WINDOW_LONG: Final[int] = int(os.getenv("ZONE_WINDOW_LONG", "250"))

# Debug‑сліди рішень (JSONL) — лише телеметрія, контракти не змінюються
# Використовується для «максимальної прозорості»: один рядок JSON на символ/батч
# Формат і набір полів компактний та стабільний, важкі об’єкти не серіалізуються
DEBUG_DECISION_DUMP_ENABLED: Final[bool] = True
DECISION_DUMP_DIR: Final[str] = "./telemetry/decision_traces"
# Мінімальний набір полів для JSONL‑сліду; решта ігнорується
DECISION_DUMP_FIELDS: Final[tuple[str, ...]] = (
    "ts",
    "symbol",
    "phase",
    "phase_score",
    "phase_reasons",
    "scenario",
    "scenario_conf",
    "bias_state",
    "btc_regime",
    "btc_htf_strength",
    # вибрані Stage1 стати
    "band_pct",
    "near_edge",
    "atr_ratio",
    "vol_z",
    "rsi",
    "dvr",
    "cd",
    "slope_atr",
)

# Stage3 StrategyPack профілі (телеметрія-first; застосовуються лише разом із фіче-флагом)
STAGE3_STRATEGY_PROFILES: Final[dict[str, dict[str, Any]]] = {
    "exhaustion_reversal_long": {
        "min_hold_seconds": 120.0,
        "adverse_move_atr": 0.45,
        "trail_arm_atr": 0.9,
        "trail_break_even_atr": 1.1,
        "trail_buffer_atr": 0.45,
        "trail_buffer_atr_low": 0.55,
        "trail_low_atr_threshold": 0.0015,
        "symbol_cooldown_sec": 900.0,
        "trade_timeout_sec": 900.0,
        "trade_timeout_sec_low": 600.0,
        "trade_timeout_sec_mid": 900.0,
        "trade_timeout_atr_low": 0.0012,
        "trade_timeout_atr_high": 0.0035,
        "debounce_window_sec": 60.0,
        # Додаткові телеметрійні параметри для Stage3 risk-аналізу
        "risk_pct": 0.35,
        "tp1_atr": 1.0,
        "tp2_atr": 1.8,
        "trail_atr": 0.8,
        "cooldown_s": 900,
        "timeout_s": 900,
        "entry_guard": {"volz_pullback_max": 0.8, "need_reclaim": True},
    }
}

# Порог застарівання Stage1 HTF статистики (мс)
STAGE1_HTF_STALE_MS: float = 120_000.0

# ── Stage2 whale presence/HTF guards ─────────────────────────────────────
STAGE2_PRESENCE_PROMOTION_GUARDS: Final[dict[str, float]] = {
    "min_bias_abs": 0.15,
    "min_htf_strength": 0.20,
    "cap_without_confirm": 0.20,
}


# ── Strict caps/overrides for whale & phase detectors ────────────────────
STRICT_ACCUM_CAPS_ENABLED: bool = True
STRICT_HTF_GRAY_GATE_ENABLED: bool = True
STRICT_LOW_ATR_OVERRIDE_ON_SPIKE: bool = True
STRICT_ZONE_RISK_CLAMP_ENABLED: bool = True

PRESENCE_CAPS: Final[dict[str, float]] = {"accum_only_cap": 0.30}
PROMOTE_REQ: Final[dict[str, float | bool]] = {
    "min_presence_open": 0.60,
    "min_htf_strength": 0.20,
    "require_bias": True,
}
HTF_GATES: Final[dict[str, float]] = {
    "gray_low": 0.10,
    "ok": 0.20,
    "gray_penalty": 0.20,
}

LOW_ATR_SYMBOLS: Final[set[str]] = {"TRXUSDT", "XRPUSDT"}
ATR_OPEN_MIN: Final[dict[str, float]] = {"default": 1.00, "low_atr": 0.95}

ZONES_OVERRIDES: Final[dict[str, dict[str, float]]] = {
    "TRXUSDT": {
        "accum_window": 15,
        "accum_range_frac_max": 0.0030,
        "accum_median_vol_min_mul": 1.20,
    },
}

LOW_ATR_SPIKE_OVERRIDE: Final[dict[str, float | int]] = {
    "band_expand_min": 0.013,
    "spike_ratio_min": 0.60,
    "abs_volz_min": 2.50,
    "dvr_min": 1.20,
    "bars_ttl": 3,
}

# ── Liquidity sweep (stop‑hunt) hinting ──────────────────────────────────
# Без впливу на контракти/сигнали: лише маркер у market_context.meta та лог‑рядок.
# Умови (евристично, без доступу до повних свічок):
#  - біля межі діапазону (near_edge == 'upper' | 'lower')
#  - підвищена участь (DVR ≥ LIQ_SWEEP_DVR_MIN, уже після нормалізації/EMA‑капа)
#  - слабкий контекст (BTC flat або HTF < SCEN_HTF_MIN)
LIQ_SWEEP_HINT_ENABLED: bool = True
LIQ_SWEEP_DVR_MIN: float = 3.0
LIQ_SWEEP_BAND_PCT_MAX: float = 0.012
LIQ_SWEEP_MIN_DWELL_BARS: int = (
    2  # скільки послідовних 1m‑барів edge має бути незмінним
)
LIQ_SWEEP_COOLDOWN_S: int = 120  # мінімальна пауза між подіями sweep

# ── Sweep→Retest→Bias FSM (контракт‑safe, лише meta/metrics) ────────────
# Усі значення під прапором SWEEP_FSM_ENABLED; вплив тільки на market_context.meta
# і легкі пром-метрики. Інтеграція з селектором — лише м'які коефіцієнти в RAM.
SWEEP_FSM_ENABLED: bool = True
SWEEP_RETEST_WINDOW_S: int = 720  # 12 хв
SWEEP_REJECT_WINDOW_S: int = 1200  # 20 хв
RETEST_HTF_MIN: float = 0.10
RETEST_DVR_MIN: float = 1.0
RETEST_PROX_PCT: float = 0.001  # ±0.1%
REJECT_DRIFT_PCT: float = 0.002  # −0.2% від рівня
CTX_BOOST: float = 0.10
CTX_SUPPRESS: float = 0.15
BIAS_TTL_S: int = 1800  # 30 хвилин

# TTL для подієвого гейджа ai_one_liq_sweep (секунди)
PROM_SWEEP_TTL_S: int = 90

# ── Sweep/Retest quick thresholds (non-invasive, meta/metrics only) ─────
# Не змінюємо існуючі значення; додаємо окремі константи для читабельності правил.
RETEST_OK_TTL_S: int = 120
RETEST_OK_HTF_MIN: float = 0.10
RETEST_OK_DVR_MIN: float = 1.0
RETEST_OK_LEVEL_TOL: float = 0.001  # ±0.1%
SWEEP_THEN_BREAKOUT_TOL: float = 0.001  # +0.1% над рівнем
SWEEP_REJECT_TOL: float = 0.002  # −0.2% від рівня

# ── Prometheus gauges (опційно) ──────────────────────────────────────────
# Примітка: Значення PROM_GAUGES_ENABLED та PROM_HTTP_PORT уже визначені вище
# як константи/Final з можливістю перевизначення через змінні оточення.
# Тут їх повторно не визначаємо, щоб не перезаписувати ENV‑значення.


def atr_open_min_for(symbol: str) -> float:
    """Повертає мінімальний ATR-поріг для відкриття залежно від символу."""

    return (
        ATR_OPEN_MIN["low_atr"]
        if str(symbol).upper() in LOW_ATR_SYMBOLS
        else ATR_OPEN_MIN["default"]
    )


# ── Symbol-specific low ATR profiles ──────────────────────────────────────
STAGE2_LOW_ATR_SYMBOL_GUARDS: Final[dict[str, dict[str, float]]] = {
    "TRXUSDT": {
        "atr_pct_max": 0.0022,
        "presence_cap": 0.35,
        "min_htf_strength": 0.22,
    },
}


# ── Stage3 supply/demand zone overrides ───────────────────────────────────
STAGE3_ZONES_THRESHOLD_OVERRIDES: Final[dict[str, dict[str, float | int | str]]] = {
    "TRXUSDT": {
        "accum_window": 15,
        "accum_range_frac_max": 0.003,
        "accum_median_vol_min": "auto×1.4",
    },
}

# ── Prefilter Strict Profiles ─────────────────────────────────────────────
# Два профілі параметрів StrictThresholds: "default" (жорсткий) та "soft" (м'який).
# Споживачі мають підбирати профіль через фіче‑флаг STAGE1_PREFILTER_STRICT_SOFT_PROFILE,
# напр.: StrictThresholds(**PREFILTER_STRICT_PROFILES["soft"]).
PREFILTER_STRICT_PROFILES: Final[dict[str, dict[str, float | bool]]] = {
    # Ближче до початкових жорстких значень; htf_score поріг піднято вище 1.0, щоби фактично
    # вимагати htf_ok=True (див. логіку у stage1/prefilter_strict.py).
    "default": {
        "min_turnover_usd": 2000.0,
        "min_band_pct": 0.006,
        "min_participation_volz": 1.8,
        "alt_participation_volz": 1.2,
        "alt_min_dvr": 0.8,
        "min_cd": 0.0,
        "htf_required": True,
        "min_htf_score": 1.01,
        "band_wide_bonus_thr": 0.03,
        "band_wide_bonus": 1.05,
        "top_k": 24,
    },
    # М'якший профіль для «тихих» режимів: знижені пороги участі, HTF не обов'язковий,
    # бонус за широкий діапазон лишається. Використовується для телеметрії та пріоритезації.
    "soft": {
        "min_turnover_usd": 2000.0,
        "min_band_pct": 0.006,
        "min_participation_volz": 1.0,
        "alt_participation_volz": 1.0,
        "alt_min_dvr": 0.7,
        "min_cd": 0.0,
        "htf_required": True,
        "min_htf_score": 0.0,
        "band_wide_bonus_thr": 0.03,
        "band_wide_bonus": 1.05,
        "top_k": 24,
    },
}
# Якщо True — snapshot зберігається лише при спрацюванні ALERT, ігноруючи NORMAL.
STAGE1_SYMBOL_SNAPSHOT_ALERTS_ONLY: Final[bool] = True

# Додаткова телеметрія: збагачувати payload події phase_detected мінімальним
# дашборд‑набором (band_pct, near_edge, atr_ratio, vol_z, rsi, dvr, cd,
# slope_atr, whale_* тощо). Rollback: вимкнути флаг.
TELEM_ENRICH_PHASE_PAYLOAD: Final[bool] = True


# ── Stage1 prefilter defaults ─────────────────────────────────────────────
FAST_SYMBOLS_TTL_MANUAL: Final[int] = 3600
FAST_SYMBOLS_TTL_AUTO: Final[int] = 1800
MANUAL_FAST_SYMBOLS_SEED: Final[list[str]] = [
    "BTCUSDT",
    "ETHUSDT",
    # "ARBUSDT",
    # "AXSUSDT",
    # "RUNEUSDT",
    # "ZRXUSDT",
    # "SAGAUSDT",
    # "LISTAUSDT",
    # "MIRAUSDT",
    "TONUSDT",
    "SNXUSDT",
]

PREFILTER_INTERVAL_SEC: Final[int] = 600
PRELOAD_1M_LOOKBACK_INIT: Final[int] = 720
PRELOAD_DAILY_DAYS: Final[int] = 120

#  ── Stage1 prefilter parameters ─────────────────────────────────────────
PREFILTER_BASE_PARAMS: Final[dict[str, float | bool]] = {
    "min_depth": 75_000.0,  # USD
    "min_atr": 0.15,  # %
    "dynamic": True,  # динамічні пороги на основі розподілів
}

#  ── Stage1 prefilter thresholds ────────────────────────────────────────
STAGE1_PREFILTER_THRESHOLDS: Final[dict[str, float | int]] = {
    "MIN_QUOTE_VOLUME": 1_500_000.0,  # USD
    "MIN_PRICE_CHANGE": 3,  # %
    "MIN_OPEN_INTEREST": 500_000.0,  # USD
    "MAX_SYMBOLS": 300,  # макс. кількість символів після prefilter
}

STAGE1_PREFILTER_HEAVY_LIMIT: Final[int] = 350
STAGE1_METRICS_BATCH: Final[int] = 12


# ── Stage1 monitor / anomaly detection ────────────────────────────────────
STAGE1_MONITOR_PARAMS: Final[dict[str, float | int]] = {
    "vol_z_threshold": 2.5,
    "rsi_overbought": 78.0,
    "rsi_oversold": 22.0,
    "min_reasons_for_alert": 2,
    "dynamic_rsi_multiplier": 1.2,
}

# ── Stage1 directional & feature toggles (legacy compatibility) ───────────
# Мінімальні параметри для напрямкових розрахунків у Stage1 (observe‑mode)
DIRECTIONAL_PARAMS: Final[dict[str, float | int]] = {
    "w_short": 3,  # вікно для коротких підрахунків DVR/CD/slope
    "min_total_volume": 1e-6,  # захист від ділення на нуль
}

# Легкі флаги для тригерів/розрахунків у Stage1 (сумісність імпортів)
USE_VOL_ATR: Final[bool] = False
USE_RSI_DIV: Final[bool] = False
USE_VWAP_DEVIATION: Final[bool] = False

STAGE1_BEARISH_REASON_BONUS: Final[float] = 0.15
STAGE1_BEARISH_TRIGGER_TAGS: Final[tuple[str, ...]] = (
    "rsi_divergence",
    "vwap_deviation",
    "trap",
)

STAGE1_TRAP_ENABLED: Final[bool] = True  # Dry-run: не впливає на ALERT, лише stats/logs
STAGE1_TRAP_INFLUENCE_ENABLED: Final[bool] = (
    True  # Керований вплив TRAP на trigger_reasons/ALERT
)
STAGE1_TRAP_MARK_STRONG: Final[bool] = (
    True  # Вважати TRAP сильним тригером за високого vol_z
)
STAGE1_TRAP_STRONG_VOLZ_THR: Final[float] = (
    2.5  # vol_z поріг для зняття low_volatility гейта
)

# Додано score_gate/cooldown/log_prefix для керування інтеграцією у Stage1 (див. trap_detector)
STAGE1_TRAP: Final[dict[str, float | int | str]] = {
    "min_dist_to_edge_pct": 0.05,
    "slope_significant": 0.5,
    "acceleration_threshold": 0.6,
    "volume_z_threshold": 2.5,
    "atr_pct_spike_ratio": 2.0,
    # Stage1 інтеграційні параметри (dry-run gate/cooldown/logging)
    "score_gate": 0.67,
    "cooldown_sec": 120,
    "log_prefix": "[TRAP]",
}

STAGE1_VOLUME_SPIKE_GATES: Final[dict[str, float | int | bool]] = {
    "use_quote_volume": True,
    "z_weight": 1.0,
    "z_thresh": 2.5,
    "z_mad_enabled": True,
    "z_mad_window": 60,
    "ema_ratio_enabled": True,
    "ema_window": 30,
    "ema_ratio_thr": 1.15,
    "min_quote_volume": 1_000_000.0,
    "low_vol_atr_pct_thr": 0.004,
    "min_body_atr_for_low_vol": 0.4,
    "two_bar_confirm_enabled": True,
    "body_ratio_min": 0.25,
    "wick_ratio_max": 0.6,
    "levels_window": 120,
    "near_edge_window_pct": 0.15,
    "near_edge_atr_mult": 1.6,
    "slope_atr_window": 3,
    "slope_atr_min": 0.1,
    "debounce_sec": 90,
}

# (Directional/ATR опції з іншої системи — видалені як невживані)


# ── Screening loop ────────────────────────────────────────────────────────
DEFAULT_TIMEFRAME: Final[str] = "1m"
DEFAULT_LOOKBACK: Final[int] = 300
SCREENING_LOOKBACK: Final[int] = 300
SCREENING_BATCH_SIZE: Final[int] = 20
SCREENING_LEVELS_UPDATE_EVERY: Final[int] = 60
TRADE_REFRESH_INTERVAL: Final[int] = 30
MIN_READY_PCT: Final[float] = 0.6
REACTIVE_STAGE1: Final[bool] = False


# (Tick size helpers перенесені/не використовуються у цьому проєкті)


# (Trigger normalisation константи — видалені як невживані)
TRIGGER_TP_SL_SWAP_LONG: Final[str] = "tp_sl_swap_long"
TRIGGER_TP_SL_SWAP_SHORT: Final[str] = "tp_sl_swap_short"
TRIGGER_NAME_MAP: Final[dict[str, str]] = {}

# Tick size конфіг (мінімальні безпечні дефолти)
TICK_SIZE_MAP: Final[dict[str, float]] = {}
TICK_SIZE_BRACKETS: Final[list[tuple[float, float]]] = []
TICK_SIZE_DEFAULT: Final[float] = 0.01

BUY_SET: Final[frozenset[str]] = frozenset(
    {
        "BUY",
        "ACCUMULATE",
        "LONG",
        "BUY_SCALP",
    }
)
SELL_SET: Final[frozenset[str]] = frozenset(
    {
        "SELL",
        "DISTRIBUTE",
        "SHORT",
        "SELL_SCALP",
    }
)

# ── Redis interval TTL hints ──────────────────────────────────────────────
INTERVAL_TTL_MAP: Final[dict[str, int]] = {
    "1m": 120,
    "3m": 360,
    "5m": 600,
    "15m": 1_800,
    "30m": 3_600,
    "1h": 7_200,
    "4h": 21_600,
    "1d": 86_400,
}


# ── WebSocket gap recovery ────────────────────────────────────────────────
# Параметри бекфілу пропущених WS-даних (використовується ws_worker)
WS_GAP_BACKFILL: Final[dict[str, int | bool]] = {
    "enabled": True,
    "lookback_minutes": 60,
    "max_parallel": 4,
    # Максимальна кількість хвилин для бекфілу за один прохід (використовується ws_worker)
    "max_minutes": 10,
    # TTL для статусу ресинхронізації у Redis (секунди)
    "status_ttl": 900,
}
# Шлях у market_context для збереження статусу ресинхронізації WS-даних
WS_GAP_STATUS_PATH: Final[tuple[str, ...]] = ("selectors", "meta", "ws_gap_status")


# Тег версії порогів Stage2
USED_THRESH_TAG: Final[str] = "strict:v1"
# ─────────────────────────────────────────────────────────────────────────────
# Stage2: HTF гістерезис та daily range fallback
# ─────────────────────────────────────────────────────────────────────────────
STAGE2_HTF_ON_THRESH: Final[float] = 0.55
STAGE2_HTF_OFF_THRESH: Final[float] = 0.45
#: Масштаб нормалізації сили HTF (відносна зміна EMA за бар, що насичує strength=1.0)
STAGE2_HTF_STRENGTH_ALPHA: Final[float] = 0.0008
STAGE2_DAILY_LOW_FALLBACK_MUL: Final[float] = 0.99
STAGE2_DAILY_HIGH_FALLBACK_MUL: Final[float] = 1.01
# Мінімальна сила HTF для сценарних гейтів (канарейковий параметр)
SCEN_HTF_MIN: Final[float] = 0.02

# Сценарні гейти/діагностичні перемикачі (джерело правди — лише config)
# Тимчасові м'які значення для 15-хв канарейки на слабкому ринку:
SCEN_PULLBACK_PRESENCE_MIN: Final[float] = 0.30
SCEN_BREAKOUT_DVR_MIN: Final[float] = 0.30
SCEN_REQUIRE_BIAS: Final[bool] = False
SCEN_PULLBACK_ALLOW_NA: Final[bool] = True
TEST_SCENARIO_SELECTOR_RELAXED: Final[bool] = True

# ── Контекст/режими/гейти сценаріїв (опційні, під прапорами) ─────────────
# Снапшоти контексту (ContextMemory) у Redis (best-effort)
CONTEXT_SNAPSHOTS_ENABLED: Final[bool] = False
# BTC‑gate над сценарним резонером (посилювати/послабляти пороги залежно від режиму BTC)
SCENARIO_BTC_GATE_ENABLED: Final[bool] = True
# Множники порогів для BTC‑гейту (ефект застосовується поза Stage1/2/3 контрактами)
# Семантика: >1.0 посилює, <1.0 послаблює відповідний поріг
SCEN_BTC_GATE: Final[dict[str, dict[str, float]]] = {
    "ACCUM": {
        "presence_min_mult": 1.1,
        "htf_min_mult": 1.1,
        "dvr_min_mult": 1.0,
    },
    "EXPANSION_SETUP": {
        "presence_min_mult": 1.0,
        "htf_min_mult": 1.0,
        "dvr_min_mult": 1.0,
    },
    "BREAKOUT_UP": {
        "presence_min_mult": 0.95,
        "htf_min_mult": 0.95,
        "dvr_min_mult": 0.95,
    },
    "BREAKOUT_DOWN": {
        "presence_min_mult": 0.95,
        "htf_min_mult": 0.95,
        "dvr_min_mult": 0.95,
    },
}

# Додатковий м'який BTC‑gate (soft penalty) — незалежний прапор
SCEN_BTC_SOFT_GATE: Final[bool] = True
# Профілі чутливості для символів (можуть використовуватись у майбутніх версіях гейту)
SCEN_SENSITIVITY: Final[dict[str, str]] = {
    "BTCUSDT": "strict",
    "TONUSDT": "normal",
    "SNXUSDT": "normal",
}

# Контекстні м'які коефіцієнти для селектора (телеметрично, без зміни Stage1/2/3):
SCEN_CONTEXT_WEIGHTS_ENABLED: Final[bool] = False
CONTEXT_ALPHA_COMPRESSION: Final[float] = 0.10
CONTEXT_BETA_ZONE: Final[float] = 0.10
CONTEXT_GAMMA_BTC_TREND: Final[float] = 0.10

# Forward‑validation вікно (барів) для якості CSV (best‑effort у тулінгу)
FORWARD_K_BARS: Final[int] = 10

# ── Канарейка Stage3 (paper) ─────────────────────────────────────────────
# Увімкнути Stage3 у paper‑режимі без реальних угод (журнал у telemetry/reports)
STAGE3_PAPER_ENABLED: bool = True
# Набір символів канарейки (за замовчуванням 4 інструменти)
STAGE3_PAPER_SYMBOLS: list[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "TONUSDT",
    "SNXUSDT",
]
# Ризик‑ліміти (без зміни контрактів Stage3, застосовуються у раннері‑канарейці)
# Фіксуємо значення для canary_min_whale_v1.
RISK_PER_TRADE_R: float = 0.25
MAX_TRADES_PER_SYMBOL: int = 4
MAX_TRADES_PER_DAY: int = 12
START_MONEY_USD: float = 100.0
MARGINE_USD_PERCENT: float = 50.0

# ── SCEN_EXPLAIN логування (рейт‑ліміт і "кожен N-й батч") ─────────────
# Глобальний перемикач на пояснювальні логи селектора (без зміни контрактів)
SCEN_EXPLAIN_ENABLED: Final[bool] = True
# Додатковий "тонкий" слід — логувати кожний N‑й батч для символу, навіть якщо ще не минуло 10 с
SCEN_EXPLAIN_VERBOSE_EVERY_N: Final[int] = 20

# ── POLICY V2 (Stage2 політика прийняття сигналів) — під фіче‑флагом ──
# Базовий прапор увімкнення ядра рішень v2 (без зміни контрактів Stage1/2/3)
POLICY_V2_ENABLED: bool = True
# Канарейка: проброс сигналів Policy v2 у Stage3 paper‑раннер (без зміни контрактів)
POLICY_V2_TO_STAGE3_ENABLED: bool = False
# Мінімальні вимоги до ймовірності перемоги та очікуваного значення
POLICY_V2_MIN_P: float = 0.58
POLICY_V2_MIN_EV: float = 0.05
# Параметри рівнів за ATR (best‑effort): TP=k*ATR, SL=k*ATR
POLICY_V2_TP_ATR: float = 2.0
POLICY_V2_SL_ATR: float = 0.8
# Альтернативні/додаткові параметри для Policy v2 (зручні синоніми під план T0)
POLICY_V2_PWIN_MIN: float = POLICY_V2_MIN_P
POLICY_V2_LEAD_S_TARGET: int = 240
IMPULSE_VOL_MULT: float = 1.8
ACCUM_BAND_PCT_MAX: float = 0.015
MEANREV_HTF_MAX: float = 0.15
MEANREV_DIST_TO_LEVEL_MAX: float = 0.0015
COOLDOWN_S: int = 90
# Підтвердження імпульсу після sweep: множник до median20 5‑хв обсягу
SWEEP_CONFIRM_5M_VOL_MULT: float = 1.8
# Загальний cooldown FSM після відхилу (секунди)
FSM_COOLDOWN_S: int = 90

# ── MANIPULATION OBSERVER (Stage2 спостережний шар) — під фіче‑флагом ──
# Публікує лише підказки/пояснення у market_context.meta.manipulation та метрики Prometheus.
# Не впливає на рішення Stage2/Stage3.
MANIPULATION_OBSERVER_ENABLED: bool = True

# ── Whale presence: carry-forward on stale (age>7s) ─────────────────────────
# Якщо китова телеметрія позначена як stale, дозволяємо мʼяко перенести останнє
# відоме значення presence з попереднього стану, щоб уникнути нульового фолбеку.
# Контракти не змінюємо; лише заповнюємо stats.whale["presence"], якщо воно None.
WHALE_PRESENCE_STALE_CARRY_FORWARD_ENABLED: bool = True

# Проксі-реекспорт флагів Stage2-lite для зручності імпорту з config
STAGE2_ENABLED_FLAG: Final[bool] = STAGE2_ENABLED
STAGE2_PROFILE_NAME: Final[str] = STAGE2_PROFILE
EXH_STRATEGY_HINT_ENABLED_FLAG: Final[bool] = EXH_STRATEGY_HINT_ENABLED
PACK_EXHAUSTION_REVERSAL_ENABLED_FLAG: Final[bool] = PACK_EXHAUSTION_REVERSAL_ENABLED
TRAP_COOLDOWN_OVERRIDE_ENABLED_FLAG: Final[bool] = TRAP_COOLDOWN_OVERRIDE_ENABLED
TRAP_COOLDOWN_OVERRIDE_ENABLED_FLAG: Final[bool] = TRAP_COOLDOWN_OVERRIDE_ENABLED


# ── Named semaphores for Stage1 fetchers ──────────────────────────────────
DEPTH_SEMAPHORE: Final[asyncio.Semaphore] = asyncio.Semaphore(8)
KLINES_SEMAPHORE: Final[asyncio.Semaphore] = asyncio.Semaphore(5)
OI_SEMAPHORE: Final[asyncio.Semaphore] = asyncio.Semaphore(8)


__all__ = [
    "ASSET_STATE",
    "BUY_SET",
    "CACHE_TTL_DAYS",
    "DATASTORE_BASE_DIR",
    "TELEMETRY_BASE_DIR",
    "DEFAULT_LOOKBACK",
    "DEFAULT_TIMEFRAME",
    "DEPTH_SEMAPHORE",
    "FAST_SYMBOLS_TTL_AUTO",
    "FAST_SYMBOLS_TTL_MANUAL",
    "FilterParams",
    "INTERVAL_TTL_MAP",
    "KLINES_SEMAPHORE",
    "K_ANOMALY_DETECTION",
    "K_ATR_PCT_STAGE2",
    "K_ATR_VS_LOW_GATE_RATIO",
    "K_CONFIDENCE_METRICS",
    "K_CUMULATIVE_DELTA",
    "K_DIRECTIONAL_CONFLICT",
    "K_DIRECTIONAL_CONFLICT_STRENGTH",
    "K_DIRECTIONAL_VOLUME_RATIO",
    "K_LOW_VOLATILITY_FLAG",
    "K_MARKET_CONTEXT",
    "K_PRICE_SLOPE_ATR",
    "K_RECOMMENDATION",
    "K_RISK_PARAMETERS",
    "K_SIGNAL",
    "K_STATS",
    "K_SYMBOL",
    "K_THRESHOLDS",
    "K_TRIGGER_REASONS",
    "K_RAW_TRIGGER_REASONS",
    "TRIGGER_TP_SL_SWAP_LONG",
    "TRIGGER_TP_SL_SWAP_SHORT",
    "TRIGGER_NAME_MAP",
    "TICK_SIZE_MAP",
    "TICK_SIZE_BRACKETS",
    "TICK_SIZE_DEFAULT",
    "MANUAL_FAST_SYMBOLS_SEED",
    "MIN_READY_PCT",
    "MetricResults",
    "NAMESPACE",
    "NETWORK_DNS_CACHE_TTL_SEC",
    "OI_SEMAPHORE",
    "PREFILTER_BASE_PARAMS",
    "PREFILTER_INTERVAL_SEC",
    "PRELOAD_1M_LOOKBACK_INIT",
    "PRELOAD_DAILY_DAYS",
    "REACTIVE_STAGE1",
    "REDIS_CACHE_TTL",
    "REDIS_CHANNEL_ASSET_STATE",
    "REDIS_CHANNEL_UI_ASSET_STATE",
    "REDIS_SNAPSHOT_KEY",
    "REDIS_SNAPSHOT_UI_KEY",
    "STAGE3_EVENTS_LOG",
    "STAGE3_SKIPS_LOG",
    "STAGE3_STRATEGY_PROFILES",
    "ALERTS_QUALITY_LOG",
    "STAGE1_LATENCY_LOG",
    "STAGE3_TELEMETRY_ENABLED",
    "STAGE1_EVENTS_LOG",
    "STAGE1_TELEMETRY_ENABLED",
    "STAGE1_SYMBOL_SNAPSHOT_ENABLED",
    "STAGE1_SYMBOL_SNAPSHOT_INTERVAL_SEC",
    "STAGE1_SYMBOL_SNAPSHOT_ALERTS_ONLY",
    "TELEM_ENRICH_PHASE_PAYLOAD",
    "SCREENING_BATCH_SIZE",
    "SCREENING_LEVELS_UPDATE_EVERY",
    "SCREENING_LOOKBACK",
    "SELL_SET",
    "STAGE1_BEARISH_REASON_BONUS",
    "STAGE1_BEARISH_TRIGGER_TAGS",
    "STAGE1_METRICS_BATCH",
    "STAGE1_MONITOR_PARAMS",
    "DIRECTIONAL_PARAMS",
    "STAGE1_PREFILTER_HEAVY_LIMIT",
    "STAGE1_PREFILTER_THRESHOLDS",
    "STAGE1_TRAP",
    "STAGE1_TRAP_ENABLED",
    "STAGE1_TRAP_INFLUENCE_ENABLED",
    "STAGE1_TRAP_MARK_STRONG",
    "STAGE1_TRAP_STRONG_VOLZ_THR",
    "STAGE1_VOLUME_SPIKE_GATES",
    "STAGE2_STATUS",
    "STAGE2_LOW_ATR_SYMBOL_GUARDS",
    "STAGE2_PRESENCE_PROMOTION_GUARDS",
    "STAGE2_WHALE_TELEMETRY",
    "STAGE3_ZONES_THRESHOLD_OVERRIDES",
    "STRICT_PROFILE_ENABLED",
    "USE_VOL_ATR",
    "USE_RSI_DIV",
    "USE_VWAP_DEVIATION",
    "INSIGHT_LAYER_ENABLED",
    "INSIGHT_TELEMETRY_ONLY",
    "UI_WHALE_PUBLISH_ENABLED",
    "UI_INSIGHT_PUBLISH_ENABLED",
    "WHALE_THROTTLE_SEC",
    "STRICT_LOG_MARKERS",
    "SymbolInfo",
    "TRADE_REFRESH_INTERVAL",
    "VOLZ_PROXY_ALLOWED_FOR_EXHAUSTION",
    "WHALE_SCORING_V2_ENABLED",
    "WS_GAP_BACKFILL",
    "WS_GAP_STATUS_PATH",
    "UI_SNAPSHOT_TTL_SEC",
    "UI_PAYLOAD_SCHEMA_VERSION",
    "UI_TP_SL_FROM_STAGE3_ENABLED",
    "UI_USE_V2_NAMESPACE",
    "UI_DUAL_PUBLISH",
    "STATE_PUBLISH_META_ENABLED",
    "UI_SMART_PUBLISH_ENABLED",
    "UI_PUBLISH_MIN_INTERVAL_MS",
    "HEAVY_COMPUTE_GATING_ENABLED",
    "SOFT_RECOMMENDATIONS_ENABLED",
    "SOFT_RECO_SCORE_THR",
    "INSIGHT_DIRECTION_ONLY",
    "PROM_GAUGES_ENABLED",
    "PROM_HTTP_PORT",
    "STAGE2_ENABLED_FLAG",
    "STAGE2_PROFILE_NAME",
    "EXH_STRATEGY_HINT_ENABLED_FLAG",
    "PACK_EXHAUSTION_REVERSAL_ENABLED_FLAG",
    "TRAP_COOLDOWN_OVERRIDE_ENABLED_FLAG",
    "STAGE2_INSIGHT",
    "STAGE2_VOLATILITY_REGIME",
    "STAGE2_CRISIS_MODE",
    "STAGE2_RUNTIME",
    "SCEN_HTF_MIN",
    "SCEN_PULLBACK_PRESENCE_MIN",
    "SCEN_BREAKOUT_DVR_MIN",
    "SCEN_REQUIRE_BIAS",
    "SCEN_PULLBACK_ALLOW_NA",
    "TEST_SCENARIO_SELECTOR_RELAXED",
    "CONTEXT_SNAPSHOTS_ENABLED",
    "SCENARIO_BTC_GATE_ENABLED",
    "SCEN_BTC_GATE",
    "SCEN_CONTEXT_WEIGHTS_ENABLED",
    "CONTEXT_ALPHA_COMPRESSION",
    "CONTEXT_BETA_ZONE",
    "CONTEXT_GAMMA_BTC_TREND",
    "FORWARD_K_BARS",
    "SCEN_EXPLAIN_ENABLED",
    "SCEN_EXPLAIN_VERBOSE_EVERY_N",
    "ZONE_WINDOWS_STRICT_ENABLED",
    "ZONE_WINDOW_SHORT",
    "ZONE_WINDOW_LONG",
    # Policy v2 flags
    "POLICY_V2_ENABLED",
    "POLICY_V2_TO_STAGE3_ENABLED",
    "POLICY_V2_MIN_P",
    "POLICY_V2_MIN_EV",
    "POLICY_V2_TP_ATR",
    "POLICY_V2_SL_ATR",
    "POLICY_V2_PWIN_MIN",
    "POLICY_V2_LEAD_S_TARGET",
    "IMPULSE_VOL_MULT",
    "ACCUM_BAND_PCT_MAX",
    "MEANREV_HTF_MAX",
    "MEANREV_DIST_TO_LEVEL_MAX",
    "COOLDOWN_S",
    "SWEEP_CONFIRM_5M_VOL_MULT",
    "FSM_COOLDOWN_S",
    # Sweep→Retest→Bias FSM & Prom TTL
    "SWEEP_FSM_ENABLED",
    "SWEEP_RETEST_WINDOW_S",
    "SWEEP_REJECT_WINDOW_S",
    "RETEST_HTF_MIN",
    "RETEST_DVR_MIN",
    "RETEST_PROX_PCT",
    "REJECT_DRIFT_PCT",
    "CTX_BOOST",
    "CTX_SUPPRESS",
    "BIAS_TTL_S",
    "PROM_SWEEP_TTL_S",
]

# ── Пер-символьні профілі (YAML) ─────────────────────────────────────────
# Файл: config/symbol_profiles.yml (необов'язковий). Якщо відсутній або немає yaml — використовуються дефолти.


def load_symbol_profiles() -> dict[str, dict[str, float]]:
    from pathlib import Path

    prof_path = Path(__file__).resolve().parent / "symbol_profiles.yml"
    if not prof_path.exists():
        return {
            "defaults": {
                "presence_min_mult": 1.0,
                "htf_min_mult": 1.0,
                "dvr_min_mult": 1.0,
                "volz_max_mult": 1.0,
                "band_expand_min_mult": 1.0,
            }
        }
    try:  # спробуємо PyYAML, якщо встановлено
        import yaml  # type: ignore

        data = yaml.safe_load(prof_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        # Наївний парсер YAML key: value (тільки верхній рівень і мапи)
        out: dict[str, dict[str, float]] = {"defaults": {}}
        current: str | None = None
        for raw in prof_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.rstrip()
            if not line or line.strip().startswith("#"):
                continue
            if not line.startswith(" ") and ":" in line:
                key = line.split(":", 1)[0].strip()
                if key:
                    current = key
                    out.setdefault(current, {})
                continue
            if current and ":" in line:
                try:
                    k, v = line.strip().split(":", 1)
                    out[current][k.strip()] = float(v.strip())
                except Exception:
                    continue
        return out


SYMBOL_PROFILES = load_symbol_profiles()
