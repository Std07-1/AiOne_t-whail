import builtins
from typing import Any, Literal, TypedDict

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

STAGE2_PROFILE: Literal["strict", "legacy"]
STRICT_PROFILE_ENABLED: bool
WHALE_SCORING_V2_ENABLED: bool

INSIGHT_LAYER_ENABLED: bool
INSIGHT_TELEMETRY_ONLY: bool
PROCESSOR_INSIGHT_WIREUP: bool
VOLZ_PROXY_ALLOWED_FOR_EXHAUSTION: bool
UI_WHALE_PUBLISH_ENABLED: bool
UI_INSIGHT_PUBLISH_ENABLED: bool
WHALE_THROTTLE_SEC: int
STRICT_LOG_MARKERS: bool

USED_THRESH_TAG: str
STRONG_REGIME: StrongRegimeConfig

# Stage2 volatility/crisis/runtime configs
STAGE2_VOLATILITY_REGIME: dict[str, float]
STAGE2_CRISIS_MODE: dict[str, Any]
STAGE2_RUNTIME: dict[str, Any]
STAGE2_PRESENCE_PROMOTION_GUARDS: dict[str, float]
STAGE2_LOW_ATR_SYMBOL_GUARDS: dict[str, dict[str, float]]
STAGE3_ZONES_THRESHOLD_OVERRIDES: dict[str, dict[str, float | int | str]]
STRICT_ACCUM_CAPS_ENABLED: bool
STRICT_HTF_GRAY_GATE_ENABLED: bool
STRICT_ZONE_RISK_CLAMP_ENABLED: bool
STRICT_LOW_ATR_OVERRIDE_ON_SPIKE: bool
PRESENCE_CAPS: dict[str, float]
HTF_GATES: dict[str, float]
LOW_ATR_SPIKE_OVERRIDE: dict[str, float | int]
PROM_GAUGES_ENABLED: bool
PROM_HTTP_PORT: int
STATE_PUBLISH_META_ENABLED: bool

NAMESPACE: str
DATASTORE_BASE_DIR: str
TELEMETRY_BASE_DIR: str
STAGE3_EVENTS_LOG: str
STAGE3_TELEMETRY_ENABLED: bool
STAGE3_STALE_TELEMETRY_COOLDOWN_SEC: float
STAGE3_PREDICTED_PROFIT_SCALE: float
DEFAULT_LOOKBACK: int
DEFAULT_TIMEFRAME: str
MIN_READY_PCT: float
MAX_PARALLEL_STAGE2: int
TRADE_REFRESH_INTERVAL: int
STAGE2_TASK_TIMEOUT: int

BUY_SET: set[str]
SELL_SET: set[str]

REDIS_CHANNEL_ASSET_STATE: str
REDIS_SNAPSHOT_KEY: str
REDIS_CHANNEL_UI_ASSET_STATE: str
REDIS_SNAPSHOT_UI_KEY: str
ADMIN_COMMANDS_CHANNEL: str
STATS_CORE_KEY: str
STATS_HEALTH_KEY: str

# Core document (Stage3 metrics) and JSON paths
REDIS_DOC_CORE: str
REDIS_CORE_PATH_TRADES: str
REDIS_CORE_PATH_STATS: str
REDIS_CORE_PATH_HEALTH: str

# TTLs and feature toggles for migration
UI_SNAPSHOT_TTL_SEC: int
CORE_TTL_SEC: int
CORE_DUAL_WRITE_OLD_STATS: bool
UI_PAYLOAD_SCHEMA_VERSION: str
UI_TP_SL_FROM_STAGE3_ENABLED: bool
UI_USE_V2_NAMESPACE: bool
UI_DUAL_PUBLISH: bool
SIMPLE_UI_MODE: bool

UI_LOCALE: str
UI_COLUMN_VOLUME: str
TICK_SIZE_MAP: dict[str, float]
TICK_SIZE_BRACKETS: list[tuple[float, float]]
TICK_SIZE_DEFAULT: float

STAGE2_STATUS: dict[str, str]
ASSET_STATE: dict[str, str]

# Canonical keys across pipeline (Stage1 → Stage2 → UI)
K_SYMBOL: str
K_SIGNAL: str
K_TRIGGER_REASONS: str
K_RAW_TRIGGER_REASONS: str
K_STATS: str
K_THRESHOLDS: str

# Stage2 output keys
K_MARKET_CONTEXT: str
K_RECOMMENDATION: str
K_CONFIDENCE_METRICS: str
K_ANOMALY_DETECTION: str
K_RISK_PARAMETERS: str

TRIGGER_TP_SL_SWAP_LONG: str
TRIGGER_TP_SL_SWAP_SHORT: str
TRIGGER_SIGNAL_GENERATED: str
TRIGGER_NAME_MAP: dict[str, str]

# Semaphores (runtime types), kept as Any for typing simplicity
OI_SEMAPHORE: Any
KLINES_SEMAPHORE: Any
DEPTH_SEMAPHORE: Any
REDIS_CACHE_TTL: int

class AssetFilterError(Exception): ...

class SymbolInfo:
    symbol: str
    status: str
    base_asset: str
    quote_asset: str
    contract_type: str
    def dict(self, *args: Any, **kwargs: Any) -> builtins.dict[str, Any]: ...
    def __init__(
        self,
        *,
        symbol: str,
        status: str,
        base_asset: str,
        quote_asset: str,
        contract_type: str,
        **kwargs: Any,
    ) -> None: ...

class FilterParams:
    min_quote_volume: float
    min_price_change: float
    min_open_interest: float
    min_orderbook_depth: float
    min_atr_percent: float
    max_symbols: int
    dynamic: bool
    strict_validation: bool
    def dict(self, *args: Any, **kwargs: Any) -> builtins.dict[str, Any]: ...
    def __init__(
        self,
        *,
        min_quote_volume: float,
        min_price_change: float,
        min_open_interest: float,
        min_orderbook_depth: float,
        min_atr_percent: float,
        max_symbols: int,
        dynamic: bool = ...,
        strict_validation: bool = ...,
        **kwargs: Any,
    ) -> None: ...

class MetricResults:
    initial_count: int
    prefiltered_count: int
    filtered_count: int
    result_count: int
    elapsed_time: float
    params: builtins.dict[str, Any]
    def dict(self, *args: Any, **kwargs: Any) -> builtins.dict[str, Any]: ...
    def __init__(
        self,
        *,
        initial_count: int,
        prefiltered_count: int,
        filtered_count: int,
        result_count: int,
        elapsed_time: float,
        params: builtins.dict[str, Any],
        **kwargs: Any,
    ) -> None: ...

ASSET_CLASS_MAPPING: dict[str, list[str]]
STAGE2_CONFIG: dict[str, Any]
OPTUNA_PARAM_RANGES: dict[str, tuple]
STAGE2_RANGE_PARAMS: dict[str, float]
STAGE2_GATE_PARAMS: dict[str, float]
STAGE2_ELITE_RULES: dict[str, Any]
STAGE2_LOW_VOL_ADAPT: dict[str, float]
STAGE2_RISK_STRATEGY_SWITCHES: dict[str, Any]
STAGE3_TRADE_PARAMS: dict[str, Any]
STAGE3_SYMBOL_PARAM_OVERRIDES: dict[str, dict[str, Any]]
STAGE3_FEED_STALE_SECONDS: float
STAGE3_FORCE_CLOSE_STALE_SECONDS: float
STAGE3_STALE_WARN_COOLDOWN_SEC: float
STAGE3_STALE_FORCE_CYCLES: int
STAGE3_SKIP_STREAK_WARN_CYCLES: int
STAGE3_PRICE_STALE_THRESHOLD_SEC: float
STAGE3_HEALTH_CHECK_INTERVAL_SEC: float
NETWORK_DNS_CACHE_TTL_SEC: float
PRICE_STREAM_SERVICE_CFG: dict[str, float | int | bool]
STAGE3_HEALTH_CFG: dict[str, Any]
STAGE2_AUDIT: dict[str, float | int | str | bool]
WS_GAP_BACKFILL: dict[str, int | bool]
WS_GAP_STATUS_PATH: tuple[str, ...]

INTERVAL_TTL_MAP: dict[str, int]

STAGE3_SHADOW_PIPELINE_ENABLED: bool
STAGE3_SHADOW_NAMESPACE: str
STAGE3_SHADOW_STREAMS: dict[str, str]
STAGE3_SHADOW_STREAM_CFG: dict[str, Any]
STAGE3_SHADOW_PRICE_STREAM_CFG: dict[str, Any]
STAGE3_SHADOW_EXECUTION_LIMITS: dict[str, float]
STAGE3_SHADOW_DUAL_WRITE_ENABLED: bool
STAGE3_SHADOW_GLOBAL_ROLLOUT: bool
STAGE3_SHADOW_CANARY_SYMBOLS: set[str]

STAGE1_PREFILTER_THRESHOLDS: dict[str, float | int]
STAGE1_METRICS_BATCH: int
STAGE1_PREFILTER_HEAVY_LIMIT: int
MANUAL_FAST_SYMBOLS_SEED: list[str]
USER_SETTINGS_DEFAULT: dict[str, str]
PRELOAD_1M_LOOKBACK_INIT: int
SCREENING_LOOKBACK: int
PRELOAD_DAILY_DAYS: int
FAST_SYMBOLS_TTL_MANUAL: int
FAST_SYMBOLS_TTL_AUTO: int
PREFILTER_INTERVAL_SEC: int
CACHE_TTL_DAYS: int
OPTUNA_SQLITE_URI: str
MIN_CONFIDENCE_TRADE_PLACEHOLDER: float
STAGE1_MONITOR_PARAMS: dict[str, float | int]
PREFILTER_BASE_PARAMS: dict[str, float | int | bool]

# Feature flags for Stage1 logic
USE_VOL_ATR: bool
USE_RSI_DIV: bool
USE_VWAP_DEVIATION: bool
STAGE1_BEARISH_TRIGGER_TAGS: tuple[str, ...]
STAGE1_BEARISH_REASON_BONUS: bool

def get_stage3_param(symbol: str, name: str, default: Any) -> Any: ...

__all__: list[str]
