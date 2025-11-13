from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from config.config import STAGE1_TRAP
from config.flags import STAGE1_EXCLUDE_LOW_VOL_ATR_TRIGGERS
from monitoring.telemetry_sink import log_stage1_event
from utils.utils import normalize_result_types, sanitize_ohlcv_numeric

if TYPE_CHECKING:  # pragma: no cover
    from data.unified_store import UnifiedDataStore
    from stage1.asset_monitoring import AssetMonitorStage1


async def _load_frame(
    store: UnifiedDataStore,
    symbol: str,
    timeframe: str,
    lookback: int,
    *,
    logger: logging.Logger,
    lower_symbol: str,
) -> tuple[Any | None, dict[str, Any]]:
    """Завантажує та санітизує OHLCV-фрейм, повертає базові метрики останнього бара."""
    try:
        df = await store.get_df(symbol, timeframe, limit=int(lookback))
    except Exception as exc:
        logger.error(
            "[LOAD] %s: помилка store.get_df — %s", lower_symbol, exc, exc_info=False
        )
        return None, {}

    if df is None:
        logger.warning("[NO_DATA] %s: store.get_df повернув None", lower_symbol)
        return None, {}

    try:
        df_len = len(df)
        is_empty = bool(getattr(df, "empty", False))
    except Exception:
        df_len = 0
        is_empty = True

    if is_empty or df_len < 5:
        logger.warning(
            "[NO_DATA] %s: недостатньо даних (rows=%s)", lower_symbol, df_len
        )
        return None, {}

    try:
        cols = getattr(df, "columns", None)
        if cols is not None and "open_time" in cols and "timestamp" not in cols:
            df = df.rename(columns={"open_time": "timestamp"})
    except Exception:
        logger.debug(
            "[LOAD] %s: не вдалося перейменувати open_time→timestamp", lower_symbol
        )

    try:
        df = sanitize_ohlcv_numeric(
            df, logger_obj=logger, log_prefix=f"[{lower_symbol}] "
        )
    except Exception as exc:
        logger.error(
            "[LOAD] %s: sanitize_ohlcv_numeric впав — %s",
            lower_symbol,
            exc,
            exc_info=False,
        )
        return None, {}

    try:
        df_len = len(df)
        is_empty = bool(getattr(df, "empty", False))
    except Exception:
        df_len = 0
        is_empty = True

    if is_empty or df_len < 5:
        logger.warning(
            "[NO_DATA] %s: очищений фрейм порожній (rows=%s)", lower_symbol, df_len
        )
        return None, {}

    try:
        current_price = float(df["close"].iloc[-1])
    except Exception:
        current_price = None

    try:
        volume_last = float(df["volume"].iloc[-1])
    except Exception:
        volume_last = None

    last_ts_val: Any = None
    try:
        if "timestamp" in getattr(df, "columns", []):
            last_ts_val = df["timestamp"].iloc[-1]
    except Exception:
        last_ts_val = None

    base = {
        "current_price": current_price,
        "volume": volume_last,
        "timestamp": last_ts_val,
    }
    return df, base


@dataclass(slots=True)
class Stage1Context:
    symbol: str
    df: Any
    normalized: dict[str, Any]
    stats: dict[str, Any]


async def prepare_stage1_context(
    symbol: str,
    monitor: AssetMonitorStage1,
    store: UnifiedDataStore,
    timeframe: str,
    lookback: int,
    *,
    logger: logging.Logger,
    on_latency: Callable[[float], None] | None = None,
) -> Stage1Context | None:
    """Завантажує історію та виконує Stage1 аналіз для символу."""
    lower_symbol = symbol.lower()
    logger.debug(
        "[LOAD] %s: Завантаження даних через store.get_df",
        lower_symbol,
    )
    df, base = await _load_frame(
        store,
        symbol,
        timeframe,
        lookback,
        logger=logger,
        lower_symbol=lower_symbol,
    )
    if df is None:
        return None

    logger.debug("[STAGE1] %s: anomaly check", lower_symbol)
    signal: dict[str, Any] | Any
    t0 = time.perf_counter()
    try:
        signal = await monitor.check_anomalies(symbol, df)
    finally:
        if on_latency is not None:
            try:
                on_latency((time.perf_counter() - t0) * 1000.0)
            except Exception:
                pass
    if not isinstance(signal, dict):
        logger.warning("[STAGE1] %s: повернув не dict", lower_symbol)
        signal = {"symbol": lower_symbol, "signal": "NONE", "stats": {}}

    stats_container = signal.get("stats") or {}
    if not isinstance(stats_container, dict):
        stats_container = {}
        signal["stats"] = stats_container

    for key, value in base.items():
        if value is not None:
            stats_container.setdefault(key, value)

    normalized = normalize_result_types(signal)
    norm_stats = normalized.get("stats")

    try:
        trig = normalized.get("trigger_reasons")
        raw_trig = normalized.get("raw_trigger_reasons")
        if bool(STAGE1_EXCLUDE_LOW_VOL_ATR_TRIGGERS):
            trig = _filter_low_atr(trig)
            raw_trig = _filter_low_atr(raw_trig)

        await log_stage1_event(
            event="stage1_signal",
            symbol=lower_symbol,
            payload={
                "signal": str(normalized.get("signal")),
                "trigger_reasons": trig,
                "raw_trigger_reasons": raw_trig,
                "stats": normalized.get("stats"),
            },
        )
    except Exception:
        logger.debug("[TELEM] %s: stage1_signal log failed", lower_symbol)

    try:
        trap_score = None
        try:
            trap_score = float((normalized.get("stats") or {}).get("trap_score"))
        except Exception:
            trap_score = None
        if isinstance(trap_score, (int, float)):
            fired = bool(
                trap_score >= float((STAGE1_TRAP or {}).get("score_gate", 0.67))
            )
            await log_stage1_event(
                event="trap_eval",
                symbol=lower_symbol,
                payload={
                    "score": float(trap_score),
                    "fired": fired,
                    "reasons": normalized.get("trigger_reasons"),
                },
            )
    except Exception:
        logger.debug("[TELEM] %s: trap_eval log failed", lower_symbol)

    if not isinstance(norm_stats, dict):
        normalized["stats"] = stats_container
    else:
        for key, value in stats_container.items():
            norm_stats.setdefault(key, value)

    return Stage1Context(
        symbol=lower_symbol,
        df=df,
        normalized=normalized,
        stats=normalized.get("stats") or {},
    )


def _filter_low_atr(value: Any) -> Any:
    if isinstance(value, list):
        return [
            item
            for item in value
            if isinstance(item, str) and item not in ("low_volatility", "low_atr")
        ]
    return value
