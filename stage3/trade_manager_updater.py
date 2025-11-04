"""Stage3 utility task: periodic trade state refresh.

Оновлює активні угоди, підтягує агреговані статистики зі Stage1 замість
сирих барів (ATR/RSI/Volume) і логгує кількість active/closed.

"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from app.settings import load_datastore_cfg
from config.config import (
    CORE_DUAL_WRITE_OLD_STATS,
    CORE_TTL_SEC,
    REDIS_CORE_PATH_HEALTH,
    REDIS_CORE_PATH_STATS,
    REDIS_CORE_PATH_TRADES,
    REDIS_DOC_CORE,
    STAGE3_FEED_STALE_SECONDS,
    STAGE3_FORCE_CLOSE_STALE_SECONDS,
    STAGE3_HEALTH_CHECK_INTERVAL_SEC,
    STAGE3_PRICE_STALE_THRESHOLD_SEC,
    STAGE3_SKIP_STREAK_WARN_CYCLES,
    STAGE3_STALE_FORCE_CYCLES,
    STAGE3_STALE_TELEMETRY_COOLDOWN_SEC,
    STAGE3_STALE_WARN_COOLDOWN_SEC,
)
from monitoring.telemetry_sink import log_stage3_event
from stage1.asset_monitoring import AssetMonitorStage1
from stage3.price_stream_service import PriceUpdate
from stage3.trade_manager import TradeLifecycleManager
from stage3.trade_update_service import TradeUpdateService
from utils.utils import safe_float

# ── Logger ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("stage3.trade_manager_updater")
if not logger.handlers:  # guard щоб не дублювати хендлери
    logger.setLevel(logging.INFO)
    try:  # optional rich
        logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    except Exception:  # broad except: rich може бути недоступний у середовищі
        logger.addHandler(logging.StreamHandler())
    logger.propagate = False


class _UpdaterHealthTracker:
    """Відстежує стан health Stage3 updater без зовнішніх залежностей."""

    def __init__(
        self,
        price_stale_threshold: float,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._clock = clock or time.time
        self._price_stale_threshold = max(float(price_stale_threshold), 1.0)
        self._symbol_refresh_wall: dict[str, float] = {}
        self._symbol_price_ts: dict[str, float] = {}
        self._cycle_count = 0
        self._cycle_avg = 0.0
        self._cycle_max = 0.0

    def record_refresh(
        self,
        symbol: str,
        *,
        data_ts: float | None = None,
        wall_ts: float | None = None,
    ) -> None:
        """Фіксує успішне оновлення символа з даними."""

        if not symbol:
            return
        now_wall = wall_ts if wall_ts is not None else self._clock()
        self._symbol_refresh_wall[symbol] = now_wall
        if data_ts is not None and data_ts > 0:
            self._symbol_price_ts[symbol] = float(data_ts)
        else:
            self._symbol_price_ts[symbol] = now_wall

    def record_cycle(self, duration: float) -> None:
        """Оновлює статистику тривалості циклів."""

        if duration < 0:
            return
        self._cycle_count += 1
        if self._cycle_count == 1:
            self._cycle_avg = duration
        else:
            self._cycle_avg += (duration - self._cycle_avg) / self._cycle_count
        if duration > self._cycle_max:
            self._cycle_max = duration

    def stale_symbols(self, now_ts: float | None = None) -> dict[str, float]:
        """Повертає символи зі строком давності даних понад поріг."""

        current_ts = now_ts if now_ts is not None else self._clock()
        stale: dict[str, float] = {}
        threshold = self._price_stale_threshold
        for symbol, price_ts in self._symbol_price_ts.items():
            age = current_ts - price_ts
            if age >= threshold:
                stale[symbol] = age
        return stale

    def to_payload(
        self,
        *,
        cycle_elapsed: float,
        skip_ewma: float,
        skipped: int,
        pressure_ratio: float,
        active_trades: int,
    ) -> dict[str, Any]:
        """Будує агрегований health-пейлоад для телеметрії/логів."""

        now_ts = self._clock()
        stale_map = self.stale_symbols(now_ts)
        worst_symbol: str | None = None
        worst_age: float | None = None
        for symbol, age in stale_map.items():
            if worst_age is None or age > worst_age:
                worst_symbol = symbol
                worst_age = age
        stale_sample = [
            {
                "symbol": symbol,
                "age_sec": round(age, 1),
            }
            for symbol, age in sorted(
                stale_map.items(), key=lambda item: item[1], reverse=True
            )[:3]
        ]
        payload = {
            "timestamp": now_ts,
            "cycle_elapsed": round(cycle_elapsed, 4),
            "cycle_avg": round(self._cycle_avg, 4),
            "cycle_max": round(self._cycle_max, 4),
            "cycle_count": self._cycle_count,
            "skipped": skipped,
            "skipped_ewma": round(skip_ewma, 4),
            "pressure": round(pressure_ratio, 4),
            "active_trades": active_trades,
            "stale_total": len(stale_map),
            "stale_sample": stale_sample,
        }
        if worst_symbol is not None and worst_age is not None:
            payload["worst_stale"] = {
                "symbol": worst_symbol,
                "age_sec": round(worst_age, 1),
            }
        return payload


def _build_trail_summary(
    trade_entry: Mapping[str, Any], entry_price: float | None
) -> dict[str, Any] | None:
    """Нормалізує трейлінг-контекст угоди для UI."""

    context_obj = (
        trade_entry.get("context") if isinstance(trade_entry, Mapping) else None
    )
    trail_ctx: Mapping[str, Any] | None = None
    if isinstance(context_obj, Mapping):
        raw_trail = context_obj.get("trail")
        if isinstance(raw_trail, Mapping):
            trail_ctx = raw_trail
    if trail_ctx is None:
        fallback_trail = trade_entry.get("trail")
        if isinstance(fallback_trail, Mapping):
            trail_ctx = fallback_trail
    if not isinstance(trail_ctx, Mapping) or not trail_ctx:
        return None

    summary: dict[str, Any] = {}
    armed = bool(trail_ctx.get("armed"))
    state_val = trail_ctx.get("state")
    if isinstance(state_val, str) and state_val:
        summary["state"] = state_val.lower()
    else:
        summary["state"] = "armed" if armed else "idle"
    summary["armed"] = armed
    if "locked" in trail_ctx:
        summary["locked"] = bool(trail_ctx.get("locked"))
    if "allow_break_even" in trail_ctx:
        summary["allow_break_even"] = bool(trail_ctx.get("allow_break_even"))
    if "armed_ts" in trail_ctx:
        summary["armed_ts"] = str(trail_ctx.get("armed_ts"))
    first_sl = safe_float(trail_ctx.get("first_sl"))
    if first_sl is not None:
        summary["first_sl"] = first_sl
    trigger_val = safe_float(trail_ctx.get("trigger"))
    if trigger_val is not None:
        summary["trigger"] = trigger_val
    buffer_val = safe_float(trail_ctx.get("last_buffer"))
    if buffer_val is not None:
        summary["buffer_abs"] = round(buffer_val, 6)
        if entry_price is not None and entry_price > 0:
            try:
                summary["buffer_pct"] = round(buffer_val / entry_price * 100.0, 3)
            except Exception:
                pass
    last_move = safe_float(trail_ctx.get("last_move_atr"))
    if last_move is not None:
        summary["move_atr"] = round(last_move, 3)
    distance_pct = safe_float(trail_ctx.get("distance_pct"))
    if distance_pct is not None:
        summary["distance_pct"] = round(distance_pct, 3)
    if not summary:
        return None
    return summary


def _collect_stage3_ui_data(
    active_trades: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, Any]]:
    """Готує targets та телеметрію Stage3 для UI-snapshot."""

    targets: dict[str, dict[str, Any]] = {}
    telemetry: dict[str, dict[str, Any]] = {}
    rr_samples: list[float] = []
    unrealized_samples: list[float] = []
    mfe_samples: list[float] = []
    mae_samples: list[float] = []
    trails_total = 0
    trails_armed = 0

    for trade in active_trades:
        symbol_raw = trade.get("symbol") if isinstance(trade, Mapping) else None
        symbol = str(symbol_raw).upper() if symbol_raw else ""
        if not symbol:
            continue

        tp_val = safe_float(trade.get("tp"))
        sl_val = safe_float(trade.get("sl"))
        entry_price = safe_float(trade.get("entry_price"))
        current_price = safe_float(trade.get("current_price"))
        rr_val = safe_float(trade.get("rr_ratio"))

        if tp_val is not None and sl_val is not None and tp_val > 0 and sl_val > 0:
            target_record: dict[str, Any] = {"tp": float(tp_val), "sl": float(sl_val)}
            context = trade.get("context") if isinstance(trade, Mapping) else None
            trail_ctx = context.get("trail") if isinstance(context, Mapping) else None
            target_record["trail_armed"] = bool(
                isinstance(trail_ctx, Mapping) and trail_ctx.get("armed")
            )
            if rr_val is None and entry_price is not None:
                try:
                    reward = abs(tp_val - entry_price)
                    risk = abs(entry_price - sl_val)
                    rr_val = reward / risk if risk > 0 else None
                except Exception:
                    rr_val = None
            if rr_val is not None:
                target_record["rr"] = round(float(rr_val), 3)
            targets[symbol] = target_record

        telemetry_entry: dict[str, Any] = {
            "symbol": symbol,
            "state": str(trade.get("status", "unknown")),
        }
        if entry_price is not None:
            telemetry_entry["entry"] = entry_price
        if current_price is not None:
            telemetry_entry["current"] = current_price
        if tp_val is not None:
            telemetry_entry["tp"] = float(tp_val)
        if sl_val is not None:
            telemetry_entry["sl"] = float(sl_val)

        computed_rr = rr_val
        if (
            computed_rr is None
            and entry_price is not None
            and sl_val is not None
            and tp_val is not None
        ):
            try:
                reward = abs(tp_val - entry_price)
                risk = abs(entry_price - sl_val)
                computed_rr = reward / risk if risk > 0 else None
            except Exception:
                computed_rr = None
        if computed_rr is not None:
            rr_round = float(round(computed_rr, 4))
            telemetry_entry["rr_ratio"] = rr_round
            rr_samples.append(rr_round)

        side = str(trade.get("side") or "buy").lower()
        if entry_price is not None and entry_price != 0.0 and current_price is not None:
            try:
                if side == "sell":
                    unrealized = (entry_price - current_price) / entry_price * 100.0
                else:
                    unrealized = (current_price - entry_price) / entry_price * 100.0
                unrealized_round = float(round(unrealized, 3))
                telemetry_entry["unrealized_pct"] = unrealized_round
                unrealized_samples.append(unrealized_round)
            except Exception:
                pass

        mfe_val = safe_float(trade.get("mfe_pct"))
        if mfe_val is not None:
            mfe_round = float(round(mfe_val, 3))
            telemetry_entry["mfe_pct"] = mfe_round
            mfe_samples.append(mfe_round)

        mae_val = safe_float(trade.get("mae_pct"))
        if mae_val is not None:
            mae_round = float(round(mae_val, 3))
            telemetry_entry["mae_pct"] = mae_round
            mae_samples.append(mae_round)

        trail_summary = _build_trail_summary(trade, entry_price)
        if trail_summary:
            telemetry_entry["trail"] = trail_summary
            trails_total += 1
            if trail_summary.get("state") == "armed" or trail_summary.get("armed"):
                trails_armed += 1

        telemetry[symbol] = telemetry_entry

    meta = {
        "avg_rr": float(sum(rr_samples) / len(rr_samples)) if rr_samples else None,
        "avg_unrealized": (
            float(sum(unrealized_samples) / len(unrealized_samples))
            if unrealized_samples
            else None
        ),
        "avg_mfe": float(sum(mfe_samples) / len(mfe_samples)) if mfe_samples else None,
        "avg_mae": float(sum(mae_samples) / len(mae_samples)) if mae_samples else None,
        "trails_total": trails_total,
        "trails_armed": trails_armed,
    }
    return targets, telemetry, meta


def _resolve_price_from_update(update: PriceUpdate) -> float | None:
    """Витягує коректну ціну з PriceUpdate (last/bid/ask)."""

    for candidate in (update.last_price, update.bid, update.ask):
        if candidate is None:
            continue
        try:
            price = float(candidate)
        except Exception:
            continue
        if price > 0.0:
            return price
    return None


async def trade_manager_updater(
    trade_manager: TradeLifecycleManager,
    store: Any,
    monitor: AssetMonitorStage1,
    trade_update_service: TradeUpdateService,
    timeframe: str = "1m",
    lookback: int = 20,
    interval_sec: int = 30,
    log_interval_sec: int | None = None,
    log_on_change: bool = True,
    max_backoff_sec: int = 300,
    backoff_multiplier: float | None = None,
    publish_ui: bool = True,
    ui_ttl: int = 90,
    skipped_ewma_alpha: float | None = None,
):
    """Фонове оновлення стану угод.

    Параметри:
        timeframe: таймфрейм барів для оновлення метрик.
        lookback: скільки барів брати для розрахунку поточних stats.
        interval_sec: базовий інтервал циклу (poll).
        log_interval_sec: мінімальний інтервал між логами (override log_on_change).
        log_on_change: логувати тільки якщо змінилась кількість активних/закритих.
        trade_update_service: сервіс, що постачає live-оновлення цін.
    """
    last_log_ts = 0.0
    last_counts: tuple[int, int] | None = None
    # Rate-limit для зведеного рядка Active/Closed
    _hb_sec = 30.0
    dynamic_interval = float(interval_sec)
    skipped_symbols = 0
    skip_reason_counts: dict[str, int] = {}
    last_warn_drift_ts = 0.0
    last_warn_pressure_ts = 0.0
    skip_streaks: dict[str, int] = {}
    skip_last_alert: dict[str, float] = {}
    health_tracker = _UpdaterHealthTracker(STAGE3_PRICE_STALE_THRESHOLD_SEC)
    last_health_event_ts = 0.0

    # Centralized config (best-effort)
    try:
        ds_cfg = load_datastore_cfg()
        tu_cfg = ds_cfg.trade_updater
    except Exception:
        tu_cfg = None
    if backoff_multiplier is None:
        backoff_multiplier = getattr(tu_cfg, "backoff_multiplier", 1.5)
    max_backoff_sec = getattr(tu_cfg, "max_backoff_sec", max_backoff_sec)

    # Метрики Prometheus видалено; зберігаємо локальні змінні стану
    skipped_ewma: float = 0.0
    # smoothing factor (конфігурований): або параметр, або ENV TRADE_UPDATER_SKIPPED_ALPHA, дефолт 0.3
    if skipped_ewma_alpha is None:
        try:
            import os

            env_val = os.getenv("TRADE_UPDATER_SKIPPED_ALPHA")
            if env_val is not None:
                skipped_ewma_alpha = float(env_val)
            elif tu_cfg is not None:
                skipped_ewma_alpha = float(getattr(tu_cfg, "skipped_ewma_alpha", 0.3))
            else:
                skipped_ewma_alpha = 0.3
        except Exception:
            skipped_ewma_alpha = 0.3
    skipped_alpha = max(0.01, min(0.95, float(skipped_ewma_alpha)))

    feed_stale_seconds_raw = getattr(tu_cfg, "feed_stale_seconds", None)
    feed_stale_seconds = safe_float(feed_stale_seconds_raw)
    if feed_stale_seconds is None:
        feed_stale_seconds = STAGE3_FEED_STALE_SECONDS

    force_close_seconds_raw = getattr(tu_cfg, "feed_force_close_seconds", None)
    force_close_seconds = safe_float(force_close_seconds_raw)
    if force_close_seconds is None:
        force_close_seconds = STAGE3_FORCE_CLOSE_STALE_SECONDS

    warn_cooldown_raw = getattr(tu_cfg, "feed_stale_warn_cooldown_sec", None)
    warn_cooldown_sec = safe_float(warn_cooldown_raw)
    if warn_cooldown_sec is None:
        warn_cooldown_sec = STAGE3_STALE_WARN_COOLDOWN_SEC

    force_cycles_val = getattr(tu_cfg, "feed_stale_force_cycles", None)
    try:
        force_cycles = (
            int(force_cycles_val)
            if force_cycles_val is not None
            else STAGE3_STALE_FORCE_CYCLES
        )
    except (TypeError, ValueError):
        force_cycles = STAGE3_STALE_FORCE_CYCLES

    feed_stale_seconds = max(1.0, feed_stale_seconds)
    force_close_seconds = max(force_close_seconds, feed_stale_seconds + 1.0)
    warn_cooldown_sec = max(5.0, warn_cooldown_sec)
    force_cycles = max(1, force_cycles)
    skip_warn_cycles_raw = getattr(tu_cfg, "skip_streak_warn_cycles", None)
    try:
        skip_warn_cycles = (
            int(skip_warn_cycles_raw)
            if skip_warn_cycles_raw is not None
            else STAGE3_SKIP_STREAK_WARN_CYCLES
        )
    except (TypeError, ValueError):
        skip_warn_cycles = STAGE3_SKIP_STREAK_WARN_CYCLES
    skip_warn_cycles = max(1, skip_warn_cycles)
    health_reasons: set[str] = {"no_price", "insufficient_bars"}

    def _normalize_epoch_ts(value: Any | None) -> float | None:
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            try:
                value = value.to_pydatetime()
            except Exception:  # pragma: no cover - fallback для нестандартних типів
                return None
        if isinstance(value, datetime):
            try:
                if value.tzinfo is None:
                    value = value.replace(tzinfo=UTC)
                return value.timestamp()
            except (TypeError, ValueError):
                return None
        if hasattr(value, "timestamp") and not isinstance(value, (int, float, str)):
            try:
                value = value.timestamp()
            except Exception:
                return None
        ts = safe_float(value)
        if ts is None:
            return None
        if ts > 1e12:
            ts /= 1000.0
        elif ts > 1e10:
            ts /= 1000.0
        if ts <= 0:
            return None
        return ts

    def _parse_iso8601(value: Any | None) -> float | None:
        if not isinstance(value, str) or not value:
            return None
        try:
            cleaned = value.replace("Z", "+00:00")
            dt_obj = datetime.fromisoformat(cleaned)
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=UTC)
            return dt_obj.timestamp()
        except (ValueError, TypeError):
            return None

    def _extract_last_bar_ts(frame: pd.DataFrame | None) -> float | None:
        if frame is None or frame.empty:
            return None
        for column in ("timestamp", "close_time", "open_time"):
            if column in frame.columns:
                try:
                    raw_val = frame[column].iloc[-1]
                except Exception:
                    continue
                ts_val = _normalize_epoch_ts(raw_val)
                if ts_val is not None:
                    return ts_val
        if isinstance(frame.index, pd.DatetimeIndex) and len(frame.index):
            return _normalize_epoch_ts(frame.index[-1])
        return None

    def _should_force_close_stale(
        trade_entry: Mapping[str, Any], price_val: Any
    ) -> tuple[bool, str]:
        price = safe_float(price_val)
        if price is None or price <= 0.0:
            return True, "no_price"

        entry_price = safe_float(trade_entry.get("entry_price"))
        sl_val = safe_float(trade_entry.get("sl"))
        tp_val = safe_float(trade_entry.get("tp"))
        side = str(trade_entry.get("side") or "buy").lower()
        eps_ratio = 0.0005  # 0.05% толеранс на округлення

        if sl_val is not None and sl_val > 0.0:
            if side == "buy":
                if price <= sl_val * (1.0 + eps_ratio):
                    return True, "price_below_sl"
            else:
                if price >= sl_val * (1.0 - eps_ratio):
                    return True, "price_above_sl"

        if tp_val is not None and tp_val > 0.0:
            if side == "buy":
                if price >= tp_val * (1.0 - eps_ratio):
                    return True, "price_above_tp"
            else:
                if price <= tp_val * (1.0 + eps_ratio):
                    return True, "price_below_tp"

        mae_pct = safe_float(trade_entry.get("mae_pct"))
        if mae_pct is not None and mae_pct >= 4.0:
            return True, "mae_pct>=4"

        if entry_price is not None and entry_price > 0.0:
            try:
                if side == "buy":
                    unrealized_pct = (price - entry_price) / entry_price * 100.0
                else:
                    unrealized_pct = (entry_price - price) / entry_price * 100.0
            except Exception:
                unrealized_pct = None
            if unrealized_pct is not None and unrealized_pct <= -3.0:
                return True, "drawdown>3pct"

        return False, "under_control"

    stale_counters: dict[str, int] = {}
    stale_warn_ts: dict[str, float] = {}
    stale_guard_ts: dict[str, float] = {}
    stale_active_since: dict[str, float] = {}
    stale_last_telemetry_ts: dict[str, float] = {}
    telemetry_cooldown = max(5.0, float(STAGE3_STALE_TELEMETRY_COOLDOWN_SEC))

    # локальний кеш створених Gauge щоб уникнути повторної реєстрації
    # Видалено _register_gauge та всі реєстрації
    if log_interval_sec is None:
        # за замовчуванням = interval_sec (раз на цикл) якщо немає режиму only-on-change
        log_interval_sec = interval_sec if not log_on_change else 0

    consecutive_high_drift = 0
    consecutive_high_pressure = 0
    drift_normal_counter = 0  # cycles below high threshold to trigger reset
    drift_reset_cycles = 3  # configurable if needed later

    while True:
        loop_time = asyncio.get_event_loop().time()
        cycle_start = loop_time
        pending_health_events: list[tuple[str, dict[str, Any]]] = []

        def _record_skip(
            symbol: str,
            reason: str,
            *,
            details: dict[str, Any] | None = None,
            events: list[tuple[str, dict[str, Any]]] = pending_health_events,
        ) -> None:
            nonlocal skipped_symbols
            skipped_symbols += 1
            skip_reason_counts[reason] = skip_reason_counts.get(reason, 0) + 1
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[trade_updater] skip %s (reason=%s, details=%s)",
                    symbol,
                    reason,
                    details,
                )
            if reason in health_reasons:
                streak = skip_streaks.get(symbol, 0) + 1
                skip_streaks[symbol] = streak
                if streak >= skip_warn_cycles:
                    now_wall = time.time()
                    last_alert = skip_last_alert.get(symbol, 0.0)
                    if now_wall - last_alert >= warn_cooldown_sec:
                        skip_last_alert[symbol] = now_wall
                        logger.warning(
                            "[trade_updater] %s пропущено %d циклів поспіль (reason=%s)",
                            symbol,
                            streak,
                            reason,
                        )
                        payload = {
                            "reason": reason,
                            "streak": streak,
                            "threshold": skip_warn_cycles,
                        }
                        if details:
                            payload.update(details)
                        events.append((symbol, payload))
            else:
                skip_streaks.pop(symbol, None)

        try:
            queue_updates = await trade_update_service.drain_updates(
                health_tracker=health_tracker
            )
        except Exception:
            logger.exception("Не вдалося зняти price_updates через сервіс")
            queue_updates = {}

        # 1) Оновити активні угоди (best-effort)
        active_list = await trade_manager.get_active_trades()
        for tr in active_list:
            sym_raw = tr.get("symbol")
            sym = str(sym_raw).upper() if sym_raw else ""
            if not sym:
                continue
            queue_update = queue_updates.get(sym)
            queue_price = (
                _resolve_price_from_update(queue_update) if queue_update else None
            )
            queue_ts = queue_update.timestamp if queue_update else None
            use_queue = queue_update is not None and queue_price is not None

            df = None
            if not use_queue:
                try:
                    df = await store.get_df(sym, timeframe, limit=lookback)
                    if (
                        df is not None
                        and not df.empty
                        and "open_time" in df.columns
                        and "timestamp" not in df.columns
                    ):
                        df = df.rename(columns={"open_time": "timestamp"})
                except Exception as e:  # broad-except: I/O / кеш / мережа не критичні
                    logger.debug(f"Failed to fetch bars for {sym}: {e}")
                    continue
            if not use_queue and (df is None or df.empty or len(df) < lookback):
                _record_skip(sym, "insufficient_bars")
                continue
            try:
                last_close_price = (
                    safe_float(df["close"].iloc[-1])
                    if df is not None and "close" in df.columns and not df.empty
                    else None
                )
                if last_close_price is not None and last_close_price <= 0:
                    last_close_price = None
            except Exception:
                last_close_price = None
            # Try to obtain current stats from monitor if such API exists
            try:
                get_stats = getattr(monitor, "get_current_stats", None)
                if callable(get_stats):
                    stats = await get_stats(sym)
                else:
                    stats = {}
            except Exception:
                stats = {}
            price: float | None = queue_price
            if price is None:
                try:
                    cp_val = (
                        stats.get("current_price") if isinstance(stats, dict) else None
                    )
                    if isinstance(cp_val, (int, float)) and float(cp_val) > 0.0:
                        price = float(cp_val)
                except Exception:
                    price = None

            if price is None:
                if last_close_price is not None:
                    price = float(last_close_price)

            if price is None:
                _record_skip(sym, "no_price")
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Skip trade update for %s: відсутня валідна ціна", sym)
                continue

            ts_candidates: list[float] = []
            if queue_ts is not None:
                ts_candidates.append(queue_ts)
            df_ts = _extract_last_bar_ts(df)
            if df_ts is not None:
                ts_candidates.append(df_ts)
            if isinstance(stats, dict):
                iso_ts = _parse_iso8601(stats.get("last_updated"))
                if iso_ts is not None:
                    ts_candidates.append(iso_ts)
                for key in ("last_ts", "last_bar_ts", "last_price_ts", "timestamp"):
                    candidate = _normalize_epoch_ts(stats.get(key))
                    if candidate is not None:
                        ts_candidates.append(candidate)
            latest_ts = max(ts_candidates) if ts_candidates else None
            now_epoch = time.time()
            if latest_ts is not None:
                age_sec = max(0.0, now_epoch - latest_ts)
                if age_sec >= force_close_seconds:
                    stale_active_since.setdefault(sym, now_epoch)
                    stale_counters[sym] = stale_counters.get(sym, 0) + 1
                    if stale_counters[sym] >= force_cycles:
                        price_for_close = (
                            price if price is not None else last_close_price
                        )
                        if price_for_close is None:
                            price_for_close = safe_float(tr.get("current_price"))
                        if price_for_close is None:
                            price_for_close = safe_float(tr.get("entry_price"))
                        should_force_close, guard_reason = _should_force_close_stale(
                            tr, price_for_close
                        )
                        if not should_force_close:
                            _record_skip(
                                sym,
                                "stale_guard",
                                details={
                                    "age_sec": round(age_sec, 1),
                                    "guard_reason": guard_reason,
                                },
                            )
                            guard_last = stale_guard_ts.get(sym, 0.0)
                            if (now_epoch - guard_last) >= warn_cooldown_sec:
                                logger.info(
                                    "[stale-feed] Guarded %s: age=%.1fs ≥ %.1fs (reason=%s)",
                                    sym,
                                    age_sec,
                                    force_close_seconds,
                                    guard_reason,
                                )
                                stale_guard_ts[sym] = now_epoch
                                stale_counters[sym] = max(force_cycles - 1, 0)
                                last_emit = stale_last_telemetry_ts.get(sym, 0.0)
                                if (now_epoch - last_emit) >= telemetry_cooldown:
                                    await log_stage3_event(
                                        "feed_stale_guarded",
                                        sym,
                                        {
                                            "age_sec": round(age_sec, 1),
                                            "force_close_seconds": force_close_seconds,
                                            "guard_reason": guard_reason,
                                            "stale_cycles": stale_counters[sym],
                                            "stale_duration": now_epoch
                                            - stale_active_since.get(sym, now_epoch),
                                        },
                                    )
                                    stale_last_telemetry_ts[sym] = now_epoch
                            continue

                        close_price_candidate = safe_float(price_for_close)
                        if close_price_candidate is None:
                            close_price_candidate = safe_float(tr.get("entry_price"))
                        close_price = float(close_price_candidate or 0.0)
                        logger.error(
                            "[stale-feed] Force-close %s: age=%.1fs ≥ %.1fs (count=%d, reason=%s)",
                            sym,
                            age_sec,
                            force_close_seconds,
                            stale_counters[sym],
                            guard_reason,
                        )
                        try:
                            await trade_manager.close_trade(
                                tr["id"], close_price, "stale_feed"
                            )
                        except Exception:
                            logger.exception(
                                "Не вдалося закрити угоду %s при застої фіду",
                                tr.get("id", "unknown"),
                            )
                        stale_since = stale_active_since.pop(sym, now_epoch)
                        await log_stage3_event(
                            "feed_stale_force_close",
                            sym,
                            {
                                "age_sec": round(age_sec, 1),
                                "force_close_seconds": force_close_seconds,
                                "stale_cycles": stale_counters.get(sym, 0),
                                "guard_reason": guard_reason,
                                "close_price": close_price,
                                "stale_duration": max(0.0, now_epoch - stale_since),
                            },
                        )
                        stale_warn_ts.pop(sym, None)
                        stale_counters.pop(sym, None)
                        stale_guard_ts.pop(sym, None)
                        stale_last_telemetry_ts.pop(sym, None)
                        _record_skip(
                            sym,
                            "stale_force_close",
                            details={"age_sec": round(age_sec, 1)},
                        )
                        continue
                elif age_sec >= feed_stale_seconds:
                    stale_active_since.setdefault(sym, now_epoch)
                    stale_counters[sym] = stale_counters.get(sym, 0) + 1
                    _record_skip(
                        sym,
                        "stale_feed",
                        details={"age_sec": round(age_sec, 1)},
                    )
                    warn_last = stale_warn_ts.get(sym, 0.0)
                    if (now_epoch - warn_last) >= warn_cooldown_sec:
                        logger.warning(
                            "[stale-feed] Skip %s: age=%.1fs ≥ %.1fs (count=%d)",
                            sym,
                            age_sec,
                            feed_stale_seconds,
                            stale_counters[sym],
                        )
                        stale_warn_ts[sym] = now_epoch
                    last_emit = stale_last_telemetry_ts.get(sym, 0.0)
                    if (now_epoch - last_emit) >= telemetry_cooldown:
                        await log_stage3_event(
                            "feed_stale_warning",
                            sym,
                            {
                                "age_sec": round(age_sec, 1),
                                "stale_cycles": stale_counters[sym],
                                "feed_stale_seconds": feed_stale_seconds,
                            },
                        )
                        stale_last_telemetry_ts[sym] = now_epoch
                    continue
                else:
                    stale_counters.pop(sym, None)
                    stale_warn_ts.pop(sym, None)
                    if sym in stale_active_since:
                        stale_since = stale_active_since.pop(sym)
                        await log_stage3_event(
                            "feed_stale_recovered",
                            sym,
                            {
                                "stale_duration": max(0.0, now_epoch - stale_since),
                                "age_sec": round(age_sec, 1),
                            },
                        )
                        stale_last_telemetry_ts.pop(sym, None)
            else:
                stale_counters.pop(sym, None)
                stale_warn_ts.pop(sym, None)
                if sym in stale_active_since:
                    stale_since = stale_active_since.pop(sym)
                    await log_stage3_event(
                        "feed_stale_recovered",
                        sym,
                        {
                            "stale_duration": max(0.0, now_epoch - stale_since),
                            "age_sec": None,
                        },
                    )
                    stale_last_telemetry_ts.pop(sym, None)

            market_data = {
                "price": price,
                "atr": stats.get("atr", 0),
                "rsi": stats.get("rsi", 0),
                "volume": stats.get("volume_mean", 0),
                "context_break": stats.get("context_break", False),
            }
            await trade_manager.update_trade(tr["id"], market_data)
            if queue_update is None:
                health_tracker.record_refresh(
                    sym,
                    data_ts=latest_ts,
                    wall_ts=now_epoch,
                )
            skip_streaks.pop(sym, None)

        # 2) Лічильники після оновлення
        active = await trade_manager.get_active_trades()
        closed = await trade_manager.get_closed_trades()
        now = asyncio.get_event_loop().time()
        counts = (len(active), len(closed))
        last_success_ts = int(now)
        exit_reason_counts: dict[str, int] = {}
        for closed_trade in closed:
            reason_val = str(closed_trade.get("exit_reason") or "").lower()
            if reason_val:
                exit_reason_counts[reason_val] = (
                    exit_reason_counts.get(reason_val, 0) + 1
                )
        should_log = False
        if log_on_change and last_counts is not None and counts != last_counts:
            should_log = True
        elif log_on_change and last_counts is None:
            # перший лог обов'язково
            should_log = True
        if log_interval_sec and (now - last_log_ts) >= log_interval_sec:
            # якщо заданий інтервал — поважаємо його (може співіснувати з on_change)
            should_log = should_log or True
        # Heartbeat раз на _hb_sec навіть без змін
        if (now - last_log_ts) >= _hb_sec:
            should_log = True

        if should_log:
            logger.info(
                f"🟢 Active trades: {counts[0]}    🔴 Closed trades: {counts[1]}"
            )
            last_log_ts = now
            last_counts = counts

        # Публікація метрик Prometheus видалена

        # 3) Поточний час виконання циклу (elapsed) ДО формування payload щоб мати drift_ratio
        elapsed = asyncio.get_event_loop().time() - cycle_start
        health_tracker.record_cycle(elapsed)

        # Update EWMA for skipped symbols before publishing (exclude cycles with zero to preserve decay behaviour)
        if skipped_symbols > 0:
            skipped_ewma = (
                skipped_alpha * skipped_symbols + (1 - skipped_alpha) * skipped_ewma
            )
        else:
            # light decay toward 0 (optional): multiply by (1 - alpha/4)
            skipped_ewma *= 1 - skipped_alpha / 4

        # Pressure ratio (avoid div by zero)
        active_trades_count = counts[0]
        max_skip_streak = max(skip_streaks.values(), default=0)
        pressure_ratio = (
            skipped_ewma / active_trades_count if active_trades_count > 0 else 0.0
        )
        pressure_norm = math.log1p(pressure_ratio) if pressure_ratio > 0 else 0.0
        health_payload = health_tracker.to_payload(
            cycle_elapsed=elapsed,
            skip_ewma=skipped_ewma,
            skipped=skipped_symbols,
            pressure_ratio=pressure_ratio,
            active_trades=active_trades_count,
        )
        health_payload["max_skip_streak"] = int(max_skip_streak)
        health_payload["dynamic_interval"] = round(dynamic_interval, 4)
        health_payload["interval_sec"] = interval_sec
        # Prometheus gauges видалено

        # Drift warnings (rate limited) & pressure warnings (rate limited)
        drift_ratio_value = elapsed / max(1e-6, interval_sec)
        now_wall = asyncio.get_event_loop().time()
        if tu_cfg is not None:
            try:
                # Drift thresholds
                # Idle mode detection (no trades) – suppress counting & reset
                idle_mode = counts[0] == 0 and len(closed) == 0
                low_pressure = pressure_ratio < 0.01
                if idle_mode or low_pressure:
                    if consecutive_high_drift:
                        logger.debug(
                            "Reset consecutive_drift_high due to idle/low-pressure (was %s)",
                            consecutive_high_drift,
                        )
                    consecutive_high_drift = 0
                    drift_normal_counter = 0
                if idle_mode:
                    # Skip drift anomaly logic entirely when idle
                    pass
                elif drift_ratio_value > tu_cfg.drift_warn_high:
                    consecutive_high_drift += 1
                    drift_normal_counter = 0
                    if (now_wall - last_warn_drift_ts) > 60:
                        logger.warning(
                            f"Drift ratio warning: {drift_ratio_value:.2f} (>{tu_cfg.drift_warn_high})"
                        )
                        last_warn_drift_ts = now_wall
                elif drift_ratio_value < tu_cfg.drift_warn_low and not idle_mode:
                    # treat *low* drift anomaly similarly (still 'high' counter for consecutive anomalous cycles)
                    consecutive_high_drift += 1
                    drift_normal_counter = 0
                    if (now_wall - last_warn_drift_ts) > 60:
                        logger.warning(
                            f"Drift ratio low warning: {drift_ratio_value:.2f} (<{tu_cfg.drift_warn_low})"
                        )
                        last_warn_drift_ts = now_wall
                else:
                    # within normal band → increment normal counter
                    if not idle_mode:
                        drift_normal_counter += 1
                        if (
                            drift_normal_counter >= drift_reset_cycles
                            and consecutive_high_drift
                        ):
                            logger.info(
                                f"Drift back to normal for {drift_normal_counter} cycles – resetting consecutive_drift_high ({consecutive_high_drift} -> 0)"
                            )
                            consecutive_high_drift = 0
                            drift_normal_counter = 0

                # Pressure threshold
                if pressure_ratio > tu_cfg.pressure_warn and not idle_mode:
                    consecutive_high_pressure += 1
                    if (now_wall - last_warn_pressure_ts) > 60:
                        logger.warning(
                            f"Pressure high: {pressure_ratio:.2f} (>{tu_cfg.pressure_warn})"
                        )
                        last_warn_pressure_ts = now_wall
                else:
                    consecutive_high_pressure = 0

                # ── Adaptive interval scaling (optional) ──
                if getattr(tu_cfg, "auto_interval_scale_enabled", False):
                    try:
                        if consecutive_high_pressure >= getattr(
                            tu_cfg, "auto_interval_scale_cycles", 3
                        ) and dynamic_interval < getattr(
                            tu_cfg, "auto_interval_scale_cap", 900.0
                        ):
                            factor = max(
                                1.01,
                                float(
                                    getattr(tu_cfg, "auto_interval_scale_factor", 1.25)
                                ),
                            )
                            new_interval = min(
                                dynamic_interval * factor,
                                getattr(tu_cfg, "auto_interval_scale_cap", 900.0),
                            )
                            if new_interval > dynamic_interval:
                                logger.warning(
                                    f"Adaptive interval scaling: {dynamic_interval:.1f}s -> {new_interval:.1f}s (pressure sustained)"
                                )
                                dynamic_interval = new_interval
                                # reset pressure counter to avoid runaway escalation
                                consecutive_high_pressure = 0
                    except Exception:
                        pass

                # ── Adaptive skipped_ewma_alpha (optional) ──
                if getattr(tu_cfg, "auto_alpha_enabled", False):
                    try:
                        turbulent = drift_ratio_value >= getattr(
                            tu_cfg, "alpha_turbulence_drift", 2.0
                        ) or pressure_ratio >= getattr(
                            tu_cfg, "alpha_turbulence_pressure", 1.5
                        )
                        calm = drift_ratio_value <= getattr(
                            tu_cfg, "alpha_calm_drift", 1.05
                        ) and pressure_ratio <= getattr(
                            tu_cfg, "alpha_calm_pressure", 0.5
                        )
                        # maintain calm counter across cycles
                        if "calm_counter" not in locals():
                            calm_counter = 0
                        if turbulent:
                            calm_counter = 0
                            # increase alpha (shorter memory)
                            new_alpha = min(
                                getattr(tu_cfg, "alpha_max", 0.6),
                                skipped_alpha + getattr(tu_cfg, "alpha_step", 0.05),
                            )
                            if abs(new_alpha - skipped_alpha) > 1e-9:
                                logger.info(
                                    f"Adaptive α increase: {skipped_alpha:.3f} -> {new_alpha:.3f} (turbulence)"
                                )
                                skipped_alpha = new_alpha
                        elif calm:
                            calm_counter += 1
                            if calm_counter >= getattr(tu_cfg, "alpha_calm_cycles", 5):
                                new_alpha = max(
                                    getattr(tu_cfg, "alpha_min", 0.05),
                                    skipped_alpha - getattr(tu_cfg, "alpha_step", 0.05),
                                )
                                if abs(new_alpha - skipped_alpha) > 1e-9:
                                    logger.info(
                                        f"Adaptive α decrease: {skipped_alpha:.3f} -> {new_alpha:.3f} (calm)"
                                    )
                                    skipped_alpha = new_alpha
                                calm_counter = 0
                        else:
                            # neither calm nor turbulent resets calm counter
                            pass
                    except Exception:
                        pass
            except Exception:
                pass

        # UI publish (Redis JSON) for centralized consumer (two keys for backward compat)
        if publish_ui:
            try:
                targets, telemetry_map, telemetry_meta = _collect_stage3_ui_data(active)
            except Exception:
                targets = {}
                telemetry_map = {}
                telemetry_meta = {}
                logger.exception("Stage3 telemetry build failed")
            else:
                telemetry_meta.update({"active": counts[0], "closed": counts[1]})
        else:
            targets = {}
            telemetry_map = {}
            telemetry_meta = {}
            payload_trades = {
                "active": counts[0],
                "closed": counts[1],
                "ts": last_success_ts,
                "interval": timeframe,
                "targets": targets,
                "exit_reason_counts": exit_reason_counts,
            }
            core_payload = {
                "trades": payload_trades,
                "skipped": skipped_symbols,
                "skipped_ewma": round(skipped_ewma, 4),
                "max_skip_streak": int(max_skip_streak),
                "last_update_ts": last_success_ts,
                "cycle_interval": interval_sec,
                "dynamic_interval": dynamic_interval,
                "drift_ratio": (elapsed / max(1e-6, interval_sec)),
                "pressure": round(pressure_ratio, 4),
                "pressure_norm": round(pressure_norm, 5),
                "thresholds": {
                    "drift_high": getattr(tu_cfg, "drift_warn_high", None),
                    "drift_low": getattr(tu_cfg, "drift_warn_low", None),
                    "pressure": getattr(tu_cfg, "pressure_warn", None),
                },
                "consecutive": {
                    "drift_high": consecutive_high_drift,
                    "pressure_high": consecutive_high_pressure,
                },
                "alpha": round(skipped_alpha, 4),
                "skip_streak_warn_cycles": skip_warn_cycles,
                "telemetry": telemetry_map,
                "telemetry_meta": telemetry_meta,
                "health": health_payload,
            }
            # top skip reasons (optional)
            if tu_cfg is not None and getattr(tu_cfg, "publish_skip_reasons", False):
                try:
                    if skip_reason_counts:
                        top_n = int(getattr(tu_cfg, "skip_reasons_top_n", 5))
                        sorted_reasons = sorted(
                            skip_reason_counts.items(),
                            key=lambda kv: kv[1],
                            reverse=True,
                        )[:top_n]
                        core_payload["skip_reasons"] = dict(sorted_reasons)
                except Exception:
                    pass
            # ── Dual-write період: нові ключі ai_one:core + (опційно) legacy "stats" ──
            try:
                # Новий єдиний документ core з json-путями
                await store.redis.jset(
                    REDIS_DOC_CORE,
                    REDIS_CORE_PATH_TRADES,
                    value=payload_trades,
                    ttl=CORE_TTL_SEC,
                )
                await store.redis.jset(
                    REDIS_DOC_CORE,
                    REDIS_CORE_PATH_STATS,
                    value=core_payload,
                    ttl=CORE_TTL_SEC,
                )
            except Exception as e:
                logger.debug(f"core dual-write (new) failed: {e}")

            if CORE_DUAL_WRITE_OLD_STATS:
                try:
                    await store.redis.jset(
                        "stats", "trades", value=payload_trades, ttl=ui_ttl
                    )
                except Exception:
                    pass
                try:
                    await store.redis.jset(
                        "stats", "core", value=core_payload, ttl=ui_ttl
                    )
                except Exception as e:  # pragma: no cover
                    logger.debug(f"core dual-write (legacy) failed: {e}")

            # Health heartbeat key (short TTL)
            try:
                hb_payload = {
                    "ts": last_success_ts,
                    "active_trades": counts[0],
                    "drift_ratio": round(drift_ratio_value, 4),
                    "pressure": round(pressure_ratio, 4),
                }
                hb_ttl = max(5, int(interval_sec * 0.9))
                # Новий core:health
                try:
                    await store.redis.jset(
                        REDIS_DOC_CORE,
                        REDIS_CORE_PATH_HEALTH,
                        value=hb_payload,
                        ttl=hb_ttl,
                    )
                except Exception:
                    logger.debug("core:health write failed", exc_info=True)
                # Legacy під час dual-write
                if CORE_DUAL_WRITE_OLD_STATS:
                    try:
                        await store.redis.jset(
                            "stats", "health", value=hb_payload, ttl=hb_ttl
                        )
                    except Exception:
                        pass
            except Exception:
                pass

        now_health = time.time()
        if (now_health - last_health_event_ts) >= STAGE3_HEALTH_CHECK_INTERVAL_SEC:
            try:
                await log_stage3_event("trade_updater_health", "global", health_payload)
            except Exception:
                logger.debug(
                    "trade_updater_health telemetry publish failed", exc_info=True
                )
            else:
                last_health_event_ts = now_health

        if pending_health_events:
            for symbol, payload in pending_health_events:
                payload.setdefault("warn_cooldown_sec", warn_cooldown_sec)
                payload.setdefault("skip_warn_cycles", skip_warn_cycles)
                try:
                    await log_stage3_event("trade_updater_skip_streak", symbol, payload)
                except Exception:
                    logger.debug("skip_streak telemetry publish failed for %s", symbol)

        # Метрики Prometheus видалено

        # Exponential backoff if cycle took longer than current interval (reset skipped counter per cycle)
        skipped_symbols = 0
        skip_reason_counts.clear()
        if elapsed > dynamic_interval:
            mul = float(backoff_multiplier) if backoff_multiplier is not None else 1.5
            dynamic_interval = min(dynamic_interval * mul, float(max_backoff_sec))
        else:
            if dynamic_interval > interval_sec:
                div = (
                    float(backoff_multiplier) if backoff_multiplier is not None else 1.5
                )
                dynamic_interval = max(float(interval_sec), dynamic_interval / div)

        sleep_for = max(0.0, dynamic_interval - elapsed)
        await asyncio.sleep(sleep_for)


__all__ = ["trade_manager_updater"]
