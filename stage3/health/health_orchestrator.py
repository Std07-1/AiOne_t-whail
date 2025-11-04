"""Агрегатор health-метрик Stage3."""

from __future__ import annotations

import asyncio
import json
import logging
import statistics
import time
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    RedisError,
    TimeoutError as RedisTimeoutError,
)

from config.config import (
    CORE_TTL_SEC,
    REDIS_CORE_PATH_HEALTH,
    REDIS_DOC_CORE,
    STAGE3_HEALTH_CFG,
    STAGE3_SHADOW_PRICE_STREAM_CFG,
)
from data.redis_connection import acquire_redis, release_redis
from monitoring.telemetry_sink import log_stage3_event
from stage3.price_stream_shadow_consumer import PriceStreamShadowConsumer
from stage3.trade_manager import TradeLifecycleManager

logger = logging.getLogger("stage3.health.orchestrator")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False


ConfidenceProvider = Callable[[], Awaitable[Mapping[str, Any]]]


class Stage3HealthOrchestrator:
    """Періодично агрегує стан Stage3 та публікує телеметрію."""

    def __init__(
        self,
        *,
        trade_manager: TradeLifecycleManager,
        price_consumer: PriceStreamShadowConsumer,
        health_cfg: Mapping[str, Any] | None = None,
        telemetry_sink=log_stage3_event,
        confidence_provider: ConfidenceProvider | None = None,
    ) -> None:
        self._trade_manager = trade_manager
        self._consumer = price_consumer
        self._telemetry = telemetry_sink
        self._confidence_provider = confidence_provider
        self._health_cfg = dict(health_cfg or STAGE3_HEALTH_CFG)
        self._stream_cfg = dict(STAGE3_SHADOW_PRICE_STREAM_CFG)
        self._interval = max(1.0, float(self._health_cfg.get("POLL_INTERVAL_SEC", 3.0)))
        self._emit_interval = max(
            self._interval,
            float(self._health_cfg.get("EMIT_INTERVAL_SEC", 10.0)),
        )
        default_ttl = max(5, int(self._health_cfg.get("TTL_SEC", 20)))
        self._ttl = max(default_ttl, CORE_TTL_SEC)
        self._pending_warn = int(self._health_cfg.get("PENDING_WARN", 3000))
        self._pending_crit = int(self._health_cfg.get("PENDING_CRITICAL", 8000))
        consumer_warn = getattr(self._consumer, "pending_warn_threshold", None)
        if isinstance(consumer_warn, int) and consumer_warn > 0:
            self._pending_warn = min(self._pending_warn, consumer_warn)
            self._pending_crit = max(self._pending_crit, consumer_warn * 2)
        self._buffer_warn = int(self._health_cfg.get("BUFFER_WARN", 100))
        self._buffer_crit = int(self._health_cfg.get("BUFFER_CRITICAL", 300))
        self._active_warn = int(self._health_cfg.get("ACTIVE_WARN", 6))
        self._active_crit = int(self._health_cfg.get("ACTIVE_CRITICAL", 12))
        self._stream_key = str(self._stream_cfg.get("STREAM_KEY", ""))
        if not self._stream_key:
            raise ValueError("STREAM_KEY для health orchestrator не налаштований")
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._redis: Redis | None = None
        self._owns_redis = False
        self._last_telemetry_ts: float = 0.0
        self._last_busy_level: int | None = None
        self._last_snapshot: dict[str, Any] | None = None
        try:
            self._trade_manager.health_snapshot = None  # type: ignore[attr-defined]
        except AttributeError:
            logger.debug("Trade manager не підтримує health_snapshot під час старту")

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    async def start(self) -> asyncio.Task[None]:
        if self._task is not None:
            raise RuntimeError("Stage3HealthOrchestrator уже запущений")
        await self._ensure_redis()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="stage3-health")
        return self._task

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.debug("Health orchestrator stop failed", exc_info=True)
            finally:
                self._task = None
        await self._close_redis()
        try:
            self._trade_manager.health_snapshot = None  # type: ignore[attr-defined]
        except AttributeError:
            logger.debug("Trade manager не підтримує health_snapshot під час зупинки")

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            started = time.monotonic()
            try:
                snapshot = await self._collect_snapshot()
                await self._cache_snapshot(snapshot)
                await self._maybe_emit_telemetry(snapshot)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("Stage3 health cycle failed", exc_info=True)
            elapsed = time.monotonic() - started
            wait_for = max(0.1, self._interval - elapsed)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait_for)
            except TimeoutError:
                continue

    async def _collect_snapshot(self) -> dict[str, Any]:
        stream_metrics = await self._collect_stream_metrics()
        trade_metrics = await self._collect_trade_metrics()
        confidence_metrics = await self._collect_confidence_metrics(trade_metrics)
        busy_level, reason = self._derive_busy_level(
            stream_metrics,
            trade_metrics,
        )
        now_iso = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
        snapshot = {
            "timestamp": now_iso,
            "busy_level": busy_level,
            "status": self._status_label(busy_level),
            "busy_reason": reason,
            "stream": stream_metrics,
            "trade_manager": trade_metrics,
            "confidence": confidence_metrics,
        }
        snapshot["stream"].setdefault("redis_stream", self._stream_key)
        self._last_snapshot = snapshot
        try:
            self._trade_manager.health_snapshot = snapshot  # type: ignore[attr-defined]
        except AttributeError:
            logger.debug("Trade manager не підтримує health_snapshot під час оновлення")
        return snapshot

    async def _collect_stream_metrics(self) -> dict[str, Any]:
        pending = max(0, int(self._consumer.last_pending_total))
        buffer_depth = max(0, self._consumer.buffer_depth)
        stream_depth = -1
        if self._redis is not None:
            try:
                stream_depth = int(await self._redis.xlen(self._stream_key))
            except (RedisError, RedisTimeoutError, RedisConnectionError):
                logger.debug("xlen failed для %s", self._stream_key, exc_info=True)
        pending_ratio = 0.0
        if self._pending_warn > 0:
            pending_ratio = pending / float(self._pending_warn)
        return {
            "pending": pending,
            "pending_ratio": round(pending_ratio, 3),
            "buffer_depth": buffer_depth,
            "stream_depth": max(stream_depth, 0),
        }

    async def _collect_trade_metrics(self) -> dict[str, Any]:
        active_count = 0
        confidence_values: list[float] = []
        lock = getattr(self._trade_manager, "lock", None)
        active = getattr(self._trade_manager, "active_trades", None)
        if isinstance(lock, asyncio.Lock) and isinstance(active, dict):
            try:
                async with lock:
                    active_count = len(active)
                    for trade in active.values():
                        value = getattr(trade, "confidence", None)
                        if value is None:
                            continue
                        try:
                            confidence_values.append(float(value))
                        except (TypeError, ValueError):
                            continue
            except Exception:
                logger.debug(
                    "Не вдалося отримати активні угоди через lock", exc_info=True
                )
        if not confidence_values and hasattr(self._trade_manager, "get_active_trades"):
            try:
                snapshot = await self._trade_manager.get_active_trades()
            except Exception:
                snapshot = []
            if snapshot:
                active_count = len(snapshot)
                for entry in snapshot:
                    value = entry.get("confidence")
                    if value is None:
                        continue
                    try:
                        confidence_values.append(float(value))
                    except (TypeError, ValueError):
                        continue
        max_parallel = getattr(self._trade_manager, "max_parallel_trades", None)
        open_slots = None
        if isinstance(max_parallel, int):
            open_slots = max(0, max_parallel - active_count)
        return {
            "active_trades": active_count,
            "open_slots": open_slots,
            "confidence_values": confidence_values,
        }

    async def _collect_confidence_metrics(
        self, trade_metrics: dict[str, Any]
    ) -> dict[str, Any]:
        values = list(trade_metrics.get("confidence_values", []))
        from_provider: Mapping[str, Any] = {}
        if self._confidence_provider is not None:
            try:
                from_provider = dict(await self._confidence_provider())
            except Exception:
                logger.debug("Confidence provider failed", exc_info=True)
        stats: dict[str, Any] = {}
        if values:
            try:
                stats["avg"] = round(statistics.fmean(values), 4)
            except (statistics.StatisticsError, ValueError):
                stats["avg"] = round(sum(values) / len(values), 4)
            stats["max"] = round(max(values), 4)
            stats["min"] = round(min(values), 4)
            stats["count"] = len(values)
        stats.update({k: from_provider[k] for k in from_provider})
        return stats

    async def _cache_snapshot(self, snapshot: dict[str, Any]) -> None:
        if self._redis is None:
            return
        payload = dict(snapshot)
        try:
            await self._redis.json().set(
                REDIS_DOC_CORE,
                f".{REDIS_CORE_PATH_HEALTH}",
                payload,
            )
            await self._redis.expire(REDIS_DOC_CORE, self._ttl)
        except Exception:
            try:
                key = f"{REDIS_DOC_CORE}:{REDIS_CORE_PATH_HEALTH}"
                await self._redis.set(key, json.dumps(payload), ex=self._ttl)
            except Exception:
                logger.debug("Не вдалося зберегти health snapshot", exc_info=True)

    async def _maybe_emit_telemetry(self, snapshot: dict[str, Any]) -> None:
        if self._telemetry is None:
            return
        busy_level = int(snapshot.get("busy_level", 0))
        now = time.monotonic()
        should_emit = False
        if self._last_busy_level is None or busy_level != self._last_busy_level:
            should_emit = True
        elif (now - self._last_telemetry_ts) >= self._emit_interval:
            should_emit = True
        if not should_emit:
            return
        payload = {
            "status": snapshot.get("status", "unknown"),
            "busy_level": busy_level,
            "busy_reason": snapshot.get("busy_reason"),
            "stream_pending": snapshot["stream"].get("pending", 0),
            "stream_depth": snapshot["stream"].get("stream_depth", 0),
            "pending_ratio": snapshot["stream"].get("pending_ratio"),
            "buffer_depth": snapshot["stream"].get("buffer_depth"),
            "active_trades": snapshot["trade_manager"].get("active_trades", 0),
            "open_slots": snapshot["trade_manager"].get("open_slots"),
            "confidence_avg": snapshot["confidence"].get("avg"),
            "confidence_max": snapshot["confidence"].get("max"),
            "confidence_min": snapshot["confidence"].get("min"),
            "redis_stream": snapshot["stream"].get("redis_stream"),
            "timestamp": snapshot.get("timestamp"),
        }
        try:
            await self._telemetry("stage3_health", "GLOBAL", payload)
        except Exception:
            logger.debug("Stage3 health telemetry emit failed", exc_info=True)
        else:
            self._last_busy_level = busy_level
            self._last_telemetry_ts = now

    def _derive_busy_level(
        self,
        stream_metrics: dict[str, Any],
        trade_metrics: dict[str, Any],
    ) -> tuple[int, str | None]:
        pending = int(stream_metrics.get("pending", 0))
        buffer_depth = int(stream_metrics.get("buffer_depth", 0))
        active = int(trade_metrics.get("active_trades", 0))
        reason: str | None = None
        level = 0
        if (
            pending >= self._pending_warn
            or buffer_depth >= self._buffer_warn
            or active >= self._active_warn
        ):
            level = 1
            if pending >= self._pending_warn:
                reason = "pending_warn"
            elif buffer_depth >= self._buffer_warn:
                reason = "buffer_warn"
            elif active >= self._active_warn:
                reason = "active_warn"
        if (
            pending >= self._pending_crit
            or buffer_depth >= self._buffer_crit
            or active >= self._active_crit
        ):
            level = 2
            if pending >= self._pending_crit:
                reason = "pending_critical"
            elif buffer_depth >= self._buffer_crit:
                reason = "buffer_critical"
            elif active >= self._active_crit:
                reason = "active_critical"
        severe_threshold = max(self._pending_crit * 1.5, self._pending_crit + 2000)
        if pending >= severe_threshold:
            level = 3
            reason = "pending_severe"
        return level, reason

    @staticmethod
    def _status_label(level: int) -> str:
        if level <= 0:
            return "ok"
        if level == 1:
            return "elevated"
        if level == 2:
            return "busy"
        return "critical"

    async def _ensure_redis(self) -> None:
        if self._redis is not None:
            return
        self._redis = await acquire_redis(decode_responses=True)
        self._owns_redis = True

    async def _close_redis(self) -> None:
        if not self._owns_redis:
            self._redis = None
            return
        try:
            await release_redis(self._redis)
        except Exception:
            logger.debug(
                "Не вдалося закрити Redis у health orchestrator", exc_info=True
            )
        finally:
            self._redis = None
            self._owns_redis = False


__all__ = ["Stage3HealthOrchestrator"]
