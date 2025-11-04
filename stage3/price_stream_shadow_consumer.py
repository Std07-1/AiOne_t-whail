"""Shadow consumer для Stage3 price stream.

Читає оновлення цін із Redis Stream shadow-пайплайна, виконує коалесування
та повертає останні оновлення по кожному символу.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)

from config.config import STAGE3_SHADOW_PRICE_STREAM_CFG
from data.redis_connection import acquire_redis, release_redis
from data.redis_stream_utils import (
    normalize_xautoclaim_response,
    xpending_xclaim_fallback,
)
from monitoring.telemetry_sink import log_stage3_event
from stage3.price_stream_shadow import PriceUpdate

logger = logging.getLogger("stage3.price_stream_shadow_consumer")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False


@dataclass(slots=True)
class ShadowStreamMessage:
    """Результат читання shadow stream після коалесування."""

    update: PriceUpdate
    message_id: str


class PriceStreamShadowConsumer:
    """Споживає shadow-stream оновлення цін із Redis."""

    def __init__(
        self,
        *,
        stream_cfg: Mapping[str, Any] | None = None,
        consumer_name: str | None = None,
        telemetry_sink=log_stage3_event,
    ) -> None:
        cfg = dict(stream_cfg or STAGE3_SHADOW_PRICE_STREAM_CFG)
        self._stream_key: str = str(cfg.get("STREAM_KEY", ""))
        if not self._stream_key:
            raise ValueError("STREAM_KEY для PriceStreamShadowConsumer не заданий")
        self._group: str = str(cfg.get("GROUP", "price_shadow"))
        self._block_ms: int = int(cfg.get("BLOCK_MS", 1000))
        self._idle_claim_ms: int = int(float(cfg.get("IDLE_CLAIM_SEC", 30.0)) * 1000)
        self._pending_warn_threshold: int = int(cfg.get("PENDING_WARN_THRESHOLD", 5000))
        self._pending_sample_interval_sec: float = float(
            cfg.get("PENDING_SAMPLE_SEC", 5.0)
        )
        prefix = str(cfg.get("CONSUMER_PREFIX", "price-shadow"))
        host = socket.gethostname()
        pid = os.getpid()
        self._consumer_name = consumer_name or f"{prefix}-{host}-{pid}"
        self._telemetry = telemetry_sink
        self._redis: Redis | None = None
        self._owns_redis = False
        self._buffer: OrderedDict[str, tuple[PriceUpdate, str]] = OrderedDict()
        self._auto_ack: list[str] = []
        self._last_pending_total: int = 0
        self._last_pending_sample_ts: float = 0.0
        self._lock = asyncio.Lock()

    @property
    def buffer_depth(self) -> int:
        """Поточна глибина локального буфера після коалесування."""

        return len(self._buffer)

    @property
    def stream_key(self) -> str:
        """Ключ Redis stream, з якого читає consumer."""

        return self._stream_key

    @property
    def group(self) -> str:
        """Ім'я consumer group для shadow stream."""

        return self._group

    @property
    def pending_warn_threshold(self) -> int:
        """Поріг попередження за кількістю pending-повідомлень."""

        return self._pending_warn_threshold

    @property
    def last_pending_total(self) -> int:
        """Останнє відоме значення XPENDING (кількість непідтверджених)."""

        return self._last_pending_total

    async def start(self) -> None:
        """Готує Redis-підключення та consumer group."""

        async with self._lock:
            await self._ensure_redis()

    async def stop(self) -> None:
        """Закриває Redis-підключення та очищає буфери."""

        async with self._lock:
            await self._close_redis()
            self._buffer.clear()
            self._auto_ack.clear()

    async def fetch(self, max_items: int | None = None) -> list[ShadowStreamMessage]:
        """Зчитує shadow stream, коалесує та повертає оновлення."""

        async with self._lock:
            await self._ensure_redis()
            await self._auto_claim()
            await self._read_stream(max_items)
            await self._flush_auto_ack()
            await self._sample_pending()
            return self._drain_buffer(max_items)

    async def ack(self, message_ids: Sequence[str]) -> None:
        """Позначає повідомлення як оброблені."""

        if not message_ids:
            return
        async with self._lock:
            if self._redis is None:
                return
            await self._xack(message_ids)

    async def _ensure_redis(self) -> None:
        if self._redis is not None:
            return
        self._redis = await acquire_redis(decode_responses=True)
        self._owns_redis = True
        await self._ensure_group()

    async def _ensure_group(self) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.xgroup_create(
                self._stream_key,
                self._group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _auto_claim(self) -> None:
        if self._redis is None:
            return
        if self._idle_claim_ms <= 0:
            return
        start_id = "0-0"
        while True:
            try:
                try:
                    resp = await self._redis.xautoclaim(
                        self._stream_key,
                        self._group,
                        self._consumer_name,
                        self._idle_claim_ms,
                        start_id,
                        50,
                    )
                except TypeError:
                    # Fallback for test fakes that use keyword args/signature
                    resp = await self._redis.xautoclaim(
                        self._stream_key,
                        self._group,
                        self._consumer_name,
                        min_idle_time=self._idle_claim_ms,
                        start=start_id,
                        count=50,
                    )
                messages, start_id = normalize_xautoclaim_response(resp)
            except ResponseError:
                # Використовуємо централізований fallback XPENDING→XCLAIM
                messages = await xpending_xclaim_fallback(
                    self._redis,
                    self._stream_key,
                    self._group,
                    self._consumer_name,
                    self._idle_claim_ms,
                    50,
                )
                start_id = "0-0"
            except (RedisTimeoutError, RedisConnectionError):
                await self._handle_redis_failure()
                return
            if not messages:
                break
            for message_id, fields in messages:
                self._stage_message(message_id, fields)
            if start_id == "0-0":
                break

    async def _read_stream(self, max_items: int | None) -> None:
        if self._redis is None:
            return
        read_count = max_items if max_items is not None else 100
        read_count = max(1, read_count)
        try:
            response = await self._redis.xreadgroup(
                groupname=self._group,
                consumername=self._consumer_name,
                streams={self._stream_key: ">"},
                count=read_count,
                block=self._block_ms,
            )
        except (RedisTimeoutError, RedisConnectionError):
            await self._handle_redis_failure()
            return
        if not response:
            return
        for stream_key, messages in response:
            if stream_key != self._stream_key:
                continue
            for message_id, fields in messages:
                self._stage_message(message_id, fields)

    def _stage_message(self, message_id: str, fields: Mapping[str, Any]) -> None:
        parsed = self._parse_message(fields)
        if parsed is None:
            self._auto_ack.append(message_id)
            return
        symbol = parsed.symbol.upper()
        if symbol in self._buffer:
            _, old_id = self._buffer.pop(symbol)
            self._auto_ack.append(old_id)
        self._buffer[symbol] = (parsed, message_id)

    def _drain_buffer(self, max_items: int | None) -> list[ShadowStreamMessage]:
        items: list[ShadowStreamMessage] = []
        count = max_items if max_items is not None else len(self._buffer)
        for _ in range(min(count, len(self._buffer))):
            _symbol, (update, message_id) = self._buffer.popitem(last=False)
            items.append(ShadowStreamMessage(update=update, message_id=message_id))
        return items

    async def _flush_auto_ack(self) -> None:
        if not self._auto_ack:
            return
        if self._redis is None:
            self._auto_ack.clear()
            return
        await self._xack(self._auto_ack)
        self._auto_ack.clear()

    async def _xack(self, message_ids: Sequence[str]) -> None:
        if self._redis is None or not message_ids:
            return
        try:
            await self._redis.xack(self._stream_key, self._group, *message_ids)
        except (RedisTimeoutError, RedisConnectionError):
            await self._handle_redis_failure()

    async def _handle_redis_failure(self) -> None:
        logger.warning("PriceStreamShadowConsumer: Redis failure, очищуємо підключення")
        await self._close_redis()

    async def _close_redis(self) -> None:
        if not self._owns_redis:
            self._redis = None
            return
        try:
            await release_redis(self._redis)
        except Exception:  # pragma: no cover - best effort
            logger.debug("release_redis failed", exc_info=True)
        finally:
            self._redis = None
            self._owns_redis = False

    def _parse_message(self, fields: Mapping[str, Any]) -> PriceUpdate | None:
        symbol = str(fields.get("symbol") or "").upper()
        if not symbol:
            return None
        try:
            timestamp = (
                float(fields.get("ts")) if fields.get("ts") is not None else None
            )
            ingested_ts = (
                float(fields.get("ingested_ts"))
                if fields.get("ingested_ts") is not None
                else None
            )

            def _float_or_none(key: str) -> float | None:
                value = fields.get(key)
                if value is None:
                    return None
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            update = PriceUpdate(
                symbol=symbol,
                timestamp=timestamp if timestamp is not None else time.time(),
                source=str(fields.get("source") or "ws"),
                bid=_float_or_none("bid"),
                ask=_float_or_none("ask"),
                last_price=_float_or_none("last_price"),
                volume=_float_or_none("volume"),
                latency=_float_or_none("latency"),
                sequence_id=(
                    int(fields.get("sequence_id"))
                    if fields.get("sequence_id")
                    else None
                ),
                ingested_ts=ingested_ts,
            )
        except Exception:
            logger.debug("PriceStreamShadowConsumer: не вдалося розпарсити %s", fields)
            return None
        return update

    async def _sample_pending(self) -> None:
        if self._redis is None:
            return
        now = time.monotonic()
        if (now - self._last_pending_sample_ts) < self._pending_sample_interval_sec:
            return
        try:
            summary = await self._redis.xpending(self._stream_key, self._group)
        except (RedisTimeoutError, RedisConnectionError):
            await self._handle_redis_failure()
            return
        total = self._extract_pending_count(summary)
        self._last_pending_total = total
        self._last_pending_sample_ts = now
        if self._pending_warn_threshold > 0 and total >= self._pending_warn_threshold:
            await self._emit_telemetry(
                "price_shadow_pending_warn",
                "GLOBAL",
                {
                    "pending": total,
                    "threshold": self._pending_warn_threshold,
                },
            )

    @staticmethod
    def _extract_pending_count(summary: Any) -> int:  # type: ignore[reportUnknownParameterType]
        if summary is None:
            return 0
        try:
            if isinstance(summary, Mapping):
                for key in ("count", "pending", "total"):
                    if key in summary and summary[key] is not None:
                        return int(summary[key])
                first_value = next(iter(summary.values()))
                return int(first_value)
            if isinstance(summary, (list, tuple)) and summary:
                return int(summary[0])
            return int(summary)
        except Exception:
            return 0

    async def _emit_telemetry(
        self, event: str, symbol: str, payload: dict[str, Any]
    ) -> None:
        if self._telemetry is None:
            return
        try:
            await self._telemetry(event, symbol, payload)
        except Exception:  # pragma: no cover
            logger.debug("Telemetry sink failed: %s", event, exc_info=True)


__all__ = [
    "PriceStreamShadowConsumer",
    "ShadowStreamMessage",
]
