"""Споживач shadow-stream для Stage3 стратегічних сигналів.

Реалізує асинхронне читання Redis Stream `strategy_open`, коалесує
повідомлення та повертає їх у вигляді `StrategyStreamMessage`. Сервіс
побудований за аналогією з price shadow consumer, але оптимізований для
невеликого обсягу повідомлень Stage2 → Stage3.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from collections import deque
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)

from config.config import STAGE3_SHADOW_STREAM_CFG, STAGE3_SHADOW_STREAMS
from data.redis_connection import acquire_redis, release_redis
from data.redis_stream_utils import (
    normalize_xautoclaim_response,
    xpending_xclaim_fallback,
)
from monitoring.telemetry_sink import log_stage3_event
from stage3.types import StrategySignal

logger = logging.getLogger("stage3.strategy_signal_stream")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False


@dataclass(slots=True)
class StrategyStreamMessage:
    """Контейнер для повідомлення зі shadow strategy stream."""

    signal: StrategySignal
    message_id: str
    received_ts: float


class StrategySignalStream:
    """Інкапсулює читання Redis stream із Stage2 сигналами."""

    def __init__(
        self,
        *,
        stream_cfg: Mapping[str, Any] | None = None,
        consumer_name: str | None = None,
        telemetry_sink=log_stage3_event,
    ) -> None:
        cfg = dict(stream_cfg or STAGE3_SHADOW_STREAM_CFG)
        self._stream_key: str = str(
            cfg.get("STRATEGY_STREAM_KEY") or STAGE3_SHADOW_STREAMS["STRATEGY_OPEN"]
        )
        if not self._stream_key:
            raise ValueError("StrategySignalStream: STREAM_KEY не заданий")

        self._group: str = str(cfg.get("STRATEGY_GROUP", "strategy_shadow"))
        self._block_ms: int = int(cfg.get("READ_BLOCK_MS", 1000))
        self._batch_size: int = int(cfg.get("BATCH_SIZE", 50))
        self._idle_claim_ms: int = int(float(cfg.get("IDLE_CLAIM_SEC", 30.0)) * 1000)
        self._reconnect_delay: float = float(cfg.get("RECONNECT_DELAY_SEC", 0.5))
        self._idle_sleep: float = float(cfg.get("IDLE_SLEEP_SEC", 0.2))

        prefix = str(cfg.get("CONSUMER_PREFIX", "strategy-shadow"))
        host = socket.gethostname()
        pid = os.getpid()
        self._consumer_name = consumer_name or f"{prefix}-{host}-{pid}"

        self._telemetry = telemetry_sink

        self._redis: Redis | None = None
        self._owns_redis = False
        self._buffer: deque[StrategyStreamMessage] = deque()
        self._pending_ack: list[str] = []
        self._lock = asyncio.Lock()

    @property
    def stream_key(self) -> str:
        """Поточний Redis stream key."""

        return self._stream_key

    async def start(self) -> None:
        """Готує Redis-підключення та consumer group."""

        async with self._lock:
            await self._ensure_redis()

    async def stop(self) -> None:
        """Закриває Redis-підключення та очищає буфери."""

        async with self._lock:
            await self._close_redis()
            self._buffer.clear()
            self._pending_ack.clear()

    async def iterate(self) -> AsyncIterator[StrategyStreamMessage]:
        """Асинхронний генератор повідомлень shadow stream."""

        while True:
            try:
                message = await self._next_message()
            except asyncio.CancelledError:
                raise
            if message is None:
                await asyncio.sleep(self._idle_sleep)
                continue
            yield message

    async def ack(self, message_id: str) -> None:
        """Позначає повідомлення як оброблене."""

        if not message_id:
            return
        async with self._lock:
            if self._redis is None:
                return
            try:
                await self._redis.xack(self._stream_key, self._group, message_id)
            except (RedisTimeoutError, RedisConnectionError):
                await self._handle_redis_failure("xack_failure")

    async def _next_message(self) -> StrategyStreamMessage | None:
        async with self._lock:
            await self._ensure_redis()
            if self._redis is None:
                return None
            await self._auto_claim_pending()
            if not self._buffer:
                await self._read_stream()
            await self._flush_pending_ack()
            if not self._buffer:
                return None
            return self._buffer.popleft()

    async def _ensure_redis(self) -> None:
        if self._redis is not None:
            return
        try:
            self._redis = await acquire_redis(decode_responses=False)
        except Exception:  # pragma: no cover - best effort
            await self._handle_redis_failure("acquire_failed")
            return
        self._owns_redis = True
        await self._ensure_group()

    async def _ensure_group(self) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.xgroup_create(
                name=self._stream_key,
                groupname=self._group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _auto_claim_pending(self) -> None:
        if self._redis is None or self._idle_claim_ms <= 0:
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
                        self._batch_size,
                    )
                except TypeError:
                    # Fallback for test fakes / older signatures
                    resp = await self._redis.xautoclaim(
                        name=self._stream_key,
                        groupname=self._group,
                        consumername=self._consumer_name,
                        min_idle_time=self._idle_claim_ms,
                        start=start_id,
                        count=self._batch_size,
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
                    self._batch_size,
                )
                start_id = "0-0"
            except (RedisTimeoutError, RedisConnectionError):
                await self._handle_redis_failure("xautoclaim_failure")
                return

            if not messages:
                break
            for message_id, fields in messages:
                self._stage_message(message_id, fields)
            if start_id == "0-0":
                break

    async def _read_stream(self) -> None:
        if self._redis is None:
            return
        try:
            response = await self._redis.xreadgroup(
                groupname=self._group,
                consumername=self._consumer_name,
                streams={self._stream_key: ">"},
                count=self._batch_size,
                block=self._block_ms,
            )
        except (RedisTimeoutError, RedisConnectionError):
            await self._handle_redis_failure("xreadgroup_failure")
            return
        if not response:
            return
        for stream_key, messages in response:
            if stream_key != self._stream_key:
                continue
            for message_id, fields in messages:
                self._stage_message(message_id, fields)

    def _stage_message(self, message_id: str, fields: Mapping[bytes, bytes]) -> None:
        decoded = self._decode_message(fields)
        if decoded is None:
            self._pending_ack.append(message_id)
            return
        self._buffer.append(
            StrategyStreamMessage(
                signal=decoded,
                message_id=message_id,
                received_ts=time.time(),
            )
        )

    def _decode_message(self, payload: Mapping[bytes, bytes]) -> StrategySignal | None:
        result: StrategySignal = {}
        trade_payload: dict[str, Any] | None = None
        guard_flags: list[str] | None = None
        try:
            for key_bytes, value_bytes in payload.items():
                try:
                    key = key_bytes.decode("utf-8")
                except Exception:
                    continue
                try:
                    value_text = value_bytes.decode("utf-8")
                except Exception:
                    continue
                if key in {"payload", "trade_payload"}:
                    trade_payload = json.loads(value_text)
                elif key == "raw_signal":
                    result["raw_signal"] = json.loads(value_text)
                elif key == "guard_flags":
                    guard_flags_raw = json.loads(value_text)
                    if isinstance(guard_flags_raw, list):
                        guard_flags = [str(item) for item in guard_flags_raw]
                elif key in {"size", "expiry_ts", "ts"}:
                    try:
                        result[key] = float(value_text)
                    except (TypeError, ValueError):
                        continue
                else:
                    result[key] = value_text
        except Exception:
            logger.debug(
                "StrategySignalStream: decode failed %s", payload, exc_info=True
            )
            return None

        if trade_payload is None:
            return None
        if not isinstance(trade_payload, dict):
            return None

        result["trade_payload"] = trade_payload
        if guard_flags is not None:
            result["guard_flags"] = guard_flags

        symbol = result.get("symbol") or trade_payload.get("symbol")
        if not symbol:
            return None
        result["symbol"] = str(symbol).upper()

        side = result.get("side") or trade_payload.get("side")
        if isinstance(side, str):
            side_up = side.upper()
            if side_up in {"LONG", "SHORT"}:
                result["side"] = side_up

        return result

    async def _flush_pending_ack(self) -> None:
        if not self._pending_ack or self._redis is None:
            return
        pending, self._pending_ack = self._pending_ack, []
        try:
            await self._redis.xack(self._stream_key, self._group, *pending)
        except (RedisTimeoutError, RedisConnectionError):
            # Якщо ack не вдався, повертаємо назад у буфер для повторної спроби
            self._pending_ack.extend(pending)
            await self._handle_redis_failure("xack_pending_failure")

    async def _handle_redis_failure(self, reason: str) -> None:
        logger.warning("StrategySignalStream: Redis failure (%s)", reason)
        await self._emit_telemetry(reason)
        await self._close_redis()
        await asyncio.sleep(self._reconnect_delay)

    async def _emit_telemetry(self, reason: str) -> None:
        if self._telemetry is None:
            return
        try:
            await self._telemetry(
                "strategy_shadow_error",
                "GLOBAL",
                {"reason": reason},
            )
        except Exception:  # pragma: no cover - телеметрія best-effort
            logger.debug("StrategySignalStream: telemetry emit failed", exc_info=True)

    async def _close_redis(self) -> None:
        if not self._owns_redis:
            self._redis = None
            return
        try:
            await release_redis(self._redis)
        except Exception:  # pragma: no cover - best effort
            logger.debug("StrategySignalStream: release redis failed", exc_info=True)
        finally:
            self._redis = None
            self._owns_redis = False


__all__ = ["StrategySignalStream", "StrategyStreamMessage"]
