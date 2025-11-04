"""Сервіс оновлення Stage3 угод на базі shadow price stream.

Сервіс приймає оновлення цін із shadow Redis Stream через переданий
consumer та синхронізує їх із TradeLifecycleManager. Мета – знизити
latency між надходженням ціни та оновленням угод, а також записувати
ключові метрики в телеметрію.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Protocol

from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    RedisError,
    TimeoutError as RedisTimeoutError,
)

from monitoring.telemetry_sink import log_stage3_event
from stage3.price_stream_shadow import PriceUpdate
from stage3.price_stream_shadow_consumer import PriceStreamShadowConsumer
from stage3.trade_manager import TradeLifecycleManager

logger = logging.getLogger("stage3.trade_update_service")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False


class HealthTrackerProto(Protocol):
    """Мінімальний протокол health-трекера для updater."""

    def record_refresh(
        self,
        symbol: str,
        *,
        data_ts: float | None = None,
        wall_ts: float | None = None,
    ) -> None:
        """Фіксує факт оновлення символа."""


class TradeUpdateService:
    """Постачає live-оновлення цін для Stage3 угод."""

    def __init__(
        self,
        trade_manager: TradeLifecycleManager,
        consumer: PriceStreamShadowConsumer,
        *,
        telemetry_sink=log_stage3_event,
    ) -> None:
        self._trade_manager = trade_manager
        self._consumer = consumer
        self._telemetry = telemetry_sink
        self._consumer_started = False

    async def aclose(self) -> None:
        """Закриває ресурсні залежності (Redis consumer)."""

        if self._consumer_started:
            try:
                await self._consumer.stop()
            except Exception:  # pragma: no cover - best effort
                logger.debug("Не вдалося коректно зупинити consumer", exc_info=True)
            finally:
                self._consumer_started = False

    async def drain_updates(
        self,
        *,
        health_tracker: HealthTrackerProto | None = None,
        limit: int | None = None,
    ) -> dict[str, PriceUpdate]:
        """Знімає оновлення та повертає останнє по кожному символу."""

        if not self._consumer:
            return {}

        await self._ensure_consumer()
        try:
            stream_messages = await self._consumer.fetch(limit)
        except (RedisError, RedisTimeoutError, RedisConnectionError) as exc:
            logger.warning("PriceUpdate stream read failed: %s", exc)
            return {}

        updates_map: dict[str, tuple[PriceUpdate, str | None]] = {}
        for message in stream_messages:
            symbol = message.update.symbol.upper()
            keep = updates_map.get(symbol)
            if keep is None or message.update.timestamp >= keep[0].timestamp:
                updates_map[symbol] = (message.update, message.message_id)

        if not updates_map:
            return {}

        pending_count = self._consumer.last_pending_total
        ack_ids: list[str] = []
        processed: dict[str, PriceUpdate] = {}

        for symbol, (update, message_id) in updates_map.items():
            await self._apply_update(
                symbol,
                update,
                pending_count=pending_count,
                message_id=message_id,
                health_tracker=health_tracker,
            )
            if message_id is not None:
                ack_ids.append(message_id)
            processed[symbol] = update

        if ack_ids:
            try:
                await self._consumer.ack(ack_ids)
            except (RedisError, RedisTimeoutError, RedisConnectionError):
                logger.debug("ACK failure для shadow stream", exc_info=True)

        return processed

    async def _apply_update(
        self,
        symbol: str,
        update: PriceUpdate,
        *,
        pending_count: int,
        message_id: str | None,
        health_tracker: HealthTrackerProto | None,
    ) -> None:
        price = self._resolve_price(update)
        if price is not None:
            await self._update_trades(symbol, price, update)

        now_wall = time.time()
        if health_tracker is not None:
            try:
                health_tracker.record_refresh(
                    symbol,
                    data_ts=update.ingested_ts or update.timestamp,
                    wall_ts=now_wall,
                )
            except Exception:  # pragma: no cover - health optional
                logger.debug("Health tracker record failed", exc_info=True)

        await self._emit_telemetry(
            symbol,
            update,
            pending_count=pending_count,
            message_id=message_id,
            now_wall=now_wall,
        )

    async def _update_trades(
        self, symbol: str, price: float, update: PriceUpdate
    ) -> None:
        trade_ids = await self._collect_trade_ids(symbol)
        if not trade_ids:
            return
        payload: dict[str, Any] = {"price": price}
        if update.volume is not None:
            payload["volume"] = update.volume
        for trade_id in trade_ids:
            try:
                await self._trade_manager.update_trade(trade_id, payload)
            except Exception:
                logger.debug("Не вдалося оновити угоду %s для %s", trade_id, symbol)

    async def _collect_trade_ids(self, symbol: str) -> list[str]:
        symbol_norm = symbol.upper()
        lock = getattr(self._trade_manager, "lock", None)
        active = getattr(self._trade_manager, "active_trades", None)
        if isinstance(lock, asyncio.Lock) and isinstance(active, dict):
            try:
                async with lock:
                    return [
                        trade_id
                        for trade_id, trade in active.items()
                        if getattr(trade, "symbol", "").upper() == symbol_norm
                    ]
            except Exception:
                logger.debug("Активні угоди недоступні через lock", exc_info=True)
        result: list[str] = []
        try:
            snapshot = await self._trade_manager.get_active_trades()
        except Exception:
            logger.debug("Не вдалося отримати snapshot активних угод", exc_info=True)
            return result
        for entry in snapshot:
            sym_val = str(entry.get("symbol", "")).upper()
            if sym_val == symbol_norm:
                trade_id = entry.get("id")
                if isinstance(trade_id, str) and trade_id:
                    result.append(trade_id)
        return result

    async def _emit_telemetry(
        self,
        symbol: str,
        update: PriceUpdate,
        *,
        pending_count: int,
        message_id: str | None,
        now_wall: float,
    ) -> None:
        if self._telemetry is None:
            return
        latency_ms = max(0.0, (now_wall - update.timestamp) * 1000.0)
        delivery_ref = update.ingested_ts or update.timestamp
        delivery_latency_ms = max(0.0, (now_wall - delivery_ref) * 1000.0)
        payload: dict[str, Any] = {
            "symbol": symbol,
            "latency_ms": round(latency_ms, 3),
            "delivery_latency_ms": round(delivery_latency_ms, 3),
            "pending_count": int(pending_count),
        }
        if message_id is not None:
            payload["redis_id"] = message_id
        if update.source:
            payload["source"] = update.source
        if update.sequence_id is not None:
            payload["sequence_id"] = int(update.sequence_id)
        try:
            await self._telemetry("price_update_processed", symbol, payload)
        except Exception:  # pragma: no cover - best effort
            logger.debug("Telemetry emit failed", exc_info=True)

    async def _ensure_consumer(self) -> None:
        if self._consumer_started:
            return
        try:
            await self._consumer.start()
            self._consumer_started = True
        except Exception as exc:
            raise RuntimeError("Не вдалося стартувати price stream consumer") from exc

    def _resolve_price(self, update: PriceUpdate) -> float | None:
        if update.last_price is not None and update.last_price > 0:
            return float(update.last_price)
        if update.bid is not None and update.ask is not None:
            if update.bid > 0 and update.ask > 0:
                return (update.bid + update.ask) / 2.0
        if update.bid is not None and update.bid > 0:
            return float(update.bid)
        if update.ask is not None and update.ask > 0:
            return float(update.ask)
        return None


__all__ = ["TradeUpdateService"]
