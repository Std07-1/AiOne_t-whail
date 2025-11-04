"""Shadow price stream producer для Stage3.

Етап 1 міграції: сервіс публікує оновлення цін у shadow-stream, маючи
WS‑джерело та легкий REST‑fallback (полінговий) за конфігурацією.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from redis.asyncio import Redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)

from config.config import (
    PRICE_STREAM_SERVICE_CFG,
    STAGE3_SHADOW_PIPELINE_ENABLED,
    STAGE3_SHADOW_PRICE_STREAM_CFG,
)
from data.redis_connection import acquire_redis, release_redis
from monitoring.telemetry_schema import SHADOW_PRICE_EVENTS
from monitoring.telemetry_sink import log_stage3_event
from utils.dns_resolver import (
    get_cached_addresses,
    mark_success,
    prefetch_host,
    remember_success,
)

logger = logging.getLogger("stage3.price_stream.shadow")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False


def _is_ws_closed_error(exc: BaseException) -> bool:
    """Перевіряє, чи є закриття WebSocket очікуваним."""

    try:
        from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
    except Exception:  # pragma: no cover - опційна залежність
        return False
    return isinstance(exc, (ConnectionClosedError, ConnectionClosedOK))


@dataclass(slots=True)
class PriceStreamConfig:
    """Конфігурація стріму цін (WS + REST fallback)."""

    ws_initial_backoff_sec: float
    ws_backoff_multiplier: float
    ws_backoff_cap_sec: float
    rest_poll_interval_sec: float
    rest_timeout_sec: float
    queue_maxsize: int
    watchdog_lag_warn_sec: float
    stale_threshold_sec: float
    queue_drop_warn_cooldown_sec: float

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> PriceStreamConfig:
        """Створює конфіг на базі словника (із дефолтами)."""

        return cls(
            ws_initial_backoff_sec=float(data.get("ws_initial_backoff_sec", 3.0)),
            ws_backoff_multiplier=float(data.get("ws_backoff_multiplier", 1.8)),
            ws_backoff_cap_sec=float(data.get("ws_backoff_cap_sec", 60.0)),
            rest_poll_interval_sec=float(data.get("rest_poll_interval_sec", 5.0)),
            rest_timeout_sec=float(data.get("rest_timeout_sec", 4.0)),
            queue_maxsize=int(data.get("queue_maxsize", 2000)),
            watchdog_lag_warn_sec=float(data.get("watchdog_lag_warn_sec", 20.0)),
            stale_threshold_sec=float(data.get("stale_threshold_sec", 60.0)),
            queue_drop_warn_cooldown_sec=float(
                data.get("queue_drop_warn_cooldown_sec", 15.0)
            ),
        )


@dataclass(slots=True)
class PriceUpdate:
    """Подія оновлення ціни для Stage3."""

    symbol: str
    timestamp: float
    source: Literal["ws", "rest"]
    bid: float | None = None
    ask: float | None = None
    last_price: float | None = None
    volume: float | None = None
    stale_age: float | None = None
    latency: float | None = None
    sequence_id: int | None = None
    ingested_ts: float | None = None


class PriceHealthWatchdog:
    """Відстежує лаг останнього оновлення ціни."""

    def __init__(
        self,
        stale_threshold_sec: float,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._clock = clock or time.monotonic
        self._stale_threshold = max(stale_threshold_sec, 1.0)
        self._last_ts: dict[str, float] = {}

    def record(self, symbol: str, ts: float) -> None:
        if symbol:
            self._last_ts[symbol] = ts

    def lag_sec(self, symbol: str) -> float | None:
        if not symbol:
            return None
        last_ts = self._last_ts.get(symbol)
        if last_ts is None:
            return None
        return max(0.0, self._clock() - last_ts)

    def is_stale(self, symbol: str) -> bool:
        lag = self.lag_sec(symbol)
        if lag is None:
            return True
        return lag >= self._stale_threshold


class PriceStreamShadowProducer:
    """Відправляє оновлення цін у shadow Redis Stream."""

    def __init__(
        self,
        *,
        config: PriceStreamConfig | None = None,
        stream_cfg: Mapping[str, Any] | None = None,
    ) -> None:
        self._config = config or PriceStreamConfig.from_mapping(
            PRICE_STREAM_SERVICE_CFG
        )
        self._stream_cfg = dict(stream_cfg or STAGE3_SHADOW_PRICE_STREAM_CFG)
        self._stream_key: str = str(self._stream_cfg.get("STREAM_KEY"))
        if not self._stream_key:
            raise ValueError("STREAM_KEY для shadow price stream не налаштований")
        self._maxlen: int = int(self._stream_cfg.get("MAXLEN", 10000))
        self._symbols: list[str] = []
        self._ws_source: (
            Callable[[Sequence[str]], AsyncIterator[PriceUpdate]] | None
        ) = None
        self._rest_source: (
            Callable[[Sequence[str]], Awaitable[list[PriceUpdate]]] | None
        ) = None
        self._telemetry: (
            Callable[[str, str, dict[str, Any]], Awaitable[None]] | None
        ) = None
        self._stop_event: asyncio.Event | None = None
        self._ws_task: asyncio.Task[Any] | None = None
        self._rest_task: asyncio.Task[Any] | None = None
        self._health_task: asyncio.Task[Any] | None = None
        self._redis: Redis | None = None
        self._owns_redis: bool = False
        self._health = PriceHealthWatchdog(self._config.stale_threshold_sec)
        self._last_failover_ts: float = 0.0
        self._last_recover_ts: float = 0.0
        self._failover_warn_ts: float = 0.0

    @property
    def health(self) -> PriceHealthWatchdog:
        return self._health

    async def start(
        self,
        symbols: Sequence[str],
        *,
        ws_source: Callable[[Sequence[str]], AsyncIterator[PriceUpdate]] | None = None,
        rest_source: (
            Callable[[Sequence[str]], Awaitable[list[PriceUpdate]]] | None
        ) = None,
        telemetry_sink: (
            Callable[[str, str, dict[str, Any]], Awaitable[None]] | None
        ) = None,
    ) -> None:
        if not STAGE3_SHADOW_PIPELINE_ENABLED:
            raise RuntimeError("Shadow pipeline вимкнений через конфіг")
        if self._ws_task is not None:
            raise RuntimeError("PriceStreamShadowProducer уже запущено")
        if not symbols:
            raise ValueError("Не задано символів для shadow price stream")

        self._symbols = [str(sym).upper() for sym in symbols if sym]
        if not self._symbols:
            raise ValueError("Список символів порожній після нормалізації")

        self._ws_source = ws_source or self._default_ws_source
        if self._config.rest_poll_interval_sec > 0:
            self._rest_source = rest_source or self._default_rest_fetch
        self._telemetry = telemetry_sink or log_stage3_event

        await self._ensure_redis()

        self._stop_event = asyncio.Event()
        self._ws_task = asyncio.create_task(self._run_ws_loop(), name="price_ws")
        if self._rest_source is not None:
            self._rest_task = asyncio.create_task(
                self._run_rest_loop(), name="price_rest"
            )
        self._health_task = asyncio.create_task(
            self._run_health_loop(), name="price_health"
        )

        logger.info("PriceStreamShadowProducer запущено у режимі shadow_stream")

    async def stop(self) -> None:
        if self._stop_event is None:
            return
        self._stop_event.set()
        tasks = [
            t
            for t in (self._ws_task, self._rest_task, self._health_task)
            if t is not None
        ]
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("PriceStreamShadowProducer stop task failed")
        self._ws_task = None
        self._rest_task = None
        self._health_task = None
        self._stop_event = None
        await self._close_redis()

    async def _run_ws_loop(self) -> None:
        assert self._ws_source is not None
        assert self._stop_event is not None
        backoff = self._config.ws_initial_backoff_sec
        logger.info("WS-стрім shadow: %s", ",".join(self._symbols))
        while not self._stop_event.is_set():
            try:
                async for update in self._ws_source(self._symbols):
                    symbol = update.symbol.upper()
                    if symbol not in self._symbols:
                        continue
                    update.symbol = symbol
                    update.source = "ws"
                    self._health.record(symbol, update.timestamp)
                    await self._ingest_update(update)
                await asyncio.sleep(min(1.0, backoff))
            except asyncio.CancelledError:
                raise
            except ConnectionResetError as exc:
                logger.warning("WS loop connection reset: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(
                    backoff * self._config.ws_backoff_multiplier,
                    self._config.ws_backoff_cap_sec,
                )
            except Exception as exc:
                if _is_ws_closed_error(exc):
                    logger.info("WS loop reconnect (close=%s)", exc)
                else:
                    logger.warning("WS loop error: %s", exc, exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(
                    backoff * self._config.ws_backoff_multiplier,
                    self._config.ws_backoff_cap_sec,
                )

    async def _run_rest_loop(self) -> None:
        assert self._rest_source is not None
        assert self._stop_event is not None
        interval = max(0.05, self._config.rest_poll_interval_sec)
        while not self._stop_event.is_set():
            await asyncio.sleep(interval)
            try:
                updates = await asyncio.wait_for(
                    self._rest_source(self._symbols),
                    timeout=self._config.rest_timeout_sec,
                )
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("REST fallback не вдався")
                continue
            for update in updates or []:
                symbol = update.symbol.upper()
                if symbol not in self._symbols:
                    continue
                update.symbol = symbol
                update.source = "rest"
                update.timestamp = update.timestamp or time.time()
                self._health.record(symbol, update.timestamp)
                await self._ingest_update(update)

    async def _run_health_loop(self) -> None:
        assert self._stop_event is not None
        interval = max(0.5, min(self._config.watchdog_lag_warn_sec, 5.0))
        while not self._stop_event.is_set():
            await asyncio.sleep(interval)
            for symbol in self._symbols:
                if not self._health.is_stale(symbol):
                    continue
                lag = self._health.lag_sec(symbol)
                payload = {
                    "lag_sec": lag,
                    "threshold": self._config.stale_threshold_sec,
                    "redis_stream": self._stream_key,
                }
                await self._emit_telemetry("price_shadow_health", symbol, payload)

    async def _ingest_update(self, update: PriceUpdate) -> None:
        update.ingested_ts = time.time()
        latency_base = update.timestamp or update.ingested_ts
        latency_ms = max(0.0, (update.ingested_ts - latency_base) * 1000.0)
        try:
            redis_id = await self._write_stream(update)
        except Exception as exc:  # pragma: no cover - мережеві збої
            await self._handle_failover(update.symbol, exc)
            return
        payload = {
            "ingest_ts": update.ingested_ts,
            "source": update.source,
            "latency_ms": latency_ms,
            "redis_stream": self._stream_key,
            "redis_id": redis_id,
            "mode": "shadow_stream",
        }
        if "price_shadow_ingest" in SHADOW_PRICE_EVENTS:
            payload.setdefault("symbol", update.symbol)
        await self._emit_telemetry("price_shadow_ingest", update.symbol, payload)

    async def _write_stream(self, update: PriceUpdate) -> str:
        await self._ensure_redis()
        if self._redis is None:
            raise RuntimeError("Redis клієнт недоступний для shadow price stream")
        fields = self._build_stream_fields(update)
        maxlen = self._maxlen
        # Невеликий запас за часом і одна додаткова спроба — менше спорадичних таймаутів
        attempts = 2
        deadline = time.monotonic() + 3.0
        last_exc: Exception | None = None
        for attempt in range(attempts + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                redis_id = await asyncio.wait_for(
                    self._redis.xadd(
                        self._stream_key,
                        fields,
                        maxlen=maxlen,
                        approximate=True,
                    ),
                    timeout=remaining,
                )
                return str(redis_id)
            except (TimeoutError, RedisTimeoutError, RedisConnectionError) as exc:
                last_exc = exc
                await self._close_redis()
                await asyncio.sleep(min(0.2 * (attempt + 1), 1.0))
                await self._ensure_redis()
            except ResponseError:
                await self._ensure_redis()
        raise RuntimeError("Не вдалося записати у shadow stream") from last_exc

    async def _handle_failover(self, symbol: str, exc: Exception) -> None:
        # CancelledError часто означає штатне скасування задачі при зупинці сервісу.
        # Не вважаємо це збоєм Redis і не створюємо шум у логах/телеметрії.
        try:
            import asyncio as _asyncio  # локальний імпорт, щоб не тягнути глобально

            if isinstance(exc, _asyncio.CancelledError):
                logger.info(
                    "Shadow price stream: cancel під час запису %s — припиняємо без failover",
                    symbol,
                )
                return
        except Exception:
            # якщо з import або isinstance щось піде не так — діємо за дефолтним шляхом
            pass
        now = time.time()
        self._last_failover_ts = now
        if (now - self._failover_warn_ts) >= 5.0:
            logger.error(
                "Shadow price stream: не вдалося записати %s (%s)",
                symbol,
                exc,
                exc_info=True,
            )
            self._failover_warn_ts = now
        payload = {
            "reason": repr(exc),
            "symbol": symbol,
        }
        await self._emit_telemetry("price_shadow_failover", symbol, payload)

    async def _ensure_redis(self) -> None:
        if self._redis is not None:
            return
        self._redis = await acquire_redis(decode_responses=True)
        self._owns_redis = True
        await self._ensure_group()
        if self._last_failover_ts and not self._last_recover_ts:
            downtime = time.time() - self._last_failover_ts
            payload = {
                "downtime_sec": downtime,
                "redis_stream": self._stream_key,
            }
            await self._emit_telemetry("price_shadow_recovered", "GLOBAL", payload)
            self._last_recover_ts = time.time()

    async def _ensure_group(self) -> None:
        if self._redis is None:
            return
        group = str(self._stream_cfg.get("GROUP", "price_shadow"))
        try:
            await self._redis.xgroup_create(
                self._stream_key,
                group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _close_redis(self) -> None:
        if not self._owns_redis:
            return
        try:
            await release_redis(self._redis)
        except Exception:  # pragma: no cover - best effort
            logger.debug("release_redis failed", exc_info=True)
        finally:
            self._redis = None
            self._owns_redis = False

    async def _emit_telemetry(
        self,
        event: str,
        symbol: str,
        payload: dict[str, Any],
    ) -> None:
        if self._telemetry is None:
            return
        try:
            await self._telemetry(event, symbol, payload)
        except Exception:  # pragma: no cover - best effort
            logger.debug("telemetry emit failed: %s", event, exc_info=True)

    @staticmethod
    def _build_stream_fields(update: PriceUpdate) -> dict[str, str]:
        ts_value = (
            float(update.timestamp) if update.timestamp is not None else time.time()
        )
        payload: dict[str, Any] = {
            "symbol": update.symbol.upper(),
            "ts": f"{ts_value:.6f}",
            "source": update.source,
            "ingested_ts": f"{(update.ingested_ts or time.time()):.6f}",
        }
        if update.bid is not None:
            payload["bid"] = f"{float(update.bid):.6f}"
        if update.ask is not None:
            payload["ask"] = f"{float(update.ask):.6f}"
        if update.last_price is not None:
            payload["last_price"] = f"{float(update.last_price):.6f}"
        if update.volume is not None:
            payload["volume"] = f"{float(update.volume):.6f}"
        if update.latency is not None:
            payload["latency"] = f"{float(update.latency):.6f}"
        if update.sequence_id is not None:
            payload["sequence_id"] = str(int(update.sequence_id))
        return payload

    async def _default_ws_source(
        self,
        symbols: Sequence[str],
    ) -> AsyncIterator[PriceUpdate]:  # pragma: no cover - зовнішній WS
        import websockets

        host = "fstream.binance.com"
        normalized = [sym.lower() for sym in symbols]
        stream_path = "/".join(f"{sym}@bookTicker" for sym in normalized)
        url = f"wss://{host}/stream?streams={stream_path}"
        await prefetch_host(host)
        base_kwargs = {"ping_interval": 20, "ping_timeout": 20}

        async def _yield_updates(
            ws: websockets.WebSocketClientProtocol,
        ) -> AsyncIterator[PriceUpdate]:
            async for raw_message in ws:
                try:
                    data = json.loads(raw_message)
                except json.JSONDecodeError:
                    continue
                payload = data.get("data", data)
                symbol = str(payload.get("s") or "").upper()
                if not symbol:
                    continue
                event_time_ms = payload.get("E") or int(time.time() * 1000)
                bid = payload.get("b")
                ask = payload.get("a")
                last_price = payload.get("c") or payload.get("p")
                yield PriceUpdate(
                    symbol=symbol,
                    timestamp=event_time_ms / 1000.0,
                    source="ws",
                    bid=float(bid) if bid is not None else None,
                    ask=float(ask) if ask is not None else None,
                    last_price=float(last_price) if last_price is not None else None,
                )

        try:
            async with websockets.connect(url, **base_kwargs) as ws:
                await mark_success(host)
                async for update in _yield_updates(ws):
                    yield update
                return
        except socket.gaierror as dns_exc:
            cached_ips = get_cached_addresses(host)
            if not cached_ips:
                raise
            logger.warning(
                "[STRICT_PHASE] WS DNS fallback активовано для %s (%s)", host, dns_exc
            )
            last_exc: Exception = dns_exc
            for ip in cached_ips:
                try:
                    async with websockets.connect(
                        url,
                        **base_kwargs,
                        host=ip,
                        port=443,
                        server_hostname=host,
                    ) as ws:
                        await remember_success(host, (ip,))
                        async for update in _yield_updates(ws):
                            yield update
                        return
                except socket.gaierror as ip_exc:
                    last_exc = ip_exc
                    continue
            raise last_exc from None

    async def _default_rest_fetch(
        self,
        symbols: Sequence[str],
    ) -> list[PriceUpdate]:  # pragma: no cover - зовнішній API
        await asyncio.sleep(self._config.rest_poll_interval_sec)
        return []


__all__ = [
    "PriceStreamConfig",
    "PriceUpdate",
    "PriceHealthWatchdog",
    "PriceStreamShadowProducer",
]
