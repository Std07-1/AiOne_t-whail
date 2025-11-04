"""StrategyService — shadow-сервіс для обробки Stage2 сигналів (Stage3/shadow).

Перенесено з каталогу services/ для кращої організації Stage3.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from redis.asyncio import Redis

from config.config import (
    STAGE3_SHADOW_DUAL_WRITE_ENABLED,
    STAGE3_SHADOW_PIPELINE_ENABLED,
    STAGE3_SHADOW_STREAM_CFG,
    STAGE3_SHADOW_STREAMS,
)
from monitoring.telemetry_sink import log_stage3_event
from stage3.types import StrategySignal, TradeCommandShadow

logger = logging.getLogger("stage3.shadow.strategy_service")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False


class StrategyService:
    """Shadow StrategyService, який читає Stage2 сигнали та формує команди OPEN."""

    def __init__(
        self,
        redis: Redis,
        *,
        stream_key: str | None = None,
        command_stream_key: str | None = None,
        consumer_group: str | None = None,
        consumer_name: str | None = None,
        read_block_ms: int | None = None,
        dual_write: bool | None = None,
    ) -> None:
        self._redis = redis
        self._stream_key = stream_key or STAGE3_SHADOW_STREAMS["STRATEGY_OPEN"]
        self._command_stream_key = (
            command_stream_key or STAGE3_SHADOW_STREAMS["TRADE_COMMAND"]
        )
        self._consumer_group = (
            consumer_group or STAGE3_SHADOW_STREAM_CFG["STRATEGY_GROUP"]
        )
        self._consumer_name = consumer_name or "strategy-shadow"
        self._read_block_ms = read_block_ms or STAGE3_SHADOW_STREAM_CFG["READ_BLOCK_MS"]
        self._dual_write = (
            STAGE3_SHADOW_DUAL_WRITE_ENABLED if dual_write is None else dual_write
        )
        self._running = False

    async def ensure_group(self) -> None:
        """Створює consumer group для shadow Stream, якщо вона ще не існує."""

        if not STAGE3_SHADOW_PIPELINE_ENABLED:
            logger.info("Shadow pipeline вимкнено — StrategyService не активний")
            return
        try:
            await self._redis.xgroup_create(
                self._stream_key,
                self._consumer_group,
                id="0-0",
                mkstream=True,
            )
            logger.info(
                "StrategyService створив shadow group %s для stream %s",
                self._consumer_group,
                self._stream_key,
            )
        except Exception as exc:  # pragma: no cover - захист від гонок
            msg = str(exc).lower()
            if "busygroup" in msg:
                logger.debug(
                    "StrategyService: consumer group %s вже існує", self._consumer_group
                )
            else:
                logger.exception("Не вдалося створити consumer group: %s", exc)
                raise

    async def run(self) -> None:
        """Основний цикл читання Stage2 сигналів і формування команд OPEN."""

        if not STAGE3_SHADOW_PIPELINE_ENABLED:
            logger.info("Shadow pipeline вимкнено — StrategyService завершує роботу")
            return
        self._running = True
        await self.ensure_group()
        last_id = ">"
        while self._running:
            try:
                response = await self._redis.xreadgroup(
                    self._consumer_group,
                    self._consumer_name,
                    {self._stream_key: last_id},
                    block=self._read_block_ms,
                    count=50,
                )
            except Exception:  # pragma: no cover - захист від збоїв Redis
                logger.exception("StrategyService: помилка читання зі stream")
                await asyncio.sleep(1.0)
                continue

            if not response:
                continue

            for _, entries in response:
                for entry_id, payload in entries:
                    await self._handle_entry(entry_id, payload)

    async def _handle_entry(self, entry_id: str, payload: dict[bytes, bytes]) -> None:
        ts_received = time.time()
        try:
            signal = self._decode_signal(payload)
        except Exception as exc:
            logger.exception("StrategyService: не вдалося декодувати сигнал: %s", exc)
            await self._ack(entry_id)
            return

        if not self._is_valid_signal(signal):
            logger.debug("StrategyService: відхилено невалідний сигнал %s", signal)
            await self._ack(entry_id)
            return

        command: TradeCommandShadow = {
            "command": "OPEN",
            "symbol": signal["symbol"],
            "payload": signal.get("trade_payload", {}),
            "request_ts": ts_received,
            "pipeline": "shadow",
            "stream_id": entry_id,
            "dual_write": self._dual_write,
        }

        try:
            command_payload = json.dumps(command["payload"])
            fields = {
                "command": command["command"],
                "symbol": command["symbol"],
                "payload": command_payload,
                "request_ts": f"{command['request_ts']:.6f}",
                "pipeline": command["pipeline"],
                "stream_id": command["stream_id"],
                "dual_write": "1" if command["dual_write"] else "0",
            }
            await self._redis.xadd(
                self._command_stream_key,
                fields=fields,
                maxlen=STAGE3_SHADOW_STREAM_CFG["MAXLEN"],
                approximate=True,
            )
        except Exception:  # pragma: no cover - захист від збоїв Redis
            logger.exception(
                "StrategyService: не вдалося записати команду у shadow stream"
            )
            await log_stage3_event(
                "strategy_shadow_error",
                signal["symbol"],
                {"reason": "redis_write_failed"},
            )
        else:
            await log_stage3_event(
                "strategy_shadow_open_enqueued",
                signal["symbol"],
                {
                    "entry_id": entry_id,
                    "lag_ms": self._calc_lag_ms(signal, ts_received),
                },
            )

        await self._ack(entry_id)

    async def _ack(self, entry_id: str) -> None:
        try:
            await self._redis.xack(self._stream_key, self._consumer_group, entry_id)
        except Exception:  # pragma: no cover - захист від збоїв Redis
            logger.exception("StrategyService: не вдалося xack %s", entry_id)

    def _decode_signal(self, payload: dict[bytes, bytes]) -> StrategySignal:
        result: StrategySignal = {}
        for key, value in payload.items():
            key_str = key.decode("utf-8")
            if key_str in {"payload", "trade_payload"}:
                result["trade_payload"] = json.loads(value)
            elif key_str == "raw_signal":
                result["raw_signal"] = json.loads(value)
            elif key_str == "guard_flags":
                result["guard_flags"] = json.loads(value)
            elif key_str in {"size", "expiry_ts", "ts"}:
                result[key_str] = float(value)
            else:
                result[key_str] = value.decode("utf-8")
        return result

    @staticmethod
    def _calc_lag_ms(signal: StrategySignal, ts_received: float) -> int:
        signal_ts = signal.get("ts")
        try:
            base_ts = float(signal_ts) if signal_ts is not None else ts_received
        except (TypeError, ValueError):
            base_ts = ts_received
        lag = max(ts_received - base_ts, 0.0)
        return int(lag * 1000)

    def _is_valid_signal(self, signal: StrategySignal) -> bool:
        if not signal.get("symbol"):
            return False
        if "trade_payload" not in signal:
            return False
        return True

    async def stop(self) -> None:
        self._running = False


__all__ = ["StrategyService"]
