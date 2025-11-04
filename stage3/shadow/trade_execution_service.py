"""Shadow TradeExecutionService (Stage3/shadow).

Перенесено з каталогу services/ для упорядкування структури Stage3.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from redis.asyncio import Redis

from config.config import (
    STAGE3_SHADOW_EXECUTION_LIMITS,
    STAGE3_SHADOW_PIPELINE_ENABLED,
    STAGE3_SHADOW_STREAM_CFG,
    STAGE3_SHADOW_STREAMS,
)
from monitoring.telemetry_sink import log_stage3_event
from stage3.types import ExecutionResultShadow, TradeCommandShadow

logger = logging.getLogger("stage3.shadow.trade_execution_service")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False


class TradeExecutionService:
    """Shadow TradeExecutionService для безпечного тестування команд Stage3."""

    def __init__(
        self,
        redis: Redis,
        *,
        command_stream_key: str | None = None,
        results_stream_key: str | None = None,
        consumer_group: str | None = None,
        consumer_name: str | None = None,
        read_block_ms: int | None = None,
        rate_limit_cfg: dict[str, float] | None = None,
    ) -> None:
        self._redis = redis
        self._command_stream_key = (
            command_stream_key or STAGE3_SHADOW_STREAMS["TRADE_COMMAND"]
        )
        self._results_stream_key = (
            results_stream_key or STAGE3_SHADOW_STREAMS["EXECUTION_RESULTS"]
        )
        self._consumer_group = (
            consumer_group or STAGE3_SHADOW_STREAM_CFG["EXECUTION_GROUP"]
        )
        self._consumer_name = consumer_name or "execution-shadow"
        self._read_block_ms = read_block_ms or STAGE3_SHADOW_STREAM_CFG["READ_BLOCK_MS"]

        cfg = rate_limit_cfg or STAGE3_SHADOW_EXECUTION_LIMITS
        self._rate_limit_per_sec = max(float(cfg.get("RATE_LIMIT_PER_SEC", 5.0)), 0.1)
        self._initial_backoff = max(float(cfg.get("INITIAL_BACKOFF_SEC", 0.5)), 0.1)
        self._max_backoff = max(
            float(cfg.get("MAX_BACKOFF_SEC", 5.0)), self._initial_backoff
        )
        self._current_backoff = self._initial_backoff
        self._last_execution_ts: float = 0.0
        self._running = False

    async def ensure_group(self) -> None:
        if not STAGE3_SHADOW_PIPELINE_ENABLED:
            logger.info("Shadow pipeline вимкнено — TradeExecutionService не активний")
            return
        try:
            await self._redis.xgroup_create(
                self._command_stream_key,
                self._consumer_group,
                id="0-0",
                mkstream=True,
            )
            logger.info(
                "TradeExecutionService створив shadow group %s для stream %s",
                self._consumer_group,
                self._command_stream_key,
            )
        except Exception as exc:  # pragma: no cover - захист від гонок
            msg = str(exc).lower()
            if "busygroup" in msg:
                logger.debug(
                    "TradeExecutionService: consumer group %s вже існує",
                    self._consumer_group,
                )
            else:
                logger.exception(
                    "Не вдалося створити consumer group для shadow execution"
                )
                raise

        try:
            await self._redis.xgroup_create(
                self._results_stream_key,
                STAGE3_SHADOW_STREAM_CFG["RESULTS_GROUP"],
                id="0-0",
                mkstream=True,
            )
        except Exception as exc:  # pragma: no cover - захист від гонок
            if "busygroup" not in str(exc).lower():
                logger.exception("Не вдалося створити consumer group для результатів")
                raise

    async def run(self) -> None:
        if not STAGE3_SHADOW_PIPELINE_ENABLED:
            logger.info(
                "Shadow pipeline вимкнено — TradeExecutionService завершує роботу"
            )
            return
        self._running = True
        await self.ensure_group()
        last_id = ">"
        while self._running:
            try:
                response = await self._redis.xreadgroup(
                    self._consumer_group,
                    self._consumer_name,
                    {self._command_stream_key: last_id},
                    block=self._read_block_ms,
                    count=50,
                )
            except Exception:  # pragma: no cover - захист від збоїв Redis
                logger.exception("TradeExecutionService: помилка читання команд")
                await asyncio.sleep(self._current_backoff)
                self._grow_backoff()
                continue

            if not response:
                continue

            for _, entries in response:
                for entry_id, data in entries:
                    await self._handle_entry(entry_id, data)

    async def _handle_entry(self, entry_id: str, data: dict[bytes, bytes]) -> None:
        start_ts = time.time()
        try:
            command = self._decode_command(data)
        except Exception:
            logger.exception("TradeExecutionService: не вдалося декодувати команду")
            await log_stage3_event(
                "execution_shadow_error",
                "UNKNOWN",
                {"reason": "decode_error", "entry_id": entry_id},
            )
            await self._ack(entry_id)
            return

        await self._respect_rate_limit(start_ts)

        try:
            result = await self._simulate_execution(command)
        except Exception:
            logger.exception("TradeExecutionService: збій під час емулювання виконання")
            await log_stage3_event(
                "execution_shadow_error",
                command.get("symbol", "UNKNOWN"),
                {"reason": "simulate_error", "entry_id": entry_id},
            )
            await self._ack(entry_id)
            self._grow_backoff()
            return

        await self._publish_result(result)
        await log_stage3_event(
            "execution_shadow_processed",
            command.get("symbol", "UNKNOWN"),
            {
                "command": command.get("command"),
                "latency_sec": result.get("latency_sec"),
                "status": result.get("status"),
            },
        )
        await self._ack(entry_id)
        self._reset_backoff()

    async def _respect_rate_limit(self, now_ts: float) -> None:
        min_interval = 1.0 / self._rate_limit_per_sec
        elapsed = now_ts - self._last_execution_ts
        if elapsed < min_interval:
            await asyncio.sleep(min_interval - elapsed)
        self._last_execution_ts = time.time()

    async def _simulate_execution(
        self, command: TradeCommandShadow
    ) -> ExecutionResultShadow:
        await asyncio.sleep(0)
        latency = max(time.time() - float(command.get("request_ts", time.time())), 0.0)
        payload = command.get("payload") or {}
        trade_id = self._extract_trade_id(payload)
        status = "accepted"
        reason = None
        if not command.get("symbol"):
            status = "error"
            reason = "missing_symbol"
        result: ExecutionResultShadow = {
            "symbol": command.get("symbol", "UNKNOWN"),
            "trade_id": trade_id,
            "command": command.get("command", "NONE"),
            "status": status,
            "reason": reason,
            "latency_sec": round(latency, 6),
            "executed_at_ts": time.time(),
        }
        return result

    async def _publish_result(self, result: ExecutionResultShadow) -> None:
        fields = {
            "symbol": result.get("symbol", "UNKNOWN"),
            "trade_id": (result.get("trade_id") or ""),
            "command": result.get("command", "NONE"),
            "status": result.get("status", "error"),
            "reason": (result.get("reason") or ""),
            "latency_sec": f"{result.get('latency_sec', 0.0):.6f}",
            "executed_at_ts": f"{result.get('executed_at_ts', time.time()):.6f}",
        }
        try:
            await self._redis.xadd(
                self._results_stream_key,
                fields=fields,
                maxlen=STAGE3_SHADOW_STREAM_CFG["MAXLEN"],
                approximate=True,
            )
        except Exception:  # pragma: no cover - захист від збоїв Redis
            logger.exception("TradeExecutionService: не вдалося записати результат")
            await log_stage3_event(
                "execution_shadow_error",
                result.get("symbol", "UNKNOWN"),
                {"reason": "redis_write_failed"},
            )

    async def _ack(self, entry_id: str) -> None:
        try:
            await self._redis.xack(
                self._command_stream_key, self._consumer_group, entry_id
            )
        except Exception:  # pragma: no cover - захист від збоїв Redis
            logger.exception("TradeExecutionService: не вдалося xack %s", entry_id)

    def _decode_command(self, data: dict[bytes, bytes]) -> TradeCommandShadow:
        command: TradeCommandShadow = {"pipeline": "shadow"}
        for key, value in data.items():
            key_str = key.decode("utf-8")
            if key_str == "payload":
                command["payload"] = self._loads_json(value)
            elif key_str == "request_ts":
                command[key_str] = self._parse_float(value)
            elif key_str == "dual_write":
                decoded = (
                    value.decode("utf-8") if isinstance(value, bytes) else str(value)
                )
                command[key_str] = decoded == "1"
            else:
                command[key_str] = value.decode("utf-8")
        return command

    @staticmethod
    def _parse_float(raw: bytes | str | None) -> float | None:
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_trade_id(payload: dict[str, Any]) -> str | None:
        candidate = payload.get("trade_id")
        if candidate is None:
            return None
        return str(candidate)

    @staticmethod
    def _loads_json(value: bytes | str) -> dict[str, Any]:
        try:
            if isinstance(value, bytes):
                return json.loads(value.decode("utf-8"))
            return json.loads(value)
        except Exception:
            return {}

    def _grow_backoff(self) -> None:
        self._current_backoff = min(self._current_backoff * 2, self._max_backoff)

    def _reset_backoff(self) -> None:
        self._current_backoff = self._initial_backoff

    async def stop(self) -> None:
        self._running = False


__all__ = ["TradeExecutionService"]
