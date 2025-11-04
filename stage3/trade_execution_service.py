"""Stage3 TradeExecutionService.

Призначення:
        • Споживає shadow-stream стратегічних сигналів та відкриває угоди через
            `TradeLifecycleManager`.
    • Обробляє `trade_cmd_queue` для ручних команд (наприклад, CLOSE/MODIFY).
    • Фіксує події у Stage3 телеметрії, щоби зберегти прозорість пайплайна.

Сервіс є проміжною ланкою між Stage2 (стратегіями) та власне
LifecycleManager, забезпечуючи можливість замінити джерела сигналів без
зміни внутрішньої логіки Stage3.
"""

from __future__ import annotations

import asyncio
import logging
from asyncio import CancelledError

from monitoring.telemetry_sink import log_stage3_event
from stage3.strategy_signal_stream import StrategySignalStream, StrategyStreamMessage
from stage3.trade_manager import TradeLifecycleManager
from stage3.types import StrategySignal, TradeCommand
from utils.utils import safe_float

logger = logging.getLogger("stage3.trade_execution_service")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False


class TradeExecutionService:
    """Оркеструє виконання Stage3 команд відкриття/закриття угод."""

    def __init__(
        self,
        trade_manager: TradeLifecycleManager,
        signal_stream: StrategySignalStream,
        *,
        command_queue: asyncio.Queue[TradeCommand] | None = None,
    ) -> None:
        self._trade_manager = trade_manager
        self._signal_stream: StrategySignalStream = signal_stream
        self._command_queue = command_queue

    async def run_open_consumer(self) -> None:
        """Споживає сигнали відкриття з Redis stream."""

        await self._run_stream_consumer()

    async def run_command_consumer(self) -> None:
        """Безкінечно споживає команди з ``trade_cmd_queue``."""

        if self._command_queue is None:
            logger.info(
                "TradeExecutionService: відсутня trade_cmd_queue, споживач не запущений"
            )
            return

        while True:
            command = await self._command_queue.get()
            try:
                await self._process_command(command)
            finally:
                self._command_queue.task_done()

    async def _run_stream_consumer(self) -> None:
        stream = self._signal_stream

        await stream.start()
        try:
            async for message in stream.iterate():
                await self._handle_stream_message(message)
        except CancelledError:
            raise
        finally:
            try:
                await stream.stop()
            except Exception:  # pragma: no cover - best effort
                logger.debug("TradeExecutionService: stream stop failed", exc_info=True)

    async def _handle_stream_message(self, message: StrategyStreamMessage) -> None:
        stream = self._signal_stream

        try:
            await self._process_open_signal(message.signal)
        except CancelledError:
            raise
        except Exception:  # pragma: no cover - обробка не має зупиняти стрім
            logger.exception(
                "TradeExecutionService: помилка обробки сигналу %s", message.signal
            )
        finally:
            try:
                await stream.ack(message.message_id)
            except Exception:  # pragma: no cover - захист від Redis збоїв
                logger.warning(
                    "TradeExecutionService: не вдалося ack stream message %s",
                    message.message_id,
                )

    async def _process_open_signal(self, signal: StrategySignal) -> None:
        symbol = str(signal.get("symbol", "") or "").upper()
        trade_payload = signal.get("trade_payload")
        if not isinstance(trade_payload, dict):
            logger.debug(
                "Пропуск сигналу на відкриття для %s: trade_payload відсутній", symbol
            )
            return

        try:
            trade_id = await self._trade_manager.open_trade(trade_payload)
        except Exception:  # pragma: no cover - safeguard від непередбачених помилок
            logger.exception("Не вдалося відкрити угоду для %s", symbol)
            await log_stage3_event(
                "open_failed",
                symbol,
                {"reason": "trade_manager_exception"},
            )
            return

        if not trade_id:
            logger.info("TradeLifecycleManager відхилив відкриття угоди для %s", symbol)
            await log_stage3_event(
                "open_rejected",
                symbol,
                {"reason": "trade_manager_reject"},
            )
            return

        await log_stage3_event(
            "open_executed",
            symbol,
            {
                "trade_id": trade_id,
                "strategy": trade_payload.get("strategy"),
                "entry_price": trade_payload.get("current_price"),
                "queued": True,
            },
        )
        logger.info(
            "✅ TradeExecutionService відкрив угоду %s (trade_id=%s)", symbol, trade_id
        )

    async def _process_command(self, command: TradeCommand) -> None:
        cmd_type = command.get("command")
        symbol = str(command.get("symbol", "") or "").upper()
        if cmd_type == "CLOSE":
            await self._handle_close_command(symbol, command)
        elif cmd_type == "MODIFY":
            logger.debug(
                "Command MODIFY для %s поки не підтримується (payload=%s)",
                symbol,
                command.get("payload"),
            )
        elif cmd_type == "OPEN":
            logger.debug("Command OPEN для %s покривається потоковою логікою", symbol)
        else:
            logger.debug("Невідомий тип команди %s для %s", cmd_type, symbol)

    async def _handle_close_command(self, symbol: str, command: TradeCommand) -> None:
        payload = command.get("payload") or {}
        origin = str(payload.get("origin") or command.get("origin") or "").lower()
        if origin == "auto_update":
            # Автооновлення вже закрило угоду у TradeLifecycleManager;
            # команду лише логуємо як телеметрію.
            await log_stage3_event(
                "close_auto_ack",
                symbol,
                {
                    "trade_id": payload.get("trade_id"),
                    "origin": origin,
                },
            )
            return

        trade_id_raw = payload.get("trade_id")
        trade_id = str(trade_id_raw) if trade_id_raw else ""
        if not trade_id:
            logger.debug(
                "CLOSE команда без trade_id для %s (payload=%s)", symbol, payload
            )
            return

        price_val = safe_float(payload.get("price"))
        if price_val is None or price_val <= 0:
            logger.debug(
                "CLOSE команда без валідної ціни для %s (payload=%s)", symbol, payload
            )
            return

        reason = str(command.get("reason") or payload.get("reason") or "manual")

        try:
            await self._trade_manager.close_trade(trade_id, float(price_val), reason)
        except Exception:  # pragma: no cover - комплексний захист
            logger.exception(
                "Не вдалося виконати CLOSE команду для %s (trade_id=%s)",
                symbol,
                trade_id,
            )
            await log_stage3_event(
                "close_failed",
                symbol,
                {
                    "trade_id": trade_id,
                    "reason": reason,
                },
            )
            return

        await log_stage3_event(
            "close_executed",
            symbol,
            {
                "trade_id": trade_id,
                "reason": reason,
            },
        )
        logger.info(
            "🔻 TradeExecutionService виконав CLOSE для %s (trade_id=%s)",
            symbol,
            trade_id,
        )


__all__ = ["TradeExecutionService"]
