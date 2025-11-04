"""Типи контрактів Stage3 для комунікації між сервісами.

Призначення: описує структури сигналів та команд, які передаються через
черги Stage3. Поточна реалізація мінімальна й сумісна з наявними
Stage2/Stage3 об'єктами — поля позначені як опційні, щоб не ламати
існуючі виклики. Надалі набір ключів можна розширювати.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict


class StrategySignal(TypedDict, total=False):
    """Сигнал від StrategyService до TradeExecutionService.

    Обов'язкові ключі:
        symbol: Тікер у форматі ``BTCUSDT``.
        side: Очікуваний напрям угоди.

    Додаткові ключі залишаємо опційними, щоб не ламати поточну передачу
    оригінальних Stage2 сигналів.
    """

    symbol: str
    side: Literal["LONG", "SHORT"]
    size: float | None
    raw_signal: dict[str, Any]
    guard_flags: list[str]
    expiry_ts: float | None
    trade_payload: dict[str, Any]


class TradeCommand(TypedDict, total=False):
    """Команди, що потрапляють у ``trade_cmd_queue``.

    command: тип операції; ``OPEN`` та ``CLOSE`` покривають наявну
    логіку, ``MODIFY`` зарезервовано для майбутніх розширень.
    """

    command: Literal["OPEN", "CLOSE", "MODIFY"]
    symbol: str
    payload: dict[str, Any]
    reason: str | None
    request_ts: float | None


class TradeCommandShadow(TradeCommand, total=False):
    """Розширений контракт для команд у shadow-пайплайні Stage3.

    Додає службову інформацію, яка допомагає синхронізувати shadow-черги
    з бойовими процесами та відстежувати джерело походження події.
    """

    pipeline: Literal["shadow", "primary"]
    stream_id: str | None
    dual_write: bool | None
    replay_ts: float | None


class ExecutionResultShadow(TypedDict, total=False):
    """Формат результатів виконання у shadow-пайплайні.

    Використовується для порівняння з бойовими показниками та валідації
    коректності нового сервісу виконання.
    """

    symbol: str
    trade_id: str | None
    command: Literal["OPEN", "CLOSE", "MODIFY", "NONE"]
    status: Literal["accepted", "rejected", "error"]
    reason: str | None
    latency_sec: float | None
    executed_at_ts: float | None
