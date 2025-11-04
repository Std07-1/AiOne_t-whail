"""Утиліти для роботи з Redis Streams (уніфікація відповідей).

Мета: надати єдиний спосіб інтерпретації відповіді XAUTOCLAIM, яка
відрізняється між клієнтами/RESP2/RESP3/cluster.

Функції тут не мають зовнішніх сайд-ефектів і не залежать від конфігів.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from redis.asyncio import Redis


def _to_str(x: Any) -> str:
    """Безпечне перетворення ідентифікатора повідомлення у рядок.

    Args:
        x: Значення (str|bytes|Any) ідентифікатора повідомлення.

    Returns:
        str: Рядкове представлення (bytes → UTF-8 з replace).
    """

    if isinstance(x, bytes):
        return x.decode("utf-8", errors="replace")
    return str(x)


def normalize_xautoclaim_response(
    resp: Any,
) -> tuple[list[tuple[str, Mapping[Any, Any]]], str]:
    """Нормалізує відповідь XAUTOCLAIM до канонічного формату.

    Підтримувані форми вхідних даних (спостерігались у різних середовищах):
    - (next_id, [ (id, {fields}), ... ])
    - [next_id, [ (id, {fields}), ... ]]
    - [(stream, (next_id, [ (id, {fields}), ... ]))]  # cluster-like

    Returns:
        (messages, next_id):
            messages: список пар (message_id, fields) з message_id як str
            next_id: наступний курсор XAUTOCLAIM у вигляді рядка
    """

    # (next_id, [ ... ])
    if isinstance(resp, tuple) and len(resp) == 2:
        next_id, batch = resp[0], resp[1]
        return _coerce_batch(batch), _to_str(next_id)

    # [next_id, [ ... ]]
    if isinstance(resp, list) and len(resp) == 2 and isinstance(resp[1], list):
        next_id, batch = resp[0], resp[1]
        return _coerce_batch(batch), _to_str(next_id)

    # [(stream, (next_id, [ ... ]))]
    if isinstance(resp, list) and len(resp) == 1 and isinstance(resp[0], (list, tuple)):
        try:
            _stream, inner = resp[0]
            if isinstance(inner, (list, tuple)) and len(inner) == 2:
                next_id, batch = inner[0], inner[1]
                return _coerce_batch(batch), _to_str(next_id)
        except Exception:
            pass

    # Порожній/justid або невідомий формат
    return [], "0-0"


def _coerce_batch(
    batch: Iterable[tuple[Any, Mapping[Any, Any]]] | None,
) -> list[tuple[str, Mapping[Any, Any]]]:
    out: list[tuple[str, Mapping[Any, Any]]] = []
    if not batch:
        return out
    for item in batch:
        try:
            msg_id, fields = item  # type: ignore[misc]
            out.append((_to_str(msg_id), fields))
        except Exception:
            continue
    return out


__all__ = ["normalize_xautoclaim_response"]


async def xpending_xclaim_fallback(
    redis: Redis,
    stream_key: str,
    group: str,
    consumer: str,
    min_idle_ms: int,
    count: int,
) -> list[tuple[str, Mapping[Any, Any]]]:
    """Fallback шлях для середовищ без підтримки XAUTOCLAIM.

    Використовує XPENDING щоб знайти pending-повідомлення і XCLAIM
    для передання їх поточному консюмеру.

    Args:
        redis: Інстанс asyncio Redis клієнта.
        stream_key: Ключ стріму.
        group: Ім'я consumer group.
        consumer: Ім'я консюмера, якому клеймити повідомлення.
        min_idle_ms: Мінімальна «відлежаність» повідомлення в мс.
        count: Максимальна кількість для одного циклу.

    Returns:
        Список пар (message_id, fields); message_id як str.
    """

    try:
        pend = await redis.xpending(stream_key, group, "-", "+", count)
    except Exception:
        return []

    claim_ids: list[str] = []
    try:
        if isinstance(pend, list):
            for entry in pend:
                # entry часто має вигляд: [id, consumer, idle_ms, deliveries]
                if isinstance(entry, (list, tuple)) and entry:
                    msg_id = _to_str(entry[0])
                    # Якщо idle_ms невідомий — клеймимо обережно (включаємо)
                    idle_ok = True
                    if len(entry) >= 3:
                        try:
                            idle_ms_val = int(entry[2])
                            idle_ok = idle_ms_val >= int(min_idle_ms)
                        except Exception:
                            idle_ok = True
                    if idle_ok:
                        claim_ids.append(msg_id)
    except Exception:
        claim_ids = []

    if not claim_ids:
        return []

    try:
        claimed = await redis.xclaim(
            stream_key,
            group,
            consumer,
            min_idle_ms,
            *claim_ids,
        )
    except Exception:
        return []

    # Переконуємось, що id у str
    out: list[tuple[str, Mapping[Any, Any]]] = []
    for item in claimed or []:
        try:
            mid, fields = item  # type: ignore[misc]
            out.append((_to_str(mid), fields))
        except Exception:
            continue
    return out


__all__.append("xpending_xclaim_fallback")
