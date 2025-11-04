"""Async Redis connection helpers for AiOne_t.

Повертає клієнти redis.asyncio з єдиного місця, щоб уникати дублів
конфігурації та допомагати керувати життєвим циклом з'єднань.
"""

from __future__ import annotations

import asyncio
from typing import Any

from redis.asyncio import Redis

from app.settings import settings

__all__ = [
    "acquire_redis",
    "release_redis",
]

# Глобальний лок для створення клієнтів (захист від умов гонки)
_LOCK = asyncio.Lock()


def _build_kwargs(
    *,
    decode_responses: bool,
    encoding: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "host": settings.redis_host,
        "port": settings.redis_port,
        "decode_responses": decode_responses,
        "encoding": encoding,
    }
    if extra:
        params.update(extra)
    return params


async def acquire_redis(
    *,
    decode_responses: bool = True,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> Redis:
    """Створює новий Redis-клієнт для асинхронного використання."""

    async with _LOCK:
        client = Redis(
            **_build_kwargs(
                decode_responses=decode_responses,
                encoding=encoding,
                extra=kwargs,
            )
        )
    return client


async def release_redis(client: Redis | None) -> None:
    """Закриває клієнт Redis, якщо він існує."""

    if client is None:
        return
    try:
        await client.close()
    except Exception:
        pass
