from __future__ import annotations

import asyncio
import types
import sys
from typing import Any

import pytest

# Створюємо фейковий redis-клієнт та модуль data.redis_connection

class _DummyRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def get(self, key: str) -> Any:
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:  # noqa: ARG002 - ttl
        self._store[key] = value


_dummy_client = _DummyRedis()


async def _acquire() -> _DummyRedis:  # type: ignore[override]
    return _dummy_client


async def _release(_client: Any) -> None:  # noqa: ARG001
    return None


@pytest.mark.asyncio
async def test_cooldown_persist_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    # Підміняємо модуль data.redis_connection у sys.modules
    fake_mod = types.ModuleType("data.redis_connection")
    setattr(fake_mod, "acquire_redis", _acquire)
    setattr(fake_mod, "release_redis", _release)

    sys.modules["data.redis_connection"] = fake_mod

    from app.process_asset_batch import _read_profile_cooldown, _write_profile_cooldown

    symbol = "ETHUSDT"
    until_ts = 1730800000.0

    # Спочатку нічого немає
    v0 = await _read_profile_cooldown(symbol)
    assert v0 is None

    # Записуємо TTL=120
    await _write_profile_cooldown(symbol, until_ts, ttl_s=120)

    # Тепер можна прочитати те саме значення
    v1 = await _read_profile_cooldown(symbol)
    assert v1 == pytest.approx(until_ts)
