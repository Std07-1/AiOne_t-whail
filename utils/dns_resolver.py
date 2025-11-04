"""Асинхронний DNS-кеш для fallback сценаріїв Stage1/Stage3.

Модуль надає обмежений API:
- `prefetch_host` — прогріває кеш та повертає свіжі адреси;
- `get_cached_addresses` — повертає кеш (дозволяє прострочені адреси);
- `mark_success` — продовжує TTL після успішного звернення;
- `remember_success` — додає/оновлює IP-адреси після fallback;
- `build_static_connector` — створює TCPConnector, що примусово використовує IP.

Таким чином Stage1/Stage3 можуть пережити тимчасові DNS-збої без втручання.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass

from aiohttp import TCPConnector
from aiohttp.abc import AbstractResolver

from config.config import NETWORK_DNS_CACHE_TTL_SEC

logger = logging.getLogger("utils.dns_resolver")
if not logger.handlers:  # використовується і з pytest, і з runtime
    logger.addHandler(logging.NullHandler())

ResolverFn = Callable[[str], Awaitable[Sequence[str]]]
ClockFn = Callable[[], float]


@dataclass(slots=True)
class _CacheEntry:
    """Зберігає перелік адрес та час закінчення TTL."""

    expires_at: float
    addresses: tuple[str, ...]


class AsyncDnsCache:
    """Простий асинхронний DNS-кеш з TTL та fallback на старі адреси."""

    def __init__(
        self,
        ttl_sec: float,
        *,
        resolver: ResolverFn | None = None,
        clock: ClockFn | None = None,
    ) -> None:
        self._ttl = max(float(ttl_sec), 1.0)
        self._clock: ClockFn = clock or time.monotonic
        self._resolver: ResolverFn = resolver or self._resolve_default
        self._cache: dict[str, _CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def prefetch(self, host: str) -> tuple[str, ...]:
        """Примусово оновлює записи у кеші (або повертає кешовані)."""

        host_key = self._normalize(host)
        if not host_key:
            return ()
        entry = self._cache.get(host_key)
        if entry and not self._is_expired(entry):
            return entry.addresses
        try:
            return await self._refresh(host_key)
        except OSError as exc:
            if entry and entry.addresses:
                logger.debug(
                    "prefetch fallback to stale entry for %s: %s", host_key, exc
                )
                return entry.addresses
            raise

    def peek(self, host: str) -> tuple[str, ...]:
        """Повертає адреси (можуть бути прострочені)."""

        host_key = self._normalize(host)
        entry = self._cache.get(host_key)
        if entry is None:
            return ()
        return entry.addresses

    async def mark_success(self, host: str) -> None:
        """Продовжує TTL для успішного використання запису."""

        host_key = self._normalize(host)
        if not host_key:
            return
        entry = self._cache.get(host_key)
        if entry is None:
            return
        self._cache[host_key] = _CacheEntry(self._clock() + self._ttl, entry.addresses)

    async def remember(self, host: str, addresses: Iterable[str]) -> None:
        """Додає нові адреси до кешу та оновлює TTL."""

        host_key = self._normalize(host)
        if not host_key:
            return
        filtered = tuple(sorted({addr.strip() for addr in addresses if addr.strip()}))
        if not filtered:
            await self.mark_success(host_key)
            return
        entry = self._cache.get(host_key)
        current = set(entry.addresses) if entry else set()
        current.update(filtered)
        merged = tuple(sorted(current))
        self._cache[host_key] = _CacheEntry(self._clock() + self._ttl, merged)

    async def _refresh(self, host_key: str) -> tuple[str, ...]:
        lock = self._locks.setdefault(host_key, asyncio.Lock())
        async with lock:
            entry = self._cache.get(host_key)
            if entry and not self._is_expired(entry):
                return entry.addresses
            addresses = await self._resolver(host_key)
            unique = tuple(self._deduplicate(addresses))
            if not unique:
                raise OSError(f"DNS resolver повернув порожній список для {host_key}")
            self._cache[host_key] = _CacheEntry(self._clock() + self._ttl, unique)
            return unique

    async def _resolve_default(self, host: str) -> tuple[str, ...]:
        loop = asyncio.get_running_loop()
        infos = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        return tuple(self._deduplicate(info[4][0] for info in infos if info[4]))

    def _deduplicate(self, data: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for item in data:
            if not item or item in seen:
                continue
            seen.add(item)
            ordered.append(item)
        return ordered

    def _is_expired(self, entry: _CacheEntry) -> bool:
        return entry.expires_at <= self._clock()

    def _normalize(self, host: str) -> str:
        return host.strip().lower()


class _StaticResolver(AbstractResolver):
    """Повертає наперед відому IP-адресу (для fallback HTTP)."""

    def __init__(self, hostname: str, address: str) -> None:
        self._hostname = hostname
        self._address = address
        self._family = socket.AF_INET6 if ":" in address else socket.AF_INET

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_UNSPEC
    ) -> list[dict[str, object]]:
        if host != self._hostname:
            raise OSError(f"StaticResolver отримав неочікуваний host={host}")
        family_to_use = self._family if family in (socket.AF_UNSPEC, 0) else family
        return [
            {
                "hostname": self._hostname,
                "host": self._address,
                "port": port,
                "family": family_to_use,
                "proto": 0,
                "flags": 0,
            }
        ]

    async def close(self) -> None:  # pragma: no cover - інтерфейс вимагає метод
        return None


_dns_cache = AsyncDnsCache(NETWORK_DNS_CACHE_TTL_SEC)


async def prefetch_host(host: str) -> tuple[str, ...]:
    """Гарантовано отримує (або прогріває) адреси для host."""

    try:
        return await _dns_cache.prefetch(host)
    except OSError as exc:
        logger.debug("DNS prefetch failed for %s: %s", host, exc)
        return _dns_cache.peek(host)


def get_cached_addresses(host: str) -> tuple[str, ...]:
    """Повертає кешовані адреси (можуть бути простроченими)."""

    return _dns_cache.peek(host)


async def mark_success(host: str) -> None:
    """Оновлює TTL запису після успішного стандартного звернення."""

    await _dns_cache.mark_success(host)


async def remember_success(host: str, addresses: Iterable[str]) -> None:
    """Зберігає IP-адреси, отримані під час fallback."""

    await _dns_cache.remember(host, tuple(addresses))


def build_static_connector(host: str, address: str) -> TCPConnector:
    """Створює TCPConnector, що нав’язує використання IP при збереженні Host.

    Використовується для fallback-запитів, де стандартний DNS недоступний.
    """

    resolver = _StaticResolver(host, address)
    # TTL DNS встановлюємо у 0, щоб не кешувати на рівні aiohttp.
    return TCPConnector(resolver=resolver, ttl_dns=0)


__all__ = [
    "AsyncDnsCache",
    "prefetch_host",
    "get_cached_addresses",
    "mark_success",
    "remember_success",
    "build_static_connector",
]
