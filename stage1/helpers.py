"""Допоміжні утиліти Stage1 (фільтр активів Binance Futures).

Шлях: ``stage1/helpers.py``

Надає:
    • універсальний кешований fetch (``fetch_cached_data``);
    • паралельний збір метрик із семафорами (``fetch_concurrently``);
    • індивідуальні функції для ATR / order book depth / open interest;
    • retry логіку через tenacity з кастомним фільтром винятків.
"""

import asyncio
import json
import logging
import time
import urllib.parse
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

import aiohttp
from aiohttp import ClientConnectionError, ClientConnectorDNSError, ClientResponseError
from rich.console import Console
from rich.logging import RichHandler
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from config.config import (
    DEPTH_SEMAPHORE,
    KLINES_SEMAPHORE,
    OI_SEMAPHORE,
    REDIS_CACHE_TTL,
)
from monitoring.telemetry.hub import get_hub
from utils.dns_resolver import (
    build_static_connector,
    get_cached_addresses,
    mark_success,
    prefetch_host,
    remember_success,
)

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.stage1.helpers")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


def _is_retryable(exc: BaseException) -> bool:
    """
    Визначає, чи варто повторювати запит (для tenacity).
    :param exc: Exception
    :return: True якщо exception підлягає повтору
    """
    logger.debug(f"[RETRY] Перевірка exception: {exc}")
    if isinstance(exc, (ClientConnectionError, asyncio.TimeoutError)):
        logger.debug("[RETRY] ClientConnectionError або TimeoutError, повторюємо")
        return True
    if isinstance(exc, ClientResponseError) and exc.status >= 500:
        logger.debug(f"[RETRY] ClientResponseError {exc.status} >= 500, повторюємо")
        return True
    logger.debug("[RETRY] Exception не підлягає повтору")
    return False


retry_decorator = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=0.5, max=10),
    retry=retry_if_exception(_is_retryable),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


# ───────────────────────────── Протоколи типів ─────────────────────────────
@runtime_checkable
class RedisJSONLike(Protocol):
    async def jget(self, *parts: str, default: object | None = None) -> object: ...

    async def jset(
        self, *parts: str, value: object, ttl: int | None = None
    ) -> None: ...


@runtime_checkable
class CacheLike(Protocol):
    redis: RedisJSONLike

    async def fetch_from_cache(
        self, symbol: str, interval: str, *, prefix: str = "candles", raw: bool = True
    ) -> object: ...

    async def delete_from_cache(
        self, symbol: str, interval: str, *, prefix: str = "candles"
    ) -> None: ...


def _infer_domain_from_url(url: str) -> str:
    try:
        path = urllib.parse.urlsplit(url).path.lower()
        if "/klines" in path:
            return "klines"
        if "/depth" in path:
            return "depth"
        if "/openinterest" in path:
            return "oi"
    except Exception:
        pass
    return "generic"


async def _fetch_json(
    session: aiohttp.ClientSession, url: str
) -> list[Any] | dict[str, Any]:
    """
    Виконує HTTP GET запит до вказаного URL і повертає JSON.
    Якщо виникає помилка (наприклад, HTTP 451 або інша),
    логгує повідомлення та повертає порожній словник.

    :param session: об'єкт aiohttp.ClientSession для виконання запиту.
    :param url: URL-адреса запиту.
    :return: JSON дані у вигляді списку або словника, або {} при помилці.
    """
    logger.debug(f"[STEP] _fetch_json: GET {url}")
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    if host:
        await prefetch_host(host)
    timeout = aiohttp.ClientTimeout(total=15)

    start = time.monotonic()
    domain = _infer_domain_from_url(url)
    recorded = False
    try:
        async with session.get(url, timeout=timeout) as resp:
            logger.debug(f"[EVENT] Відповідь отримано, статус: {resp.status}")
            resp.raise_for_status()
            result = await resp.json()
            logger.debug(f"[EVENT] JSON отримано: {type(result)}")
            if host:
                await mark_success(host)
            if isinstance(result, dict):
                try:
                    get_hub().record_latency(
                        domain, (time.monotonic() - start) * 1000.0
                    )
                    recorded = True
                except Exception:
                    pass
                return result
            if isinstance(result, list):
                try:
                    get_hub().record_latency(
                        domain, (time.monotonic() - start) * 1000.0
                    )
                    recorded = True
                except Exception:
                    pass
                return result
            # Нестандартна відповідь – повертаємо порожній словник
            return {}
    except ClientConnectorDNSError as dns_exc:
        if host:
            fallback = await _fallback_via_cached_ip(url, host, timeout, dns_exc)
            if fallback is not None:
                return fallback
        logger.error(
            "ClientConnectorDNSError для URL %s: %s",
            url,
            dns_exc,
            exc_info=True,
        )
    except ClientResponseError as e:
        if e.status == 451:
            logger.error("HTTP 451: Доступ заблоковано для URL %s", url)
        elif e.status == 429:
            try:
                get_hub().record_429(domain)
            except Exception:
                pass
        else:
            logger.error("HTTP помилка %s для URL %s", e.status, url)
    except Exception as exc:
        logger.error(
            "Неочікувана помилка при запиті до URL %s: %s", url, exc, exc_info=True
        )
    finally:
        if not recorded:
            try:
                get_hub().record_latency(domain, (time.monotonic() - start) * 1000.0)
            except Exception:
                pass
    logger.debug(f"[EVENT] Повертаємо порожній словник для {url}")
    return {}


async def _fallback_via_cached_ip(
    url: str,
    host: str,
    timeout: aiohttp.ClientTimeout,
    error: Exception,
) -> list[Any] | dict[str, Any] | None:
    """HTTP fallback через кешовані IP-адреси (коли DNS недоступний).

    Args:
        url: Повний URL запиту.
        host: Домен, який був недоступний через DNS.
        timeout: Таймаут, що застосовується до fallback-запиту.
        error: Виняток, який ініціював fallback.

    Returns:
        JSON-відповідь або None, якщо жоден IP не спрацював.
    """

    cached_addresses = get_cached_addresses(host)
    if not cached_addresses:
        logger.error(
            "DNS fallback: відсутній кеш для %s (%s)", host, error, exc_info=True
        )
        return None

    for ip in cached_addresses:
        connector = build_static_connector(host, ip)
        try:
            async with aiohttp.ClientSession(connector=connector) as fallback_session:
                async with fallback_session.get(url, timeout=timeout) as resp:
                    resp.raise_for_status()
                    payload = await resp.json()
            await remember_success(host, (ip,))
            logger.warning("[STRICT_PHASE] DNS fallback успішний: %s → %s", host, ip)
            if isinstance(payload, dict):
                return payload
            if isinstance(payload, list):
                return payload
            return {}
        except Exception as fallback_exc:
            logger.warning(
                "[STRICT_PHASE] DNS fallback невдалий: %s → %s (%s)",
                host,
                ip,
                fallback_exc,
            )
    logger.error("DNS fallback не вдався для %s: %s", host, error, exc_info=True)
    return None


@retry_decorator
async def fetch_cached_data(
    session: aiohttp.ClientSession,
    cache_handler: CacheLike,
    key: str,
    url: str,
    process_fn: Callable[[Any], Any] | None = None,
) -> dict[str, Any]:
    """
    Універсальна функція для кешованих запитів.
    :param session: aiohttp.ClientSession
    :param cache_handler: об'єкт кешу
    :param key: ключ кешу
    :param url: URL для запиту
    :param process_fn: функція обробки даних
    :return: оброблені дані
    """
    logger.debug(f"[STEP] _fetch_cached_data (JSON-first): key={key}, url={url}")
    # НОВИЙ ПІДХІД: пробуємо структурований JSON ключ замість blob
    json_cached: object | None = None
    try:
        json_cached = await cache_handler.redis.jget(
            "selectors", "meta", key, default=None
        )
    except Exception as e:  # pragma: no cover
        logger.debug(f"[EVENT] jget не вдалось для {key}: {e}")

    if isinstance(json_cached, dict):
        logger.debug(f"[EVENT] JSON кеш знайдено: {type(json_cached)}")
        return json_cached

    # FALLBACK (тимчасово): спроба старого blob кеша якщо існує
    try:
        legacy_blob = await cache_handler.fetch_from_cache(
            symbol=key, interval="global", prefix="meta"
        )
    except Exception:
        legacy_blob = None
    if isinstance(legacy_blob, (bytes, str)) and legacy_blob:
        try:
            parsed_any = json.loads(legacy_blob)
            parsed = parsed_any if isinstance(parsed_any, dict) else {}
            logger.debug("[EVENT] Використано legacy blob кеш (parsed JSON)")
            # Міг бути збережений старим шляхом — перепишемо у новий JSON ключ
            try:
                await cache_handler.redis.jset(
                    "selectors", "meta", key, value=parsed, ttl=REDIS_CACHE_TTL
                )
            except Exception:
                pass
            return parsed
        except Exception:
            # видаляємо старий blob, щоб уникати повторів
            try:
                await cache_handler.delete_from_cache(
                    symbol=key, interval="global", prefix="meta"
                )
            except Exception:
                pass

    logger.debug("[STEP] Кеш відсутній, виконуємо HTTP запит")
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        logger.debug(f"[EVENT] Відповідь API отримано, статус: {resp.status}")
        resp.raise_for_status()
        data_any = await resp.json()
        logger.debug(f"[EVENT] Дані з API отримано: {type(data_any)}")

        # Очікуємо словник; якщо список – обгортаємо або ігноруємо за замовчанням
        data: dict[str, Any]
        if isinstance(data_any, dict):
            data = data_any
        else:
            # Безпечний дефолт: зберігаємо під ключем "data"
            data = {"data": data_any}

        processed_any = process_fn(data) if process_fn else data
        processed: dict[str, Any] = (
            processed_any
            if isinstance(processed_any, dict)
            else {"data": processed_any}
        )
        logger.debug(f"[EVENT] Дані оброблено: {type(processed)}")

        try:
            await cache_handler.redis.jset(
                "selectors", "meta", key, value=processed, ttl=REDIS_CACHE_TTL
            )
            logger.debug(f"[EVENT] Дані збережено (jset JSON): {key}")
        except Exception as e:  # pragma: no cover
            logger.warning("Не вдалося зберегти JSON кеш %s: %s", key, e)
        return processed


async def fetch_concurrently(
    session: aiohttp.ClientSession,
    symbols: list[str],
    endpoint_fn: Callable[[aiohttp.ClientSession, str], Awaitable[float]],
    semaphore: asyncio.Semaphore,
    progress_callback: Callable[[], None] | None = None,  # Додаємо callback
) -> dict[str, float]:
    """
    Паралельний збір даних для списку символів.
    :param session: aiohttp.ClientSession
    :param symbols: список символів
    :param endpoint_fn: функція для отримання метрики
    :param semaphore: семафор для обмеження паралелізму
    :return: dict {symbol: value}
    """
    logger.debug(f"[STEP] _fetch_concurrently: symbols={symbols}")

    async def _fetch_single(sym: str) -> tuple[str, float]:
        async with semaphore:
            try:
                logger.debug(f"[EVENT] Запит метрики для {sym}")
                value = await endpoint_fn(session, sym)

                # Викликаємо callback для оновлення прогресу
                if progress_callback:
                    progress_callback()

                logger.debug(f"[EVENT] Метрика для {sym}: {value}")
                return sym, value
            except Exception as e:
                logger.error("Помилка для %s: %s", sym, e)
                return sym, 0.0

    tasks = [_fetch_single(sym) for sym in symbols]
    results = await asyncio.gather(*tasks)
    logger.debug(f"[EVENT] Зібрано результати: {results}")
    return dict(results)


async def fetch_atr(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    Розрахунок ATR за останні 14 днів для символу.
    :param session: aiohttp.ClientSession
    :param symbol: символ
    :return: ATR у %%
    """
    url = (
        f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1d&limit=15"
    )
    logger.debug(f"[STEP] _fetch_atr: symbol={symbol}, url={url}")
    async with KLINES_SEMAPHORE:
        try:
            data = await _fetch_json(session, url)
            logger.debug(f"[EVENT] Дані для ATR: {len(data)} записів")
            if len(data) < 15:
                logger.debug(f"[EVENT] Недостатньо даних для ATR: {len(data)}")
                return 0.0

            closes = [float(c[4]) for c in data]
            highs = [float(h[2]) for h in data]
            lows = [float(low_row[3]) for low_row in data]

            tr_values = []
            for i in range(1, len(data)):
                tr = max(
                    highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]),
                )
                tr_values.append(tr)

            atr = sum(tr_values) / len(tr_values)
            current_price = closes[-1]
            result = (atr / current_price) * 100 if current_price else 0.0
            logger.debug(f"[EVENT] ATR для {symbol}: {result}")
            return result
        except Exception as e:
            logger.error("ATR помилка для %s: %s", symbol, e)
            return 0.0


async def fetch_orderbook_depth(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    Розрахунок глибини стакану для символу.
    :param session: aiohttp.ClientSession
    :param symbol: символ
    :return: глибина стакану
    """
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=10"
    logger.debug(f"[STEP] _fetch_orderbook_depth: symbol={symbol}, url={url}")
    async with DEPTH_SEMAPHORE:
        try:
            data = await _fetch_json(session, url)
            logger.debug(f"[EVENT] Дані для depth: {type(data)}")
            total_value = 0.0
            if isinstance(data, dict):
                for side in ["bids", "asks"]:
                    side_list = data.get(side, [])
                    if isinstance(side_list, list):
                        for item in side_list[:10]:
                            try:
                                price, qty = item
                                total_value += float(price) * float(qty)
                            except Exception:
                                continue
            logger.debug(f"[EVENT] Глибина стакану для {symbol}: {total_value}")
            return total_value
        except Exception as e:
            logger.error("Depth помилка для %s: %s", symbol, e)
            return 0.0


async def fetch_open_interest(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    Отримання Open Interest для символу.
    :param session: aiohttp.ClientSession
    :param symbol: символ
    :return: open interest
    """
    url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
    logger.debug(f"[STEP] _fetch_open_interest: symbol={symbol}, url={url}")
    async with OI_SEMAPHORE:
        try:
            data = await _fetch_json(session, url)
            value = (
                float(data.get("openInterest", 0.0)) if isinstance(data, dict) else 0.0
            )
            logger.debug(f"[EVENT] Open Interest для {symbol}: {value}")
            return value
        except Exception as e:
            logger.error("OI помилка для %s: %s", symbol, e)
            return 0.0
