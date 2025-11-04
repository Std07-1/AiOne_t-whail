"""Утиліти формування Redis‑ключів та каналів (єдиний формат).

Призначення:
- Централізувати побудову ключів/каналів згідно схеми `ai_one:{domain}:{symbol}:{granularity}`.
- Уникати хардкодів і різночитань.

Використання:
    from config.config import NAMESPACE
    from config.keys import build_key

    key = build_key(NAMESPACE, "stats", symbol="btcusdt", granularity="1m")

Примітка:
- Функції НЕ імпортують `config.config`, щоб уникнути циклів. Передавайте `namespace` явно.
- Символи нормалізуються до нижнього регістру.
"""

from __future__ import annotations

from typing import Iterable


def _clean_parts(parts: Iterable[str | None]) -> list[str]:
    out: list[str] = []
    for p in parts:
        if not p:
            continue
        s = str(p).strip().strip(":")
        if s:
            out.append(s)
    return out


def build_key(
    namespace: str,
    domain: str,
    *,
    symbol: str | None = None,
    granularity: str | None = None,
    extra: Iterable[str] | None = None,
) -> str:
    """Побудувати Redis‑ключ за каноном.

    Args:
        namespace: Простір імен (напр., "ai_one").
        domain: Домен (напр., "stats" | "signal" | "levels" | "admin").
        symbol: Символ (нижній регістр), опційно.
        granularity: Інтервал ("1m", "1h"...), опційно.
        extra: Додаткові частини шляху (без двокрапок), опційно.

    Returns:
        str: Ключ формату `namespace:domain[:symbol][:granularity][:extra...]`.
    """
    sym = symbol.lower() if isinstance(symbol, str) else None
    parts = _clean_parts((namespace, domain, sym, granularity))
    if extra:
        parts.extend(_clean_parts(extra))
    return ":".join(parts)


def build_channel(namespace: str, *parts: str) -> str:
    """Побудувати ім'я каналу публікації у форматі `namespace:part1:part2...`.

    Простий синонім до build_key без доменного семантичного розрізнення.
    """
    return build_key(namespace, "channel", extra=parts)


__all__ = ["build_key", "build_channel"]
