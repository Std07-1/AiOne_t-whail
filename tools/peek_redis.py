from __future__ import annotations

import os
from collections.abc import Sequence

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def peek(namespace: str, symbols: Sequence[str]) -> int:
    if redis is None:
        print("redis lib not available")
        return 2
    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("REDIS_PORT", "6379"))
    r = redis.StrictRedis(host=host, port=port, decode_responses=True)
    for sym in symbols:
        key = f"{namespace}:whale:{sym.upper()}:1m"
        val = r.get(key)
        ttl = r.ttl(key)
        print(f"KEY {key} TTL={ttl} VAL={val}")
    return 0


if __name__ == "__main__":
    ns = os.getenv("NS", "ai_one_prebreakout")
    syms = os.getenv("SYMS", "BTCUSDT,ETHUSDT,TONUSDT,SNXUSDT").split(",")
    raise SystemExit(peek(ns, syms))
