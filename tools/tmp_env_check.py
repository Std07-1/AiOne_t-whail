from __future__ import annotations

import json
import os
import sys
import urllib.request

try:
    import redis  # type: ignore
except Exception as e:  # pragma: no cover
    print(f"ERR no redis lib: {e}")
    sys.exit(2)

# Redis connection from settings (fallback localhost:6379)
HOST = os.environ.get("REDIS_HOST", "localhost")
PORT = int(os.environ.get("REDIS_PORT", "6379"))

r = redis.StrictRedis(host=HOST, port=PORT, decode_responses=True)

symbols = ["btcusdt", "tonusdt", "snxusdt"]
for s in symbols:
    key = f"ai_one:bars:{s}:M5"
    try:
        ttl = r.ttl(key)
    except Exception as e:  # pragma: no cover
        ttl = f"ERR:{e}"
    print(f"TTL {key} = {ttl}")

# Write state hash for BTCUSDT to file (json)
state_key = "ai_one:state:BTCUSDT"
try:
    h = r.hgetall(state_key)
except Exception as e:  # pragma: no cover
    h = {"error": str(e)}

try:
    with open("state.txt", "w", encoding="utf-8") as f:
        json.dump(h, f, ensure_ascii=False, indent=2)
    print("WROTE state.txt")
except Exception as e:  # pragma: no cover
    print(f"ERR writing state.txt: {e}")

# Download metrics endpoint to metrics.txt (if reachable)
url = os.environ.get("PROM_URL", "http://localhost:9108/metrics")
try:
    with urllib.request.urlopen(url, timeout=2.0) as resp:
        data = resp.read()
    with open("metrics.txt", "wb") as f:
        f.write(data)
    print("WROTE metrics.txt")
except Exception as e:  # pragma: no cover
    print(f"WARN metrics not fetched: {e}")
