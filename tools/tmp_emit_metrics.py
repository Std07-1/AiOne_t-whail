from __future__ import annotations

import os
import time

from telemetry.prom_gauges import set_htf_strength, set_scenario

# Prometheus HTTP‑експортер належить пайплайну (app.main/tools.run_window).
# Цей утилітарний скрипт НЕ стартує сервер, лише виставляє гейджі у локальному процесі
# (корисно для локальної перевірки CollectorRegistry, не для /metrics).
try:
    import redis  # type: ignore

    r = redis.StrictRedis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        decode_responses=True,
    )
    hs = r.hget("ai_one:state:btcusdt", "htf:strength")
    val = float(hs) if hs is not None else 0.0
except Exception:
    val = 0.0

set_htf_strength("BTCUSDT", val)
set_scenario("BTCUSDT", "none", 0.0)

time.sleep(0.2)
