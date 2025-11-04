"""Quick checker for ai_one_scenario (Prometheus) and Redis state fields.

Usage (PowerShell):
  $env:PROM_URL="http://localhost:9108/metrics"; \
  C:/Users/vikto/Desktop/AiOne_t-main/venv/Scripts/python.exe tools/quick_check_scenario.py btcusdt

All outputs in Ukrainian.
"""

from __future__ import annotations

import asyncio
import os
import sys
import urllib.request
from typing import Any

try:
    from redis.asyncio import Redis
except Exception as e:  # pragma: no cover
    print(f"[ERR] Немає redis.asyncio: {e}")
    sys.exit(2)


PROM_URL = os.environ.get("PROM_URL", "http://localhost:9108/metrics")


def _fetch_metrics_text(url: str) -> str:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:  # nosec B310
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return f"[WARN] Не вдалося отримати метрики: {e}"


async def _get_redis_state(symbol: str) -> dict[str, Any]:
    # Використовуємо локальний Redis за замовчуванням
    host = os.environ.get("REDIS_HOST", "127.0.0.1")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    r = Redis(host=host, port=port, decode_responses=True)
    try:
        key_lower = f"ai_one:state:{symbol.lower()}"
        data = await r.hgetall(key_lower)
        if not data:
            key_upper = f"ai_one:state:{symbol.upper()}"
            data = await r.hgetall(key_upper)
        return data
    finally:
        try:
            await r.close()
        except Exception:
            pass


async def main() -> int:
    symbol = (
        sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SCN_SYMBOL", "btcusdt")
    ).strip()
    text = _fetch_metrics_text(PROM_URL)
    lines = []
    for line in text.splitlines():
        if "ai_one_scenario{" in line:
            if symbol.upper() in line:
                lines.append(line)
    print("# METRICS ai_one_scenario (фільтр за символом):")
    if lines:
        for line_out in lines[:10]:
            print(line_out)
    else:
        print("<порожньо або недоступно>")

    state = await _get_redis_state(symbol)
    print("\n# REDIS ai_one:state поля:")
    if state:
        print("scenario_detected=", state.get("scenario_detected"))
        print("scenario_confidence=", state.get("scenario_confidence"))
        # Додатково кілька ключових полів для контексту
        for k in ("phase", "dvr", "band_pct", "near_edge", "volume_z", "htf_strength"):
            if k in state:
                print(f"{k}=", state.get(k))
    else:
        print("<ключ не знайдено>")
    # Додатково: спробуємо витягнути знімок UI і знайти поля сценарію
    try:
        host = os.environ.get("REDIS_HOST", "127.0.0.1")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        r2 = Redis(host=host, port=port, decode_responses=True)
        snap = await r2.get("ai_one:ui_snapshot")
        if snap:
            print(
                "\n# SNAPSHOT ai_one:ui_snapshot -> scenario поля для", symbol.upper()
            )
            import json as _json

            doc = _json.loads(snap)
            assets = doc.get("assets") if isinstance(doc, dict) else None
            if isinstance(assets, list):
                for a in assets:
                    if str(a.get("symbol")).upper() == symbol.upper():
                        print("scenario_detected=", a.get("scenario_detected"))
                        print("scenario_confidence=", a.get("scenario_confidence"))
                        mc = a.get("market_context") or {}
                        meta = mc.get("meta") if isinstance(mc, dict) else None
                        if isinstance(meta, dict):
                            print(
                                "meta.scenario_detected=", meta.get("scenario_detected")
                            )
                            print(
                                "meta.scenario_confidence=",
                                meta.get("scenario_confidence"),
                            )
                        break
        try:
            await r2.close()
        except Exception:
            pass
    except Exception as e:
        print(f"[WARN] Не вдалося прочитати ui_snapshot: {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
