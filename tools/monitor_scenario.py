from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.request

try:
    import redis  # type: ignore
except Exception as e:  # pragma: no cover
    print(f"ERR no redis lib: {e}")
    sys.exit(2)

PROM_URL = os.environ.get("PROM_URL", "http://localhost:9108/metrics")
HOST = os.environ.get("REDIS_HOST", "localhost")
PORT = int(os.environ.get("REDIS_PORT", "6379"))
SYMBOL = os.environ.get("SCN_SYMBOL", "btcusdt").lower()
STATE_KEY = f"ai_one:state:{SYMBOL}"

r = redis.StrictRedis(host=HOST, port=PORT, decode_responses=True)

SCEN_LINE_RE = re.compile(
    r'^ai_one_scenario\{[^}]*symbol="BTCUSDT"[^}]*\} *([0-9Ee+\-.]+)'
)


def fetch_metrics() -> str:
    """
    Завантажує метрики Prometheus з PROM_URL.
    Повертає текст метрик або кидає виняток при помилці.
    """
    try:
        with urllib.request.urlopen(PROM_URL, timeout=3.0) as resp:
            data = resp.read().decode("utf-8", errors="replace")
            print("[STRICT_SANITY] [fetch_metrics] Метрики успішно отримано")
            return data
    except Exception as e:
        print(f"[STRICT_SANITY] [fetch_metrics] Помилка отримання метрик: {e}")
        raise


def check_state() -> tuple[str | None, float | None]:
    """
    Читає з Redis поточний сценарій та його впевненість.
    Повертає (scenario_detected, scenario_confidence) або (None, None) при помилці.
    """
    try:
        sd = r.hget(STATE_KEY, "scenario_detected")
        sc = r.hget(STATE_KEY, "scenario_confidence")
        conf = float(sc) if sc is not None else None
        print(
            f"[STRICT_SANITY] [check_state] STATE_KEY={STATE_KEY} scenario_detected={sd} scenario_confidence={conf}"
        )
        return (sd, conf)
    except Exception as e:
        print(f"[STRICT_SANITY] [check_state] Помилка читання стану: {e}")
        return (None, None)


def main(timeout_s: int = 600, poll_s: float = 5.0) -> int:
    """
    Моніторить появу сценарію у метриках Prometheus та стані Redis.
    Якщо сценарій знайдено — зберігає снапшоти у файли та завершує з кодом 0.
    Якщо таймаут — зберігає поточний стан для дебагу та завершує з кодом 1.

    Підказка:
      - Для зміни символу встановіть SCN_SYMBOL (env).
      - Для зміни адреси Prometheus — PROM_URL (env).
      - Для зміни Redis — REDIS_HOST/REDIS_PORT (env).
    """
    deadline = time.time() + timeout_s
    found_metrics = None
    found_state = None
    print(
        f"[STRICT_SANITY] [main] Моніторинг сценарію для {SYMBOL.upper()} (Redis={HOST}:{PORT}, Prometheus={PROM_URL})"
    )
    while time.time() < deadline:
        # metrics
        try:
            text = fetch_metrics()
            for line in text.splitlines():
                if (
                    line.startswith("ai_one_scenario{")
                    and f'symbol="{SYMBOL.upper()}"' in line
                ):
                    found_metrics = line.strip()
                    print(
                        f"[STRICT_SANITY] [main] Знайдено метрику сценарію: {found_metrics}"
                    )
                    break
        except Exception:
            print(
                "[STRICT_SANITY] [main] Не вдалося отримати метрики, повтор через кілька секунд"
            )

        # state
        sd, conf = check_state()
        if sd and str(sd).lower() != "none" and conf is not None and conf > 0:
            found_state = (sd, conf)
            print(f"[STRICT_SANITY] [main] Знайдено стан сценарію: {found_state}")

        if found_metrics and found_state:
            # persist snapshots
            try:
                with open("metrics_scn.txt", "w", encoding="utf-8") as f:
                    f.write(text)
                st = r.hgetall(STATE_KEY)
                with open("state_scn.json", "w", encoding="utf-8") as f:
                    json.dump(st, f, ensure_ascii=False, indent=2)
                print("[STRICT_SANITY] [main] Снапшоти метрик та стану збережено")
            except Exception as e:
                print(f"[STRICT_SANITY] [main] Помилка збереження снапшотів: {e}")
            print("METRICS:", found_metrics)
            print(
                "STATE:",
                STATE_KEY,
                "scenario_detected=",
                found_state[0],
                "scenario_confidence=",
                found_state[1],
            )
            return 0

        time.sleep(poll_s)

    print("[STRICT_SANITY] [main] TIMEOUT: сценарій не спостерігається")
    # best effort: dump current state keys for debugging
    try:
        st = r.hgetall(STATE_KEY)
        with open("state_scn.json", "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False, indent=2)
        print("[STRICT_SANITY] [main] Поточний стан збережено для дебагу")
    except Exception as e:
        print(f"[STRICT_SANITY] [main] Помилка збереження стану: {e}")
    return 1


if __name__ == "__main__":  # pragma: no cover
    print(
        "[STRICT_SANITY] [main] Запуск моніторингу сценарію.\n"
        "Підказка: змініть SCN_SYMBOL, PROM_URL, REDIS_HOST/PORT через змінні середовища.\n"
        "Ctrl+C для виходу."
    )
    raise SystemExit(main())
