"""Виводить поточні метрики Prometheus зі спливаючого /metrics HTTP‑експортера.

Експортер належить лише пайплайну (app.main, запуск через tools.run_window).
Цей інструмент НЕ стартує HTTP‑сервер, лише читає його вміст.

Запуск (Windows):
    python -m tools.dump_metrics | findstr "low_atr_override_active low_atr_override_ttl htf_strength presence"
"""

from __future__ import annotations

import os
import sys

# Фолбек імпортів при прямому виконанні як скрипт
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import urllib.request

from config.config import PROM_HTTP_PORT


def main() -> int:
    url = f"http://localhost:{int(PROM_HTTP_PORT)}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            sys.stdout.write(resp.read().decode("utf-8", errors="replace"))
        return 0
    except Exception as e:
        sys.stderr.write(f"dump_metrics: failed to fetch {url}: {e}\n")
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
