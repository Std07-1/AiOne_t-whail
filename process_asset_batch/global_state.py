"""Глобальний стан для процесингу батчів активів.
Тут зберігаються різні кеші/стани, які потрібні між викликами процесингу активів у батчах.
Це дозволяє уникнути передачі великої кількості параметрів між функціями.
"""

from __future__ import annotations

import os
from collections import deque
from typing import Any

_WHALE_METRIC_RING: dict[str, deque[tuple[float, float]]] = {}
_HINT_COOLDOWN_LAST_TS: dict[str, float] = {}

_ACCUM_MONITOR_STATE: dict[str, dict[str, Any]] = {}
_SCEN_TRACE_LAST_TS: dict[str, float] = {}
_SCEN_EXPLAIN_LAST_TS: dict[str, float] = {}
_SCEN_EXPLAIN_BATCH_COUNTER: dict[str, int] = {}
# Форс‑режим логування explain для офлайн/реплею (ENV: SCEN_EXPLAIN_FORCE_ALL)
_SCEN_EXPLAIN_FORCE_ALL: bool = bool(
    str(os.getenv("SCEN_EXPLAIN_FORCE_ALL", "")).strip().lower() in ("1", "true", "yes")
)
# Sweep debounce/TTL state
_SWEEP_LAST_TS_MS: dict[str, int] = {}
_SWEEP_LEVEL: dict[str, float] = {}
_SWEEP_LAST_SIDE: dict[str, str] = {}
# WARN‑once трекер для короткої історії (<20×1m) при перевірках sweep→*
_SWEEP_WARN_SHORT_HISTORY: set[str] = set()
_EDGE_DWELL: dict[str, deque[str]] = {}
# Кільце торкань верхнього краю для chop‑pre‑breakout (вікно ~15 барів)
_EDGE_HIT_RING: dict[str, deque[bool]] = {}
# Буфери для гістрезису сценаріїв (опційно)
_SCEN_HIST_RING: dict[str, deque[dict[str, Any]]] = {}
_SCEN_LAST_STABLE: dict[str, str | None] = {}
_SCEN_ALERT_LAST_TS: dict[str, float] = {}
_CTX_MEMORY: dict[str, Any] = {}
_LAST_PHASE: dict[str, str] = {}
_PROFILE_STATE: dict[str, dict[str, Any]] = {}
# Легка гіпотеза pre_breakout на символ (open/confirm/expired)
_HYP_STATE: dict[str, dict[str, Any]] = {}

# Додаткові стани для допоміжних утиліт (винесено з app.process_asset_batch)
# EMA стан для нормалізації DVR за символами
_DVR_EMA_STATE: dict[str, float] = {}
# Rate-limit журналу Prometheus presence за символами
_PROM_PRES_LAST_TS: dict[str, float] = {}
