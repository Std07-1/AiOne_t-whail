"""Best-effort запис телеметрії Stage3 у JSONL-файли.

Файл інкапсулює неблокуючий запис подій Stage3 (skip/open/close/stale)
у локальний журнал `telemetry/stage3_events.jsonl`. Використовується як
легковаговий sink без залежності від Redis/Prometheus, аби мати історію
для офлайн-аналізу (`tools/analyze_telemetry.py`).

Особливості:
    • Вмикається/вимикається через `STAGE3_TELEMETRY_ENABLED` (env-перемінна).
    • Шлях до директорії налаштовується `AIONE_TELEMETRY_DIR`.
    • Записи виконуються через `asyncio.to_thread`, аби не блокувати event-loop.
    • Автоматично створює директорію та гарантує атомарні дописи (lock per file).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

# Примітка: модуль телеметрії — best‑effort. Він НІКОЛИ не повинен кидати
# винятки назовні. Будь-які несумісні типи слід м'яко серіалізувати або
# пропускати з логуванням на рівні ERROR/DEBUG.
from config.config import (
    STAGE1_LATENCY_LOG,
    STAGE3_EVENTS_LOG,
    STAGE3_TELEMETRY_ENABLED,
    TELEMETRY_BASE_DIR,
)

# Stage1 телеметрія: best‑effort імпорт із дефолтами для сумісності
try:
    from config.config import (  # type: ignore[attr-defined]
        STAGE1_EVENTS_LOG,
        STAGE1_TELEMETRY_ENABLED,
    )
except Exception:  # pragma: no cover - дефолти, якщо константи відсутні
    STAGE1_EVENTS_LOG = "stage1_events.jsonl"  # noqa: N816 - узгоджене ім'я файлу
    STAGE1_TELEMETRY_ENABLED = True  # noqa: N816

logger = logging.getLogger("monitoring.telemetry_sink")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False

BASE_PATH: Path = Path(TELEMETRY_BASE_DIR)
_locks: dict[Path, asyncio.Lock] = {}


def _get_lock(path: Path) -> asyncio.Lock:
    """Повертає (або створює) asyncio.Lock для вказаного шляху."""

    lock = _locks.get(path)
    if lock is None:
        lock = asyncio.Lock()
        _locks[path] = lock
    return lock


def _utc_now() -> str:
    """ISO-час у UTC із суфіксом Z."""

    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _compact_payload(data: Mapping[str, Any] | None) -> dict[str, Any]:
    """Видаляє None/NaN та порожні значення з словника.

    Args:
        data: Вхідний словник (може бути None).

    Returns:
        dict[str, Any]: Очищений словник.
    """

    if not data:
        return {}
    compact: dict[str, Any] = {}
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, float):
            # відкидаємо NaN/inf, які зламають json.dumps
            if value != value:  # NaN check
                continue
            if value in {float("inf"), float("-inf")}:  # pragma: no branch
                continue
        compact[key] = value
    return compact


def _json_default(value: Any) -> Any:
    """Допоміжний серіалізатор для несумісних типів."""

    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        # Безпечне перетворення байтів у UTF‑8 рядок (із заміною помилок),
        # щоб уникнути збоїв при json.dumps.
        return value.decode("utf-8", errors="replace")
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    raise TypeError(f"Непідтримуваний тип для JSON: {type(value)!r}")


def _write_line(path: Path, line: str) -> None:
    """Синхронний запис рядка у файл (append)."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line)


async def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    """Асинхронно дописує JSONL-запис у файл."""

    try:
        line = json.dumps(payload, ensure_ascii=False, default=_json_default) + "\n"
    except Exception as e:
        # Best‑effort: не провалюємо бізнес-логіку через телеметрію
        logger.error("Помилка серіалізації телеметрії: %s", e)
        logger.debug("Payload, що спричинив помилку: %r", payload)
        return

    lock = _get_lock(path)
    async with lock:
        try:
            await asyncio.to_thread(_write_line, path, line)
        except Exception:
            logger.exception("Не вдалося записати телеметрію у %s", path)


async def log_stage3_event(
    event: str,
    symbol: str,
    payload: Mapping[str, Any] | None = None,
) -> None:
    """Логує подію Stage3 у JSONL, якщо телеметрія активована.

    Args:
        event: Тип події (skip / open_blocked / open_created / trade_closed / stale_guard...).
        symbol: Символ, до якого належить подія.
        payload: Додаткові метрики/контекст (опційно).
    """

    if not STAGE3_TELEMETRY_ENABLED:
        return
    if not event:
        logger.debug("Пропуск телеметрії: не задано event")
        return

    symbol_norm = (symbol or "").upper() or "UNKNOWN"
    record: dict[str, Any] = {
        "ts": _utc_now(),
        "event": event,
        "symbol": symbol_norm,
    }
    record.update(_compact_payload(payload))

    path = BASE_PATH / STAGE3_EVENTS_LOG
    await _append_jsonl(path, record)


__all__ = ["log_stage3_event", "log_stage1_latency", "BASE_PATH"]


async def log_stage1_event(
    event: str,
    symbol: str,
    payload: Mapping[str, Any] | None = None,
) -> None:
    """Логує подію Stage1 у JSONL, якщо телеметрія активована.

    Args:
        event: Тип події (stage1_signal / phase_detected / stage2_hint / state_updated ...).
        symbol: Символ, до якого належить подія.
        payload: Додаткові метрики/контекст (опційно).
    """

    if not STAGE1_TELEMETRY_ENABLED:
        return
    if not event:
        logger.debug("Пропуск Stage1 телеметрії: не задано event")
        return

    symbol_norm = (symbol or "").upper() or "UNKNOWN"
    ts_iso = _utc_now()
    # Додаємо стабільний uid для події (hash(symbol|ts)) — зручно для офлайн-аналізу
    try:
        _uid_src = f"{symbol_norm}|{ts_iso}".encode("utf-8", errors="ignore")
        uid = hashlib.sha1(_uid_src).hexdigest()[:12]
    except Exception:
        uid = None
    record: dict[str, Any] = {
        "ts": ts_iso,
        "event": event,
        "symbol": symbol_norm,
        **({"uid": uid} if uid else {}),
    }
    record.update(_compact_payload(payload))

    path = BASE_PATH / STAGE1_EVENTS_LOG
    await _append_jsonl(path, record)


async def log_stage1_latency(payload: Mapping[str, Any] | None = None) -> None:
    """Логує цикл Stage1 KPI/latency у dedicated JSONL."""

    if not payload:
        return

    record: dict[str, Any] = {
        "ts": _utc_now(),
    }
    record.update(_compact_payload(payload))

    path = BASE_PATH / STAGE1_LATENCY_LOG
    await _append_jsonl(path, record)


__all__.append("log_stage1_event")
