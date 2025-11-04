from __future__ import annotations

from typing import Any

# Простий модульний автобус доказів за символом
_STATE: dict[str, list[dict[str, Any]]] = {}


def start(symbol: str) -> None:
    """Почати збір доказів для символу (скидає попередні)."""
    _STATE[str(symbol).upper()] = []


def add(
    symbol: str, key: str, value: Any, weight: float = 1.0, note: str | None = None
) -> None:
    """Додати пункт доказу для символу."""
    sym = str(symbol).upper()
    buf = _STATE.setdefault(sym, [])
    buf.append({"key": key, "value": value, "weight": float(weight), "note": note})


def snapshot(symbol: str) -> list[dict[str, Any]]:
    """Отримати поточний список доказів (без очищення)."""
    return list(_STATE.get(str(symbol).upper(), []))


def finish(symbol: str) -> tuple[list[dict[str, Any]], str]:
    """Завершити збір: повернути (evidence_list, explain_str) і очистити буфер."""
    sym = str(symbol).upper()
    items = list(_STATE.get(sym, []))
    _STATE[sym] = []
    # Побудова короткого explain: топ-3 за вагою
    try:
        top = sorted(items, key=lambda x: float(x.get("weight", 0.0)), reverse=True)[:3]
        parts = []
        for it in top:
            k = str(it.get("key"))
            v = it.get("value")
            w = float(it.get("weight") or 0.0)
            parts.append(f"{k}={v} (w={w:.2f})")
        explain = "; ".join(parts)
    except Exception:
        explain = ""
    return items, explain
