from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _sf(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def h1_ctx(last_h1: Mapping[str, Any] | None) -> tuple[str, bool]:
    """Оцінка H1 контексту (тренд і кон-флюенс) — pure, без I/O.

    Вхід (гнучкий):
      - очікуємо поля: open, close, ma (опційно), slope (опційно).
    Правила:
      - trend: up якщо close >= open (і/або slope>0), down якщо close < open (і/або slope<0), flat інакше.
      - confluence: 1 якщо (ma існує і close >= ma) та відносний рух >= 0.2%.
    """
    if not isinstance(last_h1, Mapping):
        return ("unknown", False)
    o = _sf(last_h1.get("open"))
    c = _sf(last_h1.get("close"))
    ma = _sf(last_h1.get("ma"), 0.0)
    slope = _sf(last_h1.get("slope"), 0.0)
    trend = "flat"
    if c > o or slope > 0:
        trend = "up"
    elif c < o or slope < 0:
        trend = "down"
    else:
        trend = "flat"
    rel = 0.0 if o == 0 else abs(c - o) / max(abs(o), 1e-9)
    conf = bool((ma == 0.0 or c >= ma) and rel >= 0.002)
    return (trend, conf)


def d1_bias(last_d1: Mapping[str, Any] | None) -> str:
    """Оцінка D1 bias у {pos,neg,flat,unknown}."""
    if not isinstance(last_d1, Mapping):
        return "unknown"
    o = _sf(last_d1.get("open"))
    c = _sf(last_d1.get("close"))
    if c > o:
        return "pos"
    if c < o:
        return "neg"
    return "flat"
