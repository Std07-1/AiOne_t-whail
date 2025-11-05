"""Виявлення хибного пробою (false breakout) — pure‑утиліти без I/O.

Контракт мінімального ризику (Stage A):
- Підтримати легку перевірку для UP/DOWN без зовнішніх залежностей.
- Надати також спрощений детектор від stats (без рівня/бенду), щоб інтеграція в селектор була мінімальною.

Повертає словник:
    {"is_false": bool, "overrun_pct": float, "wick_ratio": float, "vol_z": float, "reject_bars": int}

Примітка:
- Якщо числових полів недостатньо (немає рівня/бенду/vol_z) — метрики повертаються як 0.0/1, 
  але is_false може бути True за спрощеною логікою (wick_ratio і відсутність accept).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Tuple


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def detect_false_breakout_up(
    level: float,
    band: Tuple[float, float],
    bars: Iterable[Dict[str, Any]],
    atr_pct: float,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """Повноцінний детектор для UP.

    Очікує bars зі щонайменше полями: {"high"|"h", "low"|"l", "close"|"c", "vol_z"|"volume_z"}.
    Валідація м’яка — відсутні значення замінюються на 0.0.
    """
    lo, hi = band
    min_over = _safe_float(cfg.get("min_overrun", 0.0005))
    max_over = _safe_float(cfg.get("max_overrun", 0.0030))
    min_wick = _safe_float(cfg.get("min_wick_ratio", 0.6))
    min_volz = _safe_float(cfg.get("min_vol_z", 1.5))
    max_rej = int(cfg.get("max_reject_bars", 3) or 3)

    # Візьмемо останній бар для голки
    bars_list = list(bars)
    if not bars_list:
        return {"is_false": False, "overrun_pct": 0.0, "wick_ratio": 0.0, "vol_z": 0.0, "reject_bars": 0}
    last = bars_list[-1]
    high = _safe_float(last.get("high") or last.get("h"))
    low = _safe_float(last.get("low") or last.get("l"))
    close = _safe_float(last.get("close") or last.get("c"))
    rng = max(1e-9, high - low)
    wick_ratio = max(0.0, min(1.0, (high - close) / rng)) if rng > 0 else 0.0
    overrun_pct = (high - level) / max(level, 1e-9)
    volz = _safe_float(last.get("vol_z") or last.get("volume_z"))

    # Перевірка часової відмови (reject) протягом N барів: close ≤ level
    reject_bars = 0
    for b in reversed(bars_list[-max_rej:]):
        c = _safe_float(b.get("close") or b.get("c"))
        if c <= level:
            reject_bars += 1
        else:
            break

    is_false = (
        (overrun_pct >= min_over)
        and (overrun_pct <= max_over)
        and (wick_ratio >= min_wick)
        and (volz >= min_volz if volz != 0.0 else True)
        and (reject_bars > 0 and reject_bars <= max_rej)
    )
    return {
        "is_false": bool(is_false),
        "overrun_pct": float(overrun_pct),
        "wick_ratio": float(wick_ratio),
        "vol_z": float(volz or 0.0),
        "reject_bars": int(reject_bars),
    }


def detect_false_breakout_down(
    level: float,
    band: Tuple[float, float],
    bars: Iterable[Dict[str, Any]],
    atr_pct: float,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """Повноцінний детектор для DOWN (дзеркально до UP)."""
    lo, hi = band
    min_over = _safe_float(cfg.get("min_overrun", 0.0005))
    max_over = _safe_float(cfg.get("max_overrun", 0.0030))
    min_wick = _safe_float(cfg.get("min_wick_ratio", 0.6))
    min_volz = _safe_float(cfg.get("min_vol_z", 1.5))
    max_rej = int(cfg.get("max_reject_bars", 3) or 3)

    bars_list = list(bars)
    if not bars_list:
        return {"is_false": False, "overrun_pct": 0.0, "wick_ratio": 0.0, "vol_z": 0.0, "reject_bars": 0}
    last = bars_list[-1]
    high = _safe_float(last.get("high") or last.get("h"))
    low = _safe_float(last.get("low") or last.get("l"))
    close = _safe_float(last.get("close") or last.get("c"))
    rng = max(1e-9, high - low)
    wick_ratio = max(0.0, min(1.0, (close - low) / rng)) if rng > 0 else 0.0
    overrun_pct = (level - low) / max(level, 1e-9)
    volz = _safe_float(last.get("vol_z") or last.get("volume_z"))

    reject_bars = 0
    for b in reversed(bars_list[-max_rej:]):
        c = _safe_float(b.get("close") or b.get("c"))
        if c >= level:
            reject_bars += 1
        else:
            break

    is_false = (
        (overrun_pct >= min_over)
        and (overrun_pct <= max_over)
        and (wick_ratio >= min_wick)
        and (volz >= min_volz if volz != 0.0 else True)
        and (reject_bars > 0 and reject_bars <= max_rej)
    )
    return {
        "is_false": bool(is_false),
        "overrun_pct": float(overrun_pct),
        "wick_ratio": float(wick_ratio),
        "vol_z": float(volz or 0.0),
        "reject_bars": int(reject_bars),
    }


# ── Спрощені детектори від stats (мінімальний диф для інтеграції в селектор) ───
def detect_false_breakout_up_from_stats(stats: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Легкий детектор для UP на базі доступних у stats полів.

    Умови (proxy):
      - acceptance_ok == False
      - wick_upper_q >= min_wick_ratio
      - vol_z (якщо доступний) >= min_vol_z
    """
    accept = bool(stats.get("acceptance_ok", False))
    wick_u = _safe_float(stats.get("wick_upper_q"), 0.0)
    volz = _safe_float(stats.get("vol_z") or stats.get("volume_z"), 0.0)
    min_wick = _safe_float(cfg.get("min_wick_ratio", 0.6))
    min_volz = _safe_float(cfg.get("min_vol_z", 1.5))
    is_false = (not accept) and (wick_u >= min_wick) and (volz >= min_volz or volz == 0.0)
    return {
        "is_false": bool(is_false),
        "overrun_pct": 0.0,
        "wick_ratio": float(wick_u),
        "vol_z": float(volz or 0.0),
        "reject_bars": 1,
    }
