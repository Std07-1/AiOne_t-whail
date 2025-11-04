"""Адаптер режиму волатильності для Stage1 (використовує stage2.volatility).

Ціль:
- Побудувати проксі-ряд ATR% з df (1m) без важких обчислень ATR на кожному барі.
- Викликати detect_volatility_regime і повернути режим та atr_ratio.
- Додатково обчислити crisis_vol_score як телеметрію.

Контракти:
- Жодних змін Stage1 ключів; повертаємо невеликий dict.
- Без побічних ефектів; лише логування INFO/DEBUG.
"""

from __future__ import annotations

import logging
from statistics import median
from typing import Any

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from stage2.volatility import crisis_vol_score, detect_volatility_regime

logger = logging.getLogger("volatility_adapter")
if not logger.handlers:  # guard
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


def _safe_series(values: Any) -> list[float]:
    try:
        s = pd.to_numeric(values, errors="coerce").dropna()
        return [float(x) for x in s.tolist()]
    except Exception:
        out: list[float] = []
        try:
            for v in list(values or []):
                try:
                    out.append(float(v))
                except Exception:
                    continue
        except Exception:
            return []
        return out


def compute_vol_regime_from_df(
    df: pd.DataFrame, *, max_len: int = 150
) -> dict[str, Any]:
    """Оцінити режим волатильності із df за проксі ATR% = (high-low)/close.

    Args:
        df: DataFrame із колонками high/low/close.
        max_len: максимум останніх барів для обчислення.

    Returns:
        dict: {"regime": str, "atr_ratio": float, "crisis_vol_score": float}
    """
    try:
        if df is None or df.empty:
            return {"regime": "normal", "atr_ratio": 0.0, "crisis_vol_score": 0.0}
        highs = _safe_series(df.get("high"))
        lows = _safe_series(df.get("low"))
        closes = _safe_series(df.get("close"))
        n = min(len(highs), len(lows), len(closes))
        if n == 0:
            return {"regime": "normal", "atr_ratio": 0.0, "crisis_vol_score": 0.0}
        highs = highs[-max_len:]
        lows = lows[-max_len:]
        closes = closes[-max_len:]
        atr_pct_vals: list[float] = []
        for h, low_v, c in zip(highs, lows, closes, strict=True):
            try:
                c_safe = abs(float(c))
                if c_safe <= 0:
                    atr_pct_vals.append(0.0)
                else:
                    atr_pct_vals.append(float(abs(float(h) - float(low_v)) / c_safe))
            except Exception:
                atr_pct_vals.append(0.0)
        regime, atr_ratio = detect_volatility_regime(atr_pct_vals)
        recent = atr_pct_vals[-min(len(atr_pct_vals), 60) :]
        try:
            med_val = float(median(recent)) if recent else 0.0
        except Exception:
            med_val = 0.0
        latest = float(atr_pct_vals[-1]) if atr_pct_vals else 0.0
        spike_ratio = latest / med_val if med_val > 0 else 0.0
        crisis = crisis_vol_score(atr_ratio, spike_ratio)
        logger.info(
            "[VOL_REGIME] regime=%s atr_ratio=%.3f crisis=%.2f spike_ratio=%.2f",
            regime,
            float(atr_ratio),
            float(crisis),
            float(spike_ratio),
        )
        return {
            "regime": regime,
            "atr_ratio": float(atr_ratio),
            "crisis_vol_score": float(crisis),
            "atr_spike_ratio": float(spike_ratio),
        }
    except Exception:
        logger.debug("compute_vol_regime_from_df: error", exc_info=True)
        return {"regime": "normal", "atr_ratio": 0.0, "crisis_vol_score": 0.0}
