"""Динамічні краї ренджу та близькість до краю (HTF-проксі).

Функція compute_range_edges() обчислює краї ренджу за перцентилями по вікну
переданого DataFrame (best-effort), а також повертає ознаку близькості до краю.

Призначення:
- Легка, беззалежнісна оцінка країв на наявному df (1m), щоб дати Stage1 контекст.
- Без зовнішніх I/O; жодних змін контрактів Stage1.

Args:
    df (pandas.DataFrame): Очікуються колонки 'high', 'low', 'close'.
    eps_pct (float): Поріг близькості до краю у частках ціни (0.003 = 0.3%).
    q_low (float): Нижній перцентиль (0..1) для краю, напр. 0.1.
    q_high (float): Верхній перцентиль (0..1) для краю, напр. 0.9.

Returns:
    dict: {
        'lower_edge': float|None,
        'upper_edge': float|None,
        'band_pct': float|None,           # (upper-lower)/price
        'near_edge': 'lower'|'upper'|None,
        'dist_to_edge_pct': float|None    # мін. відносна відстань до краю
    }
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def compute_range_edges(
    df: pd.DataFrame,
    *,
    eps_pct: float = 0.004,
    q_low: float = 0.10,
    q_high: float = 0.90,
) -> dict[str, Any]:
    try:
        if df is None or df.empty:
            return {
                "lower_edge": None,
                "upper_edge": None,
                "band_pct": None,
                "near_edge": None,
                "dist_to_edge_pct": None,
            }
        # Використаємо high/low для стабільніших оцінок країв
        highs = pd.to_numeric(df.get("high"), errors="coerce").dropna()
        lows = pd.to_numeric(df.get("low"), errors="coerce").dropna()
        closes = pd.to_numeric(df.get("close"), errors="coerce").dropna()
        if len(highs) == 0 or len(lows) == 0 or len(closes) == 0:
            return {
                "lower_edge": None,
                "upper_edge": None,
                "band_pct": None,
                "near_edge": None,
                "dist_to_edge_pct": None,
            }
        # Краї як перцентилі
        lower_edge = float(np.quantile(lows, q_low))
        upper_edge = float(np.quantile(highs, q_high))
        # Поточна ціна
        price = float(closes.iloc[-1])
        band = max(1e-9, upper_edge - lower_edge)
        band_pct = float(band / max(1e-9, price))
        # Близькість до краю
        dist_lower = abs(price - lower_edge) / max(1e-9, price)
        dist_upper = abs(upper_edge - price) / max(1e-9, price)
        dist_to_edge_pct = float(min(dist_lower, dist_upper))
        if dist_lower <= dist_upper:
            closest = "lower"
            dist_min = dist_lower
        else:
            closest = "upper"
            dist_min = dist_upper
        near_edge = closest if dist_min <= float(eps_pct) else None
        return {
            "lower_edge": lower_edge,
            "upper_edge": upper_edge,
            "band_pct": band_pct,
            "near_edge": near_edge,
            "dist_to_edge_pct": dist_to_edge_pct,
        }
    except Exception:
        return {
            "lower_edge": None,
            "upper_edge": None,
            "band_pct": None,
            "near_edge": None,
            "dist_to_edge_pct": None,
        }
