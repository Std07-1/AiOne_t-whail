# -*- coding: utf-8 -*-
"""Minimal core entities for simplified episode pipeline.

Retained:
  * `Direction`, `Episode`, `EpisodeConfig` (trimmed)
  * Utility: `calculate_max_runup_drawdown`

Removed (legacy / heavy analytics):
  * Feature builders, clustering, coverage metrics, reference ranges, k-means helpers.
  * Rich logging usage kept optional but can be removed later.

Aim: provide only the data containers required by `episode_finder` & pipeline.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from rich.console import Console  # type: ignore
    from rich.logging import RichHandler  # type: ignore

    _HAS_RICH = True
except ImportError:  # pragma: no cover
    _HAS_RICH = False

logger = logging.getLogger("ep2.core")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    if _HAS_RICH:
        h = RichHandler(console=Console(stderr=True), show_path=False)  # type: ignore[arg-type]
    else:
        h = logging.StreamHandler()
        h.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        )
    logger.addHandler(h)
    logger.propagate = False


class Direction(str, Enum):
    """Напрямок епізоду.

    Values:
        UP: Висхідний рух (min → max)
        DOWN: Нисхідний рух (max → min)
    """

    UP = "up"
    DOWN = "down"


@dataclass
class Episode:
    """Структура, що описує знайдений епізод великого руху.

    Attributes:
        start_idx: Позиційний індекс початку (включно).
        end_idx: Позиційний індекс завершення (включно).
        direction: Напрямок руху.
        peak_idx: Індекс пікової точки (локальний максимум/мінімум залежно від напрямку).
        move_pct: Сукупний відсотковий рух (фракція, 0.02=2%).
        duration_bars: Кількість барів у епізоді.
        max_drawdown_pct: Максимальна просадка всередині (фракція).
        max_runup_pct: Максимальний проміжний приріст (фракція).
        t_start: Час початку.
        t_end: Час завершення.
    """

    start_idx: int
    end_idx: int
    direction: Direction
    peak_idx: int
    move_pct: float
    duration_bars: int
    max_drawdown_pct: float
    max_runup_pct: float
    t_start: pd.Timestamp
    t_end: pd.Timestamp

    def to_dict(self) -> Dict[str, Any]:
        """Серіалізувати у словник (ISO timestamps)."""
        d = asdict(self)
        d["direction"] = self.direction.value
        d["t_start"] = self.t_start.isoformat()
        d["t_end"] = self.t_end.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Episode":
        """Створити Episode із словника (зворотня десеріалізація)."""
        data = data.copy()
        data["direction"] = Direction(data["direction"])
        data["t_start"] = pd.Timestamp(data["t_start"])
        data["t_end"] = pd.Timestamp(data["t_end"])
        return cls(**data)


class EpisodeConfig:
    """Minimal configuration used by pipeline & detector only.

    Kept fields: symbol, timeframe, limit, move_pct_up/down, min_bars, max_bars,
    close_col, retrace_ratio, require_retrace, min_gap_bars, merge_adjacent,
    max_episodes, adaptive_threshold, volume_z_threshold, rsi_overbought,
    rsi_oversold.
    """

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        timeframe: str = "1m",
        limit: int = 5000,
        move_pct_up: float = 0.02,
        move_pct_down: float = 0.02,
        min_bars: int = 5,
        max_bars: int = 120,
        close_col: str = "close",
        retrace_ratio: float = 0.33,
        require_retrace: bool = True,
        min_gap_bars: int = 0,
        merge_adjacent: bool = True,
        max_episodes: Optional[int] = None,
        adaptive_threshold: bool = False,
        volume_z_threshold: float = 2.0,
        rsi_overbought: float = 80.0,
        rsi_oversold: float = 20.0,
        **_extra,
    ):
        self.symbol = symbol
        self.timeframe = timeframe
        self.limit = limit
        self.move_pct_up = move_pct_up
        self.move_pct_down = move_pct_down
        self.min_bars = min_bars
        self.max_bars = max_bars
        self.close_col = close_col
        self.retrace_ratio = retrace_ratio
        self.require_retrace = require_retrace
        self.min_gap_bars = min_gap_bars
        self.merge_adjacent = merge_adjacent
        self.max_episodes = max_episodes
        self.adaptive_threshold = adaptive_threshold
        self.volume_z_threshold = volume_z_threshold
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        # preserve any unknown extras for forward compatibility
        for k, v in _extra.items():
            setattr(self, k, v)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if k.startswith("_"):
                continue
            if callable(v):
                continue
            out[k] = v
        return out


def calculate_max_runup_drawdown(
    series: pd.Series, direction: str
) -> Tuple[float, float]:
    """Обчислити максимальний проміжний приріст (runup) та просадку (drawdown) для сегмента.

    Args:
        series: Ціновий ряд (pd.Series float).
        direction: 'up' або 'down' (використовується для семантики, формула симетрична).

    Returns:
        (max_runup_pct, max_drawdown_pct) – додатні фракції.
    """
    if series is None or series.empty:
        return 0.0, 0.0
    arr = series.astype(float).values
    start = arr[0]
    if start <= 0:
        start = max(start, 1e-12)
    rel_from_start = (arr - start) / max(start, 1e-12)
    max_runup = float(np.nanmax(rel_from_start)) if len(arr) else 0.0
    running_max = np.maximum.accumulate(arr)
    dd = (running_max - arr) / np.where(running_max <= 0, 1e-12, running_max)
    max_dd = float(np.nanmax(dd)) if len(dd) else 0.0
    if not np.isfinite(max_runup):  # pragma: no cover
        max_runup = 0.0
    if not np.isfinite(max_dd):  # pragma: no cover
        max_dd = 0.0
    return max_runup, max_dd


__all__ = [
    "Direction",
    "Episode",
    "EpisodeConfig",
    "calculate_max_runup_drawdown",
]
