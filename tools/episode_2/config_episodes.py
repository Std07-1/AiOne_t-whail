# ep_2/config_episodes.py

"""Minimal episode configuration presets.

Retained keys only:
  symbol, timeframe, limit,
  move_pct_up, move_pct_down,
  min_bars, max_bars,
  close_col,
  retrace_ratio, require_retrace,
  min_gap_bars, merge_adjacent, max_episodes,
  adaptive_threshold (placeholder for future),
  volume_z_threshold, rsi_overbought, rsi_oversold.

All legacy feature / clustering / coverage / TP-SL parameters removed.
"""

# Оновлена конфігурація для 1хв
EPISODE_CONFIG_1M = {
    "symbol": "TONUSDT",
    "timeframe": "1m",
    "limit": 5000,
    "min_gap_bars": 12,
    "merge_adjacent": True,
    "max_episodes": None,
    "move_pct_up": 0.014,
    "move_pct_down": 0.014,
    "min_bars": 4,
    "max_bars": 90,
    "close_col": "close",
    "retrace_ratio": 0.28,
    "require_retrace": False,
    "adaptive_threshold": True,
    "volume_z_threshold": 1.5,
    "rsi_overbought": 78,
    "rsi_oversold": 18,
}

EPISODE_CONFIG_5M = {
    "symbol": "TONUSDT",
    "timeframe": "5m",
    "limit": 5000,
    "min_gap_bars": 7,
    "merge_adjacent": True,
    "max_episodes": None,
    "move_pct_up": 0.022,
    "move_pct_down": 0.022,
    "min_bars": 5,
    "max_bars": 120,
    "close_col": "close",
    "retrace_ratio": 0.30,
    "require_retrace": True,
    "adaptive_threshold": True,
    "volume_z_threshold": 1.5,
    "rsi_overbought": 84,
    "rsi_oversold": 22,
}


# Параметри генерації системних сигналів (Stage1) для епізодного аналізу
SYSTEM_SIGNAL_CONFIG = {"enabled": True, "lookback": 100}

# Уніфіковані константи (винесені з інших модулів)
DEFAULT_SIGNAL_ALIGN_TOLERANCE_RATIO = 0.5  # частка ширини бара
DEFAULT_GLOBAL_COVERAGE_WARN_RATIO = 0.25
DEFAULT_INSIDE_BUFFER_MINUTES = 0
DEFAULT_HIGHLIGHT_EDGE_MINUTES = 5

__all__ = [
    "EPISODE_CONFIG_1M",
    "EPISODE_CONFIG_5M",
    "EPISODE_CONFIG",
    "SYSTEM_SIGNAL_CONFIG",
    "DEFAULT_SIGNAL_ALIGN_TOLERANCE_RATIO",
    "DEFAULT_GLOBAL_COVERAGE_WARN_RATIO",
    "DEFAULT_INSIDE_BUFFER_MINUTES",
    "DEFAULT_HIGHLIGHT_EDGE_MINUTES",
]


"""(Legacy experimental notes removed for minimal build)"""
