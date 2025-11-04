"""Ultra-minimal visualization.

Kept ONLY:
    * Close price line
    * Episode spans (green=up, red=down)
    * BUY / SELL signal scatter (if provided)

Removed: rich logging, synthetic entries/exits, TP/SL zones, arrows, metrics, RSI panel, legends for extras.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

import matplotlib.pyplot as plt
import pandas as pd

__all__ = ["visualize_episodes"]
logger = logging.getLogger("ep2.visualize")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler()
    h.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(h)
    logger.propagate = False


def _to_dir(ep) -> str:
    d = None
    if isinstance(ep, dict):
        d = ep.get("direction")
    else:
        d = getattr(ep, "direction", None)
    if hasattr(d, "value"):
        d = d.value
    return (str(d).lower() if isinstance(d, str) else None) or None


def _to_time(ep, key, fallback_idx_key=None, df: Optional[pd.DataFrame] = None):
    if isinstance(ep, dict):
        v = ep.get(key)
        if v is None and fallback_idx_key and df is not None:
            idx = ep.get(fallback_idx_key)
            try:
                if idx is not None and 0 <= int(idx) < len(df):
                    return df.index[int(idx)]
            except Exception:
                return None
        return v
    v = getattr(ep, key, None)
    if v is None and fallback_idx_key and df is not None:
        idx = getattr(ep, fallback_idx_key, None)
        try:
            if idx is not None and 0 <= int(idx) < len(df):
                return df.index[int(idx)]
        except Exception:
            return None
    return v


# --------------------- Main ---------------------
def visualize_episodes(
    df: pd.DataFrame,
    episodes: List[Any],
    signals_df: Optional[pd.DataFrame] = None,
    entry_mask=None,  # ignored, for compatibility
    save_path: Optional[str] = None,
    *,
    price_col: str = "close",
):
    if df is None or df.empty:
        logger.debug("[viz] empty dataframe – skip")
        return
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("df must have DatetimeIndex")
    fig, ax = plt.subplots(figsize=(12, 6))
    # Price line
    if price_col in df.columns:
        ax.plot(
            df.index,
            df[price_col].astype(float),
            color="#0D47A1",
            lw=0.9,
            label="Close",
        )

    # Episode spans
    for ep in episodes or []:
        t_start = _to_time(ep, "t_start", "start_idx", df)
        t_end = _to_time(ep, "t_end", "end_idx", df)
        if not t_start or not t_end:
            continue
        try:
            t_start = pd.Timestamp(t_start)
            t_end = pd.Timestamp(t_end)
        except Exception:
            continue
        direction = _to_dir(ep)
        color = "#A5D6A7" if direction == "up" else "#EF9A9A"
        ax.axvspan(t_start, t_end, color=color, alpha=0.35, lw=0)

    # Signals (BUY/SELL + optional SYSTEM_SIGNAL)
    if signals_df is not None and not signals_df.empty:
        s = signals_df.copy()
        if "ts" not in s.columns:
            if s.index.name and isinstance(s.index, pd.DatetimeIndex):
                s = s.reset_index().rename(columns={s.columns[0]: "ts"})
            else:
                s["ts"] = s.index
        s["ts"] = pd.to_datetime(s["ts"], errors="coerce", utc=True)
        s = s.dropna(subset=["ts"])
        # Highlight system-generated signals (type == SYSTEM_SIGNAL)
        if "type" in s.columns:
            sys_sig = s[s["type"] == "SYSTEM_SIGNAL"]
            if not sys_sig.empty:
                # fallback price column
                price_series = sys_sig.get("price")
                if price_series is None:
                    price_series = 0
                ax.scatter(
                    sys_sig["ts"],
                    price_series,
                    s=90,
                    marker="*",
                    color="gold",
                    edgecolors="orange",
                    linewidths=1.2,
                    alpha=0.9,
                    zorder=9,
                    label="SYSTEM_SIGNAL",
                )
        if "side" in s.columns and "type" in s.columns:
            # Exclude system duplicates if they also have side BUY/SELL to avoid overplotting
            side_df = s[s["type"] != "SYSTEM_SIGNAL"]
        else:
            side_df = s
        if "side" in side_df.columns:
            buys = side_df[side_df["side"].astype(str).str.upper() == "BUY"]
            sells = side_df[side_df["side"].astype(str).str.upper() == "SELL"]
            if not buys.empty:
                ax.scatter(
                    buys["ts"],
                    buys.get("price", 0),
                    s=22,
                    color="#2E7D32",
                    label="BUY",
                    zorder=5,
                )
            if not sells.empty:
                ax.scatter(
                    sells["ts"],
                    sells.get("price", 0),
                    s=22,
                    color="#C62828",
                    label="SELL",
                    zorder=5,
                )
        elif "side" in s.columns:
            buys = s[s["side"].str.upper() == "BUY"]
            sells = s[s["side"].str.upper() == "SELL"]
            if not buys.empty:
                ax.scatter(
                    buys["ts"],
                    buys.get("price", 0),
                    s=22,
                    color="#2E7D32",
                    label="BUY",
                    zorder=5,
                )
            if not sells.empty:
                ax.scatter(
                    sells["ts"],
                    sells.get("price", 0),
                    s=22,
                    color="#C62828",
                    label="SELL",
                    zorder=5,
                )

    ax.set_title("Episodes")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    ax.grid(alpha=0.25, lw=0.6)
    ax.legend(loc="upper left", fontsize=8, frameon=True)
    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=170, bbox_inches="tight")
        logger.info("[viz] saved figure -> %s", save_path)
    try:
        plt.show()
    except Exception:
        logger.debug("[viz] backend show failed (headless?)")
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "[viz] rendered episodes=%d signals=%d",
            0 if episodes is None else len(episodes),
            0 if (signals_df is None or signals_df.empty) else len(signals_df),
        )
