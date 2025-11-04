"""Minimal system signal generator (спрощена версія).

Залишено лише одну публічну функцію `generate_system_signals(df, config)` яка:
  * Обчислює базові індикатори (RSI 14, volume z-score 30) якщо їх немає.
  * Емісує один сигнал на бар, якщо виконується будь-яка умова:
        RSI <= config.rsi_oversold  -> BUY  (reason RSI_OVERSOLD)
        RSI >= config.rsi_overbought -> SELL (reason RSI_OVERBOUGHT)
        volume_z >= config.volume_z_threshold -> BUY/SELL за напрямком поточного bar move (reason VOLUME_SPIKE)

Повертає DataFrame з колонками: ts, side, reason, price.
Якщо немає сигналів – порожній DataFrame з цими колонками.

Будь-які попередні складні механізми (динамічні пороги, ранжування, alignment, burst-аналіз) видалені.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, List

import numpy as np
import pandas as pd

logger = logging.getLogger("ep2.signal_utils")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    h = logging.StreamHandler()
    h.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(h)
    logger.propagate = False


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    close = close.astype(float)
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    # Deprecated: fillna(method="ffill") -> use .ffill()
    rsi = rsi.ffill().fillna(50.0)
    return rsi


def _compute_volume_z(volume: pd.Series, window: int = 30) -> pd.Series:
    vol = volume.astype(float)
    ma = vol.rolling(window).mean()
    sd = vol.rolling(window).std(ddof=0)
    z = (vol - ma) / sd.replace(0.0, np.nan)
    return z.fillna(0.0)


def generate_system_signals(
    df: pd.DataFrame, config: Any
) -> pd.DataFrame:  # noqa: D401
    """Згенерувати мінімальні сигнали.

    Parameters
    ----------
    df : DataFrame
        OHLCV (очікуються 'close','volume'). DatetimeIndex (UTC бажано).
    config : object
        Має атрибути: rsi_overbought, rsi_oversold, volume_z_threshold.
    """
    t0 = time.perf_counter()
    if df is None or df.empty:
        logger.debug("[signals] empty input dataframe")
        return pd.DataFrame(columns=["ts", "side", "reason", "price"])
    orig_len = len(df)
    logger.debug(
        "[signals] start generation rows=%d cols=%s thresholds(rsi_o=%s rsi_ob=%s vol_z=%s)",
        orig_len,
        list(df.columns),
        getattr(config, "rsi_oversold", None),
        getattr(config, "rsi_overbought", None),
        getattr(config, "volume_z_threshold", None),
    )
    df = df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):  # захист
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    # Індикатори (тільки якщо не присутні)
    if "rsi" not in df.columns:
        df["rsi"] = _compute_rsi(df["close"]) if "close" in df.columns else 50.0
        logger.debug("[signals] computed RSI col")
    if "volume_z" not in df.columns and "volume" in df.columns:
        df["volume_z"] = _compute_volume_z(df["volume"])  # коротка нормалізація
        logger.debug("[signals] computed volume_z col")
    elif "volume_z" not in df.columns:
        df["volume_z"] = 0.0
        logger.debug("[signals] fallback volume_z=0.0")

    rsi_overb = getattr(config, "rsi_overbought", 80.0)
    rsi_overs = getattr(config, "rsi_oversold", 20.0)
    vol_thr = getattr(config, "volume_z_threshold", 3.0)

    rows: List[dict] = []
    close = df.get("close")
    for i in range(1, len(df)):
        rsi_val = float(df.iloc[i]["rsi"]) if "rsi" in df.columns else 50.0
        volz_val = float(df.iloc[i]["volume_z"]) if "volume_z" in df.columns else 0.0
        price_now = float(close.iloc[i]) if close is not None else np.nan
        price_prev = float(close.iloc[i - 1]) if close is not None else price_now
        move_up = price_now >= price_prev

        reasons: List[str] = []
        side: str | None = None
        if rsi_val <= rsi_overs:
            reasons.append("RSI_OVERSOLD")
            side = "BUY"
        elif rsi_val >= rsi_overb:
            reasons.append("RSI_OVERBOUGHT")
            side = "SELL"
        if volz_val >= vol_thr:
            reasons.append("VOLUME_SPIKE")
            if side is None:  # якщо ще не визначено RSI
                side = "BUY" if move_up else "SELL"
        if not reasons:
            continue
        rows.append(
            {
                "ts": df.index[i],
                "side": side or ("BUY" if move_up else "SELL"),
                "reason": ",".join(reasons),
                "price": price_now,
            }
        )

    out = pd.DataFrame(rows, columns=["ts", "side", "reason", "price"])
    dur_ms = (time.perf_counter() - t0) * 1000
    if not out.empty and logger.isEnabledFor(logging.DEBUG):
        # quick reason counts
        reason_counts = {"RSI_OVERSOLD": 0, "RSI_OVERBOUGHT": 0, "VOLUME_SPIKE": 0}
        for r in out["reason"].astype(str):
            for key in list(reason_counts):
                if key in r:
                    reason_counts[key] += 1
        logger.debug(
            "[signals] result count=%d reasons=%s first_ts=%s last_ts=%s time=%.2fms",
            len(out),
            reason_counts,
            out.iloc[0]["ts"],
            out.iloc[-1]["ts"],
            dur_ms,
        )
    logger.info("[signals] generated=%d time=%.1fms", len(out), dur_ms)
    logger.debug("[signals] sample head:\n%s", out.head(5).to_string(index=False))
    return out


__all__ = ["generate_system_signals"]
