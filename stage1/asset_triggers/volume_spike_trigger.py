"""
Volume Spike Trigger (робастний, up-only)
Виявляє бичий сплеск обсягу за rolling Z-score та Volume/ATR, повертає прапор і метадані.

Особливості:
  • up-only: враховуємо лише, якщо close > open
  • rolling Z без лукапу: статистики по вікну без останнього бара
  • захист від нульової/мізерної дисперсії та NaN/inf
  • опційний шлях Volume/ATR (use_vol_atr)
"""

from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

try:
    from config.config import STAGE1_VOLUME_SPIKE_GATES
except Exception:  # pragma: no cover
    STAGE1_VOLUME_SPIKE_GATES = {}

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("asset_triggers.volume_spike")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


# ───────────────────── Основна функція тригера ──────────────────────
_DEBOUNCE_REGISTRY: dict[str, float] = {}


def volume_spike_trigger(
    df: pd.DataFrame,
    *,
    z_thresh: float = 2.0,
    window: int = 50,
    atr_window: int = 14,
    use_vol_atr: bool = False,
    require_upbar: bool = True,
    upbar_tolerance: float = 5e-4,
    min_effect_ratio: float = 1.15,
    symbol: str = "",
) -> tuple[bool, dict[str, float | bool | int | str]]:
    """Визначає бичий сплеск обсягу на останньому барі.

    Args:
        df: Очікує колонки ['open','high','low','close','volume'].
        z_thresh: Поріг для Z-score.
        window: Вікно для rolling статистик volume.
        atr_window: Вікно ATR.
        use_vol_atr: Додатковий критерій Volume/ATR > 2.0.
        require_upbar: Вимога, щоб close > open.
        upbar_tolerance: Допустиме відхилення (у частках, наприклад 5e-4 = 0.05%),
            в межах якого бар вважається нейтральним/flat і не фільтрується.
        min_effect_ratio: Мінімальне відхилення latest_vol від середнього.
        symbol: Для логування.

    Returns:
        (flag, meta): Прапор сплеску та метадані для UI/Stage2.
    """
    n = len(df)
    min_required = max(atr_window + 1, 3)
    if n < min_required:
        logger.debug(
            "[%s] [VolSpike] Недостатньо даних (n=%d, need≥%d)",
            symbol,
            n,
            min_required,
        )
        return False, {
            "z": 0.0,
            "mean": 0.0,
            "std": 0.0,
            "vol_atr": 0.0,
            "upbar": False,
        }

    # --- Останній бар ---
    # Джерело обсягу: quote_volume пріоритетно, якщо доступний і дозволено
    use_quote = bool(STAGE1_VOLUME_SPIKE_GATES.get("use_quote_volume", True))
    vol_series = (
        df.get("quote_volume")
        if (use_quote and "quote_volume" in df.columns)
        else df.get("volume")
    )
    latest_value_raw = vol_series.iloc[-1]
    latest_value = pd.to_numeric(latest_value_raw, errors="coerce")
    latest_vol = float(latest_value) if not pd.isna(latest_value) else 0.0
    open_ = float(df["open"].iloc[-1])
    close_ = float(df["close"].iloc[-1])
    tol = max(0.0, float(upbar_tolerance))
    if open_ != 0:
        rel_delta = (close_ - open_) / open_
    else:
        rel_delta = close_ - open_
    upbar = rel_delta >= -tol

    if require_upbar and not upbar:
        logger.debug(
            "[%s] [VolSpike] Відсічено: не upbar (Δ=%.5f, tol=%.5f)",
            symbol,
            rel_delta,
            tol,
        )
        return False, {
            "z": 0.0,
            "mean": 0.0,
            "std": 0.0,
            "vol_atr": 0.0,
            "upbar": False,
        }

    # --- Rolling статистики без останнього бара (анти-лукап) ---
    effective_window = min(window, max(1, n - 1))
    vol_history = pd.to_numeric(vol_series.iloc[:-1], errors="coerce").dropna()
    vol_win = vol_history.tail(effective_window)
    if len(vol_win) < max(2, min(10, effective_window)):
        logger.debug(
            "[%s] [VolSpike] Недостатньо валідних volume значень для статистики (len=%d, eff_win=%d)",
            symbol,
            len(vol_win),
            effective_window,
        )
        return False, {
            "z": 0.0,
            "mean": 0.0,
            "std": 0.0,
            "vol_atr": 0.0,
            "upbar": bool(upbar),
            "latest_vol": float(latest_vol),
        }
    mean_vol = float(vol_win.mean())
    std_vol = float(vol_win.std(ddof=0))
    if std_vol == 0:
        std_vol = 1e-9

    # Захист від нульової дисперсії забезпечено вище

    z = (latest_vol - mean_vol) / std_vol
    effect_ok = (mean_vol > 0) and (latest_vol / mean_vol >= min_effect_ratio)
    # Робастний MAD‑Z (опційно)
    if bool(STAGE1_VOLUME_SPIKE_GATES.get("z_mad_enabled", True)):
        base = pd.Series(
            vol_history.tail(int(STAGE1_VOLUME_SPIKE_GATES.get("z_mad_window", 50)))
        )
        med = float(base.median()) if len(base) else 0.0
        mad = float((base - med).abs().median()) if len(base) else 0.0
        mad = mad if mad > 0 else 1e-9
        z_mad = (latest_vol - med) / (1.4826 * mad)
    else:
        z_mad = z

    # EMA‑ratio (опційно)
    ema_ratio_pass = False
    if bool(STAGE1_VOLUME_SPIKE_GATES.get("ema_ratio_enabled", False)):
        ema_win = max(2, int(STAGE1_VOLUME_SPIKE_GATES.get("ema_window", 30)))
        ema = (
            float(vol_history.ewm(span=ema_win, adjust=False).mean().iloc[-1])
            if len(vol_history)
            else 0.0
        )
        ema_ratio = (latest_vol / ema) if ema > 0 else 0.0
        ema_ratio_pass = ema_ratio >= float(
            STAGE1_VOLUME_SPIKE_GATES.get("ema_ratio_thr", 1.20)
        )
    else:
        ema_ratio = 0.0

    # --- ATR шлях (опційно) ---
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    tr_tail = tr.dropna().tail(atr_window)
    atr = float(tr_tail.mean()) if len(tr_tail) else 0.0
    vol_atr = (latest_vol / atr) if (atr > 0) else 0.0

    # Обираємо найкращий сигнал: std‑Z або MAD‑Z або EMA‑ratio
    z_base_pass = (z >= z_thresh) and (z > 0.0) and effect_ok
    z_mad_pass = (
        (z_mad >= float(STAGE1_VOLUME_SPIKE_GATES.get("z_thresh", z_thresh)))
        and (z_mad > 0.0)
        and effect_ok
    )
    z_pass = bool(z_base_pass or z_mad_pass or ema_ratio_pass)
    atr_pass = (vol_atr > 2.0) if use_vol_atr else False

    flag = bool(z_pass or atr_pass)

    # Мінімальна ліквідність
    min_qv = float(STAGE1_VOLUME_SPIKE_GATES.get("min_quote_volume", 0.0))
    if min_qv > 0.0 and latest_vol < min_qv:
        return False, {
            "reason": "min_quote_volume",
            "z": float(z),
            "z_mad": float(z_mad),
            "ema_ratio": float(ema_ratio),
            "mean": float(mean_vol),
            "std": float(std_vol),
            "vol_atr": float(vol_atr),
            "upbar": bool(upbar),
            "latest_vol": float(latest_vol),
        }

    # Low‑vol підтвердження
    price_now = close_
    atr_pct = (atr / price_now) if price_now > 0 else 0.0
    low_vol_thr = float(STAGE1_VOLUME_SPIKE_GATES.get("low_vol_atr_pct_thr", 0.004))
    body = abs(close_ - open_)
    body_atr = (body / atr) if atr > 0 else 0.0
    two_bar_ok = False
    if bool(STAGE1_VOLUME_SPIKE_GATES.get("two_bar_confirm_enabled", True)) and n >= 2:
        prev_high = float(df["high"].iloc[-2])
        two_bar_ok = close_ > prev_high

    if atr_pct < low_vol_thr:
        if not (
            body_atr
            >= float(STAGE1_VOLUME_SPIKE_GATES.get("min_body_atr_for_low_vol", 0.40))
            or two_bar_ok
        ):
            return False, {
                "reason": "low_vol_confirm",
                "z": float(z),
                "z_mad": float(z_mad),
                "ema_ratio": float(ema_ratio),
                "atr_pct": float(atr_pct),
                "body_atr": float(body_atr),
                "two_bar_ok": bool(two_bar_ok),
            }

    # Wick/body форма свічки
    high_ = float(df["high"].iloc[-1])
    low_ = float(df["low"].iloc[-1])
    rng = max(1e-12, high_ - low_)
    lower_wick = max(0.0, open_ - low_) if close_ >= open_ else max(0.0, close_ - low_)
    body_ratio = abs(close_ - open_) / rng
    wick_ratio = lower_wick / rng
    if body_ratio < float(
        STAGE1_VOLUME_SPIKE_GATES.get("body_ratio_min", 0.25)
    ) or wick_ratio > float(STAGE1_VOLUME_SPIKE_GATES.get("wick_ratio_max", 0.55)):
        return False, {
            "reason": "wick_body_filter",
            "body_ratio": float(body_ratio),
            "wick_ratio": float(wick_ratio),
        }

    # Near‑edge фільтр (rolling екстремуми як простий проксі рівнів)
    lev_win = max(3, int(STAGE1_VOLUME_SPIKE_GATES.get("levels_window", 50)))
    highs_hist = pd.to_numeric(
        df["high"].iloc[-(lev_win + 1) : -1], errors="coerce"
    ).dropna()
    lows_hist = pd.to_numeric(
        df["low"].iloc[-(lev_win + 1) : -1], errors="coerce"
    ).dropna()
    if len(highs_hist) >= 3 and len(lows_hist) >= 3:
        recent_max = float(highs_hist.max())
        recent_min = float(lows_hist.min())
        band = max(1e-9, recent_max - recent_min)
        dist_to_edge = min(abs(close_ - recent_min), abs(recent_max - close_))
        near_pct = dist_to_edge / band if band > 0 else 1.0
        if near_pct > float(
            STAGE1_VOLUME_SPIKE_GATES.get("near_edge_window_pct", 0.15)
        ):
            return False, {
                "reason": "far_from_edge",
                "near_pct": float(near_pct),
                "band": float(band),
            }

    # Легкий directional фільтр: slope/ATR за невелике вікно
    slope_win = max(2, int(STAGE1_VOLUME_SPIKE_GATES.get("slope_atr_window", 3)))
    closes_hist = pd.to_numeric(
        df["close"].iloc[-(slope_win + 1) :], errors="coerce"
    ).dropna()
    if len(closes_hist) >= 2 and atr > 0:
        slope = (float(closes_hist.iloc[-1]) - float(closes_hist.iloc[0])) / atr
        if slope < float(STAGE1_VOLUME_SPIKE_GATES.get("slope_atr_min", 0.10)):
            return False, {
                "reason": "slope_filter",
                "slope_atr": float(slope),
            }

    # Дебаунс за символом
    debounce_sec = float(STAGE1_VOLUME_SPIKE_GATES.get("debounce_sec", 90))
    if symbol:
        now = time.monotonic()
        last = _DEBOUNCE_REGISTRY.get(symbol)
        if last is not None and (now - last) < debounce_sec:
            return False, {"reason": "debounce"}
        _DEBOUNCE_REGISTRY[symbol] = now

    logger.debug(
        "[%s] [VolSpike] upbar=%s | Z=%.2f thr=%.2f pass=%s | eff=%.2f× (min %.2f) | Vol/ATR=%.2f pass=%s",
        symbol,
        upbar,
        z,
        z_thresh,
        z_pass,
        (latest_vol / mean_vol) if mean_vol > 0 else np.nan,
        min_effect_ratio,
        vol_atr,
        atr_pass,
    )

    meta: dict[str, Any] = {
        "z": float(z),
        "z_mad": float(z_mad),
        "ema_ratio": float(ema_ratio),
        "mean": float(mean_vol),
        "std": float(std_vol),
        "vol_atr": float(vol_atr),
        "upbar": bool(upbar),
        "latest_vol": float(latest_vol),
    }
    return flag, meta
