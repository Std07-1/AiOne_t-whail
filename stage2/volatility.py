"""Визначення режиму волатильності (телеметрійний каркас).

Функція detect_volatility_regime порівнює короткий ATR із довгим і класифікує режим:
    • hyper_volatile, high_volatile або normal.
Пороги зчитуються з config.STAGE2_VOLATILITY_REGIME.

TODO:
- додати юніт-тести (happy-path + edge + псевдо-стрім) для цієї логіки;
- розглянути використання більш робастних метрик (median, trimmed mean) замість простого avg;
- врахувати різні пороги по символах/інструментах (feature-flag / config).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from rich.console import Console
from rich.logging import RichHandler

from config.config_stage2 import STAGE2_VOLATILITY_REGIME

logger = logging.getLogger("stage2.volatility")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


def _avg(values: Iterable[float]) -> float:
    """Обчислити середнє значення, ігноруючи нечислові елементи.

    Args:
        values: ітерабель об'єктів, які можна привести до float.

    Returns:
        float: середнє значення (0.0 якщо немає валідних елементів).
    """
    total = 0.0
    n = 0
    skipped = 0
    for v in values:
        try:
            fv = float(v)
        except Exception:
            skipped += 1
            continue
        total += fv
        n += 1

    avg_val = (total / n) if n else 0.0
    # DEBUG: деталізація розрахунку середнього (не чутливі дані)
    logger.debug("AVG_CALC: count=%d, skipped=%d, avg=%.6f", n, skipped, avg_val)
    # TODO: замінити на median/trimmed_mean, якщо дані містять сильні викиди
    return avg_val


def _ewma(values: list[float], alpha: float = 0.2) -> float:
    """Експоненційна середня для стабілізації коротких вибірок.

    Args:
        values: послідовність чисел (list для ефективності).
        alpha: коефіцієнт згладжування (0<alpha<=1).

    Returns:
        float: EWMA значення або 0.0, якщо немає валідних елементів.
    """
    if not values:
        return 0.0
    try:
        s = float(values[0])
        a = max(0.01, min(1.0, float(alpha)))
        for v in values[1:]:
            s = a * float(v) + (1.0 - a) * s
        return float(s)
    except Exception:
        return 0.0


def detect_volatility_regime(atr_values: Iterable[float]) -> tuple[str, float]:
    """Оцінює режим волатильності за відношенням короткого/довгого ATR.

    Args:
        atr_values: послідовність ATR або ATR% значень; очікуємо ≥ 100 спостережень
            для коректного розрахунку (у каркасі — використовуємо все, що є).

    Returns:
        tuple: (regime: str, atr_ratio: float)

    Логи:
        INFO — ключові події (вхідні дані, остаточний режим);
        DEBUG — проміжні статистики (short/long/ratio, пороги).
    """
    # Безпечна конвертація: зберігаємо сиру ітерацію для логування/діагностики
    try:
        raw_list = list(atr_values)
    except Exception as exc:
        logger.exception("Не вдалося ітерувати atr_values: %s", exc)
        # Якщо ітерація провалилася — повертаємо безпечний дефолт
        return "normal", 0.0

    # Пробуємо коректно привести кожен елемент до float і логувати пропуски
    vals: list[float] = []
    skipped_items = 0
    for x in raw_list:
        try:
            vals.append(float(x))
        except Exception:
            skipped_items += 1
            logger.debug("SKIP_ATR_VALUE: нечислове значення пропущено: %r", x)

    logger.info(
        "VOL_REGIME_INPUT: отримано=%d, валідних=%d, пропущено=%d",
        len(raw_list),
        len(vals),
        skipped_items,
    )

    if not vals:
        logger.info("VOL_REGIME_RESULT: недостатньо даних → normal (atr_ratio=0.0)")
        return "normal", 0.0

    # Спрощено: перші 10 як «short», перші 100 як «long» (або все доступне)
    win_short = vals[:10]
    win_long = vals[:100]
    # Робастні оцінки: якщо даних мало — використовуємо EWMA (менша чутливість до викидів)
    short = _avg(win_short) if len(win_short) >= 8 else _ewma(win_short)
    long_raw = _avg(win_long) if len(win_long) >= 30 else _ewma(win_long, alpha=0.1)
    # Якщо long не доступний — fallback на short або 1.0 (щоб уникнути ділення на нуль)
    long = long_raw or (short if short > 0 else 1.0)
    atr_ratio = float(short / long) if long > 0 else 0.0

    # Отримуємо пороги з конфігу; робимо захисні приводи типів
    try:
        hyper_thr = float(STAGE2_VOLATILITY_REGIME.get("hyper_threshold", 2.5))
    except Exception:
        hyper_thr = 2.5
        logger.debug(
            "VOL_REGIME_CONFIG: використано дефолт hyper_threshold=%s", hyper_thr
        )
    try:
        high_thr = float(STAGE2_VOLATILITY_REGIME.get("high_threshold", 1.8))
    except Exception:
        high_thr = 1.8
        logger.debug(
            "VOL_REGIME_CONFIG: використано дефолт high_threshold=%s", high_thr
        )

    # DEBUG: деталі розрахунку для трасування/аналізу
    logger.debug(
        "VOL_REGIME_STATS: short=%.6f, long=%.6f, atr_ratio=%.6f, high_thr=%.3f, hyper_thr=%.3f",
        short,
        long,
        atr_ratio,
        high_thr,
        hyper_thr,
    )

    # Визначаємо режим згідно порогів (строго послідовно)
    if atr_ratio >= hyper_thr:
        logger.info(
            "VOL_REGIME_RESULT: визначено режим hyper_volatile (atr_ratio=%.6f >= hyper_thr=%.3f)",
            atr_ratio,
            hyper_thr,
        )
        return "hyper_volatile", atr_ratio

    if atr_ratio >= high_thr:
        logger.info(
            "VOL_REGIME_RESULT: визначено режим high_volatile (atr_ratio=%.6f >= high_thr=%.3f)",
            atr_ratio,
            high_thr,
        )
        return "high_volatile", atr_ratio

    logger.info(
        "VOL_REGIME_RESULT: режим normal (atr_ratio=%.6f < high_thr=%.3f)",
        atr_ratio,
        high_thr,
    )
    return "normal", atr_ratio


def crisis_vol_score(
    atr_ratio: float | None, spike_ratio: float | None = None
) -> float:
    """Повертає скор «кризовості» волатильності у [0..1]."""

    spike_score = 0.0
    try:
        spike_thr = float(STAGE2_VOLATILITY_REGIME.get("crisis_spike_ratio", 3.0))
    except Exception:
        spike_thr = 3.0
        logger.debug(
            "[STRICT_PHASE] CRISIS_VOL_SCORE: дефолт crisis_spike_ratio=%.3f", spike_thr
        )
    try:
        spike_soft = float(
            STAGE2_VOLATILITY_REGIME.get("crisis_spike_soft", max(1.5, spike_thr - 1.0))
        )
    except Exception:
        spike_soft = max(1.5, spike_thr - 1.0)
        logger.debug(
            "[STRICT_PHASE] CRISIS_VOL_SCORE: дефолт crisis_spike_soft=%.3f", spike_soft
        )

    if spike_ratio is not None:
        try:
            sr = max(0.0, float(spike_ratio))
        except Exception:
            sr = 0.0
        if sr >= spike_thr and spike_thr > 0:
            logger.info(
                "[STRICT_PHASE] CRISIS_VOL_SCORE: spike_ratio=%.3f ≥ crisis_spike_ratio=%.3f → score=1.0",
                sr,
                spike_thr,
            )
            return 1.0
        if sr > 1.0 and spike_thr > 1.0:
            if sr >= spike_soft and spike_thr > spike_soft:
                frac_soft = (sr - spike_soft) / (spike_thr - spike_soft)
                spike_score = max(0.6, min(1.0, 0.6 + 0.4 * frac_soft))
            else:
                denom = max(1e-6, spike_soft - 1.0)
                frac_pre = (sr - 1.0) / denom
                spike_score = max(0.0, min(0.6, frac_pre * 0.6))

    try:
        ar = max(0.0, float(atr_ratio or 0.0))
    except Exception:
        logger.debug(
            "[STRICT_PHASE] CRISIS_VOL_SCORE: некоректний atr_ratio=%r → 0.0", atr_ratio
        )
        return spike_score

    try:
        hyper_thr = float(STAGE2_VOLATILITY_REGIME.get("hyper_threshold", 2.5))
    except Exception:
        hyper_thr = 2.5
        logger.debug(
            "[STRICT_PHASE] CRISIS_VOL_SCORE: дефолт hyper_threshold=%.3f", hyper_thr
        )
    try:
        high_thr = float(STAGE2_VOLATILITY_REGIME.get("high_threshold", 1.8))
    except Exception:
        high_thr = 1.8
        logger.debug(
            "[STRICT_PHASE] CRISIS_VOL_SCORE: дефолт high_threshold=%.3f", high_thr
        )

    if ar <= 1.0:
        logger.debug(
            "[STRICT_PHASE] CRISIS_VOL_SCORE: atr_ratio=%.6f ≤ 1.0 → score=%.3f",
            ar,
            spike_score,
        )
        return spike_score
    if ar >= hyper_thr:
        logger.info(
            "[STRICT_PHASE] CRISIS_VOL_SCORE: atr_ratio=%.6f ≥ hyper_thr=%.3f → score=1.0",
            ar,
            hyper_thr,
        )
        return max(1.0, spike_score)
    if ar >= high_thr:
        frac = (ar - high_thr) / (hyper_thr - high_thr) if hyper_thr > high_thr else 1.0
        score = round(0.6 + 0.4 * max(0.0, min(1.0, frac)), 3)
        logger.info(
            "[STRICT_PHASE] CRISIS_VOL_SCORE: atr_ratio=%.6f між high_thr=%.3f і hyper_thr=%.3f → score=%.3f",
            ar,
            high_thr,
            hyper_thr,
            score,
        )
        return max(score, spike_score)
    frac = (ar - 1.0) / (high_thr - 1.0) if high_thr > 1.0 else 0.0
    score = round(0.6 * max(0.0, min(1.0, frac)), 3)
    logger.debug(
        "[STRICT_PHASE] CRISIS_VOL_SCORE: atr_ratio=%.6f у зоні 1.0..high_thr=%.3f → score=%.3f",
        ar,
        high_thr,
        score,
    )
    return max(score, spike_score)
