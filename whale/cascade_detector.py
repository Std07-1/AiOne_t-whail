"""Detектор каскаду ліквідацій (LiquidationCascadeDetector).

Призначення:
    • Легка телеметрійна обгортка для фіксації підозри на каскад ліквідацій.
    • Працює у режимі telemetry_only: повертає ознаку/сила/тип без зовнішніх викликів.

Зауваження:
    • Це початковий каркас. Джерела даних (біржі/аналітика ліквідацій) можуть бути
      інтегровані пізніше; наразі використовуємо проксі-метрики зі Stage1 stats.
    • Усі пороги і TTL — лише з config.config (STAGE2_CRISIS_MODE). Бізнес-логіки тут немає.

"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config_stage2 import STAGE2_CRISIS_MODE

logger = logging.getLogger("app.monitoring.cascade")
if not logger.handlers:  # guard
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class LiquidationCascadeDetector:
    """Простий детектор каскаду ліквідацій на основі проксі-метрик.

    Методика (спрощено):
        • Використовує дві проксі-фічі зі Stage1 stats: volume_z (або volume_z_light) та
          відносну волатильність (atr_ratio ≈ atr_pct / low_gate).
        • Якщо обидві значно перевищують пороги — сигналізує про потенційний каскад.

    Контракти/зауваги:
        - Не змінює контракт Stage2/Stage3 — повертає простий словник:
          {"cascade_detected": bool, "severity": float, "type": "bearish"|"bullish"|None}
        - Telemetry-only: не робить зовнішніх викликів або записів (тільки логування).
    Логування:
        - INFO: виявлення каскаду (ключова подія).
        - DEBUG: деталі парсингу й розрахунків.
    """

    def __init__(self) -> None:
        # Завантажуємо пороги з конфігу; безпечний fallback у разі помилки.
        cfg = STAGE2_CRISIS_MODE or {}
        try:
            self._vol_thr: float = float(cfg.get("vol_spike_threshold", 2.5))
        except Exception:
            self._vol_thr = 2.5
        try:
            self._liq_thr: float = float(cfg.get("cascade_threshold", 3.0))
        except Exception:
            self._liq_thr = 3.0

        logger.debug(
            "LiquidationCascadeDetector ініціалізовано з порогами: vol_thr=%.3f, liq_thr=%.3f",
            self._vol_thr,
            self._liq_thr,
        )

    def detect_cascade(self, stats: Mapping[str, Any]) -> dict[str, Any]:
        """Оцінює ймовірність каскаду ліквідацій з проксі-факторів.

        Args:
            stats: Stage1 статистики для символу. Очікувані ключі:
                volume_z, volume_z_light, atr, current_price, low_gate,
                cumulative_delta, price_slope_atr, symbol (опц.)

        Returns:
            dict: {"cascade_detected": bool, "severity": float, "type": "bearish"|"bullish"|None}
        """
        symbol = stats.get("symbol")
        logger.debug("detect_cascade виклик для symbol=%s", symbol or "unknown")

        # --- Парсинг вхідних метрик (строго, але толерантно) ---
        volz_value = stats.get("volume_z") or stats.get("volume_z_light")
        volz = self.safe_float(volz_value, none_is_none=True)
        if volz is None:
            logger.debug("volume_z відсутній або некоректний, value=%s", volz_value)

        atr = self.safe_float(stats.get("atr"))
        cp = self.safe_float(stats.get("current_price"))
        low_gate = self.safe_float(stats.get("low_gate"), none_is_none=True)
        # Якщо current_price не задано або 0 — atr_pct 0 (без винятків)
        atr_pct = (atr / cp) if (atr is not None and cp and cp > 0) else 0.0
        atr_ratio = None
        if low_gate and low_gate > 0:
            try:
                atr_ratio = atr_pct / low_gate
            except Exception as e:
                logger.debug("Помилка при обчисленні atr_ratio: %s", e)
                atr_ratio = None
        else:
            logger.debug("low_gate відсутній/нульовий: low_gate=%s", low_gate)

        # Напрям визначаємо за cumulative_delta або price_slope_atr
        cd = self.safe_float(stats.get("cumulative_delta"))
        slope_atr = self.safe_float(stats.get("price_slope_atr"))
        direction = None
        try:
            if cd is not None and slope_atr is not None:
                # обираємо сильніший сигнал по абсолютному значенню
                if abs(cd) >= abs(slope_atr):
                    direction = -1 if cd < 0 else 1
                else:
                    direction = -1 if slope_atr < 0 else 1
            elif cd is not None:
                direction = -1 if cd < 0 else 1
            elif slope_atr is not None:
                direction = -1 if slope_atr < 0 else 1
        except Exception as e:
            logger.debug("Не вдалося визначити direction: %s", e)
            direction = None

        # Логування детальних метрик для дебагу
        logger.debug(
            "metrics(symbol=%s): volz=%s, atr=%.6f, cp=%s, low_gate=%s, atr_pct=%.6f, atr_ratio=%s, cd=%s, slope_atr=%s",
            symbol or "unknown",
            volz,
            atr if atr is not None else 0.0,
            cp,
            low_gate,
            atr_pct,
            atr_ratio,
            stats.get("cumulative_delta"),
            stats.get("price_slope_atr"),
        )
        logger.debug(
            "thresholds: vol_thr=%.3f, liq_thr=%.3f", self._vol_thr, self._liq_thr
        )

        # --- Нормалізація частин скору в [0..1] ---
        score_parts: list[float] = []
        if volz is not None:
            try:
                part = min(1.0, max(0.0, volz / max(self._vol_thr, 1e-9)))
                score_parts.append(part)
                logger.debug(
                    "volz part=%.3f (volz=%.3f, vol_thr=%.3f)",
                    part,
                    volz,
                    self._vol_thr,
                )
            except Exception as e:
                logger.debug("Помилка при нормалізації volz: %s", e)
        if atr_ratio is not None:
            try:
                part = min(1.0, max(0.0, atr_ratio / max(self._liq_thr, 1e-9)))
                score_parts.append(part)
                logger.debug(
                    "atr_ratio part=%.3f (atr_ratio=%s, liq_thr=%.3f)",
                    part,
                    atr_ratio,
                    self._liq_thr,
                )
            except Exception as e:
                logger.debug("Помилка при нормалізації atr_ratio: %s", e)

        severity = float(sum(score_parts) / len(score_parts)) if score_parts else 0.0

        # Просте правило виявлення: комбінований severity + мінімальний обсяг
        detected = bool(severity >= 0.6 and ((volz or 0.0) >= self._vol_thr))

        crisis_type = None
        if detected and direction is not None:
            crisis_type = "bearish" if direction < 0 else "bullish"

        result = {
            "cascade_detected": detected,
            "severity": round(severity, 3),
            "type": crisis_type,
        }

        # Логування результату
        if detected:
            logger.info(
                "Cascade detected for %s: severity=%.3f, type=%s, volz=%s, atr_ratio=%s",
                symbol or "unknown",
                result["severity"],
                crisis_type,
                volz,
                atr_ratio,
            )
            # TODO: відправити telemetry через централізований emitter (не робимо тут)
        else:
            logger.debug(
                "No cascade for %s: severity=%.3f (volz=%s, atr_ratio=%s)",
                symbol or "unknown",
                result["severity"],
                volz,
                atr_ratio,
            )

        return result
