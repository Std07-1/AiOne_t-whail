from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("large_order_detection")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class LargeOrderDetection:
    """Аналіз реальних трейдів на предмет накопичення/розподілу (каркас).

    Реалізація містить прості евристики з детальним логуванням для offline-аналізу
    та подальшої інтеграції у Stage1/Stage2 пайплайн. Всі методи роблять захищені
    перетворення даних і не мають побічних ефектів (поки що).

    TODO:
    - Винести пороги та константи у config/config.py.
    - Додати асинхронні джерела/псевдо-стріми для тестів (псевдо-стрім).
    - Написати unit-тести: happy-path + edge-cases (порожні/некоректні записи).
    """

    def __init__(self, min_trade_volume_threshold: float = 1.0) -> None:
        """Ініціалізація детектора.

        Args:
            min_trade_volume_threshold: мінімальний обсяг трейду для врахування
                в метриках (для фільтрації дрібних шумів).
        """
        self.min_trade_volume_threshold = float(min_trade_volume_threshold)
        logger.debug(
            "LargeOrderDetection.__init__ встановлено min_trade_volume_threshold=%.2f",
            self.min_trade_volume_threshold,
        )

    def detect_accumulation_distribution(
        self, trade_data: Iterable[dict[str, Any]]
    ) -> dict[str, Any]:
        """Повертає поточну фазу ринку і кілька метрик (спрощено).

        Args:
            trade_data: ітерабель колекція трейдів, де кожен trade може мати
                ключі: price, size, side ('buy'|'sell'), visible (Bool, опційно).

        Returns:
            dict: {
                "market_phase": str,
                "confidence": float (0..1),
                "accumulation_score": float (0..100),
                "target_levels": list[float]
            }
        """
        logger.info("detect_accumulation_distribution: старт аналізу трейдів")
        try:
            # Обчислюємо базові евристики та логування проміжних результатів
            stealth = self.find_stealth_buying(trade_data)
            distribution = self.find_distribution_selling(trade_data)
            markup = self.detect_markup_phase(trade_data)
            markdown = self.detect_markdown_phase(trade_data)

            patterns = {
                "stealth_accumulation": stealth,
                "distribution_phase": distribution,
                "markup_phase": markup,
                "markdown_phase": markdown,
            }
            logger.debug(
                "Евристики (stealth, distribution, markup, markdown) = %s", patterns
            )

            # Визначаємо найсильніший патерн (за значенням евристики)
            current = max(patterns, key=patterns.get)
            confidence = float(patterns.get(current, 0.0)) if patterns else 0.0

            accumulation_score = self.calculate_accumulation_score(trade_data)
            target_levels = self.calculate_institutional_targets(trade_data)

            result = {
                "market_phase": current,
                "confidence": confidence,
                "accumulation_score": accumulation_score,
                "target_levels": target_levels,
            }

            logger.info(
                "detect_accumulation_distribution: phase=%s confidence=%.2f acc_score=%.2f targets=%s",
                current,
                confidence,
                accumulation_score,
                target_levels,
            )
            return result
        except Exception as exc:
            logger.exception(
                "Помилка в detect_accumulation_distribution — повертаємо дефолт: %s",
                exc,
            )
            # Безпечний fallback
            return {
                "market_phase": "unknown",
                "confidence": 0.0,
                "accumulation_score": 0.0,
                "target_levels": [],
            }

    # -------------------------
    # Простi евристики (з логуванням)
    # -------------------------
    def find_stealth_buying(
        self, trade_data: Iterable[dict[str, Any]]
    ) -> float:  # noqa: D401
        """Оцінка прихованих покупок (0..1).

        Метод підраховує відношення обсягу 'buy' транзакцій (можливо невидимих)
        до загального обсягу. Якщо є поле 'visible' і воно False — це підсилює сигнал.
        """
        buy_vol = 0.0
        total_vol = 0.0
        for i, t in enumerate(trade_data):
            try:
                size = float(t.get("size", 0.0))
            except Exception:
                logger.debug(
                    "find_stealth_buying: невалідний size у trade[%d] — ігнор", i
                )
                continue
            if size < self.min_trade_volume_threshold:
                logger.debug(
                    "find_stealth_buying: trade[%d] занадто малий (%.2f) — ігнор",
                    i,
                    size,
                )
                continue
            side = (t.get("side") or "").lower()
            visible = t.get("visible")
            multiplier = (
                1.2 if visible is False else 1.0
            )  # невидимі trades -> трохи сильніший сигнал
            total_vol += size
            if side == "buy":
                buy_vol += size * multiplier

        ratio = (buy_vol / total_vol) if total_vol > 0 else 0.0
        ratio = max(0.0, min(1.0, ratio))
        logger.debug(
            "find_stealth_buying: buy_vol=%.2f total_vol=%.2f ratio=%.3f",
            buy_vol,
            total_vol,
            ratio,
        )
        return float(ratio)

    def find_distribution_selling(
        self, trade_data: Iterable[dict[str, Any]]
    ) -> float:  # noqa: D401
        """Оцінка розподілу (0..1).

        Аналогічно до stealth_buying, але для продажів; повертає відношення sell_vol/total_vol.
        """
        sell_vol = 0.0
        total_vol = 0.0
        for i, t in enumerate(trade_data):
            try:
                size = float(t.get("size", 0.0))
            except Exception:
                logger.debug(
                    "find_distribution_selling: невалідний size у trade[%d] — ігнор", i
                )
                continue
            if size < self.min_trade_volume_threshold:
                continue
            side = (t.get("side") or "").lower()
            visible = t.get("visible")
            multiplier = 1.2 if visible is False else 1.0
            total_vol += size
            if side == "sell":
                sell_vol += size * multiplier

        ratio = (sell_vol / total_vol) if total_vol > 0 else 0.0
        ratio = max(0.0, min(1.0, ratio))
        logger.debug(
            "find_distribution_selling: sell_vol=%.2f total_vol=%.2f ratio=%.3f",
            sell_vol,
            total_vol,
            ratio,
        )
        return float(ratio)

    def detect_markup_phase(
        self, trade_data: Iterable[dict[str, Any]]
    ) -> float:  # noqa: D401
        """Фаза росту ціни (0..1).

        Проксі: частка послідовних підйомів ціни у послідовності трейдів.
        """
        prices: list[float] = []
        for i, t in enumerate(trade_data):
            try:
                p = float(t.get("price"))
            except Exception:
                logger.debug("detect_markup_phase: trade[%d] без price — ігнор", i)
                continue
            prices.append(p)

        if len(prices) < 2:
            logger.debug("detect_markup_phase: недостатньо даних (len=%d)", len(prices))
            return 0.0

        increases = sum(1 for i in range(1, len(prices)) if prices[i] > prices[i - 1])
        score = increases / (len(prices) - 1)
        logger.debug(
            "detect_markup_phase: increases=%d steps=%d score=%.3f",
            increases,
            len(prices) - 1,
            score,
        )
        return float(max(0.0, min(1.0, score)))

    def detect_markdown_phase(
        self, trade_data: Iterable[dict[str, Any]]
    ) -> float:  # noqa: D401
        """Фаза зниження ціни (0..1).

        Проксі: частка послідовних падінь ціни у послідовності трейдів.
        """
        prices: list[float] = []
        for t in trade_data:
            try:
                p = float(t.get("price"))
            except Exception:
                continue
            prices.append(p)

        if len(prices) < 2:
            return 0.0

        decreases = sum(1 for i in range(1, len(prices)) if prices[i] < prices[i - 1])
        score = decreases / (len(prices) - 1)
        logger.debug(
            "detect_markdown_phase: decreases=%d steps=%d score=%.3f",
            decreases,
            len(prices) - 1,
            score,
        )
        return float(max(0.0, min(1.0, score)))

    def calculate_accumulation_score(
        self, trade_data: Iterable[dict[str, Any]]
    ) -> float:  # noqa: D401
        """Індекс накопичення (0..100).

        По суті — нормалізований відсоток buy-volume у шкалі 0..100.
        """
        # Для повторного проходу структуруємо сумарні обсяги
        buy = 0.0
        total = 0.0
        for t in trade_data:
            try:
                size = float(t.get("size", 0.0))
            except Exception:
                continue
            if size < self.min_trade_volume_threshold:
                continue
            total += size
            side = (t.get("side") or "").lower()
            if side == "buy":
                buy += size

        score = (buy / total * 100.0) if total > 0 else 0.0
        score = max(0.0, min(100.0, score))
        logger.debug(
            "calculate_accumulation_score: buy=%.2f total=%.2f score=%.2f",
            buy,
            total,
            score,
        )
        return float(score)

    def calculate_institutional_targets(
        self, trade_data: Iterable[dict[str, Any]]
    ) -> list[float]:  # noqa: D401
        """Приблизні таргети за кластерами обсягу (порожній список як заглушка).

        Проста евристика: середня ціна ± невеликі відсотки як початкові цілі.
        TODO: замінити на clustering по price-volume та опрацювати spread/depth.
        """
        prices: list[float] = []
        for t in trade_data:
            try:
                p = float(t.get("price"))
            except Exception:
                continue
            prices.append(p)

        if not prices:
            logger.debug("calculate_institutional_targets: немає цін — повертаємо []")
            return []

        avg = sum(prices) / len(prices)
        targets = [round(avg * 0.98, 8), round(avg * 1.02, 8)]
        logger.debug(
            "calculate_institutional_targets: avg=%.6f targets=%s", avg, targets
        )
        # TODO: записувати targets у Redis (через config) з TTL для історії
        return targets
