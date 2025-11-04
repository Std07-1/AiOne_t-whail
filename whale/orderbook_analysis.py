from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("orderbook_analysis")
if not logger.handlers:  # guard
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


@dataclass(slots=True)
class OrderbookLevel:
    price: float
    size: float


class OrderbookAnalysis:
    """
    Базовий аналіз ордербуку для пошуку маніпуляцій.

    Методи повертають булеві прапорці/списки як прості евристики.
    Логи: INFO — ключові події/результати; DEBUG — деталі розрахунків.
    TODO: інтегрувати з метриками/Redis, додати асинхронні джерела даних.
    """

    def __init__(self, large_order_threshold: float = 100.0) -> None:
        """Ініціалізація аналізатора.

        Args:
            large_order_threshold: поріг в розмірах ордера, що вважається 'великим'.
        """
        self.large_order_threshold = float(large_order_threshold)
        logger.debug(
            "OrderbookAnalysis.__init__ встановлено large_order_threshold=%.2f",
            self.large_order_threshold,
        )

    def detect_whale_manipulation(
        self, orderbook_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Агрегує декілька евристик і повертає зведений висновок.

        Args:
            orderbook_data: Об'єкт ордербуку: {"bids": [{price,size},...], "asks": [...]}.

        Returns:
            dict: Прапорці та базові оцінки маніпуляцій.
        """
        # Збір базових даних та захист від некоректного формату
        bids = orderbook_data.get("bids") or []
        asks = orderbook_data.get("asks") or []
        logger.info(
            "Запуск детекції китової активності: bids=%d, asks=%d",
            len(bids),
            len(asks),
        )

        try:
            analysis = {
                "hidden_liquidity_pools": self.find_hidden_liquidity(orderbook_data),
                "spoofing_attempts": self.detect_spoofing_patterns(orderbook_data),
                "liquidation_hunting": self.analyze_liquidation_levels(orderbook_data),
                "wall_manipulation": self.analyze_order_walls(orderbook_data),
            }
            whale_present = any(bool(v) for v in analysis.values())
            manipulation_types = [k for k, v in analysis.items() if v]
            estimated = self.estimate_whale_size(orderbook_data)

            logger.debug(
                "Аналіз ордербуку: analysis=%s, whale_present=%s, estimated_size=%.2f",
                analysis,
                whale_present,
                estimated,
            )

            # INFO-зведення для швидкого моніторингу
            if whale_present:
                logger.info(
                    "Whale detected: types=%s, estimated_size=%.2f",
                    manipulation_types,
                    estimated,
                )
            else:
                logger.debug("Whale not detected за поточними евристиками.")

            return {
                "whale_present": whale_present,
                "manipulation_type": manipulation_types,
                "estimated_size": estimated,
                "analysis": analysis,
            }
        except Exception as exc:  # захищаємо від некритичних помилок в евристиках
            logger.exception("Помилка під час detect_whale_manipulation: %s", exc)
            # Graceful fallback — відмітити як відсутність маніпуляцій
            return {
                "whale_present": False,
                "manipulation_type": [],
                "estimated_size": 0.0,
                "analysis": {},
            }

    def detect_spoofing_patterns(self, orderbook: dict[str, Any]) -> bool:
        """Виявлення штучних ордерів (спуфінг) на віддалених рівнях.

        Евристика: кілька великих лотів у діапазоні рівнів 10..20 від найкращої ціни.
        Повертає True, якщо знайдено >3 великих лотів у вказаному діапазоні.
        """
        bids = orderbook.get("bids") or []
        # Безпечне отримання зрізу — якщо рівнів менше, просто працюємо з тим, що є
        window = bids[10:20]
        count_large = 0
        for idx, level in enumerate(window):
            try:
                size = float(level.get("size", 0.0))
            except Exception:
                logger.debug(
                    "Невалідний розмір на рівні bids[%d] — ігнорується", 10 + idx
                )
                size = 0.0
            if size > self.large_order_threshold:
                count_large += 1
                logger.debug(
                    "Великий ордер у віддаленому рівні bids[%d]: size=%.2f",
                    10 + idx,
                    size,
                )

        logger.debug("detect_spoofing_patterns: count_large=%d", count_large)
        result = count_large > 3
        if result:
            logger.info(
                "Можливий spoofing: знайдено %d великих віддалених ордерів", count_large
            )
        return result

    # Спрощені заглушки для інших підпроцедур
    def find_hidden_liquidity(self, orderbook: dict[str, Any]) -> bool:  # noqa: D401
        """Пошук прихованої ліквідності: базова евристика за щільністю рівнів.

        TODO: додати аналіз швидкості появи/зникнення рівнів (time-decay), інтегрувати з реальними трейдіми.
        """
        bids = orderbook.get("bids") or []
        asks = orderbook.get("asks") or []
        result = bool(len(bids) > 20 and len(asks) > 20)
        logger.debug(
            "find_hidden_liquidity: bids=%d, asks=%d -> %s",
            len(bids),
            len(asks),
            result,
        )
        return result

    def analyze_liquidation_levels(
        self, orderbook: dict[str, Any]
    ) -> bool:  # noqa: D401
        """Оцінка ймовірності полювання на ліквідації (stop-hunt) за профілем розмірів.

        Args:
            orderbook: словник з полями "bids" і "asks", де кожен рівень — dict з
                ключами "price" та "size". Формат ожидається консультаційно як:
                {"bids": [{"price":..., "size":...}, ...], "asks": [...]}

        Returns:
            bool: True — ймовірна активність, що може вказувати на hunting for liquidations.
                  False — явних патернів не знайдено.

        Пояснення евристики:
        - Наразі робимо дві прості перевірки у топ-N bids:
            1) якщо знайдено одиночний рівень з розміром > 3 * large_order_threshold -> сигнал;
            2) якщо в топ-5 є "сходинка" — різка зміна розміру між сусідніми рівнями (ratio >= 2.0).
        - Чому: великі одиночні лоти або різкі кроки в глибині можуть використовуватись для маніпуляцій,
          що витісняють стопи / провокують ликвідації.

        TODOs (де і чому розширювати):
        - TODO(config): винести коефіцієнти (3.0 для одиночного рівня, window_size=5, step_ratio=2.0)
          у config/config.py, щоб регулювати агресивність для різних інструментів без релізу.
          Навіщо: різні активи мають різні масштаби обсягів.
        - TODO(streaming): використовувати послідовність snapshot-ів (time-decay) замість одного зрізу,
          щоб відрізнити одноразові шумові лоти від повторюваних патернів.
          Навіщо: підвищити точність та знизити FP.
        - TODO(metrics): емітити counts/ratios у Prometheus/JSONL для моніторингу FP/TP rate.
        """
        bids = orderbook.get("bids") or []
        window = bids[:5]
        logger.debug(
            "analyze_liquidation_levels: аналіз top=%d bids (large_order_threshold=%.2f)",
            len(window),
            self.large_order_threshold,
        )

        try:
            # Перевірка одиночних великих лотів
            for idx, level in enumerate(window):
                try:
                    size = float(level.get("size", 0.0))
                except Exception:
                    logger.debug(
                        "analyze_liquidation_levels: невалідний size у bids[%d] -> %r",
                        idx,
                        level,
                    )
                    continue
                logger.debug(
                    "analyze_liquidation_levels: bids[%d].size=%.2f", idx, size
                )
                if size > 3.0 * self.large_order_threshold:
                    logger.info(
                        "Можлива охота на ліквідації (single step): bids[%d].size=%.2f > 3*threshold",
                        idx,
                        size,
                    )
                    return True

            # Перевірка "сходинок" між сусідніми рівнями (ratio heuristic)
            sizes = []
            for level in enumerate(window):
                try:
                    sizes.append(float(level.get("size", 0.0)))
                except Exception:
                    sizes.append(0.0)
            logger.debug(
                "analyze_liquidation_levels: top sizes for step-analysis=%s", sizes
            )

            for j in range(1, len(sizes)):
                prev, cur = sizes[j - 1], sizes[j]
                # оберігаємо від ділення на нуль
                if prev <= 0.0:
                    continue
                ratio = cur / prev
                logger.debug(
                    "analyze_liquidation_levels: step ratio bids[%d]/bids[%d]=%.3f",
                    j,
                    j - 1,
                    ratio,
                )
                # Якщо є суттєвий стрибок (cur >> prev) — це може бути "пасткою" або ліквідаційною сходинкою
                if ratio >= 2.0 and cur >= self.large_order_threshold:
                    logger.info(
                        "Можлива охота на ліквідації (step detected): bids[%d].size=%.2f ratio=%.2f",
                        j,
                        cur,
                        ratio,
                    )
                    return True

            logger.debug(
                "analyze_liquidation_levels: явних патернів для liquidation-hunting не знайдено"
            )
            return False
        except Exception as exc:
            logger.exception("analyze_liquidation_levels: unexpected error: %s", exc)
            # Graceful fallback — не вважаємо активність за некоректних умов
            return False

    def analyze_order_walls(self, orderbook: dict[str, Any]) -> bool:  # noqa: D401
        """Виявлення видимих 'стін' (order walls) поруч з best price.

        Args:
            orderbook: словник з "bids" і "asks" (формат як у analyze_liquidation_levels).

        Returns:
            bool: True якщо виявлено помітну стіну (asks або bids) поблизу топ-рівнів.

        Евристика:
        - Для asks (top-3) перевіряємо чи є рівень > 5 * threshold.
        - Додатково: перевіряємо, чи обсяг на топ-рівні значно перевищує середній обсяг топ-N.
        """
        asks = orderbook.get("asks") or []
        bids = orderbook.get("bids") or []
        logger.debug(
            "analyze_order_walls: аналіз top asks=%d top bids=%d threshold=%.2f",
            len(asks[:3]),
            len(bids[:3]),
            self.large_order_threshold,
        )

        try:
            # Аналіз asks (стінка продажу біля best-ask)
            ask_window = asks[:3]
            ask_sizes = []
            for i, lvl in enumerate(ask_window):
                try:
                    s = float(lvl.get("size", 0.0))
                except Exception:
                    logger.debug(
                        "analyze_order_walls: невалидний size у asks[%d] -> %r", i, lvl
                    )
                    s = 0.0
                ask_sizes.append(s)
                logger.debug("analyze_order_walls: asks[%d].size=%.2f", i, s)
                if s > 5.0 * self.large_order_threshold:
                    logger.info(
                        "Виявлено order wall у asks[%d] size=%.2f (>5*threshold)", i, s
                    )
                    return True

            # Порівняльна перевірка: топ1 у відношенні до середнього top-5
            all_ask_sizes = [
                float(lvl.get("size", 0.0)) for lvl in asks[:5] if isinstance(lvl, dict)
            ]
            avg_ask = (
                (sum(all_ask_sizes) / len(all_ask_sizes)) if all_ask_sizes else 0.0
            )
            logger.debug(
                "analyze_order_walls: avg_ask_top5=%.2f ask_sizes=%s",
                avg_ask,
                all_ask_sizes,
            )
            if ask_sizes:
                top1 = ask_sizes[0]
                if (
                    avg_ask > 0
                    and top1 >= 3.0 * avg_ask
                    and top1 >= self.large_order_threshold
                ):
                    logger.info(
                        "Виявлено order wall біля best-ask: top1=%.2f >= 3*avg_top5=%.2f",
                        top1,
                        avg_ask,
                    )
                    return True

            # Аналогічний аналіз для bids (стінка покупців)
            bid_window = bids[:3]
            bid_sizes = []
            for i, lvl in enumerate(bid_window):
                try:
                    s = float(lvl.get("size", 0.0))
                except Exception:
                    logger.debug(
                        "analyze_order_walls: невалидний size у bids[%d] -> %r", i, lvl
                    )
                    s = 0.0
                bid_sizes.append(s)
                logger.debug("analyze_order_walls: bids[%d].size=%.2f", i, s)
                if s > 5.0 * self.large_order_threshold:
                    logger.info(
                        "Виявлено order wall у bids[%d] size=%.2f (>5*threshold)", i, s
                    )
                    return True

            logger.debug("analyze_order_walls: стінок не знайдено в топ-рівнях")
            return False
        except Exception as exc:
            logger.exception("analyze_order_walls: unexpected error: %s", exc)
            return False

    def estimate_whale_size(self, orderbook: dict[str, Any]) -> float:  # noqa: D401
        """Орієнтовна оцінка накопиченого обсягу 'великого гравця' по топ-рівнях.

        Args:
            orderbook: словник з "bids" і "asks".

        Returns:
            float: простий агрегат — сумарний обсяг топ-5 bids (поточна метрика).
                   Повертається як float для сумісності з існуючими компонентами.

        Пояснення і обмеження:
        - Наразі використовуємо суму top-5 bids як proxy для 'estimated_size' — це просте
          й швидке рішення для telemetriy-only показника.
        - TODO: у майбутньому замінити на більш складний weighted metric, що враховує:
            * ask-side гранулярність (баланс book),
            * spread & depth weighting (ближчі рівні вагоміші),
            * time-decay (повторювані великі лоти дають більше ваги),
            * нормалізацію по інструменту (volume-орієнтир).
          Навіщо: поточна метрика може бути недостатньо інформативною для різних інструментів;
          weighted/normalized підхід знизить помилкові інтерпретації.
        - TODO(config): виносити top_N і пороги в config/config.py.
        """
        bids = orderbook.get("bids") or []
        top = bids[:5]
        total = 0.0
        sizes = []
        for i, level in enumerate(top):
            try:
                s = float(level.get("size", 0.0))
                sizes.append(s)
                total += s
            except Exception:
                logger.debug(
                    "estimate_whale_size: невалидний розмір у top bids[%d] -> %r",
                    i,
                    level,
                )
                continue

        logger.debug(
            "estimate_whale_size: top_sizes=%s total_top5=%.2f (using top5 bids only)",
            sizes,
            total,
        )

        # Безпечний fallback — якщо нічого немає, повертаємо 0.0
        try:
            return float(total)
        except Exception:
            logger.exception(
                "estimate_whale_size: cannot cast total to float, returning 0.0"
            )
            return 0.0
