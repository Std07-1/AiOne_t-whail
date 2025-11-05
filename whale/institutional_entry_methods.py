from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("institutional_entry_methods")
if not logger.handlers:  # guard
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class InstitutionalEntryMethods:
    """Детекція VWAP/TWAP та айсберг-ордерів (каркас) з детальним логуванням.

    Коментарі / призначення:
    - Клас надає прості евристики для виявлення стратегій виконання (VWAP/TWAP)
      та проксі-детекцію iceberg-ордерів по профілю обсягів.
    - Усі методи не мають побічних ефектів і повертають прості типи для подальшої
      інтеграції у Stage1/Stage2. Логи: INFO — ключові події, DEBUG — деталі.
    - TODO: винести пороги у config/config.py (не хардкодити), додати unit-тести
      (happy-path + edge-cases + псевдо-стріми), додати можливість асинхронної
      підписки на потоки даних.
    """

    def detect_vwap_twap_strategies(
        self, price_data: Iterable[float], volume_data: Iterable[float]
    ) -> dict[str, bool]:
        """Агрегація детекцій: повертає прапорці виявлених стратегій.

        Args:
            price_data: ітерабель цін (старі -> нові).
            volume_data: ітерабель відповідних обсягів.

        Returns:
            dict: {
                "vwap_buying": bool,       # сильне відхилення ціни нижче VWAP -> покупка інститутів
                "vwap_selling": bool,      # відхилення ціни вище VWAP -> інституційний продаж
                "twap_accumulation": bool, # послідовні buy-процеси як проксі TWAP
                "iceberg_orders": bool     # виявлено патерн айсберг
            }
        """
        # Підготовка вхідних даних для логування/валідації
        prices = list(price_data)
        vols = list(volume_data)
        logger.info(
            "detect_vwap_twap_strategies: запущено (n_prices=%d, n_vols=%d)",
            len(prices),
            len(vols),
        )

        # Захищена перевірка на відповідність довжин — якщо не відповідають, лог і спроба обчислень
        if not prices or not vols:
            logger.warning(
                "detect_vwap_twap_strategies: порожні вхідні дані — повертаємо дефолти"
            )
            return {
                "vwap_buying": False,
                "vwap_selling": False,
                "twap_accumulation": False,
                "iceberg_orders": False,
            }

        if len(prices) != len(vols):
            logger.debug(
                "detect_vwap_twap_strategies: len(prices)=%d len(vols)=%d — тримати лише мінімальну довжину",
                len(prices),
                len(vols),
            )
            # Trim to shortest to уникнути проблем при zip
            min_len = min(len(prices), len(vols))
            prices = prices[-min_len:]
            vols = vols[-min_len:]
            logger.debug("detect_vwap_twap_strategies: обрізано до довжини=%d", min_len)

        # Обчислення метрик з докладним логуванням
        vwap_dev = self.calculate_vwap_deviation(prices, vols)
        logger.debug("detect_vwap_twap_strategies: vwap_deviation=%s", repr(vwap_dev))

        twap = self.analyze_twap_execution(prices)
        logger.debug("detect_vwap_twap_strategies: twap_analysis=%s", twap)

        iceberg = self.detect_iceberg_orders(vols)
        logger.debug("detect_vwap_twap_strategies: iceberg_detected=%s", iceberg)

        # Порогові значення наразі хардкодяться тут як тимчасова міра.
        # TODO: винести пороги у config/config.py та додати feature-flag для агресивності
        # Виправлення напрямку: deviation=(last-vwap)/vwap; позитивне => ціна вище VWAP
        # Пороги дрібні для швидких сигналів (±0.4%)
        vwap_buying = vwap_dev is not None and vwap_dev > +0.004
        vwap_selling = vwap_dev is not None and vwap_dev < -0.004
        twap_acc = (
            bool(twap.get("consistent_buying")) if isinstance(twap, dict) else False
        )

        logger.info(
            "detect_vwap_twap_strategies: results vwap_buying=%s vwap_selling=%s twap_acc=%s iceberg=%s",
            vwap_buying,
            vwap_selling,
            twap_acc,
            iceberg,
        )

        return {
            "vwap_buying": vwap_buying,
            "vwap_selling": vwap_selling,
            "twap_accumulation": twap_acc,
            "iceberg_orders": iceberg,
            # Додаємо величину відхилення для телеметрії/вагування bias/presence
            "vwap_deviation": float(vwap_dev) if vwap_dev is not None else None,
        }

    # -------------------------
    # Заглушки розрахунків з детальним логуванням
    # -------------------------
    def calculate_vwap_deviation(
        self, price_data: Iterable[float], volume_data: Iterable[float]
    ) -> float | None:  # noqa: D401
        """Відхилення від VWAP (спрощено).

        Повертає відношення (last_price - vwap) / vwap або None якщо недостатньо даних.

        Пояснення:
        - VWAP = sum(price*vol) / sum(vol).
        - Від'ємне значення означає, що поточна ціна нижче VWAP (потенційна інституційна покупка).
        """
        prices = list(price_data)
        vols = list(volume_data)
        logger.debug(
            "calculate_vwap_deviation: початок обчислення (n_prices=%d n_vols=%d)",
            len(prices),
            len(vols),
        )

        if not prices or not vols:
            logger.debug("calculate_vwap_deviation: недостатньо даних -> None")
            return None

        if len(prices) != len(vols):
            logger.debug(
                "calculate_vwap_deviation: різні довжини, обрізаю до мінімальної довжини"
            )
            min_len = min(len(prices), len(vols))
            prices = prices[-min_len:]
            vols = vols[-min_len:]

        try:
            # Безпечне обчислення VWAP
            num = 0.0
            den = 0.0
            for p, v in zip(prices, vols, strict=False):
                num += float(p) * float(v)
                den += float(v)
            if den == 0.0:
                logger.debug("calculate_vwap_deviation: сума обсягів = 0 -> None")
                return None
            vwap = num / den
            last_price = float(prices[-1])
            deviation = (last_price - vwap) / vwap if vwap else None
            logger.debug(
                "calculate_vwap_deviation: vwap=%.6f last_price=%.6f deviation=%s",
                vwap,
                last_price,
                repr(deviation),
            )
            return float(deviation) if deviation is not None else None
        except Exception as exc:
            logger.exception("calculate_vwap_deviation: помилка обчислення: %s", exc)
            return None

    def analyze_twap_execution(
        self, price_data: Iterable[float]
    ) -> dict[str, Any]:  # noqa: D401
        """Проста перевірка послідовності змін цін як проксі TWAP.

        Логіка: якщо більшість цін у послідовності зростають — сигнал на послідовне накопичення (TWAP).
        """
        # Явно перетворюємо вхід у список чисел, ігноруючи невалідні значення
        prices: list[float] = []
        try:
            for i, p in enumerate(price_data):
                try:
                    # Допускаємо числа і рядки, що можна привести до float
                    prices.append(float(p))
                except Exception:
                    logger.debug(
                        "analyze_twap_execution: невалідна ціна у price_data[%d] -> %s",
                        i,
                        repr(p),
                    )
                    continue
        except Exception as exc:
            logger.exception(
                "analyze_twap_execution: price_data ітерація не вдалася: %s", exc
            )
            return {"consistent_buying": False, "notes": "error_iterating_input"}

        if len(prices) < 2:
            logger.debug("analyze_twap_execution: недостатньо даних для аналізу")
            return {"consistent_buying": False, "notes": "insufficient_data"}

        inc = 0
        for i in range(1, len(prices)):
            prev = prices[i - 1]
            cur = prices[i]
            if cur >= prev:
                inc += 1

        steps = len(prices) - 1
        consistent = inc >= max(1, steps // 2)
        logger.debug(
            "analyze_twap_execution: increases=%d steps=%d consistent=%s",
            inc,
            steps,
            consistent,
        )
        return {
            "consistent_buying": bool(consistent),
            "increases": inc,
            "steps": steps,
        }

    def detect_iceberg_orders(self, volume_data: Iterable[float]) -> bool:  # noqa: D401
        """
        Айсберг-проксі по кластерам обсягів.

        Евристика (тимчасова, TODO - калібрувати):
        - Рахуємо середній обсяг; якщо є >=3 сплески > 2x середнього -> сигнал.
        - TODO: додати перевірку видимості (orderbook visible size) та часові кластери.
        """
        vols = list(volume_data)
        logger.debug("detect_iceberg_orders: початок аналізу обсягів n=%d", len(vols))

        if not vols:
            logger.debug("detect_iceberg_orders: порожні обсяги -> False")
            return False

        try:
            avg = sum(float(v) for v in vols) / len(vols)
            spikes = sum(1 for v in vols if float(v) > 2.0 * avg)
            logger.debug("detect_iceberg_orders: avg=%.6f spikes=%d", avg, spikes)

            # Тимчасовий поріг — винести в конфіг
            result = spikes >= 3
            if result:
                logger.info(
                    "detect_iceberg_orders: потенційні iceberg-патерни знайдено spikes=%d avg=%.6f",
                    spikes,
                    avg,
                )
            return bool(result)
        except Exception as exc:
            logger.exception("detect_iceberg_orders: помилка під час аналізу: %s", exc)
            return False

    # ──────────────────────────────────────────────────────────────────────────────
    # Легкий експорт прапорців alt‑confirm з результатів IEM (без I/O)
    def alt_flags_from_iem(data: dict[str, Any] | None) -> dict[str, bool]:
        """Мапить результат detect_vwap_twap_strategies() у бінарні alt‑прапорці.

        Ключі:
            - iceberg: data["iceberg_orders"]
            - vol_spike: TWAP/VWAP активність як проксі підтвердження обсягом

        Примітка: це легка евристика для телеметрії, не виконує жодних запитів/обчислень.
        """
        d = data or {}
        iceberg = bool(d.get("iceberg_orders", False))
        vol_spike = bool(
            d.get("twap_accumulation", False)
            or d.get("vwap_buying", False)
            or d.get("vwap_selling", False)
        )
        return {"iceberg": iceberg, "vol_spike": vol_spike, "zones_dist": False}
