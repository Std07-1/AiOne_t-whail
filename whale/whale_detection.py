"""Whale Detection Engine: евристичні детектори активності великих гравців.

Телеметрійний модуль без побічних ефектів: не змінює Stage1/2/3 рекомендацій,
придатний для офлайн-аналізу та майбутньої інтеграції. Мінімізує залежності,
використовуючи лише стандартну бібліотеку.

Примітка: це початковий каркас. Реальні алгоритми потребують повноцінних
даних ордербуку/трейдів/ланцюгових переказів та калібрування порогів.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterable
from typing import Any

try:
    from config.config import (  # type: ignore
        ZONE_WINDOW_LONG,
        ZONE_WINDOW_SHORT,
        ZONE_WINDOWS_STRICT_ENABLED,
    )
except Exception:  # pragma: no cover - конфіг може бути відсутній у тестах
    ZONE_WINDOWS_STRICT_ENABLED = False  # type: ignore[assignment]
    ZONE_WINDOW_SHORT = 10  # type: ignore[assignment]
    ZONE_WINDOW_LONG = 10  # type: ignore[assignment]

try:
    from config.config import (  # type: ignore
        STAGE2_PRESENCE_PROMOTION_GUARDS,
        STRICT_ZONE_RISK_CLAMP_ENABLED,
    )
except Exception:  # pragma: no cover
    STAGE2_PRESENCE_PROMOTION_GUARDS = {}  # type: ignore[assignment]
    STRICT_ZONE_RISK_CLAMP_ENABLED = False

try:
    from config.config import PROM_GAUGES_ENABLED  # type: ignore
except Exception:  # pragma: no cover
    PROM_GAUGES_ENABLED = False  # type: ignore[assignment]

if PROM_GAUGES_ENABLED:
    try:
        from prometheus_client import Gauge  # type: ignore

        cold_storage_movement_gauge = Gauge(
            "cold_storage_movement", "Total cold storage movement in USD"
        )
    except ImportError:
        cold_storage_movement_gauge = None
else:
    cold_storage_movement_gauge = None

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("whale_detection")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class BlockTradeAnalyzer:
    """Спостереження за потоками між біржами/OTC (каркас).

    Докстрінг/пояснення:
    - Клас збирає та агрегує великі on-chain / off-chain перекази для виявлення
      інституційних потоків (exchange inflow/outflow, cold storage moves).
    - Поточна реалізація — заглушка з докладним логуванням для подальшої інтеграції.

    TODO:
    - інтегрувати з реальними джерелами (blockchain indexer, on-chain feeds, OTC APIs),
      бажано асинхронно з контролем паралелізму (semaphore).
    - кешувати результати в Redis через config/config.py (ключі по шаблону ai_one:...),
      встановлювати TTL з INTERVAL_TTL_MAP або REDIS_CACHE_TTL.
    - додати unit-тести: happy-path + edge-cases + псевдо-стріми on-chain подій.
    """

    def __init__(self) -> None:
        """Ініціалізація аналізатора потоків з базовими налаштуваннями та логуванням."""
        self.exchange_flows: dict[str, float] = {}
        # Не змінюємо контракт конструктора: джерело переказів можна
        # інжектнути через set_transfer_provider() для тестів/інтеграцій.
        self._transfer_provider: Any | None = None
        self.otc_desks_monitoring: list[str] = [
            "Genesis",
            "Circle",
            "Binance OTC",
            "Kraken OTC",
        ]

        logger.debug(
            "BlockTradeAnalyzer.__init__ ініціалізовано otc_desks=%s",
            ", ".join(self.otc_desks_monitoring),
        )
        logger.info("BlockTradeAnalyzer готовий до моніторингу інституційних потоків")

    def set_transfer_provider(self, provider: Any) -> None:
        """Задає провайдер переказів для monitor_blockchain_transfers.

        Очікується callable на кшталт: provider(threshold: float) -> Iterable[dict].
        Додатковий метод не ламає контракти — конструктор лишається без аргументів.
        """
        self._transfer_provider = provider

    def track_institutional_flows(self) -> dict[str, float]:
        """Повертає агреговані патерни потоків (заглушка з логуванням).

        Повертає:
            dict: {
                "exchange_inflow": float,
                "exchange_outflow": float,
                "cold_storage_movement": float
            }

        Пояснення:
        - inflow: сума переказів, що приходять на біржі (to_exchange=True).
        - outflow: сума переказів, що йдуть з бірж (from_exchange=True).
        - cold_storage_movement: сумарні рухи в/з cold wallets.
        """
        logger.info("track_institutional_flows: запуск агрегації потоків")
        logger.debug("Моніторинг OTC desks: %s", self.otc_desks_monitoring)

        # Джерело даних — заглушка; в реальності підключаємо індексер/пул стріму
        transfers = self.monitor_blockchain_transfers(threshold=1000.0)

        inflow = 0.0
        outflow = 0.0

        # Захищена агрегація — не допускаємо винятків через некоректні записи
        for i, t in enumerate(transfers):
            try:
                amt = float(t.get("amount", 0.0))
            except Exception:
                logger.debug("Невалідна сума у transfer[%d] — пропускаємо: %s", i, t)
                continue

            if t.get("to_exchange"):
                inflow += amt
                logger.debug("transfer[%d] -> to_exchange amount=%.2f", i, amt)
            if t.get("from_exchange"):
                outflow += amt
                logger.debug("transfer[%d] -> from_exchange amount=%.2f", i, amt)

        cold = self.analyze_cold_storage_moves(transfers)

        # Оновлюємо внутрішній стан (простий агрегат)
        self.exchange_flows["inflow"] = float(inflow)
        self.exchange_flows["outflow"] = float(outflow)
        logger.info(
            "track_institutional_flows: результат inflow=%.2f outflow=%.2f cold=%.2f",
            inflow,
            outflow,
            cold,
        )

        # TODO: записувати результат у Redis з ключем з config/config.py і TTL
        return {
            "exchange_inflow": float(inflow),
            "exchange_outflow": float(outflow),
            "cold_storage_movement": float(cold),
        }

    # Джерела даних — наразі емульовані / заглушки
    def monitor_blockchain_transfers(
        self, threshold: float = 1000.0
    ) -> list[dict[str, Any]]:  # noqa: D401
        """Повертає список великих переказів (емулюється порожнім списком).

        Args:
            threshold: мінімальна сума переказу для включення в список (у базовій валюті).

        Пояснення / TODO:
        - Тут має бути виклик до on-chain індексера або підписка на стрім великих транзакцій.
        - Рекомендується асинхронна імплементація з лімітом паралельних запитів і кешуванням.
        - Не зберігати сирі ключі/секрети в логах.
        """
        # Якщо задано провайдер — використовуємо його (для тестів/інтеграцій)
        if self._transfer_provider is not None:
            try:
                transfers = list(self._transfer_provider(threshold))
                logger.debug(
                    "monitor_blockchain_transfers: отримано %d запис(и) від провайдера",
                    len(transfers),
                )
                return transfers
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "monitor_blockchain_transfers: провайдер впав: %s — повертаємо []",
                    exc,
                )
                return []

        logger.debug(
            "monitor_blockchain_transfers: провайдер відсутній, повертаємо [] (threshold=%.2f)",
            threshold,
        )
        # TODO: інтегрувати з реальним провайдером (BlockCypher/Glassnode/Custom indexer)
        # TODO: нормалізація полів transfer: {amount, from_exchange, to_exchange, txid, timestamp, to_cold, from_cold}
        return []

    def analyze_cold_storage_moves(
        self, transfers: Iterable[dict[str, Any]]
    ) -> float:  # noqa: D401
        """Оцінка сумарних рухів у/з холодних гаманців з детальним логуванням та обробкою.

        Логіка:
        - Ітеруємо по всіх переданих transfers, перевіряючи наявність прапорів to_cold та from_cold.
        - Для кожного релевантного transfer валідуємо та конвертуємо amount у float, ігноруючи невалідні.
        - Сумуємо абсолютні значення amount для рухів до/з cold storage.
        - Логуємо кожну оброблену транзакцію на DEBUG рівні для прозорості.
        - Повертаємо загальну суму як індикатор активності cold storage.

        Обробка помилок:
        - Якщо transfer не містить amount або воно не конвертується у float — пропускаємо з логуванням.
        - Якщо transfers порожній або None — повертаємо 0.0 з відповідним логуванням.
        """
        if not transfers:
            logger.info("COLD_STORAGE: transfers порожній або None — повертаємо 0.0")
            return 0.0

        total = 0.0
        processed_count = 0
        skipped_count = 0
        seen_txids: set[str] = set()

        for i, t in enumerate(transfers):
            if not isinstance(t, dict):
                logger.debug("COLD_STORAGE: transfer[%d] не є dict — пропускаємо", i)
                skipped_count += 1
                continue

            is_cold_in = bool(t.get("to_cold", False))
            is_cold_out = bool(t.get("from_cold", False))

            if not (is_cold_in or is_cold_out):
                # Не cold-related — пропускаємо без логування для уникнення шуму
                continue

            try:
                amt_raw = t.get("amount")
                if amt_raw is None:
                    logger.debug(
                        "COLD_STORAGE: transfer[%d] відсутнє amount — пропускаємо", i
                    )
                    skipped_count += 1
                    continue
                amt = abs(float(amt_raw))
                if not math.isfinite(amt) or amt <= 0:
                    logger.debug(
                        "COLD_STORAGE: transfer[%d] невалідне amount=%.2f — пропускаємо",
                        i,
                        amt,
                    )
                    skipped_count += 1
                    continue
            except (ValueError, TypeError) as exc:
                logger.debug(
                    "COLD_STORAGE: transfer[%d] помилка конвертації amount — пропускаємо: %s",
                    i,
                    exc,
                )
                skipped_count += 1
                continue

            # Перевірка на дублювання транзакцій за txid для уникнення подвійного підрахунку
            txid = t.get("txid")
            if txid is not None:
                if txid in seen_txids:
                    logger.debug(
                        "COLD_STORAGE: transfer[%d] дубль за txid=%s — пропускаємо",
                        i,
                        txid,
                    )
                    skipped_count += 1
                    continue
                seen_txids.add(txid)
            total += amt
            processed_count += 1
            logger.debug(
                "COLD_STORAGE: оброблено transfer[%d] cold_in=%s cold_out=%s amount=%.2f (накопичено total=%.2f)",
                i,
                is_cold_in,
                is_cold_out,
                amt,
                total,
            )

        logger.info(
            "COLD_STORAGE: завершено обробку %d transfers (оброблено %d, пропущено %d), total_cold_movement=%.2f",
            len(
                list(transfers)
            ),  # Використовуємо len для підрахунку, оскільки Iterable може бути генератором
            processed_count,
            skipped_count,
            total,
        )

        # TODO: додати сигнал/метри якщо рухи cold wallet перевищують поріг (alert), наприклад, через Prometheus або Redis
        threshold = 10000.0  # TODO: винести в config
        if total > threshold:
            logger.warning(
                "COLD_STORAGE: cold storage movement %.2f exceeds threshold %.2f",
                total,
                threshold,
            )
        if cold_storage_movement_gauge is not None:
            cold_storage_movement_gauge.set(total)

        return float(total)


class SupplyDemandZones:
    """Визначення зон попиту/пропозиції (каркас) з детальним логуванням.

    Призначення:
    - Ідентифікує ключові зони (accumulation / distribution / stop-hunt / liquidity).
    - Повертає структуру з ключовими рівнями, ймовірними таргетами та якістю зон.
    - Немає побічних ефектів — заглушка, готова до інтеграції з реальними алгоритмами.

        logger.debug(
            "find_accumulation_zones: n_prices=%d n_vols=%d", len(prices), len(vols)
        )
    - WARNING — відсутні або некоректні вхідні дані.

    TODO:
    - Винести пороги/константи в config/config.py.
        # Sliding-window heuristic: низька відносна волатильність + не нижчий за медіану обсяг
        try:
            # Вікна під прапором/конфігом (fallback на 10)
            window = int(ZONE_WINDOW_SHORT or 10)
            if ZONE_WINDOWS_STRICT_ENABLED:
                try:
                    window = max(window, int(ZONE_WINDOW_LONG or window))
                except Exception:
                    pass
            window = max(3, window)
            stride = max(1, window // 2)

            def _mean(x: list[float]) -> float:
                return sum(x) / len(x) if x else 0.0

            zones_raw: list[tuple[float, float]] = []
            median_vol = (
                sorted(vols)[len(vols) // 2] if vols else 0.0
            )
            for i in range(0, max(1, len(prices) - window + 1), stride):
                segment = prices[i : i + window]
                seg_vol = sum(vols[i : i + window]) if vols else 0.0
                if len(segment) < 3:
                    continue
                seg_mean = _mean(segment)
                # std без numpy
                var = sum((p - seg_mean) ** 2 for p in segment) / (len(segment) - 1)
                std = math.sqrt(var)
                cv = (std / seg_mean) if seg_mean > 0 else 0.0
                # Пороги: відносна мінливість < 0.3% та обсяг >= медіани
                if cv <= 0.003 and seg_vol >= max(1.0, median_vol):
                    low, high = float(min(segment)), float(max(segment))
                    zones_raw.append((low, high))

            # Злиття перекривних зон
            zones: list[tuple[float, float]] = []
            for lo, hi in sorted(zones_raw):
                if not zones:
                    zones.append((lo, hi))
                    continue
                plo, phi = zones[-1]
                if lo <= phi * 1.001:  # невелике перекриття/дотик
                    zones[-1] = (min(plo, lo), max(phi, hi))
                else:
                    zones.append((lo, hi))

            logger.info("find_accumulation_zones: знайдено %d зон", len(zones))
            return zones
                    "prices": list[float],
                    "volumes": list[float],
                    "time": list[int|float],  # epoch
                    ...
                }

        Returns:
            dict з полями:
                - key_levels: dict (accumulation_zones, distribution_zones, stop_hunt_levels, liquidity_pools)
                - key_levels_enriched: dict як key_levels, але збагачений volume для зон (dict із ключами {low, high, volume})
                - probable_targets: list[float]
                - risk_levels: dict з оцінками якості зон (0..1)
        """
    def identify_institutional_levels(
        self, historical_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Агрегує ключові зони та оцінює таргети/ризики.

        Args:
            historical_data: очікується словник з мінімальними полями, напр.:
                {
                    "prices": list[float],
                    "volumes": list[float],
                    "time": list[int|float],  # epoch
                    ...
                }

        Returns:
            dict з полями:
                - key_levels: dict (accumulation_zones, distribution_zones, stop_hunt_levels, liquidity_pools)
                - key_levels_enriched: dict як key_levels, але збагачений volume для зон (dict із ключами {low, high, volume})
                - probable_targets: list[float]
                - risk_levels: dict з оцінками якості зон (0..1)
        """
        logger.info("identify_institutional_levels: запуск ідентифікації зон")
        if not isinstance(historical_data, dict) or not historical_data:
            logger.warning(
                "identify_institutional_levels: порожні/неправильні historical_data -> повертаємо пусті структури"
            )
            return {"key_levels": {}, "probable_targets": [], "risk_levels": {}}

        try:
            # Базові (мінімально необхідні) ключі рівнів
            key_levels = {
                "accumulation_zones": self.find_accumulation_zones(historical_data),
                "distribution_zones": self.find_distribution_zones(historical_data),
                "stop_hunt_levels": self.identify_stop_hunt_zones(historical_data),
                "liquidity_pools": self.map_liquidity_pools(historical_data),
            }
            # logger.debug(
            #     "identify_institutional_levels: знайдено key_levels=%s", key_levels
            # )

            # Додатково: збагачення зон приблизним обсягом у межах low..high
            try:
                prices = list(historical_data.get("prices") or [])
                volumes = list(historical_data.get("volumes") or [])
                key_levels_enriched = {
                    "accumulation_zones": self._enrich_zones_with_volume(
                        prices, volumes, key_levels.get("accumulation_zones") or []
                    ),
                    "distribution_zones": self._enrich_zones_with_volume(
                        prices, volumes, key_levels.get("distribution_zones") or []
                    ),
                    # Рівні без low/high залишаємо як є
                    "stop_hunt_levels": key_levels.get("stop_hunt_levels") or [],
                    "liquidity_pools": key_levels.get("liquidity_pools") or [],
                }
            except Exception:
                key_levels_enriched = {
                    "accumulation_zones": [],
                    "distribution_zones": [],
                    "stop_hunt_levels": key_levels.get("stop_hunt_levels") or [],
                    "liquidity_pools": key_levels.get("liquidity_pools") or [],
                }

            targets = self.calculate_institutional_targets(key_levels)
            risk_levels = self.assess_zone_quality(key_levels)

            logger.info(
                "identify_institutional_levels: результати zones(accum=%d dist=%d liquidity=%d stop_hunt=%d) targets=%d",
                len(key_levels.get("accumulation_zones", [])),
                len(key_levels.get("distribution_zones", [])),
                len(key_levels.get("liquidity_pools", [])),
                len(key_levels.get("stop_hunt_levels", [])),  # noqa: E501
                len(targets),
            )

            return {
                "key_levels": key_levels,
                "key_levels_enriched": key_levels_enriched,
                "probable_targets": targets,
                "risk_levels": risk_levels,
            }
        except Exception as exc:
            logger.exception(
                "identify_institutional_levels: помилка під час обробки: %s", exc
            )
            return {
                "key_levels": {},
                "key_levels_enriched": {},
                "probable_targets": [],
                "risk_levels": {},
            }

    # -------------------------
    # Заглушки з логуванням та поясненнями
    # -------------------------
    def find_accumulation_zones(
        self, historical_data: dict[str, Any]
    ) -> list[tuple[float, float]]:  # noqa: D401
        """Пошук зон накопичення (повертає список пар [low, high]).

        Поточна реалізація — простий heuristic:
        - якщо є price-series, шукаємо ділянки з відносно плоским рухом та підвищеним обсягом.
        - TODO: Реалізувати справжній алгоритм ідентифікації зон накопичення на основі price-volume clustering, time-decay та перевірки видимості ордербуку. Поточна реалізація — простий heuristic з фіксованими порогами, який потрібно замінити на кластеризацію та калібрування порогів через config/config.py.
        """
        prices = list(historical_data.get("prices") or [])
        vols = list(historical_data.get("volumes") or [])

        logger.debug(
            "find_accumulation_zones: n_prices=%d n_vols=%d", len(prices), len(vols)
        )
        if not prices:
            logger.debug("find_accumulation_zones: немає цін -> []")
            return []

        try:
            # Базова довжина вікна з конфігу (з fallback)
            try:
                window = int(ZONE_WINDOW_SHORT or 10)
            except Exception:
                window = 10
            if ZONE_WINDOWS_STRICT_ENABLED:
                try:
                    window = max(window, int(ZONE_WINDOW_LONG or window))
                except Exception:
                    pass
            window = max(3, window)
            stride = max(1, window // 2)

            def _mean(x: list[float]) -> float:
                return sum(x) / len(x) if x else 0.0

            median_vol = sorted(vols)[len(vols) // 2] if vols else 0.0
            zones_raw: list[tuple[float, float]] = []
            for i in range(0, max(1, len(prices) - window + 1), stride):
                segment = prices[i : i + window]
                if len(segment) < 3:
                    continue
                seg_vol = sum(vols[i : i + window]) if vols else 0.0
                seg_mean = _mean(segment)
                var = sum((p - seg_mean) ** 2 for p in segment) / (len(segment) - 1)
                std = math.sqrt(var)
                cv = (std / seg_mean) if seg_mean > 0 else 0.0
                if cv <= 0.003 and seg_vol >= max(1.0, median_vol):
                    low, high = float(min(segment)), float(max(segment))
                    zones_raw.append((low, high))

            # Злиття перекривних/дотичних зон
            zones: list[tuple[float, float]] = []
            for lo, hi in sorted(zones_raw):
                if not zones:
                    zones.append((lo, hi))
                    continue
                plo, phi = zones[-1]
                if lo <= phi * 1.001:
                    zones[-1] = (min(plo, lo), max(phi, hi))
                else:
                    zones.append((lo, hi))
            logger.info("find_accumulation_zones: знайдено %d зон", len(zones))
            return zones
        except Exception as exc:  # pragma: no cover
            logger.exception("find_accumulation_zones: помилка під час пошуку: %s", exc)
            return []

        try:
            try:
                window = int(ZONE_WINDOW_SHORT or 10)
            except Exception:
                window = 10
            window = max(3, window)
            stride = max(1, window // 2)
            zones_raw: list[tuple[float, float]] = []
            avg_vol = (sum(vols) / len(vols)) if vols else 0.0
            for i in range(0, max(1, len(prices) - window + 1), stride):
                segment = prices[i : i + window]
                if len(segment) < 3:
                    continue
                seg_vol = sum(vols[i : i + window]) if vols else 0.0
                seg_mean = sum(segment) / len(segment)
                price_range = max(segment) - min(segment)
                rel_amp = (price_range / seg_mean) if seg_mean > 0 else 0.0
                if rel_amp >= 0.008 and seg_vol >= avg_vol:
                    low, high = float(min(segment)), float(max(segment))
                    zones_raw.append((low, high))

            zones: list[tuple[float, float]] = []
            for lo, hi in sorted(zones_raw):
                if not zones:
                    zones.append((lo, hi))
                    continue
                plo, phi = zones[-1]
                if lo <= phi * 1.001:
                    zones[-1] = (min(plo, lo), max(phi, hi))
                else:
                    zones.append((lo, hi))
            logger.info("find_distribution_zones: знайдено %d зон", len(zones))
            return zones
        except Exception as exc:  # pragma: no cover
            logger.exception("find_distribution_zones: помилка під час пошуку: %s", exc)
            return []

    def identify_stop_hunt_zones(
        self, historical_data: dict[str, Any]
    ) -> list[float]:  # noqa: D401
        """Ідентифікує орієнтовні рівні stop-hunt (повертає список рівнів цін).

        Проксі: часто stop-hunt відбувається біля локальних мінімумів/максимумів з кластерами маленьких стопів.
        - TODO: Реалізувати справжній алгоритм ідентифікації stop-hunt зон на основі аналізу ордербуку, кластерів стоп-ордерів та on-chain даних. Поточна реалізація — простий пошук локальних екстремумів, який потрібно замінити на більш складну логіку з калібруванням порогів через config/config.py.
        """
        prices = list(historical_data.get("prices") or [])
        logger.debug("identify_stop_hunt_zones: n_prices=%d", len(prices))
        if not prices:
            return []
        try:
            # Локальні екстремуми + простий фільтр «помітності» (prominence)
            candidates: list[float] = []
            win = max(3, min(20, (len(prices) // 10) or 3))
            half = max(1, win // 2)
            for i in range(1, len(prices) - 1):
                prev_p = float(prices[i - 1])
                cur_p = float(prices[i])
                next_p = float(prices[i + 1])
                is_ext = (cur_p < prev_p and cur_p < next_p) or (
                    cur_p > prev_p and cur_p > next_p
                )
                if not is_ext:
                    continue
                left_idx = max(0, i - half)
                right_idx = min(len(prices), i + half + 1)
                neighborhood = prices[left_idx:right_idx]
                nb_mean = sum(neighborhood) / len(neighborhood)
                nb_dev = abs(cur_p - nb_mean) / nb_mean if nb_mean > 0 else 0.0
                if nb_dev >= 0.005:  # >=0.5% відхилення від локального середнього
                    candidates.append(cur_p)
            unique = sorted(set(candidates))
            logger.info(
                "identify_stop_hunt_zones: знайдено %d candidate levels", len(unique)
            )
            return [float(x) for x in unique]
        except Exception as exc:
            logger.exception("identify_stop_hunt_zones: помилка: %s", exc)
            return []

    @staticmethod
    def _enrich_zones_with_volume(
        prices: list[float],
        volumes: list[float],
        zones: list[tuple[float, float]] | list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Повертає список зон як dict із полями {low, high, volume}.

        Проста оцінка: сума обсягів для барів, де ціна в межах [low, high].
        Якщо вхід вже dict із volume — зберігаємо існуючий обсяг.
        """
        enriched: list[dict[str, Any]] = []
        if not prices or not volumes or len(prices) != len(volumes):
            # Якщо немає даних — перетворюємо на dict без volume
            for z in zones:
                if isinstance(z, dict) and "low" in z and "high" in z:
                    low = float(z.get("low"))
                    high = float(z.get("high"))
                    vol = float(z.get("volume", 0.0))
                elif isinstance(z, tuple) and len(z) == 2:
                    low, high = float(z[0]), float(z[1])
                    vol = 0.0
                else:
                    continue
                enriched.append({"low": low, "high": high, "volume": vol})
            return enriched

        for z in zones:
            low: float
            high: float
            pre_vol: float | None = None
            try:
                if isinstance(z, dict) and "low" in z and "high" in z:
                    low = float(z.get("low"))
                    high = float(z.get("high"))
                    pre_vol = float(z.get("volume", 0.0))
                elif isinstance(z, tuple) and len(z) == 2:
                    low, high = float(z[0]), float(z[1])
                else:
                    continue
            except Exception:
                continue
            if low > high:
                low, high = high, low
            if pre_vol is not None and pre_vol > 0:
                vol_sum = pre_vol
            else:
                vol_sum = 0.0
                try:
                    for p, v in zip(prices, volumes, strict=True):
                        if low <= float(p) <= high:
                            vol_sum += float(v)
                except Exception:
                    pass
            enriched.append({"low": low, "high": high, "volume": float(vol_sum)})
        return enriched

    def map_liquidity_pools(
        self, historical_data: dict[str, Any]
    ) -> list[float]:  # noqa: D401
        """Мапування рівнів підвищеної ліквідності (порожній список — заглушка).

        Потенційна реалізація:
        - використовувати orderbook snapshots для визначення великих накопичених видимих лімітів,
        - комбінувати з on-chain inflow/outflow.
        - TODO: Реалізувати справжній алгоритм ідентифікації liquidity pools на основі аналізу ордербуку, накопичених лімітів, on-chain даних та калібрування порогів через config/config.py. Поточна реалізація — простий фільтр за розміром ордерів з фіксованим порогом.
        """
        logger.debug("map_liquidity_pools: початок аналізу liquidity pools")
        orderbook_levels = historical_data.get("orderbook_levels")
        if not orderbook_levels:
            logger.debug("map_liquidity_pools: немає orderbook_levels -> []")
            return []

        try:
            pools: list[float] = []
            # Очікуємо структуру: list[{"price": float, "size": float}]
            for i, lvl in enumerate(orderbook_levels):
                try:
                    price = float(lvl.get("price"))
                    size = float(lvl.get("size", 0.0))
                except Exception:
                    logger.debug(
                        "map_liquidity_pools: невалідний рівень orderbook_levels[%d] -> %s",
                        i,
                        lvl,
                    )
                    continue
                # Тимчасовий поріг для виявлення пулу ліквідності
                if size >= 10.0:  # TODO: винести поріг в config
                    pools.append(price)
                    logger.debug(
                        "map_liquidity_pools: pool detected price=%.6f size=%.2f",
                        price,
                        size,
                    )
            logger.info("map_liquidity_pools: знайдено %d liquidity pools", len(pools))
            return pools
        except Exception as exc:
            logger.exception("map_liquidity_pools: помилка при мапінгу: %s", exc)
            return []

    def calculate_institutional_targets(
        self, key_levels: dict[str, Any]
    ) -> list[float]:  # noqa: D401
        """Генерує орієнтовні price-targets на основі ключових рівнів.

        Простий алгоритм:
        - Для кожної accumulation-zone пропонуємо ціль вищу на +2% і +5%.
        - Для distribution-zone — цілі нижче на -2% і -5%.
        - TODO: замінити на модель з урахуванням spread/depth і probability weighting.
        """
        logger.debug(
            "calculate_institutional_targets: key_levels keys=%s",
            list(key_levels.keys()),
        )
        targets: list[float] = []
        try:
            accum = key_levels.get("accumulation_zones") or []
            dist = key_levels.get("distribution_zones") or []

            for low, high in accum:
                base = (low + high) / 2.0
                targets.append(round(base * 1.02, 8))
                targets.append(round(base * 1.05, 8))
                # logger.debug(
                #     "calculate_institutional_targets: accum base=%.6f -> targets=%.6f, %.6f",
                #     base,
                #     base * 1.02,
                #     base * 1.05,
                # )

            for low, high in dist:
                base = (low + high) / 2.0
                targets.append(round(base * 0.98, 8))
                targets.append(round(base * 0.95, 8))
                # logger.debug(
                #     "calculate_institutional_targets: dist base=%.6f -> targets=%.6f, %.6f",
                #     base,
                #     base * 0.98,
                #     base * 0.95,
                # )

            # Унікалізуємо і сортуємо результати для консистентності
            unique_sorted = sorted(set(targets))
            logger.info(
                "calculate_institutional_targets: згенеровано %d targets",
                len(unique_sorted),
            )
            return [float(x) for x in unique_sorted]
        except Exception as exc:
            logger.exception(
                "calculate_institutional_targets: помилка при обчисленні targets: %s",
                exc,
            )
            return []

    def assess_zone_quality(
        self, key_levels: dict[str, Any]
    ) -> dict[str, float]:  # noqa: D401
        """Оцінює якість зон (0..1).

        Простий scoring:
        - accumulation: більше зон та наявність liquidity_pools підвищує score.
        - distribution: велика price_range і обсяги підвищують score.
        - TODO: додати time-decay, on-chain correlation, та backtest-based scoring.
        """
        logger.debug(
            "assess_zone_quality: key_levels=%s",
            {k: len(v) if isinstance(v, list) else None for k, v in key_levels.items()},
        )
        try:
            accum = key_levels.get("accumulation_zones") or []
            dist = key_levels.get("distribution_zones") or []
            pools = key_levels.get("liquidity_pools") or []

            accum_score = min(1.0, (len(accum) * 0.2) + (len(pools) * 0.1))
            dist_score = min(1.0, (len(dist) * 0.25))
            # Сумарна оцінка ризику (нижче -> краща якість для входу)
            risk_score = max(0.0, 1.0 - accum_score)
            if STRICT_ZONE_RISK_CLAMP_ENABLED:
                dist_count = len(dist) if isinstance(dist, list) else 0
                pools_count = len(pools) if isinstance(pools, list) else 0
                if dist_count == 0 and pools_count == 0:
                    try:
                        risk_cap = float(
                            (STAGE2_PRESENCE_PROMOTION_GUARDS or {}).get(
                                "cap_without_confirm", 0.20
                            )
                            or 0.20
                        )
                    except Exception:
                        risk_cap = 0.20
                    risk_cap = max(0.0, min(1.0, risk_cap))
                    new_risk = max(0.0, min(risk_score, risk_cap))
                    if new_risk != risk_score:
                        logger.debug(
                            "assess_zone_quality: risk clamp %.3f → %.3f",
                            risk_score,
                            new_risk,
                        )
                    risk_score = new_risk

            result = {
                "accumulation": round(float(accum_score), 3),
                "distribution": round(float(dist_score), 3),
                "risk_from_zones": round(float(risk_score), 3),
            }
            logger.info("assess_zone_quality: scores=%s", result)
            return result
        except Exception as exc:
            logger.exception(
                "assess_zone_quality: помилка при оцінці якості зон: %s", exc
            )
            return {"accumulation": 0.0, "distribution": 0.0, "risk_from_zones": 1.0}
