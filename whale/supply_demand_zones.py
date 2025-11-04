from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

try:
    from config.config import (  # type: ignore
        STAGE2_PRESENCE_PROMOTION_GUARDS,
        STAGE3_ZONES_THRESHOLD_OVERRIDES,
        STRICT_ZONE_RISK_CLAMP_ENABLED,
        ZONE_WINDOW_LONG,
        ZONE_WINDOW_SHORT,
        ZONE_WINDOWS_STRICT_ENABLED,
    )
except Exception:  # pragma: no cover - конфіг може бути відсутній у тестах
    STAGE2_PRESENCE_PROMOTION_GUARDS = {}  # type: ignore[assignment]
    STAGE3_ZONES_THRESHOLD_OVERRIDES = {}  # type: ignore[assignment]
    STRICT_ZONE_RISK_CLAMP_ENABLED = False
    ZONE_WINDOWS_STRICT_ENABLED = False  # type: ignore[assignment]
    ZONE_WINDOW_SHORT = 10  # type: ignore[assignment]
    ZONE_WINDOW_LONG = 10  # type: ignore[assignment]

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("supply_demand_zones")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class SupplyDemandZones:
    """Визначення зон попиту/пропозиції (каркас) з детальним логуванням.

    Призначення:
    - Ідентифікує ключові зони (accumulation / distribution / stop-hunt / liquidity).
    - Повертає структуру з ключовими рівнями, ймовірними таргетами та якістю зон.
    - Немає побічних ефектів — заглушка, готова до інтеграції з реальними алгоритмами.

    Logging:
    - INFO — початок/кінець основних операцій, ключові результати.
    - DEBUG — внутрішні розрахунки, розміри масивів, тимчасові значення.
    - WARNING — відсутні або некоректні вхідні дані.

    TODO:
    - Винести пороги/константи в config/config.py.
    - Додати Redis-кешування результатів з правильним ключем ai_one:levels:{symbol}:{granularity} та TTL.
    - Реалізувати clustering (price-volume), time-decay та перевірку видимості ордербуку.
    - Додати unit-тести: happy-path + edge-cases + псевдо-стріми історичних даних.
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

        symbol = str(historical_data.get("symbol") or "").upper()
        overrides = STAGE3_ZONES_THRESHOLD_OVERRIDES.get(symbol, {}) if symbol else {}

        local_payload = dict(historical_data)
        local_payload["symbol"] = symbol
        if overrides:
            local_payload["_zone_overrides"] = overrides
        local_payload["_orderbook_available"] = bool(
            historical_data.get("orderbook_levels")
        )

        try:
            # Базові (мінімально необхідні) ключі рівнів
            key_levels = {
                "accumulation_zones": self.find_accumulation_zones(local_payload),
                "distribution_zones": self.find_distribution_zones(local_payload),
                "stop_hunt_levels": self.identify_stop_hunt_zones(local_payload),
                "liquidity_pools": self.map_liquidity_pools(local_payload),
            }
            # logger.debug(
            #     "identify_institutional_levels: знайдено key_levels=%s", key_levels
            # )

            # Додатково: збагачення зон приблизним обсягом у межах low..high
            try:
                prices = list(local_payload.get("prices") or [])
                volumes = list(local_payload.get("volumes") or [])
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
            accum_diag = (
                local_payload.get("_accum_diag")
                if isinstance(local_payload.get("_accum_diag"), Mapping)
                else {}
            )

            logger.info(
                "identify_institutional_levels: результати zones(accum=%d dist=%d liquidity=%d stop_hunt=%d) targets=%d",
                len(key_levels.get("accumulation_zones", [])),
                len(key_levels.get("distribution_zones", [])),
                len(key_levels.get("liquidity_pools", [])),
                len(key_levels.get("stop_hunt_levels", [])),
                len(targets),
            )

            return {
                "key_levels": key_levels,
                "key_levels_enriched": key_levels_enriched,
                "probable_targets": targets,
                "risk_levels": risk_levels,
                "meta": {
                    "symbol": symbol,
                    "orderbook_available": bool(
                        local_payload.get("_orderbook_available", False)
                    ),
                    "accum_thresholds": dict(accum_diag),
                },
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
        - TODO: замінити на кластеризацію price-volume та time-decay.
        """
        prices = list(historical_data.get("prices") or [])
        vols = list(historical_data.get("volumes") or [])
        symbol = str(historical_data.get("symbol") or "").upper()
        overrides_raw = historical_data.get("_zone_overrides")
        overrides: Mapping[str, Any] | None
        if isinstance(overrides_raw, Mapping):
            overrides = overrides_raw
        else:
            overrides = STAGE3_ZONES_THRESHOLD_OVERRIDES.get(symbol, {})

        logger.debug(
            "find_accumulation_zones: n_prices=%d n_vols=%d", len(prices), len(vols)
        )
        if not prices:
            logger.debug("find_accumulation_zones: немає цін -> []")
            return []

        # Простий heuristic: брати сусідні вікна, де variance цін мала, але сумарний обсяг вищий за медіану
        try:
            window_cfg = overrides.get("accum_window") if overrides else None
            try:
                window = max(2, int(window_cfg)) if window_cfg is not None else 10
            except Exception:
                window = 10
            # Strict override via config to avoid "flat" whales
            try:
                if bool(ZONE_WINDOWS_STRICT_ENABLED):
                    window = max(2, int(ZONE_WINDOW_SHORT))
            except Exception:
                pass
            zones: list[tuple[float, float]] = []
            median_vol = sorted(vols)[len(vols) // 2] if vols else 0.0
            range_frac_cfg = (
                overrides.get("accum_range_frac_max") if overrides else None
            )
            try:
                range_frac_max = (
                    float(range_frac_cfg) if range_frac_cfg is not None else 0.005
                )
            except Exception:
                range_frac_max = 0.005
            median_gate = max(1.0, median_vol)
            if overrides:
                median_override = overrides.get("accum_median_vol_min")
                if isinstance(median_override, (int, float)):
                    median_gate = float(median_override)
                elif isinstance(median_override, str):
                    hint = median_override.strip().lower().replace("×", "x")
                    if hint.startswith("auto"):
                        factor_txt = hint.split("x", 1)[-1]
                        try:
                            factor = float(factor_txt)
                            median_gate = max(median_gate, median_vol * factor)
                        except Exception:
                            pass
            try:
                logger.info(
                    "[ZONES_THRESH] accum symbol=%s window=%d range_frac<=%.4f median_gate>=%.3f median_vol=%.3f",
                    symbol or "unknown",
                    window,
                    range_frac_max,
                    median_gate,
                    median_vol,
                )
            except Exception:
                pass
            for i in range(0, max(1, len(prices) - window + 1), window):
                segment = prices[i : i + window]
                seg_vol = sum(vols[i : i + window]) if vols else 0.0
                if not segment:
                    continue
                price_range = max(segment) - min(segment)
                avg_price = sum(segment) / len(segment)
                if avg_price <= 0:
                    continue
                range_frac = price_range / avg_price
                if range_frac <= range_frac_max and seg_vol >= median_gate:
                    low, high = float(min(segment)), float(max(segment))
                    zones.append((low, high))
                    # logger.debug(
                    #     "find_accumulation_zones: виявлено зона low=%.6f high=%.6f vol=%.2f",
                    #     low,
                    #     high,
                    #     seg_vol,
                    # )
            logger.info("find_accumulation_zones: знайдено %d зон", len(zones))
            try:
                historical_data["_accum_diag"] = {
                    "window": window,
                    "range_frac_max": range_frac_max,
                    "median_gate": median_gate,
                    "median_vol": median_vol,
                }
            except Exception:
                pass
            return zones
        except Exception as exc:
            logger.exception("find_accumulation_zones: помилка під час пошуку: %s", exc)
            return []

    def find_distribution_zones(
        self, historical_data: dict[str, Any]
    ) -> list[tuple[float, float]]:  # noqa: D401
        """Пошук зон розподілу (повертає список пар [low, high]).

        Проксі: вимальовуються як ділянки з високою волатильністю та високими обсягами.
        """
        prices = list(historical_data.get("prices") or [])
        vols = list(historical_data.get("volumes") or [])

        logger.debug(
            "find_distribution_zones: n_prices=%d n_vols=%d", len(prices), len(vols)
        )
        if not prices:
            return []

        try:
            # Базовий розмір вікна
            window = 10  # TODO: винести у config
            # Strict override via config
            try:
                if bool(ZONE_WINDOWS_STRICT_ENABLED):
                    window = max(2, int(ZONE_WINDOW_LONG))
            except Exception:
                pass
            zones: list[tuple[float, float]] = []
            avg_vol = (sum(vols) / len(vols)) if vols else 0.0
            try:
                logger.info(
                    "[ZONES_THRESH] dist window=%d range_frac>%.4f avg_vol>=%.3f",
                    window,
                    0.010,
                    avg_vol,
                )
            except Exception:
                pass
            for i in range(0, max(1, len(prices) - window + 1), window):
                segment = prices[i : i + window]
                seg_vol = sum(vols[i : i + window]) if vols else 0.0
                if not segment:
                    continue
                price_range = max(segment) - min(segment)
                # Тимчасова логіка: велика амплітуда і обсяг -> distribution zone
                if (
                    price_range > (sum(segment) / len(segment)) * 0.01
                    and seg_vol >= avg_vol
                ):
                    low, high = float(min(segment)), float(max(segment))
                    zones.append((low, high))
                    # logger.debug(
                    #     "find_distribution_zones: виявлено зона low=%.6f high=%.6f vol=%.2f",
                    #     low,
                    #     high,
                    #     seg_vol,
                    # )
            logger.info("find_distribution_zones: знайдено %d зон", len(zones))
            return zones
        except Exception as exc:
            logger.exception("find_distribution_zones: помилка під час пошуку: %s", exc)
            return []

    def identify_stop_hunt_zones(
        self, historical_data: dict[str, Any]
    ) -> list[float]:  # noqa: D401
        """Ідентифікує орієнтовні рівні stop-hunt (повертає список рівнів цін).

        Проксі: часто stop-hunt відбувається біля локальних мінімумів/максимумів з кластерами маленьких стопів.
        """
        prices = list(historical_data.get("prices") or [])
        logger.debug("identify_stop_hunt_zones: n_prices=%d", len(prices))
        if not prices:
            return []
        try:
            # Простий підхід: локальні екстремуми як кандидати
            candidates: list[float] = []
            for i in range(1, len(prices) - 1):
                prev_p = float(prices[i - 1])
                cur_p = float(prices[i])
                next_p = float(prices[i + 1])
                # локальний мінімум або максимум
                if (cur_p < prev_p and cur_p < next_p) or (
                    cur_p > prev_p and cur_p > next_p
                ):
                    candidates.append(cur_p)
                    # logger.debug(
                    #    "identify_stop_hunt_zones: candidate at index=%d price=%.6f",
                    #    i,
                    #    cur_p,
                    # )
            # Зробимо унікальні і відсортуємо
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
