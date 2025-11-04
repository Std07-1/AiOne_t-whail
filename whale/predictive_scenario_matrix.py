from __future__ import annotations

import logging
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("predictive_scenario_matrix")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class PredictiveScenarioMatrix:
    """Генератор сценаріїв на базі whale-сигналів (каркас).

    Документація:
    - generate_whale_scenarios: формує базовий набір сценаріїв і делегує корекцію ймовірностей.
    - adjust_probabilities: застосовує прості евристики корекції на основі whale_signals
      та market_data (поки локально). У майбутньому — винести пороги/ваги в config та
      кешувати результати в Redis з TTL.
    TODO:
    - Винести константи (thresholds, biases) у config/config.py.
    - Додати unit-тести: happy-path + edge-cases + псевдо-стріми.
    - Зробити асинхронну версію з обмеженням паралелізму, якщо використовуватимемо зовнішні джерела.
    """

    def generate_whale_scenarios(
        self, market_data: dict[str, Any], whale_signals: dict[str, Any]
    ) -> dict[str, Any]:
        """Генерує початкові сценарії та застосовує корекцію ймовірностей.

        Args:
            market_data: контекст ринку (levels, vwap, volumes, recent_trades ...).
            whale_signals: результат детекції китів (OrderbookAnalysis.detect_whale_manipulation).

        Returns:
            dict: карта сценаріїв з полем 'probability' для кожного сценарію.
        """
        logger.info(
            "PredictiveScenarioMatrix.generate_whale_scenarios: старт генерації сценаріїв (market_data_keys=%s whale_keys=%s)",
            list(market_data.keys()) if isinstance(market_data, dict) else None,
            list(whale_signals.keys()) if isinstance(whale_signals, dict) else None,
        )

        # Збір контекстних метрик (захищено)
        recent_trades = (
            market_data.get("recent_trades") if isinstance(market_data, dict) else []
        )
        levels = market_data.get("levels") if isinstance(market_data, dict) else {}
        key_levels = levels.get("key_levels") if isinstance(levels, dict) else {}
        liquidity_pools = (
            key_levels.get("liquidity_pools") if isinstance(key_levels, dict) else []
        )
        accumulation_zones = (
            key_levels.get("accumulation_zones") if isinstance(key_levels, dict) else []
        )
        distribution_zones = (
            key_levels.get("distribution_zones") if isinstance(key_levels, dict) else []
        )

        # Просте витягування цін/обсягів з recent_trades
        buy_vol = 0.0
        sell_vol = 0.0
        total_vol = 0.0
        prices = []
        try:
            for t in recent_trades or []:
                size = float(t.get("size", 0.0)) if isinstance(t, dict) else 0.0
                side = (t.get("side") or "").lower() if isinstance(t, dict) else ""
                price = None
                try:
                    price = (
                        float(t.get("price"))
                        if isinstance(t, dict) and t.get("price") is not None
                        else None
                    )
                except Exception:
                    price = None
                if price is not None:
                    prices.append(price)
                if size <= 0.0:
                    continue
                total_vol += size
                if side == "buy":
                    buy_vol += size
                elif side == "sell":
                    sell_vol += size
        except Exception:
            logger.debug(
                "generate_whale_scenarios: помилка при агрегації recent_trades, ігноруємо"
            )

        buy_ratio = (buy_vol / total_vol) if total_vol > 0 else 0.0
        first_price = prices[0] if prices else None
        last_price = prices[-1] if prices else None
        price_change_pct = None
        if first_price and last_price:
            try:
                price_change_pct = (last_price - first_price) / first_price
            except Exception:
                price_change_pct = None

        # VWAP deviation якщо передано
        vwap = market_data.get("vwap") if isinstance(market_data, dict) else None
        vwap_dev = None
        try:
            if vwap is not None and last_price is not None:
                vwap_dev = (
                    (float(last_price) - float(vwap)) / float(vwap)
                    if float(vwap) != 0
                    else None
                )
        except Exception:
            vwap_dev = None

        est_size = 0.0
        try:
            est_size = (
                float(whale_signals.get("estimated_size", 0.0))
                if isinstance(whale_signals, dict)
                else 0.0
            )
        except Exception:
            est_size = 0.0

        # Порогові значення (тимчасово тут, потім у config)
        buy_ratio_threshold = 0.60
        price_move_threshold = 0.02  # 2%
        vwap_deviation_accum = -0.01  # price нижче VWAP -> accumulation signal
        liquidity_pool_min = 1
        large_whale_size = 500.0

        # Базовий набір сценаріїв з детальними conditions
        base: dict[str, Any] = {
            "ACCUMULATION_CONTINUES": {
                "conditions": [
                    {
                        "id": "buy_volume_share_high",
                        "description": "Велика частка buy-volume у недавніх трейдах",
                        "met": buy_ratio >= buy_ratio_threshold,
                        "value": round(buy_ratio, 3),
                    },
                    {
                        "id": "presence_of_accumulation_zones",
                        "description": "Існують accumulation zones у рівнях",
                        "met": bool(accumulation_zones),
                        "value": len(accumulation_zones),
                    },
                    {
                        "id": "price_bounce_or_mark_up",
                        "description": "Позитивна зміна ціни > PRICE_MOVE_THRESHOLD",
                        "met": (
                            price_change_pct is not None
                            and price_change_pct >= price_move_threshold
                        ),
                        "value": (
                            round(price_change_pct, 4)
                            if price_change_pct is not None
                            else None
                        ),
                    },
                    {
                        "id": "vwap_supporting",
                        "description": "Остання ціна нижче VWAP (підтримка для накопичення)",
                        "met": (
                            vwap_dev is not None and vwap_dev <= vwap_deviation_accum
                        ),
                        "value": round(vwap_dev, 4) if vwap_dev is not None else None,
                    },
                    {
                        "id": "estimated_whale_size",
                        "description": "Estimated whale size перевищує LARGE_WHale_SIZE",
                        "met": est_size >= large_whale_size,
                        "value": est_size,
                    },
                ],
                "probability": 0.35,
                "price_targets": ["+5%", "+8%", "+12%"],
                "triggers": ["break_above_resistance", "volume_spike"],
            },
            "DISTRIBUTION_PHASE": {
                "conditions": [
                    {
                        "id": "sell_pressure_high",
                        "description": "Переважає sell-volume у недавніх трейдах",
                        "met": (total_vol > 0 and (sell_vol / total_vol) >= 0.55),
                        "value": round(
                            (sell_vol / total_vol) if total_vol > 0 else 0.0, 3
                        ),
                    },
                    {
                        "id": "presence_of_distribution_zones",
                        "description": "Існують distribution zones у рівнях",
                        "met": bool(distribution_zones),
                        "value": len(distribution_zones),
                    },
                    {
                        "id": "price_drop_detected",
                        "description": "Негативна зміна ціни <= -PRICE_MOVE_THRESHOLD",
                        "met": (
                            price_change_pct is not None
                            and price_change_pct <= -price_move_threshold
                        ),
                        "value": (
                            round(price_change_pct, 4)
                            if price_change_pct is not None
                            else None
                        ),
                    },
                    {
                        "id": "whale_signals_indicate_distribution",
                        "description": "Маніпуляція типу distribution в whale_signals",
                        "met": any(
                            "distribution" in str(t).lower()
                            for t in (whale_signals.get("manipulation_type") or [])
                        ),
                        "value": (
                            whale_signals.get("manipulation_type")
                            if isinstance(whale_signals, dict)
                            else None
                        ),
                    },
                ],
                "probability": 0.45,
                "price_targets": ["-3%", "-7%", "-10%"],
                "triggers": ["break_below_support", "liquidation_cascade"],
            },
            "STOP_HUNT_BEFORE_MOVE": {
                "conditions": [
                    {
                        "id": "near_stop_hunt_levels",
                        "description": "Наявні stop-hunt рівні поруч з поточною ціною",
                        "met": (
                            bool(levels.get("stop_hunt_levels"))
                            if isinstance(levels, dict)
                            else False
                        ),
                        "value": (
                            len(levels.get("stop_hunt_levels") or [])
                            if isinstance(levels, dict)
                            else 0
                        ),
                    },
                    {
                        "id": "spoofing_or_wall_detected",
                        "description": "Спуфінг/wall паттерни в whale_signals.analysis або manipulation_type",
                        "met": (
                            bool(
                                whale_signals.get("analysis", {}).get(
                                    "spoofing_attempts"
                                )
                            )
                            if isinstance(whale_signals, dict)
                            and isinstance(whale_signals.get("analysis"), dict)
                            else any(
                                "spoof" in str(t).lower() or "wall" in str(t).lower()
                                for t in (whale_signals.get("manipulation_type") or [])
                            )
                        ),
                        "value": (
                            whale_signals.get("manipulation_type")
                            if isinstance(whale_signals, dict)
                            else None
                        ),
                    },
                    {
                        "id": "low_liquidity_pools",
                        "description": "Мало видимих liquidity pools -> ризик stop-hunt",
                        "met": (len(liquidity_pools) < liquidity_pool_min),
                        "value": len(liquidity_pools),
                    },
                    {
                        "id": "sudden_volume_spike_but_no_follow_through",
                        "description": "Сплеск обсягів без сталого руху (proxy)",
                        "met": (
                            buy_ratio > 0.45
                            and price_change_pct is not None
                            and abs(price_change_pct) < 0.005
                        ),
                        "value": {
                            "buy_ratio": round(buy_ratio, 3),
                            "price_change_pct": (
                                round(price_change_pct, 4)
                                if price_change_pct is not None
                                else None
                            ),
                        },
                    },
                ],
                "probability": 0.20,
                "price_targets": ["-2% quick", "+5% reversal"],
                "triggers": ["liq_level_break", "quick_reversal"],
            },
        }

        # Делегуємо корекцію ймовірностей до окремого методу (для легшого тестування)
        adjusted = self.adjust_probabilities(base, whale_signals)

        logger.info(
            "PredictiveScenarioMatrix.generate_whale_scenarios: згенеровано %d сценаріїв (probabilities=%s)",
            len(adjusted),
            {k: v.get("probability") for k, v in adjusted.items()},
        )
        return adjusted

    def adjust_probabilities(
        self, scenarios: dict[str, Any], whale_signals: dict[str, Any]
    ) -> dict[str, Any]:
        """Корекція ймовірностей на основі простих евристик.

        Пояснення логіки:
        - Якщо whale_present -> посилюємо ті сценарії, які співпадають з типами маніпуляцій.
        - Якщо estimated_size велика -> підвищуємо вагу найбільш релевантного сценарію.
        - Захищаємося від некоректних типів та не робимо радикальних змін без конфіг.
        Повертає новий словник сценаріїв (не мутує вхідний).
        """
        logger.debug(
            "adjust_probabilities: вхідні probabilities=%s",
            {k: v.get("probability") for k, v in scenarios.items()},
        )
        try:
            out = {k: dict(v) for k, v in scenarios.items()}  # shallow copy для безпеки
            whale_present = (
                bool(whale_signals.get("whale_present"))
                if isinstance(whale_signals, dict)
                else False
            )
            types = (
                whale_signals.get("manipulation_type")
                if isinstance(whale_signals, dict)
                else []
            )
            est_size = (
                float(whale_signals.get("estimated_size", 0.0))
                if isinstance(whale_signals, dict)
                else 0.0
            )

            logger.debug(
                "adjust_probabilities: whale_present=%s types=%s estimated_size=%.2f",
                whale_present,
                types,
                est_size,
            )

            # Простi локальні правила (тимчасово, TODO: винести в конфіг)
            large_size_threshold = 500.0  # TODO: перенести в config/config.py
            boost_small = 0.10
            boost_large = 0.20

            if whale_present:
                # Якщо є типи маніпуляцій — коригуємо відповідні сценарії
                if isinstance(types, list):
                    lower_types = [str(t).lower() for t in types]
                    # Якщо знайдені сигнали "distribution" -> підсилити DISTRIBUTION_PHASE
                    if any("distribution" in t for t in lower_types):
                        prev = out["DISTRIBUTION_PHASE"]["probability"]
                        out["DISTRIBUTION_PHASE"]["probability"] = min(
                            0.95, prev + boost_small
                        )
                        logger.debug(
                            "adjust_probabilities: boosted DISTRIBUTION_PHASE %.3f -> %.3f",
                            prev,
                            out["DISTRIBUTION_PHASE"]["probability"],
                        )
                    # Якщо знайдені сигнали accumulation-like -> підсилити ACCUMULATION_CONTINUES
                    if any("accum" in t or "accumulation" in t for t in lower_types):
                        prev = out["ACCUMULATION_CONTINUES"]["probability"]
                        out["ACCUMULATION_CONTINUES"]["probability"] = min(
                            0.95, prev + boost_small
                        )
                        logger.debug(
                            "adjust_probabilities: boosted ACCUMULATION_CONTINUES %.3f -> %.3f",
                            prev,
                            out["ACCUMULATION_CONTINUES"]["probability"],
                        )
                    # Якщо знайдені spoofing/spoof -> підвищити STOP_HUNT_BEFORE_MOVE (proxy)
                    if any("spoof" in t or "wall" in t for t in lower_types):
                        prev = out["STOP_HUNT_BEFORE_MOVE"]["probability"]
                        out["STOP_HUNT_BEFORE_MOVE"]["probability"] = min(
                            0.95, prev + boost_small
                        )
                        logger.debug(
                            "adjust_probabilities: boosted STOP_HUNT_BEFORE_MOVE %.3f -> %.3f",
                            prev,
                            out["STOP_HUNT_BEFORE_MOVE"]["probability"],
                        )

                # Якщо оцінений розмір значний — додатковий буст для найімовірнішого сценарію
                if est_size >= large_size_threshold:
                    # Обрати сценарій з найбільшою поточною ймовірністю та підсилити його
                    winner = max(
                        out.items(), key=lambda kv: float(kv[1].get("probability", 0.0))
                    )[0]
                    prev = out[winner]["probability"]
                    out[winner]["probability"] = min(0.99, prev + boost_large)
                    logger.debug(
                        "adjust_probabilities: large_est_size -> boosted %s %.3f -> %.3f",
                        winner,
                        prev,
                        out[winner]["probability"],
                    )

            # Захисна нормалізація (необов'язкова): якщо сума > 1.5, трохи нормалізуємо, щоб уникнути екстремумів
            total_prob = sum(float(v.get("probability", 0.0)) for v in out.values())
            logger.debug(
                "adjust_probabilities: total_prob_before_normalize=%.3f", total_prob
            )
            max_reasonable_sum = 1.5  # TODO: налаштувати за domain knowledge
            if total_prob > max_reasonable_sum and total_prob > 0:
                factor = max_reasonable_sum / total_prob
                for v in out.values():
                    old = v["probability"]
                    v["probability"] = float(old) * factor
                logger.debug(
                    "adjust_probabilities: застосовано нормалізацію factor=%.6f", factor
                )

            logger.info(
                "adjust_probabilities: скориговані probabilities=%s",
                {k: v.get("probability") for k, v in out.items()},
            )
            return out
        except Exception as exc:
            logger.exception(
                "adjust_probabilities: помилка при корекції ймовірностей: %s", exc
            )
            # У випадку помилки повертаємо оригінальний словник (без змін)
            return scenarios
