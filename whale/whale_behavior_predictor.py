from __future__ import annotations

import logging
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("whale_behavior_predictor")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class WhaleBehaviorPredictor:
    """Прогноз поведінки китів на базі профілів (каркас) з докладним логуванням.

    Мета:
    - Надати каркас для формування ймовірних наступних кроків китів на основі
      профілів та контексту ринку.
    - Наразі — детально логувати вхідні дані та прийняті рішення; пізніше
      підключити реальні алгоритми (ML / rules) та кешування результатів.

    TODO:
    - Винести пороги/мапінги профілів у config/config.py (константи).
    - Додати unit-тести: happy-path + edge-cases + псевдо-стріми (stream).
    - Додати можливість асинхронних оновлень профілів та інтеграцію з Redis.
    - Розробити та підключити реальний scoring (backtest-driven) замість current heuristics.
    """

    def __init__(self) -> None:
        """Ініціалізація з базовими профілями та логуванням."""
        # Базові профілі — легка дескрипція для генерації підказок
        self.whale_profiles: dict[str, dict[str, str]] = {
            "market_maker": {"behavior": "liquidity_provision", "style": "vwap_twap"},
            "hedge_fund": {"behavior": "momentum_trading", "style": "breakout_fading"},
            "miner": {"behavior": "selling_pressure", "style": "distribution"},
            "long_term_holder": {"behavior": "accumulation", "style": "dca"},
        }
        logger.debug(
            "WhaleBehaviorPredictor.__init__ профілі завантажено: %s",
            list(self.whale_profiles.keys()),
        )
        logger.info("WhaleBehaviorPredictor ініціалізовано (каркас прогнозування)")

    def predict_next_whale_moves(
        self, current_whale_activity: dict[str, Any], market_context: dict[str, Any]
    ) -> dict[str, Any]:
        """Генерує можливі сценарії дій китів з базовими оцінками впевненості.

        Args:
            current_whale_activity: результат детекції (наприклад, OrderbookAnalysis.detect_whale_manipulation).
            market_context: контекст ринку (levels, vwap, volumes, recent_trades тощо).

        Returns:
            dict: карта сценаріїв по профілях -> {next_likely_move, alternative_moves, timeframe, confidence}
                  Поточна реалізація — евристична та призначена для офлайн-аналізу.
        """
        logger.info(
            "predict_next_whale_moves: старт прогнозу для %d профілів",
            len(self.whale_profiles),
        )
        logger.debug(
            "predict_next_whale_moves: current_whale_activity keys=%s market_context keys=%s",
            (
                list(current_whale_activity.keys())
                if isinstance(current_whale_activity, dict)
                else None
            ),
            list(market_context.keys()) if isinstance(market_context, dict) else None,
        )

        scenarios: dict[str, Any] = {}
        for profile in self.whale_profiles:
            logger.debug(
                "Оцінка профілю='%s' (descriptor=%s)",
                profile,
                self.whale_profiles[profile],
            )
            try:
                if not self.is_profile_active(profile, current_whale_activity):
                    logger.debug(
                        "Профіль '%s' вважається неактивним за поточними сигналами",
                        profile,
                    )
                    continue

                moves = self.generate_probable_moves(profile, market_context)
                timeframe = self.get_move_timeframe(profile)
                confidence = self.calculate_confidence(profile, market_context)

                scenarios[profile] = {
                    "next_likely_move": moves[0] if moves else None,
                    "alternative_moves": moves[1:] if len(moves) > 1 else [],
                    "timeframe": timeframe,
                    "confidence": confidence,
                }

                logger.info(
                    "predict_next_whale_moves: profile=%s next=%s confidence=%.2f timeframe=%s",
                    profile,
                    scenarios[profile]["next_likely_move"],
                    confidence,
                    timeframe,
                )
            except Exception as exc:
                logger.exception(
                    "predict_next_whale_moves: помилка при обробці профілю %s: %s",
                    profile,
                    exc,
                )
                # Не даємо провалу всього прогнозу через один профіль
                continue

        ranked = self.rank_scenarios_by_probability(scenarios)
        logger.debug("predict_next_whale_moves: згенеровані сценарії=%s", ranked)
        return ranked

    # -------------------------
    # Заглушки логіки профілів з логуванням та TODO
    # -------------------------
    def is_profile_active(
        self, profile: str, activity: dict[str, Any]
    ) -> bool:  # noqa: D401
        """Перевірка активності профілю.

        Поточна реалізація — conservative: повертає True (тобто профіль розглядається).
        TODO: реалізувати реальні правила активації:
            - перевіряти activity["whale_present"]
            - шукати pattern у activity["manipulation_type"]
            - пороги по estimated_size -> поріг у config
        """
        logger.debug(
            "is_profile_active: profile=%s activity_summary=%s",
            profile,
            {
                "whale_present": (
                    bool(activity.get("whale_present"))
                    if isinstance(activity, dict)
                    else None
                ),
                "types": (
                    activity.get("manipulation_type")
                    if isinstance(activity, dict)
                    else None
                ),
            },
        )
        # Тимчасово — завжди вважаємо активним, щоб не приховувати потенційні сценарії
        return True

    def generate_probable_moves(
        self, profile: str, market_context: dict[str, Any]
    ) -> list[str]:  # noqa: D401
        """Повертає список ймовірних дій для профілю (евристика).

        Пояснення:
        - Наразі використовує просту мапу поведінок для ілюстрації.
        - TODO: замінити на правила або ML-модель, що враховує market_context.
        """
        # Тимчасова мапа дій (повинна перейти у config або окремий rules-файл)
        profile_moves_map: dict[str, list[str]] = {
            "market_maker": [
                "provide_liquidity",
                "tighten_spread",
                "pull_large_orders",
            ],
            "hedge_fund": [
                "enter_momentum_trade",
                "scale_into_trend",
                "take_profit_partial",
            ],
            "miner": ["increase_selling", "sell_to_exchange", "gradual_distribution"],
            "long_term_holder": ["accumulate_dca", "hold", "add_on_dips"],
        }
        moves = profile_moves_map.get(profile, [])
        logger.debug("generate_probable_moves: profile=%s moves=%s", profile, moves)
        return moves

    def get_move_timeframe(self, profile: str) -> str:  # noqa: D401
        """Орієнтовний timeframe для дій профілю (заглушка)."""
        tf_map = {
            "market_maker": "intra-hour",
            "hedge_fund": "intra-day",
            "miner": "multi-day",
            "long_term_holder": "weeks+",
        }
        timeframe = tf_map.get(profile, "intra-day")
        logger.debug("get_move_timeframe: profile=%s timeframe=%s", profile, timeframe)
        return timeframe

    def calculate_confidence(
        self, profile: str, market_context: dict[str, Any]
    ) -> float:  # noqa: D401
        """Оцінка впевненості (0..1) для сценарію профілю.

        Поточна логіка — простий heuristic:
        - Якщо в market_context є confidence_metrics.confidence -> використовуємо її як базу.
        - Додаткові фактори (estimated_size, volatility) можна додати згодом.
        TODO:
        - Винести ваги у конфіг; додати backtest-driven calibration.
        - Додавати penalization за відсутність recent_trades або low_liquidity.
        """
        base_conf = 0.5  # дефолтна нейтральна впевненість
        try:
            # Приклад: якщо market_context має confidence_metrics -> інтегруємо його
            cm = (
                market_context.get("confidence_metrics")
                if isinstance(market_context, dict)
                else None
            )
            if isinstance(cm, dict) and "confidence" in cm:
                base_conf = float(cm.get("confidence", base_conf))
                logger.debug(
                    "calculate_confidence: використано market_context.confidence=%.3f",
                    base_conf,
                )

            # Невелике коригування за профілем (поки статично)
            profile_bias = {
                "market_maker": 0.05,
                "hedge_fund": 0.1,
                "miner": -0.05,
                "long_term_holder": 0.0,
            }
            bias = profile_bias.get(profile, 0.0)
            conf = max(0.0, min(1.0, base_conf + bias))
            logger.debug(
                "calculate_confidence: profile=%s base=%.3f bias=%.3f final=%.3f",
                profile,
                base_conf,
                bias,
                conf,
            )
            return float(conf)
        except Exception as exc:
            logger.exception(
                "calculate_confidence: помилка при обчисленні для profile=%s: %s",
                profile,
                exc,
            )
            return 0.0

    def rank_scenarios_by_probability(
        self, scenarios: dict[str, Any]
    ) -> dict[str, Any]:  # noqa: D401
        """Сортування / нормалізація сценаріїв за confidence.

        Поточна реалізація повертає вхідну структуру, але додає логування та
        поле 'ranked_profiles' з порядком профілів за confidence (найвищий перший).

        TODO:
        - Нормалізація у вигляді ймовірностей (сумарно = 1.0) або softmax від scores.
        - Зберігати результат у кеш/Redis для reuse з TTL (через config).
        """
        logger.debug(
            "rank_scenarios_by_probability: початок ранжирування %d сценаріїв",
            len(scenarios),
        )
        try:
            # Формуємо список (profile, confidence)
            ranked = sorted(
                ((p, float(v.get("confidence", 0.0))) for p, v in scenarios.items()),
                key=lambda x: x[1],
                reverse=True,
            )
            ranked_profiles = [p for p, _ in ranked]
            logger.info(
                "rank_scenarios_by_probability: ranked_profiles=%s", ranked_profiles
            )

            # Додаємо мета-поле, але зберігаємо сумісну структуру
            result = dict(scenarios)
            result["_ranked_profiles"] = ranked_profiles
            return result
        except Exception as exc:
            logger.exception(
                "rank_scenarios_by_probability: помилка ранжування: %s", exc
            )
            return scenarios
