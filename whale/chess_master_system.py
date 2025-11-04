from __future__ import annotations

import logging
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from .orderbook_analysis import OrderbookAnalysis
from .predictive_scenario_matrix import PredictiveScenarioMatrix
from .whale_response_manager import WhaleResponseManager

logger = logging.getLogger("chess_master_system")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class ChessMasterSystem:
    """Інтеграційний фасад «шахового мислення» (каркас).

    Призначення:
    - Оркеструє виклики детекторів (OrderbookAnalysis), генераторів сценаріїв
      (PredictiveScenarioMatrix) та менеджера відповідей (WhaleResponseManager).
    - НЕ має побічних ефектів (поки) — повертає структуру з рекомендаціями
      та діагностичною інформацією для offline-аналізу / UI.

    TODOs:
    - Винести пороги/ваги у config/config.py.
    - Додати caching результатів у Redis та TTL (через config).
    - Додати unit-тести: happy-path + edge-cases + псевдо-стріми.
    """

    def __init__(self) -> None:
        # Ініціалізація підсистем (каркасні екземпляри)
        self.whale_detector = OrderbookAnalysis()
        self.scenario_planner = PredictiveScenarioMatrix()
        self.response_manager = WhaleResponseManager()
        logger.debug("ChessMasterSystem.__init__: ініціалізовано підсистеми")

    def calculate_next_moves(self, market_data: dict[str, Any]) -> dict[str, Any]:
        """Основний потік: детекція → сценарії → відповідь.

        Args:
            market_data: очікуваний формат словника з ключами типу
                "orderbook", "levels", "vwap", "volumes", "recent_trades" тощо.

        Returns:
            dict: з полями:
                - current_whale_activity: детекція ордербуку
                - probable_scenario: ключ обраного сценарію або None
                - scenario_summary: компактне резюме сценарію (для логування/UI)
                - recommended_actions: список дій з WhaleResponseManager
                - contingency_plans: запасні плани (поки заглушка)
                - confidence_level: зведена впевненість (0..1)
        """
        logger.info("calculate_next_moves: старт обробки ринкових даних")
        logger.debug(
            "calculate_next_moves: входи market_data.keys=%s",
            list(market_data.keys()) if isinstance(market_data, dict) else None,
        )

        try:
            # 1) Детекція китової активності (захищено)
            orderbook = (
                market_data.get("orderbook", {})
                if isinstance(market_data, dict)
                else {}
            )
            whale_activity = self.whale_detector.detect_whale_manipulation(orderbook)
            logger.debug("calculate_next_moves: whale_activity=%s", whale_activity)

            # 2) Генерація/корекція сценаріїв на основі детекції
            scenarios = self.scenario_planner.generate_whale_scenarios(
                market_data, whale_activity
            )
            logger.debug(
                "calculate_next_moves: scenarios (sample)=%s",
                {
                    k: v.get("probability")
                    for k, v in scenarios.items()
                    if isinstance(v, dict)
                },
            )

            # 3) Вибір найкращого сценарію (із пропуском мета-полів, напр. '_ranked_profiles')
            best_key = self.select_best_scenario(scenarios)
            logger.info("calculate_next_moves: selected_best_scenario=%s", best_key)

            # 4) Отримання плану відповіді (без підйому виключень)
            response = (
                self.response_manager.get_response_plan(best_key)
                if best_key
                else {
                    "actions": [],
                    "risk_management": {},
                    "meta": {"strategy_key": None},
                }
            )
            logger.debug(
                "calculate_next_moves: response.summary=%s",
                self._format_response_summary(response),
            )

            # 5) Підготовка вихідної структури з діагностикою
            scenario_summary = self._format_scenario_summary(scenarios, best_key)
            contingency = self.generate_contingency_plans(scenarios)
            confidence = self.calculate_overall_confidence(whale_activity, scenarios)

            result = {
                "current_whale_activity": whale_activity,
                "probable_scenario": best_key,
                "scenario_summary": scenario_summary,
                "recommended_actions": response.get("actions", []),
                "risk_management": response.get("risk_management", {}),
                "contingency_plans": contingency,
                "confidence_level": confidence,
            }

            logger.info(
                "calculate_next_moves: завершено. probable_scenario=%s confidence=%.3f actions=%d",
                best_key,
                confidence,
                len(result["recommended_actions"]),
            )
            return result
        except Exception as exc:
            # Захищаємо зовнішній виклик від падіння; повертаємо безпечний fallback
            logger.exception(
                "calculate_next_moves: критична помилка під час обробки: %s", exc
            )
            return {
                "current_whale_activity": {},
                "probable_scenario": None,
                "scenario_summary": {},
                "recommended_actions": [],
                "risk_management": {},
                "contingency_plans": [],
                "confidence_level": 0.0,
                "error": str(exc),
            }

    def select_best_scenario(
        self, scenarios: dict[str, Any]
    ) -> str | None:  # noqa: D401
        """Обирає сценарій із найбільшою probability.

        Зауваги:
        - Ігнорує служебні ключі, що починаються з '_' (наприклад, _ranked_profiles).
        - Повертає None якщо корисних сценаріїв не знайдено.
        """
        best_key: str | None = None
        best_prob = -1.0
        for k, v in (scenarios or {}).items():
            # Пропускаємо мета-поля
            if not isinstance(k, str) or k.startswith("_"):
                continue
            try:
                prob = float(v.get("probability", 0.0)) if isinstance(v, dict) else 0.0
            except Exception:
                prob = 0.0
            logger.debug("select_best_scenario: candidate=%s prob=%.6f", k, prob)
            if prob > best_prob:
                best_prob = prob
                best_key = k
        logger.debug(
            "select_best_scenario: best_key=%s best_prob=%.6f",
            best_key,
            best_prob if best_key is not None else 0.0,
        )
        return best_key

    def generate_contingency_plans(
        self, scenarios: dict[str, Any]
    ) -> list[dict[str, Any]]:  # noqa: D401
        """Формує запасні плани.

        Поточна реалізація — заглушка:
        - TODO: генерувати fallback plans на основі низькопробабілітних сценаріїв,
          винести логіку у окремий rules-файл або config.
        - TODO: кешувати плани у Redis з TTL (через config).
        """
        logger.debug(
            "generate_contingency_plans: формування запасних планів (заглушка)"
        )
        # Приклад: беремо до 2 альтернативних сценаріїв (як заглушку)
        plans: list[dict[str, Any]] = []
        try:
            if not isinstance(scenarios, dict):
                return plans
            # Відфільтруємо мета-поля та візьмемо альтернативи з найвищими probability
            candidates = [
                (k, float(v.get("probability", 0.0)))
                for k, v in scenarios.items()
                if isinstance(k, str) and not k.startswith("_") and isinstance(v, dict)
            ]
            candidates.sort(key=lambda x: x[1], reverse=True)
            # TODO: параметризувати число альтернатив через config
            for k, p in candidates[1:3]:
                plans.append(
                    {
                        "scenario_key": k,
                        "probability": p,
                        "note": "contingency (auto-generated)",
                    }
                )
            logger.debug("generate_contingency_plans: plans=%s", plans)
            return plans
        except Exception as exc:
            logger.exception(
                "generate_contingency_plans: помилка при генерації contingency plans: %s",
                exc,
            )
            return []

    def calculate_overall_confidence(
        self, whale_activity: dict[str, Any], scenarios: dict[str, Any]
    ) -> float:  # noqa: D401
        """Проста зведена впевненість (0..1).

        Логіка (тимчасова):
        - Середнє probability для всіх сценаріїв (ігноруємо мета-поля).
        - Легкий буст якщо estimated_size великий (пороговий), TODO винести пороги у config.
        """
        try:
            probs = [
                float(v.get("probability", 0.0))
                for k, v in (scenarios or {}).items()
                if isinstance(k, str) and not k.startswith("_") and isinstance(v, dict)
            ]
            if not probs:
                logger.debug(
                    "calculate_overall_confidence: немає probability в scenarios -> 0.0"
                )
                return 0.0
            base_conf = sum(probs) / len(probs)  # середня
            logger.debug(
                "calculate_overall_confidence: base_conf_from_scenarios=%.6f", base_conf
            )

            # Невелика корекція за оцінкою розміру киту
            est_size = (
                float(whale_activity.get("estimated_size", 0.0))
                if isinstance(whale_activity, dict)
                else 0.0
            )
            large_threshold = 500.0  # TODO: винести в config/config.py
            boost = 0.0
            if est_size >= large_threshold:
                boost = 0.10
                logger.debug(
                    "calculate_overall_confidence: applied boost for est_size=%.2f boost=%.3f",
                    est_size,
                    boost,
                )

            conf = max(0.0, min(1.0, base_conf + boost))
            logger.debug("calculate_overall_confidence: final_confidence=%.6f", conf)
            return float(conf)
        except Exception as exc:
            logger.exception(
                "calculate_overall_confidence: помилка при обчисленні confidence: %s",
                exc,
            )
            return 0.0

    # -------------------------
    # Допоміжні форматувальні методи для логів / UI
    # -------------------------
    def _format_scenario_summary(
        self, scenarios: dict[str, Any], chosen_key: str | None
    ) -> dict[str, Any]:
        """Повертає компактну і читабельну інфу про сценарії для UI/логів."""
        try:
            summary = {
                "n_scenarios": 0,
                "top_scenarios": [],
                "chosen": chosen_key,
            }
            if not isinstance(scenarios, dict):
                return summary
            entries = [
                (k, float(v.get("probability", 0.0)))
                for k, v in scenarios.items()
                if isinstance(k, str) and not k.startswith("_") and isinstance(v, dict)
            ]
            entries.sort(key=lambda x: x[1], reverse=True)
            summary["n_scenarios"] = len(entries)
            summary["top_scenarios"] = [
                {"key": k, "probability": p} for k, p in entries[:3]
            ]
            logger.debug("ChessMasterSystem._format_scenario_summary: %s", summary)
            return summary
        except Exception as exc:
            logger.exception(
                "_format_scenario_summary: помилка при форматуванні summary: %s", exc
            )
            return {"n_scenarios": 0, "top_scenarios": [], "chosen": chosen_key}

    def _format_response_summary(self, response: dict[str, Any]) -> dict[str, Any]:
        """Компактне резюме response для debug-логування."""
        try:
            if not isinstance(response, dict):
                return {"n_actions": 0}
            actions = response.get("actions", [])
            rm = response.get("risk_management", {})
            return {
                "n_actions": len(actions) if isinstance(actions, list) else 0,
                "risk_keys": list(rm.keys()) if isinstance(rm, dict) else [],
            }
        except Exception:
            return {"n_actions": 0}
