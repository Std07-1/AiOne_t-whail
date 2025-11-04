from __future__ import annotations

import logging
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("whale_response_manager")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class WhaleResponseManager:
    """Менеджер відповіді на дії китів (каркас із шаблонами).

    Надає прості плани дій для попередньо визначених сценаріїв.

    TODO:
    - Винести WHALE_RESPONSE_STRATEGIES у config/config.py як константу/тему.
    - Додавати можливість feature-flag/агресивності через конфіг.
    - Кешувати/зберігати плани в Redis з ключем ai_one:responses:{scenario_key} + TTL.
    - Додати unit-тести: happy-path, неправильний формат плану, невідомий ключ.
    """

    WHALE_RESPONSE_STRATEGIES: dict[str, Any] = {
        # Стратегія при виявленій акумуляції китом
        "WHALE_ACCUMULATION_DETECTED": {
            "pre_calculated_actions": [
                {
                    "condition": "price_above_accumulation_zone",
                    "action": "ENTER_LONG_DCA",
                    "note": "входити частинами при підтвердженні пробою",
                },
                {
                    "condition": "whale_buying_confirmed",
                    "action": "ADD_TO_LONG_POSITION",
                    "note": "додавати при послідовних buy-транзакціях",
                },
                {
                    "condition": "volume_increasing",
                    "action": "INCREASE_POSITION_SIZE",
                    "note": "агресивніше при рості обсягів",
                },
            ],
            "risk_management": {
                "stop_loss": "below_accumulation_low",
                "take_profit": ["first_whale_target", "second_whale_target"],
            },
        },
        # Стратегія при виявленій роздачі (distribution)
        "WHALE_DISTRIBUTION_DETECTED": {
            "pre_calculated_actions": [
                {"condition": "price_below_distribution_zone", "action": "ENTER_SHORT"},
                {"condition": "whale_selling_intensifies", "action": "ADD_TO_SHORT"},
                {"condition": "support_break_confirmed", "action": "AGGRESSIVE_SHORT"},
            ],
            "risk_management": {
                "stop_loss": "above_distribution_high",
                "take_profit": ["liquidation_cluster_below", "next_support_level"],
            },
        },
        # Стратегія при стоп-ханті / маніпуляції зі стоп-лоссами
        "WHALE_STOP_HUNT_IN_PROGRESS": {
            "pre_calculated_actions": [
                {
                    "condition": "liquidation_level_approach",
                    "action": "PREPARE_COUNTER_TRADE",
                },
                {"condition": "stop_hunt_executed", "action": "ENTER_REVERSAL"},
                {"condition": "whale_reaccumulation", "action": "JOIN_ACCUMULATION"},
            ],
            "risk_management": {
                "stop_loss": "beyond_stop_hunt_extreme",
                "position_size": "reduced_until_confirmation",
            },
        },
    }

    def list_strategies(self) -> list[str]:
        """Повертає список доступних ключів стратегій (для UI/тестів)."""
        keys = list(self.WHALE_RESPONSE_STRATEGIES.keys())
        logger.debug("WhaleResponseManager.list_strategies: available=%s", keys)
        return keys

    def _validate_plan_structure(self, plan: Any) -> bool:
        """Проста валідація структури плану — перевіряє ключі і формат actions.

        Повертає True якщо структура виглядає очікувано.
        """
        if not isinstance(plan, dict):
            logger.warning(
                "WhaleResponseManager._validate_plan_structure: plan не dict -> %s",
                type(plan),
            )
            return False

        actions = plan.get("pre_calculated_actions", [])
        if not isinstance(actions, list):
            logger.warning(
                "WhaleResponseManager._validate_plan_structure: actions не list -> %s",
                type(actions),
            )
            return False

        # Перевіряємо кожен action на наявність мінімальних полів
        for i, a in enumerate(actions):
            if not isinstance(a, dict):
                logger.debug(
                    "WhaleResponseManager._validate_plan_structure: action[%d] не dict -> %s",
                    i,
                    type(a),
                )
                return False
            if "condition" not in a or "action" not in a:
                logger.debug(
                    "WhaleResponseManager._validate_plan_structure: action[%d] пропущене поле (очікується 'condition' і 'action') -> %s",
                    i,
                    a,
                )
                return False
        # Просте OK якщо пройшли всі перевірки
        return True

    def _format_plan_summary(self, plan: dict[str, Any]) -> dict[str, Any]:
        """Готує компактний summary плану для логів / UI."""
        actions = (
            plan.get("pre_calculated_actions", []) if isinstance(plan, dict) else []
        )
        rm = plan.get("risk_management", {}) if isinstance(plan, dict) else {}
        summary = {
            "n_actions": len(actions) if isinstance(actions, list) else 0,
            "action_conditions": [
                a.get("condition") for a in actions if isinstance(a, dict)
            ],
            "risk_keys": list(rm.keys()) if isinstance(rm, dict) else [],
        }
        logger.debug("WhaleResponseManager._format_plan_summary: %s", summary)
        return summary

    def get_response_plan(self, scenario_key: str) -> dict[str, Any]:  # noqa: D401
        """Повертає план відповіді для обраного сценарію (або порожній).

        Args:
            scenario_key: ключ сценарію (строка), наприклад 'WHALE_ACCUMULATION_DETECTED'

        Returns:
            dict: {
                "actions": list[dict],            # список дій (умова + дія + optional note)
                "risk_management": dict,          # базові рекомендації risk management
                "meta": {"strategy_key": str}     # мета-інформація для аудиту/логів
            }

        Логіка:
        - Валідуємо структуру плану перед поверненням.
        - Повернення порожньої структури замість підняття виключення, щоб уникнути side-effects.
        """
        logger.info(
            "WhaleResponseManager.get_response_plan: запит плану scenario_key=%s",
            scenario_key,
        )

        if not scenario_key or not isinstance(scenario_key, str):
            logger.warning(
                "WhaleResponseManager.get_response_plan: некоректний scenario_key=%s",
                scenario_key,
            )
            return {
                "actions": [],
                "risk_management": {},
                "meta": {"strategy_key": None},
            }

        plan = self.WHALE_RESPONSE_STRATEGIES.get(scenario_key)
        if plan is None:
            logger.info(
                "WhaleResponseManager.get_response_plan: сценарій не знайдено для key=%s",
                scenario_key,
            )
            # TODO: можливо тут має бути fallback mapping (наприклад, мапа за пріоритетом)
            return {
                "actions": [],
                "risk_management": {},
                "meta": {"strategy_key": scenario_key},
            }

        # Валідація структури плану
        valid = self._validate_plan_structure(plan)
        if not valid:
            logger.error(
                "WhaleResponseManager.get_response_plan: план для %s не пройшов валідацію — повертаємо дефолт",
                scenario_key,
            )
            # TODO: логику повідомлення про неконсистентність (metrics/alert) додати тут
            return {
                "actions": [],
                "risk_management": {},
                "meta": {"strategy_key": scenario_key, "valid": False},
            }

        # Логування детального summary (DEBUG)
        summary = self._format_plan_summary(plan)
        logger.debug(
            "WhaleResponseManager.get_response_plan: returning plan summary for %s -> %s",
            scenario_key,
            summary,
        )

        actions = (
            plan.get("pre_calculated_actions", []) if isinstance(plan, dict) else []
        )
        risk_management = (
            plan.get("risk_management", {}) if isinstance(plan, dict) else {}
        )

        # Повертаємо консистентну структуру (без мутацій оригіналу)
        return {
            "actions": [dict(a) for a in actions],
            "risk_management": dict(risk_management),
            "meta": {"strategy_key": scenario_key, "valid": True},
        }
