"""Застарілий шім: перенесено до stage3.shadow.strategy_service.

Файл збережено для зворотної сумісності; нові імпорти варто робити з
`stage3.shadow.strategy_service`.
"""

from stage3.shadow.strategy_service import StrategyService  # noqa: F401

__all__ = ["StrategyService"]
