"""Застарілий шім: перенесено до stage3.shadow.trade_execution_service.

Файл збережено для зворотної сумісності; нові імпорти варто робити з
`stage3.shadow.trade_execution_service`.
"""

from stage3.shadow.trade_execution_service import TradeExecutionService  # noqa: F401

__all__ = ["TradeExecutionService"]
