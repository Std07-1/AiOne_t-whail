"""Шим для Stage3 price stream.

Цей модуль зберігає сумісність існуючих імпортів, переекспортувавши
реалізацію з ``stage3.price_stream_shadow``.
"""

from stage3.price_stream_shadow import (  # noqa: F401
    PriceHealthWatchdog,
    PriceStreamConfig,
    PriceStreamShadowProducer as PriceStreamService,
    PriceUpdate,
)

__all__ = [
    "PriceStreamConfig",
    "PriceUpdate",
    "PriceHealthWatchdog",
    "PriceStreamService",
]
