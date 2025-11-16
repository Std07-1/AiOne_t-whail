"""Prometheus-метрики для фазових гвардів."""

from __future__ import annotations

import logging

from rich.console import Console
from rich.logging import RichHandler

try:  # pragma: no cover - клієнт може бути відсутнім
    from prometheus_client import Counter
except Exception:  # pragma: no cover
    Counter = None  # type: ignore

logger = logging.getLogger("metrics.phase")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

if Counter is not None:  # pragma: no cover
    PHASE_PRESENCE_CAP_DELTA = Counter(
        "ai_one_phase_presence_cap_delta_total",
        "Скільки разів presence був зменшений гвардом presence_cap_no_bias_htf",
        ["symbol"],
    )
    PHASE_REJECT_TOTAL = Counter(
        "ai_one_phase_reject_total",
        "Кількість відмов phase із reason-code",
        labelnames=("symbol", "reason"),
    )
else:  # pragma: no cover
    PHASE_PRESENCE_CAP_DELTA = None
    PHASE_REJECT_TOTAL = None

__all__ = ["PHASE_PRESENCE_CAP_DELTA", "PHASE_REJECT_TOTAL"]
