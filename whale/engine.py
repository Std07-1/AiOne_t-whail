"""Китова телеметрія — єдине джерело правди (SoT).

Призначення:
- Централізує всю логіку розрахунку китових метрик (presence, whale_bias,
    dominance flags, vwap_deviation тощо).
- Stage2 (історичний QDE) не мають власної «китової» логіки; інтеграція відбувається через
    цей модуль (див. Stage2TelemetryEnricher у `stage2/processor_v2/apply_telemetry_and_enrich.py`).

Політики:
- Контракти Stage1/Stage2 не змінюємо; нові поля для UI/аналітики — лише в
    `market_context.meta.whale.*`.
- Жодних хардкодів у Redis: ключі/TTL — лише через `config`.

Цей модуль навмисно не має I/O side-effects: він лише збагачує переданий ctx.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from utils.utils import safe_float

from .core import WhaleCore, WhaleInput


class WhaleEngine:
    """Легкий оркестратор китової телеметрії для Stage2.

    - Не має побічних ефектів (не пише в сховище), лише збагачує ctx.meta.
    - Працює на агрегованих метриках, якщо вузькі модулі (orderbook/zones) недоступні.
    - Мінімальна залежність: WhaleTelemetryScoring (якщо немає — graceful fallback).
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._core = WhaleCore(logger=self._logger)

    def enrich_ctx(
        self, *, symbol: str, stats: Mapping[str, Any], ctx: dict[str, Any]
    ) -> dict[str, Any]:
        """Обчислює whale_presence/whale_bias та інʼєктує у ctx.meta.

        Args:
            symbol: Тікер (для логування).
            stats: Плоский словник Stage1/Stage2 stats (current_price, vwap, ...).
            ctx: Market context (буде змінений in-place).

        Returns:
            Оновлений ctx (той самий обʼєкт).
        """
        meta = ctx.setdefault("meta", {})
        overlay = meta.setdefault("insight_overlay", {})
        whale_meta = meta.setdefault("whale", {})

        stats_whale = stats.get("whale")
        whale_snapshot = stats_whale if isinstance(stats_whale, Mapping) else {}

        directional_ctx = ctx.get("directional") or {}
        directional_payload = {
            "price_slope_atr": safe_float(directional_ctx.get("price_slope_atr")),
            "directional_volume_ratio": safe_float(
                directional_ctx.get("directional_volume_ratio")
            ),
            "trap": overlay.get("trap") if isinstance(overlay, dict) else None,
        }

        stage2_derived = {
            "zones_summary": whale_snapshot.get("zones_summary", {}),
            "vol_regime": whale_snapshot.get("vol_regime"),
            "dev_level": whale_snapshot.get("dev_level"),
        }

        stats_slice = {
            "current_price": stats.get("current_price"),
            "vwap": stats.get("vwap"),
        }

        time_ctx = {
            "now_ts": (
                safe_float(whale_snapshot.get("ts_s"))
                if isinstance(whale_snapshot, Mapping)
                else 0.0
            ),
        }

        whale_input = WhaleInput(
            symbol=symbol,
            whale_snapshot=whale_snapshot,
            stats_slice=stats_slice,
            stage2_derived=stage2_derived,
            directional=directional_payload,
            time_context=time_ctx,
            meta_overlay=overlay if isinstance(overlay, dict) else None,
        )

        telemetry = self._core.compute(whale_input)

        if telemetry.meta_payload:
            whale_meta.update(telemetry.meta_payload)
        if telemetry.overlay_payload:
            overlay.update(telemetry.overlay_payload)

        return ctx
