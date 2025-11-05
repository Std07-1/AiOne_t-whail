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

try:
    from config.config_whale import STAGE2_WHALE_TELEMETRY  # type: ignore
except Exception:  # pragma: no cover
    STAGE2_WHALE_TELEMETRY = {}  # type: ignore[assignment]

# Локальний стан для EMA згладжування presence (per-symbol, телеметрія-only)
_PRESENCE_EMA_STATE: dict[str, float] = {}

try:
    # доступна логіка телеметрії whale v2
    from .whale_telemetry_scoring import WhaleTelemetryScoring
except Exception:  # pragma: no cover
    WhaleTelemetryScoring = None  # type: ignore[assignment]


class WhaleEngine:
    """Легкий оркестратор китової телеметрії для Stage2.

    - Не має побічних ефектів (не пише в сховище), лише збагачує ctx.meta.
    - Працює на агрегованих метриках, якщо вузькі модулі (orderbook/zones) недоступні.
    - Мінімальна залежність: WhaleTelemetryScoring (якщо немає — graceful fallback).
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

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

        # 1) Побудова surrogate "strats" на базі доступних полів (VWAP dev)
        vwap = safe_float(stats.get("vwap"))
        price = safe_float(stats.get("current_price"))
        vwap_dev = None
        if isinstance(vwap, float) and isinstance(price, float) and vwap > 0:
            vwap_dev = (price - vwap) / vwap
        strats: dict[str, Any] = {
            # базові прапори за замовчуванням False — евристика візьме dev
            "vwap_buying": False,
            "vwap_selling": False,
            "twap_accumulation": False,
            "iceberg_orders": False,
            "vwap_deviation": vwap_dev,
        }

        presence: float | None = None
        bias: float | None = None
        dominance: dict[str, Any] | None = None

        if WhaleTelemetryScoring is not None:
            try:
                presence = WhaleTelemetryScoring.presence_score_from(strats, zones=None)
            except Exception as exc:  # noqa: BLE001
                self._logger.debug("whale: presence_score_from fail: %s", exc)
                presence = None
            try:
                bias = WhaleTelemetryScoring.whale_bias_from(strats)
            except Exception as exc:  # noqa: BLE001
                self._logger.debug("whale: whale_bias_from fail: %s", exc)
                bias = None
            try:
                dominance = WhaleTelemetryScoring.dominance_flags(
                    whale_bias=bias,
                    vwap_dev=vwap_dev,
                    slope_atr=safe_float(
                        (ctx.get("directional") or {}).get("price_slope_atr")
                    ),
                    dvr=safe_float(
                        (ctx.get("directional") or {}).get("directional_volume_ratio")
                    ),
                    trap=(
                        (meta.get("insight_overlay") or {}).get("trap")
                        if isinstance(meta.get("insight_overlay"), dict)
                        else None
                    ),
                    iceberg=strats.get("iceberg_orders"),
                    dist_zones=None,
                )
            except Exception as exc:  # noqa: BLE001
                self._logger.debug("whale: dominance_flags fail: %s", exc)
                dominance = None
        else:
            presence = None
            bias = None
            dominance = None

        # 2) Інʼєкція в meta.whale та дзеркало у insight_overlay для strategy_selector
        if presence is not None:
            presence_val = float(presence)
            # EMA‑згладжування (за фіче‑флагом у STAGE2_WHALE_TELEMETRY)
            ema_cfg = dict((STAGE2_WHALE_TELEMETRY or {}).get("ema") or {})
            ema_enabled = bool(ema_cfg.get("enabled", False))
            alpha_raw = ema_cfg.get("alpha", 0.30)
            alpha = float(alpha_raw) if isinstance(alpha_raw, (int, float)) else 0.30
            alpha = max(0.01, min(0.99, alpha))

            presence_raw = presence_val
            if ema_enabled:
                prev = _PRESENCE_EMA_STATE.get(symbol)
                if isinstance(prev, (int, float)):
                    presence_val = float(
                        alpha * presence_val + (1.0 - alpha) * float(prev)
                    )
                # якщо попереднього не було — ініціалізуємо поточним значенням
                _PRESENCE_EMA_STATE[symbol] = presence_val
                # Для прозорості додамо сире значення у meta (контракт не порушуємо)
                whale_meta["presence_score_raw"] = float(presence_raw)
            whale_meta["presence_score"] = float(presence_val)
            overlay["whale_presence"] = float(presence_val)
        if bias is not None:
            whale_meta["whale_bias"] = float(bias)
            overlay["whale_bias"] = float(bias)
        if vwap_dev is not None:
            whale_meta["vwap_deviation"] = float(vwap_dev)
        if dominance is not None:
            whale_meta["dominance"] = dominance

        return ctx
