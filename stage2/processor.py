"""Легкий stub Stage2Processor для офлайн-реплею.

Призначення:
- Надати сумісний інтерфейс `Stage2Processor.process()` для tools/replay_stream.py
- Без зовнішніх залежностей і без впливу на Stage1 контракти.
- Використовується тільки для локального аналізу та телеметрії.

Зауваження:
- Повертає мінімальний `Stage2Output` із полем `market_context.meta` (htf_ok, vol_regime тощо, якщо є в stats).
- У продакшн-циклі Stage2 залишається вимкненим.
"""

from __future__ import annotations

from typing import Any

from utils.phase_adapter import detect_phase_from_stats


class Stage2Processor:
    def __init__(self, state_manager: Any | None = None) -> None:  # noqa: D401
        """Мінімальна ініціалізація; надає буфери для bars_* як у повній версії."""
        self.state_manager = state_manager
        self.bars_1m: dict[str, Any] = {}
        self.bars_5m: dict[str, Any] = {}
        self.bars_1d: dict[str, Any] = {}

    async def process(
        self, stage1_signal: dict[str, Any]
    ) -> dict[str, Any]:  # noqa: D401
        """Повертає мінімальний Stage2-вихід на основі stats із Stage1.

        Args:
            stage1_signal: Сигнал Stage1 зі stats.

        Returns:
            dict: Мінімальна структура Stage2Output з market_context.meta.
        """
        stats = stage1_signal.get("stats") if isinstance(stage1_signal, dict) else None
        meta: dict[str, Any] = {}
        if isinstance(stats, dict):
            # Пробуємо підкласти обрані поля у meta для телеметрії реплею
            for key in (
                "htf_ok",
                "htf_score",
                "htf_strength",
                "band_pct",
                "volatility_regime",
                "atr_ratio",
                "crisis_vol_score",
                "near_edge",
                "dist_to_edge_pct",
                "vol_z_source",
            ):
                val = stats.get(key)
                if val is not None:
                    meta[key] = val
        symbol = (
            stage1_signal.get("symbol") if isinstance(stage1_signal, dict) else None
        )
        market_context: dict[str, Any] = {"meta": meta, "symbol": symbol}

        # Легка інтеграція фазо-детектора через адаптер (телеметрія-only)
        try:
            phase_payload = (
                detect_phase_from_stats(stats or {}, symbol=symbol or "unknown")
                if isinstance(stats, dict)
                else None
            )
            if isinstance(phase_payload, dict):
                market_context.update(
                    {
                        "phase": phase_payload.get("name"),
                        "phase_score": phase_payload.get("score"),
                        "phase_reasons": phase_payload.get("reasons"),
                        "phase_profile": phase_payload.get("profile"),
                        "phase_threshold_tag": phase_payload.get("threshold_tag"),
                    }
                )
                overlay = phase_payload.get("overlay")
                if isinstance(overlay, dict) and overlay:
                    market_context["overlay"] = overlay

                # Візуальний інсайт для тихого режиму: market_context.meta.insights.quiet_mode
                if phase_payload.get("name") == "drift_trend":
                    insights = meta.setdefault("insights", {})
                    insights["quiet_mode"] = True
                    try:
                        insights["quiet_score"] = float(
                            phase_payload.get("score") or 0.0
                        )
                    except Exception:
                        pass
        except Exception:
            # Реплей має бути стійким до помилок у телеметрії
            pass

        return {
            "symbol": symbol,
            "market_context": market_context,
            "confidence_metrics": {},
        }
