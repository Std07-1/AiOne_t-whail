"""Індикатори Multi‑TF для Stage2 (телеметрійний каркас).

Призначення:
    • Надати легкий спосіб оцінки конгруентності тренду на кількох ТФ (1m/5m/15m/1h).
    • Працює у Phase 1 у телеметрійному режимі — лише повертає структуру для market_context.meta.

Зауваження:
    • Початковий каркас: використовує прості проксі з наявних stats/meta (напр., price_slope_*).
    • Всі пороги/параметри — з config.config; тут бізнес‑логіки немає.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import STAGE2_HTF_OFF_THRESH, STAGE2_HTF_ON_THRESH

logger = logging.getLogger("stage2.indicators")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class MultiTimeframeTrend:
    """Оцінка узгодженості тренду на кількох таймфреймах.

    Методика (спрощено, телеметрія):
        • Використовує проксі‑нахили/індикатори з stats/meta, якщо доступні.
        • Формує бінарні сигнали напрямку на TF {1m,5m,15m,1h} і силу у [0..1].
        • Повертає конгруентність, середню силу та єдиний напрямок.
    """

    def __init__(self) -> None:
        try:
            self.on_thr: float = float(STAGE2_HTF_ON_THRESH)
            self.off_thr: float = float(STAGE2_HTF_OFF_THRESH)
        except Exception:
            self.on_thr = 0.55
            self.off_thr = 0.45

    def _dir_strength(self, val: float | None) -> tuple[int | None, float | None]:
        if val is None:
            return None, None
        try:
            v = float(val)
        except Exception:
            return None, None
        if v >= self.on_thr:
            return 1, min(1.0, v)
        if v <= self.off_thr:
            return -1, min(1.0, 1.0 - v)
        return 0, 0.0

    def calculate_trend_confluence(
        self, meta_or_stats: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Розраховує конгруентність тренду по TF.

        Args:
            meta_or_stats: словник із Stage1/Stage2 metrics/meta (має містити htf_* якщо доступно).

        Returns:
            dict: {"confluence": bool, "direction": int|None, "strength": float}
        """
        src = meta_or_stats or {}
        # Проксі полів: чекатимемо наявність нормованих [0..1] показників напрямку/сили
        h1m = src.get("trend_1m") or src.get("htf_1m")
        h5m = src.get("trend_5m") or src.get("htf_5m")
        h15m = src.get("trend_15m") or src.get("htf_15m")
        h1h = src.get("trend_1h") or src.get("htf_1h")

        dirs: list[int] = []
        strg: list[float] = []
        for h in (h1m, h5m, h15m, h1h):
            d, s = self._dir_strength(float(h) if h is not None else None)
            if d is None or s is None:
                continue
            if d == 0:
                # Невизначений/боковик — розглядаємо як відсутність конгруентності
                return {"confluence": False, "direction": None, "strength": 0.0}
            dirs.append(d)
            strg.append(max(0.0, min(1.0, s)))

        if len(dirs) < 2:
            return {"confluence": False, "direction": None, "strength": 0.0}

        same_dir = all(d == dirs[0] for d in dirs)
        strength = float(sum(strg) / len(strg)) if strg else 0.0
        direction = dirs[0] if same_dir else None
        return {
            "confluence": bool(same_dir),
            "direction": direction,
            "strength": round(strength, 3),
        }


def impulse_score(
    slope_atr: float | None,
    dvr: float | None,
    cd: float | None,
) -> float:
    """Оцінка імпульсу 0..1 на основі slope_atr, DVR та CD.

    Args:
        slope_atr: нормований нахил ціни в ATR-одиницях (≈0..2).
        dvr: directional volume ratio (≈0..2; 1 — баланс).
        cd: кумулятивна дельта (≈-1..+1 у нормованому поданні).

    Returns:
        float: скор імпульсу у діапазоні [0.0, 1.0].

    Нотатки:
        - Безпечні приведення типів; пропуски → 0 внеску.
        - Ваги підібрані консервативно, щоб не впливати на Stage3 (телеметрія‑only).
    """
    try:
        s = max(0.0, min(1.0, float(slope_atr or 0.0)))
    except Exception:
        s = 0.0
    try:
        # DVR: відхилення від 1.0 (балансу) з обмеженням
        dev = abs(float(dvr or 1.0) - 1.0)
        dv = max(0.0, min(1.0, dev / 1.0))  # dev≈1 ⇒ 1.0
    except Exception:
        dv = 0.0
    try:
        c = max(0.0, min(1.0, abs(float(cd or 0.0))))
    except Exception:
        c = 0.0

    # Консервативний ваговий мікс; легко тюнити у майбутньому
    score = 0.5 * s + 0.3 * dv + 0.2 * c
    return round(float(max(0.0, min(1.0, score))), 3)
