from __future__ import annotations

import logging
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("structure_map")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.INFO)
    # show_path=True для відображення файлу/рядка у WARN/ERROR
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False  # Не пропагувати далі, щоб уникнути дублювання логів


def build_structure_map(
    stats: dict[str, Any] | None,
    whale_zones: dict[str, Any] | None = None,
    lookback: int = 500,
) -> dict[str, Any]:
    """StructureMap v1 — легка карта зон.

    Вихід для market_context.meta.structure:
      - zones: агрегатний сніпет по доступних зонах (якщо надійшли)
      - distance_to_next_zone: best-effort відстань (0 біля краю, None якщо невідомо)
      - zone_touch_count: best-effort лічильник дотиків (0/1 на поточному барі)
      - last_touch_age: 0, якщо біля межі; інакше None (без історії)
    """
    logger.info("[STRUCTURE_MAP] Початок побудови карти зон")
    st = stats or {}
    wz = whale_zones or {}

    # distance_to_next_zone: якщо є near_edge -> 0.0, інакше None (поки без розрахунку pips)
    near = _is_near_edge(st.get("near_edge"))
    distance = 0.0 if near else None
    logger.debug(f"[STRUCTURE_MAP] Відстань до зони: {distance}, near_edge: {near}")

    # touch_count/lta — без історії лише двійковий сигнал
    touch_count = 1 if near else 0
    last_touch_age = 0 if near else None
    logger.debug(
        f"[STRUCTURE_MAP] Лічильник дотиків: {touch_count}, вік останнього дотику: {last_touch_age}"
    )

    # Передамо компактну зведену структуру зон (якщо доступно)
    zones_snippet: dict[str, Any] = {}
    if isinstance(wz, dict) and wz:
        for k in ("key_levels", "risk_levels"):
            if k in wz:
                zones_snippet[k] = wz[k]
        logger.debug(f"[STRUCTURE_MAP] Зведені зони: {zones_snippet}")
    else:
        logger.debug("[STRUCTURE_MAP] Зони не доступні або порожні")

    result = {
        "zones": zones_snippet,
        "distance_to_next_zone": distance,
        "zone_touch_count": touch_count,
        "last_touch_age": last_touch_age,
    }
    logger.info("[STRUCTURE_MAP] Карта зон побудована успішно")
    return result


def _is_near_edge(v: Any) -> bool:
    try:
        if v is True:
            logger.debug("[STRUCTURE_MAP] near_edge: True")
            return True
        if isinstance(v, str):
            result = v.lower() in {"upper", "lower", "near", "true"}
            logger.debug(f"[STRUCTURE_MAP] near_edge рядок '{v}': {result}")
            return result
        logger.debug(f"[STRUCTURE_MAP] near_edge не розпізнано: {v}")
        return False
    except Exception as e:
        logger.warning(f"[STRUCTURE_MAP] Помилка у _is_near_edge: {e}")
        return False
