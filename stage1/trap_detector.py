"""TRAP‑детектор (Trend Reversal Acceleration Point) — спрощений каркас.

Призначення:
    • Мінімальна функція для телеметрійної фіксації потенційної точки прискорення розвороту.
    • Враховує кілька умов; активує TRAP, якщо виконано ≥ 3.

Зауваження:
    • Каркасна реалізація. Джерела ключових рівнів/структури ринку слід під'єднати пізніше
      (LevelManager або CorridorMeta через Stage2), зараз — використовує проксі-поля зі stats.

TODO:
    - Перенести пороги та константи в config/config.py.
    - Підключити LevelManager для коректних перевірок на прорив рівнів.
    - Додати метрики (Prometheus/JSONL) для спостереження за частотою TRAP.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("app.stage1.trap")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

# Пороги з конфігурації (з безпечними дефолтами)
try:  # pragma: no cover — конфіг може бути недоступний у деяких середовищах тестів
    from config.config import STAGE1_TRAP as _TRAP_CFG  # type: ignore
except Exception:  # pragma: no cover
    _TRAP_CFG = {
        "min_dist_to_edge_pct": 0.05,
        "slope_significant": 0.50,
        "acceleration_threshold": 0.60,
        "volume_z_threshold": 2.50,
        "atr_pct_spike_ratio": 2.00,
    }

try:  # pragma: no cover
    from config.config import STAGE2_PROFILE, STRICT_PROFILE_ENABLED, STRONG_REGIME
except Exception:  # pragma: no cover
    STAGE2_PROFILE = "legacy"
    STRICT_PROFILE_ENABLED = False
    STRONG_REGIME = {}

if STRICT_PROFILE_ENABLED and STAGE2_PROFILE == "strict":
    _strict_trap_cfg = dict((STRONG_REGIME or {}).get("trap_thresholds", {}) or {})
else:
    _strict_trap_cfg = {}

_trap_active_cfg = dict(_TRAP_CFG or {})
# У strict-профілі пороги затягуються значеннями зі STRONG_REGIME
_trap_active_cfg.update(_strict_trap_cfg)

MIN_DIST_TO_EDGE_PCT = float(_trap_active_cfg.get("min_dist_to_edge_pct", 0.05))
SLOPE_SIGNIFICANT = float(_trap_active_cfg.get("slope_significant", 0.50))
ACCELERATION_THRESHOLD = float(_trap_active_cfg.get("acceleration_threshold", 0.60))
VOLUME_Z_THRESHOLD = float(_trap_active_cfg.get("volume_z_threshold", 2.50))
ATR_PCT_SPIKE_RATIO = float(_trap_active_cfg.get("atr_pct_spike_ratio", 2.00))

# Опис причин для зручності логування (не змінювати канонічні теги)
REASON_DESCRIPTIONS: dict[str, str] = {
    "key_level_break_proxy": "Проксі-прорив ключового рівня (dist_to_edge_pct мале + значний slope)",
    "volatility_spike": "Сплеск волатильності (atr% значно вище звичного)",
    "volume_spike": "Сплеск обсягу (volume_z велике)",
    "acceleration_detected": "Прискорення ціни (slope зараз >> slope_prev)",
}


def _log_reason_addition(reason_key: str, details: str | None = None) -> None:
    """Допоміжна функція — логування доданої причини україно-орієнтовано."""
    desc = REASON_DESCRIPTIONS.get(reason_key, reason_key)
    if details:
        logger.info("TRAP reason: %s — %s; details=%s", reason_key, desc, details)
    else:
        logger.info("TRAP reason: %s — %s", reason_key, desc)


def detect_trap_signals(
    price_data: Mapping[str, Any], volume_data: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Визначає TRAP‑сигнал за набором простих умов.

    Args:
        price_data: словник із Stage1 stats/фічами.
        volume_data: опційний словник з обʼємними метриками (можна передавати price_data).

    Returns:
        dict: {"trap_detected": bool, "reasons": list[str]}

    Пояснення:
        - Повертаються канонічні теги у полі "reasons" (не змінювати без узгодження).
        - Детальні пояснення для людей/логів зберігаються у REASON_DESCRIPTIONS.
    """
    logger.info("Запуск detect_trap_signals для symbol=%s", price_data.get("symbol"))
    # Компонуємо stats — дозволяємо перекривання volume_data поверх price_data
    stats = dict(price_data or {})
    if volume_data:
        stats.update(volume_data)

    reasons: list[str] = []
    # Для прозорості — збережемо проміжні метрики (ratios) для телеметрії
    dist_edge: float | None = None
    slope: float | None = None
    atr_pct: float | None = None
    atr_p50: float | None = None
    atr_ratio: float | None = None
    volz: float | None = None
    slope_now: float | None = None
    slope_prev: float | None = None

    # 1) Прорив ключових рівнів — проксі: dist_to_edge_pct дуже мала та знак руху
    try:
        dist_edge = float(stats.get("dist_to_edge_pct") or 1.0)
        slope = float(stats.get("price_slope_atr") or 0.0)
        logger.debug(
            "Оцінка key_level_break_proxy: dist_edge=%.6f, slope=%.6f", dist_edge, slope
        )
        if dist_edge <= MIN_DIST_TO_EDGE_PCT and abs(slope) > SLOPE_SIGNIFICANT:
            reasons.append("key_level_break_proxy")
            _log_reason_addition(
                "key_level_break_proxy", f"dist_edge={dist_edge:.6f}, slope={slope:.4f}"
            )
        # Додаткова причина: просто близькість до краю (без вимоги до схилу)
        try:
            near_edge_tag = stats.get("near_edge")
            # Врахуємо як додатковий контекст, якщо ми достатньо близько до краю
            if dist_edge <= (MIN_DIST_TO_EDGE_PCT * 1.5) and near_edge_tag in {
                "lower",
                "upper",
            }:
                reasons.append("near_edge")
                _log_reason_addition("near_edge", f"near={near_edge_tag}")
        except Exception:
            pass
    except Exception as exc:
        # Логувати деталі лише на DEBUG, щоб не забивати INFO
        logger.debug(
            "Помилка при перевірці key_level_break_proxy: %s", exc, exc_info=True
        )

    # 2) Сплеск волатильності: поточний ATR% >> звичного (безпечна база)
    try:
        atr = float(stats.get("atr") or 0.0)
        cp = float(stats.get("current_price") or 0.0)
        atr_pct = (atr / cp) if cp > 0 else 0.0
        atr_p50 = float(stats.get("atr_pct_p50") or 0.0)
        logger.debug(
            "Оцінка volatility_spike: atr=%.6f, current_price=%.6f, atr_pct=%.6f, atr_pct_p50=%.6f",
            atr,
            cp,
            atr_pct,
            atr_p50,
        )
        base = max(atr_p50, 1e-6)
        ratio = (atr_pct / base) if base > 0 else 0.0
        atr_ratio = ratio
        if ratio > ATR_PCT_SPIKE_RATIO:
            reasons.append("volatility_spike")
            _log_reason_addition(
                "volatility_spike",
                f"atr_pct={atr_pct:.6f}, atr_pct_p50={atr_p50:.6f}, ratio={ratio:.2f}",
            )
    except Exception as exc:
        logger.debug("Помилка при перевірці volatility_spike: %s", exc, exc_info=True)

    # 3) Сплеск обсягу: volz за хвилину значно вище середнього
    try:
        volz = float(stats.get("volume_z") or stats.get("volume_z_light") or 0.0)
        logger.debug("Оцінка volume_spike: volume_z=%.6f", volz)
        if volz > VOLUME_Z_THRESHOLD:
            reasons.append("volume_spike")
            _log_reason_addition("volume_spike", f"volume_z={volz:.3f}")
    except Exception as exc:
        logger.debug("Помилка при перевірці volume_spike: %s", exc, exc_info=True)

    # 4) Прискорення ціни (друга різниця за проксі): порівняння slope_now vs slope_prev
    try:
        slope_now = float(stats.get("price_slope_atr") or 0.0)
        slope_prev = float(stats.get("price_slope_atr_prev") or 0.0)
        logger.debug(
            "Оцінка acceleration_detected: slope_now=%.6f, slope_prev=%.6f",
            slope_now,
            slope_prev,
        )
        if abs(slope_now) > abs(slope_prev) and abs(slope_now) > ACCELERATION_THRESHOLD:
            reasons.append("acceleration_detected")
            _log_reason_addition(
                "acceleration_detected",
                f"slope_now={slope_now:.4f}, slope_prev={slope_prev:.4f}",
            )
    except Exception as exc:
        logger.debug(
            "Помилка при перевірці acceleration_detected: %s", exc, exc_info=True
        )

    # Нормований скор пастки: 0..1 (3+ причин → 1.0)
    try:
        trap_score = min(1.0, float(len(reasons)) / 3.0)
    except Exception:
        trap_score = 0.0
    # Порог для is_trap: вимагати >= ~2 підтвердження (0.67)
    trap_detected = bool(trap_score >= 0.67)
    # Фінальне логування (INFO)
    if trap_detected:
        logger.info(
            "TRAP detected for symbol=%s — reasons=%s (count=%d)",
            stats.get("symbol"),
            reasons,
            len(reasons),
        )
    else:
        logger.info(
            "No TRAP for symbol=%s — reasons=%s (count=%d)",
            stats.get("symbol"),
            reasons,
            len(reasons),
        )

    # Додатковий strict-лог для телеметрії
    try:
        logger.info(
            "[STRICT_TRAP] base=%.6f ratio=%.2f score=%.2f reasons=%s",
            max(atr_p50 or 0.0, 1e-6),
            (atr_ratio or 0.0),
            trap_score,
            reasons,
        )
    except Exception:
        pass

    # TODO: додати telemetry/event emission (наприклад, publish в Redis/metrics) тут або у Stage1 caller
    return {
        "trap_detected": trap_detected,
        "reasons": reasons,
        "reason_codes": list(reasons),  # дублюємо канонічні теги для явності
        "trap_score": trap_score,
        "ratios": {
            "dist_to_edge_pct": dist_edge,
            "slope": slope,
            "slope_now": slope_now,
            "slope_prev": slope_prev,
            "atr_pct": atr_pct,
            "atr_pct_p50": atr_p50,
            "atr_spike_ratio": atr_ratio,
            "volume_z": volz,
        },
    }
