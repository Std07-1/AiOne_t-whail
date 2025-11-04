"""Перетворення Phase/Whale-телеметрії у «м'які» Stage2-сигнали.

Контракти:
- Вхід: один словник події (наприклад, "phase_detected") з полями phase/score/htf_*/whale_*.
- Вихід: Stage2-подібний об'єкт (ключі не змінюють публічних контрактів),
  нові поля додаються лише у market_context.meta.* або confidence_metrics.

Примітки:
- Тонкий адаптер над whale.insight_builder: тут немає дублювання китової логіки.
- «Софт» рекомендації не впливають на базову логіку Stage1/Stage2; призначені для UI/телеметрії.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Literal

from rich.console import Console  # type: ignore
from rich.logging import RichHandler  # type: ignore

# Використовуємо готову логіку класифікації/порад із InsightBuilder
from whale import insight_builder

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("phase_signalizer")
if not logger.handlers:  # guard від подвійного підключення
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False

Direction = Literal["LONG", "SHORT"] | None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _as_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        logger.warning(f"[STRICT_SANITY] Неможливо привести до float: {v!r}")
        return default


def _derive_direction(event: dict[str, Any]) -> Direction:
    """Визначення напрямку: мінімальна евристика за знаком whale_bias.

    Примітка: уникаємо дублювання складної логіки — деталізація в InsightBuilder.
    """
    bias = _as_float(event.get("whale_bias"), 0.0)
    logger.debug(f"[STRICT_PHASE] derive_direction bias={bias:.3f}")
    if bias > 0.01:
        return "LONG"
    if bias < -0.01:
        return "SHORT"
    return None


def _confidence(event: dict[str, Any], overlay: Mapping[str, Any]) -> float:
    """Оцінка впевненості (0..1): комбінує локальний score з insight‑score.

    - Локальний base: event["score"], HTF/whale presence.
    - Insight: opportunity_risk_score(overlay) — вже враховує імпульс/ризики та
      fallback‑логіку з китів (через конфіг), тож не дублюємо розрахунки.
    """
    base = _clamp(_as_float(event.get("score"), 0.0), 0.0, 1.0)
    presence = _clamp(_as_float(event.get("whale_presence"), 0.0), 0.0, 1.0)
    htf_strength = abs(_as_float(event.get("htf_strength"), 0.0))
    htf_norm = _clamp(htf_strength / 0.05, 0.0, 1.0)

    insight = 0.0
    try:
        insight = float(insight_builder.opportunity_risk_score(overlay))
    except Exception as ex:
        logger.warning(f"[STRICT_SANITY] Помилка opportunity_risk_score: {ex!r}")
        insight = 0.0

    # Вага: більше довіряємо базовим фазам/HTF, але підсилюємо інсайтом
    conf = 0.45 * base + 0.25 * presence + 0.15 * htf_norm + 0.15 * insight

    # Понижуючі фактори: старі китові дані або hyper‑волатильність
    if bool(event.get("whale_stale")):
        logger.info("[STRICT_WHALE] whale_stale=True, пониження conf")
        conf *= 0.7
    if str(event.get("whale_vol_regime") or "").lower() == "hyper":
        logger.info("[STRICT_VOLZ] whale_vol_regime=hyper, пониження conf")
        conf *= 0.85
    conf = _clamp(conf, 0.0, 1.0)
    logger.debug(
        f"[STRICT_PHASE] confidence={conf:.3f} base={base:.3f} presence={presence:.3f} htf_norm={htf_norm:.3f} insight={insight:.3f}"
    )
    return conf


def _build_overlay(event: Mapping[str, Any]) -> dict[str, Any]:
    """Побудова overlay для InsightBuilder з наявних полів події.

    Враховуємо тільки доступне: імпульс/ризики, а також китову секцію для fallback.
    """

    def gf(key: str, default: float = 0.0) -> float:
        try:
            return float(event.get(key)) if event.get(key) is not None else default
        except Exception:
            logger.warning(
                f"[STRICT_SANITY] Неможливо привести overlay {key} до float: {event.get(key)!r}"
            )
            return default

    overlay: dict[str, Any] = {
        # Імпульс/ризики (якщо присутні в події)
        "impulse_up": gf("impulse_up", 0.0),
        "impulse_down": gf("impulse_down", 0.0),
        "crisis_vol_score": gf("crisis_vol_score", 0.0),
        "trap_score": gf("trap_score", 0.0),
        # Whale fallback поля — InsightBuilder сам застосує політику fallback_from_whales
        "whale_presence": gf("whale_presence", 0.0),
        "whale_bias": gf("whale_bias", 0.0),
        "vwap_dev": gf("whale_vwap_dev", 0.0),
        "vol_regime": (event.get("whale_vol_regime") or ""),
        "accum_zones_count": int(event.get("whale_accum_cnt") or 0),
        "dist_zones_count": int(event.get("whale_dist_cnt") or 0),
    }
    logger.debug(f"[STRICT_PHASE] overlay={overlay}")
    return overlay


def event_to_soft_signal(event: dict[str, Any]) -> dict[str, Any]:
    """Мапінг Phase/Whale-події у Stage2-подібний «софт»-сигнал.

    Args:
        event: Телеметрія однієї події (наприклад, запис із "phase_detected").

    Returns:
        dict: Об'єкт із ключами Stage2Output (контракт не порушено),
            рекомендація типу SOFT_BUY/SELL/OBSERVE, market_context.meta.insights з деталями.
    """
    phase = str(event.get("phase") or "").lower()
    overlay = _build_overlay(event)
    dir_: Direction = _derive_direction(event)
    conf = _confidence(event, overlay)
    price = _as_float(event.get("current_price"), 0.0)
    atr = max(0.0, _as_float(event.get("atr"), 0.0))

    # Quiet mode (participation-light): фіксуємо як інсайт
    quiet_mode = phase == "drift_trend"

    # --- Делегуємо класифікацію у InsightBuilder ---
    try:
        decision = insight_builder.classify_enriched(
            {
                "scenario": str(event.get("scenario") or phase).upper(),
                "confidence": conf,
                "rr_live": event.get("rr_live"),
                "atr_ratio": event.get("atr_ratio"),
                "trap_score": overlay.get("trap_score"),
                "whale_bias": overlay.get("whale_bias"),
                "overlay": overlay,
            }
        )
    except Exception as ex:
        logger.warning(f"[STRICT_SANITY] classify_enriched помилка: {ex!r}")
        decision = "OBSERVE"

    # Мапінг рішення у «софт» рекомендації (напрямок — за whale_bias знаком)
    # Додаткові guard'и політик:
    #  - stale китові дані → тільки OBSERVE
    #  - hyper волатильність → блокувати еміт «ALERT_*»/«SOFT_*» → OBSERVE
    vol_reg = str(event.get("whale_vol_regime") or "").lower()
    whale_stale = bool(event.get("whale_stale"))
    if whale_stale or vol_reg == "hyper" or (not bool(event.get("htf_ok", False))):
        logger.info(
            f"[STRICT_GUARD] symbol={event.get('symbol')} ts={event.get('ts')} phase={phase} whale_stale={whale_stale} vol_reg={vol_reg} htf_ok={event.get('htf_ok', False)} → OBSERVE"
        )
        reco = "OBSERVE"
    else:
        if decision == "TRADEABLE" and dir_ is not None:
            reco = "SOFT_BUY" if dir_ == "LONG" else "SOFT_SELL"
        else:
            reco = "OBSERVE"
    logger.info(
        f"[STRICT_PHASE] symbol={event.get('symbol')} ts={event.get('ts')} phase={phase} reco={reco} conf={conf:.3f} dir={dir_} decision={decision}"
    )

    # Ризик-параметри (прикладні, «м'які»)
    rr_ratio = 2.0
    sl_mult = 1.5
    tp_mult = 2.5
    if price > 0 and atr > 0 and dir_ in ("LONG", "SHORT"):
        if dir_ == "LONG":
            stop_loss = price - sl_mult * atr
            take_profit = price + tp_mult * atr
        else:
            stop_loss = price + sl_mult * atr
            take_profit = price - tp_mult * atr
    else:
        stop_loss = None
        take_profit = None

    # Narrative (людинозрозуміле пояснення)
    # Побудуємо коротку narrative з порад InsightBuilder
    try:
        advice = insight_builder.next_actions_enriched(
            scenario=str(event.get("scenario") or phase).upper(),
            strategy=str(event.get("strategy") or ""),
            whale_bias=overlay.get("whale_bias"),
            overlay=overlay,
            rr_live=event.get("rr_live"),
            atr_ratio=event.get("atr_ratio"),
        )
    except Exception as ex:
        logger.warning(f"[STRICT_SANITY] next_actions_enriched помилка: {ex!r}")
        advice = []

    if advice:
        narrative = "; ".join(advice[:3])  # лаконічно
    else:
        narrative = _build_narrative(event, dir_, conf, quiet_mode)

    whale_meta = {
        "presence": _as_float(event.get("whale_presence"), 0.0),
        "bias": _as_float(event.get("whale_bias"), 0.0),
        "vwap_dev": _as_float(event.get("whale_vwap_dev"), 0.0),
        "vol_regime": event.get("whale_vol_regime"),
        "age_ms": event.get("whale_age_ms"),
        "stale": bool(event.get("whale_stale")),
        "accum_cnt": event.get("whale_accum_cnt"),
        "dist_cnt": event.get("whale_dist_cnt"),
    }

    market_context = {
        "triggers": [],  # тригери Stage1 відсутні у цьому режимі
        "nearest_support": None,
        "nearest_resistance": None,
        "meta": {
            "insights": {
                "quiet_mode": quiet_mode,
                "quiet_score": (
                    _as_float(event.get("score"), 0.0) if quiet_mode else None
                ),
                "direction": dir_,
                "phase": phase,
                "phase_score": _as_float(event.get("score"), 0.0),
                "near_edge": event.get("near_edge"),
                "band_pct": _as_float(event.get("band_pct"), 0.0),
                "htf_ok": bool(event.get("htf_ok", False)),
                "htf_strength": _as_float(event.get("htf_strength"), 0.0),
                "whale": whale_meta,
                "insight_category": decision,
                "advice": advice,
            }
        },
    }

    strategy_hint = event.get("strategy_hint")
    if isinstance(strategy_hint, str) and strategy_hint:
        meta_section = market_context.get("meta", {})
        insights_section = meta_section.get("insights")
        if isinstance(insights_section, dict):
            insights_section["strategy_hint"] = strategy_hint
        meta_section["strategy_hint"] = strategy_hint
        meta_section.setdefault("strategy", strategy_hint)
        market_context["meta"] = meta_section

    confidence_metrics = {
        "confidence": conf,
        "prob_upside": (
            conf if dir_ == "LONG" else (1.0 - conf if dir_ == "SHORT" else 0.5)
        ),
    }

    risk_parameters = {
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "rr_ratio": rr_ratio,
    }

    anomaly_detection = {"volume_outlier": False}

    symbol = event.get("symbol")
    ts = str(event.get("ts") or event.get("timestamp_iso") or "")

    logger.debug(
        f"[STRICT_PHASE] symbol={symbol} ts={ts} reco={reco} narrative={narrative!r} conf={conf:.3f}"
    )

    return {
        "symbol": symbol,
        "market_context": market_context,
        "recommendation": reco,
        "narrative": narrative,
        "confidence_metrics": confidence_metrics,
        "risk_parameters": risk_parameters,
        "anomaly_detection": anomaly_detection,
        "timestamp": ts,
    }


def _build_narrative(
    event: dict[str, Any], dir_: Direction, conf: float, quiet: bool
) -> str:
    sym = str(event.get("symbol") or "?")
    phase = str(event.get("phase") or "?")
    presence = _as_float(event.get("whale_presence"), 0.0)
    bias = _as_float(event.get("whale_bias"), 0.0)
    near = str(event.get("near_edge") or "none")
    volreg = str(event.get("whale_vol_regime") or "?")
    htf_ok = bool(event.get("htf_ok", False))

    if not htf_ok:
        logger.info(
            f"[STRICT_GUARD] {sym}: HTF не підтверджує {phase}; спостереження без дій."
        )
        return f"{sym}: HTF не підтверджує {phase}; спостереження без дій."

    if quiet:
        logger.info(
            f"[STRICT_PHASE] {sym}: тихий тренд ({phase}), whale присутність {presence:.2f}, упередженість {bias:+.2f}, край діапазону: {near}; конфіденс {conf:.2f}."
        )
        return (
            f"{sym}: тихий тренд ({phase}), whale присутність {presence:.2f}, упередженість {bias:+.2f},"
            f" край діапазону: {near}; конфіденс {conf:.2f}."
        )

    if dir_ == "LONG":
        logger.info(
            f"[STRICT_PHASE] {sym}: LONG {phase}, whale {presence:.2f}, bias {bias:+.2f}, vol {volreg}, край {near}, conf {conf:.2f}"
        )
        return (
            f"{sym}: фаза {phase}, підтверджено HTF, whale присутність {presence:.2f},"
            f" упередженість {bias:+.2f}; волатильність {volreg}; край: {near}; конфіденс {conf:.2f}."
        )
    if dir_ == "SHORT":
        logger.info(
            f"[STRICT_PHASE] {sym}: SHORT {phase}, whale {presence:.2f}, bias {bias:+.2f}, vol {volreg}, край {near}, conf {conf:.2f}"
        )
        return (
            f"{sym}: фаза {phase}, підтверджено HTF, whale присутність {presence:.2f},"
            f" упередженість {bias:+.2f}; волатильність {volreg}; край: {near}; конфіденс {conf:.2f}."
        )
    logger.info(
        f"[STRICT_PHASE] {sym}: фаза {phase}, напрямок не визначено; whale присутність {presence:.2f}, упередженість {bias:+.2f}; край: {near}; конфіденс {conf:.2f}."
    )
    return (
        f"{sym}: фаза {phase}, напрямок не визначено; whale присутність {presence:.2f},"
        f" упередженість {bias:+.2f}; край: {near}; конфіденс {conf:.2f}."
    )
