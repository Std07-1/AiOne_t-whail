from __future__ import annotations

import logging
import sys
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.constants import (
    K_CUMULATIVE_DELTA,
    K_DIRECTIONAL_VOLUME_RATIO,
    K_PRICE_SLOPE_ATR,
)
from config.flags import ROUTER_ALERT_REQUIRE_DOMINANCE

try:  # noqa: SIM105 - best-effort імпорт
    from telemetry.prom_gauges import (  # type: ignore
        inc_whale_no_payload as _inc_whale_no_payload,
    )
except Exception:  # pragma: no cover - телеметрія може бути вимкнена
    _inc_whale_no_payload = None  # type: ignore[assignment]

logger = logging.getLogger("router_signal_v2")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    # Пишемо у stdout, щоб tee у tools.run_window коректно дзеркалив у файл UTF-8
    logger.addHandler(RichHandler(console=Console(file=sys.stdout), show_path=True))
    logger.propagate = False


def router_signal_v2(
    stats: dict[str, Any], profile_cfg: dict[str, Any]
) -> tuple[str, float, list[str]]:
    """Обчислює signal_v2 на базі stats (whale + directional контекст).

    Args:
        stats: Контейнер статистик (очікує stats.whale, band_pct, near_edge, rsi, price_slope_atr, dvr, cd).
        profile_cfg: Конфіг роутера для профілю (див. STAGE2_SIGNAL_V2_PROFILES[STAGE2_PROFILE]).

    Returns:
        (sig, conf, reasons)
    """
    st = stats
    wh = st.get("whale") or {}
    presence_raw = wh.get("presence")
    presence: float | None
    if isinstance(presence_raw, (int, float)):
        try:
            presence = float(presence_raw)
        except Exception:
            presence = None
    else:
        presence = None
    missing = bool(wh.get("missing"))
    bias = float(wh.get("bias") or 0.0)
    vdev = float(wh.get("vwap_dev") or 0.0)
    zones = wh.get("zones_summary") or {}
    accum = int(zones.get("accum_cnt") or 0)
    dist = int(zones.get("dist_cnt") or 0)
    stale = bool(wh.get("stale"))

    band = float(st.get("band_pct") or 1.0)
    near = st.get("near_edge")
    rsi = float(st.get("rsi") or 50.0)
    slope = float(st.get(K_PRICE_SLOPE_ATR) or st.get("price_slope_atr") or 0.0)
    dvr = float(st.get(K_DIRECTIONAL_VOLUME_RATIO) or 1.0)
    cd = float(st.get(K_CUMULATIVE_DELTA) or st.get("cumulative_delta") or 0.0)

    # Логування вхідних значень
    symbol = st.get("symbol") or st.get("market") or "-"
    ts = st.get("ts") or "-"
    pres_log = presence if presence is not None else float("nan")
    logger.info(
        "[STRICT_WHALE] %s %s вхідні дані router_signal_v2 presence=%.3f bias=%.3f vdev=%.4f band=%.4f near=%s rsi=%.1f slope=%.3f dvr=%.3f cd=%.3f",
        symbol,
        ts,
        pres_log,
        bias,
        vdev,
        band,
        near,
        rsi,
        slope,
        dvr,
        cd,
    )

    sig = "AVOID"
    conf = 0.0
    reasons: list[str] = []
    if missing:
        sig = "OBSERVE"
        reasons.append("missing")
        if callable(_inc_whale_no_payload):
            try:
                sym_up = str(symbol).upper()
                if sym_up and sym_up != "-":
                    _inc_whale_no_payload(sym_up)
            except Exception:
                pass
    if presence is None and "missing" not in reasons:
        sig = "OBSERVE"
        reasons.append("no_whale")
        if callable(_inc_whale_no_payload):
            try:
                sym_up = str(symbol).upper()
                if sym_up and sym_up != "-":
                    _inc_whale_no_payload(sym_up)
            except Exception:
                pass
    if stale:
        sig = "OBSERVE"
        reasons.append("stale")

    # Придатні для логів ворота домінування
    dom = (wh.get("dominance") or {}) if isinstance(wh, dict) else {}
    dom_buy = bool(dom.get("buy"))
    dom_sell = bool(dom.get("sell"))

    if sig == "AVOID":
        lg = profile_cfg.get("long", {})
        sh = profile_cfg.get("short", {})
        # Long кандидат
        long_cond = (
            band <= float(lg.get("band_max", 0.02))
            and near == "upper"
            and float(lg.get("rsi_lo", 45.0)) <= rsi <= float(lg.get("rsi_hi", 65.0))
            and slope >= float(lg.get("slope_min", 0.5))
            and cd >= float(lg.get("cd_min", 0.2))
            and dvr <= float(lg.get("dvr_max", 0.8))
        )
        if long_cond:
            logger.info(
                "[STRICT_WHALE] %s %s кандидат LONG presence=%.3f bias=%.3f vdev=%.4f accum=%d dom_buy=%s",
                symbol,
                ts,
                presence,
                bias,
                vdev,
                accum,
                dom_buy,
            )
            if (
                presence >= float(lg.get("presence_alert_min", 0.65))
                and vdev >= float(lg.get("vdev_alert_min", 0.01))
                and accum >= int(lg.get("zones_accum_min_alert", 3))
            ):
                # Кандидат на ALERT_BUY; за флагом можемо вимагати dominance.buy
                if ROUTER_ALERT_REQUIRE_DOMINANCE and not dom_buy:
                    # Понижуємо до SOFT_BUY без зміни базового soft-контракту
                    sig = "SOFT_BUY"
                    conf = min(
                        1.0,
                        0.4 * presence + 0.3 * max(0.0, bias) + 10.0 * max(0.0, vdev),
                    )
                    reasons = [
                        "band_narrow",
                        "near_upper",
                        "slope_strong",
                        "cd_ok",
                        "dvr_ok",
                        "presence_ok",
                        "bias_ok",
                        "vdev_ok",
                        "accum_zones>=thr",
                    ]
                    logger.info(
                        "[STRICT_GUARD] %s %s вимога ALERT_BUY: dominance відсутня -> понижено до SOFT_BUY conf=%.3f",
                        symbol,
                        ts,
                        conf,
                    )
                else:
                    sig = "ALERT_BUY"
                    conf = min(
                        1.0,
                        0.5 * presence + 0.3 * max(0.0, bias) + 20.0 * max(0.0, vdev),
                    )
                    reasons = [
                        "band_narrow",
                        "near_upper",
                        "slope_strong",
                        "cd_ok",
                        "dvr_ok",
                        "presence_strong",
                        "vdev_strong",
                        "accum_zones>=thr",
                    ]
                    logger.info(
                        "[STRICT_WHALE] %s %s обрано ALERT_BUY conf=%.3f dom_buy=%s",
                        symbol,
                        ts,
                        conf,
                        dom_buy,
                    )
            elif (
                presence >= float(lg.get("presence_soft_min", 0.5))
                and bias >= float(lg.get("bias_soft_min", 0.25))
                and vdev >= float(lg.get("vdev_soft_min", 0.005))
                and accum >= int(lg.get("zones_accum_min", 3))
            ):
                sig = "SOFT_BUY"
                conf = min(1.0, 0.4 * presence + 0.3 * bias + 10.0 * vdev)
                reasons = [
                    "band_narrow",
                    "near_upper",
                    "slope_strong",
                    "cd_ok",
                    "dvr_ok",
                    "presence_ok",
                    "bias_ok",
                    "vdev_ok",
                    "accum_zones>=thr",
                ]
                logger.info(
                    "[STRICT_WHALE] %s %s обрано SOFT_BUY conf=%.3f",
                    symbol,
                    ts,
                    conf,
                )
        # Short кандидат
        if sig == "AVOID":
            short_cond = (
                band <= float(sh.get("band_max", 0.02))
                and near == "lower"
                and float(sh.get("rsi_lo", 35.0))
                <= rsi
                <= float(sh.get("rsi_hi", 55.0))
                and slope <= -float(sh.get("slope_abs_min", 0.5))
                and cd <= -float(sh.get("cd_abs_min", 0.2))
                and dvr <= float(sh.get("dvr_max", 0.8))
            )
            if short_cond:
                logger.info(
                    "[STRICT_WHALE] %s %s кандидат SHORT presence=%.3f bias=%.3f vdev=%.4f dist=%d dom_sell=%s",
                    symbol,
                    ts,
                    presence,
                    bias,
                    vdev,
                    dist,
                    dom_sell,
                )
                if (
                    presence >= float(sh.get("presence_alert_min", 0.65))
                    and (-vdev) >= float(sh.get("vdev_alert_min", 0.01))
                    and dist >= int(sh.get("zones_dist_min_alert", 3))
                ):
                    sig = "ALERT_SELL"
                    conf = min(
                        1.0,
                        0.5 * presence + 0.3 * max(0.0, -bias) + 20.0 * max(0.0, -vdev),
                    )
                    reasons = [
                        "band_narrow",
                        "near_lower",
                        "slope_strong_neg",
                        "cd_ok_neg",
                        "dvr_ok",
                        "presence_strong",
                        "vdev_strong_neg",
                        "dist_zones>=thr",
                    ]
                    logger.info(
                        "[STRICT_WHALE] %s %s обрано ALERT_SELL conf=%.3f",
                        symbol,
                        ts,
                        conf,
                    )
                elif (
                    presence >= float(sh.get("presence_soft_min", 0.5))
                    and (-bias) >= float(sh.get("bias_soft_min", 0.25))
                    and (-vdev) >= float(sh.get("vdev_soft_min", 0.005))
                    and dist >= int(sh.get("zones_dist_min", 3))
                ):
                    sig = "SOFT_SELL"
                    conf = min(1.0, 0.4 * presence + 0.3 * (-bias) + 10.0 * (-vdev))
                    reasons = [
                        "band_narrow",
                        "near_lower",
                        "slope_strong_neg",
                        "cd_ok_neg",
                        "dvr_ok",
                        "presence_ok",
                        "bias_ok_neg",
                        "vdev_ok_neg",
                        "dist_zones>=thr",
                    ]
                    logger.info(
                        "[STRICT_WHALE] %s %s обрано SOFT_SELL conf=%.3f",
                        symbol,
                        ts,
                        conf,
                    )

    # Підсумкове логування рішення
    logger.info(
        "[STRICT_WHALE] %s %s результат sig=%s conf=%.3f reasons=%s presence=%.3f bias=%.3f gates=dominance_buy:%s dominance_sell:%s",
        symbol,
        ts,
        sig,
        conf,
        reasons,
        pres_log,
        bias,
        dom_buy,
        dom_sell,
    )

    return sig, float(round(conf, 3)), reasons
