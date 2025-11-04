from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("whale_telemetry_scoring")
if not logger.handlers:  # guard
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

try:
    from config.config_whale import STAGE2_WHALE_TELEMETRY  # type: ignore
except Exception:  # pragma: no cover
    STAGE2_WHALE_TELEMETRY = {}  # type: ignore[assignment]

try:
    from config.config import (  # type: ignore
        PRESENCE_CAPS,
        STRICT_ACCUM_CAPS_ENABLED,
        STRONG_REGIME,
        WHALE_SCORING_V2_ENABLED,
    )
except Exception:  # pragma: no cover
    PRESENCE_CAPS = {}
    STRICT_ACCUM_CAPS_ENABLED = False
    STRONG_REGIME = {}
    WHALE_SCORING_V2_ENABLED = False


class WhaleTelemetryScoring:
    """Утиліти для обчислення телеметрії whale-присутності та домінування."""

    @staticmethod
    def presence_score_from(
        strats: dict[str, Any] | None, zones: dict[str, Any] | None
    ) -> float:
        """Обчислити presence_score (0..1) з детальним логуванням.

        Args:
            strats: результат InstitutionalEntryMethods.detect_vwap_twap_strategies
                (очікувані ключі: vwap_buying, vwap_selling, twap_accumulation, iceberg_orders, vwap_deviation)
            zones: результат SupplyDemandZones.identify_institutional_levels
                (очікувані ключі: key_levels {accumulation_zones, distribution_zones, liquidity_pools},
                 risk_levels {accumulation, distribution, risk_from_zones})

        Returns:
            float: інтегральний скор в діапазоні [0.0, 1.0].

        Notes:
            - Метод захищений від помилок у вхідних даних — у разі проблем повертає 0.0..1.0.
            - Логи: INFO — загальний підсумок; DEBUG — проміжні компоненти скору.
        """
        logger.info(
            "WhaleTelemetryScoring.presence_score_from: початок обчислення presence_score"
        )
        logger.debug(
            "WhaleTelemetryScoring.presence_score_from: входи strats_keys=%s zones_keys=%s",
            list(strats.keys()) if isinstance(strats, dict) else None,
            list(zones.keys()) if isinstance(zones, dict) else None,
        )

        cfg: dict[str, Any] = dict(STAGE2_WHALE_TELEMETRY or {})
        caps_cfg = PRESENCE_CAPS if isinstance(PRESENCE_CAPS, Mapping) else {}
        try:
            zones_none_cap = float(caps_cfg.get("accum_only_cap", 0.30) or 0.30)
        except Exception:
            zones_none_cap = 0.30
        try:
            accum_only_cap = float(caps_cfg.get("accum_only_cap", 0.35) or 0.35)
        except Exception:
            accum_only_cap = 0.35

        def _safe_size(val: Any) -> int:
            try:
                if isinstance(val, (list, tuple, set)):
                    return int(len(val))
                if val is None:
                    return 0
                return int(val)
            except Exception:
                return 0

        try:
            if not cfg:
                logger.debug(
                    "presence: STAGE2_WHALE_TELEMETRY порожній, використовуємо дефолтні ваги",
                )

            weights_cfg = cfg.get("presence_weights")
            # Updated defaults to be slightly more sensitive to TWAP/zones
            w_vwap = float((weights_cfg or {}).get("vwap", 0.25))
            w_twap = float((weights_cfg or {}).get("twap", 0.20))
            w_iceb = float((weights_cfg or {}).get("iceberg", 0.20))
            w_accum = float((weights_cfg or {}).get("accum", 0.10))
            w_dist = float((weights_cfg or {}).get("dist", 0.20))
            max_zones = max(1, int(cfg.get("max_zones_contrib", 4) or 4))
            strong_dev_thr = float(cfg.get("strong_dev_thr", 0.02) or 0.02)
            # relaxed vwap deviation threshold for flagging VWAP-based presence
            vwap_flag_dev_thr = float(cfg.get("vwap_flag_dev_thr", 0.005) or 0.005)
            dev_bonus = float(cfg.get("dev_bonus", 0.10) or 0.10)
            iceberg_cap = float(cfg.get("iceberg_only_cap", 0.40) or 0.40)
            try:
                zones_none_cap = float(
                    cfg.get("presence_zones_none_cap", zones_none_cap) or zones_none_cap
                )
            except Exception:
                zones_none_cap = 0.30
            try:
                accum_only_cap = float(
                    cfg.get("presence_accum_only_cap", accum_only_cap) or accum_only_cap
                )
            except Exception:
                accum_only_cap = 0.35

            zones_total_cap: float | None = None
            strict_cfg: dict[str, Any] = (
                dict((STRONG_REGIME or {}).get("whale_scoring", {}) or {})
                if WHALE_SCORING_V2_ENABLED
                else {}
            )

            if strict_cfg:
                # allow strict regime to override weights (keeps backwards compat)
                w_vwap = float(strict_cfg.get("vwap_flag", w_vwap))
                w_twap = float(strict_cfg.get("twap", w_twap))
                w_iceb = float(strict_cfg.get("iceberg", w_iceb))
                w_accum = float(strict_cfg.get("zones_accum_cap", w_accum))
                w_dist = float(strict_cfg.get("zones_dist_cap", w_dist))
                zones_total_cap = float(strict_cfg.get("zones_max", w_accum + w_dist))
                dev_bonus = float(strict_cfg.get("dev_bonus", dev_bonus))
                strong_dev_thr = float(strict_cfg.get("dev_strong", strong_dev_thr))
                vwap_flag_dev_thr = float(
                    strict_cfg.get("vwap_flag_dev_thr", vwap_flag_dev_thr)
                )
            else:
                zones_total_cap = None

            score = 0.0
            s = strats or {}
            vwap_flag = False
            vwap_dev_val = None
            vwap_flag_proxy = False
            if isinstance(s, dict):
                # determine vwap flag either by explicit buying/selling or by deviation
                vwap_flag = bool(s.get("vwap_buying")) or bool(s.get("vwap_selling"))
                vwap_dev_val = s.get("vwap_deviation")
                try:
                    if not vwap_flag and isinstance(vwap_dev_val, (int, float)):
                        if abs(float(vwap_dev_val)) >= float(vwap_flag_dev_thr):
                            vwap_flag = True
                except Exception:
                    # fall back to explicit flags
                    pass

                if vwap_flag:
                    score += w_vwap
                    logger.debug("presence: w_vwap=%.3f додано (vwap_flag)", w_vwap)
                    try:
                        # Якщо прапор VWAP встановлено лише за рахунок відхилення (proxy), зафіксуємо це
                        explicit_vwap = bool(s.get("vwap_buying")) or bool(
                            s.get("vwap_selling")
                        )
                        if (not explicit_vwap) and isinstance(
                            vwap_dev_val, (int, float)
                        ):
                            if abs(float(vwap_dev_val)) >= float(vwap_flag_dev_thr):
                                vwap_flag_proxy = True
                    except Exception:
                        vwap_flag_proxy = False
                if bool(s.get("twap_accumulation")):
                    score += w_twap
                    logger.debug(
                        "presence: w_twap=%.3f додано (twap_accumulation)", w_twap
                    )
                if bool(s.get("iceberg_orders")):
                    score += w_iceb
                    logger.debug(
                        "presence: w_iceberg=%.3f додано (iceberg_orders)", w_iceb
                    )

            # Трактуємо None як порожню структуру зон, аби уникати особливих шляхів
            if not isinstance(zones, dict):
                logger.info("[STRICT_WHALE] zones=None->[] (телеметрія) ")
            z = zones or {}
            accum_cnt = 0
            dist_cnt = 0
            liquidity_cnt = 0
            accum_quality = None
            dist_quality = None
            orderbook_available = True
            if isinstance(z, dict):
                kl = z.get("key_levels") or {}
                if isinstance(kl, dict):
                    accum_cnt = _safe_size(kl.get("accumulation_zones"))
                    dist_cnt = _safe_size(kl.get("distribution_zones"))
                    liquidity_cnt = _safe_size(kl.get("liquidity_pools"))
                    logger.debug(
                        "presence: key_levels counts accum=%d dist=%d",
                        accum_cnt,
                        dist_cnt,
                    )
                rl = z.get("risk_levels") or {}
                if isinstance(rl, dict):
                    try:
                        accum_quality = float(rl.get("accumulation"))
                    except Exception:
                        accum_quality = None
                    try:
                        dist_quality = float(rl.get("distribution"))
                    except Exception:
                        dist_quality = None
                    logger.debug(
                        "presence: risk_levels accum_quality=%s dist_quality=%s",
                        accum_quality,
                        dist_quality,
                    )
                meta = z.get("meta")
                if isinstance(meta, Mapping):
                    try:
                        orderbook_available = bool(
                            meta.get("orderbook_available", True)
                        )
                    except Exception:
                        orderbook_available = True

            norm = float(max_zones)
            accum_contrib = w_accum * min(1.0, accum_cnt / norm)
            dist_contrib = w_dist * min(1.0, dist_cnt / norm)
            if isinstance(accum_quality, (int, float)):
                accum_contrib *= max(0.5, min(1.0, float(accum_quality)))
            if isinstance(dist_quality, (int, float)):
                dist_contrib *= max(0.5, min(1.0, float(dist_quality)))
            if not orderbook_available:
                logger.debug("presence: liquidity_unavailable (orderbook absent)")

            zones_sum = accum_contrib + dist_contrib
            if zones_total_cap is not None:
                zones_sum = min(zones_sum, zones_total_cap)
            score += zones_sum
            logger.debug(
                "presence: zone contrib accum=%.3f dist=%.3f -> score=%.3f",
                accum_contrib,
                dist_contrib,
                score,
            )

            try:
                dist_for_cap = int(dist_cnt)
                liq_for_cap = int(liquidity_cnt)
            except Exception:
                dist_for_cap = 0
                liq_for_cap = 0

            if STRICT_ACCUM_CAPS_ENABLED and dist_for_cap == 0 and liq_for_cap == 0:
                cap_value = float(zones_none_cap)
                prior_score = score
                score = min(score, cap_value)
                if prior_score > cap_value:
                    logger.debug(
                        "presence: accum-only flat -> score capped %.3f → %.3f",
                        prior_score,
                        score,
                    )

            dev = s.get("vwap_deviation")
            if vwap_flag and isinstance(dev, (int, float)):
                dev_abs = abs(float(dev))
                # Залишаємо одноступеневий бонус presence (телеметрія‑only)
                if dev_abs >= strong_dev_thr:
                    score += dev_bonus
                    logger.debug(
                        "presence: strong vwap deviation %.4f >= %.4f -> +%.3f",
                        dev_abs,
                        strong_dev_thr,
                        dev_bonus,
                    )

            # asymmetry bonus: when distribution zones dominate (dist >=2), no accumulation zones,
            # and VWAP deviation is negative (selling pressure), give a small additional boost
            try:
                if (
                    isinstance(z, dict)
                    and dist_cnt >= 2
                    and accum_cnt == 0
                    and isinstance(vwap_dev_val, (int, float))
                    and float(vwap_dev_val) < 0.0
                ):
                    asym = 0.05
                    score += asym
                    logger.debug(
                        "presence: asymmetry bonus applied dist_cnt=%d accum_cnt=%d vwap_dev=%.4f -> +%.3f",
                        dist_cnt,
                        accum_cnt,
                        float(vwap_dev_val),
                        asym,
                    )
            except Exception:
                logger.debug("presence: asymmetry bonus check failed", exc_info=True)

            only_iceberg = bool(s.get("iceberg_orders")) and not (
                bool(s.get("vwap_buying"))
                or bool(s.get("vwap_selling"))
                or bool(s.get("twap_accumulation"))
            )
            if only_iceberg:
                score = min(score, iceberg_cap)
                logger.info(
                    "presence: iceberg-only case detected -> score capped to %.3f",
                    iceberg_cap,
                )

            accum_only_sources = (
                accum_cnt > 0
                and dist_cnt == 0
                and not vwap_flag
                and not bool(s.get("twap_accumulation"))
                and not bool(s.get("iceberg_orders"))
            )
            if (
                STRICT_ACCUM_CAPS_ENABLED
                and accum_only_sources
                and score > accum_only_cap
            ):
                logger.info(
                    "presence: accum-only case detected -> score capped %.3f → %.3f",
                    score,
                    accum_only_cap,
                )
                score = accum_only_cap

            # Якщо vwаp‑присутність лише за проксі‑ознакою (відхилення), жорстко обмежимо presence
            try:
                proxy_cap = float(cfg.get("presence_proxy_cap", 0.85) or 0.85)
            except Exception:
                proxy_cap = 0.85
            if vwap_flag_proxy:
                old_score = score
                score = min(score, proxy_cap)
                if score < old_score:
                    logger.info(
                        "presence: vwap-proxy flag detected -> score capped %.3f -> %.3f",
                        old_score,
                        score,
                    )

        except Exception:
            logger.exception(
                "presence: unexpected error while computing presence_score"
            )
            score = 0.0

        # Де-насищення: якщо зони відсутні (zones is None), presence не має перевищувати 0.30
        try:
            zones_is_none = not isinstance(zones, dict)
        except Exception:
            zones_is_none = True
        if STRICT_ACCUM_CAPS_ENABLED and zones_is_none:
            if score > zones_none_cap:
                logger.info(
                    "presence: zones=None -> cap score %.3f → %.3f",
                    score,
                    zones_none_cap,
                )
            score = min(score, zones_none_cap)

        if not math.isfinite(score):
            score = 0.0
        final = float(max(0.0, min(1.0, round(score, 3))))
        # strict telemetry log to help debugging presence calibration
        logger.info(
            "[STRICT_WHALE] presence_score finished=%.3f raw_score=%.6f w_vwap=%.3f w_twap=%.3f w_iceb=%.3f w_accum=%.3f w_dist=%.3f vwap_flag=%s vwap_dev=%s vwap_proxy=%s",
            final,
            score,
            w_vwap,
            w_twap,
            w_iceb,
            w_accum,
            w_dist,
            vwap_flag,
            repr(vwap_dev_val),
            str(vwap_flag_proxy),
        )
        logger.debug(
            "WhaleTelemetryScoring.presence_score_from: final_components score_raw=%.6f",
            score,
        )
        return final

    @staticmethod
    def bias_sign_from_vwap_dev(dev: float | int | None, threshold: float) -> int:
        """Визначає знак відхилення від VWAP за заданим порогом."""

        if not isinstance(dev, (int, float)):
            return 0
        dev_val = float(dev)
        if dev_val >= threshold:
            return 1
        if dev_val <= -threshold:
            return -1
        return 0

    @staticmethod
    def dominance_flags(
        *,
        whale_bias: float | None,
        vwap_dev: float | int | None,
        slope_atr: float | int | None,
        dvr: float | int | None,
        trap: Mapping[str, Any] | None,
        iceberg: bool | None,
        dist_zones: int | None,
    ) -> dict[str, Any]:
        """Обчислює прапорці домінування buy/sell з урахуванням альтернативних підтверджень."""

        cfg = dict(STAGE2_WHALE_TELEMETRY or {})

        dom_cfg = cfg.get("dominance") or {}
        bias_thr = float(cfg.get("bias_confirm_thr", 0.6) or 0.6)
        vwap_thr = float(dom_cfg.get("vwap_dev_min", 0.01) or 0.01)
        slope_abs_min = float(dom_cfg.get("slope_abs_min", 3.0) or 3.0)
        alt_cfg = dom_cfg.get("alt_confirm") or {}
        need_any = max(1, int(alt_cfg.get("need_any", 2) or 2))
        use_vol = bool(alt_cfg.get("vol_spike", False))
        use_iceberg = bool(alt_cfg.get("iceberg", False))
        dist_min_raw = alt_cfg.get("dist_zones_min")
        dist_min = int(dist_min_raw) if isinstance(dist_min_raw, (int, float)) else None

        bias_val = float(whale_bias) if isinstance(whale_bias, (int, float)) else 0.0
        buy_bias = bias_val >= bias_thr
        sell_bias = bias_val <= -bias_thr

        slope_val = float(slope_atr) if isinstance(slope_atr, (int, float)) else 0.0
        slope_up = slope_val >= slope_abs_min
        slope_down = slope_val <= -slope_abs_min
        slope_abs = abs(slope_val)

        vwap_val = float(vwap_dev) if isinstance(vwap_dev, (int, float)) else 0.0
        vwap_up_ok = vwap_val >= vwap_thr
        vwap_down_ok = vwap_val <= -vwap_thr

        dvr_val = float(dvr) if isinstance(dvr, (int, float)) else 0.0
        dvr_ok = dvr_val >= 1.8

        trap_reasons: list[str] = []
        if isinstance(trap, Mapping):
            raw_reasons = trap.get("reasons")
            if isinstance(raw_reasons, list):
                trap_reasons = [str(r).lower() for r in raw_reasons]

        vol_spike_hit = any(
            tag in trap_reasons for tag in ("volume_spike", "volatility_spike")
        )
        iceberg_hit = bool(iceberg)
        dist_hit = (
            dist_min is not None
            and isinstance(dist_zones, (int, float))
            and int(dist_zones) >= dist_min
        )

        alt_hits = 0
        if use_vol and vol_spike_hit:
            alt_hits += 1
        if use_iceberg and iceberg_hit:
            alt_hits += 1
        if dist_min is not None and dist_hit:
            alt_hits += 1

        alt_ready = alt_hits >= need_any

        dominance_down_base = sell_bias and slope_down and dvr_ok and vwap_down_ok
        dominance_up_base = buy_bias and slope_up and dvr_ok and vwap_up_ok

        dominance_down_alt = slope_down and vwap_down_ok and alt_ready
        dominance_up_alt = slope_up and vwap_up_ok and alt_ready

        dominance_down = dominance_down_base or dominance_down_alt
        dominance_up = dominance_up_base or dominance_up_alt

        dominance_down_source = (
            "alt"
            if dominance_down and not dominance_down_base and dominance_down_alt
            else ("base" if dominance_down_base else None)
        )
        dominance_up_source = (
            "alt"
            if dominance_up and not dominance_up_base and dominance_up_alt
            else ("base" if dominance_up_base else None)
        )

        dominance_alt_path = (dominance_down and dominance_down_source == "alt") or (
            dominance_up and dominance_up_source == "alt"
        )

        return {
            "dominance_down": dominance_down,
            "dominance_up": dominance_up,
            "dominance_down_source": dominance_down_source,
            "dominance_up_source": dominance_up_source,
            "dominance_alt_hits": alt_hits,
            "dominance_alt_ready": alt_ready,
            "dominance_alt_path": dominance_alt_path,
            "dominance_slope_abs": slope_abs,
            "dominance_dvr": dvr_val,
        }

    @staticmethod
    def whale_bias_from(strats: dict[str, Any] | None) -> float:
        """Оцінити напрямковий whale_bias в діапазоні [-1..+1] з логуванням.

        Args:
            strats: результат InstitutionalEntryMethods.detect_vwap_twap_strategies
                (ключі: vwap_buying, vwap_selling, twap_accumulation, vwap_deviation)

        Returns:
            float: bias у [-1.0, +1.0], округлений до 3 знаків.

        Пояснення евристики:
        - Наявність vwap_buying -> позитивний bias (buy), vwap_selling -> негативний (sell).
        - TWAP як підтвердження підсилює напрямок.
        - Сильне відхилення VWAP (>=2%) підсилює знак додатково.
        - Айсберг сам по собі не міняє знак, дає лише невеликий шум (розширення).
        """
        logger.info("WhaleTelemetryScoring.whale_bias_from: початок обчислення bias")
        logger.debug(
            "WhaleTelemetryScoring.whale_bias_from: strats_keys=%s",
            list(strats.keys()) if isinstance(strats, dict) else None,
        )

        s = strats or {}
        try:
            cfg = dict(STAGE2_WHALE_TELEMETRY or {})
            if not cfg:
                logger.debug(
                    "whale_bias: STAGE2_WHALE_TELEMETRY порожній, використовуємо дефолтні ваги",
                )

            buy = bool(s.get("vwap_buying"))
            sell = bool(s.get("vwap_selling"))
            twap_buy = bool(s.get("twap_accumulation"))
            dev = s.get("vwap_deviation")

            dev_thr = float(cfg.get("bias_vwap_dev_thr", 0.01) or 0.01)
            strong_dev_thr = float(cfg.get("strong_dev_thr", 0.02) or 0.02)
            # Базові налаштування bias
            bias_base = 0.6
            # Нова дворівнева шкала підсилення за |vwap_dev| застосовується нижче
            bias_dev_boost = 0.25
            iceberg_eps = 0.02

            strict_cfg: dict[str, Any] = (
                dict((STRONG_REGIME or {}).get("whale_scoring", {}) or {})
                if WHALE_SCORING_V2_ENABLED
                else {}
            )
            if strict_cfg:
                bias_base = float(strict_cfg.get("bias_base", bias_base))
                bias_dev_boost = float(strict_cfg.get("bias_dev_boost", bias_dev_boost))
                iceberg_eps = float(strict_cfg.get("bias_iceberg_eps", iceberg_eps))
                strong_dev_thr = float(strict_cfg.get("dev_strong", strong_dev_thr))

            dev_sign = WhaleTelemetryScoring.bias_sign_from_vwap_dev(
                dev,
                threshold=abs(dev_thr),
            )
            positive_base = bias_base
            negative_base = -bias_base
            if dev_sign > 0:
                bias = positive_base
            elif dev_sign < 0:
                bias = negative_base
            else:
                bias = 0.0

            if sell and not buy:
                bias = negative_base
            elif buy and not sell:
                bias = positive_base
            elif buy and sell:
                bias = 0.0

            logger.debug(
                "whale_bias: base bias=%.3f (buy=%s sell=%s dev_sign=%d)",
                bias,
                buy,
                sell,
                dev_sign,
            )

            twap_target = min(1.0, positive_base + (bias_dev_boost * 0.5))
            if twap_buy and bias > 0.0:
                bias = max(bias, twap_target)
                logger.debug(
                    "whale_bias: twap_accumulation підтвердив покупців -> bias=%.3f",
                    bias,
                )

            if isinstance(dev, (int, float)):
                dev_abs = abs(float(dev))
                # Дворівнева шкала: 0.012–0.024 → +0.04; ≥0.024 → +0.08
                # Порог strong_gate залишається відносним до конфіга; середній поріг — strong_dev_thr
                high_gate = strong_dev_thr * (
                    1.2 if strict_cfg else 1.5
                )  # ≈ 0.024 у строгому
                mid_gate = strong_dev_thr  # ≈ 0.02 або нижче за конфігом

                def _apply_dev_boost(cur_bias: float, boost: float, sign: int) -> float:
                    """Застосувати підсилення до базового bias з урахуванням знаку."""
                    if sign > 0:
                        return max(cur_bias, min(1.0, positive_base + boost))
                    if sign < 0:
                        return min(cur_bias, max(-1.0, negative_base - boost))
                    # якщо знак невизначений, зрушуємо від бази у бік boost
                    return (
                        min(1.0, positive_base + boost)
                        if cur_bias >= 0.0
                        else max(-1.0, negative_base - boost)
                    )

                if dev_abs >= high_gate:
                    # сильне відхилення: +0.08
                    bias = _apply_dev_boost(
                        bias, 0.08, dev_sign or (1 if bias >= 0 else -1)
                    )
                    logger.debug(
                        "whale_bias: сильне відхилення VWAP %.4f >= %.4f -> +0.08 bias=%.3f",
                        float(dev),
                        high_gate,
                        bias,
                    )
                elif dev_abs >= mid_gate:
                    # помірне відхилення: +0.04
                    bias = _apply_dev_boost(
                        bias, 0.04, dev_sign or (1 if bias >= 0 else -1)
                    )
                    logger.debug(
                        "whale_bias: помірне відхилення VWAP %.4f >= %.4f -> +0.04 bias=%.3f",
                        float(dev),
                        mid_gate,
                        bias,
                    )

            if bool(s.get("iceberg_orders")):
                noise = iceberg_eps
                if bias > 0:
                    bias = min(1.0, bias + noise)
                elif bias < 0:
                    bias = max(-1.0, bias - noise)
                logger.debug(
                    "whale_bias: iceberg_orders -> bias скориговано на %.3f до %.3f",
                    noise,
                    bias,
                )

            final_bias = float(round(max(-1.0, min(1.0, bias)), 3))
            logger.info(
                "WhaleTelemetryScoring.whale_bias_from: finished bias=%.3f", final_bias
            )
            return final_bias
        except Exception:
            logger.exception(
                "whale_bias: unexpected error computing bias, returning 0.0"
            )
            return 0.0
