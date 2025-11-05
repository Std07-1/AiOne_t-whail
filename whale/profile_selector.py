"""Селектор профілю (телеметрія‑only) для Stage1 hints.

Контракт:
- select_profile(stats, whale, symbol) -> (profile, confidence, reasons)
    • Жодного I/O, лише чисті обчислення.
    • Повертає ім'я профілю і впевненість (0..1), та короткі причини.

Профілі (повна матриця):
    - probe_up, grab_upper, pullback_up, range_fade, down_trend, grab_lower

Додатково:
- apply_hysteresis(prev_profile, new_profile, last_switch_ts, hysteresis_s) -> str
    • Антифлікер між перемиканнями профілю.
"""

from __future__ import annotations

from typing import Any, Mapping, Tuple, List

# Мінімальна інтеграція false_breakout (Stage A): pure‑виклик від stats
try:
    from config.config_whale_profiles import (
        get_false_breakout_cfg,
        market_class_for_symbol,
    )
    from whale.false_breakout import detect_false_breakout_up_from_stats
except Exception:  # захист від твердих падінь у середовищах без модулів

    def get_false_breakout_cfg(market_class: str) -> dict:  # type: ignore
        return {"min_wick_ratio": 0.6, "min_vol_z": 1.5}

    def market_class_for_symbol(symbol: str) -> str:  # type: ignore
        s = str(symbol or "").upper()
        return (
            "BTC" if s.startswith("BTC") else ("ETH" if s.startswith("ETH") else "ALTS")
        )

    def detect_false_breakout_up_from_stats(stats: Mapping[str, Any], cfg: Mapping[str, Any]) -> Mapping[str, Any]:  # type: ignore
        return {
            "is_false": False,
            "overrun_pct": 0.0,
            "wick_ratio": 0.0,
            "vol_z": 0.0,
            "reject_bars": 0,
        }


# Edge-band (Stage B): pure utils
try:
    from config.config_whale_profiles import get_profile_thresholds as _get_thr
    from levels.edge_band import (
        band_width as _band_width,
        edge_features as _edge_features,
    )
except Exception:  # pragma: no cover - fallbacks

    def _get_thr(_mc: str, _p: str) -> Mapping[str, Any]:  # type: ignore
        return {}

    def _band_width(atr_pct: float, tick: float, k: float) -> float:  # type: ignore
        try:
            return max(
                float(atr_pct or 0.0) * float(k or 0.0), float(tick or 0.0) * 3.0
            )
        except Exception:
            return 0.0

    def _edge_features(_bars: List[Mapping[str, Any]], _band: Tuple[float, float]) -> Mapping[str, float]:  # type: ignore
        return {
            "edge_hits": 0.0,
            "accept_in_band": 0.0,
            "band_squeeze": 0.0,
            "max_pullback_in_band": 0.0,
        }


def apply_hysteresis(
    prev_profile: str | None,
    new_profile: str,
    last_switch_ts: float | None,
    hysteresis_s: int | float,
) -> str:
    """Застосовує гістерезис до переключення профілю.

    Якщо з моменту останньої зміни профілю минуло менше hysteresis_s сек і
    новий профіль відрізняється від попереднього — утримуємо попередній.

    Args:
        prev_profile: попередній активний профіль (або None).
        new_profile: новий кандидат профілю.
        last_switch_ts: timestamp останньої зміни (time.time()).
        hysteresis_s: мін. тривалість між перемиканнями.

    Returns:
        Ім'я обраного профілю (prev або new).
    """
    if not prev_profile:
        return new_profile
    try:
        import time as _t

        now = _t.time()
    except Exception:
        now = 0.0
    try:
        last = float(last_switch_ts or 0.0)
        h = float(hysteresis_s or 0.0)
    except Exception:
        last = 0.0
        h = 0.0
    if new_profile != prev_profile and (now - last) < h:
        return prev_profile
    return new_profile


def _safe_float(x: Any, default: float | None = None) -> float | None:
    try:
        v = float(x)
        return v
    except Exception:
        return default


def select_profile(
    stats: Mapping[str, Any] | None,
    whale: Mapping[str, Any] | None,
    symbol: str,
) -> Tuple[str, float, List[str]]:
    """Вибір профілю за евристиками без I/O.

    Єдиний контракт: працюємо на доступних у stats/whale полях, уникаємо падінь.
    Поведінка (узагальнено):
      - probe_up: домінують покупки (dominance.buy), bias>0, vdev>0, поруч із верхнім краєм
      - grab_upper: великий верхній хвіст (wick_upper_q ≥ 0.8), слабка акцептація
      - pullback_up: bias>0, vdev>=0, але після нещодавнього зниження (retest/near_lower)
      - range_fade: вузький діапазон (band_pct низький), немає чіткої dominance
      - down_trend: dominance.sell & bias<0 & vdev<0
      - grab_lower: великий нижній хвіст (wick_lower_q ≥ 0.8), слабка акцептація
    """
    reasons: List[str] = []
    if not isinstance(whale, Mapping):
        return ("range_fade", 0.5, ["no_whale"])

    # Безпечне зчитування базових метрик
    bias = _safe_float(whale.get("bias"), 0.0) or 0.0
    vdev = _safe_float(whale.get("vwap_dev"), 0.0) or 0.0
    pres = _safe_float(whale.get("presence"), 0.0) or 0.0
    band_pct = _safe_float((stats or {}).get("band_pct"), None)
    near_edge = (stats or {}).get("near_edge")
    near_upper = bool(near_edge is True or str(near_edge).lower() == "upper")
    near_lower = bool(near_edge is True or str(near_edge).lower() == "lower")
    wick_u = _safe_float((stats or {}).get("wick_upper_q"), 0.0) or 0.0
    wick_l = _safe_float((stats or {}).get("wick_lower_q"), 0.0) or 0.0
    acc_ok = bool((stats or {}).get("acceptance_ok", False))
    # slope_atr (за наявності) для діагностики невідповідностей edge/slope
    slope = _safe_float((stats or {}).get("price_slope_atr"))
    if slope is None:
        slope = _safe_float((stats or {}).get("slope_atr"))

    def _augment_mismatch(rs: List[str]) -> List[str]:
        try:
            s_abs = abs(float(slope)) if isinstance(slope, (int, float)) else None
            is_edge = bool(near_upper or near_lower)
            # edge присутній, але нахил слабкий → edge_no_slope
            if is_edge and (s_abs is not None) and (s_abs < 0.2):
                rs.append("edge_no_slope")
            # сильний нахил, але немає краю діапазону → slope_no_edge
            if (not is_edge) and (s_abs is not None) and (s_abs >= 0.8):
                rs.append("slope_no_edge")
        except Exception:
            pass
        return rs

    dom = whale.get("dominance") if isinstance(whale, Mapping) else None
    dom_buy = bool((dom or {}).get("buy")) if isinstance(dom, Mapping) else False
    dom_sell = bool((dom or {}).get("sell")) if isinstance(dom, Mapping) else False

    # 0) Grab верх/низ: довгі хвости та відсутність прийняття — ВИЩИЙ пріоритет
    if (wick_u or 0.0) >= 0.8 and not acc_ok and bias <= 0.2:
        reasons = ["wick_u>=0.8", "no_acceptance"]
        conf = max(0.0, min(1.0, 0.45 + 0.25 * (wick_u - 0.8) + 0.2 * pres))
        return ("grab_upper", float(conf), _augment_mismatch(reasons))
    if (wick_l or 0.0) >= 0.8 and not acc_ok and bias >= -0.2:
        reasons = ["wick_l>=0.8", "no_acceptance"]
        conf = max(0.0, min(1.0, 0.45 + 0.25 * (wick_l - 0.8) + 0.2 * pres))
        return ("grab_lower", float(conf), _augment_mismatch(reasons))

    # 1) Chop pre-breakout up (Stage B band‑оцінка, із fallbacks):
    #    часті торкання верхнього краю в межах band, squeeze усередині band,
    #    слабка акцептація, нахил ≥ 0.6, присутність ≥ 0.65, |vwap_dev| невеликий.
    #    Повертаємо профіль нейтральної гіпотези.
    try:
        mc = market_class_for_symbol(symbol)
    except Exception:
        mc = "ALTS"
    thr_band = _get_thr(mc, "chop_pre_breakout_up")
    band_k = float((thr_band or {}).get("band_k", 0.5) or 0.5)
    edge_hits_min = int((thr_band or {}).get("edge_hits_min", 3) or 3)
    squeeze_min = float((thr_band or {}).get("band_squeeze_min", 0.35) or 0.35)
    pullback_ratio_max = float(
        (thr_band or {}).get("max_pullback_in_band_ratio_max", 0.7) or 0.7
    )
    # Джерела band: atr_pct і tick із stats; база — остання ціна (stats.last_price/close)
    atr_pct = _safe_float((stats or {}).get("atr_pct"), 0.01) or 0.01
    tick = (
        _safe_float((stats or {}).get("tick") or (stats or {}).get("tick_size"), 0.0001)
        or 0.0001
    )
    base_price = (
        _safe_float((stats or {}).get("last_price"))
        or _safe_float((stats or {}).get("close"))
        or _safe_float((stats or {}).get("price"))
        or _safe_float((whale or {}).get("last_price"))
        or 0.0
    ) or 0.0
    w = _band_width(float(atr_pct), float(tick), float(band_k))
    band_lo = float(base_price - w)
    band_hi = float(base_price + w)
    # Спроба дістати останні 15 барів зі stats (гнучкі ключі)
    bars = None
    try:
        candidate_keys = ("bars", "ohlcv_tail", "ohlcv", "bars_tail")
        for kkey in candidate_keys:
            v = (stats or {}).get(kkey)
            if isinstance(v, list) and v:
                bars = v
                break
    except Exception:
        bars = None
    edge_win = 15
    ef = None
    if isinstance(bars, list) and bars:
        try:
            ef = _edge_features(bars, (band_lo, band_hi))
        except Exception:
            ef = None
    # Fall back до попереднього підрахунку, якщо немає bars/ef
    if ef is None:
        try:
            edge_hits = int((stats or {}).get("edge_hits_upper") or 0)
            edge_win = int((stats or {}).get("edge_hits_window") or 0)
        except Exception:
            edge_hits, edge_win = 0, 0
        band_ok = bool(band_pct is not None and band_pct <= 0.025)
        edge_hits_ok = bool(edge_hits >= edge_hits_min and edge_win >= 10)
        slope_abs = abs(float(slope)) if isinstance(slope, (int, float)) else 0.0
        vdev_abs = abs(vdev)
        if (
            near_upper
            and edge_hits_ok
            and band_ok
            and slope_abs >= 0.6
            and pres >= 0.65
            and vdev_abs <= 0.012
            and not bool(acc_ok)
        ):
            reasons = [
                "chop_pre_breakout",
                f"edge_hits={edge_hits}/{edge_win}",
                "slope_twap_ok",
            ]
            # False breakout перевірка
            try:
                cfg_fb = get_false_breakout_cfg(mc)
                fb = detect_false_breakout_up_from_stats(dict(stats or {}), cfg_fb)
            except Exception:
                fb = {"is_false": False}
            if bool(fb.get("is_false")):
                rf_reasons = reasons + [
                    "false_breakout_up",
                    f"wick_ratio>={fb.get('wick_ratio', 0):.2f}",
                    "no_acceptance",
                ]
                conf_rf = max(0.0, min(1.0, 0.45 + 0.2 * pres))
                return ("range_fade", float(conf_rf), _augment_mismatch(rf_reasons))
            if 0.2 <= abs(bias) <= 0.5:
                reasons.append("bias_weak_mid")
            conf = max(
                0.0, min(1.0, 0.6 + 0.2 * min(1.0, pres) + 0.1 * min(1.0, slope_abs))
            )
            return ("chop_pre_breakout_up", float(conf), _augment_mismatch(reasons))
    else:
        # Основний шлях: band‑фічі
        edge_hits = int(float(ef.get("edge_hits", 0.0)))
        band_squeeze = float(ef.get("band_squeeze", 0.0) or 0.0)
        mpb = float(ef.get("max_pullback_in_band", 0.0) or 0.0)
        slope_abs = abs(float(slope)) if isinstance(slope, (int, float)) else 0.0
        vdev_abs = abs(vdev)
        pullback_ok = bool(mpb <= pullback_ratio_max * max(w, 1e-9))
        if (
            edge_hits >= edge_hits_min
            and band_squeeze >= squeeze_min
            and pullback_ok
            and slope_abs >= 0.6
            and pres >= 0.65
            and vdev_abs <= 0.012
            and not bool(acc_ok)
        ):
            reasons = [
                "chop_pre_breakout",
                f"band=[{band_lo:.4f},{band_hi:.4f}]",
                f"edge_hits={edge_hits}/15",
                "band_squeeze_ok",
                "slope_twap_ok",
            ]
            # False breakout перевірка (Stage A інтегрована)
            try:
                cfg_fb = get_false_breakout_cfg(mc)
                fb = detect_false_breakout_up_from_stats(dict(stats or {}), cfg_fb)
            except Exception:
                fb = {"is_false": False}
            if bool(fb.get("is_false")):
                rf_reasons = reasons + [
                    "false_breakout_up",
                    f"wick_ratio>={fb.get('wick_ratio', 0):.2f}",
                    "no_acceptance",
                ]
                conf_rf = max(0.0, min(1.0, 0.45 + 0.2 * pres))
                return ("range_fade", float(conf_rf), _augment_mismatch(rf_reasons))
            if 0.2 <= abs(bias) <= 0.5:
                reasons.append("bias_weak_mid")
            conf = max(
                0.0, min(1.0, 0.6 + 0.2 * min(1.0, pres) + 0.1 * min(1.0, slope_abs))
            )
            return ("chop_pre_breakout_up", float(conf), _augment_mismatch(reasons))
    slope_abs = abs(float(slope)) if isinstance(slope, (int, float)) else 0.0
    vdev_abs = abs(vdev)
    if (
        near_upper
        and edge_hits >= 3
        and edge_win >= 10
        and (band_pct is not None and band_pct <= 0.025)
        and slope_abs >= 0.6
        and pres >= 0.65
        and vdev_abs <= 0.012
        and not bool(acc_ok)
    ):
        reasons = [
            "chop_pre_breakout",
            f"edge_hits={edge_hits}/{edge_win}",
            "slope_twap_ok",
        ]
        # Перевірка на хибний пробій: якщо True — перемикаємося на range_fade з причинами
        try:
            mc = market_class_for_symbol(symbol)
            cfg_fb = get_false_breakout_cfg(mc)
            fb = detect_false_breakout_up_from_stats(dict(stats or {}), cfg_fb)
        except Exception:
            fb = {"is_false": False}
        if bool(fb.get("is_false")):
            rf_reasons = reasons + [
                "false_breakout_up",
                f"wick_ratio>={fb.get('wick_ratio', 0):.2f}",
                "no_acceptance",
            ]
            conf_rf = max(0.0, min(1.0, 0.45 + 0.2 * pres))
            return ("range_fade", float(conf_rf), _augment_mismatch(rf_reasons))
        if 0.2 <= abs(bias) <= 0.5:
            reasons.append("bias_weak_mid")
        conf = max(
            0.0, min(1.0, 0.6 + 0.2 * min(1.0, pres) + 0.1 * min(1.0, slope_abs))
        )
        return ("chop_pre_breakout_up", float(conf), _augment_mismatch(reasons))

    # 2) Сильні трендові: probe_up
    if dom_buy and bias > 0.0 and vdev > 0.0:
        reasons = ["dom_buy", "bias>0", "vdev>0"]
        if near_upper:
            reasons.append("near_upper")
        conf = max(0.0, min(1.0, 0.5 + 0.3 * pres + 0.2 * min(abs(bias), 1.0)))
        return ("probe_up", float(conf), _augment_mismatch(reasons))

    # 3) Pullback up: позитивний bias, невеликий vdev, поруч із нижньою гранню
    if (
        bias > 0.0
        and vdev >= -0.002
        and (near_lower or (band_pct is not None and band_pct <= 0.06))
    ):
        reasons = ["bias>0", "vdev>=-0.2%", "near_lower|tight_band"]
        conf = max(0.0, min(1.0, 0.45 + 0.2 * pres + 0.15 * min(abs(bias), 1.0)))
        return ("pullback_up", float(conf), _augment_mismatch(reasons))

    # 4) Range fade: вузький діапазон і відсутня dominance
    if band_pct is not None and band_pct <= 0.05 and not (dom_buy or dom_sell):
        reasons = ["band_tight", "no_dominance"]
        conf = max(0.0, min(1.0, 0.4 + 0.3 * (0.06 - band_pct)))
        return ("range_fade", float(conf), _augment_mismatch(reasons))

    # 5) Down trend: переносимо в кінець за пріоритетом
    if dom_sell and bias < 0.0 and vdev < 0.0:
        reasons = ["dom_sell", "bias<0", "vdev<0"]
        if near_lower:
            reasons.append("near_lower")
        conf = max(0.0, min(1.0, 0.5 + 0.3 * pres + 0.2 * min(abs(bias), 1.0)))
        return ("down_trend", float(conf), _augment_mismatch(reasons))

    # За замовчуванням — флет/fade режим
    reasons = ["default"]
    conf = max(0.0, min(1.0, 0.4 + 0.3 * pres))
    return ("range_fade", float(conf), _augment_mismatch(reasons))
