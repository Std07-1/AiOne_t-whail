"""Адаптер для підключення strict phase detector (Stage2) у Stage1 як телеметрії.

Ціль:
- Побудувати мінімальний context із Stage1.stats для stage2.phase_detector.detect_phase.
- Не змінювати контракти Stage1 — лише додати stats["phase"] з телеметрією.

Примітки:
- Параметр low_gate_effective передається, якщо відомий із thresholds.effective.
- Якщо деякі поля відсутні (whale, HTF), детектор працює у best-effort режимі.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import (
    HTF_GATES,
    K_CUMULATIVE_DELTA,
    K_DIRECTIONAL_VOLUME_RATIO,
    K_PRICE_SLOPE_ATR,
    STAGE1_HTF_STALE_MS,
    STRICT_HTF_GRAY_GATE_ENABLED,
)
from stage2.phase_detector import detect_phase as _detect_phase
from utils.utils import safe_float, safe_number


def _build_phase_state_hint(raw: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Повертає стиснутий phase_state_hint для market_context/meta."""

    if not isinstance(raw, Mapping):
        return None
    phase_val = raw.get("current_phase") or raw.get("last_detected_phase")
    phase = str(phase_val).strip() if isinstance(phase_val, str) else None
    age_s = safe_float(raw.get("age_s"))
    score = safe_float(raw.get("phase_score"))
    reason = raw.get("last_reason")
    presence = safe_float(raw.get("last_whale_presence"))
    bias = safe_float(raw.get("last_whale_bias"))
    htf_strength = safe_float(raw.get("last_htf_strength"))
    updated_ts = safe_float(raw.get("updated_ts"))
    if phase is None and age_s is None and score is None:
        return None
    hint: dict[str, Any] = {
        "phase": phase,
        "age_s": age_s,
        "score": score,
        "reason": reason,
    }
    if presence is not None:
        hint["presence"] = presence
    if bias is not None:
        hint["bias"] = bias
    if htf_strength is not None:
        hint["htf_strength"] = htf_strength
    if updated_ts is not None:
        hint["updated_ts"] = updated_ts
    return hint


try:
    from config import config as _cfg  # type: ignore
except Exception:  # pragma: no cover - fallback на дефолтні значення
    _cfg = None

logger = logging.getLogger("phase_adapter")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


def _cfg_value(name: str, default: Any) -> Any:
    try:
        return getattr(_cfg, name)
    except Exception:
        return default


def build_phase_context_from_stats(
    stats: Mapping[str, Any], *, symbol: str, low_gate_effective: float | None = None
) -> dict[str, Any]:
    """Побудувати мінімальний market_context для PhaseDetector з Stage1.stats.

    Args:
        stats: Stage1.stats словник з ключовими полями (band_pct, near_edge, directional тощо).
        symbol: Тікер для логування/контексту.
        low_gate_effective: Опційний low_gate із thresholds.effective (для альтернатив у фазах).

    Returns:
        dict: market_context-лайт, сумісний з phase_detector.
    """
    band_pct = stats.get("band_pct")
    near_edge = stats.get("near_edge")

    # Directional
    dvr = stats.get(K_DIRECTIONAL_VOLUME_RATIO)
    cd = stats.get(K_CUMULATIVE_DELTA)
    slope_atr = stats.get(K_PRICE_SLOPE_ATR)

    # Whale telemetry: best-effort, якщо вже вбудовано у stats.whale (наприклад у screening_producer)
    whale_raw = stats.get("whale")
    whale = whale_raw if isinstance(whale_raw, Mapping) else {}
    vwap_dev = whale.get("vwap_dev") if isinstance(whale, Mapping) else None
    presence = whale.get("presence") if isinstance(whale, Mapping) else None
    bias = whale.get("bias") if isinstance(whale, Mapping) else None

    def _normalize_zones_summary(source: Any) -> dict[str, int] | None:
        if not isinstance(source, Mapping):
            return None
        try:
            accum_raw = source.get("accum_cnt", source.get("accum", 0))
            dist_raw = source.get("dist_cnt", source.get("dist", 0))
            accum_cnt = int(accum_raw) if isinstance(accum_raw, (int, float)) else 0
            dist_cnt = int(dist_raw) if isinstance(dist_raw, (int, float)) else 0
        except Exception:
            return None
        return {"accum_cnt": accum_cnt, "dist_cnt": dist_cnt}

    zones_source = whale.get("zones_summary") if isinstance(whale, Mapping) else None
    if zones_source is None:
        extra_zones = stats.get("zones_summary")
        if isinstance(extra_zones, Mapping):
            zones_source = extra_zones
    zones_summary = _normalize_zones_summary(zones_source)

    # Trap блок: phase_detector очікує meta.trap.trap_score
    trap = stats.get("trap") if isinstance(stats.get("trap"), dict) else None
    trap_score = None
    trap_override = False
    trap_spike_ratio = None
    if isinstance(trap, dict):
        ts = trap.get("score")
        try:
            trap_score = float(ts) if ts is not None else None
        except Exception:
            trap_score = None
        trap_override = bool(trap.get("cooldown_override"))
        try:
            ratios = (
                trap.get("ratios") if isinstance(trap.get("ratios"), Mapping) else {}
            )
            trap_spike_ratio = safe_float((ratios or {}).get("atr_spike_ratio"))
        except Exception:
            trap_spike_ratio = None

    # HTF індикатор: якщо є явний stats['htf_ok'] — використовуємо його;
    # як альтернатива — якщо задано числовий 'htf_slope', вважаємо ok=True коли slope>=0.
    htf_ok_val = None
    try:
        if isinstance(stats.get("htf_ok"), bool):
            htf_ok_val = bool(stats.get("htf_ok"))
        else:
            htf_slope = safe_float(stats.get("htf_slope"))
            if isinstance(htf_slope, (int, float)):
                htf_ok_val = bool(float(htf_slope) >= 0.0)
        # телеметрія: htf_score/htf_strength якщо доступні в stats
        try:
            hs = stats.get("htf_score")
            htf_score = float(hs) if hs is not None else None
        except Exception:
            htf_score = None
        try:
            hst = stats.get("htf_strength")
            htf_strength = float(hst) if hst is not None else None
        except Exception:
            htf_strength = None
    except Exception:
        htf_ok_val = None
        htf_score = None
        htf_strength = None

    if htf_ok_val is not None:
        try:
            logger.info(
                "[HTF] symbol=%s ok=%s score=%.4f strength=%.4f",
                symbol,
                str(htf_ok_val),
                htf_score if htf_score is not None else float("nan"),
                htf_strength if htf_strength is not None else float("nan"),
            )
        except Exception:
            pass

    trap_meta: dict[str, Any] = {}
    if trap_score is not None:
        trap_meta["trap_score"] = trap_score
    if trap_spike_ratio is not None:
        trap_meta["atr_spike_ratio"] = trap_spike_ratio
    if trap_override:
        trap_meta["cooldown_override"] = True
    if trap_override:
        trap_meta["cooldown_override"] = True

    volatility_meta: dict[str, Any] = {}
    vol_regime_stats = (
        stats.get("volatility_regime")
        if isinstance(stats.get("volatility_regime"), Mapping)
        else None
    )
    if isinstance(vol_regime_stats, Mapping):
        regime_val = vol_regime_stats.get("regime") or stats.get("vol_regime_strict")
        if regime_val is not None:
            volatility_meta["regime"] = str(regime_val)
        atr_ratio_val = safe_float(vol_regime_stats.get("atr_ratio"))
        if atr_ratio_val is not None:
            volatility_meta["atr_ratio"] = atr_ratio_val
        crisis_val = safe_float(vol_regime_stats.get("crisis_vol_score"))
        if crisis_val is not None:
            volatility_meta["crisis_vol_score"] = crisis_val
        spike_val = safe_float(vol_regime_stats.get("atr_spike_ratio"))
        if spike_val is not None:
            volatility_meta["atr_spike_ratio"] = spike_val
        reason_val = vol_regime_stats.get("crisis_reason")
        if reason_val is not None:
            volatility_meta["crisis_reason"] = reason_val

    whale_meta = {
        "vwap_deviation": safe_float(vwap_dev),
        "presence_score": safe_float(presence),
        "whale_bias": safe_float(bias),
    }
    if zones_summary is not None:
        whale_meta["zones_summary"] = zones_summary

    ctx: dict[str, Any] = {
        "symbol": symbol,
        "key_levels_meta": {
            "band_pct": safe_float(band_pct),
            "is_near_edge": (
                bool(near_edge) if isinstance(near_edge, bool) else near_edge
            ),
        },
        "meta": {
            "source_stage": "stage1",
            # htf_ok: передаємо визначене вище значення або None (best‑effort)
            "htf_ok": htf_ok_val,
            "htf_score": htf_score,
            "htf_strength": htf_strength,
            "atr_ratio": safe_float(stats.get("atr_ratio")),
            "low_gate_effective": safe_float(low_gate_effective),
            "trap": trap_meta if trap_meta else {},
            "volatility": volatility_meta if volatility_meta else {},
            "whale": whale_meta,
        },
        "directional": {
            K_DIRECTIONAL_VOLUME_RATIO: safe_float(dvr),
            K_CUMULATIVE_DELTA: safe_float(cd),
            K_PRICE_SLOPE_ATR: safe_float(slope_atr),
        },
    }

    logger.debug(
        "[STRICT_PHASE] build_phase_context_from_stats | symbol=%s band_pct=%.4f near_edge=%s dvr=%.4f cd=%.4f slope_atr=%.4f vwap_dev=%.4f presence=%.4f bias=%.4f trap_score=%.4f trap_spike=%.4f trap_override=%s htf_ok=%s htf_score=%.4f htf_strength=%.4f",
        symbol,
        safe_number(band_pct, 0.0),
        str(near_edge),
        safe_number(dvr, 0.0),
        safe_number(cd, 0.0),
        safe_number(slope_atr, 0.0),
        safe_number(vwap_dev, 0.0),
        safe_number(presence, 0.0),
        safe_number(bias, 0.0),
        safe_number(trap_score, 0.0),
        safe_number(trap_spike_ratio, 0.0),
        str(trap_override),
        str(htf_ok_val),
        safe_number(htf_score, 0.0) if htf_score is not None else 0.0,
        safe_number(htf_strength, 0.0) if htf_strength is not None else 0.0,
    )

    return ctx


def detect_phase_from_stats(
    stats: Mapping[str, Any], *, symbol: str, low_gate_effective: float | None = None
) -> dict[str, Any] | None:
    """Виклик stage2.phase_detector.detect_phase, маплячи Stage1.stats на очікувані поля.

    Args:
        stats: Stage1.stats із полями band_pct/near_edge та directional.
        symbol: символ для контексту/логів.
        low_gate_effective: ефективний low_gate з thresholds (якщо доступний).

    Returns:
        dict|None: Телеметрія фази: {name, score, reasons, volz_source, overlay, profile, threshold_tag, strategy_hint}.
    """
    # Підготуємо stats для phase_detector: він очікує volume_z_light або vol_z
    # У Stage1 назва поля — volume_z. Передамо як vol_z без зміни Stage1 контрактів.
    phase_stats: dict[str, Any] = {
        "symbol": symbol,
        # передаємо дубльоване поле як vol_z
        "vol_z": safe_float(stats.get("volume_z")),
        # опціонально: rsi/atr/current_price, якщо доступні
        "rsi": safe_float(stats.get("rsi")),
        "atr": safe_float(stats.get("atr")),
        "current_price": safe_float(stats.get("current_price")),
    }
    ctx = build_phase_context_from_stats(
        stats, symbol=symbol, low_gate_effective=low_gate_effective
    )

    logger.debug(
        "[STRICT_PHASE] detect_phase_from_stats | symbol=%s vol_z=%.4f rsi=%.4f atr=%.4f current_price=%.4f",
        symbol,
        (
            safe_float(stats.get("volume_z"))
            if stats.get("volume_z") is not None
            else float("nan")
        ),
        safe_float(stats.get("rsi")) if stats.get("rsi") is not None else float("nan"),
        safe_float(stats.get("atr")) if stats.get("atr") is not None else float("nan"),
        (
            safe_float(stats.get("current_price"))
            if stats.get("current_price") is not None
            else float("nan")
        ),
    )

    res = _detect_phase(phase_stats, ctx)
    phase_state_hint = _build_phase_state_hint(phase_stats.get("phase_state"))
    if res is None:
        logger.debug(
            "[STRICT_PHASE] detect_phase_from_stats | symbol=%s phase=None (детектор не повернув результат)",
            symbol,
        )
        return None
    _meta = ctx.get("meta", {}) if isinstance(ctx, dict) else {}
    htf_gate_reason: str | None = None
    _htf_ok = _meta.get("htf_ok") if isinstance(_meta, dict) else None
    _htf_score = _meta.get("htf_score") if isinstance(_meta, dict) else None
    _htf_strength = _meta.get("htf_strength") if isinstance(_meta, dict) else None

    try:
        htf_strength_val = float(_htf_strength) if _htf_strength is not None else None
    except Exception:
        htf_strength_val = None

    gates_cfg = HTF_GATES if isinstance(HTF_GATES, Mapping) else {}
    try:
        htf_gray_thr = float(gates_cfg.get("gray_low", 0.10) or 0.10)
    except Exception:
        htf_gray_thr = 0.10
    try:
        htf_ok_thr = float(gates_cfg.get("ok", 0.20) or 0.20)
    except Exception:
        htf_ok_thr = 0.20
    try:
        htf_gray_penalty = float(gates_cfg.get("gray_penalty", 0.20) or 0.20)
    except Exception:
        htf_gray_penalty = 0.20

    if STRICT_HTF_GRAY_GATE_ENABLED and htf_strength_val is not None:
        if htf_strength_val < htf_gray_thr:
            if isinstance(_meta, dict):
                _meta["htf_ok"] = False
                _meta["htf_gate_reason"] = "gray_low"
            _htf_ok = False
            htf_gate_reason = "gray_low"
            try:
                logger.info(
                    "[HTF] symbol=%s ok=False reason=gray_low strength=%.4f thr=%.4f",
                    symbol,
                    float(htf_strength_val),
                    float(htf_gray_thr),
                )
            except Exception:
                pass
        elif htf_strength_val < htf_ok_thr:
            try:
                res.score = max(0.0, float(res.score or 0.0) - htf_gray_penalty)
                logger.info(
                    "[HTF] symbol=%s gray_penalty=%.2f strength=%.4f range=[%.2f,%.2f)",
                    symbol,
                    float(htf_gray_penalty),
                    float(htf_strength_val),
                    float(htf_gray_thr),
                    float(htf_ok_thr),
                )
            except Exception:
                pass

    stats_mapping = stats if isinstance(stats, Mapping) else {}
    stale_field_present = (
        isinstance(stats_mapping, Mapping) and "htf_stale_ms" in stats_mapping
    )
    htf_stale_ms = (
        safe_float(stats_mapping.get("htf_stale_ms")) if stale_field_present else None
    )
    htf_stale_limit = safe_float(stats_mapping.get("htf_stale_limit_ms"))
    # Отримуємо default_stale_limit з config, а не з settings
    default_stale_limit = safe_float(STAGE1_HTF_STALE_MS)
    if htf_stale_limit is None:
        htf_stale_limit = default_stale_limit
    elif default_stale_limit is not None:
        try:
            htf_stale_limit = min(htf_stale_limit, default_stale_limit)
        except Exception:
            pass
    if (
        STRICT_HTF_GRAY_GATE_ENABLED
        and stale_field_present
        and htf_stale_ms is not None
        and htf_stale_limit is not None
        and htf_stale_ms > htf_stale_limit
    ):
        if isinstance(_meta, dict):
            _meta["htf_ok"] = False
            _meta["htf_gate_reason"] = "stale"
        _htf_ok = False
        htf_gate_reason = "stale"
        try:
            logger.info(
                "[HTF] symbol=%s ok=False reason=stale htf_stale_ms=%.0f limit_ms=%.0f",
                symbol,
                float(htf_stale_ms),
                float(htf_stale_limit),
            )
        except Exception:
            pass

    logger.info(
        "[STRICT_PHASE] detect_phase_from_stats | symbol=%s phase=%s score=%.4f reasons=%s threshold_tag=%s strategy_hint=%s htf_ok=%s htf_score=%.4f htf_strength=%.4f",
        symbol,
        res.phase,
        float(res.score or 0.0),
        ",".join(res.reasons or []),
        getattr(res, "threshold_tag", None),
        getattr(res, "strategy_hint", None),
        str(_htf_ok),
        float(_htf_score) if _htf_score is not None else float("nan"),
        float(_htf_strength) if _htf_strength is not None else float("nan"),
    )
    diag_phase: dict[str, Any] | None = None
    res_debug = getattr(res, "debug", None)
    if isinstance(res_debug, Mapping):
        diag_phase = {
            "missing_fields": res_debug.get("missing_fields"),
            "reason_codes": list(res_debug.get("reason_codes", [])),
            "guard_notes": list(res_debug.get("guard_notes", [])),
            "reject_stage": res_debug.get("reject_stage"),
            "low_atr_override_active": res_debug.get("low_atr_override_active"),
        }
    diagnostics = {
        "htf": {
            "ok": bool(_htf_ok) if isinstance(_htf_ok, bool) else None,
            "score": safe_float(_htf_score),
            "strength": htf_strength_val,
            "gate_reason": (
                htf_gate_reason
                or (
                    _meta.get("htf_gate_reason") if isinstance(_meta, Mapping) else None
                )
            ),
            "stale_ms": htf_stale_ms,
            "stale_limit_ms": htf_stale_limit,
        }
    }
    if diag_phase:
        diagnostics["phase_detector"] = diag_phase

    return {
        "name": res.phase,
        "score": float(res.score or 0.0),
        "reasons": list(res.reasons or []),
        "volz_source": res.volz_source,
        "overlay": res.overlay or {},
        "profile": res.profile,
        "threshold_tag": res.threshold_tag,
        "strategy_hint": res.strategy_hint,
        "phase_state_hint": phase_state_hint,
        "diagnostics": diagnostics,
    }


def resolve_scenario(
    phase: str | None, stats: Mapping[str, Any]
) -> tuple[str | None, float]:
    """Визначає сценарій та його впевненість на базі фази та Stage1.stats.

    Правила (scenario-only, НЕ змінює пороги фаз):
      - breakout_confirmation: post_breakout поблизу краю, vol_z ≤ 1.8, DVR ≥ 0.5,
        band_expand ≥ 0.010 (якщо доступно), HTF ≥ 0.20; htf_stale → −0.10 до conf (не скасовує).
      - pullback_continuation: momentum, bias≠0, presence ≥ 0.60, HTF ≥ 0.20.

    Повертає (scenario_detected, scenario_confidence).
    """
    try:
        # EvidenceBus: підготовка допоміжної функції (без зміни контрактів)
        symbol: str = ""
        try:
            from config.constants import K_SYMBOL  # type: ignore  # noqa: E402

            symbol = str(
                (stats or {}).get(K_SYMBOL) or (stats or {}).get("symbol") or ""
            ).upper()
        except Exception:
            symbol = str((stats or {}).get("symbol") or "").upper()

        def _ev_add(k: str, v: Any, w: float = 1.0, note: str | None = None) -> None:
            try:
                if not symbol:
                    return
                from utils.evidence_bus import add as _ev_add_impl  # noqa: E402

                _ev_add_impl(symbol, k, v, float(w), note)
            except Exception:
                pass

        phase_name = (phase or "").strip() if isinstance(phase, str) else ""
        st = stats if isinstance(stats, Mapping) else {}
        # базові поля
        near_edge = st.get("near_edge")
        volz = safe_float(st.get("volume_z"))
        dvr = safe_float(st.get(K_DIRECTIONAL_VOLUME_RATIO))
        htf_strength = None
        try:
            htf_strength = (
                float(st.get("htf_strength"))
                if st.get("htf_strength") is not None
                else None
            )
        except Exception:
            htf_strength = None
        whale = st.get("whale") if isinstance(st.get("whale"), Mapping) else {}
        presence = safe_float((whale or {}).get("presence")) or 0.0
        bias = safe_float((whale or {}).get("bias")) or 0.0
        # band_expand (якщо доступно у stats)
        try:
            bex = st.get("band_expand")
            band_expand = float(bex) if bex is not None else None
        except Exception:
            band_expand = None
        # Початкові докази (best-effort): ключові фічі поточного бару
        _ev_add("near_edge", near_edge, 0.9)
        _ev_add("vol_z", volz, 0.8)
        _ev_add("dvr", dvr, 0.9)
        _ev_add("htf_strength", htf_strength, 1.0)
        _ev_add("presence", presence, 1.0)
        _ev_add("compression.index", band_expand or 0.0, 0.7)
        try:
            from utils.btc_regime import get_btc_regime_v2  # noqa: E402

            _ev_add(
                "btc_regime_v2", get_btc_regime_v2(dict(st), read_from_redis=True), 0.6
            )
        except Exception:
            pass

        # гейти з конфігу
        try:
            min_htf = float(HTF_GATES.get("ok", 0.20))
        except Exception:
            min_htf = 0.20
        # Оверрайд порогу через config: SCEN_HTF_MIN
        try:
            min_htf = float(_cfg_value("SCEN_HTF_MIN", min_htf))
        except Exception:
            pass
        from config.config import PROMOTE_REQ  # lazy import to avoid cycles

        try:
            pres_min = float(PROMOTE_REQ.get("min_presence_open", 0.60))  # type: ignore[arg-type]
        except Exception:
            pres_min = 0.60
        # Оверрайд через config: SCEN_PULLBACK_PRESENCE_MIN
        try:
            pres_min = float(_cfg_value("SCEN_PULLBACK_PRESENCE_MIN", pres_min))
        except Exception:
            pass
        require_bias = bool(PROMOTE_REQ.get("require_bias", True))  # type: ignore[arg-type]
        # Овертайд через config: SCEN_REQUIRE_BIAS
        try:
            require_bias = bool(_cfg_value("SCEN_REQUIRE_BIAS", require_bias))
        except Exception:
            pass

        # breakout_confirmation (post_breakout)
        if phase_name == "post_breakout":
            cond_near = (near_edge is True) or (
                isinstance(near_edge, str)
                and near_edge.lower() in {"upper", "lower", "near", "true"}
            )
            # Узгодження з офлайном: vol_z ≤ 1.0
            cond_volz = (volz is not None) and (volz <= 1.0)
            # Оверрайд порогу DVR через config: SCEN_BREAKOUT_DVR_MIN
            try:
                _dvr_min = float(_cfg_value("SCEN_BREAKOUT_DVR_MIN", 0.5))
            except Exception:
                _dvr_min = 0.5
            cond_dvr = (dvr is None) or (dvr >= _dvr_min)
            # У strict-режимі вимагаємо наявний HTF і поріг
            cond_htf = (htf_strength is not None) and (htf_strength >= min_htf)
            cond_band = True if band_expand is None else (band_expand >= 0.010)
            # Evidence: фіксуємо стани гейтів
            _ev_add("gate.near_edge", bool(cond_near), 1.0)
            _ev_add("gate.volz_ok", bool(cond_volz), 0.8)
            _ev_add("gate.dvr_ok", bool(cond_dvr), 0.9)
            _ev_add("gate.htf_ok", bool(cond_htf), 1.0)
            _ev_add("gate.band_ok", bool(cond_band), 0.6)
            if cond_near and cond_volz and cond_dvr and cond_htf and cond_band:
                # базова впевненість + легкий бонус за сильніші умови
                conf = 0.5
                if dvr and dvr >= 0.7:
                    conf += 0.1
                if volz is not None and volz <= 1.2:
                    conf += 0.05
                # htf_stale знижує на 0.10, але не скасовує
                try:
                    stale_ms = safe_float(st.get("htf_stale_ms"))
                    stale_lim = safe_float(st.get("htf_stale_limit_ms")) or _cfg_value(
                        "STAGE1_HTF_STALE_MS", STAGE1_HTF_STALE_MS
                    )
                    if (
                        stale_ms is not None
                        and stale_lim is not None
                        and stale_ms > stale_lim
                    ):
                        conf = max(0.0, conf - 0.10)
                        _ev_add("htf_stale", stale_ms, 0.4, "stale_ms>limit")
                except Exception:
                    pass
                _ev_add("candidate", "breakout_confirmation", float(conf))
                return ("breakout_confirmation", min(1.0, conf))

        # pullback_continuation (momentum)
        if phase_name == "momentum":
            # Дозволити відсутній presence як True (керується через config)
            try:
                _allow_na_pres = bool(_cfg_value("SCEN_PULLBACK_ALLOW_NA", False))
            except Exception:
                _allow_na_pres = False
            cond_pres = (presence is None and _allow_na_pres) or (
                presence is not None and presence >= pres_min
            )
            # У strict-режимі вимагаємо наявний HTF і поріг
            cond_htf = (htf_strength is not None) and (htf_strength >= min_htf)
            cond_bias = (abs(bias) > 0.0) if require_bias else True
            # Evidence: фіксуємо стани гейтів
            _ev_add("gate.presence_ok", bool(cond_pres), 1.0)
            _ev_add("gate.htf_ok", bool(cond_htf), 1.0)
            _ev_add("gate.bias_ok", bool(cond_bias), 0.5)
            if cond_pres and cond_htf and cond_bias:
                # конф — від 0.3 до 1.0 з урахуванням presence
                base = 0.3
                bonus = (
                    max(0.0, presence - pres_min) * 0.7
                )  # presence 0.60..1.0 → +0..0.28
                conf = min(1.0, base + bonus)
                _ev_add("candidate", "pullback_continuation", float(conf))
                return ("pullback_continuation", conf)

    except Exception:
        pass
    # Діагностичний relaxed-режим: дозволити м'якші сценарії навіть без точної фази
    # УВАГА: лише для коротких канарейкових перевірок, вимикайте після тестів.
    try:
        _relaxed = bool(_cfg_value("TEST_SCENARIO_SELECTOR_RELAXED", False))
    except Exception:
        _relaxed = False
    if _relaxed:
        try:
            # Переобчислимо мінімальні пороги, враховуючи можливі оверрайди вище
            try:
                min_htf = float(HTF_GATES.get("ok", 0.20))
            except Exception:
                min_htf = 0.20
            # Оверрайд через config: SCEN_HTF_MIN
            try:
                min_htf = float(_cfg_value("SCEN_HTF_MIN", min_htf))
            except Exception:
                pass
            from config.config import PROMOTE_REQ  # lazy import

            try:
                pres_min = float(PROMOTE_REQ.get("min_presence_open", 0.60))  # type: ignore[arg-type]
            except Exception:
                pres_min = 0.60
            # Оверрайд через config: SCEN_PULLBACK_PRESENCE_MIN
            try:
                pres_min = float(_cfg_value("SCEN_PULLBACK_PRESENCE_MIN", pres_min))
            except Exception:
                pass
            presence = safe_float(
                (stats or {}).get("whale", {}).get("presence")
                if isinstance(stats, Mapping)
                else None
            )
            bias = safe_float(
                (stats or {}).get("whale", {}).get("bias")
                if isinstance(stats, Mapping)
                else None
            )
            htf_strength = None
            try:
                htf_strength = (
                    float((stats or {}).get("htf_strength"))
                    if isinstance(stats, Mapping)
                    and (stats or {}).get("htf_strength") is not None
                    else None
                )
            except Exception:
                htf_strength = None
            near_edge = (
                (stats or {}).get("near_edge") if isinstance(stats, Mapping) else None
            )
            volz = safe_float(
                (stats or {}).get("volume_z") if isinstance(stats, Mapping) else None
            )
            dvr = safe_float(
                (stats or {}).get(K_DIRECTIONAL_VOLUME_RATIO)
                if isinstance(stats, Mapping)
                else None
            )
            try:
                _dvr_min = float(_cfg_value("SCEN_BREAKOUT_DVR_MIN", 0.5))
            except Exception:
                _dvr_min = 0.5
            # Relaxed pullback: допускаємо phase None | momentum при достатніх htf/presence
            try:
                allow_na_pres = bool(_cfg_value("SCEN_PULLBACK_ALLOW_NA", False))
            except Exception:
                allow_na_pres = False
            has_pres = (presence is not None and presence >= pres_min) or (
                presence is None and allow_na_pres
            )
            has_htf = (htf_strength is None) or (htf_strength >= min_htf)
            if has_pres and has_htf:
                base = 0.31
                bonus = (
                    max(0.0, (presence or 0.0) - pres_min) * 0.7
                    if presence is not None
                    else 0.0
                )
                return ("pullback_continuation", min(1.0, base + bonus))
            # Relaxed breakout: поблизу краю + DVR/volz/HTF
            cond_near = (near_edge is True) or (
                isinstance(near_edge, str)
                and near_edge.lower() in {"upper", "lower", "near", "true"}
            )
            cond_volz = (volz is None) or (volz <= 1.8)
            cond_dvr = (dvr is None) or (dvr >= _dvr_min)
            cond_htf = (htf_strength is None) or (htf_strength >= min_htf)
            if cond_near and cond_volz and cond_dvr and cond_htf:
                return ("breakout_confirmation", 0.41)
        except Exception:
            pass
    return (None, 0.0)
