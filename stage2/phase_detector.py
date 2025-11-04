"""Phase detector for Stage2.

Визначає ринкову фазу на основі легких ознак зі Stage1 stats та
контексту Stage2 (LevelManager/meta). Мінімізує залежності та
не змінює контрактів Stage2Output — лише додає поля в market_context.

Фази (статусно-агностичні, без внутрішнього state machine):
  - pre_breakout: вузький діапазон, назріває breakout (обсяг/дельта ростуть).
  - post_breakout: імовірний ретест після нещодавнього розширення діапазону.
  - momentum: продовження тренду після імпульсу (тяглість).
  - exhaustion: виснаження імпульсу (овер-розтяг, vol spike).
  - false_breakout: ознаки фальшивого пробою (суперечливі обсяги/HTF).

Notes:
  - Детектор працює на best-effort правилах і обережно повертає None,
    якщо даних замало, щоб не спричиняти жорсткі гейти.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import (
    EXH_STRATEGY_HINT_ENABLED_FLAG,
    K_CUMULATIVE_DELTA,
    K_DIRECTIONAL_VOLUME_RATIO,
    K_PRICE_SLOPE_ATR,
    PROMOTE_REQ,
    STAGE2_LOW_ATR_SYMBOL_GUARDS,
    STAGE2_PRESENCE_PROMOTION_GUARDS,
    STAGE2_PROFILE,
    STRICT_LOW_ATR_OVERRIDE_ON_SPIKE,
    STRICT_PROFILE_ENABLED,
    VOLZ_PROXY_ALLOWED_FOR_EXHAUSTION,
)
from config.config_stage2 import PARTICIPATION_LIGHT_THRESHOLDS, STAGE2_PHASE_THRESHOLDS
from config.flags import FEATURE_PARTICIPATION_LIGHT

# Деякі суворі конфігурації можуть бути відсутні в мінімальній збірці config;
# забезпечимо безпечні дефолти, щоб не падати при імпорті.
try:  # pragma: no cover — захист від середовищ без strict-конфігурації
    from config.config import STRONG_REGIME, USED_THRESH_TAG  # type: ignore
except Exception:  # pragma: no cover
    # Спроба альтернативного імпорту безпосередньо з config_stage2
    try:
        from config.config_stage2 import STRONG_REGIME, USED_THRESH_TAG  # type: ignore
    except Exception:  # pragma: no cover
        STRONG_REGIME = {}
        USED_THRESH_TAG = "legacy"
from utils.utils import safe_float

from .indicators import impulse_score
from .volatility import crisis_vol_score

# Логування
logger = logging.getLogger("stage2.phase_detector")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


STRATEGY_HINT_EXHAUSTION_REVERSAL = "exhaustion_reversal_long"


@dataclass
class PhaseResult:
    phase: str | None
    score: float
    reasons: list[str]
    volz_source: str | None = None  # 'light'|'raw'|'proxy'
    volz_proxy_components: dict[str, float] | None = None
    overlay: dict[str, float] | None = (
        None  # телеметрійні скори (impulse_up/down, crisis_vol, trap)
    )
    profile: str | None = None
    strategy_hint: str | None = None
    threshold_tag: str | None = None


def _get(ctx: Mapping[str, Any], *path: str) -> Any:
    cur: Any = ctx
    for key in path:
        if not isinstance(cur, Mapping):
            return None
        cur = cur.get(key)
    return cur


def _apply_phase_postprocessors(
    result: PhaseResult, locals_snapshot: Mapping[str, Any], symbol: str
) -> PhaseResult:
    """Застосувати додаткові постпроцесори до результату фази."""

    try:
        ctx = locals_snapshot.get("ctx")  # type: ignore[assignment]
    except Exception:
        ctx = None

    atr_ratio_val = None
    if isinstance(ctx, Mapping):
        atr_ratio_val = safe_float(_get(ctx, "meta", "atr_ratio"))

    low_override_active = bool(locals_snapshot.get("low_atr_override_active"))

    if (
        result.phase is None
        and atr_ratio_val is not None
        and atr_ratio_val < 1.0
        and not low_override_active
    ):
        reasons = list(result.reasons or [])
        if "low_atr_guard" not in reasons:
            reasons.append("low_atr_guard")

        overlay_payload: dict[str, Any] = dict(result.overlay or {})
        postprocessors = overlay_payload.get("postprocessors")
        if isinstance(postprocessors, list):
            if "accum_monitor" not in postprocessors:
                postprocessors.append("accum_monitor")
        else:
            overlay_payload["postprocessors"] = ["accum_monitor"]

        try:
            logger.info(
                "[STRICT_GUARD] symbol=%s assign=accum_monitor atr_ratio=%.4f",
                symbol,
                atr_ratio_val,
            )
        except Exception:
            pass

        result.phase = "accum_monitor"
        result.score = 0.0
        result.reasons = reasons
        result.overlay = overlay_payload

    return result


def detect_phase(
    stats: Mapping[str, Any] | None, context: Mapping[str, Any] | None
) -> PhaseResult:
    """Грубий детектор ринкової фази.

    Args:
        stats: Stage1.stats (плоскі метрики).
        context: Stage2 market_context з LevelManager meta.

    Returns:
        PhaseResult: phase у {pre_breakout, post_breakout, momentum, exhaustion, false_breakout} або None.

    Пояснення:
        Функція робить best-effort висновок фази на основі "легких" ознак.
        Вона навмисно консервативна: повертає None якщо даних замало або ознаки суперечливі.
        Важливо: не змінює контракт Stage2Output — лише формується PhaseResult для market_context.

    TODO:
        - Додати unit-тести: happy-path для кожної фази + псевдо-стрім-тест для pre_breakout.
        - Перевірити вплив параметрів з STAGE2_PHASE_THRESHOLDS на PnL (A/B тест).
        - Розглянути експериментальну політику для volz_source == "proxy" (зараз воно відсікає exhaustion/false_breakout).
    """
    stats = stats or {}
    ctx = context or {}

    # Ідентифікатор інструменту для логування (не критично для логіки)
    symbol = _get(ctx, "symbol") or stats.get("symbol") or "unknown"
    logger.info("Початок детекції фази для %s", symbol)

    strict_profile_active = bool(STAGE2_PROFILE == "strict" and STRICT_PROFILE_ENABLED)
    profile_tag = "strict" if strict_profile_active else "legacy"
    base_thresholds = (
        STRONG_REGIME.get("phase_thresholds", {})
        if strict_profile_active
        else STAGE2_PHASE_THRESHOLDS or {}
    )
    if isinstance(base_thresholds, Mapping):
        thr: dict[str, Any] = dict(base_thresholds)
    else:
        thr = {}
    if strict_profile_active:
        impulse_overlay_cfg = STRONG_REGIME.get("impulse_overlay", {}) or {}
        try:
            impulse_slope = float(impulse_overlay_cfg.get("slope_min", 1.5))
        except Exception:
            impulse_slope = 1.5
        try:
            impulse_vwap_abs = float(impulse_overlay_cfg.get("vwap_dev_abs_min", 0.01))
        except Exception:
            impulse_vwap_abs = 0.01
        try:
            impulse_vol_z_max = float(impulse_overlay_cfg.get("vol_z_max", 1.8))
        except Exception:
            impulse_vol_z_max = 1.8
        thr["impulse_down"] = {
            "slope_min": impulse_slope,
            "vwap_dev_max": -abs(impulse_vwap_abs),
            "vol_z_max": impulse_vol_z_max,
        }
        thr["impulse_up"] = {
            "slope_min": impulse_slope,
            "vwap_dev_min": abs(impulse_vwap_abs),
            "vol_z_max": impulse_vol_z_max,
        }
    else:
        thr = thr or {}

    allow_proxy_exhaustion = bool(VOLZ_PROXY_ALLOWED_FOR_EXHAUSTION)
    if strict_profile_active:
        ex_cfg = thr.get("exhaustion")
        if (
            isinstance(ex_cfg, Mapping)
            and "deny_proxy_volz" in ex_cfg
            and not allow_proxy_exhaustion
        ):
            allow_proxy_exhaustion = not bool(ex_cfg.get("deny_proxy_volz"))

    threshold_tag = USED_THRESH_TAG if strict_profile_active else "legacy"

    # -------------------------
    # Витяг ознак (light-weight features)
    # -------------------------
    # band_pct: відношення ширини бенду/діапазону (0..1)
    band_pct = safe_float(_get(ctx, "key_levels_meta", "band_pct"))
    is_near_edge = _get(ctx, "key_levels_meta", "is_near_edge")
    # htf_ok: булева індикація сумісності з вищими таймфреймами (True=підтверджено)
    htf_ok = _get(ctx, "meta", "htf_ok")
    htf_strength = safe_float(_get(ctx, "meta", "htf_strength"))

    # Напрямковий об'єм та кумулятивна дельта (можуть бути None)
    dvr = safe_float(_get(ctx, "directional", K_DIRECTIONAL_VOLUME_RATIO))
    cd = safe_float(_get(ctx, "directional", K_CUMULATIVE_DELTA))
    slope_atr = safe_float(_get(ctx, "directional", K_PRICE_SLOPE_ATR))

    # Volz: намагаємось взяти "light" версію, якщо її немає — fallback на "raw"
    volz_v1 = safe_float(stats.get("volume_z_light"))
    volz_v2 = safe_float(stats.get("vol_z"))
    volz = volz_v1 if volz_v1 is not None else volz_v2
    volz_source: str | None = None
    if volz_v1 is not None:
        volz_source = "light"
    elif volz_v2 is not None:
        volz_source = "raw"

    # Effective volz aggregator: prefer median of recent volz samples if provided
    volz_eff: float | None = None
    try:
        # Accept either a list of samples in stats['vol_z_samples'] or stats['vol_z_history']
        samples = None
        if isinstance(stats.get("vol_z_samples"), list):
            samples = [safe_float(x) for x in stats.get("vol_z_samples")]
        elif isinstance(stats.get("vol_z_history"), list):
            samples = [safe_float(x) for x in stats.get("vol_z_history")]
        # Filter valid floats
        if isinstance(samples, list):
            vals = [float(x) for x in samples if isinstance(x, (int, float))]
            if vals:
                vals_sorted = sorted(vals)
                m = len(vals_sorted)
                if m % 2 == 1:
                    volz_eff = float(vals_sorted[m // 2])
                else:
                    volz_eff = float(
                        (vals_sorted[m // 2 - 1] + vals_sorted[m // 2]) / 2.0
                    )
                volz_source = volz_source or "history"
    except Exception:
        volz_eff = None

    # RSI: віддаємо перевагу швидкому RSI якщо є
    rsi_v1 = safe_float(stats.get("rsi_fast"))
    rsi_v2 = safe_float(stats.get("rsi"))
    rsi = rsi_v1 if rsi_v1 is not None else rsi_v2

    # ATR відносно ціни — відсоткова міра волатильності
    atr_pct = None
    cp = safe_float(stats.get("current_price"))
    atr = safe_float(stats.get("atr"))
    if cp and atr and cp > 0:
        atr_pct = atr / cp
    else:
        atr_pct = safe_float(stats.get("atr_pct"))

    whale_ctx = _get(ctx, "meta", "whale") or {}
    vwap_dev_whale = safe_float(whale_ctx.get("vwap_deviation"))
    whale_presence = safe_float(whale_ctx.get("presence_score"))
    whale_bias = safe_float(whale_ctx.get("whale_bias"))
    dominance_up_flag = bool(whale_ctx.get("dominance_up"))
    dominance_down_flag = bool(whale_ctx.get("dominance_down"))
    dominance_up_source = whale_ctx.get("dominance_up_source")
    dominance_down_source = whale_ctx.get("dominance_down_source")
    dominance_alt_hits = safe_float(whale_ctx.get("dominance_alt_hits"))
    dominance_alt_ready = bool(whale_ctx.get("dominance_alt_ready"))

    symbol_upper = str(symbol).upper() if isinstance(symbol, str) else str(symbol)
    guard_cfg = dict(STAGE2_PRESENCE_PROMOTION_GUARDS or {})
    try:
        min_bias_abs = float(guard_cfg.get("min_bias_abs", 0.15) or 0.15)
    except Exception:
        min_bias_abs = 0.15
    try:
        min_htf_strength_base = float(guard_cfg.get("min_htf_strength", 0.20) or 0.20)
    except Exception:
        min_htf_strength_base = 0.20
    try:
        cap_without_confirm = float(guard_cfg.get("cap_without_confirm", 0.35) or 0.35)
    except Exception:
        cap_without_confirm = 0.35

    try:
        low_atr_cfg_raw = (STAGE2_LOW_ATR_SYMBOL_GUARDS or {}).get(symbol_upper, {})
    except Exception:
        low_atr_cfg_raw = {}
    low_atr_cfg = dict(low_atr_cfg_raw) if isinstance(low_atr_cfg_raw, Mapping) else {}
    min_htf_strength = min_htf_strength_base
    latr_min_val = low_atr_cfg.get("min_htf_strength")
    if isinstance(latr_min_val, (int, float)):
        try:
            min_htf_strength = max(min_htf_strength, float(latr_min_val))
        except Exception:
            pass

    presence_val = (
        float(whale_presence) if isinstance(whale_presence, (int, float)) else None
    )
    bias_val = float(whale_bias) if isinstance(whale_bias, (int, float)) else None
    htf_strength_val = (
        float(htf_strength) if isinstance(htf_strength, (int, float)) else None
    )

    bias_ok = bias_val is not None and abs(bias_val) >= min_bias_abs
    htf_strength_ok = (
        htf_strength_val is not None and htf_strength_val >= min_htf_strength
    )

    guard_notes: list[str] = []
    guard_block = False

    if (
        presence_val is not None
        and presence_val > cap_without_confirm
        and not (bias_ok and htf_strength_ok)
    ):
        guard_notes.append("presence_cap_no_bias_htf")
        capped_presence = float(min(presence_val, cap_without_confirm))
        try:
            logger.info(
                "[STRICT_GUARD] symbol=%s guard=presence_cap_no_bias_htf presence=%.3f -> %.3f bias=%.3f htf_strength=%.3f",
                symbol,
                presence_val,
                capped_presence,
                bias_val if bias_val is not None else float("nan"),
                htf_strength_val if htf_strength_val is not None else float("nan"),
            )
        except Exception:
            pass
        whale_presence = capped_presence
        if isinstance(whale_ctx, dict):
            whale_ctx["presence_score"] = capped_presence
        presence_val = capped_presence

    try:
        min_presence_open = float(
            (PROMOTE_REQ or {}).get("min_presence_open", 0.60) or 0.60
        )
    except Exception:
        min_presence_open = 0.60

    atr_pct_thr = low_atr_cfg.get("atr_pct_max")
    if isinstance(atr_pct_thr, (int, float)) and isinstance(atr_pct, (int, float)):
        if float(atr_pct) <= float(atr_pct_thr):
            guard_notes.append("low_atr_guard")
            presence_cap_low = low_atr_cfg.get("presence_cap")
            try:
                presence_cap_low_val = (
                    float(presence_cap_low)
                    if presence_cap_low is not None
                    else cap_without_confirm
                )
            except Exception:
                presence_cap_low_val = cap_without_confirm
            if presence_val is not None and presence_val > presence_cap_low_val:
                try:
                    logger.info(
                        "[STRICT_GUARD] symbol=%s guard=low_atr_presence presence=%.3f -> %.3f atr_pct=%.5f thr=%.5f",
                        symbol,
                        presence_val,
                        presence_cap_low_val,
                        float(atr_pct),
                        float(atr_pct_thr),
                    )
                except Exception:
                    pass
                whale_presence = presence_cap_low_val
                if isinstance(whale_ctx, dict):
                    whale_ctx["presence_score"] = presence_cap_low_val
                presence_val = presence_cap_low_val
            guard_block = True

    low_atr_override_ctx: Mapping[str, Any] | None = None
    if isinstance(stats, Mapping):
        overrides_map = stats.get("overrides")
        if isinstance(overrides_map, Mapping):
            candidate = overrides_map.get("low_atr_spike")
            if isinstance(candidate, Mapping):
                low_atr_override_ctx = candidate
        if low_atr_override_ctx is None:
            candidate_legacy = stats.get("low_atr_override")
            if isinstance(candidate_legacy, Mapping):
                low_atr_override_ctx = candidate_legacy

    low_atr_override_active = False
    ttl_int = 0
    if isinstance(low_atr_override_ctx, Mapping):
        try:
            ttl_val = low_atr_override_ctx.get("ttl")
            ttl_int = int(ttl_val) if ttl_val is not None else 0
        except Exception:
            ttl_int = 0
        low_atr_override_active = (
            bool(low_atr_override_ctx.get("active")) and ttl_int > 0
        )

    if guard_block and STRICT_LOW_ATR_OVERRIDE_ON_SPIKE and low_atr_override_active:
        presence_ready = presence_val is not None and float(presence_val) >= float(
            min_presence_open
        )
        if presence_ready and bias_ok and htf_strength_ok:
            guard_block = False
            guard_notes.append("low_atr_override")
            try:
                logger.info(
                    "[LOW_ATR_OVERRIDE] symbol=%s active ttl=%s metrics=%s",
                    symbol,
                    ttl_int,
                    low_atr_override_ctx.get("metrics"),
                )
            except Exception:
                pass
        else:
            try:
                logger.info(
                    "[LOW_ATR_OVERRIDE] symbol=%s blocked presence_ready=%s bias_ok=%s htf_ok=%s ttl=%s",
                    symbol,
                    str(presence_ready),
                    str(bias_ok),
                    str(htf_strength_ok),
                    ttl_int,
                )
            except Exception:
                pass

    # Пороги з конфігу (можуть бути None => подальші блоки ставлять дефолти)

    # Вузький HTF-override: за чітких імпульсних умов дозволяємо м'яке True для htf_ok
    try:
        if strict_profile_active:
            ov = (STRONG_REGIME or {}).get("overrides", {}) or {}
            htf_ov = (ov.get("htf_override") or {}) if isinstance(ov, Mapping) else {}
            slope_thr = float(htf_ov.get("slope_atr_min", 1.6))
            pres_thr = float(htf_ov.get("presence_min", 0.60))
            bias_abs_thr = float(htf_ov.get("bias_abs_min", 0.30))
            volz_max_thr = float(htf_ov.get("vol_z_max", 1.6))
            dvr_min_thr = float(htf_ov.get("dvr_min", 0.30))
            near_req = bool(htf_ov.get("near_edge_required", True))

            cond_slope = (
                isinstance(slope_atr, (int, float)) and float(slope_atr) >= slope_thr
            )
            cond_pres = (
                isinstance(whale_presence, (int, float))
                and float(whale_presence) >= pres_thr
            )
            cond_bias = (
                isinstance(whale_bias, (int, float))
                and abs(float(whale_bias)) >= bias_abs_thr
            )
            cond_volz = (volz is None) or (
                isinstance(volz, (int, float)) and float(volz) <= volz_max_thr
            )
            cond_dvr = (dvr is None) or (
                isinstance(dvr, (int, float)) and float(dvr) >= dvr_min_thr
            )
            cond_edge = (is_near_edge is True) if near_req else True

            if (
                cond_slope
                and cond_pres
                and cond_bias
                and cond_volz
                and cond_dvr
                and cond_edge
            ):
                if htf_ok is not True:  # лише якщо не true
                    htf_ok = True
                    try:
                        logger.info(
                            "[STRICT_SANITY] htf_override_applied symbol=%s slope=%.2f presence=%.2f bias=%.2f volz=%s dvr=%s near_edge=%s",
                            symbol,
                            (
                                float(slope_atr)
                                if isinstance(slope_atr, (int, float))
                                else float("nan")
                            ),
                            (
                                float(whale_presence)
                                if isinstance(whale_presence, (int, float))
                                else float("nan")
                            ),
                            (
                                float(whale_bias)
                                if isinstance(whale_bias, (int, float))
                                else float("nan")
                            ),
                            (
                                f"{float(volz):.2f}"
                                if isinstance(volz, (int, float))
                                else "na"
                            ),
                            (
                                f"{float(dvr):.2f}"
                                if isinstance(dvr, (int, float))
                                else "na"
                            ),
                            is_near_edge,
                        )
                    except Exception:
                        pass
    except Exception:
        # Нічого не робимо — override best-effort
        pass

    # -------------------------
    # Proxy volz: якщо volz відсутній — пробуємо скласти проксі з directional metrics
    # -------------------------
    volz_proxy_used = False
    proxy_components: dict[str, float] | None = None
    if volz is None:
        try:
            # Простий агрегат: відхилення dvr від 1.0, absolute(cd) та slope_abs дають індикатор активності
            if isinstance(dvr, (int, float)) and isinstance(cd, (int, float)):
                dvr_dev = abs(float(dvr) - 1.0)
                cd_abs = abs(float(cd))
                slope_abs = abs(float(slope_atr) if slope_atr is not None else 0.0)
                # TODO: експериментувати з вагами (1.5, 0.3, 0.7) та межею 1.6
                volz_raw = 0.4 + 0.6 * (
                    min(2.0, dvr_dev * 1.5 + cd_abs * 0.3 + slope_abs * 0.7)
                )
                volz = float(min(1.6, volz_raw))
                volz_proxy_used = True
                volz_source = "proxy"
                proxy_components = {
                    "dvr_dev": float(dvr_dev),
                    "cd_abs": float(cd_abs),
                    "slope_abs": float(slope_abs),
                }
                # TODO: записувати proxy_components в лог/метрики для аналізу
        except Exception:
            # Невелика захисна заглушка — не ламаємо основну логіку
            pass

    # Логування витягнутих значень і порогів для дебагу (DEBUG)
    logger.debug(
        "features: symbol=%s band_pct=%s is_near_edge=%s htf_ok=%s dvr=%s cd=%s slope_atr=%s volz=%s rsi=%s atr_pct=%s cp=%s atr=%s",
        symbol,
        band_pct,
        is_near_edge,
        htf_ok,
        dvr,
        cd,
        slope_atr,
        volz,
        rsi,
        atr_pct,
        cp,
        atr,
    )
    logger.info(
        "[STRICT_PHASE_INPUTS] symbol=%s volz=%s volz_eff=%s volz_source=%s proxy_used=%s",
        symbol,
        volz,
        volz_eff,
        volz_source,
        volz_proxy_used,
    )
    # Додатковий STRICT‑лог для джерела volz, якщо використано проксі‑оцінку
    try:
        if str(volz_source).lower() == "proxy":
            logger.info(
                "[STRICT_VOLZ] symbol=%s source=proxy volz=%.3f dvr=%s cd=%s slope_atr=%s",
                symbol,
                float(volz) if isinstance(volz, (int, float)) else float("nan"),
                dvr,
                cd,
                slope_atr,
            )
    except Exception:
        pass
    logger.debug("thresholds snapshot: %s", thr)

    reasons: list[str] = []

    # Універсальний телеметрійний overlay (impulse/crisis/trap)
    trap_score_o = 0.0
    try:
        dir_ctx = _get(ctx, "directional") or {}
        dvr_o = safe_float(dir_ctx.get(K_DIRECTIONAL_VOLUME_RATIO))
        cd_o = safe_float(dir_ctx.get(K_CUMULATIVE_DELTA))
        slope_o = safe_float(dir_ctx.get(K_PRICE_SLOPE_ATR))
        imp_o = impulse_score(slope_o, dvr_o, cd_o)
        imp_up_o = (
            imp_o
            if (isinstance(slope_o, (int, float)) and float(slope_o) >= 0.0)
            else 0.0
        )
        imp_dn_o = (
            imp_o
            if (isinstance(slope_o, (int, float)) and float(slope_o) < 0.0)
            else 0.0
        )
        atr_ratio_o = safe_float(_get(ctx, "meta", "atr_ratio"))
        crisis_o = crisis_vol_score(atr_ratio_o)
        trap_ctx_o = _get(ctx, "meta", "trap") or {}
        try:
            trap_score_o = float(
                getattr(trap_ctx_o, "get", dict.get)(trap_ctx_o, "trap_score", 0.0)
            )
        except Exception:
            trap_score_o = 0.0
        # Підсвітити імпульс сильніше у вузькому бенді: застосувати поріг 0.70
        imp_up_v = float(round(imp_up_o, 3))
        imp_dn_v = float(round(imp_dn_o, 3))
        if isinstance(band_pct, (int, float)) and float(band_pct) <= 0.30:
            imp_up_v = imp_up_v if imp_up_v >= 0.70 else 0.0
            imp_dn_v = imp_dn_v if imp_dn_v >= 0.70 else 0.0
        overlay_o: dict[str, float] | None = {
            "impulse_up": imp_up_v,
            "impulse_down": imp_dn_v,
            "crisis_vol_score": float(round(crisis_o, 3)),
            "trap_score": float(round(trap_score_o, 3)),
            "volz_eff": (
                float(round(volz_eff, 3))
                if isinstance(volz_eff, (int, float))
                else (float(volz) if isinstance(volz, (int, float)) else 0.0)
            ),
            "dominance_up": 1.0 if dominance_up_flag else 0.0,
            "dominance_down": 1.0 if dominance_down_flag else 0.0,
            "dominance_alt_hits": float(dominance_alt_hits or 0.0),
            "dominance_alt_ready": 1.0 if dominance_alt_ready else 0.0,
        }
    except Exception:
        overlay_o = None

    if guard_block:
        if guard_notes:
            reasons.extend(guard_notes)
        try:
            logger.info(
                "[STRICT_GUARD] symbol=%s guard=low_atr_block presence=%.3f bias=%.3f htf_strength=%.3f atr_pct=%s notes=%s",
                symbol,
                presence_val if presence_val is not None else float("nan"),
                bias_val if bias_val is not None else float("nan"),
                htf_strength_val if htf_strength_val is not None else float("nan"),
                (
                    f"{float(atr_pct):.6f}"
                    if isinstance(atr_pct, (int, float))
                    else "na"
                ),
                "+".join(guard_notes),
            )
        except Exception:
            pass
        return _apply_phase_postprocessors(
            PhaseResult(
                None,
                0.0,
                reasons,
                volz_source,
                proxy_components,
                overlay_o,
                profile=profile_tag,
                threshold_tag=threshold_tag,
            ),
            locals(),
            str(symbol),
        )
    if guard_notes:
        reasons.extend(guard_notes)

    # -------------------------
    # 0) Імпульсні стани — сильне домінування сторони ринку
    # -------------------------
    imp_dn_cfg = thr.get("impulse_down", {}) or {}
    imp_up_cfg = thr.get("impulse_up", {}) or {}
    try:
        imp_dn_slope = float(imp_dn_cfg.get("slope_min", 1.5))
        imp_dn_vwap = float(imp_dn_cfg.get("vwap_dev_max", -0.01))
        imp_dn_volz = float(imp_dn_cfg.get("vol_z_max", 1.8))
    except Exception:
        imp_dn_slope, imp_dn_vwap, imp_dn_volz = 1.5, -0.01, 1.8
    try:
        imp_up_slope = float(imp_up_cfg.get("slope_min", 1.5))
        imp_up_vwap = float(imp_up_cfg.get("vwap_dev_min", 0.01))
        imp_up_volz = float(imp_up_cfg.get("vol_z_max", 1.8))
    except Exception:
        imp_up_slope, imp_up_vwap, imp_up_volz = 1.5, 0.01, 1.8

    if (
        dominance_down_flag
        and slope_atr is not None
        and slope_atr <= -imp_dn_slope
        and vwap_dev_whale is not None
        and vwap_dev_whale <= imp_dn_vwap
        and (volz is None or volz <= imp_dn_volz)
        and float(overlay_o.get("impulse_down", 0.0) if overlay_o else imp_dn_o) >= 0.55
    ):
        score = float(
            max(0.8, min(0.95, (overlay_o or {}).get("impulse_down", imp_dn_o) or 0.85))
        )
        reasons.extend(
            [
                "whale_dominance",
                "slope_extreme",
                "vwap_under",
                "vol_ok" if volz is None or volz <= imp_dn_volz else "vol_unknown",
            ]
        )
        if dominance_down_source == "alt":
            reasons.append("dominance_alt_path")
        logger.info(
            "Фаза виявлена: impulse_down для %s (score=%.2f) reasons=%s",
            symbol,
            score,
            reasons,
        )
        return PhaseResult(
            "impulse_down",
            score,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            threshold_tag=threshold_tag,
        )

    if (
        dominance_up_flag
        and slope_atr is not None
        and slope_atr >= imp_up_slope
        and vwap_dev_whale is not None
        and vwap_dev_whale >= imp_up_vwap
        and (volz is None or volz <= imp_up_volz)
        and float(overlay_o.get("impulse_up", 0.0) if overlay_o else imp_up_o) >= 0.55
    ):
        score = float(
            max(0.8, min(0.95, (overlay_o or {}).get("impulse_up", imp_up_o) or 0.85))
        )
        reasons.extend(
            [
                "whale_dominance",
                "slope_extreme",
                "vwap_above",
                "vol_ok" if volz is None or volz <= imp_up_volz else "vol_unknown",
            ]
        )
        if dominance_up_source == "alt":
            reasons.append("dominance_alt_path")
        logger.info(
            "Фаза виявлена: impulse_up для %s (score=%.2f) reasons=%s",
            symbol,
            score,
            reasons,
        )
        return PhaseResult(
            "impulse_up",
            score,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            threshold_tag=threshold_tag,
        )

    # -------------------------
    # 1) Exhaustion — ознаки виснаження імпульсу
    # -------------------------
    # Важливі умови: широкий band_pct, великий volz, високий RSI або альтернативою - ATR spike
    ex = thr.get("exhaustion", {})
    try:
        ex_band = float(ex.get("band_min", 0.45))
    except Exception:
        ex_band = 0.45
    try:
        ex_volz = float(ex.get("vol_z_min", 2.0))
    except Exception:
        ex_volz = 2.0
    try:
        ex_rsi_hi = float(ex.get("rsi_hi", 70))
    except Exception:
        ex_rsi_hi = 70.0
    try:
        ex_alt_mult_raw = ex.get("alt_atr_mult")
        if ex_alt_mult_raw is None:
            ex_alt_mult_raw = ex.get("atr_mult_low_gate", 1.75)
        ex_alt_mult = float(ex_alt_mult_raw)
    except Exception:
        ex_alt_mult = 1.75
    try:
        ex_short_trap = float(ex.get("short_trap_min", 0.67))
    except Exception:
        ex_short_trap = 0.67
    try:
        ex_short_slope = float(ex.get("short_slope_abs", 0.6))
    except Exception:
        ex_short_slope = 0.6
    try:
        ex_short_turnover = float(ex.get("short_turnover_min", 3_000_000.0))
    except Exception:
        ex_short_turnover = 3_000_000.0
    try:
        ex_short_rsi = float(ex.get("short_rsi_max", 35.0))
    except Exception:
        ex_short_rsi = 35.0
    try:
        ex_short_spike = float(ex.get("short_spike_ratio", 3.0))
    except Exception:
        ex_short_spike = 3.0
    logger.debug(
        "exhaustion thresholds: band_min=%s vol_z_min=%s rsi_hi=%s alt_mult=%s allow_proxy=%s",
        ex_band,
        ex_volz,
        ex_rsi_hi,
        ex_alt_mult,
        allow_proxy_exhaustion,
    )

    # Альтернативна умова: великий ATR відносно локального low_gate + high RSI
    ex_alt_hit = False
    try:
        if atr_pct is not None and rsi is not None:
            base_low_gate = max(
                0.0, safe_float(_get(ctx, "meta", "low_gate_effective")) or 0.0
            )
            if atr_pct >= base_low_gate * ex_alt_mult and rsi >= ex_rsi_hi:
                ex_alt_hit = True
    except Exception:
        ex_alt_hit = False

    # Додаткові safety-умови: HTF проти або CD flip (<=0) роблять exhaustion більш імовірним
    htf_ok_flag = _get(ctx, "meta", "htf_ok")
    cd_val = cd
    htf_or_cd_ok = (isinstance(htf_ok_flag, bool) and htf_ok_flag is False) or (
        isinstance(cd_val, (int, float)) and float(cd_val) <= 0.0
    )
    cond_ex = (
        (band_pct is not None and band_pct >= ex_band)
        and (volz is not None and volz >= ex_volz)
        and (rsi is not None and rsi >= ex_rsi_hi)
    ) or ex_alt_hit

    near_edge_val = stats.get("near_edge")
    trap_stats = stats.get("trap") if isinstance(stats.get("trap"), Mapping) else {}
    try:
        trap_score_stats = safe_float(trap_stats.get("score"))
    except Exception:
        trap_score_stats = None
    if trap_score_stats is None:
        trap_score_stats = safe_float(stats.get("trap_score"))
    turnover_usd = safe_float(stats.get("turnover_usd"))
    atr_pct_p50 = safe_float(stats.get("atr_pct_p50"))
    spike_ratio = None
    try:
        if isinstance(atr_pct, (int, float)) and isinstance(atr_pct_p50, (int, float)):
            if atr_pct_p50 > 0:
                spike_ratio = float(atr_pct) / float(atr_pct_p50)
    except Exception:
        spike_ratio = None
    cond_ex_short = (
        (trap_score_stats is not None and trap_score_stats >= ex_short_trap)
        and (turnover_usd is not None and turnover_usd >= ex_short_turnover)
        and (
            isinstance(slope_atr, (int, float)) and float(slope_atr) <= -ex_short_slope
        )
        and (
            isinstance(near_edge_val, str)
            and near_edge_val.lower() == "lower"
            or near_edge_val is True
        )
        and (
            rsi is None
            or (isinstance(rsi, (int, float)) and float(rsi) <= ex_short_rsi)
        )
        and (spike_ratio is not None and spike_ratio >= ex_short_spike)
    )
    if cond_ex_short:
        reasons.extend(
            [
                "trap_extreme",
                "turnover_heavy",
                "slope_down",
                "spike_ratio",
            ]
        )
        if rsi is not None and float(rsi) <= ex_short_rsi:
            reasons.append("rsi_exhausted")
        strategy_hint = None
        if EXH_STRATEGY_HINT_ENABLED_FLAG:
            volz_guard = False
            if isinstance(volz, (int, float)) and float(volz) >= ex_volz:
                volz_guard = True
            elif isinstance(volz_eff, (int, float)) and float(volz_eff) >= ex_volz:
                volz_guard = True
            dvr_guard = True
            if isinstance(dvr, (int, float)):
                dvr_guard = float(dvr) <= 1.35
            if volz_guard and dvr_guard:
                strategy_hint = STRATEGY_HINT_EXHAUSTION_REVERSAL
                reasons.append(f"strategy_hint_{STRATEGY_HINT_EXHAUSTION_REVERSAL}")
                try:
                    logger.info(
                        "[STRICT_PHASE] strategy_hint=%s symbol=%s volz=%s dvr=%s spike_ratio=%s trap=%.2f",
                        strategy_hint,
                        symbol,
                        (
                            f"{float(volz):.2f}"
                            if isinstance(volz, (int, float))
                            else (
                                f"{float(volz_eff):.2f}"
                                if isinstance(volz_eff, (int, float))
                                else "na"
                            )
                        ),
                        (
                            f"{float(dvr):.2f}"
                            if isinstance(dvr, (int, float))
                            else "na"
                        ),
                        (
                            f"{float(spike_ratio):.2f}"
                            if isinstance(spike_ratio, (int, float))
                            else "na"
                        ),
                        (
                            float(trap_score_stats)
                            if isinstance(trap_score_stats, (int, float))
                            else float("nan")
                        ),
                    )
                except Exception:
                    pass
        score = 0.95
        logger.info(
            "Фаза виявлена: exhaustion для %s (score=%.2f) reasons=%s",
            symbol,
            score,
            reasons,
        )
        return PhaseResult(
            "exhaustion",
            score,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            strategy_hint=strategy_hint,
            threshold_tag=threshold_tag,
        )

    # Заборона для proxy-джерела: не повертаємо exhaustion при volz_source='proxy'
    # Пояснення: proxy волюме може бути шумним і призводити до false positive; можна пом'якшити в майбутньому.
    if cond_ex and htf_or_cd_ok and (allow_proxy_exhaustion or volz_source != "proxy"):
        score = 0.9
        reasons.extend(
            [
                "band_wide",
                "vol_spike" if not ex_alt_hit else "atr_spike_proxy",
                "rsi_extreme",
            ]
        )
        if volz_proxy_used and allow_proxy_exhaustion and volz_source == "proxy":
            reasons.append("proxy_volz_allowed")
        logger.info(
            "Фаза виявлена: exhaustion для %s (score=%.2f) reasons=%s",
            symbol,
            score,
            reasons,
        )
        # Повертаємо без overlay — нижче додамо універсальний блок overlay перед фінальним return
        return PhaseResult(
            "exhaustion",
            score,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            threshold_tag=threshold_tag,
        )

    # -------------------------
    # 2) False-breakout — конфліктні сигнали (слабкий directional volume + HTF conflict)
    # -------------------------
    fb = thr.get("false_breakout", {})
    try:
        fb_dvr_max = float(fb.get("dvr_max", 1.2))  # дозволимо шкалу біля 1.0
        fb_cd_sign = float(fb.get("cd_against_thresh", 0.0))
    except Exception:
        fb_dvr_max, fb_cd_sign = 1.2, 0.0
    logger.debug(
        "false_breakout thresholds: dvr_max=%s cd_against_thresh=%s",
        fb_dvr_max,
        fb_cd_sign,
    )

    # Умова: dvr низький, cd проти напрямку і HTF показує конфлікт
    if (
        (dvr is not None and dvr <= fb_dvr_max)
        and (cd is not None and cd <= fb_cd_sign)
        and (isinstance(htf_ok, bool) and htf_ok is False)
    ) and (volz_source != "proxy"):
        score = 0.8
        reasons.extend(["weak_directional_volume", "cd_against", "htf_conflict"])
        logger.info(
            "Фаза виявлена: false_breakout для %s (score=%.2f) reasons=%s",
            symbol,
            score,
            reasons,
        )
        return PhaseResult(
            "false_breakout",
            score,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            threshold_tag=threshold_tag,
        )

    # -------------------------
    # 3) Momentum — продовження тренду після імпульсу
    # -------------------------
    mm = thr.get("momentum", {})
    try:
        mm_slope = float(mm.get("slope_min", 0.5))
        mm_rsi_lo = float(mm.get("rsi_lo", 40))  # ширше вікно
        mm_rsi_hi = float(mm.get("rsi_hi", 65))
        mm_cd = float(mm.get("cd_min", 0.2))
        mm_volz = float(mm.get("vol_z_max", 1.2))
    except Exception:
        mm_slope, mm_rsi_lo, mm_rsi_hi, mm_cd, mm_volz = 0.5, 40.0, 65.0, 0.2, 1.2
    logger.debug(
        "momentum thresholds: slope_min=%s rsi_lo=%s rsi_hi=%s cd_min=%s vol_z_max=%s",
        mm_slope,
        mm_rsi_lo,
        mm_rsi_hi,
        mm_cd,
        mm_volz,
    )

    # Momentum: сильний slope, середній RSI, позитивний CD, помірний/низький volz, HTF підтверджує
    if (
        (slope_atr is not None and slope_atr >= mm_slope)
        and (rsi is not None and mm_rsi_lo <= rsi <= mm_rsi_hi)
        and (cd is not None and cd >= mm_cd)
        and (volz is None or volz <= mm_volz)
        and (isinstance(htf_ok, bool) and htf_ok is True)
    ):
        score = 0.75
        reasons.extend(["slope_strong", "rsi_mid", "cd_ok", "htf_ok"])
        if volz_proxy_used:
            reasons.append("volz_proxy")
        logger.info(
            "Фаза виявлена: momentum для %s (score=%.2f) reasons=%s",
            symbol,
            score,
            reasons,
        )
        return PhaseResult(
            "momentum",
            score,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            threshold_tag=threshold_tag,
        )

    # -------------------------
    # 3b) Drift-trend — participation-light (quiet mode)
    #     Лише телеметрійна класифікація без гейтів: м'який трендовий дрейф,
    #     коли ознаки схожі на momentum, але участь обсягу не дотягує до сильного breakout.
    # -------------------------
    if FEATURE_PARTICIPATION_LIGHT:
        dt_cfg = (
            PARTICIPATION_LIGHT_THRESHOLDS
            if isinstance(PARTICIPATION_LIGHT_THRESHOLDS, Mapping)
            else {}
        )
        try:
            dt_volz_min = float(dt_cfg.get("vol_z_min", 1.0))
        except Exception:
            dt_volz_min = 1.0
        try:
            dt_dvr_max = float(dt_cfg.get("dvr_max", 0.8))
        except Exception:
            dt_dvr_max = 0.8
        try:
            dt_cd_min = float(dt_cfg.get("cd_min", 0.0))
        except Exception:
            dt_cd_min = 0.0
        try:
            dt_slope_min = float(dt_cfg.get("slope_min", 0.4))
        except Exception:
            dt_slope_min = 0.4
        try:
            dt_band_max = float(dt_cfg.get("band_max", 0.35))
        except Exception:
            dt_band_max = 0.35
        try:
            dt_htf_required = bool(dt_cfg.get("htf_required", False))
        except Exception:
            dt_htf_required = False

        band_ok = (band_pct is None) or (band_pct <= dt_band_max)
        htf_ok_soft = (htf_ok is True) or ((htf_ok is None) and not dt_htf_required)
        slope_ok = slope_atr is not None and slope_atr >= dt_slope_min
        cd_ok = (cd is None) or (cd >= dt_cd_min)
        volz_ok = volz is not None and abs(volz) >= dt_volz_min
        dvr_light = (dvr is None) or (dvr <= dt_dvr_max)

        if band_ok and htf_ok_soft and slope_ok and cd_ok and volz_ok and dvr_light:
            score = 0.55
            reasons.extend(
                [
                    "slope_ok",
                    "volz_light",
                    "cd_ok" if cd is None or cd >= dt_cd_min else "cd_missing",
                    "participation_light",
                ]
            )
            if volz_proxy_used:
                reasons.append("volz_proxy")
            logger.info(
                "Фаза виявлена: drift_trend (participation-light) для %s (score=%.2f) reasons=%s",
                symbol,
                score,
                reasons,
            )
            return PhaseResult(
                "drift_trend",
                score,
                reasons,
                volz_source,
                proxy_components,
                overlay_o,
                profile=profile_tag,
                threshold_tag=threshold_tag,
            )

    # -------------------------
    # 4) Post-breakout — ретест біля крайових рівнів після розширення діапазону
    # -------------------------
    pb = thr.get("post_breakout", {})
    try:
        pb_volz = float(pb.get("vol_z_max", 1.0))
        pb_dvr = float(pb.get("dvr_min", 0.5))
    except Exception:
        pb_volz, pb_dvr = 1.0, 0.5
    logger.debug("post_breakout thresholds: vol_z_max=%s dvr_min=%s", pb_volz, pb_dvr)

    # Полегшення DVR у суворих умовах ретесту (strict overrides)
    dvr_req = pb_dvr
    volz_cap = pb_volz
    try:
        if strict_profile_active:
            ov = (STRONG_REGIME or {}).get("overrides", {}) or {}
            pbr = (
                (ov.get("post_breakout_relief") or {})
                if isinstance(ov, Mapping)
                else {}
            )
            dvr_base = float(pbr.get("dvr_min_base", pb_dvr))
            dvr_rel = float(pbr.get("dvr_min_relaxed", max(0.0, pb_dvr - 0.1)))
            rsi_lo = float(pbr.get("rsi_lo", 45.0))
            rsi_hi = float(pbr.get("rsi_hi", 62.0))
            volz_relax_cap = float(pbr.get("vol_z_max_relief", max(pb_volz, 1.2)))
            pres_thr = float(pbr.get("presence_min", 0.75))
            bias_thr = float(pbr.get("bias_abs_min", 0.30))

            cond_pres2 = (
                isinstance(whale_presence, (int, float))
                and float(whale_presence) >= pres_thr
            )
            cond_bias2 = (
                isinstance(whale_bias, (int, float))
                and abs(float(whale_bias)) >= bias_thr
            )
            cond_rsi2 = (rsi is None) or (
                isinstance(rsi, (int, float)) and rsi_lo <= float(rsi) <= rsi_hi
            )
            cond_volz2 = volz is not None and float(volz) <= volz_relax_cap

            if (
                cond_pres2
                and cond_bias2
                and cond_rsi2
                and cond_volz2
                and (is_near_edge is True)
            ):
                dvr_req = min(dvr_base, dvr_rel)
                volz_cap = max(pb_volz, volz_relax_cap)
    except Exception:
        pass

    # Пояснення: близькість до краю + низький vol (ретест) + достатній DVR
    if (
        (is_near_edge is True)
        and (volz is not None and volz <= volz_cap)
        and (dvr is not None and dvr >= dvr_req)
    ):
        score = 0.7
        reasons.extend(["near_edge", "low_vol", "dvr_ok"])
        try:
            if dvr_req < pb_dvr:
                reasons.append("pb_dvr_relief")
                logger.info(
                    "[STRICT_SANITY] pb_dvr_relief_applied symbol=%s dvr_req=%.2f (base=%.2f) volz_cap=%.2f",
                    symbol,
                    dvr_req,
                    pb_dvr,
                    volz_cap,
                )
        except Exception:
            pass
        if volz_proxy_used:
            reasons.append("volz_proxy")
        logger.info(
            "Фаза виявлена: post_breakout для %s (score=%.2f) reasons=%s",
            symbol,
            score,
            reasons,
        )
        return PhaseResult(
            "post_breakout",
            score,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            threshold_tag=threshold_tag,
        )

    # 4b) Post-breakdown — симетрія до post_breakout, але для ведмежого ретесту вниз
    pd = thr.get("post_breakdown", {})
    try:
        pd_volz = float(pd.get("vol_z_max", 1.2))
        pd_dvr = float(pd.get("dvr_min", 0.5))
    except Exception:
        pd_volz, pd_dvr = 1.2, 0.5
    # умови вниз: біля верхнього краю (edge), низький vol, достатній DVR, напрямок проти (cd<=0) і слабкий/негативний схил
    if (
        (is_near_edge is True)
        and (volz is not None and volz <= pd_volz)
        and (dvr is not None and dvr >= pd_dvr)
        and (cd is not None and cd <= 0.0)
        and (slope_atr is not None and slope_atr <= 0.2)
    ):
        score = 0.7
        reasons.extend(
            ["near_edge", "low_vol", "dvr_ok", "cd_down", "slope_flat_or_down"]
        )
        if volz_proxy_used:
            reasons.append("volz_proxy")
        logger.info(
            "Фаза виявлена: post_breakdown для %s (score=%.2f) reasons=%s",
            symbol,
            score,
            reasons,
        )
        return PhaseResult(
            "post_breakdown",
            score,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            threshold_tag=threshold_tag,
        )

    # -------------------------
    # 5) Pre-breakout — вузький діапазон + ознаки старту імпульсу
    # -------------------------
    pre = thr.get("pre_breakout", {})
    try:
        pre_band = float(pre.get("band_max", 0.18))
    except Exception:
        pre_band = 0.18
    try:
        pre_volz = float(pre.get("vol_z_min", 1.4))
    except Exception:
        pre_volz = 1.4
    try:
        pre_dvr = float(pre.get("dvr_min", 0.6))
    except Exception:
        pre_dvr = 0.6
    try:
        pre_cd = float(pre.get("cd_min", 0.3))
    except Exception:
        pre_cd = 0.3
    try:
        pre_slope = float(pre.get("slope_min", 0.2))
    except Exception:
        pre_slope = 0.2
    pre_alt_cfg = pre.get("alt") if isinstance(pre.get("alt"), Mapping) else None
    try:
        pre_band_alt = float(
            (pre_alt_cfg or {}).get("band_max", pre.get("band_max_alt", 0.12))
        )
    except Exception:
        pre_band_alt = 0.12
    try:
        pre_slope_alt = float(
            (pre_alt_cfg or {}).get("slope_min", pre.get("slope_min_alt", 1.0))
        )
    except Exception:
        pre_slope_alt = 1.0
    try:
        pre_dvr_alt = float(
            (pre_alt_cfg or {}).get("dvr_min", pre.get("dvr_min_alt", 0.8))
        )
    except Exception:
        pre_dvr_alt = 0.8
    try:
        pre_cd_alt = float(
            (pre_alt_cfg or {}).get("cd_min", pre.get("cd_min_alt", 0.2))
        )
    except Exception:
        pre_cd_alt = 0.2
    guard_band_limit = float(pre.get("hard_guard_band_max", max(0.18, pre_band)))
    logger.debug(
        "pre_breakout thresholds: band_max=%s vol_z_min=%s dvr_min=%s cd_min=%s slope_min=%s | alt: band_max=%s slope_min_alt=%s dvr_min_alt=%s cd_min_alt=%s | guard=%s",
        pre_band,
        pre_volz,
        pre_dvr,
        pre_cd,
        pre_slope,
        pre_band_alt,
        pre_slope_alt,
        pre_dvr_alt,
        pre_cd_alt,
        guard_band_limit,
    )

    # Hard-guard: якщо band_pct перевищує ліміт — не вважаємо pre_breakout
    anti_breakout_guard = False
    if (
        isinstance(band_pct, (int, float))
        and float(band_pct) <= 0.16
        and (
            (isinstance(volz_eff, (int, float)) and float(volz_eff) >= 1.8)
            or (isinstance(volz, (int, float)) and float(volz) >= 1.8)
        )
        and isinstance(dvr, (int, float))
        and float(dvr) >= 0.8
        and isinstance(cd, (int, float))
        and float(cd) >= 0.35
        and isinstance(htf_ok, bool)
        and htf_ok is True
    ):
        whale_bias_val = whale_bias if isinstance(whale_bias, (int, float)) else None
        presence_val = (
            whale_presence if isinstance(whale_presence, (int, float)) else None
        )
        vwap_guard = (
            vwap_dev_whale if isinstance(vwap_dev_whale, (int, float)) else None
        )
        if (whale_bias_val is not None and whale_bias_val <= -0.2) or (
            presence_val is not None
            and presence_val >= 0.75
            and vwap_guard is not None
            and vwap_guard < 0.0
        ):
            anti_breakout_guard = True

    if isinstance(band_pct, (int, float)) and float(band_pct) > guard_band_limit:
        pre_primary = False
        pre_alt = False
    else:
        # Основні умови: tight band + vol spike + directional support + positive CD + HTF підтверджує
        pre_primary = (
            (band_pct is not None and band_pct <= pre_band)
            and (volz is not None and volz >= pre_volz)
            and (slope_atr is not None and slope_atr >= pre_slope)
            and (dvr is not None and dvr >= pre_dvr)
            and (cd is not None and cd >= pre_cd)
            and (isinstance(htf_ok, bool) and htf_ok is True)
        )
        # Альтернатива: лише при дуже вузькому бенді без vol_spike
        pre_alt = (
            (band_pct is not None and band_pct <= pre_band_alt)
            and (slope_atr is not None and slope_atr >= pre_slope_alt)
            and (dvr is not None and dvr >= pre_dvr_alt)
            and (cd is not None and cd >= pre_cd_alt)
            and (isinstance(htf_ok, bool) and htf_ok is True)
        )
        # Guardrail reasons (telemetry-only): якщо близько до умов, але щось не дотягує
        band_ok = band_pct is not None and band_pct <= pre_band
        volz_ok = volz is not None and volz >= pre_volz
        slope_ok = slope_atr is not None and slope_atr >= pre_slope
        dvr_ok = dvr is not None and dvr >= pre_dvr
        cd_ok = cd is not None and cd >= pre_cd
        htf_ok_flag2 = isinstance(htf_ok, bool) and htf_ok is True
        if band_ok and volz_ok and not pre_primary:
            if not slope_ok:
                reasons.append("pre_guard_slope_neg")
            if not dvr_ok:
                reasons.append("pre_guard_dvr_low")
            if not cd_ok:
                reasons.append("pre_guard_cd_low")
            if not htf_ok_flag2:
                reasons.append("pre_guard_htf_conflict")
    if anti_breakout_guard:
        pre_primary = False
        pre_alt = False
        reasons.append("anti_breakout_whale_guard")
        try:
            logger.info(
                "[STRICT_PHASE] symbol=%s guard=anti_breakout_whale_guard band_pct=%.3f volz_eff=%s dvr=%.3f cd=%.3f presence=%s bias=%s vwap_dev=%s",
                symbol,
                float(band_pct) if isinstance(band_pct, (int, float)) else float("nan"),
                (
                    f"{float(volz_eff):.3f}"
                    if isinstance(volz_eff, (int, float))
                    else "nan"
                ),
                float(dvr) if isinstance(dvr, (int, float)) else float("nan"),
                float(cd) if isinstance(cd, (int, float)) else float("nan"),
                (
                    f"{float(whale_presence):.3f}"
                    if isinstance(whale_presence, (int, float))
                    else "na"
                ),
                (
                    f"{float(whale_bias):.3f}"
                    if isinstance(whale_bias, (int, float))
                    else "na"
                ),
                (
                    f"{float(vwap_dev_whale):.4f}"
                    if isinstance(vwap_dev_whale, (int, float))
                    else "na"
                ),
            )
        except Exception:
            pass
    if pre_primary or pre_alt:
        score = 0.65
        if pre_primary:
            reasons.extend(
                ["band_tight", "vol_spike", "slope_pos", "dvr_ok", "cd_ok", "htf_ok"]
            )
        else:
            reasons.extend(
                ["band_very_tight", "slope_strong", "dvr_ok", "cd_ok", "htf_ok"]
            )
        if volz_proxy_used and pre_primary:
            reasons.append("volz_proxy")
        logger.info(
            "Фаза виявлена: pre_breakout для %s (score=%.2f) reasons=%s",
            symbol,
            score,
            reasons,
        )
        try:
            logger.info(
                "[STRICT_PHASE] symbol=%s phase=pre_breakout score=%.2f reasons=%s",
                symbol,
                score,
                "+".join(reasons),
            )
        except Exception:
            pass
        return PhaseResult(
            "pre_breakout",
            score,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            threshold_tag=threshold_tag,
        )

    # -------------------------
    # Нічого не підходить — повертаємо None (консервативний підхід)
    # -------------------------
    # Рахуємо кількість відсутніх полів для діагностики/логування
    missing = sum(
        1
        for v in (
            band_pct,
            dvr,
            cd,
            slope_atr,
            volz_v1 if volz_proxy_used else volz,
            rsi,
            atr_pct,
        )
        if v is None
    )
    if not reasons:
        logger.info(
            "Не виявлено фази для %s — недостатньо сигналів (missing_fields=%d).",
            symbol,
            missing,
        )
    else:
        logger.info(
            "Не виявлено фази для %s — накопичені причини: %s (missing_fields=%d).",
            symbol,
            reasons,
            missing,
        )

    # TODO: додати telemetry/metrics: counters для кожного типу невизначеності (missing_fields>n)
    # TODO: посилити логування proxy_components для post-mortem аналізу
    # TODO: інтеграційний тест: перевірити, що volz_source=='proxy' не веде до exhaustion/false_breakout

    # Універсальний телеметрійний overlay для Market Insight (Phase 1.5)
    return _apply_phase_postprocessors(
        PhaseResult(
            None,
            0.0,
            reasons,
            volz_source,
            proxy_components,
            overlay_o,
            profile=profile_tag,
            threshold_tag=threshold_tag,
        ),
        locals(),
        str(symbol),
    )
