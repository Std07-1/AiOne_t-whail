from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Literal

# Конфіг читаємо безпосередньо (джерело правди — config.config)
try:  # pragma: no cover
    from config import config as cfg  # type: ignore
except Exception:  # pragma: no cover

    class _Dummy:
        POLICY_V2_ENABLED = False
        POLICY_V2_MIN_P = 0.58
        POLICY_V2_MIN_EV = 0.05
        POLICY_V2_TP_ATR = 2.0
        POLICY_V2_SL_ATR = 0.8
        SWEEP_CONFIRM_5M_VOL_MULT = 1.8
        FSM_COOLDOWN_S = 90

    cfg = _Dummy()  # type: ignore

"""
Policy Engine v2 (Stage2) — прийняття керованих сигналів під фіче‑флагом.

Цілі T0:
- Контракти Stage1/Stage2/Stage3 НЕ змінюємо; нові дані лише у market_context.meta.*
- Видавати SignalDecision з class_/side/p_win/sl_atr/tp_atr/ttl_s/reason/features.
- Жодних блокуючих I/O: працюємо лише на переданих stats/ds (best‑effort).
- Лаконічний лог однією строкою: [SIGNAL_V2] ...
"""


@dataclass
class SignalDecision:
    symbol: str
    class_: Literal["IMPULSE", "ACCUM_BREAKOUT", "MEAN_REVERT"]
    side: Literal["long", "short"]
    ts_ms: int
    p_win: float
    sl_atr: float
    tp_atr: float
    ttl_s: int
    reason: str
    features: dict[str, float]


def infer_regime(stats: dict[str, Any]) -> dict[str, float]:
    """Оцінка ринкового режиму (one‑hot): trend_up, trend_down, range_low_vol, range_high_vol.

    Вхідні ознаки: htf_strength ∈ [0,1], band_pct, price_slope_atr. Вихід — one‑hot dict.
    """
    try:
        htf = float(stats.get("htf_strength") or 0.0)
    except Exception:
        htf = 0.0
    try:
        slope = float(stats.get("price_slope_atr") or 0.0)
    except Exception:
        slope = 0.0
    try:
        band = float(stats.get("band_pct") or 0.0)
    except Exception:
        band = 0.0
    out = {
        "trend_up": 0.0,
        "trend_down": 0.0,
        "range_low_vol": 0.0,
        "range_high_vol": 0.0,
    }
    if htf >= 0.20:
        if slope >= 0.8:
            out["trend_up"] = min(1.0, htf)
        elif slope <= -0.8:
            out["trend_down"] = min(1.0, htf)
        else:
            out["range_high_vol"] = min(1.0, max(0.2, htf))
    else:
        out["range_low_vol" if band <= cfg.ACCUM_BAND_PCT_MAX else "range_high_vol"] = (
            max(0.0, 0.5 * htf)
        )
    return out


def compute_5m_and_median20(ds: Any) -> tuple[float, float] | None:
    """Повертає пару (vol_5m_sum, median20_1m_vol) або None, якщо історії недостатньо.

    Вимагає: ≥5 значень для 5m та ≥10 (краще 20) для median20. Без I/O.
    """
    try:
        tail_any = (ds or {}).get("volume_1m_tail", [])
        tail = list(tail_any or [])
        if len(tail) < 5 or len(tail) < 10:
            return None
        vol5 = float(sum(tail[-5:]))
        tail_n = tail[-20:] if len(tail) >= 20 else tail[-10:]
        tail_sorted = sorted(float(x) for x in tail_n)
        m_idx = len(tail_sorted) // 2
        median20 = float(tail_sorted[m_idx]) if tail_sorted else 0.0
        return (vol5, median20)
    except Exception:
        return None


def ramp_score(stats: dict[str, Any]) -> float:
    """Наростання тиску (best‑effort з наявних ознак)."""
    try:
        return float(stats.get("ramp_score") or 0.0)
    except Exception:
        return 0.0


def absorption_score(ds: Any) -> float:
    """Поглинання біля краю (best‑effort з ds/stats)."""
    try:
        return float((ds or {}).get("absorption_score") or 0.0)
    except Exception:
        return 0.0


def score_pwin(features: dict[str, float]) -> float:
    """Легка логістика v1 із ручними вагами (плейсхолдер)."""
    import math

    w = {
        "regime_conf": 1.2,
        "ramp_score": 0.6,
        "absorption_score": 0.6,
        "retest_ok": 0.8,
        "breakout_5m_vol_ok": 0.9,
        "slope_atr": 0.5,
    }
    z = 0.0
    for k, wt in w.items():
        z += wt * float(features.get(k, 0.0) or 0.0)
    # базовий зсув у нейтральній зоні
    z -= 1.5
    return 1.0 / (1.0 + math.exp(-z))


def propose_levels_atr(stats: dict[str, Any], cls: str) -> tuple[float, float]:
    """Рівні в ATR‑множниках (tp_atr, sl_atr) залежно від класу."""
    if cls == "IMPULSE":
        return (1.8, 0.9)
    if cls == "ACCUM_BREAKOUT":
        return (2.2, 1.1)
    if cls == "MEAN_REVERT":
        return (0.8, 0.6)
    # дефолт
    try:
        return (float(cfg.POLICY_V2_TP_ATR), float(cfg.POLICY_V2_SL_ATR))
    except Exception:
        return (2.0, 0.8)


def decide(
    stats: dict[str, Any], ds: Any, flags: dict[str, Any] | None = None
) -> SignalDecision | None:
    """Приймає рішення по одному з класів або повертає None, якщо умови не виконані.

    Гейти (best‑effort, без I/O):
      - IMPULSE: 5m_vol ≥ IMPULSE_VOL_MULT×median20 AND |slope_atr| ≥ 0.8
      - ACCUM_BREAKOUT: band_pct ≤ ACCUM_BAND_PCT_MAX AND presence_sustain≈1 AND ramp_score ≥ τ1 AND sweep_ctx==true
      - MEAN_REVERT: retest_ok==true AND vol_z ≤ 0 AND htf_strength < MEANREV_HTF_MAX AND dist_to_level ≤ τ2
    """
    logger = logging.getLogger(__name__)
    symbol = (
        str((stats or {}).get("symbol") or (stats or {}).get("SYMBOL") or "").upper()
        or "?"
    )
    # Регім (один найсильніший)
    regimes = infer_regime(stats or {})
    # Найсильніший режим може бути використаний у наступних ітераціях (one-hot уже у features)

    # Обчислення допоміжних ознак
    try:
        slope_atr = float((stats or {}).get("price_slope_atr") or 0.0)
    except Exception:
        slope_atr = 0.0
    try:
        band_pct = float((stats or {}).get("band_pct") or 0.0)
    except Exception:
        band_pct = 0.0
    try:
        presence_sustain = float((stats or {}).get("presence_sustain") or 0.0)
    except Exception:
        presence_sustain = 0.0
    rs = ramp_score(stats or {})
    abs_score = absorption_score(ds)
    sweep_ctx = bool(
        (stats or {}).get("sweep_ctx") or (stats or {}).get("sweep_then_breakout")
    )
    retest_ok = bool((stats or {}).get("retest_ok"))
    try:
        vol_z = float((stats or {}).get("vol_z") or 0.0)
    except Exception:
        vol_z = 0.0
    try:
        htf = float((stats or {}).get("htf_strength") or 0.0)
    except Exception:
        htf = 0.0
    try:
        dist_to_level = float((stats or {}).get("dist_to_level") or 0.0)
    except Exception:
        dist_to_level = 1.0

    v = compute_5m_and_median20(ds)
    if v is None:
        return None
    vol5, med20 = v

    # Гейти
    cls: str | None = None
    side: str | None = None
    reason = ""
    # IMPULSE
    if (
        vol5 >= float(getattr(cfg, "IMPULSE_VOL_MULT", 1.8)) * max(1.0, med20)
        and abs(slope_atr) >= 0.8
    ):
        cls = "IMPULSE"
        side = "long" if slope_atr >= 0.8 else "short"
        reason = f"5m_vol={vol5:.0f}≥{getattr(cfg,'IMPULSE_VOL_MULT',1.8):.1f}×median20({med20:.0f}), |slope_atr|≥0.8"
    # ACCUM_BREAKOUT
    elif (
        band_pct <= float(getattr(cfg, "ACCUM_BAND_PCT_MAX", 0.015))
        and presence_sustain >= 0.95
        and rs >= 0.5
        and bool(sweep_ctx)
    ):
        cls = "ACCUM_BREAKOUT"
        ne = str((stats or {}).get("near_edge") or "").lower()
        side = (
            "long"
            if ne == "upper"
            else ("short" if ne == "lower" else ("long" if slope_atr >= 0 else "short"))
        )
        reason = "accum_breakout gates met"
    # MEAN_REVERT
    elif (
        retest_ok
        and (vol_z <= 0.0)
        and (htf < float(getattr(cfg, "MEANREV_HTF_MAX", 0.15)))
        and (dist_to_level <= float(getattr(cfg, "MEANREV_DIST_TO_LEVEL_MAX", 0.0015)))
    ):
        cls = "MEAN_REVERT"
        ne = str((stats or {}).get("near_edge") or "").lower()
        side = (
            "short"
            if ne == "upper"
            else ("long" if ne == "lower" else ("short" if slope_atr > 0 else "long"))
        )
        reason = "mean_revert gates met"

    if not cls or not side:
        return None

    # Ймовірність перемоги p_win і прості фічі
    feats: dict[str, float] = {
        "regime_trend_up": float(regimes.get("trend_up", 0.0)),
        "regime_trend_down": float(regimes.get("trend_down", 0.0)),
        "regime_range_lv": float(regimes.get("range_low_vol", 0.0)),
        "regime_range_hv": float(regimes.get("range_high_vol", 0.0)),
        "rs": float(rs),
        "abs": float(abs_score),
        "slope_atr": float(slope_atr),
        "presence_sustain": float(presence_sustain),
        "vol5_over_m20": float((vol5 / med20) if (med20 > 0) else 0.0),
    }
    # p_win за новою формулою
    try:
        base = 0.55
        p = base
        # +0.15*clip(|slope_atr|,0,1)
        sa = min(1.0, max(0.0, abs(feats.get("slope_atr", 0.0))))
        p += 0.15 * sa
        # +0.10*ramp_score
        p += 0.10 * min(1.0, max(0.0, feats.get("rs", 0.0)))
        # +0.10*absorption_score
        p += 0.10 * min(1.0, max(0.0, feats.get("abs", 0.0)))
        # +0.10*I(sweep_ctx)
        p += 0.10 * (1.0 if sweep_ctx else 0.0)
        # -0.10*I(htf_strength < 0.10)
        p -= 0.10 * (1.0 if htf < 0.10 else 0.0)
        if p < 0.0:
            p = 0.0
        if p > 0.95:
            p = 0.95
    except Exception:
        p = 0.5

    if p < float(
        getattr(cfg, "POLICY_V2_PWIN_MIN", getattr(cfg, "POLICY_V2_MIN_P", 0.58))
    ):
        return None

    tp_atr, sl_atr = propose_levels_atr(stats or {}, cls)
    dec = SignalDecision(
        symbol=symbol,
        class_=cls,  # type: ignore[arg-type]
        side=side,  # type: ignore[arg-type]
        ts_ms=int(time.time() * 1000),
        p_win=p,
        sl_atr=sl_atr,
        tp_atr=tp_atr,
        ttl_s=int(
            getattr(
                cfg,
                "POLICY_V2_LEAD_S_TARGET",
                getattr(cfg, "COOLDOWN_S", getattr(cfg, "FSM_COOLDOWN_S", 90)),
            )
        ),
        reason=reason,
        features=feats,
    )
    try:
        logger.info(
            "[SIGNAL_V2] ts_ms=%d symbol=%s class=%s side=%s p=%.2f sl_atr=%.2f tp_atr=%.2f ttl_s=%d reason=%s feats=%s",
            dec.ts_ms,
            symbol,
            dec.class_,
            dec.side,
            dec.p_win,
            dec.sl_atr,
            dec.tp_atr,
            dec.ttl_s,
            dec.reason,
            dec.features,
        )
    except Exception:
        pass
    return dec
