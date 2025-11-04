from __future__ import annotations

from typing import Any

from config.config import (
    SCEN_BREAKOUT_DVR_MIN,
    SCEN_BTC_GATE,
    SCEN_HTF_MIN,
    SCEN_PULLBACK_PRESENCE_MIN,
    SCENARIO_BTC_GATE_ENABLED,
    SYMBOL_PROFILES,
)


def apply_btc_gate_and_profiles(
    symbol: str,
    scn_name: str | None,
    scn_conf: float,
    stats: dict[str, Any],
    btc_regime: str | None,
) -> tuple[str | None, float, dict[str, float]]:
    """Застосувати BTC‑gate та пер‑символьні мультиплікатори без зміни контрактів Stage1/2/3.

    Якщо умови після множників не виконуються — приглушити сценарій (name=None, conf=0.0).
    Повертає (name, conf, used_thresholds)
    """
    used: dict[str, float] = {}
    if not scn_name:
        return (None, 0.0, used)

    sym = (symbol or "").upper()
    prof = SYMBOL_PROFILES.get(sym, {})
    defaults = SYMBOL_PROFILES.get("defaults", {})
    pm = float(prof.get("presence_min_mult", defaults.get("presence_min_mult", 1.0)))
    hm = float(prof.get("htf_min_mult", defaults.get("htf_min_mult", 1.0)))
    dm = float(prof.get("dvr_min_mult", defaults.get("dvr_min_mult", 1.0)))

    pres_min = float(SCEN_PULLBACK_PRESENCE_MIN) * pm
    htf_min = float(SCEN_HTF_MIN) * hm
    dvr_min = float(SCEN_BREAKOUT_DVR_MIN) * dm

    # BTC regime gate multipliers (optional)
    if SCENARIO_BTC_GATE_ENABLED and btc_regime in SCEN_BTC_GATE:
        g = SCEN_BTC_GATE[btc_regime]
        pres_min *= float(g.get("presence_min_mult", 1.0))
        htf_min *= float(g.get("htf_min_mult", 1.0))
        dvr_min *= float(g.get("dvr_min_mult", 1.0))

    used.update(
        {
            "presence_min_eff": pres_min,
            "htf_min_eff": htf_min,
            "dvr_min_eff": dvr_min,
        }
    )

    # Extract signals
    try:
        wh = stats.get("whale") if isinstance(stats.get("whale"), dict) else {}
        presence = float((wh or {}).get("presence") or 0.0)
        bias = float((wh or {}).get("bias") or 0.0)
    except Exception:
        presence, bias = 0.0, 0.0
    try:
        htf = float(stats.get("htf_strength") or 0.0)
    except Exception:
        htf = 0.0
    try:
        dvr = float(stats.get("dvr") or stats.get("directional_volume_ratio") or 0.0)
    except Exception:
        dvr = 0.0

    # Simple suppression logic: if effective thresholds fail, drop scenario
    if scn_name == "pullback_continuation":
        if not (presence >= pres_min and htf >= htf_min and abs(bias) > 0.0):
            return (None, 0.0, used)
    if scn_name == "breakout_confirmation":
        if not (dvr >= dvr_min and htf >= htf_min):
            return (None, 0.0, used)

    return (scn_name, float(scn_conf or 0.0), used)
