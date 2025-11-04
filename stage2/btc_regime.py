from __future__ import annotations

import time
from typing import Any

from config.config import NAMESPACE
from config.keys import build_key

# Легка евристика для BTC‑режимів (без торгів):
#  - ACCUM: низька band_expand, слабкий HTF, низька присутність китів
#  - EXPANSION_SETUP: band_expand зростає, HTF поліпшується
#  - BREAKOUT_UP/DOWN: post_breakout або momentum із явним bias


def detect_btc_regime(
    stats: dict[str, Any], context: dict[str, Any]
) -> tuple[str, list[str]]:
    try:
        bex = float(stats.get("band_expand") or 0.0)
    except Exception:
        bex = 0.0
    try:
        htf = float(stats.get("htf_strength") or 0.0)
    except Exception:
        htf = 0.0
    try:
        wh = stats.get("whale") if isinstance(stats.get("whale"), dict) else {}
        presence = float((wh or {}).get("presence") or 0.0)
        bias = float((wh or {}).get("bias") or 0.0)
    except Exception:
        presence, bias = 0.0, 0.0

    # Контекстні агрегати
    # контекстні метрики (best-effort)
    bes = float(context.get("band_expand_score") or 0.0)

    state = "ACCUM"
    reasons: list[str] = []

    if bex >= 0.012 or bes >= 0.012:
        if bias >= 0.1:
            state = "BREAKOUT_UP"
            reasons.append("band_expand_high+bias_pos")
        elif bias <= -0.1:
            state = "BREAKOUT_DOWN"
            reasons.append("band_expand_high+bias_neg")
        else:
            state = "EXPANSION_SETUP"
            reasons.append("band_expand_high")
    else:
        if htf >= 0.20 and presence >= 0.40:
            state = "EXPANSION_SETUP"
            reasons.append("htf_mid+presence_mid")
        else:
            state = "ACCUM"
            reasons.append("flat_context")

    return state, reasons


async def publish_btc_regime(store: Any, state: str, reasons: list[str]) -> None:
    """Запис режиму в Redis як JSON (best-effort)."""
    try:
        jset = getattr(store.redis, "jset", None)
        if callable(jset):
            key = build_key(NAMESPACE, "global", extra=["btc_regime"])
            payload = {
                "state": state,
                "reasons": reasons,
                "ts": int(time.time() * 1000),
            }
            await jset(key, payload, ttl=600)
    except Exception:
        return
