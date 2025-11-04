from __future__ import annotations

from typing import Any

from utils.btc_regime import get_btc_regime_v2


def build_context_frame(
    stats: dict[str, Any] | None,
    ctx_meta: dict[str, Any] | None = None,
    htf_strength_series: list[float] | None = None,
    window_bars: int = 288,
) -> dict[str, Any]:
    """Побудувати ContextFrame v1 із легких доступних сигналів.

    Повертає dict для market_context.meta.context_frame:
      - swing: {hh, ll, time_since_swing_touch}
      - compression: {index}
      - regime_timeline: частки {'trend_up','trend_down','flat'}
      - persistence: {near_edge_persist, presence_sustain}

    Примітки:
      - hh/ll у цій версії — best-effort, читаються зі stats якщо присутні.
      - compression.index — використовує band_expand (або ctx_meta.band_expand_score).
      - persistence — береться з ctx_meta, якщо доступно; інакше обчислюється бінарно з поточного бару.
      - regime_timeline — якщо немає історії, віддає one-hot для поточного режиму.
    """
    st = stats or {}
    cm = ctx_meta or {}

    # swing (best-effort з поточного статсу)
    swing = {
        "hh": st.get("last_high"),
        "ll": st.get("last_low"),
        "time_since_swing_touch": 0 if _is_near_edge(st.get("near_edge")) else None,
    }

    # compression
    try:
        bex = float(st.get("band_expand") or 0.0)
    except Exception:
        bex = 0.0
    if bex == 0.0:
        try:
            bex = float(cm.get("band_expand_score") or 0.0)
        except Exception:
            bex = 0.0
    compression = {"index": bex}

    # persistence
    try:
        nep = float(cm.get("near_edge_persist") or 0.0)
    except Exception:
        nep = 0.0
    try:
        ps = float(cm.get("presence_sustain") or 0.0)
    except Exception:
        ps = 0.0
    if nep == 0.0 and ps == 0.0:
        # Бінарний fallback із одиничного бару
        nep = 1.0 if _is_near_edge(st.get("near_edge")) else 0.0
        try:
            wh = st.get("whale") if isinstance(st.get("whale"), dict) else {}
            pres = float((wh or {}).get("presence") or 0.0)
            ps = 1.0 if pres >= 0.60 else 0.0
        except Exception:
            ps = 0.0
    persistence = {
        "near_edge_persist": round(float(nep), 3),
        "presence_sustain": round(float(ps), 3),
    }

    # regime_timeline (one-hot якщо немає історії)
    regime = get_btc_regime_v2(st, read_from_redis=True)
    if htf_strength_series:
        # Простий підрахунок плоскої/трендової долі за силою HTF
        trend_up = 0
        trend_down = 0
        flat = 0
        for v in htf_strength_series:
            try:
                v = float(v)
            except Exception:
                v = 0.0
            if v < 0.02:  # узгоджено з SCEN_HTF_MIN за замовчуванням
                flat += 1
            else:
                # без напряму — відносимо до flat
                flat += 1
        total = max(1, trend_up + trend_down + flat)
        regime_timeline = {
            "trend_up": round(trend_up / total, 3),
            "trend_down": round(trend_down / total, 3),
            "flat": round(flat / total, 3),
        }
    else:
        regime_timeline = {
            "trend_up": 1.0 if regime == "trend_up" else 0.0,
            "trend_down": 1.0 if regime == "trend_down" else 0.0,
            "flat": 1.0 if regime == "flat" else 0.0,
        }

    return {
        "swing": swing,
        "compression": compression,
        "persistence": persistence,
        "regime_timeline": regime_timeline,
    }


def _is_near_edge(v: Any) -> bool:
    try:
        if v is True:
            return True
        if isinstance(v, str):
            return v.lower() in {"upper", "lower", "near", "true"}
        return False
    except Exception:
        return False
