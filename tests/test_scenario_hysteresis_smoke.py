from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from config.config import (
    SCEN_BREAKOUT_DVR_MIN,
    SCEN_HTF_MIN,
    SCEN_PULLBACK_PRESENCE_MIN,
)
from utils.phase_adapter import resolve_scenario


def _ring_accepts_pullback(ring: list[dict[str, float]]) -> bool:
    """Minimal stub of hysteresis rule: 2x same scn OR sum conf>=0.30 over 3,
    and for pullback last 2 bars meet presence/htf thresholds from config."""
    if not ring:
        return False
    scn = ring[-1]["scn"]
    # 2 last equal
    cond1 = len(ring) >= 2 and ring[-1]["scn"] == scn and ring[-2]["scn"] == scn
    # sum conf over last 3 for same scn
    s3 = ring[-3:] if len(ring) >= 1 else []
    sum_conf = sum(float(e.get("conf", 0.0)) for e in s3 if e.get("scn") == scn)
    cond2 = sum_conf >= 0.30
    # sustained thresholds for pullback
    if scn == "pullback_continuation" and len(ring) >= 2:
        last2 = ring[-2:]
        ok = 0
        for e in last2:
            if float(e.get("presence", 0.0)) >= float(
                SCEN_PULLBACK_PRESENCE_MIN
            ) and float(e.get("htf", 0.0)) >= float(SCEN_HTF_MIN):
                ok += 1
        cond1 = cond1 and ok >= 2
        cond2 = cond2 and ok >= 2
    return bool(cond1 or cond2)


def test_breakout_rule_strict_volz_htf_required() -> None:
    # Strict: vol_z must be <= 1.0, and htf_strength present and >= SCEN_HTF_MIN
    stats: Mapping[str, Any] = {
        "near_edge": True,
        "volume_z": 0.9,
        "directional_volume_ratio": float(SCEN_BREAKOUT_DVR_MIN),
        "htf_strength": float(SCEN_HTF_MIN) + 1e-6,
    }
    name, conf = resolve_scenario("post_breakout", stats)
    assert name == "breakout_confirmation" and conf > 0.0

    # vol_z too high → reject
    stats2 = dict(stats)
    stats2["volume_z"] = 1.5
    name2, conf2 = resolve_scenario("post_breakout", stats2)
    assert name2 is None and conf2 == 0.0

    # missing htf → reject under strict rule
    stats3 = dict(stats)
    stats3.pop("htf_strength", None)
    name3, conf3 = resolve_scenario("post_breakout", stats3)
    assert name3 is None and conf3 == 0.0


def test_pullback_hysteresis_sustained_thresholds() -> None:
    # Build a ring with two last pullbacks but one below thresholds → should NOT accept
    ring = [
        {
            "scn": "pullback_continuation",
            "conf": 0.16,
            "presence": float(SCEN_PULLBACK_PRESENCE_MIN),
            "htf": float(SCEN_HTF_MIN),
        },
        {
            "scn": "pullback_continuation",
            "conf": 0.16,
            "presence": float(SCEN_PULLBACK_PRESENCE_MIN) - 0.01,
            "htf": float(SCEN_HTF_MIN),
        },
    ]
    assert _ring_accepts_pullback(ring) is False

    # Now both last two meet thresholds → accept
    ring2 = [
        {
            "scn": "pullback_continuation",
            "conf": 0.14,
            "presence": float(SCEN_PULLBACK_PRESENCE_MIN),
            "htf": float(SCEN_HTF_MIN),
        },
        {
            "scn": "pullback_continuation",
            "conf": 0.16,
            "presence": float(SCEN_PULLBACK_PRESENCE_MIN) + 0.01,
            "htf": float(SCEN_HTF_MIN) + 0.01,
        },
    ]
    assert _ring_accepts_pullback(ring2) is True
