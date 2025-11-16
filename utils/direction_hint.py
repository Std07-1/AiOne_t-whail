"""Utility helpers for deriving soft direction hints.

The helpers here intentionally avoid importing heavy modules so that unit
 tests can import them without starting the full trading pipeline.
"""

from __future__ import annotations

__all__ = ["infer_direction_hint"]


def infer_direction_hint(
    phase: str | None,
    phase_state_current: str | None,
    whale_bias: float | None,
    scenario: str | None,
) -> str | None:
    """Return a soft direction hint based on phase, state, bias and scenario.

    The function is deliberately pure so it can be exercised from unit tests
    without touching the rest of the pipeline. It encodes the current policy
    where only pullback_continuation scenarios are allowed to emit a hint and
    requires whale bias alignment with either the detected phase or the
    carried phase state.
    """

    try:
        bias = float(whale_bias) if whale_bias is not None else None
    except Exception:
        bias = None
    if bias is None:
        return None

    scen = (scenario or "").strip().lower()
    if scen != "pullback_continuation":
        return None

    phase_norm = (phase or "").strip().lower()
    phase_state_norm = (phase_state_current or "").strip().lower()
    phases = {phase_norm, phase_state_norm}

    if bias <= -0.25 and phases.intersection({"trend_down", "false_breakout"}):
        return "short"
    if bias >= 0.25 and phases.intersection({"trend_up", "false_breakout"}):
        return "long"
    return None
