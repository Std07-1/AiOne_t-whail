from __future__ import annotations

import pytest

from stage2 import phase_detector as pd
from stage2.phase_detector import PhaseResult


@pytest.fixture(autouse=True)
def reset_phase_state_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure tests can freely toggle the carry-forward flag without impacting others.
    monkeypatch.setattr(pd, "PHASE_STATE_ENABLED_FLAG", False, raising=False)


def _make_stats(reason: str, age_s: float) -> dict[str, object]:
    return {
        "phase_state": {
            "current_phase": "trend_up",
            "phase_score": 0.77,
            "age_s": age_s,
            "last_reason": reason,
        }
    }


def test_phase_state_carry_applies_for_soft_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pd, "PHASE_STATE_ENABLED_FLAG", True, raising=False)
    stats = _make_stats("presence_cap_no_bias_htf", age_s=45.0)
    result = PhaseResult(phase=None, score=0.1, reasons=["presence_cap_no_bias_htf"])

    applied, info = pd._apply_phase_state_effective(
        result=result,
        stats_obj=stats,
        symbol="TEST",
        raw_phase=None,
        raw_score=0.1,
    )

    assert applied is True
    assert info is not None
    assert result.phase == "trend_up"
    assert result.score == pytest.approx(0.77)
    assert info["reason"] == "presence_cap_no_bias_htf"
    assert info["phase"] == "trend_up"


def test_phase_state_carry_skips_hard_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pd, "PHASE_STATE_ENABLED_FLAG", True, raising=False)
    stats = _make_stats("htf_conflict", age_s=30.0)
    result = PhaseResult(phase=None, score=0.4, reasons=["htf_conflict"])

    applied, info = pd._apply_phase_state_effective(
        result=result,
        stats_obj=stats,
        symbol="TEST",
        raw_phase=None,
        raw_score=0.4,
    )

    assert applied is False
    assert info is None
    assert result.phase is None
    assert result.score == pytest.approx(0.4)
