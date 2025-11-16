from __future__ import annotations

import time

from stage2.phase_state import PhaseStateManager


class _FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    def setex(
        self, key: str, ttl: int, value: str
    ) -> None:  # pragma: no cover - simple stub
        self._store[key] = value

    def set(self, key: str, value: str) -> None:
        self._store[key] = value

    def get(self, key: str) -> str | None:
        return self._store.get(key)


def _make_manager() -> PhaseStateManager:
    return PhaseStateManager(redis_client=_FakeRedis(), namespace="test_phase_state")


def test_phase_state_normal_update() -> None:
    mgr = _make_manager()
    state = mgr.update_from_detector(
        symbol="TEST",
        granularity="1m",
        raw_phase="momentum",
        raw_score=0.72,
        phase_reason=None,
        whale_presence=0.65,
        whale_bias=0.35,
        htf_strength=0.4,
        now_ts=time.time(),
    )
    assert state.current_phase == "momentum"
    assert state.phase_score == 0.72
    assert state.age_s == 0.0
    assert state.last_reason is None


def test_phase_state_carry_forward_soft_reason() -> None:
    mgr = _make_manager()
    now = time.time()
    mgr.update_from_detector(
        symbol="TEST",
        granularity="1m",
        raw_phase="momentum",
        raw_score=0.8,
        phase_reason=None,
        whale_presence=0.7,
        whale_bias=0.4,
        htf_strength=0.5,
        now_ts=now,
    )
    later_state = mgr.update_from_detector(
        symbol="TEST",
        granularity="1m",
        raw_phase=None,
        raw_score=0.0,
        phase_reason="presence_cap_no_bias_htf",
        whale_presence=0.7,
        whale_bias=0.35,
        htf_strength=0.45,
        now_ts=now + 30,
    )
    assert later_state.current_phase == "momentum"
    assert later_state.last_reason == "presence_cap_no_bias_htf"
    assert later_state.age_s > 0.0


def test_phase_state_hard_reset_on_conflict_reason() -> None:
    mgr = _make_manager()
    now = time.time()
    mgr.update_from_detector(
        symbol="TEST",
        granularity="1m",
        raw_phase="momentum",
        raw_score=0.9,
        phase_reason=None,
        whale_presence=0.6,
        whale_bias=0.3,
        htf_strength=0.6,
        now_ts=now,
    )
    reset_state = mgr.update_from_detector(
        symbol="TEST",
        granularity="1m",
        raw_phase=None,
        raw_score=0.0,
        phase_reason="htf_conflict",
        whale_presence=0.3,
        whale_bias=-0.2,
        htf_strength=0.1,
        now_ts=now + 10,
    )
    assert reset_state.current_phase is None
    assert reset_state.phase_score == 0.0
    assert reset_state.last_reason == "htf_conflict"
    assert reset_state.age_s == 0.0


def test_phase_state_no_carry_when_reason_not_soft() -> None:
    mgr = _make_manager()
    now = time.time()
    mgr.update_from_detector(
        symbol="TEST",
        granularity="1m",
        raw_phase="momentum",
        raw_score=0.85,
        phase_reason=None,
        whale_presence=0.6,
        whale_bias=0.35,
        htf_strength=0.4,
        now_ts=now,
    )
    state = mgr.update_from_detector(
        symbol="TEST",
        granularity="1m",
        raw_phase=None,
        raw_score=0.0,
        phase_reason="trend_weak",
        whale_presence=0.55,
        whale_bias=0.34,
        htf_strength=0.3,
        now_ts=now + 30,
    )
    assert state.current_phase is None
    assert state.last_reason == "trend_weak"


def test_phase_state_bias_flip_blocks_carry() -> None:
    mgr = _make_manager()
    now = time.time()
    mgr.update_from_detector(
        symbol="TEST",
        granularity="1m",
        raw_phase="momentum",
        raw_score=0.9,
        phase_reason=None,
        whale_presence=0.7,
        whale_bias=0.4,
        htf_strength=0.5,
        now_ts=now,
    )
    state = mgr.update_from_detector(
        symbol="TEST",
        granularity="1m",
        raw_phase=None,
        raw_score=0.0,
        phase_reason="presence_cap_no_bias_htf",
        whale_presence=0.65,
        whale_bias=-0.5,
        htf_strength=0.45,
        now_ts=now + 40,
    )
    assert state.current_phase is None
    assert state.last_reason == "presence_cap_no_bias_htf"
