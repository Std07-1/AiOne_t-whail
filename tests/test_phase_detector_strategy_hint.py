import pytest

from config.config import (
    K_CUMULATIVE_DELTA,
    K_DIRECTIONAL_VOLUME_RATIO,
    K_PRICE_SLOPE_ATR,
)
from stage2 import phase_detector


def _base_stats() -> dict[str, float | str]:
    return {
        "vol_z": 2.6,
        "trap": {"score": 0.72},
        "turnover_usd": 4_500_000.0,
        "near_edge": "lower",
        "rsi": 30.0,
        "atr_pct": 0.009,
        "atr_pct_p50": 0.002,
    }


def _base_context(dvr_value: float) -> dict[str, object]:
    return {
        "symbol": "TESTUSDT",
        "key_levels_meta": {"band_pct": 0.03, "is_near_edge": True},
        "meta": {"htf_ok": False},
        "directional": {
            K_DIRECTIONAL_VOLUME_RATIO: dvr_value,
            K_CUMULATIVE_DELTA: -0.4,
            K_PRICE_SLOPE_ATR: -0.72,
        },
    }


def _assert_exhaustion_phase(result: phase_detector.PhaseResult) -> None:
    assert result.phase == "exhaustion"
    # Перевіряємо близькість до 0.9 з допуском 10%
    assert result.score == pytest.approx(0.9, rel=0.1)


def test_exhaustion_hint_emitted_when_guards_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(phase_detector, "EXH_STRATEGY_HINT_ENABLED_FLAG", True)

    stats = _base_stats()
    context = _base_context(1.1)

    result = phase_detector.detect_phase(stats, context)

    _assert_exhaustion_phase(result)
    assert result.strategy_hint == phase_detector.STRATEGY_HINT_EXHAUSTION_REVERSAL
    assert any(
        reason.startswith("strategy_hint_") for reason in result.reasons
    ), "strategy_hint reason must be present"


def test_exhaustion_hint_suppressed_when_dvr_high(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(phase_detector, "EXH_STRATEGY_HINT_ENABLED_FLAG", True)

    stats = _base_stats()
    context = _base_context(1.5)  # вище гейту 1.35 → hint вимикається

    result = phase_detector.detect_phase(stats, context)

    _assert_exhaustion_phase(result)
    assert result.strategy_hint is None
    assert not any(reason.startswith("strategy_hint_") for reason in result.reasons)
