from utils.direction_hint import infer_direction_hint


def test_direction_hint_short_bias_negative() -> None:
    hint = infer_direction_hint(
        phase="false_breakout",
        phase_state_current=None,
        whale_bias=-0.4,
        scenario="pullback_continuation",
    )
    assert hint == "short"


def test_direction_hint_long_from_state() -> None:
    hint = infer_direction_hint(
        phase=None,
        phase_state_current="trend_up",
        whale_bias=0.35,
        scenario="pullback_continuation",
    )
    assert hint == "long"


def test_direction_hint_none_without_bias() -> None:
    hint = infer_direction_hint(
        phase="trend_up",
        phase_state_current="trend_up",
        whale_bias=None,
        scenario="pullback_continuation",
    )
    assert hint is None


def test_direction_hint_none_wrong_scenario() -> None:
    hint = infer_direction_hint(
        phase="trend_down",
        phase_state_current="trend_down",
        whale_bias=-0.5,
        scenario="breakout_confirmation",
    )
    assert hint is None
