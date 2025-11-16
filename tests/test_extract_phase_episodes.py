from pathlib import Path

from tools.extract_phase_episodes import extract_episodes, write_markdown

SAMPLE_LOG = """[01/01/25 00:00:00] INFO     [PHASE_STATE_UPDATE] symbol=btcusdt current_phase=trend_up age_s=10.0 reason=init
[01/01/25 00:00:00] INFO     [HTF] symbol=btcusdt ok=True score=1.0 strength=0.42 reason=dummy
[01/01/25 00:00:00] INFO     [STRICT_WHALE] symbol=btcusdt bias=0.30 presence=0.10
[01/01/25 00:00:01] INFO     [SCENARIO_TRACE] symbol=btcusdt candidate=breakout_confirmation pred={'near_edge': 'upper',
                             'penalty': 0.2, 'direction_hint': 'short'} decision=ACCEPT reason=phase=None
                             conf=0.45
[01/01/25 00:00:01] INFO     [SCEN_EXPLAIN] symbol=btcusdt scenario=breakout_confirmation direction_hint=short
[01/01/25 00:00:02] INFO     [PHASE_STATE_CARRY] symbol=btcusdt phase_raw=None phase=false_breakout age_s=33.0 reason=htf_gray_low
[01/01/25 00:00:03] INFO     [PROM] context=dummy
                    INFO     [SCENARIO_ALERT] symbol=btcusdt activate=pullback_continuation conf=0.60 side=long direction_hint=long
[01/01/25 00:00:03] INFO     [PHASE_STATE_CARRY] symbol=btcusdt phase_raw=None phase=momentum age_s=12.0 reason=from_alert
[01/01/25 00:00:04] INFO     [SCENARIO_TRACE] symbol=ethusdt candidate=mean_revert decision=ACCEPT reason=phase=ACCUM conf=0.20
"""


def test_extract_and_markdown(tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    log_path.write_text(SAMPLE_LOG, encoding="utf-8")

    episodes = extract_episodes(
        log_path=log_path,
        symbol="btcusdt",
        min_conf=0.4,
        carried_only=False,
        max_episodes=5,
    )
    assert len(episodes) == 4
    assert episodes[0].scenario == "breakout_confirmation"
    assert episodes[1].phase == "false_breakout"
    assert episodes[2].scenario == "pullback_continuation"
    assert episodes[0].phase_state_current == "trend_up"
    assert episodes[0].phase_state_age_s == 10.0
    assert episodes[0].whale_bias == 0.30
    assert episodes[0].htf_strength == 0.42
    assert episodes[0].direction_hint == "short"
    assert episodes[1].direction_hint == "short"
    assert episodes[2].direction_hint == "long"

    carry_only = extract_episodes(
        log_path=log_path,
        symbol="btcusdt",
        min_conf=0.4,
        carried_only=True,
        max_episodes=5,
    )
    assert len(carry_only) == 2

    out_path = tmp_path / "episodes.md"
    write_markdown(out_path, episodes)
    md_lines = [
        ln for ln in out_path.read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    # header + delimiter + 4 data rows expected
    assert len(md_lines) == 6
    assert "phase_state_current" in md_lines[0]
    assert "direction_hint" in md_lines[0]
    assert "breakout_confirmation" in md_lines[2]
    assert "false_breakout" in md_lines[3]
    assert "trend_up" in md_lines[2]
    assert "0.30" in md_lines[2]
    assert "short" in md_lines[2]
