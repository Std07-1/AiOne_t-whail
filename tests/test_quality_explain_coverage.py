from __future__ import annotations

from datetime import datetime, timedelta

from tools.scenario_quality_report import _analyze


def _ts(dt: datetime) -> str:
    # format: [MM/DD/YY HH:MM:SS]
    return dt.strftime("[%m/%d/%y %H:%M:%S]")


def test_explain_coverage_and_latency_present():
    now = datetime.utcnow().replace(microsecond=0)
    sym = "btcusdt"
    scen = "pullback_continuation"

    t0 = now
    t_ex = t0 + timedelta(seconds=5)
    t_act = t0 + timedelta(seconds=10)

    lines = [
        f"{_ts(t0)} [SCENARIO_TRACE] symbol={sym} candidate={scen} decision=ACCEPT causes=near_edge,htf_ok",
        f'{_ts(t_ex)} [SCEN_EXPLAIN] symbol={sym} scenario={scen} explain="in_zone=1; compression=0.5"',
        f"{_ts(t_act)} [SCENARIO_ALERT] symbol={sym} activate={scen} conf=0.70",
    ]

    rows = _analyze(lines)
    # Find our row
    row = next(r for r in rows if r["symbol"] == sym.upper() and r["scenario"] == scen)
    assert row["explain_coverage_rate"] == 1.0
    # latency from first TRACE (t0) to explain (t_ex) ~= 5000 ms
    assert row["explain_latency_ms"] == 5000


def test_explain_coverage_absent():
    now = datetime.utcnow().replace(microsecond=0)
    sym = "btcusdt"
    scen = "breakout_confirmation"

    t0 = now
    t_act = t0 + timedelta(seconds=20)

    lines = [
        f"{_ts(t0)} [SCENARIO_TRACE] symbol={sym} candidate={scen} decision=ACCEPT causes=volz,near_edge",
        f"{_ts(t_act)} [SCENARIO_ALERT] symbol={sym} activate={scen} conf=0.55",
    ]

    rows = _analyze(lines)
    row = next(r for r in rows if r["symbol"] == sym.upper() and r["scenario"] == scen)
    assert row["explain_coverage_rate"] == 0.0
    assert row["explain_latency_ms"] == 0
