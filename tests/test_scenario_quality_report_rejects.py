from __future__ import annotations

from datetime import datetime

from tools import scenario_quality_report as sqr


def _ts() -> str:
    now = datetime.utcnow()
    return now.strftime("[%m/%d/%y %H:%M:%S]")


essential_trace_prefix = " pred={'near_edge': None, 'dvr': None, 'vol_z': None, 'htf_strength': 0.1, 'presence': 0.3}"


def test_quality_report_reject_breakdown_top3() -> None:
    ts = _ts()
    lines: list[str] = [
        f"{ts} [SCENARIO_TRACE] symbol=btcusdt candidate=pullback_continuation"
        f"{essential_trace_prefix} decision=REJECT reason=phase=ACCUM_MONITOR conf=0.00 causes=presence_missing,htf_below_min",
        f"{ts} [SCENARIO_TRACE] symbol=btcusdt candidate=pullback_continuation"
        f"{essential_trace_prefix} decision=REJECT reason=phase=ACCUM_MONITOR conf=0.00 causes=presence_below_min",
        f"{ts} [SCENARIO_TRACE] symbol=btcusdt candidate=pullback_continuation"
        f"{essential_trace_prefix} decision=ACCEPT reason=phase=SOMETHING conf=0.70",
        f"{ts} [SCENARIO_ALERT] symbol=btcusdt activate=pullback_continuation conf=0.70",
    ]

    rows = sqr._analyze(lines)
    assert rows, "Очікуємо принаймні один рядок для BTCUSDT/pullback_continuation"

    # Знайдемо саме нашу пару (symbol, scenario)
    row = next(
        r
        for r in rows
        if r["symbol"] == "BTCUSDT" and r["scenario"] == "pullback_continuation"
    )

    # Було 2 REJECT із двома різними причинами — отже top1_rate і top2_rate по 0.5
    rates = {row.get("reject_top1_rate"), row.get("reject_top2_rate")}  # type: ignore[index]
    assert rates == {0.5}

    # Імена мають бути з множини відомих причин
    names = {row.get("reject_top1"), row.get("reject_top2")}  # type: ignore[index]
    assert names.issubset({"presence_missing", "presence_below_min"})
