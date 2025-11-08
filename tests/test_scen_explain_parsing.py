from __future__ import annotations

import re


def test_scen_explain_line_matches_quality_regex():
    # Імітація нового уніфікованого рядка [SCEN_EXPLAIN]
    line = (
        "[11/06/25 10:00:00] [SCEN_EXPLAIN] symbol=btcusdt "
        'scenario=breakout_confirmation explain="" '
        "ctx={nep=0.42,ps=0.38,htfs=0.21} btc_gate=skipped conf=0.33"
    )
    # Регекс із tools.scenario_quality_report
    explain_re = re.compile(
        r"\\[SCEN_EXPLAIN\\].*symbol=([a-z0-9]+).*scenario=([a-z_]+|none).*explain=\"([^\"]*)\""
    )
    m = explain_re.search(line)
    assert m, "SCEN_EXPLAIN рядок має відповідати шаблону scenario_quality_report"
    assert m.group(1) == "btcusdt"
    assert m.group(2) == "breakout_confirmation"
    assert m.group(3) == ""
