from __future__ import annotations

from process_asset_batch.global_state import (
    _SCEN_EXPLAIN_BATCH_COUNTER,
    _SCEN_EXPLAIN_LAST_TS,
)
from process_asset_batch.helpers import _explain_should_log, explain_should_log


def test_explain_should_log_rate_limit_basic():
    sym = "btcusdt"
    _SCEN_EXPLAIN_LAST_TS.clear()
    _SCEN_EXPLAIN_BATCH_COUNTER.clear()

    now = 100.0
    assert _explain_should_log(sym, now, min_period_s=10.0) is True

    # set last ts to now; within 5s should be False
    _SCEN_EXPLAIN_LAST_TS[sym] = now
    assert _explain_should_log(sym, now + 5.0, min_period_s=10.0) is False
    # after 10s or more should be True
    assert _explain_should_log(sym, now + 10.0, min_period_s=10.0) is True


def test_explain_should_log_every_n_behavior():
    sym = "ethusdt"
    _SCEN_EXPLAIN_LAST_TS.clear()
    _SCEN_EXPLAIN_BATCH_COUNTER.clear()

    assert (
        explain_should_log(sym, 0.0, min_period_s=10.0, every_n=3, force_all=False)
        is True
    )
    assert (
        explain_should_log(sym, 1.0, min_period_s=10.0, every_n=3, force_all=False)
        is False
    )
    assert (
        explain_should_log(sym, 2.0, min_period_s=10.0, every_n=3, force_all=False)
        is False
    )
    # третій виклик (лічильник % 3 == 0) має логувати heartbeat
    assert (
        explain_should_log(sym, 3.0, min_period_s=10.0, every_n=3, force_all=False)
        is True
    )


def test_explain_should_log_respects_force_all():
    sym = "xrpusdt"
    _SCEN_EXPLAIN_LAST_TS.clear()
    _SCEN_EXPLAIN_BATCH_COUNTER.clear()

    assert explain_should_log(sym, 0.0, min_period_s=30.0, force_all=True) is True
    # одразу після цього, навіть без проходження min_period, має залишатися True
    assert explain_should_log(sym, 0.5, min_period_s=30.0, force_all=True) is True
    # коли force_all=False і минув лише 1с, очікуємо False
    assert explain_should_log(sym, 1.0, min_period_s=30.0, force_all=False) is False
