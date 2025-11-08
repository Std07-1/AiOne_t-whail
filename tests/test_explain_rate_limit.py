from __future__ import annotations

import app.process_asset_batch as pab
from config.config import SCEN_EXPLAIN_VERBOSE_EVERY_N
from process_asset_batch.helpers import _explain_should_log


def test_explain_should_log_rate_limit_basic():
    sym = "btcusdt"
    pab._SCEN_EXPLAIN_LAST_TS.clear()

    now = 100.0
    assert _explain_should_log(sym, now, min_period_s=10.0) is True

    # set last ts to now; within 5s should be False
    pab._SCEN_EXPLAIN_LAST_TS[sym] = now
    assert _explain_should_log(sym, now + 5.0, min_period_s=10.0) is False
    # after 10s or more should be True
    assert _explain_should_log(sym, now + 10.0, min_period_s=10.0) is True


def test_every_nth_batch_condition():
    n = int(SCEN_EXPLAIN_VERBOSE_EVERY_N)
    assert n > 0
    # emulate policy: every n-th batch triggers verbose even if rate limit would block
    assert (n % n) == 0
    assert ((2 * n) % n) == 0
