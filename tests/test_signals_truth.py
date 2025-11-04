from __future__ import annotations

from pathlib import Path

from tools import signals_truth as st


def test_parse_signal_line() -> None:
    line = "[SIGNAL_V2] symbol=BTCUSDT class=IMPULSE side=long p=0.62 sl=0.80 tp=2.00 reason=ok feats={}"  # noqa: E501
    tmp = Path("./.tmp_test_log.txt")
    tmp.write_text(line, encoding="utf-8")
    try:
        rows = list(st._iter_signal_lines([tmp]))
        assert rows and rows[0][0] == "BTCUSDT" and rows[0][1] == "IMPULSE"
    finally:
        try:
            tmp.unlink()
        except Exception:
            pass


def test_truth_functions_return_string() -> None:
    sig = ("BTCUSDT", "IMPULSE", "long", 0.6)
    verdict = st.evaluate_impulse(sig, Path("datastore"))
    assert verdict in {"TP", "FP", "UNDECIDED"}
