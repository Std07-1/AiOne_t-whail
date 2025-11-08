import math

import pytest

from tools.forward_from_log import (
    _dedup_key,
    _parse_symbol,
    _parse_ts_ms,
    _passes_thresholds,
)


# ── _parse_ts_ms tests ───────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "line,expected",
    [
        ("ts=1681234567890 presence=0.1", 1681234567890),
        ("2025-11-04T12:34:56Z [STRICT_WHALE] presence=0.5", 1762259696000),
        ("[11/04/25 12:34:56] symbol=BTCUSDT presence=0.5", 1762259696000),
    ],
)
def test_parse_ts_ms_various(line, expected):
    assert _parse_ts_ms(line) == expected


def test_parse_ts_ms_strict_error():
    with pytest.raises(ValueError):
        _parse_ts_ms("no timestamp here", strict=True)


# ── _parse_symbol tests ──────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "line,expected",
    [
        ("symbol=BTCUSDT presence=0.1", "BTCUSDT"),
        ("btcusdt bias=0.2", "BTCUSDT"),
        ("Some prefix ETHUSDT other", "ETHUSDT"),
    ],
)
def test_parse_symbol(line, expected):
    assert _parse_symbol(line) == expected


def test_parse_symbol_strict_error():
    with pytest.raises(ValueError):
        _parse_symbol("no symbol here", strict=True)


# ── Dedup key tests ─────────────────────────────────────────────────────────
def test_dedup_key_direction():
    k_pos = _dedup_key("BTCUSDT", 1000, 0.5)
    k_neg = _dedup_key("BTCUSDT", 1000, -0.5)
    assert k_pos != k_neg
    assert k_pos.endswith("|+")
    assert k_neg.endswith("|-")


def test_dedup_no_merge_nearby():
    # ts ±1 не мають зливатися
    k1 = _dedup_key("BTCUSDT", 1000, 0.1)
    k2 = _dedup_key("BTCUSDT", 1001, 0.1)
    assert k1 != k2


# ── Threshold filter tests ───────────────────────────────────────────────────
@pytest.mark.parametrize(
    "presence,bias,pres_min,bias_min,expected",
    [
        (0.75, 0.60, 0.75, 0.60, True),  # дорівнює порогу → проходить
        (0.7499, 0.60, 0.75, 0.60, False),  # трохи нижче presence
        (0.80, 0.5999, 0.75, 0.60, False),  # bias трохи нижче
        (0.80, -0.60, 0.75, 0.60, True),  # знак не впливає (abs)
        (math.nan, 0.60, 0.75, 0.60, False),
    ],
)
def test_passes_thresholds(presence, bias, pres_min, bias_min, expected):
    assert _passes_thresholds(presence, bias, pres_min, bias_min) is expected


# ── Integration style mini-run for thresholds ───────────────────────────────
def test_thresholds_edge_equal():
    assert _passes_thresholds(0.75, 0.60, 0.75, 0.60)
