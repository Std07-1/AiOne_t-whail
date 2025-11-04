from __future__ import annotations

from stage2 import policy_engine_v2 as pe


def test_impulse_gate_returns_decision() -> None:
    # stats: достатній нахил і прості ознаки
    stats = {
        "symbol": "TESTUSDT",
        "price_slope_atr": 1.0,
        "band_pct": 0.02,
        "presence_sustain": 0.5,
        "htf_strength": 0.3,
    }
    # ds: сформуємо vol5 і median20
    ds = {
        "volume_1m_tail": [10.0] * 19 + [50.0]
    }  # median~10, vol5=sum(last5)≈90 ≥ 1.8×10
    dec = pe.decide(stats, ds)
    assert dec is not None
    assert dec.class_ == "IMPULSE"
    assert dec.side in ("long", "short")
    assert 0.0 <= dec.p_win <= 1.0


def test_no_gate_returns_none() -> None:
    stats = {
        "symbol": "TESTUSDT",
        "price_slope_atr": 0.0,
        "band_pct": 0.10,
        "presence_sustain": 0.1,
        "htf_strength": 0.01,
    }
    ds = {"volume_1m_tail": [1.0] * 10}
    dec = pe.decide(stats, ds)
    assert dec is None
