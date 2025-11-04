import time
from typing import Any

import pytest

from utils.phase_signalizer import event_to_soft_signal


def test_soft_signal_propagates_strategy_hint(monkeypatch: Any) -> None:
    from whale import insight_builder as ib  # type: ignore

    monkeypatch.setattr(ib, "classify_enriched", lambda *_args, **_kw: "TRADEABLE")
    monkeypatch.setattr(ib, "next_actions_enriched", lambda *a, **k: ["ok"])

    ev = {
        "symbol": "TESTUSDT",
        "phase": "exhaustion",
        "score": 0.8,
        "htf_ok": True,
        "htf_strength": 0.04,
        "whale_presence": 0.7,
        "whale_bias": 0.2,
        "whale_vwap_dev": 0.4,
        "whale_vol_regime": "normal",
        "current_price": 100.0,
        "atr": 1.2,
        "near_edge": "lower",
        "band_pct": 0.8,
        "ts": "2025-01-01T00:00:00Z",
        "strategy_hint": "exhaustion_reversal_long",
    }

    out = event_to_soft_signal(ev)
    meta = out["market_context"]["meta"]
    assert meta.get("strategy_hint") == "exhaustion_reversal_long"
    assert meta.get("strategy") == "exhaustion_reversal_long"
    assert meta.get("insights", {}).get("strategy_hint") == "exhaustion_reversal_long"


@pytest.mark.parametrize(
    "phase, bias, presence, htf_ok, expected",
    [
        ("pre_breakout", 0.7, 0.8, True, "SOFT_BUY"),
        ("momentum", -0.65, 0.75, True, "SOFT_SELL"),
    ],
)
def test_soft_signal_happy_and_short_mirror(
    monkeypatch: Any,
    phase: str,
    bias: float,
    presence: float,
    htf_ok: bool,
    expected: str,
) -> None:
    # Monkeypatch classify_enriched to always allow trading for the test
    from whale import insight_builder as ib  # type: ignore

    monkeypatch.setattr(ib, "classify_enriched", lambda *_args, **_kw: "TRADEABLE")
    monkeypatch.setattr(ib, "next_actions_enriched", lambda *a, **k: ["ok"])

    ev = {
        "symbol": "TESTUSDT",
        "phase": phase,
        "score": 0.7,
        "htf_ok": htf_ok,
        "htf_strength": 0.03,
        "whale_presence": presence,
        "whale_bias": bias,
        "whale_vwap_dev": 0.5 if bias > 0 else -0.5,
        "whale_vol_regime": "normal",
        "current_price": 100.0,
        "atr": 1.0,
        "near_edge": True,
        "band_pct": 0.9,
        "ts": "2025-01-01T00:00:00Z",
    }
    out = event_to_soft_signal(ev)
    assert out["recommendation"] == expected
    # confidence should be sizeable when presence/bias are high and non-stale
    assert float(out["confidence_metrics"]["confidence"]) > 0.4


def test_soft_signal_stale_forces_observe(monkeypatch: Any) -> None:
    from whale import insight_builder as ib  # type: ignore

    monkeypatch.setattr(ib, "classify_enriched", lambda *_args, **_kw: "TRADEABLE")

    ev = {
        "symbol": "TESTUSDT",
        "phase": "pre_breakout",
        "score": 0.8,
        "htf_ok": True,
        "whale_presence": 0.9,
        "whale_bias": 0.8,
        "whale_vwap_dev": 0.6,
        "whale_vol_regime": "normal",
        "whale_stale": True,  # <- ключова перевірка
        "current_price": 100.0,
        "atr": 1.0,
        "ts": "2025-01-01T00:00:00Z",
    }
    out = event_to_soft_signal(ev)
    assert out["recommendation"] == "OBSERVE"


def test_soft_signal_hyper_vol_blocks(monkeypatch: Any) -> None:
    from whale import insight_builder as ib  # type: ignore

    monkeypatch.setattr(ib, "classify_enriched", lambda *_args, **_kw: "TRADEABLE")

    ev = {
        "symbol": "TESTUSDT",
        "phase": "pre_breakout",
        "score": 0.8,
        "htf_ok": True,
        "whale_presence": 0.9,
        "whale_bias": 0.8,
        "whale_vwap_dev": 0.6,
        "whale_vol_regime": "hyper",  # <- блокує
        "current_price": 100.0,
        "atr": 1.0,
        "ts": "2025-01-01T00:00:00Z",
    }
    out = event_to_soft_signal(ev)
    assert out["recommendation"] == "OBSERVE"


@pytest.mark.skip(
    reason="Flap guard не реалізовано: потребує історії/кільця для debounce"
)
def test_flap_guard_series_debounced() -> None:
    pass


def test_latency_throughput_900_events_under_budget(monkeypatch: Any) -> None:
    # Виміряти середній час обробки 900 подій (<200мс/бар цілком має виконуватись для цієї легкої гілки)
    from whale import insight_builder as ib  # type: ignore

    monkeypatch.setattr(ib, "classify_enriched", lambda *_args, **_kw: "TRADEABLE")

    base = {
        "symbol": "TESTUSDT",
        "phase": "pre_breakout",
        "score": 0.6,
        "htf_ok": True,
        "whale_presence": 0.6,
        "whale_bias": 0.5,
        "whale_vwap_dev": 0.4,
        "whale_vol_regime": "normal",
        "current_price": 100.0,
        "atr": 1.2,
        "ts": "2025-01-01T00:00:00Z",
    }
    events = []
    for i in range(900):
        ev = dict(base)
        ev["whale_presence"] = 0.4 + 0.6 * ((i % 300) / 300)
        ev["whale_bias"] = 0.3 + 0.7 * ((i % 150) / 150)
        events.append(ev)

    t0 = time.perf_counter()
    for ev in events:
        _ = event_to_soft_signal(ev)
    dt = time.perf_counter() - t0
    avg_ms = (dt / len(events)) * 1000.0
    assert avg_ms < 200.0, f"avg_ms per event too high: {avg_ms:.2f}ms"
