import asyncio
import logging
from collections import deque
from types import SimpleNamespace

import pytest

import app.process_asset_batch as pab
import process_asset_batch.router_signal_v2 as router_module
from config.constants import (
    K_CUMULATIVE_DELTA,
    K_DIRECTIONAL_VOLUME_RATIO,
    K_PRICE_SLOPE_ATR,
)


@pytest.fixture(autouse=True)
def _reset_warn_state():
    pab._WHALE_WARN_LAST_TS.clear()
    pab._WHALE_METRIC_RING.clear()
    yield
    pab._WHALE_WARN_LAST_TS.clear()
    pab._WHALE_METRIC_RING.clear()


def test_whale_diag_warn_emits(monkeypatch):
    calls: list[str] = []
    records: list[logging.LogRecord] = []

    class _Spy(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    spy = _Spy(level=logging.WARNING)
    monkeypatch.setattr(
        pab,
        "inc_whale_presence_zero_nonstale",
        lambda s: calls.append(s),
        raising=False,
    )
    pab._WHALE_METRIC_RING["btcusdt"] = deque([(0.5, 0.2), (0.0, -0.1)], maxlen=3)
    logger = pab.logger
    logger.addHandler(spy)
    try:
        triggered = pab._maybe_warn_whale_presence_zero("btcusdt", 0.0, False, 4200)
    finally:
        logger.removeHandler(spy)
    assert triggered is True
    msgs = [rec.getMessage() for rec in records]
    assert any("[WHALE_DIAG]" in msg for msg in msgs)
    assert any("head=0.50/0.20" in msg for msg in msgs)
    assert any("tail=0.00/-0.10" in msg for msg in msgs)
    assert calls == ["BTCUSDT"]


def test_whale_diag_warn_skips_when_stale(monkeypatch):
    records: list[logging.LogRecord] = []

    class _Spy(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    spy = _Spy(level=logging.WARNING)
    monkeypatch.setattr(
        pab, "inc_whale_presence_zero_nonstale", lambda _symbol: None, raising=False
    )
    logger = pab.logger
    logger.addHandler(spy)
    try:
        triggered = pab._maybe_warn_whale_presence_zero("ethusdt", 0.0, True, 9100)
    finally:
        logger.removeHandler(spy)
    assert triggered is False
    assert not any("[WHALE_DIAG]" in rec.getMessage() for rec in records)


def test_no_whale_payload_warn_sets_presence_none(monkeypatch):
    class DummyContext:
        def __init__(self) -> None:
            stats = {
                "symbol": "ethusdt",
                "band_pct": 0.01,
                "near_edge": "upper",
                "volume_z": 0.0,
                "rsi": 55.0,
                "atr": 1.0,
                "current_price": 2000.0,
                K_DIRECTIONAL_VOLUME_RATIO: 0.5,
                K_CUMULATIVE_DELTA: 0.1,
                K_PRICE_SLOPE_ATR: 0.0,
            }
            self.df = object()
            self.normalized = {
                "signal": "NORMAL",
                "stats": stats,
                "trigger_reasons": [],
                "raw_trigger_reasons": [],
            }
            self.stats = stats

    async def fake_prepare_stage1_context(*_args, **_kwargs):
        return DummyContext()

    monkeypatch.setattr(pab, "prepare_stage1_context", fake_prepare_stage1_context)
    monkeypatch.setattr(pab, "STAGE2_HINT_ENABLED", False)
    monkeypatch.setattr(pab, "STAGE2_SIGNAL_V2_ENABLED", False)
    monkeypatch.setattr(pab, "SCENARIO_TRACE_ENABLED", False)
    monkeypatch.setattr(pab, "SCENARIO_CANARY_PUBLISH_CANDIDATE", False)
    monkeypatch.setattr(pab, "update_accum_monitor", lambda *a, **k: {})
    monkeypatch.setattr(
        pab,
        "detect_phase_from_stats",
        lambda *a, **k: {"name": None, "score": 0.0, "reasons": []},
    )
    monkeypatch.setattr(pab, "resolve_scenario", lambda *a, **k: (None, 0.0))
    monkeypatch.setattr(pab, "emit_prom_presence", lambda *a, **k: None)
    monkeypatch.setattr(pab, "router_signal_v2", lambda *a, **k: ("OBSERVE", 0.0, []))

    class DummyRedis:
        def __init__(self) -> None:
            self.cfg = SimpleNamespace(namespace="ai_one")

        async def jget(self, *_parts, **_kwargs):
            return None

    class DummyStore:
        def __init__(self) -> None:
            self.redis = DummyRedis()

    state_manager = pab.AssetStateManager(initial_assets=["ethusdt"])

    records: list[str] = []

    class _Spy(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:  # type: ignore[override]
            records.append(record.getMessage())

    spy = _Spy(level=logging.WARNING)
    pab.logger.addHandler(spy)
    try:

        async def _run() -> None:
            await pab.ProcessAssetBatchv1.process_asset_batch(
                ["ETHUSDT"],
                monitor=None,  # type: ignore[arg-type]
                store=DummyStore(),
                timeframe="1m",
                lookback=10,
                state_manager=state_manager,
            )

        asyncio.run(_run())
    finally:
        pab.logger.removeHandler(spy)

    assert any("no_whale_payload" in msg for msg in records)
    whale_stats = state_manager.state["ethusdt"]["stats"].get("whale")
    assert isinstance(whale_stats, dict)
    assert whale_stats.get("presence") is None
    assert whale_stats.get("stale") is True


def test_router_signal_no_payload_increments_counter(monkeypatch):
    hits: list[str] = []

    def _stub(symbol: str) -> None:
        hits.append(symbol)

    monkeypatch.setattr(router_module, "_inc_whale_no_payload", _stub, raising=False)

    stats = {
        "symbol": "ethusdt",
        "whale": {
            "bias": 0.0,
            "vwap_dev": 0.0,
            "zones_summary": {},
            "stale": True,
        },
        "band_pct": 0.01,
        "near_edge": "upper",
        "rsi": 50.0,
        K_DIRECTIONAL_VOLUME_RATIO: 0.8,
        K_CUMULATIVE_DELTA: 0.1,
        K_PRICE_SLOPE_ATR: 0.0,
    }
    profile_cfg = {"long": {}, "short": {}}

    sig, conf, reasons = router_module.router_signal_v2(stats, profile_cfg)

    assert sig == "OBSERVE"
    assert conf == 0.0
    assert "no_whale" in reasons
    assert hits == ["ETHUSDT"]
