from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from config.config import K_PRICE_SLOPE_ATR
from config.flags import WHALE_CARRY_FORWARD_TTL_S
from process_asset_batch.pipeline.whale_stage2 import run_whale_stage2


class DummyRedis:
    async def jget(self, *_args: object, **_kwargs: object) -> None:
        return None


class DummyStore:
    def __init__(self) -> None:
        self.redis = DummyRedis()
        self.cfg = SimpleNamespace(namespace="ai_one")


class DummyStateManager:
    def __init__(self, snapshot: dict[str, object] | None) -> None:
        self._snapshot = snapshot
        self.state: dict[str, dict[str, object]] = {}
        self.saved: dict[tuple[str, str], dict[str, object]] = {}

    def get_field(self, _symbol: str, path: str) -> dict[str, object] | None:
        assert path == "stats.whale_last"
        return self._snapshot

    def set_field(self, symbol: str, path: str, value: dict[str, object]) -> None:
        self.saved[(symbol, path)] = value


def _base_normalized() -> dict[str, object]:
    return {
        "stats": {
            "atr": 1.0,
            "current_price": 100.0,
            K_PRICE_SLOPE_ATR: 0.0,
        }
    }


@pytest.mark.asyncio
async def test_carry_forward_within_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    ttl_s = float(WHALE_CARRY_FORWARD_TTL_S)
    now_ts = 1_000.0
    last_ts_s = now_ts - (ttl_s - 5.0)
    last_snapshot = {
        "version": "v2",
        "ts": int(last_ts_s * 1000),
        "ts_s": last_ts_s,
        "age_ms": 0,
        "age_s": 0.0,
        "presence": 0.42,
        "bias": 0.25,
        "vwap_dev": 0.015,
        "missing": False,
        "stale": False,
        "dev_level": "moderate",
        "zones_summary": {"accum_cnt": 3, "dist_cnt": 1},
        "vol_regime": "normal",
    }

    monkeypatch.setattr(
        "process_asset_batch.pipeline.whale_stage2.time.time",
        lambda: now_ts,
    )

    normalized = _base_normalized()
    state_manager = DummyStateManager(last_snapshot)
    store = DummyStore()
    logger = logging.getLogger("test.carry_forward.pass")

    await run_whale_stage2(
        symbol="BTCUSDT",
        lower_symbol="btcusdt",
        normalized=normalized,  # type: ignore[arg-type]
        store=store,  # type: ignore[arg-type]
        state_manager=state_manager,  # type: ignore[arg-type]
        logger=logger,
    )

    whale = normalized["stats"]["whale"]  # type: ignore[index]
    assert whale["missing"] is False
    assert whale["stale"] is False
    assert pytest.approx(whale["presence"], rel=1e-6) == 0.42
    assert "carry_forward" in whale.get("reasons", [])
    # Оновлений снепшот записується назад у state_manager
    assert ("btcusdt", "stats.whale_last") in state_manager.saved


@pytest.mark.asyncio
async def test_carry_forward_skips_when_expired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ttl_s = float(WHALE_CARRY_FORWARD_TTL_S)
    now_ts = 2_000.0
    last_ts_s = now_ts - (ttl_s + 5.0)
    old_snapshot = {
        "version": "v2",
        "ts": int(last_ts_s * 1000),
        "ts_s": last_ts_s,
        "age_ms": int(ttl_s + 5.0) * 1000,
        "age_s": ttl_s + 5.0,
        "presence": 0.73,
        "bias": -0.1,
        "vwap_dev": -0.02,
        "missing": False,
        "stale": False,
        "zones_summary": {"accum_cnt": 1, "dist_cnt": 4},
        "vol_regime": "normal",
    }

    monkeypatch.setattr(
        "process_asset_batch.pipeline.whale_stage2.time.time",
        lambda: now_ts,
    )

    normalized = _base_normalized()
    state_manager = DummyStateManager(old_snapshot)
    store = DummyStore()
    logger = logging.getLogger("test.carry_forward.fail")

    await run_whale_stage2(
        symbol="ETHUSDT",
        lower_symbol="ethusdt",
        normalized=normalized,  # type: ignore[arg-type]
        store=store,  # type: ignore[arg-type]
        state_manager=state_manager,  # type: ignore[arg-type]
        logger=logger,
    )

    whale = normalized["stats"]["whale"]  # type: ignore[index]
    assert whale["missing"] is True
    assert whale["stale"] is True
    assert whale["presence"] == 0.0
    assert "carry_forward" not in whale.get("reasons", [])
    assert state_manager.saved == {}
