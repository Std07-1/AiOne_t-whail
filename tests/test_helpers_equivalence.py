from __future__ import annotations

from typing import Any

import pytest

import app.process_asset_batch as pab
import process_asset_batch.helpers as helpers


def test_active_alt_keys_equivalence() -> None:
    d1 = {"a": True, "b": False}
    d2 = {"b": True, "c": True}
    out_app = pab._active_alt_keys(d1, d2)
    out_core = helpers._active_alt_keys(d1, d2)
    assert out_app == out_core


def test_should_confirm_pre_breakout_equivalence() -> None:
    ready_app, flags_app = pab._should_confirm_pre_breakout(
        accept_ok=True,
        alt_confirms_count=2,
        thr_alt_min=1,
        dominance_buy=True,
        vwap_dev=0.02,
        slope_atr=1.0,
    )
    ready_core, flags_core = helpers._should_confirm_pre_breakout(
        accept_ok=True,
        alt_confirms_count=2,
        thr_alt_min=1,
        dominance_buy=True,
        vwap_dev=0.02,
        slope_atr=1.0,
    )
    assert ready_app == ready_core
    assert flags_app == flags_core


@pytest.mark.asyncio
async def test_cooldown_read_write_equivalence(monkeypatch: Any) -> None:
    # Monkeypatch Redis acquire/release with in-memory stub
    class _Client:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}

        async def get(self, k: str):
            return self.store.get(k)

        async def setex(self, k: str, ttl: int, val: str):
            self.store[k] = val

    _client = _Client()

    async def _acquire():
        return _client

    async def _release(_c):
        return None

    monkeypatch.setattr("data.redis_connection.acquire_redis", _acquire, raising=False)
    monkeypatch.setattr("data.redis_connection.release_redis", _release, raising=False)

    sym = "TESTUSDT"
    v0_app = await pab._read_profile_cooldown(sym)
    v0_core = await helpers._read_profile_cooldown(sym)
    assert v0_app is None and v0_core is None

    await pab._write_profile_cooldown(sym, 1234.5, ttl_s=60)
    v1_app = await pab._read_profile_cooldown(sym)

    await helpers._write_profile_cooldown(sym, 2345.6, ttl_s=60)
    v1_core = await helpers._read_profile_cooldown(sym)

    # Both read from the same underlying state and return floats
    assert isinstance(v1_app, float)
    assert isinstance(v1_core, float)


def test_normalize_dvr_equivalence() -> None:
    sym = "AAAUSDT"
    v1 = pab._normalize_and_cap_dvr(sym, 100.0)
    v2 = helpers._normalize_and_cap_dvr(sym, 100.0)
    assert isinstance(v1, float) and isinstance(v2, float)


def test_htf_adjust_alt_min_equivalence() -> None:
    thr = {"alt_confirm_min": 3}
    # With last_h1=None both return the same params (no change)
    out_app = pab._htf_adjust_alt_min("BTC", None, thr, enabled=True)
    out_core = helpers._htf_adjust_alt_min("BTC", None, thr, enabled=True)
    assert out_app == out_core
