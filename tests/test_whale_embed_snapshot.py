from __future__ import annotations

from process_asset_batch.whale_embed import ensure_whale_snapshot


def test_ensure_whale_snapshot_handles_missing_payload() -> None:
    """Перевіряє, що None повертає повністю дефолтний снепшот."""

    snap = ensure_whale_snapshot(None, now_ts=100.0, ttl_s=60)

    assert snap == {
        "presence": 0.0,
        "bias": 0.0,
        "vwap_dev": 0.0,
        "ts": 0,
        "missing": True,
        "stale": True,
    }


def test_ensure_whale_snapshot_normalizes_fresh_payload() -> None:
    """Перевіряє санітизацію чисел та правильний розрахунок stale."""

    payload = {
        "presence": 1.2,
        "bias": -1.5,
        "vwap_dev": "0.4",
        "ts": 70,
    }

    snap = ensure_whale_snapshot(payload, now_ts=100.0, ttl_s=60)

    assert snap["presence"] == 1.0
    assert snap["bias"] == -1.0
    assert snap["vwap_dev"] == 0.4
    assert snap["ts"] == 70
    assert snap["missing"] is False
    assert snap["stale"] is False
