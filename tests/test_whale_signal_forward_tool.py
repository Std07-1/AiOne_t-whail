from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from tools.whale_signal_forward import collect_forward_slices, render_markdown


def _event(**kwargs: object) -> str:
    base = {
        "ts": "2025-11-12T00:00:00Z",
        "event": "whale_signal_v1",
        "symbol": "BTCUSDT",
        "enabled": True,
        "profile": "strong",
        "direction": "long",
        "confidence": 0.9,
        "presence": 0.65,
        "bias": 0.2,
        "vwap_dev": 0.01,
        "vol_regime": "normal",
        "age_s": 15,
        "missing": False,
        "stale": False,
        "dominance": {"buy": True, "sell": False},
        "zones_summary": {"accum_cnt": 4, "dist_cnt": 1},
        "reasons": ["profile:strong"],
    }
    base.update(kwargs)
    return json.dumps(base)


def test_collect_forward_slices_creates_outputs(tmp_path: Path) -> None:
    events_path = tmp_path / "stage1_events.jsonl"
    events_path.write_text(
        "\n".join(
            [
                _event(symbol="BTCUSDT", enabled=True, confidence=0.85),
                _event(symbol="ETHUSDT", enabled=False, profile="soft"),
                json.dumps({"event": "phase_detected"}),
            ]
        ),
        encoding="utf-8",
    )

    args = SimpleNamespace(
        events=str(events_path),
        out_dir=str(tmp_path / "out"),
        since=None,
        until=None,
        symbols=None,
        max_sample_rows=10,
        json=None,
        markdown=None,
    )

    summary = collect_forward_slices(args)

    assert summary.total_events == 2
    assert summary.enabled_events == 1
    assert summary.profile_counts["strong"] == 1
    assert summary.profile_counts["soft"] == 1
    assert summary.symbols["BTCUSDT"]["enabled"] == 1
    assert summary.symbols["ETHUSDT"]["enabled"] == 0

    on_path = Path(args.out_dir) / "whale_forward_on.csv"
    off_path = Path(args.out_dir) / "whale_forward_off.csv"
    assert on_path.exists()
    assert off_path.exists()

    md = render_markdown(summary)
    assert "Total events" in md
    assert "BTCUSDT" in md


def test_collect_forward_slices_symbol_filter(tmp_path: Path) -> None:
    events_path = tmp_path / "stage1_events.jsonl"
    events_path.write_text(
        "\n".join(
            [
                _event(symbol="BTCUSDT", enabled=True),
                _event(symbol="ETHUSDT", enabled=True),
            ]
        ),
        encoding="utf-8",
    )
    args = SimpleNamespace(
        events=str(events_path),
        out_dir=str(tmp_path / "out"),
        since=None,
        until=None,
        symbols="BTCUSDT",
        max_sample_rows=10,
        json=None,
        markdown=None,
    )

    summary = collect_forward_slices(args)
    assert summary.total_events == 1
    assert "ETHUSDT" not in summary.symbols
