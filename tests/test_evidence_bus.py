from __future__ import annotations

from utils import evidence_bus as ev


def test_evidence_bus_collect_and_explain() -> None:
    ev.start("BTCUSDT")
    ev.add("BTCUSDT", "presence", 0.7, 0.9, "ok")
    ev.add("BTCUSDT", "htf_strength", 0.3, 0.8, None)
    ev.add("BTCUSDT", "compression.index", 0.15, 0.5, None)

    items, explain = ev.finish("BTCUSDT")

    assert len(items) == 3
    # explain має містити ключ з найвищою вагою першою (presence)
    assert "presence=0.7" in explain
