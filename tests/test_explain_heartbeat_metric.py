from __future__ import annotations

import pytest

from telemetry import prom_gauges as pg


def test_explain_heartbeat_counter_increments(monkeypatch: pytest.MonkeyPatch) -> None:
    """Перевіряє, що інкремент heartbeat explain з індексом символу виконує виклик лічильника."""

    class StubMetric:
        def __init__(self) -> None:
            self.calls: dict[str, int] = {}

        def labels(self, *, symbol: str):
            class Adapter:
                def __init__(self, outer: StubMetric, sym: str) -> None:
                    self._outer = outer
                    self._symbol = sym

                def inc(self) -> None:
                    self._outer.calls[self._symbol] = (
                        self._outer.calls.get(self._symbol, 0) + 1
                    )

            return Adapter(self, symbol)

    stub = StubMetric()
    monkeypatch.setattr(pg, "_explain_heartbeat", stub)

    pg.inc_explain_heartbeat("BTCUSDT")

    assert stub.calls.get("BTCUSDT") == 1
