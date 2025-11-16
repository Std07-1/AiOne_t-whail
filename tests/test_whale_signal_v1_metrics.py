from __future__ import annotations

from types import SimpleNamespace

import metrics.whale_signal_v1_metrics as mod


class _FakeCollector:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def labels(self, **labels):  # type: ignore[return-value]
        def _capture(method: str):
            def _inner(*args, **kwargs):
                self.calls.append((method, args, labels))
                return None

            return _inner

        return SimpleNamespace(set=_capture("set"), inc=_capture("inc"))


def test_record_signal_updates_all_metrics(monkeypatch) -> None:
    confidence = _FakeCollector()
    enabled = _FakeCollector()
    profile = _FakeCollector()
    direction = _FakeCollector()
    disabled = _FakeCollector()

    monkeypatch.setattr(mod, "_CONFIDENCE", confidence)
    monkeypatch.setattr(mod, "_ENABLED", enabled)
    monkeypatch.setattr(mod, "_PROFILE", profile)
    monkeypatch.setattr(mod, "_DIRECTION", direction)
    monkeypatch.setattr(mod, "_DISABLED", disabled)

    mod.record_signal(
        "btcusdt",
        {
            "confidence": 0.72,
            "enabled": False,
            "profile": "strong",
            "direction": "long",
            "reasons": ["dominance_missing", "age_exceeded"],
        },
    )

    assert ("set", (0.72,), {"symbol": "BTCUSDT"}) in confidence.calls
    assert ("set", (0.0,), {"symbol": "BTCUSDT"}) in enabled.calls
    assert ("inc", tuple(), {"symbol": "BTCUSDT", "profile": "strong"}) in profile.calls
    assert (
        "inc",
        tuple(),
        {"symbol": "BTCUSDT", "direction": "long"},
    ) in direction.calls
    assert len(disabled.calls) == 2
    assert disabled.calls[0][2]["reason"] == "dominance_missing"
    assert disabled.calls[1][2]["reason"] == "age_exceeded"


def test_record_signal_no_prometheus(monkeypatch) -> None:
    monkeypatch.setattr(mod, "_CONFIDENCE", None)
    monkeypatch.setattr(mod, "_ENABLED", None)
    mod.record_signal("btcusdt", {"confidence": 1})


def test_record_signal_skips_nonlist_reasons(monkeypatch) -> None:
    confidence = _FakeCollector()
    enabled = _FakeCollector()
    disabled = _FakeCollector()

    monkeypatch.setattr(mod, "_CONFIDENCE", confidence)
    monkeypatch.setattr(mod, "_ENABLED", enabled)
    monkeypatch.setattr(mod, "_PROFILE", None)
    monkeypatch.setattr(mod, "_DIRECTION", None)
    monkeypatch.setattr(mod, "_DISABLED", disabled)

    mod.record_signal(
        "ethusdt", {"confidence": "0.9", "enabled": False, "reasons": "oops"}
    )
    assert not disabled.calls
