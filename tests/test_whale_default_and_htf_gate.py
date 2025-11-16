from __future__ import annotations

import pytest

import utils.phase_adapter as pa


def test_phase_adapter_htf_gray_penalty(monkeypatch):
    # Prepare fake detector result with high score
    class DummyRes:
        def __init__(self) -> None:
            self.phase = "momentum"
            self.score = 0.9
            self.reasons = ["dummy"]
            self.volz_source = "real"
            self.overlay = {}
            self.profile = "strict"
            self.threshold_tag = "t"
            self.strategy_hint = None

    monkeypatch.setattr(pa, "_detect_phase", lambda *_a, **_k: DummyRes())

    # Craft stats with htf_strength in gray band (between gray_low and ok)
    from config.config import HTF_GATES

    gray_low = float(HTF_GATES.get("gray_low", 0.10))
    ok_thr = float(HTF_GATES.get("ok", 0.20))
    penalty = float(HTF_GATES.get("gray_penalty", 0.20))
    mid = (gray_low + ok_thr) / 2.0

    stats = {"volume_z": 1.0, "htf_strength": mid}

    out = pa.detect_phase_from_stats(stats, symbol="ZZZUSDT")
    assert out is not None
    # Score should be penalized by gray_penalty (floored at 0)
    assert abs(out["score"] - max(0.0, 0.9 - penalty)) < 1e-6


def test_phase_adapter_exposes_diagnostics(monkeypatch):
    class DummyRes:
        def __init__(self) -> None:
            self.phase = None
            self.score = 0.0
            self.reasons = ["htf_gate"]
            self.volz_source = "real"
            self.overlay = {}
            self.profile = "strict"
            self.threshold_tag = "t"
            self.strategy_hint = None
            self.debug = {
                "missing_fields": 4,
                "reason_codes": ["htf_gate"],
                "guard_notes": ["low_atr_guard"],
                "reject_stage": "htf_gate",
                "low_atr_override_active": False,
            }

    monkeypatch.setattr(pa, "_detect_phase", lambda *_a, **_k: DummyRes())
    monkeypatch.setattr(pa, "STRICT_HTF_GRAY_GATE_ENABLED", True)

    from config.config import HTF_GATES

    gray_low = float(HTF_GATES.get("gray_low", 0.10))
    stats = {"volume_z": 1.0, "htf_strength": gray_low / 2.0}

    out = pa.detect_phase_from_stats(stats, symbol="XYZUSDT")
    assert out is not None
    diag = out.get("diagnostics")
    assert isinstance(diag, dict)
    phase_diag = diag.get("phase_detector")
    assert isinstance(phase_diag, dict)
    assert phase_diag.get("missing_fields") == 4
    assert phase_diag.get("reject_stage") == "htf_gate"
    htf_diag = diag.get("htf")
    assert isinstance(htf_diag, dict)
    assert htf_diag.get("gate_reason") == "gray_low"
    assert htf_diag.get("ok") is False


def test_phase_adapter_emits_phase_state_hint(monkeypatch):
    class DummyRes:
        def __init__(self) -> None:
            self.phase = "momentum"
            self.score = 0.5
            self.reasons = ["presence_cap_no_bias_htf"]
            self.volz_source = "real"
            self.overlay = {}
            self.profile = "strict"
            self.threshold_tag = "tag"
            self.strategy_hint = None

    def _fake_detect_phase(stats: dict[str, object], *_args, **_kwargs):
        stats["phase_state"] = {
            "current_phase": "momentum",
            "phase_score": 0.5,
            "age_s": 12.5,
            "last_reason": "presence_cap_no_bias_htf",
            "last_whale_presence": 0.42,
            "last_whale_bias": 0.31,
            "last_htf_strength": 0.28,
            "updated_ts": 1234567890,
        }
        return DummyRes()

    monkeypatch.setattr(pa, "_detect_phase", _fake_detect_phase)

    stats = {"volume_z": 1.0}
    out = pa.detect_phase_from_stats(stats, symbol="HINTUSDT")

    hint = out.get("phase_state_hint")
    assert isinstance(hint, dict)
    assert hint["phase"] == "momentum"
    assert hint["reason"] == "presence_cap_no_bias_htf"
    assert hint["age_s"] == pytest.approx(12.5)
    assert hint["presence"] == pytest.approx(0.42)
    assert hint["bias"] == pytest.approx(0.31)
    assert hint["htf_strength"] == pytest.approx(0.28)
    assert hint["updated_ts"] == pytest.approx(1234567890)
