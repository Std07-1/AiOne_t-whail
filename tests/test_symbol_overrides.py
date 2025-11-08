from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def test_apply_symbol_overrides_deltas_and_override(monkeypatch):
    mod = importlib.import_module("config.config_whale_profiles")
    # Базові пороги
    base = {
        "presence_min": 0.70,
        "bias_abs_min": 0.55,
        "vwap_dev_min": 0.010,
        "alt_confirm_min": 2,
    }
    # Налаштовуємо оверрайди для символу/профілю
    monkeypatch.setattr(
        mod,
        "SYMBOL_OVERRIDES",
        {
            "TESTUSDT": {
                "probe_up": {
                    "presence_min_delta": -0.05,
                    "vwap_dev_min_delta": -0.002,
                    "alt_confirm_min_override": 1,
                }
            }
        },
        raising=False,
    )
    # Застосування
    out, applied = mod.apply_symbol_overrides(base, "TESTUSDT", "probe_up")
    assert applied is True
    # Перевіряємо очікувані зміни
    assert out["presence_min"] == pytest.approx(0.65)
    assert out["vwap_dev_min"] == pytest.approx(0.008)
    assert out["alt_confirm_min"] == 1  # override має пріоритет
    # Інші поля не змінюються
    assert out["bias_abs_min"] == base["bias_abs_min"]


def test_ton_canary_override_applied():
    mod = importlib.import_module("config.config_whale_profiles")
    base = dict(mod.get_profile_thresholds("ALTS", "chop_pre_breakout_up"))
    assert int(base.get("alt_confirm_min", 0)) == 3
    out, applied = mod.apply_symbol_overrides(base, "TONUSDT", "chop_pre_breakout_up")
    assert applied is True
    assert int(out.get("alt_confirm_min", 0)) == 2
    assert out.get("presence_min") < base.get("presence_min")
