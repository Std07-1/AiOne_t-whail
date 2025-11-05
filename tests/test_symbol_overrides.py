from __future__ import annotations

import importlib
from typing import Dict, Any


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
    assert out["presence_min"] == 0.65
    assert out["vwap_dev_min"] == 0.008
    assert out["alt_confirm_min"] == 1  # override має пріоритет
    # Інші поля не змінюються
    assert out["bias_abs_min"] == base["bias_abs_min"]
