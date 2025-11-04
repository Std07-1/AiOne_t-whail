def test_trend_up_when_momentum_and_positive_cd(monkeypatch):
    # Ensure threshold known
    import importlib

    cfg = importlib.import_module("config.config")
    monkeypatch.setattr(cfg, "SCEN_HTF_MIN", 0.05, raising=False)

    from utils.btc_regime import get_btc_regime_v2

    stats = {
        "htf_strength": 0.20,
        "phase": {"name": "momentum"},
        "cd": 1.0,
        "slope_atr": -0.1,
    }
    assert get_btc_regime_v2(stats) == "trend_up"


def test_trend_down_when_drift_trend_and_negative_slope(monkeypatch):
    import importlib

    cfg = importlib.import_module("config.config")
    monkeypatch.setattr(cfg, "SCEN_HTF_MIN", 0.05, raising=False)

    from utils.btc_regime import get_btc_regime_v2

    stats = {
        "htf_strength": 0.20,
        "phase": {"name": "drift_trend"},
        "cd": 0.0,
        "slope_atr": -0.01,
    }
    assert get_btc_regime_v2(stats) == "trend_down"


def test_flat_when_low_htf_ignores_phase():
    from utils.btc_regime import get_btc_regime_v2

    stats = {
        "htf_strength": 0.0,
        "phase": {"name": "momentum"},
        "cd": 10.0,
        "slope_atr": 0.5,
    }
    assert get_btc_regime_v2(stats) == "flat"
