from pathlib import Path

import pandas as pd
from whale.institutional_entry_methods import InstitutionalEntryMethods
from whale.supply_demand_zones import SupplyDemandZones
from whale.whale_telemetry_scoring import WhaleTelemetryScoring

from utils.phase_adapter import detect_phase_from_stats

# Path to datastore with 1m bars snapshots
DATASTORE = Path(__file__).resolve().parents[1] / "datastore"


# Загрузити df із datastore для тестів
def _load_df(symbol: str, interval: str = "1m") -> pd.DataFrame:
    fname = f"{symbol.lower()}_bars_{interval}_snapshot.jsonl"
    path = DATASTORE / fname
    df = pd.read_json(path, lines=True)
    if "open_time" in df.columns and "timestamp" not in df.columns:
        df = df.rename(columns={"open_time": "timestamp"})
    return df


# Тест базової роботи телеметрії whale та адаптера фаз
def test_whale_fallback_and_phase_basic():
    # Choose a canary present in datastore
    symbol = "btcusdt"
    df = _load_df(symbol)
    assert not df.empty and len(df) >= 50

    closes = list(df["close"].astype(float).tail(200))
    vols = list(df["volume"].astype(float).tail(200))
    assert len(closes) == len(vols) and len(closes) > 10

    iem = InstitutionalEntryMethods()
    sdz = SupplyDemandZones()
    strats = iem.detect_vwap_twap_strategies(closes, vols)
    zones = sdz.identify_institutional_levels(
        {"prices": closes, "volumes": vols, "symbol": symbol.upper()}
    )
    presence = WhaleTelemetryScoring.presence_score_from(strats, zones)

    vdev = strats.get("vwap_deviation")
    bias = 0.0
    if isinstance(vdev, (int, float)):
        mag = min(1.0, abs(float(vdev)) / 0.02)
        bias = mag if float(vdev) > 0 else -mag

    assert isinstance(presence, (int, float))
    assert -1.0 <= float(bias) <= 1.0

    # Build minimal stats for phase adapter
    current_price = float(df["close"].iloc[-1])
    atr_proxy = max(1e-8, float((df["high"].tail(14) - df["low"].tail(14)).mean()))

    stats = {
        "current_price": current_price,
        "atr": atr_proxy,
        "volume_z": 1.0,
        "whale": {
            "presence": float(presence),
            "bias": float(bias),
            "zones_summary": {
                "accum_cnt": len(
                    (zones or {}).get("key_levels", {}).get("accumulation_zones", [])
                ),
                "dist_cnt": len(
                    (zones or {}).get("key_levels", {}).get("distribution_zones", [])
                ),
            },
        },
    }

    phase = detect_phase_from_stats(stats, symbol=symbol, low_gate_effective=None)
    assert isinstance(phase, dict)
    assert "name" in phase and "score" in phase
