import pandas as pd

from tools.whale_publisher import build_whale_payload


def test_whale_payload_shape():
    # згенеруємо 200 рядків M1
    rows = []
    base_ts_ms = 1_700_000_000_000
    for i in range(200):
        price = 100 + 0.01 * i
        rows.append(
            {
                "open_time": base_ts_ms + i * 60_000,
                "open": price,
                "high": price + 0.2,
                "low": price - 0.2,
                "close": price + (0.05 if i % 10 == 0 else 0.0),
                "volume": 100 + (i % 20),
            }
        )
    df = pd.DataFrame(rows)
    payload = build_whale_payload(df)
    assert set(["presence", "bias", "vwap_dev", "stale", "ts_ms"]).issubset(
        payload.keys()
    )
    assert 0.0 <= float(payload["presence"]) <= 1.0
    assert -1.0 <= float(payload["bias"]) <= 1.0
    assert isinstance(payload["stale"], bool)
