import pandas as pd

from tools.htf_publisher import _resample_ohlcv


def test_resample_basic_m5():
    # Побудуємо 10 хв M1 з лінійним зростанням і фіксованим обсягом
    rows = []
    base_ts_ms = 1_700_000_000_000
    for i in range(10):
        rows.append(
            {
                "open_time": base_ts_ms + i * 60_000,
                "open": 100 + i,
                "high": 101 + i,
                "low": 99 + i,
                "close": 100.5 + i,
                "volume": 10,
            }
        )
    df = pd.DataFrame(rows)
    df.index = pd.to_datetime(df["open_time"], unit="ms", utc=True)

    m5 = _resample_ohlcv(df, "M5")
    # 10 хв → 2 бара M5
    assert len(m5) == 2
    # Перевіримо перший бар
    first = m5.iloc[0]
    assert first["open"] == 100
    assert first["high"] == 101 + 4
    assert first["low"] == 99
    assert first["close"] == 100.5 + 4
    assert first["volume"] == 50
