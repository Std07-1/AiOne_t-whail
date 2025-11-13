from tools.quality_snapshot import _parse_min_signal_metrics


def test_parse_min_signal_metrics_aggregates() -> None:
    metrics_text = (
        'ai_one_min_signal_candidates_total{symbol="BTCUSDT",side="long"} 3\n'
        'ai_one_min_signal_candidates_total{symbol="BTCUSDT",side="short"} 1\n'
        'ai_one_min_signal_open_total{symbol="BTCUSDT",side="long"} 2\n'
        'ai_one_min_signal_exit_total{symbol="BTCUSDT",side="long",reason="time_exit"} 1\n'
        'ai_one_min_signal_exit_total{symbol="BTCUSDT",side="long",reason="presence_drop"} 2\n'
    )

    stats = _parse_min_signal_metrics(metrics_text)
    btc = stats.get("BTCUSDT")
    assert btc is not None
    assert btc["candidates"] == 4
    assert btc["open"] == 2
    assert btc["exit_total"] == 3
    assert btc["exit_breakdown"]["time_exit"] == 1
    assert btc["exit_breakdown"]["presence_drop"] == 2
