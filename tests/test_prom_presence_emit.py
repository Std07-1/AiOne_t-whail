from app import process_asset_batch as pab


def test_emit_prom_presence_calls_once(monkeypatch):
    calls: list[tuple[str, float]] = []

    # Замінюємо клієнтську функцію Prometheus на заглушку
    monkeypatch.setattr(
        pab,
        "set_presence",
        lambda sym, val: calls.append((sym, float(val))),
        raising=False,
    )

    stats = {"whale": {"presence": 0.55}}

    # Виклик помічника має відбутися рівно один раз
    ok = pab._emit_prom_presence(stats, "BTCUSDT")
    assert ok is True
    assert len(calls) == 1
    assert calls[0][0] == "BTCUSDT"
    assert abs(calls[0][1] - 0.55) < 1e-9

    # Відсутність presence — повторний виклик не додає нових викликів
    ok2 = pab._emit_prom_presence({}, "BTCUSDT")
    assert ok2 is False
    assert len(calls) == 1
