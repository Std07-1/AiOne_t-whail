def test_low_atr_override_ttl_decrement():
    state = {"ttl": 3, "last_ts": 0, "reason": "spike_detected"}
    for _ in range(3):
        state["ttl"] = max(0, state["ttl"] - 1)
    assert state["ttl"] == 0
