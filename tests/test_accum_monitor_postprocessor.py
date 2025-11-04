def test_accum_monitor_fallback_no_override():
    atr_ratio = 0.74
    phase = None
    low_atr_override_active = False
    reasons = []
    if phase is None and atr_ratio < 1.0 and not low_atr_override_active:
        phase, score = "ACCUM_MONITOR", 0.0
        reasons.append("low_atr_guard")
    assert phase == "ACCUM_MONITOR" and "low_atr_guard" in reasons and score == 0.0
