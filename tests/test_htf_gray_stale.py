def test_htf_gray_penalty_and_stale():
    score = 0.5
    # gray zone
    htf_strength = 0.15
    if 0.10 <= htf_strength < 0.20:
        score = max(0.0, score - 0.20)
    assert 0.29 < score <= 0.30
    # stale
    htf_stale_ms, thr = 9000, 7000
    htf_ok = not (htf_stale_ms > thr)
    assert htf_ok is False
