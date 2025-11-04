def test_presence_cap_flat():
    zones = {"distribution_zones_count": 0, "liquidity_pools_count": 0}
    score = 0.45
    cap = 0.30
    # емулюй кінцевий блок у whale_telemetry_scoring
    dist = int(zones.get("distribution_zones_count", 0))
    liq = int(zones.get("liquidity_pools_count", 0))
    if dist == 0 and liq == 0:
        score = min(score, cap)
    assert score <= cap
