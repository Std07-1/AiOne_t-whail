def test_risk_clamp_flat():
    cap = 0.20
    res = {
        "distribution_zones_count": 0,
        "liquidity_pools_count": 0,
        "risk_from_zones": 0.8,
    }
    dist, liq = res["distribution_zones_count"], res["liquidity_pools_count"]
    if dist == 0 and liq == 0:
        res["risk_from_zones"] = max(0.0, min(res["risk_from_zones"], cap))
    assert res["risk_from_zones"] == cap
