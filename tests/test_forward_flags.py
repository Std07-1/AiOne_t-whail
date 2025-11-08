from tools import forward_from_log as fwd


def test_soft_thresholds_applied_when_no_cli():
    # Enable soft flag and set profiles
    fwd.FORWARD_SOFT_THRESH_ENABLED = True
    fwd.FORWARD_SOFT_THRESH = {
        "whale": {"presence_min": 0.5, "bias_abs_min": 0.3, "whale_max_age_sec": 1800},
        "explain": {"presence_min": 0.35, "bias_abs_min": 0.2, "explain_ttl_sec": 1200},
    }
    pres, bias, whale_age, ex_ttl, soft = fwd._resolve_thresholds(
        source="whale",
        presence_min=None,
        bias_abs_min=None,
        whale_max_age_sec=None,
        explain_ttl_sec=None,
    )
    assert soft is True
    assert pres == 0.5 and bias == 0.3 and whale_age == 1800
    # explain path
    pres2, bias2, whale_age2, ex_ttl2, soft2 = fwd._resolve_thresholds(
        source="explain",
        presence_min=None,
        bias_abs_min=None,
        whale_max_age_sec=None,
        explain_ttl_sec=None,
    )
    assert soft2 is True
    assert pres2 == 0.35 and bias2 == 0.2 and ex_ttl2 == 1200
    # whale_age should carry hard default when not applicable in explain
    assert whale_age2 == 600  # hard default retained for whale-age on explain source


def test_soft_thresholds_not_override_explicit_cli():
    fwd.FORWARD_SOFT_THRESH_ENABLED = True
    fwd.FORWARD_SOFT_THRESH = {
        "whale": {"presence_min": 0.5, "bias_abs_min": 0.3, "whale_max_age_sec": 1800}
    }
    pres, bias, whale_age, ex_ttl, soft = fwd._resolve_thresholds(
        source="whale",
        presence_min=0.72,
        bias_abs_min=0.61,
        whale_max_age_sec=900,
        explain_ttl_sec=None,
    )
    # Since presence/bias/age specified explicitly, soft should not apply
    assert soft is False
    assert pres == 0.72 and bias == 0.61 and whale_age == 900
    # explain ttl should fall back to hard default 600
    assert ex_ttl == 600
