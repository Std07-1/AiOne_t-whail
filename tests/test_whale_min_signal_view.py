from stage3.whale_min_signal_view import whale_min_signal_view


def test_whale_min_signal_view_with_valid_snapshot() -> None:
    stats_payload = {
        "whale": {
            "presence": 0.7,
            "bias": 0.4,
            "vwap_dev": 0.12,
            "dominance": {"buy": True, "sell": False},
            "vol_regime": "high",
            "zones_summary": {"accum_cnt": 4, "dist_cnt": 1},
            "missing": False,
            "stale": False,
            "age_s": 12,
            "reasons": ["fresh"],
        },
        "tags": ["whale_soft_stale"],
    }

    view = whale_min_signal_view(stats_payload)

    assert view["presence"] == 0.7
    assert view["bias"] == 0.4
    assert view["vwap_dev"] == 0.12
    assert view["dominance"] == {"buy": True, "sell": False}
    assert view["vol_regime"] == "high"
    assert view["zones_summary"] == {"accum_cnt": 4, "dist_cnt": 1}
    assert view["tags"] == ["whale_soft_stale"]
    assert view["reasons"] == ["fresh"]


def test_whale_min_signal_view_missing_and_stale() -> None:
    stats_payload = {
        "whale": {
            "presence": 0.2,
            "bias": -0.1,
            "vwap_dev": -0.03,
            "dominance": {"buy": False, "sell": True},
            "vol_regime": "normal",
            "zones_summary": {"accum_cnt": 1, "dist_cnt": 3},
            "missing": True,
            "stale": True,
            "age_s": 300,
        },
        "tags": ["whale_missing", "whale_stale"],
    }

    view = whale_min_signal_view(stats_payload)

    assert view["missing"] is True
    assert view["stale"] is True
    assert view["age_s"] == 300
    assert view["tags"] == ["whale_missing", "whale_stale"]


def test_whale_min_signal_view_without_whale_payload() -> None:
    stats_payload = {"tags": ["no_whale"]}

    view = whale_min_signal_view(stats_payload)

    assert view["presence"] == 0.0
    assert view["bias"] == 0.0
    assert view["vwap_dev"] == 0.0
    assert view["dominance"] == {"buy": False, "sell": False}
    assert view["vol_regime"] == "unknown"
    assert view["zones_summary"] == {"accum_cnt": 0, "dist_cnt": 0}
    assert view["tags"] == ["no_whale"]
