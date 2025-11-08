from __future__ import annotations

import os
from typing import Optional  # noqa: F401

_ENABLED = str(os.environ.get("PROM_GAUGES_ENABLED", "false")).lower() == "true"
_HAVE_CLIENT = False
_c_profile = None  # type: ignore[var-annotated]
_c_dedup = None  # type: ignore[var-annotated]
_c_ttf05 = None  # type: ignore[var-annotated]

if _ENABLED:
    try:
        from prometheus_client import Counter

        _c_profile = Counter(
            "ai_one_forward_profile_emitted_total",
            "Forward cases emitted (unique alerts)",
            labelnames=("profile",),
        )
        _c_dedup = Counter(
            "ai_one_forward_dedup_dropped_total",
            "Forward cases dropped due to cross-profile dedup",
        )
        _c_ttf05 = Counter(
            "ai_one_forward_ttf05_bucket_total",
            "TTF 0.5% bucket counts",
            labelnames=("bucket",),
        )
        _HAVE_CLIENT = True
    except Exception:
        _HAVE_CLIENT = False


def inc_profile_emitted(profile: str, value: int = 1) -> None:
    if not (_ENABLED and _HAVE_CLIENT and _c_profile is not None):
        return
    try:
        _c_profile.labels(profile=profile).inc(max(0, int(value)))
    except Exception:
        pass


def inc_dedup_dropped(value: int = 1) -> None:
    if not (_ENABLED and _HAVE_CLIENT and _c_dedup is not None):
        return
    try:
        _c_dedup.inc(max(0, int(value)))
    except Exception:
        pass


def inc_ttf05_bucket(ttf_bars: int) -> None:
    """Інкрементувати лічильник для TTF 0.5% у відповідний бакет.

    Бакети:
    - le5  (≤5)
    - le10 (≤10)
    - le20 (≤20)
    - gt20 (>20)
    """
    if not (_ENABLED and _HAVE_CLIENT and _c_ttf05 is not None):
        return
    try:
        v = int(ttf_bars)
    except Exception:
        return
    try:
        if v <= 5:
            _c_ttf05.labels(bucket="le5").inc()
        elif v <= 10:
            _c_ttf05.labels(bucket="le10").inc()
        elif v <= 20:
            _c_ttf05.labels(bucket="le20").inc()
        else:
            _c_ttf05.labels(bucket="gt20").inc()
    except Exception:
        pass
