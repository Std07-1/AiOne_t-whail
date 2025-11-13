from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any


def embed_whale_metrics(
    store: Any,
    symbol: str,
    normalized: dict[str, Any],
    *,
    ensure_default: Callable[[dict[str, Any]], Any] | None = None,
    get_prev_presence: Callable[[str], float | None] | None = None,
    set_prev_presence: Callable[[str, float], Any] | None = None,
) -> None:
    """Best-effort підготовка контейнера stats.whale перед Stage2."""
    stats = normalized.setdefault("stats", {})
    if callable(ensure_default):
        try:
            ensure_default(stats)
        except Exception:
            pass

    if callable(get_prev_presence) and callable(set_prev_presence):
        try:
            prev = get_prev_presence(symbol)
        except Exception:
            prev = None
        if isinstance(prev, (int, float)):
            whale = stats.setdefault("whale", {})
            whale.setdefault("presence", float(prev))
            if callable(set_prev_presence):
                try:
                    set_prev_presence(symbol, float(prev))
                except Exception:
                    pass


def _sanitize_float(value: Any, *, default: float = 0.0) -> float:
    """Повертає скінченне float-значення або запасне значення."""

    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def ensure_whale_snapshot(
    payload: dict[str, Any] | None,
    now_ts: float,
    *,
    ttl_s: int,
) -> dict[str, Any]:
    """Нормалізує телеметрію китів та додає прапорці missing/stale."""

    if payload is None:
        return {
            "presence": 0.0,
            "bias": 0.0,
            "vwap_dev": 0.0,
            "ts": 0,
            "missing": True,
            "stale": True,
        }

    presence_legacy = payload.get("presence")
    presence_score = payload.get("presence_score")
    presence_source_field = (
        "presence_score" if presence_score is not None else "presence"
    )
    presence_source = presence_score if presence_score is not None else presence_legacy
    presence = max(0.0, min(1.0, _sanitize_float(presence_source)))
    presence_legacy_val: float | None = None
    if presence_legacy is not None:
        presence_legacy_val = max(0.0, min(1.0, _sanitize_float(presence_legacy)))
    presence_score_val: float | None = None
    if presence_score is not None:
        presence_score_val = max(0.0, min(1.0, _sanitize_float(presence_score)))

    bias = max(-1.0, min(1.0, _sanitize_float(payload.get("bias"))))

    vwap_legacy = payload.get("vwap_dev")
    vwap_new = payload.get("vwap_deviation")
    vwap_source_field = "vwap_deviation" if vwap_new is not None else "vwap_dev"
    vwap_source = vwap_new if vwap_new is not None else vwap_legacy
    vwap_dev = _sanitize_float(vwap_source)
    vwap_legacy_val: float | None = None
    if vwap_legacy is not None:
        vwap_legacy_val = _sanitize_float(vwap_legacy)
    vwap_new_val: float | None = None
    if vwap_new is not None:
        vwap_new_val = _sanitize_float(vwap_new)

    ts_value = _sanitize_float(payload.get("ts"))
    if ts_value > 1e11:
        ts_value /= 1000.0
    ts_value = max(0.0, ts_value)
    ts = int(ts_value)

    age = max(0.0, now_ts - ts_value)
    stale = age > float(ttl_s)

    result = {
        "presence": presence,
        "bias": bias,
        "vwap_dev": vwap_dev,
        "ts": ts,
        "missing": False,
        "stale": stale,
    }
    if presence_legacy_val is not None:
        result["presence_legacy"] = presence_legacy_val
    if presence_score_val is not None:
        result["presence_score"] = presence_score_val
    result["presence_source"] = presence_source_field
    if vwap_legacy_val is not None:
        result["vwap_dev_legacy"] = vwap_legacy_val
    if vwap_new_val is not None:
        result["vwap_deviation"] = vwap_new_val
    result["vwap_dev_source"] = vwap_source_field

    schema = payload.get("schema")
    if isinstance(schema, str) and schema:
        result["schema"] = schema

    return result
