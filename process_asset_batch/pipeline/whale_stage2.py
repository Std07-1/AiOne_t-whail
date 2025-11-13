from __future__ import annotations

import inspect
import logging
import time
from typing import Any, cast

from rich.console import Console
from rich.logging import RichHandler

from config.flags import (
    WHALE_CARRY_FORWARD_TTL_S,
    WHALE_MISSING_STALE_MODEL_ENABLED,
    WHALE_SOFT_STALE_TTL_S,
)
from process_asset_batch.whale_embed import ensure_whale_snapshot

try:
    # не обов'язкова телеметрія; якщо модуль відсутній — тихо пропускаємо
    from metrics.whale_states import (
        inc_redis_miss as _inc_redis_miss,
    )
    from metrics.whale_states import (
        set_missing as _set_missing,
    )
    from metrics.whale_states import (
        set_stale as _set_stale,
    )
except Exception:  # pragma: no cover
    _inc_redis_miss = None  # type: ignore
    _set_missing = None  # type: ignore
    _set_stale = None  # type: ignore

try:
    from process_asset_batch.helpers import update_accum_monitor
except Exception:  # pragma: no cover

    def update_accum_monitor(*_args, **_kwargs):
        return {"accum_cnt": 0, "dist_cnt": 0, "ts": int(time.time())}


logger = logging.getLogger("process_asset_batch.pipeline.whale_stage2")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

_SNAPSHOT_TTL_S = 120.0


def _ensure_list(ctx: dict[str, Any], key: str) -> list[str]:
    value = ctx.get(key)
    if isinstance(value, list):
        return cast(list[str], value)
    new_list: list[str] = []
    ctx[key] = new_list
    return new_list


def _ensure_dict(ctx: dict[str, Any], key: str) -> dict[str, Any]:
    value = ctx.get(key)
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    new_dict: dict[str, Any] = {}
    ctx[key] = new_dict
    return new_dict


def _as_positive_float(raw: Any) -> float:
    try:
        numeric = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return numeric if numeric > 0.0 else 0.0


def _coerce_ts_seconds(raw: Any) -> float | None:
    try:
        ts_value = float(raw)
    except (TypeError, ValueError):
        return None
    if ts_value > 1e11:
        ts_value /= 1000.0
    if ts_value < 0.0:
        return 0.0
    return ts_value


def _snapshot_age_seconds(
    snapshot: dict[str, Any], reference_ts: float
) -> float | None:
    ts_candidate = snapshot.get("ts_s", snapshot.get("ts"))
    ts_seconds = _coerce_ts_seconds(ts_candidate)
    if ts_seconds is None:
        return None
    return max(0.0, reference_ts - ts_seconds)


def _to_seconds(ts_any: Any) -> float:
    """Нормалізує ts (секунди або мілісекунди) до секунд."""
    try:
        ts = float(ts_any or 0.0)
    except Exception:
        return 0.0
    if ts <= 0:
        return 0.0
    # якщо виглядає як мілісекунди — перевести в секунди
    return ts / 1000.0 if ts > 10_000_000_000 else ts


def _clip(v: Any, lo: float, hi: float, default: float) -> float:
    try:
        x = float(v)
    except Exception:
        return default
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


async def run_whale_stage2(
    symbol: str,
    ctx: dict[str, Any],
    state_manager: Any,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """
    Stage2: читає whale-пейлоад із Redis, нормалізує snapshot,
    застосовує carry-forward, формує zones/dev_level/vol_regime,
    вшиває в normalized.stats.whale і повертає embedding.
    Логування фіксує хіт Redis і фінальні прапори missing/stale для діагностики.
    """
    lower_symbol = symbol.lower()
    now_ts = float(now_ts if isinstance(now_ts, (int, float)) else time.time())

    # --- 1) Читання Redis (sync/async-safe) ---
    redis = (ctx or {}).get("redis")
    redis_payload: dict[str, Any] | None = None
    try:
        if hasattr(redis, "get_whale"):
            maybe = redis.get_whale(lower_symbol, "1m")
            redis_payload = await maybe if inspect.isawaitable(maybe) else maybe
    except Exception:
        redis_payload = None

    if logger.isEnabledFor(logging.DEBUG):
        raw_ts = (redis_payload or {}).get("ts")
        logger.debug(
            "[STRICT_WHALE] Stage2 fetch symbol=%s redis=%s raw_ts=%s",
            lower_symbol,
            "hit" if redis_payload is not None else "miss",
            raw_ts,
        )

    if redis_payload is None and callable(_inc_redis_miss):
        try:
            _inc_redis_miss(lower_symbol)  # type: ignore[misc]
        except Exception:
            pass

    # --- 2) Нормалізація snapshot + TTL ---
    snap = ensure_whale_snapshot(redis_payload, now_ts, ttl_s=120)

    # Якщо модель missing/stale вимкнена прапором — форсувати обидва прапори у False
    if not bool(WHALE_MISSING_STALE_MODEL_ENABLED):
        snap["missing"] = False
        snap["stale"] = False

    # --- 3) Carry-forward (одиниці часу узгоджені у СЕКУНДАХ) ---
    if snap.get("missing", False):
        try:
            last = state_manager.get_field(lower_symbol, "stats.whale_last")
        except Exception:
            last = None
        if isinstance(last, dict):
            last_ts_s = _to_seconds(last.get("ts"))
            if last_ts_s > 0.0 and (now_ts - last_ts_s) <= float(
                WHALE_CARRY_FORWARD_TTL_S
            ):
                snap = {
                    **last,
                    "missing": False,
                    "stale": False,
                }
                rs = last.get("reasons")
                rs_list = rs if isinstance(rs, list) else []
                snap["reasons"] = list({*map(str, rs_list), "carry_forward"})

    # --- 4) Гейджі missing/stale ---
    if callable(_set_missing):
        try:
            _set_missing(lower_symbol, 1 if snap.get("missing", False) else 0)  # type: ignore[misc]
        except Exception:
            pass
    if callable(_set_stale):
        try:
            _set_stale(lower_symbol, 1 if snap.get("stale", False) else 0)  # type: ignore[misc]
        except Exception:
            pass

    # --- 5) Час/вік ---
    ts_s = _to_seconds(snap.get("ts"))
    ts_ms = int(ts_s * 1000) if ts_s > 0 else 0
    age_s = int(now_ts - ts_s) if ts_s > 0 else 0
    age_ms = age_s * 1000

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "[STRICT_WHALE] Stage2 snapshot symbol=%s missing=%s stale=%s age_ms=%d reasons=%s",
            lower_symbol,
            snap.get("missing", False),
            snap.get("stale", False),
            age_ms,
            snap.get("reasons"),
        )

    # --- 6) Zones summary ---
    zones_summary = {"accum_cnt": 0, "dist_cnt": 0}
    try:
        zones = redis_payload.get("zones") if isinstance(redis_payload, dict) else None
        if isinstance(zones, dict):
            accum_raw = zones.get("accum")
            dist_raw = zones.get("dist")
            zones_summary["accum_cnt"] = (
                int(accum_raw) if isinstance(accum_raw, (int, float)) else 0
            )
            zones_summary["dist_cnt"] = (
                int(dist_raw) if isinstance(dist_raw, (int, float)) else 0
            )
        else:
            prev = state_manager.get_field(lower_symbol, "stats.whale_last")
            prev_z = prev.get("zones_summary") if isinstance(prev, dict) else None
            if isinstance(prev_z, dict):
                zones_summary["accum_cnt"] = int(prev_z.get("accum_cnt", 0))
                zones_summary["dist_cnt"] = int(prev_z.get("dist_cnt", 0))
    except Exception:
        pass

    # --- 7) dev_level ---
    dev_level = None
    try:
        explain = (
            redis_payload.get("explain") if isinstance(redis_payload, dict) else None
        )
        if isinstance(explain, dict):
            dev_level = explain.get("dev_level")
        if dev_level is None:
            prev = state_manager.get_field(lower_symbol, "stats.whale_last")
            if isinstance(prev, dict):
                dev_level = prev.get("dev_level")
    except Exception:
        dev_level = None

    # --- 8) vol_regime з normalized.stats ---
    normalized = ctx.setdefault("normalized", {})
    stats = normalized.setdefault("stats", {})
    try:
        atr_val = float(stats.get("atr"))
    except Exception:
        atr_val = None
    try:
        cp_val = float(stats.get("current_price"))
    except Exception:
        cp_val = None
    atr_pct_local = (
        (atr_val / cp_val * 100.0) if (atr_val and cp_val and cp_val > 0) else None
    )
    if isinstance(atr_pct_local, (int, float)):
        if atr_pct_local >= 3.5:
            vol_regime = "hyper"
        elif atr_pct_local >= 2.0:
            vol_regime = "high"
        else:
            vol_regime = "normal"
    else:
        vol_regime = "unknown"

    # --- 9) Числові поля (додатковий захист) ---
    presence_f = _clip(snap.get("presence"), 0.0, 1.0, 0.0)
    bias_f = _clip(snap.get("bias"), -1.0, 1.0, 0.0)
    vdev_f = _clip(snap.get("vwap_dev"), -1e9, 1e9, 0.0)

    # --- 10) Dominance: з payload або легкий fallback ---
    dominance = None
    try:
        dom_payload = (
            redis_payload.get("dominance") if isinstance(redis_payload, dict) else None
        )
        if (
            isinstance(dom_payload, dict)
            and isinstance(dom_payload.get("buy"), bool)
            and isinstance(dom_payload.get("sell"), bool)
        ):
            dominance = {
                "buy": bool(dom_payload["buy"]),
                "sell": bool(dom_payload["sell"]),
            }
        else:
            try:
                slope_atr = float(stats.get("price_slope_atr") or 0.0)
            except Exception:
                slope_atr = 0.0
            vdev_abs_min = 0.01
            slope_abs_min = 0.5
            z_accum_min = 3
            z_dist_min = 3
            dom_buy = bool(
                (vdev_f >= vdev_abs_min)
                and (abs(slope_atr) >= slope_abs_min)
                and (zones_summary["accum_cnt"] >= z_accum_min)
            )
            dom_sell = bool(
                ((-vdev_f) >= vdev_abs_min)
                and (abs(slope_atr) >= slope_abs_min)
                and (zones_summary["dist_cnt"] >= z_dist_min)
            )
            dominance = {"buy": dom_buy, "sell": dom_sell}
    except Exception:
        dominance = None

    # --- 11) Embed у stats.whale ---
    whale_embedded: dict[str, Any] = {
        "version": str((redis_payload or {}).get("version", "v2")),
        "ts": int(ts_ms),
        "ts_s": int(ts_s) if ts_s else 0,
        "age_ms": int(age_ms),
        "age_s": int(age_s),
        "presence": presence_f,
        "bias": bias_f,
        "vwap_dev": vdev_f,
        "missing": bool(snap.get("missing", False)),
        "stale": bool(snap.get("stale", False)),
        "dev_level": dev_level,
        "zones_summary": zones_summary,
        "vol_regime": vol_regime,
    }
    if isinstance(dominance, dict):
        whale_embedded["dominance"] = {
            "buy": bool(dominance.get("buy")),
            "sell": bool(dominance.get("sell")),
        }
    if isinstance(snap.get("reasons"), list) and snap["reasons"]:
        whale_embedded["reasons"] = [str(r) for r in snap["reasons"]]

    stats["whale"] = whale_embedded

    # --- 12) Постпроцесори + теги ---
    try:
        from process_asset_batch.helpers import (
            update_accum_monitor,  # local import safe
        )

        accum_payload = update_accum_monitor(
            lower_symbol,
            accum_cnt=zones_summary["accum_cnt"],
            dist_cnt=zones_summary["dist_cnt"],
        )
        stats_post = stats.setdefault("postprocessors", {})
        stats_post["accum_monitor"] = accum_payload
        mc_meta = normalized.setdefault("market_context", {}).setdefault("meta", {})
        mc_meta.setdefault("postprocessors", {})["accum_monitor"] = accum_payload
    except Exception:
        pass

    try:
        tags = stats.get("tags")
        if not isinstance(tags, list):
            tags = []
        if whale_embedded.get("stale") and "whale_stale" not in tags:
            tags.append("whale_stale")
        if whale_embedded.get("missing") and "whale_missing" not in tags:
            tags.append("whale_missing")
        if tags:
            stats["tags"] = tags
    except Exception:
        pass

    # --- 13) Зберегти свіжий снепшот у стейті для майбутнього carry-forward ---
    try:
        if not whale_embedded.get("missing") and not whale_embedded.get("stale"):
            state_manager.set_field(
                lower_symbol, "stats.whale_last", dict(whale_embedded)
            )
    except Exception:
        try:
            asset_entry = state_manager.state.setdefault(lower_symbol, {})
            stats_entry = asset_entry.setdefault("stats", {})
            stats_entry["whale_last"] = dict(whale_embedded)
        except Exception:
            pass

    # --- 14) Soft-stale тег для логів (без зміни блокувальної логіки) ---
    try:
        if whale_embedded.get("stale") and ts_s > 0:
            soft_age = now_ts - ts_s
            if soft_age <= float(WHALE_SOFT_STALE_TTL_S):
                stats.setdefault("tags", [])
                if "whale_soft_stale" not in stats["tags"]:
                    stats["tags"].append("whale_soft_stale")
    except Exception:
        pass

    return whale_embedded
