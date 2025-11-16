"""Whale telemetry core — єдине pure-ядро для Stage2/UI.

Цей модуль реалізує контракти WhaleInput → WhaleTelemetry (див. docs/whale_contract.md)
і не має побічних I/O ефектів. Він обʼєднує попередню логіку з
`process_asset_batch.pipeline.whale_stage2` та `whale.engine`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Mapping

from config.flags import WHALE_SOFT_STALE_TTL_S
from config.config_whale import STAGE2_WHALE_TELEMETRY
from utils.utils import safe_float

try:  # легкий імпорт, ядро працює й без скорингу
    from .whale_telemetry_scoring import WhaleTelemetryScoring
except Exception:  # pragma: no cover
    WhaleTelemetryScoring = None  # type: ignore[assignment]

_PRESENCE_EMA_STATE: dict[str, float] = {}


@dataclass(frozen=True)
class WhaleInput:
    """Узгоджений SoT для обчислення китової телеметрії."""

    symbol: str
    whale_snapshot: Mapping[str, Any]
    stats_slice: Mapping[str, Any]
    stage2_derived: Mapping[str, Any]
    directional: Mapping[str, Any]
    time_context: Mapping[str, Any]
    meta_overlay: Mapping[str, Any] | None = None


@dataclass
class WhaleTelemetry:
    """Результат роботи ядра."""

    stats_payload: dict[str, Any]
    meta_payload: dict[str, Any]
    overlay_payload: dict[str, Any]
    tags: list[str] = field(default_factory=list)


class WhaleCore:
    """Pure‑ядро для Stage2/UI.

    - Будує `stats.whale.*` (SoT для Stage3/політик).
    - Рахує presence/bias/dominance для `ctx.meta.whale` та `meta.insight_overlay`.
    - Єдиний власник EMA згладжування presence.
    """

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def compute(self, whale_input: WhaleInput) -> WhaleTelemetry:
        stats_payload, tag_list = self._build_stats_payload(whale_input)
        meta_payload, overlay_payload = self._build_ui_projection(
            whale_input, stats_payload
        )
        return WhaleTelemetry(
            stats_payload=stats_payload,
            meta_payload=meta_payload,
            overlay_payload=overlay_payload,
            tags=tag_list,
        )

    # --- Internal helpers -------------------------------------------------
    def _build_stats_payload(
        self, whale_input: WhaleInput
    ) -> tuple[dict[str, Any], list[str]]:
        snapshot = dict(whale_input.whale_snapshot or {})
        stage2 = dict(whale_input.stage2_derived or {})
        time_ctx = dict(whale_input.time_context or {})

        presence = _clip(snapshot.get("presence"), 0.0, 1.0, default=0.0)
        bias = _clip(snapshot.get("bias"), -1.0, 1.0, default=0.0)
        vwap_dev = _clip(snapshot.get("vwap_dev"), -1e9, 1e9, default=0.0)

        ts_ms = _coerce_int(snapshot.get("ts"))
        if ts_ms <= 0:
            ts_ms = _coerce_int(snapshot.get("ts_ms"))
        ts_s = _coerce_int(snapshot.get("ts_s"))
        if ts_s <= 0 and ts_ms > 0:
            ts_s = int(ts_ms / 1000)
        if ts_ms <= 0 and ts_s > 0:
            ts_ms = ts_s * 1000

        now_ts = safe_float(time_ctx.get("now_ts")) or 0.0
        age_s = _coerce_int(snapshot.get("age_s"))
        if age_s <= 0 and ts_s > 0 and now_ts > 0:
            age_s = max(0, int(now_ts - ts_s))
        age_ms = _coerce_int(snapshot.get("age_ms"))
        if age_ms <= 0 and age_s > 0:
            age_ms = age_s * 1000

        zones_summary = _normalize_zones(snapshot.get("zones_summary"), stage2)
        dev_level = snapshot.get("dev_level") or stage2.get("dev_level")
        vol_regime = snapshot.get("vol_regime") or stage2.get("vol_regime") or "unknown"

        dominance_payload = snapshot.get("dominance")
        dominance = self._resolve_dominance(
            dominance_payload,
            zones_summary=zones_summary,
            directional=whale_input.directional,
            fallback_vwap_dev=vwap_dev,
        )

        stats_payload: dict[str, Any] = {
            "version": str(snapshot.get("version", stage2.get("version", "v2"))),
            "ts": int(ts_ms) if ts_ms > 0 else 0,
            "ts_s": int(ts_s) if ts_s > 0 else 0,
            "age_ms": int(age_ms) if age_ms > 0 else 0,
            "age_s": int(age_s) if age_s > 0 else 0,
            "presence": presence,
            "bias": bias,
            "vwap_dev": vwap_dev,
            "missing": bool(snapshot.get("missing", False)),
            "stale": bool(snapshot.get("stale", False)),
            "dev_level": dev_level,
            "zones_summary": zones_summary,
            "vol_regime": vol_regime,
        }
        if dominance is not None:
            stats_payload["dominance"] = dominance
        reasons = snapshot.get("reasons")
        if isinstance(reasons, list) and reasons:
            stats_payload["reasons"] = [str(r) for r in reasons]

        tags: list[str] = []
        if stats_payload["stale"]:
            tags.append("whale_stale")
        if stats_payload["missing"]:
            tags.append("whale_missing")
        if stats_payload["stale"] and stats_payload["ts_s"] > 0 and now_ts > 0:
            soft_age = now_ts - stats_payload["ts_s"]
            if 0 <= soft_age <= float(WHALE_SOFT_STALE_TTL_S):
                tags.append("whale_soft_stale")

        return stats_payload, tags

    def _resolve_dominance(
        self,
        dominance_payload: Any,
        *,
        zones_summary: dict[str, int],
        directional: Mapping[str, Any],
        fallback_vwap_dev: float,
    ) -> dict[str, bool] | None:
        if (
            isinstance(dominance_payload, Mapping)
            and isinstance(dominance_payload.get("buy"), bool)
            and isinstance(dominance_payload.get("sell"), bool)
        ):
            return {
                "buy": bool(dominance_payload.get("buy")),
                "sell": bool(dominance_payload.get("sell")),
            }

        slope_atr = safe_float(directional.get("price_slope_atr")) or 0.0
        vdev_abs_min = 0.01
        slope_abs_min = 0.5
        z_accum_min = 3
        z_dist_min = 3

        dom_buy = bool(
            (fallback_vwap_dev >= vdev_abs_min)
            and (abs(slope_atr) >= slope_abs_min)
            and (zones_summary.get("accum_cnt", 0) >= z_accum_min)
        )
        dom_sell = bool(
            ((-fallback_vwap_dev) >= vdev_abs_min)
            and (abs(slope_atr) >= slope_abs_min)
            and (zones_summary.get("dist_cnt", 0) >= z_dist_min)
        )
        if not dom_buy and not dom_sell:
            return None
        return {"buy": dom_buy, "sell": dom_sell}

    def _build_ui_projection(
        self,
        whale_input: WhaleInput,
        stats_payload: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        meta_payload: dict[str, Any] = {}
        overlay_payload: dict[str, Any] = {}
        if WhaleTelemetryScoring is None:
            return meta_payload, overlay_payload

        stats_slice = dict(whale_input.stats_slice or {})
        directional = dict(whale_input.directional or {})
        vwap = safe_float(stats_slice.get("vwap"))
        price = safe_float(stats_slice.get("current_price"))
        vwap_dev = None
        if vwap and price and vwap > 0:
            vwap_dev = (price - vwap) / vwap

        strats = {
            "vwap_buying": False,
            "vwap_selling": False,
            "twap_accumulation": bool(directional.get("twap_accumulation", False)),
            "iceberg_orders": bool(directional.get("iceberg_orders", False)),
            "vwap_deviation": vwap_dev,
        }

        presence = None
        bias = None
        dominance = None
        try:
            presence = WhaleTelemetryScoring.presence_score_from(strats, zones=None)
        except Exception as exc:  # noqa: BLE001
            self._logger.debug("whale core presence failed: %s", exc)
        try:
            bias = WhaleTelemetryScoring.whale_bias_from(strats)
        except Exception as exc:  # noqa: BLE001
            self._logger.debug("whale core bias failed: %s", exc)
        try:
            dominance = WhaleTelemetryScoring.dominance_flags(
                whale_bias=bias,
                vwap_dev=vwap_dev,
                slope_atr=safe_float(directional.get("price_slope_atr")),
                dvr=safe_float(directional.get("directional_volume_ratio")),
                trap=directional.get("trap"),
                iceberg=strats.get("iceberg_orders"),
                dist_zones=None,
            )
        except Exception as exc:  # noqa: BLE001
            self._logger.debug("whale core dominance failed: %s", exc)

        if presence is not None:
            presence_val = float(presence)
            presence_raw = presence_val
            ema_cfg = dict((STAGE2_WHALE_TELEMETRY or {}).get("ema") or {})
            if bool(ema_cfg.get("enabled", False)) and whale_input.symbol:
                alpha = float(ema_cfg.get("alpha", 0.30))
                alpha = max(0.01, min(0.99, alpha))
                prev = _PRESENCE_EMA_STATE.get(whale_input.symbol)
                if isinstance(prev, (float, int)):
                    presence_val = float(
                        alpha * presence_val + (1.0 - alpha) * float(prev)
                    )
                _PRESENCE_EMA_STATE[whale_input.symbol] = presence_val
                meta_payload["presence_score_raw"] = float(presence_raw)
            meta_payload["presence_score"] = float(presence_val)
            overlay_payload["whale_presence"] = float(presence_val)

        if bias is not None:
            meta_payload["whale_bias"] = float(bias)
            overlay_payload["whale_bias"] = float(bias)

        if vwap_dev is not None:
            meta_payload["vwap_deviation"] = float(vwap_dev)

        if dominance is not None:
            meta_payload["dominance"] = dominance

        return meta_payload, overlay_payload


def _clip(value: Any, lo: float, hi: float, *, default: float) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if num < lo:
        return lo
    if num > hi:
        return hi
    return num


def _coerce_int(value: Any) -> int:
    try:
        num = int(float(value))
    except (TypeError, ValueError):
        return 0
    return num


def _normalize_zones(candidate: Any, stage2: Mapping[str, Any]) -> dict[str, int]:
    if isinstance(candidate, Mapping):
        accum = candidate.get("accum_cnt") or candidate.get("accum")
        dist = candidate.get("dist_cnt") or candidate.get("dist")
        try:
            accum_cnt = int(accum)
        except (TypeError, ValueError):
            accum_cnt = 0
        try:
            dist_cnt = int(dist)
        except (TypeError, ValueError):
            dist_cnt = 0
        return {"accum_cnt": max(0, accum_cnt), "dist_cnt": max(0, dist_cnt)}
    stage_candidate = stage2.get("zones_summary")
    if isinstance(stage_candidate, Mapping):
        return {
            "accum_cnt": int(stage_candidate.get("accum_cnt", 0)),
            "dist_cnt": int(stage_candidate.get("dist_cnt", 0)),
        }
    return {"accum_cnt": 0, "dist_cnt": 0}
