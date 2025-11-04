"""Асинхронний стрім-реплей Stage1→Stage2 для офлайн-аналізу.

Скрипт зчитує історичні свічки (локальний snapshot або Binance API),
проганяє їх через монітор Stage1 та процесор Stage2, а результат
зберігає у JSONL/CSV для подальшої аналітики.

Використовується лише у дев-режимі: побічних ефектів для бойового
пайплайна немає (жодного Redis/Stage3).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
import numpy as np
import pandas as pd
from whale.whale_telemetry_scoring import WhaleTelemetryScoring

from stage1.asset_monitoring import AssetMonitorStage1
from stage2.processor import Stage2Processor
from utils.utils import ensure_epoch_ms_columns

try:
    from config.config import STAGE2_RUNTIME  # локальне перевизначення для реплею
    from config.config import TELEMETRY_BASE_DIR
except (
    Exception
):  # pragma: no cover — у тестових/ізольованих середовищах беремо дефолти
    STAGE2_RUNTIME = {}
    TELEMETRY_BASE_DIR = "./telemetry"

try:
    # Деякі середовища можуть не мати цього ключа у config
    from config.config import STAGE1_VOLUME_SPIKE_GATES  # type: ignore
except Exception:  # pragma: no cover
    STAGE1_VOLUME_SPIKE_GATES = {}  # type: ignore[assignment]
try:  # опційні прапори/правила для паперових входів під час реплею
    from config.config import REPLAY_PAPER_TRADES_ENABLED, REPLAY_PAPER_TRADES_RULE
except Exception:  # pragma: no cover
    REPLAY_PAPER_TRADES_ENABLED = False  # type: ignore[assignment]
    REPLAY_PAPER_TRADES_RULE = {}  # type: ignore[assignment]
from importlib import import_module

from data.raw_data import OptimizedDataFetcher

try:
    _institutional_module = import_module("whale.institutional_entry_methods")
    _supply_module = import_module("whale.supply_demand_zones")
except ModuleNotFoundError:
    # Сумісність із попередньою структурою моніторингу (monitoring.whale_detection).
    _institutional_module = import_module(
        "monitoring.whale_detection.institutional_entry_methods"
    )
    _supply_module = import_module("monitoring.whale_detection.supply_demand_zones")

InstitutionalEntryMethods = _institutional_module.InstitutionalEntryMethods
SupplyDemandZones = _supply_module.SupplyDemandZones


# ── Логування ──
logger = logging.getLogger("tools.replay_stream")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

ROOT = Path(__file__).resolve().parents[1]
DATASTORE = ROOT / "datastore"
DEFAULT_WINDOW = 720
INTERVAL_TO_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


# ── In-memory cache/redis шими для Stage1 ──
class _RedisJSON:
    def __init__(self) -> None:
        self._store: dict[tuple[str, ...], Any] = {}

    async def jget(self, *parts: str, default: Any | None = None) -> Any:
        return self._store.get(tuple(parts), default)

    async def jset(
        self, *parts: str, value: Any, ttl: int | None = None
    ) -> None:  # noqa: ARG002
        self._store[tuple(parts)] = value


class InMemoryCache:
    """MVP-кеш для Stage1 (достатньо для порогів/мета).

    Підтримує лише JSON-методи; blob-методи повертають None.
    """

    def __init__(self) -> None:
        self.redis = _RedisJSON()
        self._legacy_store: dict[tuple[str, str, str], Any] = {}

    async def fetch_from_cache(
        self, symbol: str, interval: str, *, prefix: str = "candles", raw: bool = True
    ) -> Any:  # noqa: ARG002
        return self._legacy_store.get((symbol, interval, prefix))

    async def store_in_cache(
        self,
        key: str,
        scope: str,
        payload: Any,
        ttl: Any | None = None,  # noqa: ARG002
        raw: bool = True,
    ) -> None:  # noqa: ARG002
        self._legacy_store[(key, scope, "meta")] = payload

    async def delete_from_cache(
        self, symbol: str, interval: str, *, prefix: str = "candles"
    ) -> None:  # noqa: ARG002
        self._legacy_store.pop((symbol, interval, prefix), None)


class DummyStateManager:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {}


@dataclass(slots=True)
class ReplayConfig:
    symbol: str
    interval: str = "1m"
    source: str = "snapshot"  # snapshot | binance
    start_ms: int | None = None
    end_ms: int | None = None
    limit: int | None = None
    window: int = DEFAULT_WINDOW
    dump_dir: Path = Path("./replay_dump")


@dataclass(slots=True)
class ReplayStats:
    bars: int
    processed: int
    stage1_signals: int
    stage2_outputs: int
    dump_dir: Path


def _parse_ts(value: str | int | float | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, float):
        return int(value)
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.isdigit():
        return int(candidate)
    try:
        dt = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"Неможливо розпарсити timestamp: {value}") from None
    return int(dt.timestamp() * 1000)


async def _load_from_snapshot(cfg: ReplayConfig) -> pd.DataFrame:
    fname = f"{cfg.symbol.lower()}_bars_{cfg.interval}_snapshot.jsonl"
    path = DATASTORE / fname
    if not path.exists():
        raise FileNotFoundError(f"Snapshot не знайдено: {path}")

    df = pd.read_json(path, lines=True)
    if df.empty:
        raise ValueError(f"Snapshot {path} порожній")

    if "open_time" not in df.columns:
        df.rename(columns={"timestamp": "open_time"}, inplace=True)
    df = ensure_epoch_ms_columns(df, columns=("open_time", "close_time"))
    df["timestamp"] = df.get("timestamp", df["open_time"])

    # Фільтрація start/end/limit
    if cfg.start_ms is not None:
        df = df[df["open_time"] >= cfg.start_ms]
    if cfg.end_ms is not None:
        df = df[df["open_time"] <= cfg.end_ms]
    df = df.sort_values("open_time").reset_index(drop=True)
    if cfg.limit is not None and cfg.limit > 0:
        df = df.tail(cfg.limit).reset_index(drop=True)

    return df


async def _load_from_binance(cfg: ReplayConfig) -> pd.DataFrame:
    interval_ms = INTERVAL_TO_MS.get(cfg.interval)
    if interval_ms is None:
        raise ValueError(f"Непідтримуваний інтервал {cfg.interval}")

    end_ms = cfg.end_ms
    if end_ms is None:
        end_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    start_ms = cfg.start_ms

    expected = 0
    if start_ms is not None:
        expected = max(0, int(math.ceil((end_ms - start_ms) / interval_ms)) + 5)
    limit = cfg.limit or cfg.window or DEFAULT_WINDOW
    limit = max(limit, expected or limit)

    async with aiohttp.ClientSession() as session:
        fetcher = OptimizedDataFetcher(session)
        df = await fetcher.get_data(
            cfg.symbol,
            cfg.interval,
            limit=limit,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
        )

    if df.empty:
        raise ValueError("Binance повернув порожній набір даних")

    df = df.sort_values("timestamp").reset_index(drop=True)
    if start_ms is not None:
        df = df[df["timestamp"] >= start_ms]
    if cfg.end_ms is not None:
        df = df[df["timestamp"] <= cfg.end_ms]
    if cfg.limit is not None and cfg.limit > 0:
        df = df.tail(cfg.limit).reset_index(drop=True)

    df = ensure_epoch_ms_columns(
        df,
        columns=("timestamp",),
        drop_out_of_range=False,
    )
    df.rename(columns={"timestamp": "open_time"}, inplace=True)
    df["timestamp"] = df["open_time"]
    df["close_time"] = df["open_time"] + (interval_ms - 1)
    return df


async def load_bars(cfg: ReplayConfig) -> pd.DataFrame:
    if cfg.source == "snapshot":
        return await _load_from_snapshot(cfg)
    if cfg.source == "binance":
        return await _load_from_binance(cfg)
    raise ValueError(f"Невідоме джерело {cfg.source}")


def _sanitize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _sanitize(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, (list, tuple, set)):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, (np.integer, int)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        val = float(obj)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, datetime):
        return obj.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def _ts_to_iso(ts_ms: int) -> str:
    return (
        datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat().replace("+00:00", "Z")
    )


def _write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    def _flatten(value: Any) -> Any:
        sanitized = _sanitize(value)
        if sanitized is None:
            return ""
        if isinstance(sanitized, (dict, list, tuple, set)):
            return json.dumps(sanitized, ensure_ascii=False)
        if isinstance(sanitized, float):
            if math.isnan(sanitized) or math.isinf(sanitized):
                return ""
            return sanitized
        return sanitized

    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _flatten(row.get(key)) for key in fieldnames})


async def run_replay(cfg: ReplayConfig) -> ReplayStats:
    df = await load_bars(cfg)
    if df.empty:
        raise ValueError("Порожній DataFrame після фільтрації")

    dump_dir = cfg.dump_dir
    dump_dir.mkdir(parents=True, exist_ok=True)

    # Файл із інсайт‑рядками реплею у каталозі телеметрії (щоб не змішувати з live)
    try:
        tdir = Path(TELEMETRY_BASE_DIR)
    except Exception:  # pragma: no cover
        tdir = Path("./telemetry")
    tdir.mkdir(parents=True, exist_ok=True)

    # Обчислимо часові межі для імені файлу (із дат афіксом)
    w_start_ms = int(df["open_time"].iloc[0])
    w_end_ms = int(df["open_time"].iloc[-1])

    def _iso_compact(ts_ms: int) -> str:
        return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).strftime("%Y%m%dT%H%M%S")

    insights_path = tdir / (
        f"replay_insights_{cfg.symbol.upper()}_{cfg.interval}_{_iso_compact(w_start_ms)}_{_iso_compact(w_end_ms)}.jsonl"
    )
    paper_path = tdir / (
        f"replay_paper_trades_{cfg.symbol.upper()}_{cfg.interval}_{_iso_compact(w_start_ms)}_{_iso_compact(w_end_ms)}.jsonl"
    )

    cache = InMemoryCache()
    state_manager = DummyStateManager()
    stage1 = AssetMonitorStage1(cache_handler=cache, state_manager=state_manager)
    stage2 = Stage2Processor(state_manager=state_manager)
    # Whale Detection Engine components (телеметрія лише для аналітики)
    whale_exec = InstitutionalEntryMethods()
    whale_zones = SupplyDemandZones()

    stage1_path = dump_dir / "stage1_signals.jsonl"
    stage1_events_path = dump_dir / "stage1_events.jsonl"
    stage2_path = dump_dir / "stage2_outputs.jsonl"
    summary_path = dump_dir / "summary.csv"

    processed = 0
    # ── Міні-KPI лічильники фаз ──
    phase_counts: dict[str, int] = {
        "drift_trend": 0,
        "pre_breakout": 0,
        "post_breakout": 0,
        "momentum": 0,
        "exhaustion": 0,
        "false_breakout": 0,
        "none": 0,
    }
    phase_scores_sum: dict[str, float] = {k: 0.0 for k in phase_counts}
    summary_rows: list[dict[str, Any]] = []

    with (
        stage1_path.open("w", encoding="utf-8") as s1_file,
        stage1_events_path.open("w", encoding="utf-8") as s1e_file,
        stage2_path.open("w", encoding="utf-8") as s2_file,
        insights_path.open("w", encoding="utf-8") as insights_file,
    ):
        paper_file = (
            paper_path.open("w", encoding="utf-8")
            if REPLAY_PAPER_TRADES_ENABLED
            else None
        )
        warmup = max(stage1.atr_manager.period + 1, stage1.volumez_manager.window + 1)

        for idx in range(len(df)):
            window = df.iloc[: idx + 1].copy()
            if len(window) < warmup:
                continue
            stage2.bars_1m[cfg.symbol] = window
            stage2.bars_5m[cfg.symbol] = window
            stage2.bars_1d[cfg.symbol] = window

            stats = await stage1.update_statistics(cfg.symbol, window)
            signal = await stage1.check_anomalies(cfg.symbol, window, stats)
            ts_ms = int(window["open_time"].iloc[-1])
            stage1_record = {
                "timestamp_ms": ts_ms,
                "timestamp_iso": _ts_to_iso(ts_ms),
                **_sanitize(signal),
            }
            s1_file.write(json.dumps(stage1_record, ensure_ascii=False) + "\n")

            # ── Stage1 events (self-sufficient analyzer) ──
            try:
                # stage1_signal event row (для розділу Stage1 та follow-through)
                s1_event_signal = {
                    "ts": stage1_record["timestamp_iso"],
                    "event": "stage1_signal",
                    "symbol": str(cfg.symbol).upper(),
                    # дублюємо ключові поля на корінь задля сумісності аналайзера
                    "signal": stage1_record.get("signal"),
                    "trigger_reasons": stage1_record.get("trigger_reasons")
                    or stage1_record.get("raw_trigger_reasons"),
                    # payload зі stats для follow-through верифікації
                    "payload": {
                        "timestamp": ts_ms,
                        "stats": {
                            "current_price": (stats or {}).get("current_price"),
                            "atr": (stats or {}).get("atr"),
                        },
                    },
                }
                s1e_file.write(
                    json.dumps(_sanitize(s1_event_signal), ensure_ascii=False) + "\n"
                )
            except Exception:
                # телеметрія — best‑effort, не ламаємо реплей
                pass

            stage2_result = await stage2.process(signal)
            stage2_record = {
                "timestamp_ms": ts_ms,
                "timestamp_iso": _ts_to_iso(ts_ms),
                **_sanitize(stage2_result),
            }
            s2_file.write(json.dumps(stage2_record, ensure_ascii=False) + "\n")

            # ── KPI: фази ──
            mc = stage2_result.get("market_context") or {}
            phase = mc.get("phase")
            pscore = mc.get("phase_score")
            phase_key = str(phase) if isinstance(phase, str) else "none"
            if phase_key not in phase_counts:
                phase_key = "none"
            phase_counts[phase_key] += 1
            try:
                phase_scores_sum[phase_key] += float(pscore or 0.0)
            except Exception:
                pass

            current_price = float(stats.get("current_price") or 0.0)
            atr_value = float(stats.get("atr") or 0.0)
            atr_pct = atr_value / current_price if current_price > 0 else None
            volume_z = stats.get("volume_z")
            ctx = stage2_result.get("market_context") or {}
            key_levels_meta = ctx.get("key_levels_meta") or {}
            meta_ctx = ctx.get("meta") or {}
            dist_to_edge_pct = meta_ctx.get("dist_to_edge_pct") or key_levels_meta.get(
                "dist_to_edge_pct"
            )
            nearest_edge = (
                meta_ctx.get("near_edge")
                or key_levels_meta.get("near_edge")
                or key_levels_meta.get("nearest_edge")
            )
            low_gate_effective = meta_ctx.get("low_gate_effective")
            low_gate_base = meta_ctx.get("low_gate")
            htf_ok = meta_ctx.get("htf_ok")
            htf_strength = meta_ctx.get("htf_strength")
            htf_score = meta_ctx.get("htf_score")
            # Stage2 diagnostics we want to surface in summary for later analysis
            rr_live = meta_ctx.get("rr_live")
            rr_thresh = meta_ctx.get("rr_thresh")
            atr_ratio = meta_ctx.get("atr_ratio")
            atr_ratio_min = meta_ctx.get("atr_ratio_min")
            trend_override_1m = meta_ctx.get("trend_override_1m")
            elite_payload = stage2_result.get("elite_signal_extra") or {}
            elite_reasons = ",".join(elite_payload.get("reasons") or [])
            elite_metrics = elite_payload.get("metrics") or {}

            # ── Whale Detection Engine (телеметрія) ──
            try:
                # Використовуємо останні N значень для VWAP/TWAP/Iceberg
                n_tail = min(50, len(window))
                prices_tail = [float(x) for x in window["close"].tail(n_tail).tolist()]
                vols_tail = [float(x) for x in window["volume"].tail(n_tail).tolist()]
                whale_strats = whale_exec.detect_vwap_twap_strategies(
                    prices_tail, vols_tail
                )

                # Зони попиту/пропозиції — по всьому доступному вікну
                hist_data = {
                    "prices": [float(x) for x in window["close"].tolist()],
                    "volumes": [float(x) for x in window["volume"].tolist()],
                    "symbol": cfg.symbol.upper(),
                }
                ob_levels = (
                    (stats or {}).get("orderbook_levels")
                    if isinstance(stats, dict)
                    else None
                )
                if not ob_levels and isinstance(stats, dict):
                    ob_levels = stats.get("orderbook_depth_levels")
                if ob_levels:
                    hist_data["orderbook_levels"] = ob_levels
                zones_payload = whale_zones.identify_institutional_levels(hist_data)
                key_levels = zones_payload.get("key_levels") or {}
                accum_zones = key_levels.get("accumulation_zones") or []
                dist_zones = key_levels.get("distribution_zones") or []
                liq_pools = key_levels.get("liquidity_pools") or []
                targets = zones_payload.get("probable_targets") or []
                zone_quality = zones_payload.get("risk_levels") or {}
                zq_acc = zone_quality.get("accumulation")
                zq_dist = zone_quality.get("distribution")
                zq_risk = zone_quality.get("risk_from_zones")
            except Exception:  # noqa: BLE001
                whale_strats = {
                    "vwap_buying": False,
                    "vwap_selling": False,
                    "twap_accumulation": False,
                    "iceberg_orders": False,
                }
                accum_zones = []
                dist_zones = []
                liq_pools = []
                targets = []
                zq_acc = None
                zq_dist = None
                zq_risk = None

            # ── Нові поля телеметрії з Stage2.meta ──
            trend_conf = (
                (meta_ctx.get("trend_confluence") or {})
                if isinstance(meta_ctx.get("trend_confluence"), dict)
                else {}
            )
            vol_reg = (
                (meta_ctx.get("volatility_regime") or {})
                if isinstance(meta_ctx.get("volatility_regime"), dict)
                else {}
            )
            trap_info = (
                (meta_ctx.get("trap") or {})
                if isinstance(meta_ctx.get("trap"), dict)
                else {}
            )
            cascade_info = (
                (meta_ctx.get("cascade") or {})
                if isinstance(meta_ctx.get("cascade"), dict)
                else {}
            )
            trap_reasons = (
                ",".join(trap_info.get("reasons") or [])
                if isinstance(trap_info.get("reasons"), (list, tuple))
                else ""
            )

            # ── Обчислення інтегральних whale‑метрик інсайту (presence/bias) ──
            presence_score = WhaleTelemetryScoring.presence_score_from(
                whale_strats, zones_payload
            )
            whale_bias = WhaleTelemetryScoring.whale_bias_from(whale_strats)
            # Watch‑теги з meta, якщо наявні
            raw_tags = meta_ctx.get("tags") if isinstance(meta_ctx, dict) else None
            tags = [str(t) for t in (raw_tags or []) if isinstance(t, str)]
            watch_tags = [
                t
                for t in tags
                if t
                in (
                    "momentum_overbought_watch",
                    "momentum_cooling_box",
                    "retest_watch",
                )
            ]

            # Запис інсайту у окремий файл телеметрії реплею
            insight_row = {
                "ts": stage2_record["timestamp_iso"],
                "ts_ms": ts_ms,
                "symbol": cfg.symbol.upper(),
                "interval": cfg.interval,
                "signal": stage1_record.get("signal"),
                "whale_presence": presence_score,
                "whale_bias": whale_bias,
                "vwap_deviation": whale_strats.get("vwap_deviation"),
                "watch_tags": watch_tags,
                "near_edge": bool(nearest_edge) if nearest_edge is not None else None,
                "dist_to_edge_pct": dist_to_edge_pct,
            }
            insights_file.write(
                json.dumps(_sanitize(insight_row), ensure_ascii=False) + "\n"
            )

            # ── phase_detected подія для аналайзера (включно з базовими метриками) ──
            try:
                mc = stage2_result.get("market_context") or {}
                meta_ctx = mc.get("meta") or {}
                # побудувати простий stage2_hint з whale‑метрик (напрям і сила)
                hint_dir = None
                hint_score = None
                try:
                    if isinstance(whale_bias, (int, float)) and isinstance(
                        presence_score, (int, float)
                    ):
                        if whale_bias > 0:
                            hint_dir = "UP"
                        elif whale_bias < 0:
                            hint_dir = "DOWN"
                        hint_score = float(abs(whale_bias)) * float(presence_score)
                except Exception:
                    hint_dir = None
                    hint_score = None

                phase_event = {
                    "ts": stage2_record["timestamp_iso"],
                    "event": "phase_detected",
                    "symbol": str(cfg.symbol).upper(),
                    # кореневі поля, які читає аналайзер напряму
                    "phase": mc.get("phase"),
                    "score": mc.get("phase_score"),
                    "reasons": mc.get("phase_reasons"),
                    # directional/basic
                    "band_pct": meta_ctx.get("band_pct"),
                    "near_edge": meta_ctx.get("near_edge"),
                    "atr_ratio": meta_ctx.get("atr_ratio"),
                    # vol_z з Stage1.stats (очікуване ім'я саме vol_z у аналайзері)
                    "vol_z": (stats or {}).get("volume_z"),
                    # DVR/CD/схил ATR — якщо доступні у Stage1.stats
                    "dvr": (stats or {}).get("directional_volume_ratio"),
                    "cd": (stats or {}).get("cumulative_delta"),
                    "slope_atr": (stats or {}).get("price_slope_atr"),
                    # HTF телеметрія (best‑effort)
                    "htf_ok": meta_ctx.get("htf_ok"),
                    "htf_score": meta_ctx.get("htf_score"),
                    "htf_strength": meta_ctx.get("htf_strength"),
                    # Whale поля для покриття секції whale
                    "whale_presence": presence_score,
                    "whale_bias": whale_bias,
                    "whale_vwap_dev": whale_strats.get("vwap_deviation"),
                    "whale_vol_regime": (
                        (meta_ctx.get("volatility_regime") or {}).get("regime")
                        if isinstance(meta_ctx.get("volatility_regime"), dict)
                        else None
                    ),
                    # Підказка Stage2 для секції hints (мінімальна)
                    "payload": {
                        "stage2_hint": {
                            "dir": hint_dir,
                            "score": hint_score,
                        }
                    },
                }
                s1e_file.write(
                    json.dumps(_sanitize(phase_event), ensure_ascii=False) + "\n"
                )
            except Exception:
                # не критично для реплею
                pass

            summary_rows.append(
                {
                    "timestamp_ms": ts_ms,
                    "timestamp_iso": stage2_record["timestamp_iso"],
                    # Ціна на закритті поточної свічки (для follow-through аналізу)
                    "price": current_price,
                    "signal": stage1_record.get("signal"),
                    "recommendation": stage2_result.get("recommendation"),
                    # Stage2 stores confidence under confidence_metrics -> composite_confidence
                    "confidence": (
                        stage2_result.get("confidence_metrics", {}) or {}
                    ).get("composite_confidence"),
                    # keep full metrics for debugging/analysis
                    "confidence_metrics": (
                        json.dumps(
                            stage2_result.get("confidence_metrics") or {},
                            ensure_ascii=False,
                        )
                        if stage2_result.get("confidence_metrics")
                        else ""
                    ),
                    # diagnostic fields from Stage2 post-processing
                    "reco_original": stage2_result.get("reco_original"),
                    "reco_gate_reason": stage2_result.get("reco_gate_reason"),
                    "elite_eligible": elite_payload.get("eligible"),
                    "elite_reasons": elite_reasons,
                    "trigger_reasons": ",".join(
                        stage1_record.get("trigger_reasons", []) or []
                    ),
                    "atr_pct": atr_pct,
                    "volume_z": volume_z,
                    "dist_to_edge_pct": dist_to_edge_pct,
                    "nearest_edge": nearest_edge,
                    "low_gate_effective": low_gate_effective,
                    "low_gate_base": low_gate_base,
                    "htf_ok": htf_ok,
                    "htf_strength": htf_strength,
                    "htf_score": htf_score,
                    # RR/ATR diagnostics (may be NaN/None when not available)
                    "rr_live": rr_live,
                    "rr_thresh": rr_thresh,
                    "atr_ratio": atr_ratio,
                    "atr_ratio_min": atr_ratio_min,
                    "trend_override_1m": trend_override_1m,
                    "scenario": ctx.get("scenario"),
                    "elite_metrics": (
                        json.dumps(elite_metrics, ensure_ascii=False)
                        if elite_metrics
                        else ""
                    ),
                    # ── Телеметрія кризових режимів/конфлюенсу ──
                    "trend_confluence_confluence": trend_conf.get("confluence"),
                    "trend_confluence_direction": trend_conf.get("direction"),
                    "trend_confluence_strength": trend_conf.get("strength"),
                    "volatility_regime": vol_reg.get("regime"),
                    "volatility_atr_ratio": vol_reg.get("atr_ratio"),
                    "trap_detected": trap_info.get("trap_detected"),
                    "trap_reasons": trap_reasons,
                    "cascade_detected": cascade_info.get("cascade_detected"),
                    "cascade_severity": cascade_info.get("severity"),
                    "cascade_type": cascade_info.get("type"),
                    # ── Whale Detection Engine ──
                    "whale_vwap_buying": whale_strats.get("vwap_buying"),
                    "whale_vwap_selling": whale_strats.get("vwap_selling"),
                    "whale_twap_accumulation": whale_strats.get("twap_accumulation"),
                    "whale_iceberg_orders": whale_strats.get("iceberg_orders"),
                    "whale_accum_zones_count": len(accum_zones),
                    "whale_dist_zones_count": len(dist_zones),
                    "whale_liquidity_pools_count": len(liq_pools),
                    "whale_targets_count": len(targets),
                    "whale_zone_quality_accumulation": zq_acc,
                    "whale_zone_quality_distribution": zq_dist,
                    "whale_zone_risk_from_zones": zq_risk,
                }
            )

            # ── «Паперові» входи в реплеї (лише під фіче‑флаг) ──
            if REPLAY_PAPER_TRADES_ENABLED:
                try:
                    rule = dict(REPLAY_PAPER_TRADES_RULE or {})
                    pres_min = float(rule.get("presence_min", 0.25) or 0.25)
                    bias_abs_min = float(rule.get("bias_abs_min", 0.6) or 0.6)
                    # Перевірка «біля рівня»: або meta.near_edge==True, або dist<= конфіг‑порогу
                    dist_thr = float(
                        rule.get(
                            "dist_to_edge_pct_max",
                            float(
                                STAGE1_VOLUME_SPIKE_GATES.get(
                                    "near_edge_window_pct", 0.15
                                )
                                or 0.15
                            ),
                        )
                    )
                    near_ok = False
                    if isinstance(nearest_edge, bool):
                        near_ok = nearest_edge
                    if not near_ok and isinstance(dist_to_edge_pct, (int, float)):
                        near_ok = float(dist_to_edge_pct) <= dist_thr

                    if (
                        presence_score >= pres_min
                        and abs(float(whale_bias)) >= bias_abs_min
                        and near_ok
                    ):
                        side = "BUY" if whale_bias > 0 else "SELL"
                        paper_row = {
                            "ts": stage2_record["timestamp_iso"],
                            "ts_ms": ts_ms,
                            "symbol": cfg.symbol.upper(),
                            "interval": cfg.interval,
                            "side": side,
                            "price": current_price,
                            "presence": presence_score,
                            "bias": whale_bias,
                            "dist_to_edge_pct": dist_to_edge_pct,
                            "reason": "whale_rule_v1",
                        }
                        if paper_file is not None:
                            paper_file.write(
                                json.dumps(_sanitize(paper_row), ensure_ascii=False)
                                + "\n"
                            )
                except Exception:
                    logger.debug("paper-trade eval failed", exc_info=True)
            processed += 1
        if paper_file is not None:
            paper_file.close()

    _write_summary_csv(summary_path, summary_rows)

    # ── Обчислення та збереження міні-KPI ──
    breakout_like = phase_counts.get("pre_breakout", 0) + phase_counts.get(
        "post_breakout", 0
    )
    kpi = {
        "symbol": cfg.symbol.upper(),
        "interval": cfg.interval,
        "bars": int(len(df)),
        "processed": processed,
        "counts": phase_counts,
        "breakout_like": breakout_like,
        "drift_trend": phase_counts.get("drift_trend", 0),
        "ratios": {
            "drift_vs_breakout": (
                (phase_counts.get("drift_trend", 0) / breakout_like)
                if breakout_like > 0
                else None
            )
        },
        "avg_scores": {
            k: (
                (phase_scores_sum.get(k, 0.0) / phase_counts[k])
                if phase_counts.get(k, 0) > 0
                else None
            )
            for k in phase_counts
        },
    }
    with (dump_dir / "kpi.json").open("w", encoding="utf-8") as f:
        json.dump(_sanitize(kpi), f, ensure_ascii=False, indent=2)

    return ReplayStats(
        bars=len(df),
        processed=processed,
        stage1_signals=processed,
        stage2_outputs=processed,
        dump_dir=dump_dir,
    )


async def _cli_main(args: argparse.Namespace) -> None:
    # Опційно вимикаємо fast-path у Stage2 лише на час цього реплею
    restore_fast_path: bool | None = None
    if getattr(args, "disable_fast_path", False):
        try:
            # Запам'ятати попередній стан і вимкнути
            restore_fast_path = bool(STAGE2_RUNTIME.get("fast_path_enabled", False))
            STAGE2_RUNTIME["fast_path_enabled"] = False
            logger.info(
                "Fast-path вимкнено для цього реплею (STAGE2_RUNTIME['fast_path_enabled']=False)"
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "Не вдалося змінити STAGE2_RUNTIME.fast_path_enabled — продовжуємо зі значенням за замовчуванням"
            )
    cfg = ReplayConfig(
        symbol=args.symbol,
        interval=args.interval,
        source=args.source,
        start_ms=_parse_ts(args.start),
        end_ms=_parse_ts(args.end),
        limit=args.limit,
        window=args.window,
        dump_dir=Path(args.dump_dir),
    )
    stats = await run_replay(cfg)
    logger.info(
        "Оброблено %d/%d барів для %s (%s). Дані у %s",
        stats.processed,
        stats.bars,
        cfg.symbol,
        cfg.interval,
        stats.dump_dir,
    )
    # Відновлюємо попередній стан fast-path після реплею
    if restore_fast_path is not None:
        STAGE2_RUNTIME["fast_path_enabled"] = restore_fast_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Стрім-реплей Stage1→Stage2")
    parser.add_argument("symbol", help="Символ Binance, напр. BTCUSDT")
    parser.add_argument("--interval", default="1m", help="Інтервал свічок")
    parser.add_argument(
        "--source",
        default="snapshot",
        choices=["snapshot", "binance"],
        help="Джерело даних",
    )
    parser.add_argument("--start", help="Початковий час (ISO або epoch ms)")
    parser.add_argument("--end", help="Кінцевий час (ISO або epoch ms)")
    parser.add_argument("--limit", type=int, help="Максимальна кількість барів")
    parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_WINDOW,
        help="Розмір вікна історії для Stage1",
    )
    parser.add_argument(
        "--dump-dir",
        default="./replay_dump",
        help="Каталог для результатів",
    )
    parser.add_argument(
        "--disable-fast-path",
        action="store_true",
        help="Тимчасово вимкнути fast-path у Stage2 на час реплею (для повільного шляху валідації)",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(args=list(argv) if argv is not None else None)
    asyncio.run(_cli_main(args))


if __name__ == "__main__":
    main()
