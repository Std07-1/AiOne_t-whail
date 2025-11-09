"""WhaleWorker — легкий воркер китової телеметрії .

Призначення:
- Читати останні бари із UnifiedDataStore (Redis/Disk) для 1m інтервалу
- Обчислювати прості метрики: vwap_deviation, twap-схил, iceberg-спайки обсягу
- Формувати payload-и whale.v2 та insight.card.v2 (телеметрія‑only)
- Публікувати у Redis під ключами ai_one:whale:{symbol}:1m та ai_one:insight:{symbol}:1m

Важливо:
- Ізольований модуль (без залежностей від історичних шляхів типу qde/*)
- Керується фіче‑флагами з config.flags/config
- Мінімальний вплив: throttle за WHALE_THROTTLE_SEC
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import numpy as np
from rich.console import Console
from rich.logging import RichHandler

from app.settings import load_datastore_cfg, settings
from config.config import (
    INSIGHT_LAYER_ENABLED,
    INTERVAL_TTL_MAP,
    STAGE2_WHALE_TELEMETRY,
    STRICT_LOG_MARKERS,
    WHALE_SCORING_V2_ENABLED,
    WHALE_THROTTLE_SEC,
)
from data.unified_store import StoreConfig, StoreProfile, UnifiedDataStore

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.whale_worker")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


EMA_PRESENCE: dict[str, float] = {}
@dataclass
class Ring:
    closes: deque[float]
    volumes: deque[float]

    @classmethod
    def with_capacity(cls, size: int) -> Ring:
        return cls(deque(maxlen=size), deque(maxlen=size))

    def push(self, close: float, volume: float) -> None:
        self.closes.append(float(close))
        self.volumes.append(float(volume))

    def ready(self, min_len: int) -> bool:
        return len(self.closes) >= min_len and len(self.volumes) >= min_len


def _safe_mean_std(x: list[float]) -> tuple[float, float]:
    if not x:
        return 0.0, 0.0
    arr = np.asarray(x, dtype=float)
    return float(arr.mean()), float(arr.std(ddof=0))


def _vwap_dev(closes: list[float], volumes: list[float], window: int) -> float:
    if not closes or not volumes:
        return 0.0
    c = np.asarray(closes[-window:], dtype=float)
    v = np.asarray(volumes[-window:], dtype=float)
    denom = float(v.sum()) or 1.0
    vwap = float((c * v).sum() / denom)
    last = float(c[-1])
    if vwap == 0:
        return 0.0
    return (last - vwap) / vwap


def _slope_twap(closes: list[float], window: int) -> float:
    # Проста нормалізована оцінка схилу (різниця між середнім другої та першої половини)
    if len(closes) < window:
        return 0.0
    arr = np.asarray(closes[-window:], dtype=float)
    half = window // 2
    a, b = arr[:half], arr[half:]
    if not len(a) or not len(b):
        return 0.0
    ma = float(a.mean())
    mb = float(b.mean())
    base = float(arr.mean()) or 1.0
    return (mb - ma) / base


def _iceberg_flag(volumes: list[float], z_thr: float = 2.5) -> bool:
    if len(volumes) < 10:
        return False
    mu, sigma = _safe_mean_std(volumes[:-1])
    if sigma <= 0:
        return False
    z = (volumes[-1] - mu) / sigma
    return bool(z >= z_thr)


def _volume_spike(volumes: list[float], z_thr: float = 1.8) -> bool:
    if len(volumes) < 8:
        return False
    mu, sigma = _safe_mean_std(volumes[:-1])
    if sigma <= 0:
        return False
    z = (volumes[-1] - mu) / sigma
    return bool(z >= z_thr)


def _apply_presence_ema(symbol: str, value: float, *, alpha: float, enabled: bool) -> float:
    try:
        x = float(value)
    except Exception:
        x = 0.0
    if not enabled:
        EMA_PRESENCE.pop(symbol, None)
        return float(round(max(0.0, min(1.0, x)), 4))
    prev = EMA_PRESENCE.get(symbol)
    ema = x if prev is None else alpha * x + (1.0 - alpha) * float(prev)
    EMA_PRESENCE[symbol] = ema
    return float(round(max(0.0, min(1.0, ema)), 4))


def compute_dominance(
    vwap_dev: float,
    slope_twap: float,
    flags: dict[str, Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    try:
        vdev_min = float(cfg.get("vwap_dev_min", 0.0) or 0.0)
    except Exception:
        vdev_min = 0.0
    try:
        slope_abs_min = float(cfg.get("slope_abs_min", 0.0) or 0.0)
    except Exception:
        slope_abs_min = 0.0
    alt_cfg = cfg.get("alt_confirm") or {}
    need_any = int(alt_cfg.get("need_any", 1) or 0)
    zone_min = int(alt_cfg.get("dist_zones_min", 0) or 0)

    iceberg = bool(flags.get("iceberg"))
    vol_spike = bool(flags.get("vol_spike"))
    accum_cnt = int(flags.get("zones_accum", 0) or 0)
    dist_cnt = int(flags.get("zones_dist", 0) or 0)

    buy_hits: list[str] = []
    sell_hits: list[str] = []
    if iceberg:
        buy_hits.append("iceberg")
        sell_hits.append("iceberg")
    if vol_spike:
        buy_hits.append("vol_spike")
        sell_hits.append("vol_spike")
    if zone_min and accum_cnt >= zone_min:
        buy_hits.append("zones")
    if zone_min and dist_cnt >= zone_min:
        sell_hits.append("zones")

    buy_candidate = bool(vwap_dev >= vdev_min and slope_twap >= slope_abs_min)
    sell_candidate = bool((-vwap_dev) >= vdev_min and (-slope_twap) >= slope_abs_min)
    buy_confirmed = buy_candidate and len(buy_hits) >= need_any
    sell_confirmed = sell_candidate and len(sell_hits) >= need_any

    return {
        "buy": bool(buy_confirmed),
        "sell": bool(sell_confirmed),
        "meta": {
            "need_any": need_any,
            "buy_candidate": buy_candidate,
            "sell_candidate": sell_candidate,
            "buy_hits": buy_hits,
            "sell_hits": sell_hits,
            "buy_confirmed": buy_confirmed,
            "sell_confirmed": sell_confirmed,
        },
    }


def _zones_simple(
    closes: list[float], vwap: float, lookback: int = 20
) -> dict[str, int]:
    # Дуже проста евристика: лічильники барів вище/нижче VWAP
    tail = closes[-lookback:] if len(closes) >= lookback else closes
    accum = int(sum(1 for x in tail if x > vwap))
    dist = int(sum(1 for x in tail if x < vwap))
    return {"accum": accum, "dist": dist, "stop_hunt": 0}


async def _bootstrap_store() -> UnifiedDataStore:
    cfg = load_datastore_cfg()
    from redis.asyncio import Redis

    r = Redis(host=settings.redis_host, port=settings.redis_port)
    # RAM-only fallback if Redis is not available
    try:
        await asyncio.wait_for(r.ping(), timeout=0.5)
        redis_available = True
    except Exception:
        redis_available = False
        if STRICT_LOG_MARKERS:
            logger.warning("[STRICT_WHALE] Redis недоступний — RAM-only режим")
    try:
        profile_data = cfg.profile.model_dump()
    except Exception:
        profile_data = cfg.profile.dict()
    io_attempts = cfg.io_retry_attempts if redis_available else 0
    io_backoff = cfg.io_retry_backoff if redis_available else 0.0
    store_cfg = StoreConfig(
        namespace=cfg.namespace,
        base_dir=cfg.base_dir,
        profile=StoreProfile(**profile_data),
        intervals_ttl=cfg.intervals_ttl,
        write_behind=cfg.write_behind,
        validate_on_read=cfg.validate_on_read,
        validate_on_write=cfg.validate_on_write,
        io_retry_attempts=io_attempts,
        io_retry_backoff=io_backoff,
    )
    ds = UnifiedDataStore(redis=r, cfg=store_cfg)
    await ds.start_maintenance()
    return ds


async def _get_symbols(ds: UnifiedDataStore) -> list[str]:
    # Використати fast_symbols, якщо доступні; fallback — seed із конфігу
    syms = await ds.get_fast_symbols()
    if syms:
        return [s.upper() for s in syms]
    from config.config import MANUAL_FAST_SYMBOLS_SEED

    return [s.upper() for s in MANUAL_FAST_SYMBOLS_SEED]


async def run_whale_worker() -> None:
    """
    Асинхронно запускає WhaleWorker, який обробляє ринкові дані для кількох символів з метою виявлення активності "whale" та публікує presence-скори й інсайти.
    Основні кроки воркера у циклі:
    1. Перевіряє, чи увімкнено whale scoring; якщо ні — завершує роботу.
    2. Ініціалізує джерела даних та параметри конфігурації.
    3. Для кожного символу:
        - Оновлює або створює кільцевий буфер (Ring) останніх барів (ціна, обсяг).
        - Завантажує історичні дані при потребі, додає останній бар.
        - Розраховує ознаки: відхилення VWAP, схил TWAP, детекцію iceberg-ордерів.
        - Виводить стратегії-ознаки та presence-скор whale-активності.
        - Обчислює directional bias на основі VWAP та інших сигналів.
        - Формує та публікує payload whale-активності у Redis.
        - За потреби — публікує insight-картку з summary-скором і тегами.
        - Веде деталізоване логування, якщо увімкнено STRICT_LOG_MARKERS.
    4. Обробляє виключення та дотримується throttle з конфігурації.
    Повертає:
        None
    Виключення:
        asyncio.CancelledError — при скасуванні задачі.
        Інші виключення логуються та не зупиняють цикл.
    """
    logger.info(
        "Запуск WhaleWorker (WHALE_SCORING_V2_ENABLED=%s)", WHALE_SCORING_V2_ENABLED
    )
    if not WHALE_SCORING_V2_ENABLED:
        logger.info("WhaleWorker вимкнено (WHALE_SCORING_V2_ENABLED=False)")
        return

    ds = await _bootstrap_store()
    logger.info("Ініціалізовано UnifiedDataStore для WhaleWorker")
    weights = dict(STAGE2_WHALE_TELEMETRY.get("presence_weights", {}))
    tail_window = int(STAGE2_WHALE_TELEMETRY.get("tail_window", 50))
    strong_dev_thr = float(STAGE2_WHALE_TELEMETRY.get("strong_dev_thr", 0.02))
    mid_dev_thr = float(STAGE2_WHALE_TELEMETRY.get("mid_dev_thr", 0.012))
    dev_bonus_hi = float(STAGE2_WHALE_TELEMETRY.get("dev_bonus_high", 0.08))
    dev_bonus_lo = float(STAGE2_WHALE_TELEMETRY.get("dev_bonus_low", 0.04))
    iceberg_only_cap = float(STAGE2_WHALE_TELEMETRY.get("iceberg_only_cap", 0.40))
    presence_proxy_cap = float(STAGE2_WHALE_TELEMETRY.get("presence_proxy_cap", 0.85))
    presence_zones_none_cap = float(STAGE2_WHALE_TELEMETRY.get("presence_zones_none_cap", 0.30))
    presence_accum_only_cap = float(STAGE2_WHALE_TELEMETRY.get("presence_accum_only_cap", 0.35))
    bias_vwap_thr = float(STAGE2_WHALE_TELEMETRY.get("bias_vwap_dev_thr", 0.01))
    ema_cfg = dict(STAGE2_WHALE_TELEMETRY.get("ema", {}))
    ema_alpha = float(ema_cfg.get("alpha", 0.30))
    ema_enabled = bool(ema_cfg.get("enabled", True))
    dom_cfg = dict(STAGE2_WHALE_TELEMETRY.get("dominance", {}))

    rings: dict[str, Ring] = {}

    ttl = int(INTERVAL_TTL_MAP.get("1m", 180))

    while True:
        # цикл обробки та публікації
        try:
            symbols = await _get_symbols(ds)
            logger.debug("Отримано список символів для обробки: %s", symbols)
            for sym in symbols:
                # 1) оновити буфер барів (RAM→Redis→Disk)
                ring = rings.get(sym)
                if ring is None:
                    ring = Ring.with_capacity(max(250, tail_window))
                    rings[sym] = ring
                    logger.debug(
                        "Створено новий Ring для %s (maxlen=%d)",
                        sym,
                        ring.closes.maxlen,
                    )
                    # Початкове наповнення з диска/Redis
                    try:
                        df = await ds.get_df(sym, "1m", limit=max(300, tail_window))
                        if df is not None and len(df):
                            for _, row in df.tail(ring.closes.maxlen).iterrows():
                                close = float(row.get("close"))
                                vol = float(row.get("volume"))
                                ring.push(close, vol)
                            logger.debug(
                                "Ініціалізовано Ring історичними даними для %s (n=%d)",
                                sym,
                                len(ring.closes),
                            )
                        else:
                            logger.debug("Дані для %s не знайдено або порожні", sym)
                    except Exception as exc:
                        logger.warning(
                            "Помилка при завантаженні історії для %s: %s", sym, exc
                        )
                # Додатково спробуємо підтягнути останній бар (якщо з'явився новий)
                try:
                    last = await ds.get_last(sym, "1m")
                    if isinstance(last, dict):
                        c = float(last.get("close"))
                        v = float(last.get("volume"))
                        ring.push(c, v)
                        logger.debug(
                            "Додано останній бар для %s: close=%.2f, volume=%.2f",
                            sym,
                            c,
                            v,
                        )
                except Exception as exc:
                    logger.warning(
                        "Помилка при отриманні останнього бару для %s: %s", sym, exc
                    )

                if not ring.ready(min_len=max(20, tail_window // 2)):
                    logger.debug(
                        "Недостатньо даних для %s (len=%d), пропуск",
                        sym,
                        len(ring.closes),
                    )
                    continue

                closes = list(ring.closes)
                volumes = list(ring.volumes)

                vdev = _vwap_dev(closes, volumes, window=tail_window)
                slope = _slope_twap(closes, window=min(20, tail_window))
                iceberg = _iceberg_flag(volumes, z_thr=2.5)
                vol_spike = _volume_spike(volumes, z_thr=2.0)

                logger.debug(
                    "Розраховано метрики для %s: vdev=%.5f, slope=%.5f, iceberg=%s, vol_spike=%s",
                    sym,
                    vdev,
                    slope,
                    iceberg,
                    vol_spike,
                )

                # Стратегії‑ознаки
                strats = {
                    "vwap_buying": vdev > 0.0,
                    "vwap_selling": vdev < 0.0,
                    "twap_accumulation": slope > 0.001,
                    "iceberg_orders": iceberg,
                    "vol_spike": vol_spike,
                }

                vwap_abs = abs(vdev)
                vwap_level = "none"
                if vwap_abs >= strong_dev_thr:
                    vwap_level = "strong"
                elif vwap_abs >= mid_dev_thr:
                    vwap_level = "mid"

                vwap_est = float(closes[-1]) - vdev * float(closes[-1])
                zones = _zones_simple(closes, vwap=vwap_est, lookback=20)

                presence = 0.0
                presence += (1.0 if vdev != 0.0 else 0.0) * float(weights.get("vwap", 0.25))
                presence += (1.0 if iceberg else 0.0) * float(weights.get("iceberg", 0.20))
                presence += (1.0 if zones.get("accum", 0) >= 3 else 0.0) * float(
                    weights.get("accum", 0.10)
                )
                presence += (1.0 if zones.get("dist", 0) >= 3 else 0.0) * float(
                    weights.get("dist", 0.30)
                )
                presence += (1.0 if slope > 0.0 else 0.0) * float(weights.get("twap", 0.15))
                if vwap_level == "strong":
                    presence += dev_bonus_hi
                elif vwap_level == "mid":
                    presence += dev_bonus_lo

                if strats["iceberg_orders"] and not any(
                    [
                        strats["vwap_buying"],
                        strats["vwap_selling"],
                        strats["twap_accumulation"],
                    ]
                ):
                    presence = min(presence, iceberg_only_cap)

                if not any(
                    [
                        strats["vwap_buying"],
                        strats["vwap_selling"],
                        strats["twap_accumulation"],
                        strats["iceberg_orders"],
                    ]
                ):
                    presence = min(presence, presence_proxy_cap)
                    if (zones.get("accum", 0) or 0) <= 0 and (zones.get("dist", 0) or 0) <= 0:
                        presence = min(presence, presence_zones_none_cap)
                    elif (zones.get("accum", 0) or 0) > 0 and (zones.get("dist", 0) or 0) <= 0:
                        presence = min(presence, presence_accum_only_cap)

                presence_capped = max(0.0, min(1.0, presence))
                presence = _apply_presence_ema(sym, presence_capped, alpha=ema_alpha, enabled=ema_enabled)

                bias = 0.0
                if abs(vdev) >= bias_vwap_thr:
                    base = min(1.0, vwap_abs / max(1e-6, strong_dev_thr)) * 0.5
                    bias = math.copysign(base, vdev)
                    if slope > 0 and vdev > 0:
                        bias = min(1.0, bias + 0.1)
                    elif slope < 0 and vdev < 0:
                        bias = max(-1.0, bias - 0.1)
                    if iceberg:
                        bias += 0.02 if vdev > 0 else -0.02
                    if vol_spike:
                        bias += 0.03 if vdev > 0 else -0.03
                    bias = math.copysign(min(1.0, max(0.0, abs(bias))), vdev)
                else:
                    bias = 0.0

                logger.debug(
                    "Presence=%.4f, Bias=%.4f, vwap_level=%s, strats=%s, zones=%s",
                    presence,
                    bias,
                    vwap_level,
                    strats,
                    zones,
                )

                dom_flags = {
                    "iceberg": iceberg,
                    "vol_spike": vol_spike,
                    "zones_accum": zones.get("accum", 0),
                    "zones_dist": zones.get("dist", 0),
                }
                dominance_data = compute_dominance(vdev, slope, dom_flags, dom_cfg)
                dom_buy = bool(dominance_data.get("buy"))
                dom_sell = bool(dominance_data.get("sell"))
                dominance_meta = dominance_data.get("meta", {})

                whale_payload = {
                    "schema": "whale.v2",
                    "ts": int(time.time() * 1000),
                    "symbol": sym,
                    "presence_score": presence,
                    "bias": bias,
                    "vwap_deviation": float(round(vdev, 6)),
                    "strats": strats,
                    "zones": zones,
                    "dominance": {"buy": dom_buy, "sell": dom_sell},
                    "dominance_meta": dominance_meta,
                    "features": {
                        "slope_twap": float(round(slope, 6)),
                        "vol_spike": bool(vol_spike),
                        "iceberg": bool(iceberg),
                    },
                    "weights_profile": "strict",
                    "explain": {
                        "dev_level": vwap_level,
                        "raw_presence": float(round(presence_capped, 4)),
                        "reasons": [
                            n
                            for n in [
                                "iceberg" if iceberg else None,
                                "vol_spike" if vol_spike else None,
                                "vwap_strong" if vwap_level == "strong" else None,
                                "vwap_mid" if vwap_level == "mid" else None,
                            ]
                            if n is not None
                        ],
                    },
                }

                if INSIGHT_LAYER_ENABLED:
                    # Телеметрія‑only картка
                    insight_payload = {
                        "schema": "insight.card.v2",
                        "ts": int(time.time() * 1000),
                        "symbol": sym,
                        "class": "OBSERVE",
                        "score": float(
                            round(
                                max(0.0, min(1.0, presence * 0.6 + abs(bias) * 0.4)), 4
                            )
                        ),
                        "tags": [
                            t
                            for t in [
                                "momentum_overbought_watch" if bias > 0.6 else None,
                                "retest_watch" if 0.4 < abs(bias) < 0.6 else None,
                                "whipsaw_risk_watch" if presence < 0.3 else None,
                            ]
                            if t is not None
                        ],
                        "phase": "none",
                        "overlay": {"impulse_up": float(round(max(0.0, slope), 4))},
                        "whale": {"presence": presence, "bias": bias},
                        "regime": {"atr_ratio": 1.0, "label": "normal"},
                        "next_actions": [
                            "Чекати ретесту або підтвердження",
                        ],
                        "explain": {
                            "used_thresholds": "strict",
                            "note": "fallback_whales",
                        },
                    }
                    try:
                        await ds.redis.jset(
                            "insight", sym, "1m", value=insight_payload, ttl=ttl
                        )
                        logger.info(
                            "Опубліковано insight_payload для %s (score=%.3f, tags=%s)",
                            sym,
                            insight_payload["score"],
                            insight_payload["tags"],
                        )
                    except Exception as exc:
                        logger.warning(
                            "Не вдалося опублікувати insight_payload для %s: %s",
                            sym,
                            exc,
                        )

                if STRICT_LOG_MARKERS:
                    logger.info(
                        "[STRICT_WHALE] symbol=%s presence=%.3f bias=%.3f vdev=%.4f iceberg=%s slope=%.4f",
                        sym,
                        presence,
                        bias,
                        vdev,
                        str(iceberg),
                        slope,
                    )
        except asyncio.CancelledError:
            logger.info("WhaleWorker отримав сигнал скасування, завершення роботи.")
            raise
        except Exception as exc:
            logger.error("WhaleWorker loop error: %s", exc, exc_info=True)

        # Throttle
        logger.debug(
            "Затримка між ітераціями WhaleWorker: %d сек",
            max(1, int(WHALE_THROTTLE_SEC)),
        )
        await asyncio.sleep(max(1, int(WHALE_THROTTLE_SEC)))


if __name__ == "__main__":
    asyncio.run(run_whale_worker())
