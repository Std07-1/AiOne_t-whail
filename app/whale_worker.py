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

import numpy as np
from rich.console import Console
from rich.logging import RichHandler

from app.settings import load_datastore_cfg, settings
from config.config import (
    INSIGHT_LAYER_ENABLED,
    INTERVAL_TTL_MAP,
    PROM_GAUGES_ENABLED,
    STRICT_LOG_MARKERS,
    WHALE_SCORING_V2_ENABLED,
    WHALE_STALE_MS,
    WHALE_THROTTLE_SEC,
)
from config.config_whale import STAGE2_WHALE_TELEMETRY
from config.keys import normalize_symbol
from data.unified_store import StoreConfig, StoreProfile, UnifiedDataStore

try:
    from config import config as _config_module
except Exception:  # pragma: no cover - fallback на дефолти
    _config_module = None

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.whale_worker")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


# Порог попередження для занадто довгого інтервалу між публікаціями (мс)
_PUBLISH_GAP_WARN_MS = 6_000
# Останній ts публікації по символу
_LAST_PUBLISH_TS: dict[str, int] = {}

set_whale_publish_gap = None  # type: ignore[assignment]
observe_whale_publish_gap_bucket = None  # type: ignore[assignment]
inc_whale_publish_gap_warn = None  # type: ignore[assignment]

if PROM_GAUGES_ENABLED:
    try:
        from telemetry.prom_gauges import (  # type: ignore
            inc_whale_publish_gap_warn as _inc_whale_publish_gap_warn,
        )
        from telemetry.prom_gauges import (  # type: ignore
            observe_whale_publish_gap_bucket as _observe_whale_publish_gap_bucket,
        )
        from telemetry.prom_gauges import (  # type: ignore
            set_whale_publish_gap as _set_whale_publish_gap,
        )

        set_whale_publish_gap = _set_whale_publish_gap  # type: ignore[assignment]
        observe_whale_publish_gap_bucket = _observe_whale_publish_gap_bucket  # type: ignore[assignment]
        inc_whale_publish_gap_warn = _inc_whale_publish_gap_warn  # type: ignore[assignment]
    except Exception:
        logger.debug("Не вдалося імпортувати прометей-метрики для whale_worker")
        set_whale_publish_gap = None  # type: ignore[assignment]
        observe_whale_publish_gap_bucket = None  # type: ignore[assignment]
        inc_whale_publish_gap_warn = None  # type: ignore[assignment]


def _resolve_stale_ms() -> float:
    """
    Розв'язує значення WHALE_STALE_MS з конфігурації з безпечним дефолтом.
    Додає логування для діагностики прийняття значення.
    """
    logger.debug("Початок _resolve_stale_ms() - читаю config.WHALE_STALE_MS")
    candidate = None
    if _config_module is not None:
        candidate = getattr(_config_module, "WHALE_STALE_MS", None)
    if candidate is None:
        candidate = WHALE_STALE_MS

    try:
        value = float(candidate)
        logger.info(
            "WHALE_STALE_MS встановлено з config: %.0f ms",
            value,
        )
        return value
    except Exception as exc:
        logger.warning("Не вдалося перетворити config.WHALE_STALE_MS на float: %s", exc)
        logger.debug("Деталі помилки при парсингу WHALE_STALE_MS", exc_info=True)

    fallback = 7_000.0
    logger.info(
        "WHALE_STALE_MS не знайдено — використано дефолтне значення: %.0f ms", fallback
    )
    return fallback


_WHALE_STALE_MS = max(1_000.0, _resolve_stale_ms())


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


def _zones_simple(
    closes: list[float], vwap: float, lookback: int = 20
) -> dict[str, int]:
    # Дуже проста евристика: лічильники барів вище/нижче VWAP
    tail = closes[-lookback:] if len(closes) >= lookback else closes
    accum = int(sum(1 for x in tail if x > vwap))
    dist = int(sum(1 for x in tail if x < vwap))
    return {"accum": accum, "dist": dist, "stop_hunt": 0}


async def _bootstrap_store() -> UnifiedDataStore:
    logger.debug("Початок _bootstrap_store()")
    t0 = time.time()
    cfg = load_datastore_cfg()
    try:
        from config.config import NAMESPACE as cfg_ns  # type: ignore
    except Exception:  # pragma: no cover - fallback коли модуль недоступний
        cfg_ns = getattr(cfg, "namespace", "")

    effective_ns = str(cfg_ns).strip(":")
    current_ns = str(getattr(cfg, "namespace", "")).strip(":")
    if effective_ns and effective_ns != current_ns:
        logger.info(
            "[STRICT_WHALE] Namespace override активний: %s → %s",
            current_ns or "<unset>",
            effective_ns,
        )
        if hasattr(cfg, "model_copy"):
            cfg = cfg.model_copy(update={"namespace": effective_ns})  # type: ignore[attr-defined]
        else:  # pragma: no cover - підтримка pydantic v1
            setattr(cfg, "namespace", effective_ns)
        current_ns = effective_ns
    else:
        current_ns = current_ns or effective_ns

    try:
        logger.debug(
            "Loaded datastore cfg: namespace=%s base_dir=%s intervals_ttl=%s write_behind=%s validate_on_read=%s validate_on_write=%s",
            current_ns,
            cfg.base_dir,
            getattr(cfg, "intervals_ttl", None),
            getattr(cfg, "write_behind", None),
            getattr(cfg, "validate_on_read", None),
            getattr(cfg, "validate_on_write", None),
        )
    except Exception:
        logger.debug("Не вдалося логувати повністю cfg (часткова серіалізація)")

    from redis.asyncio import Redis

    r = Redis(host=settings.redis_host, port=settings.redis_port)
    # RAM-only fallback if Redis is not available
    try:
        t_ping0 = time.time()
        await asyncio.wait_for(r.ping(), timeout=0.5)
        ping_ms = int((time.time() - t_ping0) * 1000)
        redis_available = True
        logger.debug(
            "Redis доступний host=%s port=%s ping_ms=%d",
            settings.redis_host,
            settings.redis_port,
            ping_ms,
        )
    except Exception as exc:
        redis_available = False
        if STRICT_LOG_MARKERS:
            logger.warning("[STRICT_WHALE] Redis недоступний — RAM-only режим: %s", exc)
        else:
            logger.warning("Redis недоступний — RAM-only режим")
        logger.debug("Redis connection error details: %s", exc, exc_info=True)

    try:
        profile_data = cfg.profile.model_dump()
        logger.debug("Серіалізація profile via model_dump успішна")
    except Exception:
        try:
            profile_data = cfg.profile.dict()
            logger.debug("Серіалізація profile via dict() успішна")
        except Exception as exc:
            logger.warning("Не вдалося серіалізувати profile: %s", exc)
            logger.debug("profile serialization error", exc_info=True)
            profile_data = {}

    io_attempts = cfg.io_retry_attempts if redis_available else 0
    io_backoff = cfg.io_retry_backoff if redis_available else 0.0

    logger.debug(
        "IO retry config: redis_available=%s io_attempts=%s io_backoff=%s",
        redis_available,
        io_attempts,
        io_backoff,
    )

    store_cfg = StoreConfig(
        namespace=current_ns,
        base_dir=cfg.base_dir,
        profile=StoreProfile(**profile_data),
        intervals_ttl=cfg.intervals_ttl,
        write_behind=cfg.write_behind,
        validate_on_read=cfg.validate_on_read,
        validate_on_write=cfg.validate_on_write,
        io_retry_attempts=io_attempts,
        io_retry_backoff=io_backoff,
    )

    logger.debug(
        "Створено StoreConfig: namespace=%s base_dir=%s profile_keys=%s intervals_ttl=%s",
        store_cfg.namespace,
        store_cfg.base_dir,
        (
            list(getattr(store_cfg.profile, "__dict__", {}).keys())
            if store_cfg.profile
            else None
        ),
        store_cfg.intervals_ttl,
    )

    ds = UnifiedDataStore(redis=r, cfg=store_cfg)
    try:
        t_maint0 = time.time()
        await ds.start_maintenance()
        maint_ms = int((time.time() - t_maint0) * 1000)
        logger.debug(
            "UnifiedDataStore.start_maintenance() виконано успішно (ms=%d)", maint_ms
        )
    except Exception as exc:
        logger.warning("Помилка під час start_maintenance(): %s", exc)
        logger.debug("start_maintenance exception details", exc_info=True)

    took_ms = int((time.time() - t0) * 1000)
    logger.debug(
        "_bootstrap_store() завершено, total_ms=%d redis_available=%s",
        took_ms,
        redis_available,
    )
    return ds


async def _get_symbols(ds: UnifiedDataStore) -> list[str]:
    """
    Отримати список символів для обробки.
    Логування: час виконання, кількість отриманих символів, fallback та помилки.
    """
    logger.debug("Початок отримання символів через get_fast_symbols()")
    t0 = time.time()
    syms = None
    try:
        syms = await ds.get_fast_symbols()
    except Exception as exc:
        # Критична помилка при читанні fast_symbols — логнути та перейти на seed
        logger.warning("[STRICT_WHALE] get_fast_symbols() помилка: %s", exc)
        syms = None

    # Якщо отримано результати — нормалізувати та повернути
    if syms:
        try:
            res = [str(s).upper() for s in syms]
        except Exception as exc:
            logger.warning(
                "[STRICT_WHALE] Помилка нормалізації fast_symbols: %s — використовую seed",
                exc,
            )
            res = []
        took_ms = int((time.time() - t0) * 1000)
        logger.debug(
            "Отримано fast_symbols: count=%d time_ms=%d",
            len(res),
            took_ms,
        )
        return res

    # Fallback — ручний seed з конфігу
    from config.config import (
        MANUAL_FAST_SYMBOLS_SEED,  # локальний імпорт для тестів/моків
    )

    try:
        seed = [str(s).upper() for s in MANUAL_FAST_SYMBOLS_SEED]
    except Exception as exc:
        logger.error(
            "[STRICT_WHALE] MANUAL_FAST_SYMBOLS_SEED некоректний: %s — повертаю пустий список",
            exc,
            exc_info=True,
        )
        seed = []

    took_ms = int((time.time() - t0) * 1000)
    logger.debug(
        "Використано MANUAL_FAST_SYMBOLS_SEED: count=%d time_ms=%d",
        len(seed),
        took_ms,
    )
    return seed


async def run_whale_worker() -> None:
    """
    Асинхронно запускає WhaleWorker, який обробляє ринкові дані для кількох символів з метою виявлення активності "whale" та публікує presence-скори й інсайти.
    Ланцюг: публікує Redis ключ whale/{symbol}/1m → Stage2 читає → router приймає рішення.
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

    rings: dict[str, Ring] = {}

    ttl = int(INTERVAL_TTL_MAP.get("1m", 180))

    try:
        throttle_sec = float(WHALE_THROTTLE_SEC)
    except Exception:
        throttle_sec = 3.0
    throttle_sec = max(1.0, min(3.0, throttle_sec))

    logger.debug(
        "Конфіг: tail_window=%d strong_dev_thr=%.4f mid_dev_thr=%.4f throttle=%.1f",
        tail_window,
        strong_dev_thr,
        mid_dev_thr,
        throttle_sec,
    )

    while True:
        # цикл обробки та публікації
        loop_t0 = time.time()
        try:
            logger.debug("Початок ітерації WhaleWorker")
            symbols = await _get_symbols(ds)
            logger.debug("Отримано список символів для обробки: count=%d", len(symbols))
            for sym in symbols:
                sym_t0 = time.time()
                logger.debug("Початок обробки символу %s", sym)
                sym_for_keys = normalize_symbol(sym)
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
                        t_df0 = time.time()
                        df = await ds.get_df(sym, "1m", limit=max(300, tail_window))
                        t_df = int((time.time() - t_df0) * 1000)
                        if df is not None and len(df):
                            for _, row in df.tail(ring.closes.maxlen).iterrows():
                                close = float(row.get("close"))
                                vol = float(row.get("volume"))
                                ring.push(close, vol)
                            logger.debug(
                                "Ініціалізовано Ring історичними даними для %s (n=%d) load_ms=%d",
                                sym,
                                len(ring.closes),
                                t_df,
                            )
                        else:
                            logger.debug(
                                "Дані для %s не знайдено або порожні load_ms=%d",
                                sym,
                                t_df,
                            )
                    except Exception as exc:
                        logger.warning(
                            "Помилка при завантаженні історії для %s: %s", sym, exc
                        )
                # Додатково спробуємо підтягнути останній бар (якщо з'явився новий)
                try:
                    t_last0 = time.time()
                    last = await ds.get_last(sym, "1m")
                    t_last = int((time.time() - t_last0) * 1000)
                    if isinstance(last, dict):
                        c = float(last.get("close"))
                        v = float(last.get("volume"))
                        ring.push(c, v)
                        logger.debug(
                            "Додано останній бар для %s: close=%.6f, volume=%.6f load_ms=%d",
                            sym,
                            c,
                            v,
                            t_last,
                        )
                    else:
                        logger.debug(
                            "last для %s відсутній або некоректний load_ms=%d",
                            sym,
                            t_last,
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

                # метрики
                t_calc0 = time.time()
                vdev = _vwap_dev(closes, volumes, window=tail_window)
                slope = _slope_twap(closes, window=min(20, tail_window))
                iceberg = _iceberg_flag(volumes, z_thr=2.5)
                t_calc = int((time.time() - t_calc0) * 1000)

                logger.debug(
                    "Розраховано метрики для %s: vdev=%.6f, slope=%.6f, iceberg=%s calc_ms=%d last_close=%.6f last_vol=%.6f",
                    sym,
                    vdev,
                    slope,
                    iceberg,
                    t_calc,
                    float(closes[-1]),
                    float(volumes[-1]),
                )

                # Стратегії‑ознаки
                strats = {
                    "vwap_buying": vdev > 0.0,
                    "vwap_selling": vdev < 0.0,
                    "twap_accumulation": slope > 0.001,
                    "iceberg_orders": iceberg,
                }

                vwap_abs = abs(vdev)
                vwap_level = "none"
                if vwap_abs >= strong_dev_thr:
                    vwap_level = "strong"
                elif vwap_abs >= mid_dev_thr:
                    vwap_level = "mid"

                # Зони (дуже проста евристика)
                vwap_est = float(closes[-1]) - vdev * float(closes[-1])
                zones = _zones_simple(closes, vwap=vwap_est, lookback=20)

                # Presence score
                presence_raw = 0.0
                presence_raw += (1.0 if (vdev > 0 or vdev < 0) else 0.0) * float(
                    weights.get("vwap", 0.25)
                )
                presence_raw += (1.0 if iceberg else 0.0) * float(
                    weights.get("iceberg", 0.20)
                )
                presence_raw += (1.0 if zones.get("accum", 0) >= 3 else 0.0) * float(
                    weights.get("accum", 0.10)
                )
                presence_raw += (1.0 if zones.get("dist", 0) >= 3 else 0.0) * float(
                    weights.get("dist", 0.30)
                )
                presence_raw += (1.0 if slope > 0.0 else 0.0) * float(
                    weights.get("twap", 0.15)
                )
                # dev bonus
                if vwap_level == "strong":
                    presence_raw += dev_bonus_hi
                elif vwap_level == "mid":
                    presence_raw += dev_bonus_lo
                # iceberg-only cap
                if strats["iceberg_orders"] and not any(
                    [
                        strats["vwap_buying"],
                        strats["vwap_selling"],
                        strats["twap_accumulation"],
                    ]
                ):
                    presence_raw = min(presence_raw, iceberg_only_cap)

                presence = float(round(max(0.0, min(1.0, presence_raw)), 4))

                logger.debug(
                    "Presence raw=%.6f -> clamped=%.4f (vwap_level=%s, dev_bonus_hi=%.4f dev_bonus_lo=%.4f iceberg_cap=%.4f)",
                    presence_raw,
                    presence,
                    vwap_level,
                    dev_bonus_hi,
                    dev_bonus_lo,
                    iceberg_only_cap,
                )

                # Bias (sign від vdev + підсилення slope + корекція iceberg)
                bias = 0.0
                if vdev != 0.0:
                    base = min(1.0, vwap_abs / max(1e-6, strong_dev_thr) * 0.5)
                    bias = math.copysign(base, vdev)
                    # twap boost ~0.1 у бік slope
                    bias += (
                        0.1
                        if (slope > 0 and bias > 0)
                        else (-0.1 if (slope < 0 and bias < 0) else 0.0)
                    )
                    # iceberg epsilon
                    if iceberg:
                        bias += 0.02 if bias > 0 else -0.02
                    bias = float(max(-1.0, min(1.0, bias)))

                logger.debug(
                    "Обчислено bias=%.4f (vdev=%.6f slope=%.6f iceberg=%s)",
                    bias,
                    vdev,
                    slope,
                    iceberg,
                )

                # Dominance flags (buy/sell) — легка версія на базі конфігурації, без складних нормалізацій
                try:
                    dom_cfg = dict(STAGE2_WHALE_TELEMETRY.get("dominance", {}))
                    vdev_thr = float(dom_cfg.get("vwap_dev_min", 0.01))
                    alt = dom_cfg.get("alt_confirm", {}) or {}
                    need_any = int(alt.get("need_any", 1))
                    use_iceberg = bool(alt.get("iceberg", True))
                    use_volspike = bool(alt.get("vol_spike", True))
                    dist_min = int(alt.get("dist_zones_min", 3))

                    alt_buy_ct = 0
                    if use_iceberg:
                        alt_buy_ct += 1 if iceberg else 0
                    if use_volspike:
                        # У цій легкій версії використовуємо iceberg як проксі vol_spike
                        alt_buy_ct += 1 if iceberg else 0
                    if dist_min:
                        alt_buy_ct += 1 if (zones.get("accum", 0) >= dist_min) else 0

                    alt_sell_ct = 0
                    if use_iceberg:
                        alt_sell_ct += 1 if iceberg else 0
                    if use_volspike:
                        alt_sell_ct += 1 if iceberg else 0
                    if dist_min:
                        alt_sell_ct += 1 if (zones.get("dist", 0) >= dist_min) else 0

                    dom_buy = bool((vdev >= vdev_thr) and (alt_buy_ct >= need_any))
                    dom_sell = bool(((-vdev) >= vdev_thr) and (alt_sell_ct >= need_any))
                    logger.debug(
                        "Dominance cfg vdev_thr=%.6f alt_buy_ct=%d alt_sell_ct=%d -> buy=%s sell=%s",
                        vdev_thr,
                        alt_buy_ct,
                        alt_sell_ct,
                        dom_buy,
                        dom_sell,
                    )
                except Exception:
                    dom_buy = False
                    dom_sell = False
                    logger.debug(
                        "Dominance computation failed, defaulting to False/False"
                    )

                ts_now_ms = int(time.time() * 1000)
                whale_payload = {
                    "schema": "whale.v2",
                    "ts": ts_now_ms,
                    "symbol": sym,
                    "presence_score": presence,
                    "bias": bias,
                    "vwap_deviation": float(round(vdev, 6)),
                    "strats": strats,
                    "zones": zones,
                    "dominance": {"buy": dom_buy, "sell": dom_sell},
                    "weights_profile": "strict",
                    "explain": {
                        "dev_level": vwap_level,
                        "notes": [
                            n
                            for n in [
                                (
                                    "asymmetry_bonus"
                                    if (zones.get("dist", 0) - zones.get("accum", 0))
                                    * (1 if bias < 0 else -1)
                                    > 2
                                    else None
                                ),
                                "strong_vwap_dev" if vwap_level == "strong" else None,
                            ]
                            if n is not None
                        ],
                    },
                }
                try:
                    await ds.redis.jset(
                        "whale", sym_for_keys, "1m", value=whale_payload, ttl=ttl
                    )
                    logger.info(
                        "Опубліковано whale_payload для %s (ключ=whale/%s ttl=%d presence=%.3f bias=%.3f)",
                        sym,
                        sym_for_keys,
                        ttl,
                        presence,
                        bias,
                    )
                    logger.debug("whale_payload (debug) for %s: %s", sym, whale_payload)
                    prev_ts = _LAST_PUBLISH_TS.get(sym)
                    gap_ms = int(ts_now_ms - prev_ts) if prev_ts is not None else 0
                    _LAST_PUBLISH_TS[sym] = ts_now_ms
                    if set_whale_publish_gap:
                        try:
                            set_whale_publish_gap(sym, gap_ms)
                        except Exception:
                            pass
                    if prev_ts is not None:
                        if observe_whale_publish_gap_bucket:
                            try:
                                observe_whale_publish_gap_bucket(sym, gap_ms)
                            except Exception:
                                pass
                        age_ms = gap_ms
                        if STRICT_LOG_MARKERS:
                            logger.info(
                                "[STRICT_WHALE] %s publish_gap_ms=%d age_ms=%d stale_thr=%d",
                                sym,
                                gap_ms,
                                age_ms,
                                int(_WHALE_STALE_MS),
                            )
                        if gap_ms >= _PUBLISH_GAP_WARN_MS:
                            logger.warning(
                                "[WHALE_DIAG] %s publish_gap_ms=%d (>= %d)",
                                sym,
                                gap_ms,
                                _PUBLISH_GAP_WARN_MS,
                            )
                            if inc_whale_publish_gap_warn:
                                try:
                                    inc_whale_publish_gap_warn(sym)
                                except Exception:
                                    pass
                except Exception as exc:
                    if STRICT_LOG_MARKERS:
                        logger.debug(
                            "[STRICT_WHALE] jset whale failed for %s: %s", sym, exc
                        )
                    else:
                        logger.warning(
                            "Не вдалося опублікувати whale_payload для %s: %s", sym, exc
                        )

                if INSIGHT_LAYER_ENABLED:
                    # Телеметрія‑only картка
                    insight_payload = {
                        "schema": "insight.card.v2",
                        "ts": ts_now_ms,
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
                            "insight",
                            sym_for_keys,
                            "1m",
                            value=insight_payload,
                            ttl=ttl,
                        )
                        logger.info(
                            "Опубліковано insight_payload для %s (score=%.3f, tags=%s)",
                            sym,
                            insight_payload["score"],
                            insight_payload["tags"],
                        )
                        logger.debug(
                            "insight_payload (debug) for %s: %s", sym, insight_payload
                        )
                    except Exception as exc:
                        logger.warning(
                            "Не вдалося опублікувати insight_payload для %s: %s",
                            sym,
                            exc,
                        )
                else:
                    logger.debug(
                        "INSIGHT_LAYER_ENABLED=False, пропускаю публікацію insight для %s",
                        sym,
                    )

                if STRICT_LOG_MARKERS:
                    logger.info(
                        "[STRICT_WHALE] symbol=%s ttl=%d presence=%.3f bias=%.3f vdev=%.4f iceberg=%s slope=%.4f",
                        sym,
                        ttl,
                        presence,
                        bias,
                        vdev,
                        str(iceberg),
                        slope,
                    )

                sym_dt_ms = int((time.time() - sym_t0) * 1000)
                logger.debug("Завершено обробку %s за %d ms", sym, sym_dt_ms)
        except asyncio.CancelledError:
            logger.info("WhaleWorker отримав сигнал скасування, завершення роботи.")
            raise
        except Exception as exc:
            logger.error("WhaleWorker loop error: %s", exc, exc_info=True)

        # Throttle
        loop_dt_ms = int((time.time() - loop_t0) * 1000)
        logger.debug(
            "Ітерація WhaleWorker завершена loop_time_ms=%d, затримка між ітераціями: %.1f сек",
            loop_dt_ms,
            throttle_sec,
        )
        await asyncio.sleep(throttle_sec)


if __name__ == "__main__":
    asyncio.run(run_whale_worker())
