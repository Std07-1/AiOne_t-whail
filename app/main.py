"""AiOne_t — точка входу системи.

Завдання модуля:
    • Bootstrap UnifiedDataStore та пов'язані сервіси (metrics, admin, health)
    • Підготовка списку активів (ручний або автоматичний префільтр)
    • Preload історії / денні рівні / ініціалізація LevelManager
    • Запуск WebSocket стрімера (WSWorker) та Stage1 моніторингу
    • Запуск Screening Producer + публікація початкового snapshot у Redis
    • Запуск менеджера угод (TradeLifecycleManager) та оновлювача

Архітектурні акценти:
    • Єдине джерело даних: UnifiedDataStore (Redis + RAM)
    • Мінімум побічних ефектів у глобальному просторі — все через bootstrap()
    • Логування уніфіковане (RichHandler, українська локалізація повідомлень)
"""

import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from redis.asyncio import Redis
from rich.console import Console
from rich.logging import RichHandler

from app.screening_producer import AssetStateManager, screening_producer
from app.settings import load_datastore_cfg, settings
from app.thresholds import Thresholds
from app.utils.helper import estimate_atr_pct, store_to_dataframe
from config.config import (
    FAST_SYMBOLS_TTL_AUTO,
    FAST_SYMBOLS_TTL_MANUAL,
    MANUAL_FAST_SYMBOLS_SEED,
    PREFILTER_BASE_PARAMS,
    PREFILTER_INTERVAL_SEC,
    PRELOAD_1M_LOOKBACK_INIT,
    PRELOAD_DAILY_DAYS,
    REACTIVE_STAGE1,
    SCREENING_LOOKBACK,
    STAGE1_MONITOR_PARAMS,
    STAGE1_PREFILTER_THRESHOLDS,
)
from config.flags import DYNAMIC_SEMAPHORE_TUNING
from config.TOP100_THRESHOLDS import TOP100_THRESHOLDS

# UnifiedDataStore now the single source of truth
from data.unified_store import StoreConfig, StoreProfile, UnifiedDataStore

# ─────────────────────────── Імпорти бізнес-логіки ───────────────────────────
from data.ws_worker import WSWorker
from monitoring.telemetry.hub import get_hub
from monitoring.telemetry.semaphore_tuner import SemaphoreTuner
from stage1.asset_monitoring import AssetMonitorStage1
from stage1.indicators import calculate_global_levels
from stage1.optimized_asset_filter import get_filtered_assets
from stage2.level_manager import LevelManager
from UI.publish_full_state import publish_full_state
from UI.ui_consumer import UIConsumer
from utils.utils import get_tick_size

# admin subsystem видалено
from .preload_and_update import (
    merge_prefilter_with_manual,
    periodic_prefilter_and_update,
    preload_1m_history,
    preload_daily_levels,
)

# Завантажуємо налаштування з .env
load_dotenv()

# ───────────────────────────── Prometheus метрики (опційно) ──────────────
try:
    from config import config as _cfg  # type: ignore
except Exception:  # pragma: no cover - дефолт у разі збою імпорту
    _cfg = None

# Дозволяємо оверрайд порт/ввімкнення через змінні середовища для A/B канарейки
_env_prom_enabled = os.getenv("PROM_GAUGES_ENABLED")
if _env_prom_enabled is not None:
    prom_enabled = str(_env_prom_enabled).strip().lower() in {"1", "true", "yes", "on"}
else:
    prom_enabled = bool(getattr(_cfg, "PROM_GAUGES_ENABLED", True))

_env_prom_port = os.getenv("PROM_HTTP_PORT")
try:
    prom_port = (
        int(_env_prom_port)
        if _env_prom_port is not None
        else int(getattr(_cfg, "PROM_HTTP_PORT", 9108))
    )
except Exception:
    prom_port = int(getattr(_cfg, "PROM_HTTP_PORT", 9108))

if prom_enabled:
    try:
        from telemetry.prom_gauges import init_metrics

        init_metrics(int(prom_port))
    except Exception:
        # best-effort: метрики не блокують старт системи
        pass

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.main")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.INFO)
    # Пишемо у stdout, щоб tee у tools.run_window коректно дзеркалив у файл UTF-8
    logger.addHandler(RichHandler(console=Console(file=sys.stdout), show_path=True))
    logger.propagate = False  # Не пропагувати далі, щоб уникнути дублювання логів

# ───────────────────────────── Санітарна перевірка конфігурації Stage2‑lite ─────────
try:
    from config.config_stage2 import validate_stage2_config

    _cfg_hints = validate_stage2_config()
    for _h in _cfg_hints or []:
        # Легкі попередження на старті процесу; не блокують запуск
        logger.warning("[STRICT_SANITY] Stage2-lite config hint: %s", _h)
except Exception:
    # Best-effort: у середовищах без повного конфігу пропускаємо
    pass


# (FastAPI вилучено) — якщо потрібен REST інтерфейс у майбутньому,
# повернемо створення app/router

# ───────────────────────────── Глобальні змінні модуля ─────────────────────────────
# Єдиний інстанс UnifiedDataStore (створюється в bootstrap)
store: UnifiedDataStore | None = None

# Повністю видалено калібрацію та RAMBuffer — єдиний шар даних UnifiedDataStore

# ───────────────────────────── Шлях / каталоги ─────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
# Каталог зі статичними файлами (фронтенд WebApp)
STATIC_DIR = BASE_DIR / "static"


async def bootstrap() -> UnifiedDataStore:
    """Ініціалізація інфраструктурних компонентів.

    Кроки:
      1. Завантаження datastore конфігурації
      2. Підключення до Redis
      3. Ініціалізація UnifiedDataStore + maintenance loop
    4. Запуск командного адміністративного циклу
    """
    # Ініціалізація глобального сховища
    global store

    # Завантаження конфігурації datastore
    cfg = load_datastore_cfg()
    # Дозволяємо перевизначити namespace через STATE_NAMESPACE/config.config.NAMESPACE поверх YAML (для канарейок/ізоляції стану)
    """
    Нотатки і прийняті рішення
Корінь проблеми після попередніх виправлень — сідер писав у дефолтний ai_one, бо ENV у PowerShell не застосувався, а код читав NAMESPACE на імпорті. Додав --namespace, щоб уникнути пастки ENV/шела.
У процесі виявлено логічну похибку: при HIT в Redis телеметрія whale не вбудовувалась (умовний блок формував whale_embedded лише для MISS). Виправлено з мінімальним дифом без зміни контрактів.
Додана цільова діагностика ключів [STRICT_WHALE] Redis MISS key=... ns=... db=... — допоможе швидко ловити розсинхрони namespace/DB/TTL надалі.
Що далі
Бажаєте, запущу швидку перевірку сигналів:
Можу виконати tools.signals_truth на поточному логауті (потрібно підтвердити шлях до out_dir/run.log, який створює shim).
За потреби — залишити app.whale_worker у фоні для постійного оновлення або додати tasks.json для швидкого старту воркера з VS Code.
Після стабілізації — знизити рівень HIT‑логу до DEBUG, щоб уникати шуму у прод.
Підсумок
Whale‑ключі тепер у правильному namespace; підтверджені значення presence/bias/vdev у Redis і успішне зчитування у пайплайні (HIT).
Виправлено критичний дефект, що блокував вбудовування на HIT.
Запущено фонового воркера для оновлення телеметрії.
Усі quality‑гейти зелені.


    """
    try:
        from config.config import (
            NAMESPACE as CFG_NS,  # lazy import для коректної _apply_sets
        )
    except Exception:
        CFG_NS = cfg.namespace  # fallback без перевизначення
    if str(cfg.namespace).strip(":") != str(CFG_NS).strip(":"):
        logger.info(
            "[Launch] Namespace override активний: %s → %s (ENV/flags)",
            cfg.namespace,
            CFG_NS,
        )
        cfg.namespace = str(CFG_NS).strip(":")
    logger.info(
        "[Launch] datastore.yaml loaded: namespace=%s base_dir=%s",
        cfg.namespace,
        cfg.base_dir,
    )
    # Використовуємо значення з app.settings (підтримує .env через pydantic-settings)
    redis = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
    )
    logger.info(
        "[Launch] Redis client created host=%s port=%s",
        settings.redis_host,
        settings.redis_port,
    )
    # Швидка перевірка доступності Redis; якщо повільна відповідь/тимчасова недоступність — не відрізаємо повністю I/O
    # Даємо більше часу на ping на Windows/Docker та залишаємо хоча б 1 спробу I/O навіть при збої ping
    redis_available = True
    try:
        await asyncio.wait_for(redis.ping(), timeout=2.0)
    except Exception:
        redis_available = False
        logger.warning(
            "[Launch] Redis недоступний або повільний ping — тимчасово працюємо з обмеженим I/O (best-effort)"
        )
    # Pydantic v2: use model_dump(); fallback to dict() for backward compat
    try:
        profile_data = cfg.profile.model_dump()
    except Exception:
        profile_data = cfg.profile.dict()
    # Параметри IO-ретраїв: якщо Redis недоступний/повільний — не вимикаємо повністю I/O,
    # залишаємо хоча б 1 швидку спробу (щоб уникнути постійних дефолтів при короткочасних збоях ping)
    io_attempts = cfg.io_retry_attempts if redis_available else 1
    io_backoff = cfg.io_retry_backoff if redis_available else 0.0

    # Ініціалізація UnifiedDataStore
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
    store = UnifiedDataStore(redis=redis, cfg=store_cfg)
    await store.start_maintenance()
    logger.info("[Launch] UnifiedDataStore maintenance loop started")
    # adapters removed – use store directly
    # Admin subsystem повністю вимкнено
    logger.info("[Launch] Admin command loop disabled (removed)")
    return store


def launch_ui_consumer() -> None:
    """Запускає `UI.ui_consumer_entry` у новому терміналі (Windows / *nix)."""
    proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if sys.platform.startswith("win"):
        subprocess.Popen(
            ["start", "cmd", "/k", "python", "-m", "UI.ui_consumer_entry"],
            shell=True,
            cwd=proj_root,  # запуск з кореня проекту, щоб UI бачився як модуль
        )
    else:
        # In headless environments (WSL, CI) gnome-terminal may be missing.
        # Check availability and avoid raising FileNotFoundError.
        term = shutil.which("gnome-terminal")
        if not term:
            logger.info(
                "UI consumer terminal not available (gnome-terminal not found); skipping launch."
            )
            return
        try:
            subprocess.Popen(
                [term, "--", "python3", "-m", "UI.ui_consumer_entry"], cwd=proj_root
            )
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("Не вдалося запустити UI consumer: %s", e)


def validate_settings() -> None:
    """Перевіряє необхідні змінні середовища (Redis + Binance ключі)."""
    missing: list[str] = []
    if not os.getenv("REDIS_URL"):
        if not settings.redis_host:
            missing.append("REDIS_HOST")
        if not settings.redis_port:
            missing.append("REDIS_PORT")

    if not settings.binance_api_key:
        missing.append("BINANCE_API_KEY")
    if not settings.binance_secret_key:
        missing.append("BINANCE_SECRET_KEY")

    if missing:
        raise ValueError(f"Відсутні налаштування: {', '.join(missing)}")

    logger.info("Налаштування перевірено — OK.")


# Legacy init_system removed (UnifiedDataStore handles Redis connection)


async def noop_healthcheck() -> None:
    """Легкий healthcheck-плейсхолдер (RAMBuffer видалено)."""
    while True:
        await asyncio.sleep(120)


async def run_pipeline() -> None:
    """
    Основний асинхронний цикл застосунку (оркестрація компонентів).

    Коротко:
      - Ініціалізує datastore та HTTP-сесію
      - Отримує/формує список швидких символів (manual або auto-prefilter)
      - Підвантажує історію 1m / денні рівні, ініціалізує менеджери стану
      - Запускає WS воркера, продюсер скринінгу, публікацію метрик та періодичний префільтр
      - Обробляє коректне завершення та очищення ресурсів

    TODOs (короткий чекліст):
      - [TODO] Додати feature-flag для запуску UIConsumer / screening_producer
      - [TODO] Перевірити TTL при записі fast_symbols (через config)
      - [TODO] Лімітувати concurency WS/Screening у разі великої кількості символів
      - [TODO] Додати метрики latency для кожного таска (avg_wall_ms)
    """
    # bootstrap datastore і базова валідація
    ds = await bootstrap()

    # Копія порогів для префільтра — робимо локальну зміну без модифікації глобальних констант
    thresholds = STAGE1_PREFILTER_THRESHOLDS.copy()

    # Redis з'єднання для публікацій (decode_responses для читабельності JSON у redis)
    redis_conn = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        decode_responses=True,
        encoding="utf-8",
    )

    # Якщо datastore має роботу з Redis — запускаємо UI в окремому терміналі (Windows/*nix)
    if getattr(ds.cfg, "io_retry_attempts", 0) > 0:
        launch_ui_consumer()

    # HTTP сесія для зовнішніх запитів (наприклад, API для історії)
    session = aiohttp.ClientSession()
    tasks_to_run: list[asyncio.Task[object]] = []

    try:
        # Прапорець: чи використовувати ручний список символів (seed + Redis overrides)
        # TODO: зробити це опцією через config/feature-flag
        # use_manual_list = False
        use_manual_list = True

        # Отримуємо ручні оверрайди з UnifiedDataStore (якщо метод доступний)
        try:
            manual_overrides = await ds.get_manual_fast_symbols()
        except AttributeError:
            manual_overrides = []  # backward-compatibility: метод може відсутній

        # Seed з конфігу + оверрайди з Redis → унікальний та відсортований список
        manual_seed_cfg = MANUAL_FAST_SYMBOLS_SEED
        manual_cfg_set = {str(sym).lower() for sym in manual_seed_cfg if sym}
        manual_override_set = {str(sym).lower() for sym in manual_overrides if sym}
        manual_union = sorted(manual_cfg_set | manual_override_set)

        # Логування доданих ручних символів (якщо є нові)
        if manual_override_set - manual_cfg_set:
            extra_overrides = sorted(list(manual_override_set - manual_cfg_set))
            logger.info(
                "[Main] Додані ручні символи з Redis: %s (усього %d)",
                extra_overrides[:10],
                len(extra_overrides),
            )
        if manual_union:
            logger.info(
                "[Main] Активний ручний whitelist: %s (усього %d)",
                manual_union[:10],
                len(manual_union),
            )

        # Якщо вирішили використовувати ручний список — виставляємо його у datastore
        if use_manual_list:
            fast_symbols = manual_union.copy()
            # TTL для ручного списку з config (FAST_SYMBOLS_TTL_MANUAL)
            await ds.set_fast_symbols(fast_symbols, ttl=FAST_SYMBOLS_TTL_MANUAL)
            logger.info(f"[Main] Використовуємо ручний список символів: {fast_symbols}")
        else:
            # Автоматичний первинний префільтр
            logger.info("[Main] Запускаємо первинний префільтр...")
            fast_symbols = await get_filtered_assets(
                session=session,
                cache_handler=ds,
                min_quote_vol=thresholds["MIN_QUOTE_VOLUME"],
                min_price_change=thresholds["MIN_PRICE_CHANGE"],
                min_oi=thresholds["MIN_OPEN_INTEREST"],
                min_depth=float(PREFILTER_BASE_PARAMS["min_depth"]),
                min_atr=float(PREFILTER_BASE_PARAMS["min_atr"]),
                max_symbols=int(thresholds["MAX_SYMBOLS"]),
                dynamic=bool(PREFILTER_BASE_PARAMS["dynamic"]),
            )

            # Зливаємо результати префільтра з ручним seed/overrides (консервативна операція)
            fast_symbols, manual_added = merge_prefilter_with_manual(
                fast_symbols or [], manual_seed_cfg, manual_overrides
            )
            # Записуємо auto-list у datastore з TTL з конфига
            await ds.set_fast_symbols(fast_symbols, ttl=FAST_SYMBOLS_TTL_AUTO)
            if manual_added:
                logger.info(
                    "[Main] Додано ручні символи до префільтра: %s",
                    sorted(list(manual_added)),
                )
            logger.info(f"[Main] Первинний префільтр: {len(fast_symbols)} символів")

        # Читаємо остаточний перелік fast_symbols (гарантія, що записувалися через ds)
        fast_symbols = await ds.get_fast_symbols()
        if not fast_symbols:
            logger.error("[Main] Не вдалося отримати список символів. Завершення.")
            return

        logger.info(
            "[Main] Початковий список символів: %s (кількість: %s)",
            fast_symbols,
            len(fast_symbols),
        )

        # Preload 1m history та денних рівнів — потрібні для accurate level detection
        await preload_1m_history(
            fast_symbols, ds, lookback=PRELOAD_1M_LOOKBACK_INIT, session=session
        )
        daily_data = await preload_daily_levels(
            fast_symbols, ds, days=PRELOAD_DAILY_DAYS, session=session
        )

        # Ініціалізація менеджера станів активів (внутрішній кеш станів)
        assets_current = [s.lower() for s in fast_symbols]
        state_manager = AssetStateManager(assets_current)
        try:
            # Підключаємо datastore як кеш-хендлер для state_manager (якщо підтримується)
            state_manager.set_cache_handler(ds)
        except Exception:
            # Невелика терпимість — state_manager може працювати і без ds
            pass

        # Ініціалізація монітора Stage1 з конфігурою порогів/параметрів
        logger.info("[Main] Ініціалізуємо AssetMonitorStage1...")
        monitor = AssetMonitorStage1(
            cache_handler=ds,
            state_manager=state_manager,
            feature_switches={},
            vol_z_threshold=float(STAGE1_MONITOR_PARAMS.get("vol_z_threshold", 2.0)),
            rsi_overbought=STAGE1_MONITOR_PARAMS.get("rsi_overbought"),
            rsi_oversold=STAGE1_MONITOR_PARAMS.get("rsi_oversold"),
            min_reasons_for_alert=int(
                STAGE1_MONITOR_PARAMS.get("min_reasons_for_alert", 2)
            ),
            dynamic_rsi_multiplier=float(
                STAGE1_MONITOR_PARAMS.get("dynamic_rsi_multiplier", 1.1)
            ),
        )

        # Передамо денні рівні в монітор (щоб мати базу для global levels)
        for sym, df in daily_data.items():
            try:
                monitor.global_levels[sym] = calculate_global_levels(df, window=20)
            except Exception as exc:
                # DEBUG: не критично, пропускаємо символ і продовжуємо
                logger.debug(
                    "[Main] Не вдалося оновити денні рівні для %s: %s", sym, exc
                )

        # Ініціалізація LevelManager та підсадка денних рівнів (як початковий стан)
        level_manager = LevelManager()
        try:
            for sym, levels in getattr(monitor, "global_levels", {}).items():
                if isinstance(levels, list) and levels:
                    level_manager.set_daily_levels(sym, levels)
        except Exception:
            logger.debug(
                "[Main] Ініціалізація LevelManager денними рівнями пропущена",
                exc_info=True,
            )

        # Для кожного символу оновлюємо intraday-рівні, atr_pct, tick_size та заповнюємо state_manager.meta
        for sym in fast_symbols:
            try:
                df_1m = await store_to_dataframe(ds, sym, limit=500)
            except Exception:
                df_1m = None
            if df_1m is None or df_1m.empty:
                # Якщо немає достатньої історії — пропускаємо
                continue
            # Оновлення intraday-рівнів у LevelManager
            try:
                level_manager.set_intraday_levels(sym, df_1m, window=20)
            except Exception:
                logger.debug(
                    "[Main] set_intraday_levels(%s) failed", sym, exc_info=True
                )
            try:
                price_hint = float(df_1m["close"].iloc[-1])
            except Exception:
                price_hint = None
            atr_pct = estimate_atr_pct(df_1m)
            tick_size = get_tick_size(sym, price_hint=price_hint)
            # Оновлюємо метадані активу в state_manager (для UI/дальшої логіки)
            state_manager.update_asset(
                sym.lower(),
                {
                    "meta": {
                        "atr_pct": atr_pct,
                        "tick_size": tick_size,
                    }
                },
            )

        # Попереднє завантаження специфічних порогів (TOP100) у монітор
        for sym, cfg in TOP100_THRESHOLDS.items():
            monitor._symbol_cfg[sym] = Thresholds(symbol=sym, config=cfg)
        logger.info("[Main] TOP100_THRESHOLDS предзавантаження успішне.")

        # Підвісимо монітор у datastore для доступу іншими компонентами (best-effort)
        try:
            ds.stage1_monitor = monitor  # type: ignore[attr-defined]
        except Exception:
            pass

        # Запуск WebSocket воркера для стрімінгу даних і health-check loop
        ws_worker = WSWorker(
            fast_symbols,
            store=ds,
            selectors_key="selectors:fast_symbols",
            intervals_ttl={"1m": 90, "1h": 65 * 60},
        )
        ws_task = asyncio.create_task(ws_worker.consume())
        health_task = asyncio.create_task(noop_healthcheck())

        # Вбудований паблішер метрик для UI (публікує snapshot у канал ui.metrics)
        async def ui_metrics_publisher() -> None:
            channel = "ui.metrics"
            while True:
                snap = ds.metrics_snapshot()
                try:
                    # Спроба отримати "гарячі" символи з LRU RAM cache (необов'язково)
                    lru = getattr(getattr(ds, "ram", None), "_lru", None)
                    if lru is not None:
                        hot_symbols = list({s for (s, _i) in lru.keys()})
                        snap["hot_symbols"] = len(hot_symbols)
                    else:
                        snap["hot_symbols"] = None
                except Exception:
                    snap["hot_symbols"] = None
                try:
                    # Публікація у Redis (якщо Redis доступний)
                    redis_pub = getattr(getattr(ds, "redis", None), "r", None)
                    if redis_pub is not None:
                        await redis_pub.publish(channel, json.dumps(snap))
                except Exception as exc:
                    # DEBUG: не ламаємо головний цикл через невдалу публікацію метрик
                    logger.debug("ui_metrics publish failed: %s", exc)
                await asyncio.sleep(5)

        metrics_task: asyncio.Task[None] | None = None
        if getattr(ds.cfg, "io_retry_attempts", 0) > 0:
            # Якщо datastore має Redis, запускаємо публікацію метрик та UI consumer
            metrics_task = asyncio.create_task(ui_metrics_publisher())
            logger.info("[Main] Ініціалізуємо UI-споживача...")
            UIConsumer()
        else:
            logger.info("[Main] Пропускаємо UI (RAM-only режим)")

        # Чи дозволений реактивний режим (якщо увімкнено — screening_producer не запускаємо)
        try:
            reactive_enabled = bool(REACTIVE_STAGE1)
        except Exception:
            reactive_enabled = False

        # Запуск Screening Producer (якщо не реактивний режим)
        prod: asyncio.Task[object] | None = None
        if not reactive_enabled:
            logger.info("[Main] Запускаємо Screening Producer...")
            prod = asyncio.create_task(
                screening_producer(
                    monitor=monitor,
                    store=ds,
                    store_fast_symbols=ds,
                    assets=fast_symbols,
                    redis_conn=redis_conn,
                    timeframe="1m",
                    lookback=SCREENING_LOOKBACK,
                    interval_sec=12,
                    reference_symbol=settings.reference_symbol,
                    state_manager=state_manager,
                    level_manager=level_manager,
                )
            )

        # Публікуємо початковий стан у Redis / UI — потрібен для візуалізації старту системи
        logger.info("[Main] Публікуємо початковий стан в Redis...")
        await publish_full_state(state_manager, ds, redis_conn)

        # Запуск періодичного префільтра (якщо використовується авто-режим)
        prefilter_task: asyncio.Task[object] | None = None
        if not use_manual_list:
            prefilter_task = asyncio.create_task(
                periodic_prefilter_and_update(
                    ds,
                    session,
                    thresholds,
                    interval=PREFILTER_INTERVAL_SEC,
                    buffer=ds,
                )
            )

        # Формуємо список тасків для await (гарантуємо, що всі критичні таски запущені)
        tasks_to_run = [ws_task, health_task]
        if metrics_task is not None:
            tasks_to_run.append(metrics_task)
        if prod is not None:
            tasks_to_run.append(prod)
        if prefilter_task is not None:
            tasks_to_run.append(prefilter_task)

        try:
            from app.whale_worker import run_whale_worker

            logger.info("[Main] Запускаємо WhaleWorker у фоні")
            tasks_to_run.append(asyncio.create_task(run_whale_worker()))
        except Exception:
            logger.warning("[Main] Не вдалося запустити WhaleWorker", exc_info=True)

        # ───────────────────── Dry‑run тюнер семафорів (під фіче‑флагом) ─────────────────────
        tuner_task: asyncio.Task[None] | None = None
        if bool(DYNAMIC_SEMAPHORE_TUNING):
            logger.info(
                "[Main] Увімкнено DYNAMIC_SEMAPHORE_TUNING (dry‑run supervisor)"
            )

            async def _tuner_supervisor() -> None:
                """Стежить за телеметрією та за підозри запускає 5‑хв сесію тюнера.

                Умови запуску сесії (приклад під наш сценарій):
                  - є хоча б один 429 у вікні 60с; або
                  - p90 латентності ≥ 300мс в будь‑якому домені.

                Сесія: тюнер викликається щохвилини протягом 5 хвилин (run_periodic(60)).
                Після завершення — мінімальна пауза 60с перед можливим наступним запуском.
                """

                tuner = SemaphoreTuner()
                active_task: asyncio.Task[None] | None = None
                last_end: float = 0.0
                session_len_sec = 300  # 5 хвилин
                check_interval_sec = 30

                while True:
                    try:
                        snap = get_hub().snapshot()
                        suspicious = False
                        for payload in snap.values():
                            lat = payload.get("latency", {})
                            p90 = float(lat.get("p90") or 0.0)
                            c429 = int(payload.get("count_429") or 0)
                            if c429 > 0 or p90 >= 300.0:
                                suspicious = True
                                break

                        now = time.monotonic()
                        if (
                            suspicious
                            and active_task is None
                            and (now - last_end) >= 60.0
                        ):

                            async def _session() -> None:
                                task = asyncio.create_task(
                                    tuner.run_periodic(interval_sec=60)
                                )
                                try:
                                    await asyncio.sleep(session_len_sec)
                                finally:
                                    task.cancel()
                                    try:
                                        await asyncio.gather(
                                            task, return_exceptions=True
                                        )
                                    except Exception:
                                        pass

                            active_task = asyncio.create_task(_session())

                        if active_task and active_task.done():
                            last_end = time.monotonic()
                            active_task = None
                    except Exception:
                        logger.debug("[Main] tuner_supervisor error", exc_info=True)
                    await asyncio.sleep(check_interval_sec)

            tuner_task = asyncio.create_task(_tuner_supervisor())
            tasks_to_run.append(tuner_task)

        # Очікуємо завершення всіх завдань (в нормі вони працюють вічно)
        await asyncio.gather(*tasks_to_run)
    except asyncio.CancelledError:
        # Коректне завершення при cancel (наприклад SIGINT)
        logger.info("[Main] Завершення за скасуванням (CancelledError)")
        raise
    except Exception:
        # Логування повного стека для несподіваних помилок — важливо для RCA
        logger.error("[Main] run_pipeline error", exc_info=True)
    finally:
        # Graceful shutdown: скасовуємо незавершені таски та закриваємо ресурси
        to_cancel: list[asyncio.Task[object]] = []
        for task in tasks_to_run:
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
                to_cancel.append(task)
        if to_cancel:
            # Return_exceptions=True щоб дозволити всім таскам завершитись і не піднімати нові ексепшени
            await asyncio.gather(*to_cancel, return_exceptions=True)

        # Закриваємо aiohttp session (best-effort)
        try:
            await session.close()
        except Exception as exc:
            logger.debug("[Main] aiohttp session close failed: %s", exc)


# (metrics endpoint видалено разом із FastAPI роутингом)


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        # М'яке завершення без стека трейсів
        logger.info(
            "Зупинено користувачем (KeyboardInterrupt). Завершення без помилок."
        )
    except Exception as e:
        logger.error("Помилка виконання: %s", e, exc_info=True)
        sys.exit(1)
