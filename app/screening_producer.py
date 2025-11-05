"""Stage1 стан публікатор (Stage2 вимкнено).

Цей модуль зберігає лише Stage1 виявлення аномалій та публікацію стану. Застарілі
Stage2/Stage3 пайплайни були видалені для спрощення runtime footprint.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.logging import RichHandler

from app.settings import settings
from app.utils.helper import estimate_atr_pct, store_to_dataframe
from config.config import (
    DEFAULT_LOOKBACK,
    DEFAULT_TIMEFRAME,
    K_SIGNAL,
    K_STATS,
    MIN_READY_PCT,
    PREFILTER_STRICT_PROFILES,
    SCREENING_BATCH_SIZE,
    SCREENING_LEVELS_UPDATE_EVERY,
    STAGE1_SYMBOL_SNAPSHOT_ALERTS_ONLY,
    STAGE1_SYMBOL_SNAPSHOT_ENABLED,
    STAGE1_SYMBOL_SNAPSHOT_INTERVAL_SEC,
    TRADE_REFRESH_INTERVAL,
)
from config.config import (
    NAMESPACE as _NS,
)
from config.flags import (
    STAGE1_MONITOR_LIMIT_TO_STRICT_PREFILTER as _USE_STRICT_FILTER,
)
from config.flags import (
    STAGE1_PREFILTER_STRICT_AUTHORITATIVE_IF_PRESENT as _AUTHORITATIVE,
)
from config.flags import (
    STAGE1_PREFILTER_STRICT_BG_INTERVAL_SEC as _BG_INTERVAL,
)
from config.flags import (
    STAGE1_PREFILTER_STRICT_BG_REFRESH_ENABLED as _BG_ENABLED,
)
from config.flags import (
    STAGE1_PREFILTER_STRICT_ENABLED as _STRICT_ENABLED,
)
from config.flags import (
    STAGE1_PREFILTER_STRICT_SOFT_PROFILE as _SOFT_PROFILE,
)
from config.keys import build_key as _k
from monitoring.telemetry_sink import log_stage1_event, log_stage1_latency
from stage1.asset_monitoring import AssetMonitorStage1
from stage1.prefilter_strict import StrictThresholds, prefilter_symbols
from UI.publish_full_state import RedisLike, publish_full_state
from utils.utils import create_no_data_signal, get_tick_size

from .asset_state_manager import AssetStateManager
from .process_asset_batch import ProcessAssetBatchv1

if TYPE_CHECKING:  # pragma: no cover - type hints only
    from data.unified_store import UnifiedDataStore


logger = logging.getLogger("app.screening_producer")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

    async def screening_producer(
        monitor: AssetMonitorStage1,
        store: UnifiedDataStore,
        store_fast_symbols: UnifiedDataStore,
        assets: list[str],
        redis_conn: RedisLike,
        *,
        reference_symbol: str = settings.reference_symbol,  # за дефолтом BTCUSDT
        timeframe: str = DEFAULT_TIMEFRAME,
        lookback: int = DEFAULT_LOOKBACK,
        interval_sec: int = TRADE_REFRESH_INTERVAL,
        min_ready_pct: float = MIN_READY_PCT,
        state_manager: AssetStateManager | None = None,
        level_manager: Any | None = None,
        enable_stage2: bool = False,
    ) -> None:
        """
        Основний цикл Stage1: динамічне оновлення whitelist активів, перевірка готовності даних, оновлення рівнів, батч-обробка сигналів (Stage1 anomaly/whale/hints/phase), публікація повного стану у Redis.

        Args:
            monitor (AssetMonitorStage1): Монітор для виявлення аномалій (Stage1).
            store (UnifiedDataStore): Сховище історичних даних (Redis/локально).
            store_fast_symbols (UnifiedDataStore): Джерело whitelist fast_symbols.
            assets (list[str]): Початковий список активів.
            redis_conn (RedisLike): З'єднання з Redis для публікації стану.
            reference_symbol (str, optional): Базовий символ для контролю (default: "BTCUSDT").
            timeframe (str, optional): Таймфрейм для аналізу (default: з config).
            lookback (int, optional): Глибина історії (default: з config).
            interval_sec (int, optional): Інтервал оновлення (default: з config).
            min_ready_pct (float, optional): Мінімальна частка готових активів для запуску (default: з config).
            state_manager (AssetStateManager, optional): Менеджер стану активів.
            level_manager (Any, optional): Менеджер рівнів (ATR/tick/meta).
            enable_stage2 (bool, optional): Чи вмикати Stage2 (ігнорується, лише лог warning).

        Мікро-контракт:
            - Динамічно оновлює whitelist fast_symbols.
            - Чекає, поки достатньо активів мають історію (min_ready_pct).
            - Оновлює ATR/tick/meta та рівні (через state_manager/level_manager).
            - Обробляє активи батчами через process_asset_batch (Stage1 anomaly, whale, hints, phase).
            - Публікує повний стан у Redis (publish_full_state).
            - Логи KPI та контроль часу циклу.

        Примітки:
            - Контракти Stage1Signal не змінюються.
            - Stage2/Stage3 не використовуються (enable_stage2 лише для сумісності).
            - Всі ключі та TTL — через config/config.py.
            - Логи: [KPI], [STATE], [STRICT_*] для діагностики.
        """
        logger.info(
            "🚀 Старт screening_producer (Stage1-only): %d активів, таймфрейм %s, lookback %d",
            len(assets),
            timeframe,
            lookback,
        )
        if enable_stage2:
            logger.info("[Stage2] Запит вимкнено: Stage2 недоступний у цьому режимі")

        _last_levels_update_ts: dict[str, int] = {}

        # ── Запуск фонового оновлення строгого пре‑фільтра (best‑effort) ──
        bg_prefilter_task: asyncio.Task[Any] | None = None
        try:
            if _BG_ENABLED and _STRICT_ENABLED:

                async def _bg_strict_prefilter() -> None:
                    while True:
                        try:
                            # Використовуємо останні snapshots з монітора; якщо порожні — все одно публікуємо (порожній whitelist)
                            snapshots = list(
                                getattr(monitor, "asset_stats", {}).values()
                            )
                            if not snapshots:
                                # Bootstrap-режим: якщо ще немає snapshot'ів — заповнимо строгий whitelist
                                # за fast_symbols або канарейками, щоб уникнути deadlock'у на холодному старті.
                                try:
                                    syms = await store_fast_symbols.get_fast_symbols()
                                except Exception:
                                    syms = None
                                sym_list = [str(s).lower() for s in (syms or [])]
                                if not sym_list:
                                    # Мінімальні канарейки
                                    sym_list = [
                                        settings.reference_symbol.lower(),
                                        "ethusdt",
                                    ]
                                payload = {
                                    "ts": int(time.time() * 1000),
                                    "top_k": len(sym_list),
                                    "items": [
                                        {
                                            "symbol": s,
                                            "score": 0.0,
                                            "lane": "bootstrap",
                                            "card": {},
                                        }
                                        for s in sym_list
                                    ],
                                }
                                try:
                                    prefilter_key = _k(
                                        _NS, "prefilter", extra=("strict", "list")
                                    )
                                    jset = getattr(
                                        getattr(store, "redis", None), "jset", None
                                    )
                                    if callable(jset):
                                        await jset(
                                            prefilter_key,
                                            value=payload,
                                            ttl=max(10, int(_BG_INTERVAL or 12)),
                                        )
                                        logger.info(
                                            "[STRICT_STATE] Bootstrap strict whitelist опубліковано (n=%d)",
                                            len(sym_list),
                                        )
                                except Exception:
                                    logger.debug(
                                        "[PREFILTER_STRICT_BG] bootstrap publish failed",
                                        exc_info=True,
                                    )
                                # Чекаємо інтервал і переходимо до наступного циклу
                                await asyncio.sleep(max(3, int(_BG_INTERVAL or 12)))
                                continue
                            profile_name = "soft" if _SOFT_PROFILE else "default"
                            profile = PREFILTER_STRICT_PROFILES.get(profile_name, {})
                            thresholds = (
                                StrictThresholds(**profile)
                                if profile
                                else StrictThresholds()
                            )
                            # Єдиний уніфікований Redis — через UnifiedDataStore.redis
                            redis_cli = getattr(store, "redis", None)
                            prefilter_symbols(
                                snapshots,
                                thresholds,
                                redis_client=redis_cli,
                            )
                        except Exception:
                            logger.debug(
                                "[PREFILTER_STRICT_BG] refresh failed", exc_info=True
                            )
                        # Інтервал між оновленнями
                        await asyncio.sleep(max(3, int(_BG_INTERVAL or 12)))

                bg_prefilter_task = asyncio.create_task(_bg_strict_prefilter())
                logger.info(
                    "[PREFILTER_STRICT_BG] Фоновий рефреш пре‑фільтра увімкнено"
                )
        except Exception:
            logger.debug("[PREFILTER_STRICT_BG] init failed", exc_info=True)

        if state_manager is None:
            assets_current = [s.lower() for s in (assets or [])]
            state_manager = AssetStateManager(assets_current)
        else:
            assets_current = list(state_manager.state.keys())

        for sym in list(assets_current):
            state_manager.init_asset(sym)

        ref_symbol = reference_symbol.lower()
        if ref_symbol not in state_manager.state:
            state_manager.init_asset(ref_symbol)

        _last_symbol_snapshot_ts: float = 0.0
        while True:
            start_time = time.time()

            try:
                new_assets_raw = await store_fast_symbols.get_fast_symbols()
            except Exception as exc:
                logger.error(
                    "[STRICT_STATE] reference=%s: Помилка оновлення whitelist fast_symbols: %s",
                    reference_symbol,
                    exc,
                )
                new_assets_raw = None

            if new_assets_raw:
                new_assets = [s.lower() for s in new_assets_raw]
                current_set = set(assets_current)
                new_set = set(new_assets)
                added = new_set - current_set
                removed = current_set - new_set
                for s in added:
                    state_manager.init_asset(s)
                assets_current = list(new_set)
                for s in removed:
                    state_manager.state.pop(s, None)
                if added or removed:
                    logger.info(
                        "[STRICT_STATE] reference=%s: 🔄 Оновлено список активів: +%d/-%d (усього: %d)",
                        reference_symbol,
                        len(added),
                        len(removed),
                        len(assets_current),
                    )

            # РАННІЙ гейт: обмежити сам перелік активів за суворим пре‑фільтром ДО будь-яких get_df
            try:
                if _USE_STRICT_FILTER and assets_current:
                    prefilter_key = _k(_NS, "prefilter", extra=("strict", "list"))
                    strict_list: set[str] | None = None
                    have_payload: bool = False
                    try:
                        # UnifiedDataStore.redis — це RedisAdapter; реальний клієнт під атрибутом .r
                        getter = getattr(getattr(store, "redis", None), "r", None)
                        getter = getattr(getter, "get", None)
                        if callable(getter):
                            raw = await getter(prefilter_key)
                            if isinstance(raw, (bytes, bytearray)):
                                raw = raw.decode("utf-8", errors="ignore")
                            if isinstance(raw, str) and raw:
                                import json as _json

                                have_payload = True
                                data = _json.loads(raw)
                                items = (
                                    data.get("items")
                                    if isinstance(data, dict)
                                    else None
                                )
                                if isinstance(items, list):
                                    strict_list = {
                                        str(it.get("symbol", "")).lower()
                                        for it in items
                                        if isinstance(it, dict)
                                    }
                    except Exception:
                        logger.debug(
                            "[STRICT_STATE] Не вдалося отримати список строгого пре‑фільтра (ранній гейт)",
                            exc_info=True,
                        )

                    # Якщо є payload у Redis — дотримуємось його навіть якщо список порожній (за флагом)
                    if have_payload and strict_list is not None and _AUTHORITATIVE:
                        before_total = len(assets_current)
                        assets_current = [s for s in assets_current if s in strict_list]
                        after_total = len(assets_current)
                        logger.info(
                            "[STRICT_STATE] Ранній гейт пре‑фільтром: %d→%d символів (до get_df)",
                            before_total,
                            after_total,
                        )
                    elif strict_list:
                        before_total = len(assets_current)
                        assets_current = [s for s in assets_current if s in strict_list]
                        after_total = len(assets_current)
                        if after_total < before_total:
                            logger.info(
                                "[STRICT_STATE] Ранній гейт пре‑фільтром: %d→%d символів (до get_df)",
                                before_total,
                                after_total,
                            )
            except Exception:
                logger.debug(
                    "[STRICT_STATE] early strict prefilter gating failed", exc_info=True
                )

            ready_assets: list[str] = []
            for s in assets_current:
                try:
                    df_tmp = await store.get_df(s, timeframe, limit=lookback)
                except Exception as exc:
                    logger.debug(
                        "[STRICT_STATE] symbol=%s: Помилка отримання df: %s", s, exc
                    )
                    continue
                if df_tmp is not None and not df_tmp.empty and len(df_tmp) >= lookback:
                    ready_assets.append(s)

            # Додатковий гейт: обмежити моніторинг символами зі строгого пре‑фільтра
            try:
                if _USE_STRICT_FILTER:
                    # Ключ суворого пре‑фільтра: ai_one:prefilter:strict:list
                    prefilter_key = _k(_NS, "prefilter", extra=("strict", "list"))
                    strict_list: set[str] | None = None
                    have_payload: bool = False
                    try:
                        # UnifiedDataStore.redis — це RedisAdapter; реальний клієнт під атрибутом .r
                        getter = getattr(getattr(store, "redis", None), "r", None)
                        getter = getattr(getter, "get", None)
                        if callable(getter):
                            raw = await getter(prefilter_key)
                            if isinstance(raw, (bytes, bytearray)):
                                raw = raw.decode("utf-8", errors="ignore")
                            if isinstance(raw, str) and raw:
                                import json as _json

                                have_payload = True
                                data = _json.loads(raw)
                                items = (
                                    data.get("items")
                                    if isinstance(data, dict)
                                    else None
                                )
                                if isinstance(items, list):
                                    strict_list = {
                                        str(it.get("symbol", "")).lower()
                                        for it in items
                                        if isinstance(it, dict)
                                    }
                    except Exception:
                        logger.debug(
                            "[STRICT_STATE] Не вдалося отримати список строгого пре‑фільтра з Redis",
                            exc_info=True,
                        )

                    # Якщо payload є — застосовуємо навіть порожній список (за флагом)
                    if have_payload and strict_list is not None and _AUTHORITATIVE:
                        before = len(ready_assets)
                        ready_assets = [s for s in ready_assets if s in strict_list]
                        after = len(ready_assets)
                        logger.info(
                            "[STRICT_STATE] Застосовано строгий пре‑фільтр: %d→%d символів",
                            before,
                            after,
                        )
                    elif strict_list:
                        before = len(ready_assets)
                        ready_assets = [s for s in ready_assets if s in strict_list]
                        after = len(ready_assets)
                        logger.info(
                            "[STRICT_STATE] Застосовано строгий пре‑фільтр: %d→%d символів",
                            before,
                            after,
                        )
            except Exception:
                logger.debug(
                    "[STRICT_STATE] strict prefilter gating failed", exc_info=True
                )

            ready_count = len(ready_assets)
            min_ready = max(1, int(len(assets_current) * min_ready_pct))
            if ready_count < min_ready:
                logger.warning(
                    "[STRICT_STATE] reference=%s: ⏳ Недостатньо даних: %d/%d активів готові. Очікування %d сек...",
                    reference_symbol,
                    ready_count,
                    len(assets_current),
                    interval_sec,
                )
                try:
                    not_ready = [s for s in assets_current if s not in ready_assets]
                    for s in not_ready:
                        state_manager.update_asset(s, create_no_data_signal(s))
                    if not_ready:
                        await publish_full_state(state_manager, store, redis_conn)
                        logger.info(
                            "[STRICT_STATE] reference=%s: Оновлено NO_DATA для %d активів",
                            reference_symbol,
                            len(not_ready),
                        )
                except Exception as exc:
                    logger.error(
                        "[STRICT_STATE] reference=%s: Помилка під час оновлення NO_DATA: %s",
                        reference_symbol,
                        exc,
                    )
                await asyncio.sleep(interval_sec)
                continue

            # Оновлення рівнів (періодично)
            levels_update_every = int(SCREENING_LEVELS_UPDATE_EVERY or 25)
            now_ts = int(time.time())
            for s in ready_assets:
                last_ts = _last_levels_update_ts.get(s, 0)
                if (now_ts - last_ts) < levels_update_every:
                    continue
                df_1m = await store_to_dataframe(store, s, limit=500)
                if df_1m is None or df_1m.empty:
                    logger.debug(
                        "[STRICT_LEVELS] symbol=%s: df_1m порожній, пропуск оновлення meta",
                        s,
                    )
                    continue
                atr_pct = estimate_atr_pct(df_1m)
                try:
                    price_hint = float(df_1m["close"].iloc[-1])
                except Exception:
                    price_hint = None
                tick_size = get_tick_size(s, price_hint=price_hint)
                meta_update = {"meta": {"atr_pct": atr_pct, "tick_size": tick_size}}
                try:
                    state_manager.update_asset(s, meta_update)
                    logger.debug(
                        "[STRICT_LEVELS] symbol=%s: Оновлено meta atr_pct=%.4f tick_size=%s",
                        s,
                        atr_pct,
                        tick_size,
                    )
                except Exception:
                    logger.debug(
                        "[STRICT_LEVELS] symbol=%s: Не вдалося оновити meta",
                        s,
                        exc_info=True,
                    )
                if level_manager is not None:
                    try:
                        level_manager.update_meta(s, atr_pct=atr_pct, tick_size=tick_size)  # type: ignore[attr-defined]
                        logger.debug(
                            "[STRICT_LEVELS] symbol=%s: Оновлено meta у level_manager",
                            s,
                        )
                    except Exception:
                        logger.debug(
                            "[STRICT_LEVELS] symbol=%s: level_manager.update_meta() failed",
                            s,
                        )
                _last_levels_update_ts[s] = now_ts

            # Обробка батчами
            batch_size = max(1, int(SCREENING_BATCH_SIZE or 20))
            tasks: list[asyncio.Task[Any]] = []
            for i in range(0, len(ready_assets), batch_size):
                batch = ready_assets[i : i + batch_size]
                logger.info(
                    "[STRICT_BATCH] reference=%s: Обробка батчу %s",
                    reference_symbol,
                    batch,
                )
                tasks.append(
                    asyncio.create_task(
                        ProcessAssetBatchv1.process_asset_batch(
                            batch, monitor, store, timeframe, lookback, state_manager
                        )
                    )
                )
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=False)
                logger.info(
                    "[STRICT_BATCH] reference=%s: Завершено обробку %d батчів",
                    reference_symbol,
                    len(tasks),
                )

            # Публікація стану
            await publish_full_state(state_manager, store, redis_conn)
            logger.info(
                "[STRICT_STATE] reference=%s: Опубліковано повний стан у Redis",
                reference_symbol,
            )

            # ───────────────────── Періодичні Stage1 symbol_snapshot події ─────────────────────
            try:
                if STAGE1_SYMBOL_SNAPSHOT_ENABLED and (
                    (time.time() - _last_symbol_snapshot_ts)
                    >= int(STAGE1_SYMBOL_SNAPSHOT_INTERVAL_SEC or 30)
                ):
                    if bool(STAGE1_SYMBOL_SNAPSHOT_ALERTS_ONLY):
                        assets_for_snap = state_manager.get_alert_signals()
                    else:
                        assets_for_snap = list(state_manager.state.values())
                    for asset in assets_for_snap:
                        sym = str(
                            asset.get("symbol") or asset.get("Symbol") or ""
                        ).lower()
                        if not sym:
                            continue
                        payload = {
                            "signal": asset.get(K_SIGNAL),
                            "stats": asset.get(K_STATS),
                            "trigger_reasons": asset.get("trigger_reasons"),
                            "hints": asset.get("hints"),
                            "state": asset.get("state"),
                            "thresholds": asset.get("thresholds"),
                        }
                        try:
                            await log_stage1_event(
                                event="symbol_snapshot", symbol=sym, payload=payload
                            )
                        except Exception:
                            logger.debug("[TELEM] %s: symbol_snapshot log failed", sym)
                    _last_symbol_snapshot_ts = time.time()
            except Exception:
                logger.debug("[TELEM] symbol_snapshot loop failed", exc_info=True)

            # KPI та сон
            processing_time = time.time() - start_time
            sleep_time = (
                1
                if processing_time >= interval_sec
                else max(1, interval_sec - int(processing_time))
            )
            try:
                total_assets = len(state_manager.state)
                alert_signals = state_manager.get_alert_signals()
                alert_rate = (
                    (len(alert_signals) / max(1, ready_count)) if ready_count else 0.0
                )
                logger.info(
                    "[KPI] reference=%s: ready=%d/%d alerts=%d (rate=%.2f) avg_wall_ms=%.0f",
                    reference_symbol,
                    ready_count,
                    total_assets,
                    len(alert_signals),
                    alert_rate,
                    processing_time * 1000.0,
                )
                try:
                    await log_stage1_latency(
                        {
                            "reference": reference_symbol,
                            "ready_total": ready_count,
                            "assets_total": total_assets,
                            "alerts": len(alert_signals),
                            "alert_rate": alert_rate,
                            "processing_ms": processing_time * 1000.0,
                            "sleep_sec": sleep_time,
                        }
                    )
                except Exception:
                    logger.debug(
                        "[TELEM] %s: stage1 latency log failed",
                        reference_symbol,
                        exc_info=True,
                    )
            except Exception as exc:
                logger.debug(
                    "[KPI] reference=%s: Помилка KPI-логування: %s",
                    reference_symbol,
                    exc,
                )
            logger.info(
                "[STRICT_STATE] reference=%s: ⏳ Час обробки циклу: %.2f сек, очікування: %d сек",
                reference_symbol,
                processing_time,
                sleep_time,
            )
            await asyncio.sleep(sleep_time)

        # Завершення: прибрати фонову задачу рефрешу пре‑фільтра
        try:
            if bg_prefilter_task is not None:
                bg_prefilter_task.cancel()
                try:
                    await bg_prefilter_task
                except Exception:
                    pass
        except Exception:
            pass
