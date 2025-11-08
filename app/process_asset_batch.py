from __future__ import annotations

import logging
import os
import sys
import time
from collections import OrderedDict, deque
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import (
    CONTEXT_SNAPSHOTS_ENABLED,
    K_CUMULATIVE_DELTA,
    K_DIRECTIONAL_VOLUME_RATIO,
    K_PRICE_SLOPE_ATR,
    K_SIGNAL,
    K_STATS,
    NAMESPACE,
    SCEN_EXPLAIN_ENABLED,  # type: ignore
    SCEN_EXPLAIN_VERBOSE_EVERY_N,  # type: ignore
    STAGE1_TRAP,
    STAGE2_PROFILE,
    STAGE2_SIGNAL_V2_PROFILES,
    TELEM_ENRICH_PHASE_PAYLOAD,
)

# strict overrides/whitelist were previously used for heavy-compute gating; now unused
from config.flags import (
    SCENARIO_CANARY_PUBLISH_CANDIDATE,
    SCENARIO_TRACE_ENABLED,
    STAGE1_EXCLUDE_LOW_VOL_ATR_TRIGGERS,
    STAGE1_PREFILTER_STRICT_ENABLED,
    STAGE2_HINT_ENABLED,
    STAGE2_SIGNAL_V2_ENABLED,
    STAGE2_WHALE_EMBED_ENABLED,
    STRICT_SCENARIO_HYSTERESIS_ENABLED,
)
from monitoring.telemetry_sink import log_stage1_event
from process_asset_batch.global_state import (
    _CTX_MEMORY,
    _EDGE_DWELL,
    _EDGE_HIT_RING,
    _HINT_COOLDOWN_LAST_TS,
    _HYP_STATE,
    _LAST_PHASE,
    _PROFILE_STATE,
    _SCEN_ALERT_LAST_TS,
    _SCEN_EXPLAIN_BATCH_COUNTER,
    _SCEN_EXPLAIN_FORCE_ALL,
    _SCEN_EXPLAIN_LAST_TS,
    _SCEN_HIST_RING,
    _SCEN_LAST_STABLE,
    _SCEN_TRACE_LAST_TS,
    _SWEEP_LAST_SIDE,
    _SWEEP_LAST_TS_MS,
    _SWEEP_LEVEL,
    _SWEEP_WARN_SHORT_HISTORY,
    _WHALE_METRIC_RING,
)
from process_asset_batch.helpers import (
    active_alt_keys,
    compute_ctx_persist,
    emit_prom_presence,
    ensure_whale_default,
    explain_should_log,
    htf_adjust_alt_min,
    normalize_and_cap_dvr,
    read_profile_cooldown,
    score_scale_for_class,
    should_confirm_pre_breakout,
    update_accum_monitor,
    write_profile_cooldown,
)
from process_asset_batch.router_signal_v2 import router_signal_v2
from stage1.asset_monitoring import AssetMonitorStage1
from utils.phase_adapter import detect_phase_from_stats, resolve_scenario
from utils.utils import (
    create_error_signal,
    create_no_data_signal,
    normalize_result_types,
    sanitize_ohlcv_numeric,
)

# Локальний helper для вибірки last_h1 (мінімальний диф, без зміни контрактів)
try:  # pragma: no cover
    from process_asset_batch.helpers import (
        extract_last_h1 as _extract_last_h1,  # type: ignore
    )
except Exception:  # pragma: no cover
    _extract_last_h1 = None  # type: ignore[assignment]

from .asset_state_manager import AssetStateManager

# Policy v2 інтеграція (під прапором). Безпечний фолбек при відсутності модуля.
try:  # noqa: E402
    from stage2 import policy_engine_v2  # type: ignore
except Exception:  # pragma: no cover
    policy_engine_v2 = None  # type: ignore

# Латентність Stage1: експорт опційної функції спостереження гістограми
try:
    from config.config import PROM_GAUGES_ENABLED as _PROM_ENABLED  # type: ignore
except Exception:
    _PROM_ENABLED = True  # type: ignore[assignment]
if _PROM_ENABLED:
    try:
        from telemetry.prom_gauges import (  # type: ignore
            record_stage1_latency_ms as _rec_stage1_latency_ms,
        )

        record_stage1_latency_ms = _rec_stage1_latency_ms  # type: ignore[assignment]
    except Exception:
        record_stage1_latency_ms = None  # type: ignore[assignment]

# Опційний JSONL‑дамп рішень (best‑effort)
write_decision_trace = None  # type: ignore[assignment]
try:
    from telemetry.decision_dump import (  # type: ignore
        write_decision_trace as _write_decision_trace,
    )

    write_decision_trace = _write_decision_trace  # type: ignore[assignment]
except Exception:
    write_decision_trace = None  # type: ignore[assignment]

logger = logging.getLogger("process_asset_batch")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    # Пишемо у stdout, щоб tee у tools.run_window коректно дзеркалив у файл UTF-8
    logger.addHandler(RichHandler(console=Console(file=sys.stdout), show_path=True))
    logger.propagate = False
    # Додатковий файловий хендлер для реального часу (tools.verify_explain очікує ./logs/app.log)
    try:
        _logs_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(_logs_dir, exist_ok=True)
        _fh = logging.FileHandler(
            os.path.join(_logs_dir, "app.log"), encoding="utf-8", delay=True
        )
        _fh.setLevel(logging.INFO)
        # Лаконічний формат — тільки повідомлення; RichHandler already formats for console
        _fh.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(_fh)
    except Exception:
        # best-effort: якщо файл недоступний — не блокуємо пайплайн
        pass

if TYPE_CHECKING:  # pragma: no cover - type hints only
    from data.unified_store import UnifiedDataStore


# Rate-limit журналу для Prometheus presence (на символ): ~1 запис / 30с
_PROM_PRES_LAST_TS: dict[str, float] = {}
_PROM_CTX_LAST_TS: dict[str, float] = {}

# Кільце для контекстних метрик (12 останніх барів)
_CONTEXT_RING: dict[str, deque[dict[str, Any]]] = {}

# Policy v2: пер-(symbol,class) cooldown для лічильника "emitted"
_POLICY_V2_LAST_SIGNAL_TS: dict[tuple[str, str], float] = {}


# ── Допоміжні утиліти для нормалізації/гейтів ───────────────────────────
_DVR_EMA_STATE: dict[str, float] = {}

# ── Опційні Prometheus метрики ───────────────────────────────────────────
# За замовчуванням — no-op; якщо ввімкнено в конфігурації і пакет доступний — підміняються реальними функціями.
set_low_atr = None  # type: ignore[assignment]
set_htf_strength = None  # type: ignore[assignment]
set_presence = None  # type: ignore[assignment]
set_phase = None  # type: ignore[assignment]
set_scenario = None  # type: ignore[assignment]
inc_scenario_reject = None  # type: ignore[assignment]
inc_explain_line = None  # type: ignore[assignment]
inc_explain_heartbeat = None  # type: ignore[assignment]
set_liq_sweep = None  # type: ignore[assignment]
inc_liq_sweep_total = None  # type: ignore[assignment]
inc_sweep_then_breakout = None  # type: ignore[assignment]
inc_sweep_reject = None  # type: ignore[assignment]
set_retest_ok = None  # type: ignore[assignment]
record_stage1_latency_ms = None  # type: ignore[assignment]
try:
    from config.config import PROM_GAUGES_ENABLED  # noqa: F401
except Exception:  # pragma: no cover - захисний фолбек
    PROM_GAUGES_ENABLED = True  # type: ignore[assignment]
    logging.info("P_A_B. Prometheus метрики увімкнено за замовчуванням")
else:
    if PROM_GAUGES_ENABLED:
        try:
            from telemetry.prom_gauges import (  # type: ignore
                set_htf_strength as _set_htf_strength,
            )
            from telemetry.prom_gauges import (
                set_low_atr as _set_low_atr,
            )
            from telemetry.prom_gauges import (
                set_phase as _set_phase,
            )
            from telemetry.prom_gauges import (
                set_presence as _set_presence,
            )
            from telemetry.prom_gauges import (
                set_scenario as _set_scenario,
            )

            try:
                from telemetry.prom_gauges import (  # type: ignore
                    inc_scenario_reject as _inc_scenario_reject,
                )
            except Exception:
                _inc_scenario_reject = None  # type: ignore[assignment]
            try:
                from telemetry.prom_gauges import (  # type: ignore
                    inc_explain_line as _inc_explain_line,
                )
            except Exception:
                _inc_explain_line = None  # type: ignore[assignment]
            try:
                from telemetry.prom_gauges import (  # type: ignore
                    inc_explain_heartbeat as _inc_explain_heartbeat,
                )
            except Exception:
                _inc_explain_heartbeat = None  # type: ignore[assignment]

            # Додаткові (опційні) лічильники для chop/hypothesis
            try:
                from telemetry.prom_gauges import (  # type: ignore
                    inc_chop_hits_total as _inc_chop_hits_total,
                )
            except Exception:
                _inc_chop_hits_total = None  # type: ignore[assignment]
            try:
                from telemetry.prom_gauges import (  # type: ignore
                    inc_hypothesis_confirm as _inc_hyp_confirm,
                )
                from telemetry.prom_gauges import (  # type: ignore
                    inc_hypothesis_expired as _inc_hyp_expired,
                )
                from telemetry.prom_gauges import (  # type: ignore
                    inc_hypothesis_open as _inc_hyp_open,
                )
            except Exception:
                _inc_hyp_open = None  # type: ignore[assignment]
                _inc_hyp_confirm = None  # type: ignore[assignment]
                _inc_hyp_expired = None  # type: ignore[assignment]

            # Нові контекстні/режимні гейджі (опційно)
            try:
                from telemetry.prom_gauges import (
                    inc_liq_sweep_total as _inc_liq_sweep_total,
                )
                from telemetry.prom_gauges import (
                    inc_sweep_reject as _inc_sweep_reject,
                )
                from telemetry.prom_gauges import (
                    inc_sweep_then_breakout as _inc_sweep_then_breakout,
                )
                from telemetry.prom_gauges import (
                    set_bias_state as _set_bias_state,
                )
                from telemetry.prom_gauges import (  # type: ignore  # noqa: E402
                    set_btc_regime as _set_btc_regime,
                )
                from telemetry.prom_gauges import (
                    set_context_near_edge_persist as _set_ctx_nep,
                )
                from telemetry.prom_gauges import (
                    set_context_presence_sustain as _set_ctx_ps,
                )
                from telemetry.prom_gauges import (
                    set_liq_sweep as _set_liq_sweep,
                )
                from telemetry.prom_gauges import (
                    set_retest_ok as _set_retest_ok,
                )
            except Exception:  # noqa: PERF203 - best-effort optional imports
                _set_ctx_nep = None  # type: ignore[assignment]
                _set_ctx_ps = None  # type: ignore[assignment]
                _set_btc_regime = None  # type: ignore[assignment]
                _set_liq_sweep = None  # type: ignore[assignment]
                _inc_liq_sweep_total = None  # type: ignore[assignment]
                _set_bias_state = None  # type: ignore[assignment]
                _inc_sweep_then_breakout = None  # type: ignore[assignment]
                _inc_sweep_reject = None  # type: ignore[assignment]
                _set_retest_ok = None  # type: ignore[assignment]

            # Присвоюємо лише якщо імпорт вдався
            set_low_atr = _set_low_atr  # type: ignore[assignment]
            set_htf_strength = _set_htf_strength  # type: ignore[assignment]
            set_presence = _set_presence  # type: ignore[assignment]
            set_phase = _set_phase  # type: ignore[assignment]
            set_scenario = _set_scenario  # type: ignore[assignment]
            inc_scenario_reject = _inc_scenario_reject  # type: ignore[assignment]
            inc_explain_line = _inc_explain_line  # type: ignore[assignment]
            inc_explain_heartbeat = _inc_explain_heartbeat  # type: ignore[assignment]
            set_context_near_edge_persist = _set_ctx_nep  # type: ignore[assignment]
            set_context_presence_sustain = _set_ctx_ps  # type: ignore[assignment]
            set_btc_regime = _set_btc_regime  # type: ignore[assignment]
            set_liq_sweep = _set_liq_sweep  # type: ignore[assignment]
            inc_liq_sweep_total = _inc_liq_sweep_total  # type: ignore[assignment]
            set_bias_state = _set_bias_state  # type: ignore[assignment]
            inc_sweep_then_breakout = _inc_sweep_then_breakout  # type: ignore[assignment]
            inc_sweep_reject = _inc_sweep_reject  # type: ignore[assignment]
            set_retest_ok = _set_retest_ok  # type: ignore[assignment]
            # експорт нових опційних лічильників
            try:
                inc_chop_hits_total = _inc_chop_hits_total  # type: ignore[assignment]
            except Exception:
                inc_chop_hits_total = None  # type: ignore[assignment]
            try:
                inc_hypothesis_open = _inc_hyp_open  # type: ignore[assignment]
                inc_hypothesis_confirm = _inc_hyp_confirm  # type: ignore[assignment]
                inc_hypothesis_expired = _inc_hyp_expired  # type: ignore[assignment]
            except Exception:
                inc_hypothesis_open = None  # type: ignore[assignment]
                inc_hypothesis_confirm = None  # type: ignore[assignment]
                inc_hypothesis_expired = None  # type: ignore[assignment]
            # Профільні метрики
            try:
                from telemetry.prom_gauges import (
                    inc_profile_hint_emitted as _inc_profile_hint_emitted,
                )
                from telemetry.prom_gauges import (
                    inc_profile_switch as _inc_profile_switch,
                )
                from telemetry.prom_gauges import (  # type: ignore
                    set_profile_active as _set_profile_active,
                )
                from telemetry.prom_gauges import (
                    set_profile_confidence as _set_profile_confidence,
                )

                # False breakout лічильник (опційно)
                try:
                    from telemetry.prom_gauges import (
                        inc_false_breakout_detected as _inc_false_breakout_detected,
                    )
                except Exception:
                    _inc_false_breakout_detected = None  # type: ignore[assignment]

                set_profile_active = _set_profile_active  # type: ignore[assignment]
                set_profile_confidence = _set_profile_confidence  # type: ignore[assignment]
                inc_profile_switch = _inc_profile_switch  # type: ignore[assignment]
                inc_profile_hint_emitted = _inc_profile_hint_emitted  # type: ignore[assignment]
                try:
                    inc_false_breakout_detected = _inc_false_breakout_detected  # type: ignore[assignment]
                except Exception:
                    inc_false_breakout_detected = None  # type: ignore[assignment]
            except Exception:
                set_profile_active = None  # type: ignore[assignment]
                set_profile_confidence = None  # type: ignore[assignment]
                inc_profile_switch = None  # type: ignore[assignment]
                inc_profile_hint_emitted = None  # type: ignore[assignment]
        except Exception:  # pragma: no cover - відсутній пакет або помилка імпорту
            # Залишаємо no-op
            pass


class ProcessAssetBatchv1:
    """
    Клас для обробки батчів активів: Stage1 → whale (Redis/локальний fallback) → Stage2-lite hints → фазування → оновлення state_manager.
    """

    @staticmethod
    async def process_asset_batch(
        symbols: list[str],
        monitor: AssetMonitorStage1,
        store: UnifiedDataStore,
        timeframe: str,
        lookback: int,
        state_manager: AssetStateManager,
    ) -> None:
        """
        Обробляє батч символів: Stage1 → whale (Redis/локальний fallback) → Stage2-lite hints → фазування → оновлення state_manager.

        Args:
            symbols (list[str]): Список символів для обробки.
            monitor (AssetMonitorStage1): Монітор активів для виявлення аномалій (Stage1).
            store (UnifiedDataStore): Сховище історичних даних (з підтримкою Redis).
            timeframe (str): Таймфрейм для аналізу (наприклад, '1m').
            lookback (int): Глибина історії (кількість свічок).
            state_manager (AssetStateManager): Менеджер стану активів (оновлює сигнал/статистику).

        Мікро-контракт:
            Вхід: symbols, monitor, store, timeframe, lookback, state_manager
            Алгоритм:
            1. Stage1: Аналіз аномалій, формування сигналу (ALERT/NORMAL).
            2. Whale: Витягує whale-метрики з Redis або локально (VWAP/TWAP, SDZ, scoring).
            3. Stage2-lite hints: Генерує directional підказки (якщо флаг увімкнено).
            4. Фазування: Визначає фазу ринку (strict_phase).
            5. Оновлює state_manager для кожного символу (ключі: symbol, signal, stats, thresholds, trigger_reasons, raw_trigger_reasons, hints, state).
            Вихід: Оновлені записи у state_manager (без повернення значення).

        Примітки:
            - Контракти Stage1Signal не змінюються.
            - Whale/Stage2-lite — лише якщо відповідні фіче-флаги активні.
            - Логи: [STRICT_WHALE], [STAGE2_HINT], [STRICT_PHASE] для діагностики.
            - Всі ключі — через config/config.py (без літералів).
        """
        logger.info("[BATCH] symbols=%s (n=%d)", symbols, len(symbols))
        for symbol in symbols:
            lower_symbol = symbol.lower()
            try:
                logger.debug("[LOAD] %s: Завантаження даних...", lower_symbol)
                df = await store.get_df(symbol, timeframe, limit=lookback)
                if df is None or df.empty or len(df) < 5:
                    logger.warning(
                        "[NO_DATA] %s: Недостатньо даних, пропускаємо", lower_symbol
                    )
                    state_manager.update_asset(
                        lower_symbol, create_no_data_signal(symbol)
                    )
                    continue
                if "open_time" in df.columns and "timestamp" not in df.columns:
                    df = df.rename(columns={"open_time": "timestamp"})

                df = sanitize_ohlcv_numeric(
                    df, logger_obj=logger, log_prefix=f"[{lower_symbol}] "
                )
                if df.empty:
                    logger.warning(
                        "[NO_DATA] %s: sanitized frame порожній, пропускаємо",
                        lower_symbol,
                    )
                    state_manager.update_asset(
                        lower_symbol, create_no_data_signal(symbol)
                    )
                    continue

                try:
                    current_price = float(df["close"].iloc[-1])
                except Exception:
                    current_price = None
                try:
                    volume_last = float(df["volume"].iloc[-1])
                except Exception:
                    volume_last = None
                last_ts_val = None
                if "timestamp" in df.columns:
                    try:
                        last_ts_val = df["timestamp"].iloc[-1]
                    except Exception:
                        last_ts_val = None

                logger.debug("[STAGE1] %s: anomaly check", lower_symbol)
                _t0_ms = time.perf_counter()
                try:
                    signal = await monitor.check_anomalies(symbol, df)
                finally:
                    try:
                        if "record_stage1_latency_ms" in globals() and record_stage1_latency_ms is not None:  # type: ignore[name-defined]
                            dt_ms = (time.perf_counter() - _t0_ms) * 1000.0
                            record_stage1_latency_ms(float(dt_ms))  # type: ignore[misc]
                    except Exception:
                        pass
                if not isinstance(signal, dict):
                    logger.warning("[STAGE1] %s: повернув не dict", lower_symbol)
                    signal = {"symbol": lower_symbol, "signal": "NONE", "stats": {}}

                stats_container = signal.get("stats") or {}
                if not isinstance(stats_container, dict):
                    stats_container = {}
                    signal["stats"] = stats_container
                if current_price is not None:
                    stats_container["current_price"] = current_price
                if volume_last is not None:
                    stats_container["volume"] = volume_last
                if last_ts_val is not None:
                    stats_container["timestamp"] = last_ts_val

                normalized = normalize_result_types(signal)
                norm_stats = normalized.get("stats")
                # Stage1: телеметрія сигналу (ALERT/NORMAL) + причини
                try:
                    # За потреби повністю виключаємо low_volatility/low_atr з логів stage1_signal
                    trig = normalized.get("trigger_reasons")
                    raw_trig = normalized.get("raw_trigger_reasons")
                    if bool(STAGE1_EXCLUDE_LOW_VOL_ATR_TRIGGERS):

                        def _flt(val: Any) -> Any:
                            if isinstance(val, list):
                                return [
                                    x
                                    for x in val
                                    if isinstance(x, str)
                                    and x not in ("low_volatility", "low_atr")
                                ]
                            return val

                        trig = _flt(trig)
                        raw_trig = _flt(raw_trig)

                    await log_stage1_event(
                        event="stage1_signal",
                        symbol=lower_symbol,
                        payload={
                            "signal": str(normalized.get("signal")),
                            "trigger_reasons": trig,
                            "raw_trigger_reasons": raw_trig,
                            "stats": normalized.get("stats"),
                        },
                    )
                except Exception:
                    logger.debug("[TELEM] %s: stage1_signal log failed", lower_symbol)

                # TRAP: телеметрія оцінки (score + fired)
                try:
                    trap_score = None
                    try:
                        trap_score = float(
                            (normalized.get("stats") or {}).get("trap_score")
                        )
                    except Exception:
                        trap_score = None
                    if isinstance(trap_score, (int, float)):
                        fired = bool(
                            trap_score
                            >= float((STAGE1_TRAP or {}).get("score_gate", 0.67))
                        )
                        await log_stage1_event(
                            event="trap_eval",
                            symbol=lower_symbol,
                            payload={
                                "score": float(trap_score),
                                "fired": fired,
                                "reasons": normalized.get("trigger_reasons"),
                            },
                        )
                except Exception:
                    logger.debug("[TELEM] %s: trap_eval log failed", lower_symbol)
                if not isinstance(norm_stats, dict):
                    normalized["stats"] = stats_container
                else:
                    for k, v in stats_container.items():
                        norm_stats.setdefault(k, v)

                # Whale embed: Redis → локальний фолбек
                whale_embedded: dict[str, Any] | None = None
                try:
                    if STAGE2_WHALE_EMBED_ENABLED:
                        logger.debug(
                            "[STRICT_WHALE] %s: спроба отримати whale-метрики з Redis...",
                            lower_symbol,
                        )
                        w = None
                        jget = getattr(store.redis, "jget", None)
                        if callable(jget):
                            sym_for_keys = str(symbol).upper()
                            w = await jget("whale", sym_for_keys, "1m", default=None)
                        if not isinstance(w, dict):
                            logger.info(
                                "[STRICT_WHALE] %s: whale-метрики не знайдено в Redis, очікуємо оновлення від воркера",
                                lower_symbol,
                            )
                            w = {}
                        else:
                            try:
                                ts_dbg = (
                                    int(w.get("ts"))
                                    if isinstance(w.get("ts"), (int, float))
                                    else None
                                )
                            except Exception:
                                ts_dbg = None
                            # Легка інформативна мітка, що хітнули Redis і маємо валідний словник
                            if ts_dbg is not None:
                                now_ms_dbg = int(time.time() * 1000)
                                age_dbg = int(now_ms_dbg - ts_dbg)
                                logger.info(
                                    "[STRICT_WHALE] %s: whale-метрики отримано (age_ms=%s)",
                                    lower_symbol,
                                    age_dbg,
                                )

                        ts_ms = (
                            w.get("ts")
                            if isinstance(w.get("ts"), (int, float))
                            else None
                        )
                        now_ms = int(time.time() * 1000)
                        age_ms = int(now_ms - int(ts_ms)) if ts_ms is not None else None
                        stale = bool(age_ms is None or age_ms > 7000)
                        explain = (
                            w.get("explain")
                            if isinstance(w.get("explain"), dict)
                            else {}
                        )
                        zones = (
                            w.get("zones") if isinstance(w.get("zones"), dict) else {}
                        )
                        accum_raw = (
                            zones.get("accum") if isinstance(zones, dict) else None
                        )
                        dist_raw = (
                            zones.get("dist") if isinstance(zones, dict) else None
                        )
                        presence = w.get("presence_score")
                        bias = w.get("bias")
                        vdev = w.get("vwap_deviation")

                        try:
                            atr_val = float(normalized["stats"].get("atr"))
                        except Exception:
                            atr_val = None
                        try:
                            cp_val2 = float(normalized["stats"].get("current_price"))
                        except Exception:
                            cp_val2 = None
                        atr_pct_local = (
                            (atr_val / cp_val2 * 100.0)
                            if atr_val and cp_val2 and cp_val2 > 0
                            else None
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

                            whale_embedded = {
                                "version": "v2",
                                "ts": ts_ms,
                                "age_ms": age_ms,
                                "stale": stale,
                                "presence": presence,
                                "bias": bias,
                                "vwap_dev": vdev,
                                "dev_level": (
                                    explain.get("dev_level")
                                    if isinstance(explain, dict)
                                    else None
                                ),
                                "zones_summary": {
                                    "accum_cnt": (
                                        int(accum_raw)
                                        if isinstance(accum_raw, (int, float))
                                        else 0
                                    ),
                                    "dist_cnt": (
                                        int(dist_raw)
                                        if isinstance(dist_raw, (int, float))
                                        else 0
                                    ),
                                },
                                "vol_regime": vol_regime,
                            }
                            # Домінація: віддаємо перевагу значенню з WhaleWorker; локальний фолбек — якщо немає
                            try:
                                dom_in = (
                                    w.get("dominance") if isinstance(w, dict) else None
                                )
                                if (
                                    isinstance(dom_in, dict)
                                    and isinstance(dom_in.get("buy"), bool)
                                    and isinstance(dom_in.get("sell"), bool)
                                ):
                                    whale_embedded["dominance"] = {
                                        "buy": bool(dom_in.get("buy")),
                                        "sell": bool(dom_in.get("sell")),
                                    }
                                else:
                                    profile_cfg = STAGE2_SIGNAL_V2_PROFILES.get(
                                        STAGE2_PROFILE
                                    ) or STAGE2_SIGNAL_V2_PROFILES.get("strict", {})
                                    dom_cfg = (profile_cfg or {}).get("dominance", {})
                                    slope_atr_val = 0.0
                                    try:
                                        slope_atr_val = float(
                                            (normalized.get("stats") or {}).get(
                                                K_PRICE_SLOPE_ATR
                                            )
                                            or 0.0
                                        )
                                    except Exception:
                                        slope_atr_val = 0.0
                                    vdev_abs_min = float(
                                        dom_cfg.get("vdev_abs_min", 0.01)
                                    )
                                    slope_abs_min = float(
                                        dom_cfg.get("slope_atr_abs_min", 0.5)
                                    )
                                    z_accum_min = int(dom_cfg.get("zones_accum_min", 3))
                                    z_dist_min = int(dom_cfg.get("zones_dist_min", 3))
                                    dom_buy = bool(
                                        isinstance(vdev, (int, float))
                                        and vdev is not None
                                        and float(vdev) >= vdev_abs_min
                                        and abs(slope_atr_val) >= slope_abs_min
                                        and int(zones.get("accum") or 0) >= z_accum_min
                                    )
                                    dom_sell = bool(
                                        isinstance(vdev, (int, float))
                                        and vdev is not None
                                        and (-float(vdev)) >= vdev_abs_min
                                        and abs(slope_atr_val) >= slope_abs_min
                                        and int(zones.get("dist") or 0) >= z_dist_min
                                    )
                                    whale_embedded["dominance"] = {
                                        "buy": dom_buy,
                                        "sell": dom_sell,
                                    }
                            except Exception:
                                # best-effort, пропускаємо у разі проблем
                                pass
                            normalized.setdefault("stats", {})["whale"] = whale_embedded
                            accum_cnt = int(
                                (whale_embedded.get("zones_summary") or {}).get(
                                    "accum_cnt", 0
                                )
                            )
                            dist_cnt = int(
                                (whale_embedded.get("zones_summary") or {}).get(
                                    "dist_cnt", 0
                                )
                            )
                            accum_payload = update_accum_monitor(
                                lower_symbol,
                                accum_cnt=accum_cnt,
                                dist_cnt=dist_cnt,
                            )
                            stats_post = normalized.setdefault("stats", {}).setdefault(
                                "postprocessors", {}
                            )
                            stats_post["accum_monitor"] = accum_payload
                            mc_meta = normalized.setdefault(
                                "market_context", {}
                            ).setdefault("meta", {})
                            meta_post = mc_meta.setdefault("postprocessors", {})
                            meta_post["accum_monitor"] = accum_payload
                            if whale_embedded.get("stale"):
                                tags = (
                                    normalized["stats"].get("tags")
                                    if isinstance(normalized["stats"].get("tags"), list)
                                    else []
                                )
                                if "whale_stale" not in tags:
                                    tags.append("whale_stale")
                                normalized["stats"]["tags"] = tags
                            logger.debug(
                                "[STRICT_WHALE] %s: whale-метрики %s",
                                lower_symbol,
                                whale_embedded,
                            )
                except Exception as exc:
                    logger.error(
                        "[STRICT_WHALE] %s: помилка whale-метрик: %s", lower_symbol, exc
                    )
                    whale_embedded = None

                # Гарантуємо дефолтні whale‑метрики (presence=0.0, stale=True)
                try:
                    ensure_whale_default(normalized.setdefault("stats", {}))
                except Exception:
                    pass

                # Якщо телеметрія позначена як stale і presence відсутній,
                # мʼяко переносимо попереднє значення з state (під фіче‑флагом).
                try:
                    from config import config as _cfg

                    if bool(
                        getattr(
                            _cfg,
                            "WHALE_PRESENCE_STALE_CARRY_FORWARD_ENABLED",
                            True,
                        )
                    ):
                        we = (normalized.get("stats") or {}).get("whale")
                        if isinstance(we, dict) and bool(we.get("stale")):
                            pres_val = we.get("presence")
                            if not isinstance(pres_val, (int, float)):
                                prev_entry = state_manager.state.get(lower_symbol, {})
                                prev_pres: float | None = None
                                try:
                                    prev_pres = float(
                                        (prev_entry.get("stats") or {})
                                        .get("whale", {})
                                        .get("presence")
                                    )
                                except Exception:
                                    prev_pres = None
                                if (
                                    isinstance(prev_pres, (int, float))
                                    and prev_pres > 0.0
                                ):
                                    we["presence"] = prev_pres
                                    try:
                                        logger.debug(
                                            "[STRICT_WHALE] %s carry-forward presence=%.3f",
                                            lower_symbol,
                                            prev_pres,
                                        )
                                    except Exception:
                                        pass
                except Exception:
                    pass
                # Stage2-lite hints
                try:
                    if STAGE2_HINT_ENABLED and isinstance(whale_embedded, dict):
                        logger.debug(
                            "[STAGE2_HINT] %s: перевірка умов...", lower_symbol
                        )
                        if (
                            not whale_embedded.get("stale")
                            and isinstance(whale_embedded.get("presence"), (int, float))
                            and isinstance(whale_embedded.get("bias"), (int, float))
                            and isinstance(whale_embedded.get("vwap_dev"), (int, float))
                        ):
                            presence_f = float(whale_embedded["presence"])
                            bias_f = float(whale_embedded["bias"])
                            vdev_f = float(whale_embedded["vwap_dev"])
                            vol_reg = str(whale_embedded.get("vol_regime", "unknown"))

                            # Базові пороги (fallback), можуть бути перевизначені профілем
                            presence_min = 0.55
                            bias_min = 0.60
                            vdev_min = 0.01
                            cooldown_s = 240
                            hysteresis_s = 30
                            k_range: tuple[int, int] | None = None

                            # Профільний двигун (телеметрія‑only, під фіче‑флагом), не змінює контракти
                            _prof_enabled = False
                            try:
                                import config.flags as _flags  # type: ignore

                                _prof_enabled = bool(
                                    getattr(_flags, "PROFILE_ENGINE_ENABLED", False)
                                )
                            except Exception:
                                _prof_enabled = False

                            profile_name = None
                            profile_conf = None
                            if _prof_enabled:
                                try:
                                    from config.config_whale_profiles import (
                                        apply_symbol_overrides as _apply_thr_ov,
                                    )
                                    from config.config_whale_profiles import (
                                        get_profile_thresholds as _thr,
                                    )
                                    from config.config_whale_profiles import (
                                        market_class_for_symbol as _mc,
                                    )
                                    from whale.institutional_entry_methods import (
                                        alt_flags_from_iem as _alt_from_iem,
                                    )
                                    from whale.orderbook_analysis import (
                                        alt_flags_from_stats as _alt_from_stats,
                                    )
                                    from whale.profile_selector import (
                                        apply_hysteresis as _apply_hysteresis,
                                    )
                                    from whale.profile_selector import (
                                        select_profile as _select_profile,
                                    )

                                    mc = _mc(lower_symbol)
                                    # Підрахунок edge‑hits (upper) у 15‑барному вікні для селектора
                                    try:
                                        st_local = normalized.get("stats") or {}
                                        ne = st_local.get("near_edge")
                                        near_upper_flag = bool(
                                            ne is True or str(ne).lower() == "upper"
                                        )
                                        er = _EDGE_HIT_RING.setdefault(
                                            lower_symbol, deque(maxlen=15)
                                        )
                                        er.append(bool(near_upper_flag))
                                        st_local["edge_hits_upper"] = int(
                                            sum(1 for x in er if x)
                                        )
                                        st_local["edge_hits_window"] = int(len(er))
                                        normalized["stats"] = st_local
                                    except Exception:
                                        pass
                                    prof_raw, profile_conf, reasons_prof = (
                                        _select_profile(
                                            normalized.get("stats"),
                                            whale_embedded,
                                            lower_symbol,
                                        )
                                    )
                                    # Гістерезис профілю
                                    st = _PROFILE_STATE.setdefault(lower_symbol, {})
                                    prev_prof = st.get("active")
                                    last_sw = st.get("last_switch_ts")
                                    thr0 = _thr(mc, prof_raw)
                                    if isinstance(thr0, dict) and thr0:
                                        hysteresis_s = int(
                                            thr0.get("hysteresis_s", hysteresis_s)
                                        )
                                    profile_name = _apply_hysteresis(
                                        prev_prof,
                                        prof_raw,
                                        float(last_sw or 0.0),
                                        hysteresis_s,
                                    )
                                    hysteresis_applied = bool(
                                        prev_prof and prof_raw != profile_name
                                    )
                                    if profile_name != prev_prof and prev_prof:
                                        st["last_switch_ts"] = time.time()
                                        try:
                                            if callable(
                                                locals().get("inc_profile_switch")
                                            ):
                                                locals()["inc_profile_switch"](str(prev_prof), str(profile_name))  # type: ignore[index]
                                        except Exception:
                                            pass
                                        st["active"] = profile_name
                                    elif not prev_prof:
                                        st["active"] = profile_name

                                    thr = _thr(mc, profile_name)
                                    if isinstance(thr, dict) and thr:
                                        presence_min = float(
                                            thr.get("presence_min", presence_min)
                                        )
                                        bias_min = float(
                                            thr.get("bias_abs_min", bias_min)
                                        )
                                        vdev_min = float(
                                            thr.get("vwap_dev_min", vdev_min)
                                        )
                                        cooldown_s = int(
                                            thr.get("cooldown_s", cooldown_s)
                                        )
                                        k_range = tuple(thr.get("k_range", (5, 10)))  # type: ignore[assignment]
                                        # ATR-scale (за vol_regime), масштабуємо пороги лінійно
                                        try:
                                            scale_map = thr.get("atr_scale") or {}
                                            scale = float(scale_map.get(str(vol_reg), 1.0) or 1.0)  # type: ignore[arg-type]
                                            presence_min *= scale
                                            bias_min *= scale
                                            vdev_min *= scale
                                        except Exception:
                                            pass
                                    # ── HTF: alt_confirm_min полегшення при up+confluence для BTC/ETH ──
                                    try:
                                        from config.flags import (
                                            HTF_CONTEXT_ENABLED,
                                        )

                                        _htf_on = HTF_CONTEXT_ENABLED
                                    except Exception:
                                        _htf_on = False
                                    # Витягаємо last_h1 через єдиний helper (мінімальний диф)
                                    try:
                                        st_local = normalized.get("stats") or {}
                                        last_h1 = (
                                            _extract_last_h1(st_local)
                                            if callable(_extract_last_h1)
                                            else None
                                        )
                                    except Exception:
                                        last_h1 = None
                                    try:
                                        if isinstance(thr, dict) and thr:
                                            thr = htf_adjust_alt_min(
                                                mc, last_h1, thr, enabled=bool(_htf_on)
                                            )
                                    except Exception:
                                        pass
                                    # Символьні оверрайди профільних порогів (телеметрія‑only)
                                    try:
                                        thr2, _ov_applied = _apply_thr_ov(
                                            thr if isinstance(thr, dict) else {},
                                            lower_symbol,
                                            str(profile_name or ""),
                                        )
                                        if isinstance(thr2, dict) and thr2:
                                            thr = thr2
                                        if _ov_applied:
                                            try:
                                                reasons_prof.append("override=1")  # type: ignore[arg-type]
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass
                                    logger.info(
                                        "[PROFILE] %s profile=%s conf=%.2f | thr(p=%.2f b=%.2f v=%.3f dom=%s alt_min=%d cool=%ds) reasons=%s",
                                        lower_symbol,
                                        profile_name,
                                        float(profile_conf or 0.0),
                                        float(presence_min),
                                        float(bias_min),
                                        float(vdev_min),
                                        (
                                            str(
                                                bool(
                                                    thr.get("require_dominance", False)
                                                )
                                            )
                                            if isinstance(thr, dict)
                                            else "False"
                                        ),
                                        (
                                            int(thr.get("alt_confirm_min", 0))
                                            if isinstance(thr, dict)
                                            else 0
                                        ),
                                        int(cooldown_s),
                                        (
                                            reasons_prof
                                            if "reasons_prof" in locals()
                                            else []
                                        ),
                                    )
                                    # Додаємо у market_context.meta спостережний профіль
                                    try:
                                        normalized.setdefault(
                                            "market_context", {}
                                        ).setdefault("meta", {})["profile"] = {
                                            "name": profile_name,
                                            "conf": float(profile_conf or 0.0),
                                            "k_range": k_range or (5, 10),
                                        }
                                    except Exception:
                                        pass
                                    # Метрики профілю (best-effort)
                                    try:
                                        if callable(locals().get("set_profile_active")):
                                            locals()["set_profile_active"](symbol.upper(), str(profile_name), mc)  # type: ignore[index]
                                        if callable(
                                            locals().get("set_profile_confidence")
                                        ):
                                            locals()["set_profile_confidence"](symbol.upper(), float(profile_conf or 0.0))  # type: ignore[index]
                                    except Exception:
                                        pass
                                except Exception:
                                    logger.debug(
                                        "[PROFILE] %s: selector failed",
                                        lower_symbol,
                                        exc_info=True,
                                    )

                            # Alt-confirm прапорці (дедуп у множину)
                            from_stats = {}
                            from_iem = {}
                            try:
                                from_stats = _alt_from_stats(
                                    normalized.get("stats") or {}
                                )
                            except Exception:
                                pass
                            try:
                                iem_res = (normalized.get("stats") or {}).get("iem")
                                if isinstance(iem_res, dict):
                                    from_iem = _alt_from_iem(iem_res)
                            except Exception:
                                pass
                            active_alt = active_alt_keys(from_stats, from_iem)
                            alt_confirms_count = len(active_alt)

                            # Домінування за напрямом кандидата (за знаком bias)
                            dom = whale_embedded.get("dominance") or {}
                            dom_buy = bool((dom or {}).get("buy"))
                            dom_sell = bool((dom or {}).get("sell"))
                            candidate_up = bias_f >= 0.0
                            dom_ok = True
                            if (
                                _prof_enabled
                                and isinstance(thr, dict)
                                and bool(thr.get("require_dominance", False))
                            ):
                                dom_ok = bool(dom_buy if candidate_up else dom_sell)
                            alt_ok = True
                            if _prof_enabled and isinstance(thr, dict):
                                alt_ok = alt_confirms_count >= int(
                                    thr.get("alt_confirm_min", 0)
                                )

                            dir_hint: str | None = None
                            # ── HTF контекст (Stage C, опційно): BTC/ETH up+confluence → бонус до conf і полегшення alt_confirm_min ──
                            try:
                                from config.flags import (
                                    HTF_CONTEXT_ENABLED as _HTF_ON,  # type: ignore
                                )
                            except Exception:
                                _HTF_ON = False  # type: ignore[assignment]  # noqa: N806
                            htf_bonus_conf = 0.0
                            if bool(_htf_on) and mc in ("BTC", "ETH"):
                                try:
                                    from context.htf_context import (
                                        h1_ctx as _h1_ctx,  # type: ignore
                                    )
                                except Exception:
                                    _h1_ctx = None  # type: ignore[assignment]
                                # Єдина точка вилучення last_h1 (helper)
                                try:
                                    st_local = normalized.get("stats") or {}
                                    last_h1 = (
                                        _extract_last_h1(st_local)
                                        if callable(_extract_last_h1)
                                        else None
                                    )
                                except Exception:
                                    last_h1 = None
                                if _h1_ctx and isinstance(last_h1, dict):
                                    try:
                                        h1_trend, h1_conf = _h1_ctx(last_h1)
                                    except Exception:
                                        h1_trend, h1_conf = ("unknown", False)
                                    if h1_trend == "up" and bool(h1_conf):
                                        htf_bonus_conf = 0.05
                                        try:
                                            reasons_prof.append("h1_trend=up")  # type: ignore[arg-type]
                                            reasons_prof.append("h1_conf=1")  # type: ignore[arg-type]
                                        except Exception:
                                            pass
                            if (
                                vdev_f >= vdev_min
                                and abs(bias_f) >= bias_min
                                and presence_f >= presence_min
                                and dom_ok
                                and alt_ok
                            ):
                                dir_hint = (
                                    "long"
                                    if (bias_f > 0.0 or dom_buy)
                                    else "short" if (bias_f < 0.0 or dom_sell) else None
                                )

                            if dir_hint and vol_reg != "hyper":
                                now_ts = time.time()
                                last_ts = _HINT_COOLDOWN_LAST_TS.get(lower_symbol, 0.0)
                                if (now_ts - last_ts) >= cooldown_s:
                                    ring = _WHALE_METRIC_RING.get(lower_symbol)
                                    if ring is None:
                                        ring = deque(maxlen=3)
                                        _WHALE_METRIC_RING[lower_symbol] = ring
                                    ring.append((presence_f, bias_f))
                                    stable_ok = True
                                    if len(ring) >= 2:
                                        dp = abs(ring[-1][0] - ring[-2][0])
                                        db = abs(ring[-1][1] - ring[-2][1])
                                        if max(dp, db) > 0.10:
                                            stable_ok = False
                                    if stable_ok:
                                        # Обчислення score за профільною схемою (0..1)
                                        def _clip01(x: float) -> float:
                                            return (
                                                0.0 if x < 0 else (1.0 if x > 1 else x)
                                            )

                                        # Нормування проксі
                                        pres_n = max(0.0, min(1.0, presence_f))
                                        bias_n = max(0.0, min(1.0, abs(bias_f)))
                                        vdev_n = max(
                                            0.0,
                                            min(1.0, abs(vdev_f) / max(vdev_min, 1e-6)),
                                        )
                                        agrees = (
                                            1.0
                                            if (
                                                (bias_f >= 0 and dom_buy)
                                                or (bias_f <= 0 and dom_sell)
                                                or (bias_f == 0)
                                            )
                                            else 0.0
                                        )
                                        w1, w2, w3, w4, w5, w6 = (
                                            0.35,
                                            0.25,
                                            0.10,
                                            0.10,
                                            0.10,
                                            0.10,
                                        )
                                        # бонус до профільної впевненості від HTF (якщо є)
                                        prof_conf_eff = float(
                                            profile_conf or 0.0
                                        ) + float(htf_bonus_conf or 0.0)
                                        score = _clip01(
                                            w1 * pres_n
                                            + w2 * bias_n * (1.0 if agrees else 0.8)
                                            + w3 * vdev_n
                                            + w4 * (1.0 if dom_ok else 0.0)
                                            + w5 * prof_conf_eff
                                            + w6 * (1.0 if alt_ok else 0.0)
                                        )
                                        # Пер‑клас масштаб score (BTC=1.0, ETH=0.95, ALTS=0.85)
                                        try:
                                            score *= score_scale_for_class(mc)
                                        except Exception:
                                            pass
                                        cooldown_active = False
                                        # cooldown пенальті (якщо активний)
                                        try:
                                            cd_until = (
                                                _PROFILE_STATE.get(lower_symbol) or {}
                                            ).get("cooldown_until_ts")
                                            if not isinstance(cd_until, (int, float)):
                                                # Мʼякий відновлювач cooldown із Redis (при рестарті)
                                                cd_restore = (
                                                    await read_profile_cooldown(
                                                        lower_symbol
                                                    )
                                                )
                                                if isinstance(cd_restore, (int, float)):
                                                    _PROFILE_STATE.setdefault(
                                                        lower_symbol, {}
                                                    )["cooldown_until_ts"] = cd_restore
                                                    cd_until = cd_restore
                                            if isinstance(
                                                cd_until, (int, float)
                                            ) and now_ts < float(cd_until):
                                                score *= 0.5
                                                dir_hint = (
                                                    None  # форсуємо нейтральний вихід
                                                )
                                                cooldown_active = True
                                        except Exception:
                                            pass
                                        # Пенальті/стеля для stale whale-підказок + нейтралізація
                                        try:
                                            if bool(whale_embedded.get("stale")):
                                                from config.config_stage2 import (
                                                    STAGE2_INSIGHT as _INS_CFG,
                                                )

                                                stale_penalty = float(
                                                    (_INS_CFG or {}).get(
                                                        "stale_hint_penalty", 0.5
                                                    )
                                                )
                                                stale_cap = float(
                                                    (_INS_CFG or {}).get(
                                                        "stale_hint_cap", 0.35
                                                    )
                                                )
                                                score = min(
                                                    stale_cap, score * stale_penalty
                                                )
                                                # fail-open: додатково форсуємо NEUTRAL та зменшуємо
                                                dir_hint = None
                                                score *= 0.5
                                                stale_flag = True
                                        except Exception:
                                            pass
                                        else:
                                            stale_flag = False
                                        reasons: list[str] = []
                                        if presence_f >= presence_min:
                                            reasons.append("presence_ok")
                                        if abs(bias_f) >= bias_min:
                                            reasons.append("bias_ok")
                                        if abs(vdev_f) >= vdev_min:
                                            reasons.append("vwap_dev_ok")
                                        reasons.append(f"profile={profile_name}")
                                        reasons.append(f"dom_ok={(1 if dom_ok else 0)}")
                                        reasons.append(f"alt_ok={(1 if alt_ok else 0)}")
                                        if htf_bonus_conf > 0.0:
                                            reasons.append("htf_bonus=1")
                                        reasons.append(
                                            f"hysteresis={(1 if 'hysteresis_applied' in locals() and hysteresis_applied else 0)}"
                                        )
                                        reasons.append(
                                            f"cooldown={(1 if cooldown_active else 0)}"
                                        )
                                        reasons.append(
                                            f"stale={(1 if stale_flag else 0)}"
                                        )
                                        reasons.append(f"vol_regime={vol_reg}")
                                        # False breakout → range_fade: форсуємо нейтральний напрям і маркер причини
                                        try:
                                            if str(profile_name) == "range_fade":
                                                rp = (
                                                    locals().get("reasons_prof")
                                                    if "reasons_prof" in locals()
                                                    else []
                                                )
                                                if any(
                                                    isinstance(r, str)
                                                    and "false_breakout" in r
                                                    for r in (rp or [])
                                                ):
                                                    reasons.append(
                                                        "range_fade_from_false_breakout=1"
                                                    )
                                                    dir_hint = None
                                                    try:
                                                        if callable(
                                                            locals().get(
                                                                "inc_false_breakout_detected"
                                                            )
                                                        ):
                                                            locals()[
                                                                "inc_false_breakout_detected"
                                                            ](
                                                                symbol.upper(), "up"
                                                            )  # type: ignore[index]
                                                    except Exception:
                                                        pass
                                        except Exception:
                                            pass
                                        # Діагностика pre_breakout_flag для probe_up: поруч із верхнім краєм і достатній нахил TWAP
                                        try:
                                            if str(profile_name) == "probe_up":
                                                st_local = normalized.get("stats") or {}
                                                ne = st_local.get("near_edge")
                                                near_upper_flag = bool(
                                                    ne is True
                                                    or str(ne).lower() == "upper"
                                                )
                                                try:
                                                    s_val = float(
                                                        st_local.get(K_PRICE_SLOPE_ATR)
                                                        or st_local.get(
                                                            "price_slope_atr"
                                                        )
                                                        or 0.0
                                                    )
                                                except Exception:
                                                    s_val = 0.0
                                                slope_ok = bool(s_val >= 0.8)
                                                reasons.append(
                                                    f"pre_breakout={(1 if (near_upper_flag and slope_ok) else 0)}"
                                                )
                                                reasons.append(
                                                    f"slope_twap_ok={(1 if slope_ok else 0)}"
                                                )
                                        except Exception:
                                            pass
                                        # ── Pre‑breakout гіпотеза: NEUTRAL під час chop; confirm при accept або alt_confirm≥thr ──
                                        try:
                                            st_local = normalized.get("stats") or {}
                                            ne = st_local.get("near_edge")
                                            near_upper_flag = bool(
                                                ne is True or str(ne).lower() == "upper"
                                            )
                                            accept_ok = bool(
                                                st_local.get("acceptance_ok", False)
                                            )
                                        except Exception:
                                            near_upper_flag, accept_ok = False, False
                                        # Відкриття та підтримка гіпотези, якщо активний chop‑профіль
                                        if str(profile_name) == "chop_pre_breakout_up":
                                            # edge hits для reasons (best‑effort)
                                            try:
                                                eh = int(
                                                    st_local.get("edge_hits_upper") or 0
                                                )
                                                ew = int(
                                                    st_local.get("edge_hits_window")
                                                    or 0
                                                )
                                            except Exception:
                                                eh, ew = 0, 0
                                            hs = _HYP_STATE.get(lower_symbol) or {}
                                            if (not hs) or (
                                                now_ts
                                                >= float(
                                                    hs.get("expires_ts", 0.0) or 0.0
                                                )
                                            ):
                                                _HYP_STATE[lower_symbol] = {
                                                    "type": "pre_breakout",
                                                    "state": "open",
                                                    "opened_ts": now_ts,
                                                    "expires_ts": now_ts + 120.0,
                                                    "edge_hits": eh,
                                                    "window": ew,
                                                }
                                                try:
                                                    if callable(
                                                        locals().get(
                                                            "inc_hypothesis_open"
                                                        )
                                                    ):
                                                        locals()["inc_hypothesis_open"]("pre_breakout")  # type: ignore[index]
                                                except Exception:
                                                    pass
                                                logger.info(
                                                    "[HYP] %s pre_breakout OPEN | edge_hits=%d/%d",
                                                    lower_symbol,
                                                    eh,
                                                    ew,
                                                )
                                            # Під час chop форсуємо нейтральний напрям + причини діагностики
                                            dir_hint = None
                                            reasons.append("chop_pre_breakout")
                                            reasons.append("pre_breakout=1")
                                            reasons.append("pre_breakout_wait")
                                            reasons.append(f"edge_hits={eh}/{ew}")
                                            try:
                                                s_val = float(
                                                    st_local.get(K_PRICE_SLOPE_ATR)
                                                    or st_local.get("price_slope_atr")
                                                    or 0.0
                                                )
                                            except Exception:
                                                s_val = 0.0
                                            reasons.append(
                                                f"slope_twap_ok={(1 if s_val>=0.8 else 0)}"
                                            )
                                            # інкремент chop‑лічильника (best‑effort)
                                            try:
                                                if callable(
                                                    locals().get("inc_chop_hits_total")
                                                ):
                                                    locals()["inc_chop_hits_total"](symbol.upper(), "upper")  # type: ignore[index]
                                            except Exception:
                                                pass
                                        # Confirm: accept або alt_confirm достатньо
                                        try:
                                            thr_alt_min = (
                                                int(
                                                    (thr or {}).get(
                                                        "alt_confirm_min", 0
                                                    )
                                                )
                                                if isinstance(thr, dict)
                                                else 0
                                            )
                                        except Exception:
                                            thr_alt_min = 0
                                        confirm_flags: OrderedDict[str, bool] | None = (
                                            None
                                        )
                                        confirm_ready = False
                                        if str(profile_name) == "chop_pre_breakout_up":
                                            confirm_ready, confirm_flags = (
                                                should_confirm_pre_breakout(
                                                    accept_ok=accept_ok,
                                                    alt_confirms_count=alt_confirms_count,
                                                    thr_alt_min=thr_alt_min,
                                                    dominance_buy=dom_buy,
                                                    vwap_dev=vdev_f,
                                                    slope_atr=s_val,
                                                )
                                            )
                                            for key, ok in (
                                                confirm_flags or {}
                                            ).items():
                                                reasons.append(
                                                    f"{key}={(1 if ok else 0)}"
                                                )
                                            if confirm_ready:
                                                hs = _HYP_STATE.setdefault(
                                                    lower_symbol, {}
                                                )
                                                hs.update(
                                                    {
                                                        "type": "pre_breakout",
                                                        "state": "confirmed",
                                                        "confirmed_ts": now_ts,
                                                    }
                                                )
                                                try:
                                                    if callable(
                                                        locals().get(
                                                            "inc_hypothesis_confirm"
                                                        )
                                                    ):
                                                        locals()[
                                                            "inc_hypothesis_confirm"
                                                        ](
                                                            "pre_breakout"
                                                        )  # type: ignore[index]
                                                except Exception:
                                                    pass
                                                reasons.append("pre_breakout_confirm")
                                                # Після підтвердження дозволяємо напрямок UP
                                                dir_hint = "long"
                                        # Експірація open‑гіпотези
                                        try:
                                            hs = _HYP_STATE.get(lower_symbol) or {}
                                            if (
                                                hs
                                                and hs.get("type") == "pre_breakout"
                                                and hs.get("state") == "open"
                                                and now_ts
                                                > float(
                                                    hs.get("expires_ts", 0.0) or 0.0
                                                )
                                            ):
                                                hs["state"] = "expired"
                                                try:
                                                    if callable(
                                                        locals().get(
                                                            "inc_hypothesis_expired"
                                                        )
                                                    ):
                                                        locals()["inc_hypothesis_expired"]("pre_breakout")  # type: ignore[index]
                                                except Exception:
                                                    pass
                                                logger.info(
                                                    "[HYP] %s pre_breakout EXPIRED",
                                                    lower_symbol,
                                                )
                                        except Exception:
                                            pass
                                        hint_obj = {
                                            "dir": (dir_hint or "neutral"),
                                            "score": round(score, 4),
                                            "reasons": reasons,
                                            "ts": int(time.time() * 1000),
                                            "cooldown_s": cooldown_s,
                                        }
                                        # Виносимо стан гіпотези у meta (спостережно)
                                        try:
                                            mc_meta = normalized.setdefault(
                                                "market_context", {}
                                            ).setdefault("meta", {})
                                            hyp = _HYP_STATE.get(lower_symbol)
                                            if (
                                                hyp
                                                and hyp.get("type") == "pre_breakout"
                                            ):
                                                mc_meta.setdefault("hypothesis", {})[
                                                    "pre_breakout"
                                                ] = {
                                                    "state": hyp.get("state"),
                                                    "edge_hits": hyp.get("edge_hits"),
                                                    "window": hyp.get("window"),
                                                }
                                        except Exception:
                                            pass
                                        logger.info(
                                            "[STAGE2_HINT] %s dir=%s score=%.3f presence=%.2f bias=%.2f vdev=%.3f vol=%s",
                                            lower_symbol,
                                            (dir_hint or "neutral"),
                                            float(hint_obj["score"]),
                                            presence_f,
                                            bias_f,
                                            vdev_f,
                                            vol_reg,
                                        )
                                        try:
                                            logger.info(
                                                "[STRICT_WHALE] dir=%s score=%.2f | reasons=%s",
                                                (dir_hint or "neutral"),
                                                float(hint_obj["score"]),
                                                ",".join(reasons[:3])
                                                + ("..." if len(reasons) > 3 else ""),
                                            )
                                        except Exception:
                                            pass
                                        normalized.setdefault("stats", {})[
                                            "stage2_hint"
                                        ] = hint_obj
                                        # Дублюємо хінт у market_context.meta (спостережно)
                                        try:
                                            normalized.setdefault(
                                                "market_context", {}
                                            ).setdefault("meta", {})[
                                                "stage2_hint"
                                            ] = hint_obj
                                        except Exception:
                                            pass
                                        # Публікуємо як загальний manipulation_hint у Prometheus (long→up, short→down)
                                        try:
                                            from telemetry.prom_gauges import (
                                                set_manipulation_hint as _mhint,
                                            )

                                            dir_map = {"long": "up", "short": "down"}
                                            dlabel = dir_map.get(str(dir_hint))
                                            if dlabel:
                                                _mhint(symbol.upper(), dlabel, 1)
                                        except Exception:
                                            pass
                                        # Кулдаун після позитивної емісії
                                        try:
                                            if dir_hint in ("long", "short"):
                                                until = now_ts + float(cooldown_s)
                                                _PROFILE_STATE.setdefault(
                                                    lower_symbol, {}
                                                )["cooldown_until_ts"] = until
                                                # Мʼякий персист у Redis з TTL=120с
                                                try:
                                                    await write_profile_cooldown(
                                                        lower_symbol, until, ttl_s=120
                                                    )
                                                except Exception:
                                                    pass
                                                if callable(
                                                    locals().get(
                                                        "inc_profile_hint_emitted"
                                                    )
                                                ):
                                                    locals()["inc_profile_hint_emitted"]("up" if dir_hint == "long" else "down")  # type: ignore[index]
                                        except Exception:
                                            pass
                                        try:
                                            price_now = None
                                            try:
                                                price_now = float(
                                                    (normalized.get("stats") or {}).get(
                                                        "current_price"
                                                    )
                                                )
                                            except Exception:
                                                price_now = None
                                            await log_stage1_event(
                                                event="stage2_hint",
                                                symbol=lower_symbol,
                                                payload={
                                                    "dir": dir_hint,
                                                    "score": float(hint_obj["score"]),
                                                    "reasons": hint_obj.get("reasons"),
                                                    "price": price_now,
                                                    # Додаємо контекст для офлайн аналізу
                                                    "presence": presence_f,
                                                    "bias": bias_f,
                                                    "vwap_dev": vdev_f,
                                                    "vol_regime": vol_reg,
                                                },
                                            )
                                        except Exception:
                                            logger.debug(
                                                "[TELEM] %s: stage2_hint log failed",
                                                lower_symbol,
                                            )
                                        _HINT_COOLDOWN_LAST_TS[lower_symbol] = now_ts
                                    else:
                                        logger.debug(
                                            "[STAGE2_HINT] %s: не стабільний (dp=%.3f, db=%.3f)",
                                            lower_symbol,
                                            dp,
                                            db,
                                        )
                                else:
                                    logger.debug(
                                        "[STAGE2_HINT] %s: cooldown ще не минув (%.1f сек)",
                                        lower_symbol,
                                        now_ts - last_ts,
                                    )
                except Exception as exc:
                    logger.error(
                        "[STAGE2_HINT] %s: помилка Stage2-lite hints: %s",
                        lower_symbol,
                        exc,
                    )

                # Перевизначимо фазу після whale
                try:
                    stats_for_phase = normalized.get("stats") or {}
                    # DVR нормалізація (EMA, cap) перед фазуванням/сценарієм
                    try:
                        raw_dvr = (stats_for_phase or {}).get(
                            K_DIRECTIONAL_VOLUME_RATIO
                        )
                        dvr_norm = normalize_and_cap_dvr(lower_symbol, raw_dvr)
                        normalized.setdefault("stats", {})[
                            K_DIRECTIONAL_VOLUME_RATIO
                        ] = dvr_norm
                        # короткий аліас, якщо використовується споживачами
                        normalized["stats"].setdefault("dvr", dvr_norm)
                    except Exception:
                        pass
                    # Джерело істини для low_atr_spike override: Redis → stats → none
                    try:
                        jget = getattr(store.redis, "jget", None)
                        if callable(jget):
                            override_redis = await jget(
                                "overrides",
                                "low_atr_spike",
                                symbol.upper(),
                                default=None,
                            )
                        else:
                            override_redis = None
                    except Exception:
                        override_redis = None
                    if isinstance(override_redis, dict):
                        try:
                            normalized.setdefault("stats", {}).setdefault(
                                "overrides", {}
                            )["low_atr_spike"] = override_redis
                            stats_for_phase = normalized.get("stats") or {}
                        except Exception:
                            pass
                    # Prometheus: low_atr override стан/ttl (опційно)
                    try:
                        if callable(set_low_atr):  # type: ignore[arg-type]
                            ov = None
                            # пріоритет: Redis → stats.overrides
                            if isinstance(override_redis, dict):
                                ov = override_redis
                            else:
                                try:
                                    ov = (stats_for_phase.get("overrides") or {}).get(
                                        "low_atr_spike"
                                    )
                                except Exception:
                                    ov = None
                            ttl = 0.0
                            active = 0
                            if isinstance(ov, dict):
                                try:
                                    ttl = float(ov.get("ttl") or 0.0)
                                except Exception:
                                    ttl = 0.0
                                try:
                                    active = 1 if (ov.get("active") or ttl > 0.0) else 0
                                except Exception:
                                    active = 1 if ttl > 0.0 else 0
                            set_low_atr(symbol.upper(), int(active), float(ttl))  # type: ignore[misc]
                    except Exception:
                        pass
                    phase_existing = (
                        stats_for_phase.get("phase")
                        if isinstance(stats_for_phase.get("phase"), dict)
                        else None
                    )
                    low_gate_eff = None
                    try:
                        low_gate_eff = float(
                            (normalized.get("thresholds") or {}).get("low_gate")
                        )
                    except Exception:
                        low_gate_eff = None
                    phase_info = detect_phase_from_stats(
                        stats_for_phase,
                        symbol=lower_symbol,
                        low_gate_effective=low_gate_eff,
                    )
                    # Публікація bias_state (−1/0/+1) на кожному батчі — єдине місце,
                    # де ми оновлюємо market_context.meta та Prometheus незалежно від того,
                    # чи було 'phase' у stats раніше
                    try:
                        cd_val = float(
                            (stats_for_phase or {}).get(K_CUMULATIVE_DELTA)
                            or (stats_for_phase or {}).get("cumulative_delta")
                            or 0.0
                        )
                    except Exception:
                        cd_val = 0.0
                    try:
                        slope_val = float(
                            (stats_for_phase or {}).get(K_PRICE_SLOPE_ATR)
                            or (stats_for_phase or {}).get("price_slope_atr")
                            or 0.0
                        )
                    except Exception:
                        slope_val = 0.0
                    _bias_state_now = 0
                    if cd_val >= 0.35 and slope_val >= 0.8:
                        _bias_state_now = 1
                    elif cd_val <= -0.35 and slope_val <= -0.8:
                        _bias_state_now = -1
                    try:
                        normalized.setdefault("market_context", {}).setdefault(
                            "meta", {}
                        )["bias_state"] = int(_bias_state_now)
                        if callable(set_bias_state):  # type: ignore[name-defined]
                            set_bias_state(symbol.upper(), int(_bias_state_now))  # type: ignore[misc]
                    except Exception:
                        pass
                    if isinstance(phase_info, dict) and (
                        not isinstance(phase_existing, dict)
                        or not phase_existing.get("name")
                    ):
                        normalized.setdefault("stats", {})["phase"] = phase_info
                        # Scenario selection (real-time): map phase/stat → scenario
                        try:
                            scn_name: str | None
                            scn_conf: float
                            # Evidence bus: старт для символу
                            try:
                                from utils.evidence_bus import (
                                    start as _ev_start,  # noqa: E402
                                )

                                _ev_start(symbol.upper())
                            except Exception:
                                pass
                            scn_name, scn_conf = resolve_scenario(
                                phase_info.get("name"), stats_for_phase
                            )
                            # ── Контекст: оновити пам'ять та експорт ─────────────
                            try:
                                from config.keys import (
                                    build_key,  # type: ignore  # noqa: E402
                                )
                                from context.context_memory import (
                                    ContextMemory,  # type: ignore  # noqa: E402
                                )

                                ctx = _CTX_MEMORY.get(lower_symbol)
                                if ctx is None:
                                    ctx = ContextMemory(N=60)
                                    _CTX_MEMORY[lower_symbol] = ctx
                                # зібрати фічі
                                wh = (
                                    stats_for_phase.get("whale")
                                    if isinstance(stats_for_phase.get("whale"), dict)
                                    else {}
                                )
                                ctx.update(
                                    near_edge=stats_for_phase.get("near_edge"),
                                    band_expand=stats_for_phase.get("band_expand"),
                                    vol_z=stats_for_phase.get("volume_z"),
                                    dvr=stats_for_phase.get(K_DIRECTIONAL_VOLUME_RATIO),
                                    cd=stats_for_phase.get(K_CUMULATIVE_DELTA),
                                    slope_atr=stats_for_phase.get(K_PRICE_SLOPE_ATR),
                                    rsi=stats_for_phase.get("rsi"),
                                    htf_strength=stats_for_phase.get("htf_strength"),
                                    whale_presence=(wh or {}).get("presence"),
                                    whale_bias=(wh or {}).get("bias"),
                                    low_atr_override_active=(
                                        (normalized.get("stats") or {})
                                        .get("overrides", {})
                                        .get("low_atr_spike", {})
                                        .get("active")
                                    ),
                                )
                                ctx_meta = ctx.export_meta(w=3)
                                normalized.setdefault("market_context", {}).setdefault(
                                    "meta", {}
                                )["context"] = ctx_meta
                                # Додатково: ContextFrame v1
                                try:
                                    from utils.context_frame import (  # type: ignore  # noqa: E402
                                        build_context_frame,
                                    )

                                    cf = build_context_frame(stats_for_phase, ctx_meta)
                                    normalized.setdefault(
                                        "market_context", {}
                                    ).setdefault("meta", {})["context_frame"] = cf
                                except Exception:
                                    pass
                                # StructureMap v1 (best-effort із whale-зон)
                                try:
                                    from utils.structure_map import (  # type: ignore  # noqa: E402
                                        build_structure_map,
                                    )

                                    wh_zones = (
                                        (stats_for_phase.get("whale") or {}).get(
                                            "zones"
                                        )
                                        if isinstance(
                                            stats_for_phase.get("whale"), dict
                                        )
                                        else None
                                    )
                                    st_map = build_structure_map(
                                        stats_for_phase, wh_zones
                                    )
                                    normalized.setdefault(
                                        "market_context", {}
                                    ).setdefault("meta", {})["structure"] = st_map
                                except Exception:
                                    pass
                                # bias_state вже виставлено вище (meta + Prometheus);
                                # тут залишаємо лише контекстні метрики без дублювання.
                                # Prometheus (опційно)
                                try:
                                    if callable(set_context_near_edge_persist):  # type: ignore[name-defined]
                                        set_context_near_edge_persist(  # type: ignore[misc]
                                            symbol.upper(),
                                            float(
                                                ctx_meta.get("near_edge_persist", 0.0)
                                            ),
                                        )
                                    if callable(set_context_presence_sustain):  # type: ignore[name-defined]
                                        set_context_presence_sustain(  # type: ignore[misc]
                                            symbol.upper(),
                                            float(
                                                ctx_meta.get("presence_sustain", 0.0)
                                            ),
                                        )
                                except Exception:
                                    pass
                                # Redis снапшот (best-effort)
                                if CONTEXT_SNAPSHOTS_ENABLED:
                                    try:
                                        jset = getattr(store.redis, "jset", None)
                                        if callable(jset):
                                            key = build_key(
                                                NAMESPACE,
                                                "context_snapshot",
                                                symbol=lower_symbol,
                                            )
                                            await jset(key, ctx_meta, ttl=600)
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                            # ── BTC regime (BTCUSDT) ────────────────────────────
                            try:
                                if lower_symbol == "btcusdt":
                                    from stage2.btc_regime import (  # type: ignore  # noqa: E402
                                        detect_btc_regime,
                                        publish_btc_regime,
                                    )

                                    btc_state, btc_reasons = detect_btc_regime(
                                        stats_for_phase, ctx_meta
                                    )
                                    # Prometheus one-hot
                                    try:
                                        if callable(set_btc_regime):  # type: ignore[name-defined]
                                            for s in (
                                                "ACCUM",
                                                "EXPANSION_SETUP",
                                                "BREAKOUT_UP",
                                                "BREAKOUT_DOWN",
                                            ):
                                                set_btc_regime(s, 1 if s == btc_state else 0)  # type: ignore[misc]
                                    except Exception:
                                        pass
                                    # Redis publish (best-effort)
                                    await publish_btc_regime(
                                        store, btc_state, btc_reasons
                                    )
                            except Exception:
                                pass

                            # ── BTC-гейт і пер-символьні профілі (перед гістерезисом) ──
                            try:
                                from utils.scenario_gate import (
                                    apply_btc_gate_and_profiles,  # type: ignore  # noqa: E402
                                )

                                btc_regime_state = None
                                try:
                                    # Витягуємо з normalized.meta якщо вже було пораховано
                                    btc_regime_state = (
                                        normalized.get("market_context", {})
                                        .get("meta", {})
                                        .get("btc_regime_state")
                                    )
                                except Exception:
                                    btc_regime_state = None
                                gated_name, gated_conf, used_thr = (
                                    apply_btc_gate_and_profiles(
                                        symbol.upper(),
                                        scn_name,
                                        scn_conf,
                                        stats_for_phase,
                                        btc_regime_state,
                                    )
                                )
                                # пояснювальний лог
                                try:
                                    # Формат уніфіковано під тулінг якості: scenario=..., explain="..."
                                    # Порожній explain допустимий для "серцебиття" у NEUTRAL/gray, щоб підвищити ExpCov
                                    logger.info(
                                        '[SCEN_EXPLAIN] symbol=%s scenario=%s explain="" ctx={nep=%.2f,ps=%.2f,htfs=%.2f} btc_gate=%s conf=%.2f',
                                        lower_symbol,
                                        (scn_name or "-"),
                                        float(ctx_meta.get("near_edge_persist", 0.0)),
                                        float(ctx_meta.get("presence_sustain", 0.0)),
                                        float(ctx_meta.get("htf_sustain", 0.0)),
                                        (
                                            "applied"
                                            if (
                                                gated_name != scn_name
                                                or gated_conf != scn_conf
                                            )
                                            else "skipped"
                                        ),
                                        float(scn_conf or 0.0),
                                    )
                                except Exception:
                                    pass
                                scn_name, scn_conf = gated_name, gated_conf
                            except Exception:
                                pass

                            # ── BTC soft gate v1 (penalty 0.2 для альтів у BTC flat/low-HTF) ──
                            btc_regime = None
                            btc_htf_s = 0.0
                            _btc_soft_penalty_applied = False
                            try:
                                # Обчислити контекст BTC (best-effort)
                                from utils.btc_regime import (  # type: ignore  # noqa: E402
                                    apply_btc_gate,
                                    get_btc_htf_strength,
                                    get_btc_regime_v2,
                                )

                                try:
                                    now_ms = int(time.time() * 1000)
                                except Exception:
                                    now_ms = 0
                                # v2: режим з урахуванням phase/cd/slope (hint)
                                btc_regime = get_btc_regime_v2(
                                    stats_for_phase, read_from_redis=True
                                )
                                btc_htf_s = float(
                                    get_btc_htf_strength(read_from_redis=True) or 0.0
                                )
                                # Записати у market_context.meta
                                try:
                                    _meta_soft = normalized.setdefault(
                                        "market_context", {}
                                    ).setdefault("meta", {})
                                    _meta_soft["btc_regime"] = btc_regime
                                    _meta_soft["btc_htf_strength"] = float(btc_htf_s)
                                except Exception:
                                    pass
                                # Застосувати штраф, якщо увімкнено
                                try:
                                    import config.config as _cfg  # type: ignore  # noqa: E402

                                    btc_soft_enabled = bool(
                                        getattr(_cfg, "SCEN_BTC_SOFT_GATE", True)
                                    )
                                    scen_htf_min = float(
                                        getattr(_cfg, "SCEN_HTF_MIN", 0.02)
                                    )
                                except Exception:
                                    btc_soft_enabled = True  # type: ignore[assignment]
                                    scen_htf_min = 0.02  # type: ignore[assignment]
                                new_conf, applied = apply_btc_gate(
                                    symbol.upper(),
                                    float(scn_conf or 0.0),
                                    str(btc_regime or ""),
                                    float(btc_htf_s),
                                    float(scen_htf_min),
                                    enabled=bool(btc_soft_enabled),
                                )
                                if applied:
                                    scn_conf = new_conf
                                    _btc_soft_penalty_applied = True
                            except Exception:
                                pass
                            # ── Liquidity sweep hint (non-invasive, meta only) ──
                            try:
                                import config.config as _cfg  # type: ignore  # noqa: E402

                                if bool(getattr(_cfg, "LIQ_SWEEP_HINT_ENABLED", True)):
                                    st = normalized.get("stats", {}) or {}
                                    # Доступні базові фічі з stats_for_phase
                                    near_edge = (stats_for_phase or {}).get("near_edge")
                                    try:
                                        band_pct = float(
                                            (stats_for_phase or {}).get("band_pct")
                                            or 1.0
                                        )
                                    except Exception:
                                        band_pct = 1.0
                                    try:
                                        dvr_val = float(
                                            (stats_for_phase or {}).get(K_DIRECTIONAL_VOLUME_RATIO)  # type: ignore[arg-type]
                                            or (
                                                st.get("dvr")
                                            )  # fallback з normalized.stats
                                            or 0.0
                                        )
                                    except Exception:
                                        dvr_val = 0.0
                                    try:
                                        htf_s = float(
                                            (stats_for_phase or {}).get("htf_strength")
                                            or 0.0
                                        )
                                    except Exception:
                                        htf_s = 0.0
                                    now_ms = int(time.time() * 1000)
                                    cp = None
                                    try:
                                        cp = float((st or {}).get("current_price"))
                                    except Exception:
                                        cp = None
                                    # Пороги з конфігу
                                    try:
                                        dvr_min = float(
                                            getattr(_cfg, "LIQ_SWEEP_DVR_MIN", 3.0)
                                        )
                                    except Exception:
                                        dvr_min = 3.0
                                    try:
                                        bp_max = float(
                                            getattr(
                                                _cfg, "LIQ_SWEEP_BAND_PCT_MAX", 0.012
                                            )
                                        )
                                    except Exception:
                                        bp_max = 0.012
                                    try:
                                        dwell_bars = int(
                                            getattr(_cfg, "LIQ_SWEEP_MIN_DWELL_BARS", 2)
                                        )
                                    except Exception:
                                        dwell_bars = 2
                                    try:
                                        cooldown_ms = (
                                            int(
                                                getattr(
                                                    _cfg, "LIQ_SWEEP_COOLDOWN_S", 120
                                                )
                                            )
                                            * 1000
                                        )
                                    except Exception:
                                        cooldown_ms = 120000
                                    try:
                                        scen_htf_min = float(
                                            getattr(_cfg, "SCEN_HTF_MIN", 0.20)
                                        )
                                    except Exception:
                                        scen_htf_min = 0.20

                                    side = None
                                    if isinstance(
                                        near_edge, str
                                    ) and near_edge.lower() in {"upper", "lower"}:
                                        side = near_edge.lower()
                                    elif near_edge is True:
                                        # Якщо детектор повернув True без сторони — залишимо side невідомим
                                        side = "edge"

                                    weak_ctx = (
                                        str(btc_regime or "").lower() == "flat"
                                    ) or (htf_s < scen_htf_min)
                                    tight_band = band_pct <= bp_max
                                    high_participation = dvr_val >= dvr_min
                                    # Дебаунс: dwell + cooldown
                                    dwell_ok = False
                                    if side:
                                        dq = _EDGE_DWELL.setdefault(
                                            lower_symbol,
                                            deque(maxlen=max(2, dwell_bars)),
                                        )
                                        dq.append(str(side))
                                        dwell_ok = len(dq) >= dwell_bars and all(
                                            x == str(side) for x in dq
                                        )
                                    cooldown_ok = bool(
                                        now_ms
                                        - int(_SWEEP_LAST_TS_MS.get(lower_symbol, 0))
                                        >= cooldown_ms
                                    )

                                    if (
                                        side
                                        and weak_ctx
                                        and tight_band
                                        and high_participation
                                        and dwell_ok
                                        and cooldown_ok
                                    ):
                                        # Записати підказку в meta (не впливає на рішення Stage1/2/3)
                                        _meta_ls = normalized.setdefault(
                                            "market_context", {}
                                        ).setdefault("meta", {})
                                        _meta_ls["liquidity_sweep_hint"] = {
                                            "ts_ms": int(now_ms),
                                            "side": side,
                                            "price": float(cp or 0.0),
                                            "dvr_raw": round(float(dvr_val), 3),
                                            "band_pct": round(float(band_pct), 5),
                                            "htf_strength": round(float(htf_s), 4),
                                            "btc_regime": str(btc_regime or ""),
                                        }
                                        _SWEEP_LAST_TS_MS[lower_symbol] = int(now_ms)
                                        if isinstance(cp, (int, float)):
                                            _SWEEP_LEVEL[lower_symbol] = float(cp)
                                        _SWEEP_LAST_SIDE[lower_symbol] = str(side)
                                        try:
                                            logger.info(
                                                "[STRICT_VOLZ] %s LIQ_SWEEP_HINT side=%s dvr=%.2f band_pct=%.4f htf=%.4f btc=%s",
                                                lower_symbol,
                                                side,
                                                float(dvr_val),
                                                float(band_pct),
                                                float(htf_s),
                                                str(btc_regime or ""),
                                            )
                                        except Exception:
                                            pass
                                        # Prometheus one-shot gauge (TTL керується конфігом, підтримка нижче)
                                        try:
                                            if callable(set_liq_sweep):  # type: ignore[name-defined]
                                                set_liq_sweep(symbol.upper(), str(side), 1)  # type: ignore[misc]
                                            if callable(inc_liq_sweep_total):  # type: ignore[name-defined]
                                                inc_liq_sweep_total(symbol.upper(), str(side))  # type: ignore[misc]
                                        except Exception:
                                            pass
                                    # TTL підтримка для ai_one_liq_sweep
                                    try:
                                        last_ts = int(
                                            _SWEEP_LAST_TS_MS.get(lower_symbol, 0)
                                        )
                                        if last_ts and callable(set_liq_sweep):  # type: ignore[name-defined]
                                            try:
                                                from config.config import (
                                                    PROM_SWEEP_TTL_S as _SWEEP_TTL_S,
                                                )

                                                ttl_ms = int(float(_SWEEP_TTL_S) * 1000)
                                            except Exception:
                                                ttl_ms = 60000
                                            active = (
                                                1 if (now_ms - last_ts) <= ttl_ms else 0
                                            )
                                            sside = _SWEEP_LAST_SIDE.get(
                                                lower_symbol
                                            ) or str(side or "")
                                            if sside:
                                                set_liq_sweep(symbol.upper(), sside, int(active))  # type: ignore[misc]
                                    except Exception:
                                        pass
                            except Exception:
                                # non-fatal hinting
                                pass
                            # ── Post‑rules: sweep→breakout / sweep→reject / retest_ok (meta only) + bias ──
                            try:
                                last_ts = int(
                                    _SWEEP_LAST_TS_MS.get(lower_symbol, 0) or 0
                                )
                                lvl = float(_SWEEP_LEVEL.get(lower_symbol, 0.0) or 0.0)
                                if (
                                    last_ts
                                    and lvl > 0.0
                                    and isinstance(df, object)
                                    and len(df.index)
                                    >= 10  # фолбек: дозволяємо 10..19 як мінімум
                                ):
                                    age_ms = int(time.time() * 1000) - last_ts
                                    age_s = age_ms / 1000.0
                                    # 5m підтвердження breakout
                                    if age_ms >= 5 * 60 * 1000:
                                        try:
                                            # Агрегація 5×1m як фолбек для 5m
                                            vol5 = float((df["volume"].tail(5)).sum())
                                            # Медіана по доступному вікну (до 20), але не менше 10
                                            tail_n = max(
                                                10, min(20, int(len(df.index)))
                                            )
                                            med20 = float(
                                                (df["volume"].tail(tail_n)).median()
                                            )
                                            if (
                                                tail_n < 20
                                                and lower_symbol
                                                not in _SWEEP_WARN_SHORT_HISTORY
                                            ):
                                                _SWEEP_WARN_SHORT_HISTORY.add(
                                                    lower_symbol
                                                )
                                                try:
                                                    logger.warning(
                                                        "[STRICT_VOLZ] %s SWEEP_CHECK short history: using median(%d) instead of median(20)",
                                                        lower_symbol,
                                                        tail_n,
                                                    )
                                                except Exception:
                                                    pass
                                            close_now = float(df["close"].iloc[-1])
                                        except Exception:
                                            vol5, med20, close_now = 0.0, 0.0, 0.0
                                            try:
                                                logger.warning(
                                                    "[STRICT_VOLZ] %s SWEEP_CHECK missing 5m/median20 data for validation",
                                                    lower_symbol,
                                                )
                                            except Exception:
                                                pass
                                        if close_now > (lvl * 1.001) and vol5 > med20:
                                            mc = normalized.setdefault(
                                                "market_context", {}
                                            ).setdefault("meta", {})
                                            mc["sweep_then_breakout"] = True
                                            logger.info(
                                                "[STRICT_VOLZ] %s SWEEP_THEN_BREAKOUT level=%.2f close=%.2f vol5m=%.0f med20=%.0f",
                                                lower_symbol,
                                                lvl,
                                                close_now,
                                                vol5,
                                                med20,
                                            )
                                            try:
                                                if callable(inc_sweep_then_breakout):  # type: ignore[name-defined]
                                                    inc_sweep_then_breakout(symbol.upper())  # type: ignore[misc]
                                            except Exception:
                                                pass
                                    # 10–20m відмова (reject)
                                    # використовуємо вікно з конфігу (best-effort)
                                    try:
                                        import config.config as _cfg  # noqa: E402

                                        rej_win = float(
                                            getattr(_cfg, "SWEEP_REJECT_WINDOW_S", 1200)
                                        )
                                    except Exception:
                                        rej_win = 1200.0
                                    if 0 <= age_s <= float(rej_win):
                                        try:
                                            close_now = float(df["close"].iloc[-1])
                                        except Exception:
                                            close_now = 0.0
                                            try:
                                                logger.warning(
                                                    "[STRICT_VOLZ] %s SWEEP_REJECT_CHECK missing price data",
                                                    lower_symbol,
                                                )
                                            except Exception:
                                                pass
                                        try:
                                            vol_z_cur = float(
                                                (stats_for_phase or {}).get("volume_z")
                                                or 0.0
                                            )
                                        except Exception:
                                            vol_z_cur = 0.0
                                        # дрейф вниз більш ніж на REJECT_DRIFT_PCT і слабкий vol_z
                                        try:
                                            import config.config as _cfg  # noqa: E402

                                            rej_drift = float(
                                                getattr(_cfg, "REJECT_DRIFT_PCT", 0.002)
                                            )
                                        except Exception:
                                            rej_drift = 0.002
                                        if (close_now < (lvl * (1.0 - rej_drift))) and (
                                            vol_z_cur <= 0.0
                                        ):
                                            mc = normalized.setdefault(
                                                "market_context", {}
                                            ).setdefault("meta", {})
                                            mc["sweep_reject"] = True
                                            logger.info(
                                                "[STRICT_VOLZ] %s SWEEP_REJECT level=%.2f close=%.2f volz=%.2f",
                                                lower_symbol,
                                                lvl,
                                                close_now,
                                                vol_z_cur,
                                            )
                                            try:
                                                if callable(inc_sweep_reject):  # type: ignore[name-defined]
                                                    inc_sweep_reject(symbol.upper())  # type: ignore[misc]
                                            except Exception:
                                                pass
                                            # Встановлюємо контекстний bias=down із TTL
                                            try:
                                                import config.config as _cfg  # noqa: E402

                                                fsm_on = bool(
                                                    getattr(
                                                        _cfg, "SWEEP_FSM_ENABLED", True
                                                    )
                                                )
                                                bias_ttl = int(
                                                    getattr(_cfg, "BIAS_TTL_S", 1800)
                                                )
                                            except Exception:
                                                fsm_on, bias_ttl = True, 1800
                                            if fsm_on:
                                                mc["bias"] = {
                                                    "side": "down",
                                                    "ttl_ts": int(time.time())
                                                    + int(bias_ttl),
                                                    "set_ts": int(time.time()),
                                                }
                                                logger.info(
                                                    "[SWEEP] %s BIAS_SET side=down ttl_s=%d",
                                                    lower_symbol,
                                                    int(bias_ttl),
                                                )
                                                try:
                                                    if callable(set_bias_state):  # type: ignore[name-defined]
                                                        set_bias_state(symbol.upper(), -1)  # type: ignore[misc]
                                                except Exception:
                                                    pass
                                    # Retest‑лонг hint після breakout
                                    try:
                                        mc_meta = normalized.get(
                                            "market_context", {}
                                        ).get("meta", {})
                                        brk = bool(mc_meta.get("sweep_then_breakout"))
                                    except Exception:
                                        brk = False
                                    if brk:
                                        near_e = (stats_for_phase or {}).get(
                                            "near_edge"
                                        )
                                        try:
                                            htf_now = float(
                                                (stats_for_phase or {}).get(
                                                    "htf_strength"
                                                )
                                                or 0.0
                                            )
                                        except Exception:
                                            htf_now = 0.0
                                        try:
                                            dvr_now = float(
                                                (stats_for_phase or {}).get(
                                                    K_DIRECTIONAL_VOLUME_RATIO
                                                )
                                                or 0.0
                                            )
                                        except Exception:
                                            dvr_now = 0.0
                                        try:
                                            close_now = float(df["close"].iloc[-1])
                                        except Exception:
                                            close_now = 0.0
                                        try:
                                            import config.config as _cfg  # noqa: E402

                                            retest_prox_pct = float(
                                                getattr(_cfg, "RETEST_PROX_PCT", 0.001)
                                            )
                                            ret_htf_min = float(
                                                getattr(_cfg, "RETEST_HTF_MIN", 0.10)
                                            )
                                            ret_dvr_min = float(
                                                getattr(_cfg, "RETEST_DVR_MIN", 1.0)
                                            )
                                        except Exception:
                                            (
                                                retest_prox_pct,
                                                ret_htf_min,
                                                ret_dvr_min,
                                            ) = (0.001, 0.10, 1.0)
                                        within = (
                                            abs(close_now - lvl) / max(lvl, 1e-9)
                                        ) <= retest_prox_pct
                                        if (
                                            str(near_e).lower() == "upper"
                                            and within
                                            and htf_now >= ret_htf_min
                                            and dvr_now >= ret_dvr_min
                                        ):
                                            mc = normalized.setdefault(
                                                "market_context", {}
                                            ).setdefault("meta", {})
                                            mc["retest_ok"] = True
                                            logger.info(
                                                "[STRICT_VOLZ] %s RETEST_OK level=%.2f close=%.2f htf=%.3f dvr=%.2f",
                                                lower_symbol,
                                                lvl,
                                                close_now,
                                                htf_now,
                                                dvr_now,
                                            )
                                            # метрика з TTL
                                            try:
                                                from config.config import (  # noqa: E402
                                                    RETEST_OK_TTL_S as _RET_TTL_S,
                                                )

                                                ttl_s = int(_RET_TTL_S)
                                            except Exception:
                                                ttl_s = 120
                                            try:
                                                if callable(set_retest_ok):  # type: ignore[name-defined]
                                                    set_retest_ok(symbol.upper(), 1, ttl_s)  # type: ignore[misc]
                                            except Exception:
                                                pass
                                    # Інвалідація bias за рухом вище рівня ретесту +0.1% при DVR/об'ємах
                                    try:
                                        import config.config as _cfg  # noqa: E402

                                        fsm_on = bool(
                                            getattr(_cfg, "SWEEP_FSM_ENABLED", True)
                                        )
                                        ret_pct = float(
                                            getattr(_cfg, "RETEST_PROX_PCT", 0.001)
                                        )
                                    except Exception:
                                        fsm_on, ret_pct = True, 0.001
                                    if fsm_on:
                                        try:
                                            vol5 = float((df["volume"].tail(5)).sum())
                                            tail_n = max(
                                                10, min(20, int(len(df.index)))
                                            )
                                            med20 = float(
                                                (df["volume"].tail(tail_n)).median()
                                            )
                                            if (
                                                tail_n < 20
                                                and lower_symbol
                                                not in _SWEEP_WARN_SHORT_HISTORY
                                            ):
                                                _SWEEP_WARN_SHORT_HISTORY.add(
                                                    lower_symbol
                                                )
                                                try:
                                                    logger.warning(
                                                        "[STRICT_VOLZ] %s SWEEP_CHECK short history: using median(%d) instead of median(20)",
                                                        lower_symbol,
                                                        tail_n,
                                                    )
                                                except Exception:
                                                    pass
                                        except Exception:
                                            vol5, med20 = 0.0, 0.0
                                        try:
                                            dvr_now = float(
                                                (stats_for_phase or {}).get(
                                                    K_DIRECTIONAL_VOLUME_RATIO
                                                )
                                                or 0.0
                                            )
                                        except Exception:
                                            dvr_now = 0.0
                                        try:
                                            close_now = float(df["close"].iloc[-1])
                                        except Exception:
                                            close_now = 0.0
                                        if (
                                            (close_now > (lvl * (1.0 + float(ret_pct))))
                                            and (dvr_now >= 1.0)
                                            and (vol5 > med20)
                                        ):
                                            mc = normalized.setdefault(
                                                "market_context", {}
                                            ).setdefault("meta", {})
                                            if mc.get("bias"):
                                                mc["bias"] = None
                                                logger.info(
                                                    "[SWEEP] %s BIAS_INVALIDATE level=%.2f close=%.2f dvr=%.2f vol5=%.0f med20=%.0f",
                                                    lower_symbol,
                                                    lvl,
                                                    close_now,
                                                    dvr_now,
                                                    vol5,
                                                    med20,
                                                )
                                                try:
                                                    if callable(set_bias_state):  # type: ignore[name-defined]
                                                        set_bias_state(symbol.upper(), 0)  # type: ignore[misc]
                                                except Exception:
                                                    pass
                            except Exception:
                                pass
                            # ── М'які контекстні коефіцієнти (після BTC‑гейтів) ──
                            try:
                                import os as _os  # noqa: E402  # local import to avoid global dependency

                                import config.config as _cfg  # type: ignore  # noqa: E402

                                env_ctx = _os.getenv("SCEN_CONTEXT_WEIGHTS_ENABLED")
                                if env_ctx is not None:
                                    ctx_enabled = str(env_ctx).strip().lower() in {
                                        "1",
                                        "true",
                                        "yes",
                                        "on",
                                    }
                                else:
                                    ctx_enabled = bool(
                                        getattr(
                                            _cfg, "SCEN_CONTEXT_WEIGHTS_ENABLED", True
                                        )
                                    )

                                if scn_name and ctx_enabled:
                                    meta_ctx = normalized.get("market_context", {}).get(
                                        "meta", {}
                                    )
                                    cf = (
                                        meta_ctx.get("context_frame", {})
                                        if isinstance(meta_ctx, dict)
                                        else {}
                                    )
                                    st_map = (
                                        meta_ctx.get("structure", {})
                                        if isinstance(meta_ctx, dict)
                                        else {}
                                    )
                                    # compression + persistence
                                    comp = float(
                                        (cf.get("compression") or {}).get("index")
                                        or 0.0
                                    )
                                    nep = float(
                                        (cf.get("persistence") or {}).get(
                                            "near_edge_persist"
                                        )
                                        or 0.0
                                    )
                                    psu = float(
                                        (cf.get("persistence") or {}).get(
                                            "presence_sustain"
                                        )
                                        or 0.0
                                    )
                                    alpha = float(
                                        getattr(_cfg, "CONTEXT_ALPHA_COMPRESSION", 0.10)
                                    )
                                    beta = float(
                                        getattr(_cfg, "CONTEXT_BETA_ZONE", 0.10)
                                    )
                                    gamma = float(
                                        getattr(_cfg, "CONTEXT_GAMMA_BTC_TREND", 0.10)
                                    )
                                    # Зона: якщо поруч із валідною зоною (distance_to_next_zone==0)
                                    in_zone = bool(
                                        (st_map or {}).get("distance_to_next_zone")
                                        == 0.0
                                    )
                                    # BTC regime (one-hot) з context_frame.regime_timeline
                                    rt = (
                                        (cf.get("regime_timeline") or {})
                                        if isinstance(cf, dict)
                                        else {}
                                    )
                                    bullish = float(rt.get("trend_up") or 0.0)
                                    # Формула (м'які множники, обрізати до [0,1.0])
                                    mul = 1.0
                                    if comp >= 0.0 and max(nep, psu) >= 0.0:
                                        mul *= 1.0 + alpha * min(
                                            1.0, max(0.0, comp)
                                        ) * min(1.0, max(nep, psu))
                                    if in_zone:
                                        mul *= 1.0 + beta
                                    if scn_name in (
                                        "breakout_confirmation",
                                        "pullback_continuation",
                                    ):
                                        mul *= 1.0 + gamma * min(1.0, max(0.0, bullish))
                                    scn_conf = max(
                                        0.0, min(1.0, float(scn_conf or 0.0) * mul)
                                    )
                            except Exception:
                                pass
                            # Опціональна стабілізація (гістерезис): 2 бари поспіль або сума conf ≥ 0.30 за 3 бари
                            stable_scn = scn_name
                            stable_conf = float(scn_conf or 0.0)

                            # Підготуємо виміри для гістерезису
                            pres_val_h = 0.0
                            try:
                                _wh_h = (
                                    (stats_for_phase or {}).get("whale")
                                    if isinstance(
                                        (stats_for_phase or {}).get("whale"), dict
                                    )
                                    else {}
                                )
                                pres_val_h = float((_wh_h or {}).get("presence") or 0.0)
                            except Exception:
                                pres_val_h = 0.0
                            htf_val_h = 0.0
                            try:
                                htf_val_h = float(
                                    (stats_for_phase or {}).get("htf_strength") or 0.0
                                )
                            except Exception:
                                htf_val_h = 0.0

                            ring = _SCEN_HIST_RING.get(lower_symbol)
                            if ring is None:
                                ring = deque(maxlen=3)
                                _SCEN_HIST_RING[lower_symbol] = ring
                            ring.append(
                                {
                                    "scn": scn_name,
                                    "conf": float(scn_conf or 0.0),
                                    "presence": pres_val_h,
                                    "htf": htf_val_h,
                                    "ts": int(time.time() * 1000),
                                }
                            )

                            # ── Контекстне кільце (12 барів) та Prometheus-гейджі ──
                            try:
                                ctx_ring = _CONTEXT_RING.get(lower_symbol)
                                if ctx_ring is None:
                                    ctx_ring = deque(maxlen=12)
                                    _CONTEXT_RING[lower_symbol] = ctx_ring
                                ctx_ring.append(
                                    {
                                        "near_edge": stats_for_phase.get("near_edge"),
                                        "presence": pres_val_h,
                                    }
                                )
                                # Обчислити персистентність/стійкість і виставити гейджі (опційно)
                                try:
                                    from config.config import (  # noqa: E402
                                        SCEN_PULLBACK_PRESENCE_MIN as _PRES_MIN,
                                    )

                                    pres_thr = float(_PRES_MIN)
                                except Exception:
                                    pres_thr = 0.60
                                edge_persist, pres_sustain = compute_ctx_persist(
                                    ctx_ring, pres_thr
                                )
                                if (
                                    edge_persist is not None
                                    and pres_sustain is not None
                                ):
                                    if callable(set_context_near_edge_persist):  # type: ignore[name-defined]
                                        set_context_near_edge_persist(symbol.upper(), float(edge_persist))  # type: ignore[misc]
                                    if callable(set_context_presence_sustain):  # type: ignore[name-defined]
                                        set_context_presence_sustain(symbol.upper(), float(pres_sustain))  # type: ignore[misc]
                                    # Рейт-обмежений лог
                                    now = time.time()
                                    last = float(
                                        _PROM_CTX_LAST_TS.get(lower_symbol) or 0.0
                                    )
                                    if now - last >= 30.0:
                                        _PROM_CTX_LAST_TS[lower_symbol] = now
                                        logger.info(
                                            "[PROM] context set symbol=%s edge_persist=%.2f presence_sustain=%.2f",
                                            lower_symbol,
                                            float(edge_persist),
                                            float(pres_sustain),
                                        )
                            except Exception:
                                pass

                            if STRICT_SCENARIO_HYSTERESIS_ENABLED and scn_name:
                                hist = list(ring)
                                cond1 = False
                                if len(hist) >= 2:
                                    cond1 = (
                                        hist[-1]["scn"] == scn_name
                                        and hist[-2]["scn"] == scn_name
                                    )
                                sum_conf = sum(
                                    float(e.get("conf") or 0.0)
                                    for e in (hist[-3:] if len(hist) >= 1 else [])
                                    if e.get("scn") == scn_name
                                )
                                cond2 = sum_conf >= 0.30
                                # Для pullback_continuation — вимагаємо сталу присутність (2 останні бари)
                                if scn_name == "pullback_continuation":
                                    sustained_ok = False
                                    if len(hist) >= 2:
                                        last2 = hist[-2:]
                                        # Пороги з конфігу
                                        try:
                                            from config.config import (
                                                SCEN_PULLBACK_PRESENCE_MIN as _PRES_MIN,
                                            )

                                            pres_thr = float(_PRES_MIN)
                                        except Exception:
                                            pres_thr = 0.60
                                        try:
                                            from config.config import (
                                                SCEN_HTF_MIN as _SCEN_HTF_MIN,
                                            )

                                            htf_thr = float(_SCEN_HTF_MIN)
                                        except Exception:
                                            htf_thr = 0.20
                                        cnt = 0
                                        for e in last2:
                                            try:
                                                if (
                                                    float(e.get("presence") or 0.0)
                                                    >= pres_thr
                                                    and float(e.get("htf") or 0.0)
                                                    >= htf_thr
                                                ):
                                                    cnt += 1
                                            except Exception:
                                                pass
                                        sustained_ok = cnt >= 2
                                    cond1 = cond1 and sustained_ok
                                    cond2 = cond2 and sustained_ok

                                if not (cond1 or cond2):
                                    # Не підтвердився — не публікуємо сценарій
                                    stable_scn = None
                                    stable_conf = 0.0

                            # Публікація (з урахуванням гістерезису)
                            mc_meta = normalized.setdefault(
                                "market_context", {}
                            ).setdefault("meta", {})
                            # кешуємо btc_regime_state у meta (best-effort)
                            try:
                                if lower_symbol == "btcusdt":
                                    mc_meta["btc_regime_state"] = btc_state  # type: ignore[name-defined]
                            except Exception:
                                pass
                            prev_stable = _SCEN_LAST_STABLE.get(lower_symbol)
                            if stable_scn:
                                mc_meta["scenario_detected"] = stable_scn
                                mc_meta["scenario_confidence"] = float(stable_conf)
                                # Лог переходу (активація)
                                if prev_stable != stable_scn:
                                    _SCEN_LAST_STABLE[lower_symbol] = stable_scn
                                    try:
                                        logger.info(
                                            "[SCENARIO_ALERT] symbol=%s activate=%s conf=%.2f",
                                            lower_symbol,
                                            stable_scn,
                                            float(stable_conf),
                                        )
                                    except Exception:
                                        pass
                            else:
                                # Канарейкове публікування кандидата (нестабільного)
                                if SCENARIO_CANARY_PUBLISH_CANDIDATE and scn_name:
                                    try:
                                        mc_meta["scenario_detected"] = scn_name
                                        mc_meta["scenario_confidence"] = float(
                                            scn_conf or 0.0
                                        )
                                        logger.info(
                                            "[SCENARIO_TRACE] symbol=%s canary_publish=%s conf=%.2f",
                                            lower_symbol,
                                            scn_name,
                                            float(scn_conf or 0.0),
                                        )
                                    except Exception:
                                        # fallback до none нижче
                                        mc_meta.setdefault("scenario_detected", None)
                                        mc_meta.setdefault("scenario_confidence", 0.0)
                                else:
                                    # явно проставляємо none/0.0 для стабільності UI
                                    mc_meta.setdefault("scenario_detected", None)
                                    mc_meta.setdefault("scenario_confidence", 0.0)
                                if prev_stable is not None:
                                    # Очистка стабільного стану
                                    _SCEN_LAST_STABLE[lower_symbol] = None
                                    # Симетричний деактиваційний лог (rate-limited)
                                    try:
                                        now_ts = time.time()
                                        last_ts = float(
                                            _SCEN_ALERT_LAST_TS.get(lower_symbol) or 0.0
                                        )
                                        if now_ts - last_ts >= 10.0:
                                            _SCEN_ALERT_LAST_TS[lower_symbol] = now_ts
                                            # класифікація причини
                                            cause = "confidence_decay"
                                            try:
                                                hist = list(
                                                    _SCEN_HIST_RING.get(lower_symbol)
                                                    or []
                                                )
                                                # presence/htf перевірка для останніх 2 (пороги з конфігу)
                                                if len(hist) >= 1:
                                                    last = hist[-1]
                                                    try:
                                                        from config.config import (
                                                            SCEN_HTF_MIN as _SCEN_HTF_MIN,
                                                        )

                                                        htf_thr = float(_SCEN_HTF_MIN)
                                                    except Exception:
                                                        htf_thr = 0.20
                                                    if (
                                                        float(last.get("htf") or 0.0)
                                                        < htf_thr
                                                    ):
                                                        cause = "htf_weak"
                                                if (
                                                    prev_stable
                                                    == "pullback_continuation"
                                                    and len(hist) >= 2
                                                ):
                                                    last2 = hist[-2:]
                                                    try:
                                                        from config.config import (
                                                            SCEN_PULLBACK_PRESENCE_MIN as _PRES_MIN,
                                                        )

                                                        pres_thr = float(_PRES_MIN)
                                                    except Exception:
                                                        pres_thr = 0.60
                                                    try:
                                                        from config.config import (
                                                            SCEN_HTF_MIN as _SCEN_HTF_MIN,
                                                        )

                                                        htf_thr = float(_SCEN_HTF_MIN)
                                                    except Exception:
                                                        htf_thr = 0.20
                                                    ok_cnt = 0
                                                    for e in last2:
                                                        try:
                                                            if (
                                                                float(
                                                                    e.get("presence")
                                                                    or 0.0
                                                                )
                                                                >= pres_thr
                                                                and float(
                                                                    e.get("htf") or 0.0
                                                                )
                                                                >= htf_thr
                                                            ):
                                                                ok_cnt += 1
                                                        except Exception:
                                                            pass
                                                    if ok_cnt < 2:
                                                        cause = "presence_drop"
                                                # якщо кандидат змінився на інший сценарій — phase_change
                                                cand = None
                                                if len(hist) >= 1:
                                                    cand = hist[-1].get("scn")
                                                if cand and cand != prev_stable:
                                                    cause = "phase_change"
                                                # попередня conf (по гісторії для prev_scn)
                                                prev_conf = 0.0
                                                for e in reversed(hist):
                                                    if e.get("scn") == prev_stable:
                                                        try:
                                                            prev_conf = float(
                                                                e.get("conf") or 0.0
                                                            )
                                                        except Exception:
                                                            prev_conf = 0.0
                                                        break
                                            except Exception:
                                                cause = "confidence_decay"
                                                prev_conf = 0.0
                                            logger.info(
                                                "[SCENARIO_ALERT] symbol=%s deactivate=%s cause=%s conf_prev=%.2f",
                                                lower_symbol,
                                                prev_stable,
                                                cause,
                                                float(prev_conf),
                                            )
                                            # Prometheus: лічильник відмов за конкретною причиною
                                            try:
                                                if callable(inc_scenario_reject):  # type: ignore[arg-type]
                                                    inc_scenario_reject(symbol.upper(), str(cause))  # type: ignore[misc]
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass
                            # Prometheus (опційно)
                            try:
                                if callable(set_scenario):  # type: ignore[arg-type]
                                    # Для канарейки — віддаємо кандидат, якщо стабільного немає
                                    if stable_scn:
                                        set_scenario(
                                            symbol.upper(),
                                            stable_scn,
                                            float(stable_conf),
                                        )  # type: ignore[misc]
                                    elif SCENARIO_CANARY_PUBLISH_CANDIDATE and scn_name:
                                        set_scenario(
                                            symbol.upper(),
                                            scn_name,
                                            float(scn_conf or 0.0),
                                        )  # type: ignore[misc]
                                    else:
                                        set_scenario(
                                            symbol.upper(),
                                            "none",
                                            0.0,
                                        )  # type: ignore[misc]
                                if callable(set_phase):  # type: ignore[arg-type]
                                    cur_phase = str(phase_info.get("name") or "none")
                                    cur_score = float(
                                        phase_info.get("score", 0.0) or 0.0
                                    )
                                    last = _LAST_PHASE.get(symbol.upper())
                                    if last and last != cur_phase:
                                        try:
                                            set_phase(symbol.upper(), last, 0.0)  # type: ignore[misc]
                                        except Exception:
                                            pass
                                    if cur_phase != "none" and cur_score > 0.0:
                                        set_phase(symbol.upper(), cur_phase, cur_score)  # type: ignore[misc]
                                        _LAST_PHASE[symbol.upper()] = cur_phase
                                    else:
                                        if last:
                                            try:
                                                set_phase(symbol.upper(), last, 0.0)  # type: ignore[misc]
                                            except Exception:
                                                pass
                                        _LAST_PHASE.pop(symbol.upper(), None)
                            except Exception:
                                pass
                            # JSONL‑слід рішення (мінімальний набір полів)
                            try:
                                if callable(write_decision_trace):  # type: ignore[arg-type]
                                    # Актуальне ім'я сценарію/конфіденція для дампу: стабільний або кандидат
                                    _scn = stable_scn or scn_name or None
                                    _conf = (
                                        float(stable_conf)
                                        if stable_scn
                                        else float(scn_conf or 0.0)
                                    )
                                    # bias_state з meta (виставляється раніше)
                                    try:
                                        meta_obj = normalized.get(
                                            "market_context", {}
                                        ).get("meta", {})
                                        _bias_state = int(meta_obj.get("bias_state", 0))
                                    except Exception:
                                        _bias_state = 0
                                    _bundle = {
                                        "ts": int(time.time() * 1000),
                                        "symbol": symbol.upper(),
                                        "phase": str(phase_info.get("name") or "none"),
                                        "phase_score": float(
                                            phase_info.get("score", 0.0) or 0.0
                                        ),
                                        # reasons як компактний рядок
                                        "phase_reasons": "+".join(
                                            phase_info.get("reasons", []) or []
                                        ),
                                        "scenario": _scn or "none",
                                        "scenario_conf": float(_conf or 0.0),
                                        "bias_state": int(_bias_state),
                                        "btc_regime": str(btc_regime or ""),
                                        "btc_htf_strength": float(btc_htf_s or 0.0),
                                        # Вибрані Stage1 фічі
                                        "band_pct": (stats_for_phase or {}).get(
                                            "band_pct"
                                        ),
                                        "near_edge": (stats_for_phase or {}).get(
                                            "near_edge"
                                        ),
                                        "atr_ratio": (stats_for_phase or {}).get(
                                            "atr_ratio"
                                        ),
                                        "vol_z": (stats_for_phase or {}).get(
                                            "volume_z"
                                        ),
                                        "rsi": (stats_for_phase or {}).get("rsi"),
                                        "dvr": (stats_for_phase or {}).get(
                                            K_DIRECTIONAL_VOLUME_RATIO
                                        ),
                                        "cd": (stats_for_phase or {}).get(
                                            K_CUMULATIVE_DELTA
                                        ),
                                        "slope_atr": (stats_for_phase or {}).get(
                                            K_PRICE_SLOPE_ATR
                                        ),
                                    }
                                    # best‑effort запис у JSONL (guarded by flag усередині функції)
                                    write_decision_trace(symbol.upper(), _bundle)  # type: ignore[misc]
                                    # компактний покажчик у meta для UI/інспекції
                                    try:
                                        mc_meta.setdefault("decision_last_ts", _bundle["ts"])  # type: ignore[index]
                                        mc_meta.setdefault("decision_phase", _bundle["phase"])  # type: ignore[index]
                                        mc_meta.setdefault("decision_scenario", _bundle["scenario"])  # type: ignore[index]
                                        mc_meta.setdefault("decision_confidence", _bundle["scenario_conf"])  # type: ignore[index]
                                    except Exception:
                                        pass
                            except Exception:
                                # ніколи не впливає на основний шлях
                                pass
                            # EvidenceBus: фініш і прикріплення у meta
                            try:
                                from utils.evidence_bus import (
                                    finish as _ev_finish,  # noqa: E402
                                )

                                ev_list, ev_explain = _ev_finish(symbol.upper())
                                if ev_list:
                                    mc_meta["evidence"] = ev_list
                                if ev_explain:
                                    mc_meta["scenario_explain"] = ev_explain
                                # Рейт-лімітований explain-лог (≤1/10с/символ) + кожен N‑й батч
                                try:
                                    if SCEN_EXPLAIN_ENABLED:
                                        now_ts = time.time()
                                        # оновлюємо лічильник батчів для символу
                                        bidx = (
                                            int(
                                                _SCEN_EXPLAIN_BATCH_COUNTER.get(
                                                    lower_symbol, 0
                                                )
                                                or 0
                                            )
                                            + 1
                                        )
                                        _SCEN_EXPLAIN_BATCH_COUNTER[lower_symbol] = bidx
                                        should_log = explain_should_log(
                                            lower_symbol, now_ts, 10.0
                                        ) or (
                                            SCEN_EXPLAIN_VERBOSE_EVERY_N > 0
                                            and (
                                                bidx % int(SCEN_EXPLAIN_VERBOSE_EVERY_N)
                                                == 0
                                            )
                                        )
                                        # Додатковий форс‑вивід, якщо змінна оточення увімкнена
                                        if _SCEN_EXPLAIN_FORCE_ALL:
                                            should_log = True
                                        if should_log:
                                            _SCEN_EXPLAIN_LAST_TS[lower_symbol] = now_ts
                                            scen_tag = str(
                                                mc_meta.get("scenario_detected")
                                                or "none"
                                            )
                                            logger.info(
                                                '[SCEN_EXPLAIN] symbol=%s scenario=%s explain="%s"',
                                                lower_symbol,
                                                scen_tag,
                                                str(ev_explain or ""),
                                            )
                                            try:
                                                if callable(inc_explain_line):  # type: ignore[arg-type]
                                                    inc_explain_line(symbol.upper())  # type: ignore[misc]
                                                if scen_tag == "none" and callable(inc_explain_heartbeat):  # type: ignore[arg-type]
                                                    inc_explain_heartbeat(symbol.upper())  # type: ignore[misc]
                                            except Exception:
                                                pass
                                except Exception:
                                    pass
                            except Exception:
                                pass
                            # Scenario trace (рейт-лімітований)
                            try:
                                if SCENARIO_TRACE_ENABLED:
                                    now_ts = time.time()
                                    last = float(
                                        _SCEN_TRACE_LAST_TS.get(lower_symbol) or 0.0
                                    )
                                    if now_ts - last >= 10.0:
                                        _SCEN_TRACE_LAST_TS[lower_symbol] = now_ts
                                        # побудова pred-словника для ключових сигналів
                                        pred = {
                                            "near_edge": stats_for_phase.get(
                                                "near_edge"
                                            ),
                                            "dvr": stats_for_phase.get(
                                                K_DIRECTIONAL_VOLUME_RATIO
                                            ),
                                            "vol_z": stats_for_phase.get("volume_z"),
                                            "htf_strength": stats_for_phase.get(
                                                "htf_strength"
                                            ),
                                            "presence": (
                                                (
                                                    stats_for_phase.get("whale") or {}
                                                ).get("presence")
                                                if isinstance(
                                                    stats_for_phase.get("whale"), dict
                                                )
                                                else None
                                            ),
                                        }
                                        # BTC soft gate контекст у TRACE
                                        try:
                                            pred["btc_regime"] = btc_regime
                                            pred["btc_htf_strength"] = float(btc_htf_s)
                                            if _btc_soft_penalty_applied:
                                                pred["penalty"] = 0.2
                                        except Exception:
                                            pass
                                        decision = "ACCEPT" if scn_name else "REJECT"
                                        reason = f"phase={phase_info.get('name')} conf={float(scn_conf or 0.0):.2f}"
                                        # Додаткові причини REJECT для прозорості
                                        if decision == "REJECT":
                                            extra: list[str] = []
                                            try:
                                                # presence
                                                p = pred.get("presence")
                                                try:
                                                    from config.config import (  # noqa: E402
                                                        SCEN_PULLBACK_PRESENCE_MIN as _PRES_MIN,
                                                    )

                                                    pres_thr = float(_PRES_MIN)
                                                except Exception:
                                                    pres_thr = 0.60
                                                if p is None:
                                                    extra.append("presence_missing")
                                                else:
                                                    try:
                                                        if float(p) < pres_thr:
                                                            extra.append(
                                                                "presence_below_min"
                                                            )
                                                    except Exception:
                                                        extra.append(
                                                            "presence_below_min"
                                                        )
                                                # htf
                                                try:
                                                    from config.config import (  # noqa: E402
                                                        SCEN_HTF_MIN as _SCEN_HTF_MIN,
                                                    )

                                                    htf_thr = float(_SCEN_HTF_MIN)
                                                except Exception:
                                                    htf_thr = 0.20
                                                try:
                                                    hs = pred.get("htf_strength")
                                                    if (
                                                        hs is not None
                                                        and float(hs) < htf_thr
                                                    ):
                                                        extra.append("htf_below_min")
                                                except Exception:
                                                    pass
                                                # BTC soft penalty
                                                try:
                                                    if _btc_soft_penalty_applied:
                                                        extra.append("btc_flat_penalty")
                                                except Exception:
                                                    pass
                                            except Exception:
                                                pass
                                            if extra:
                                                reason = (
                                                    f"{reason} causes={','.join(extra)}"
                                                )
                                        # Prometheus: лічильник відмов (низька кардинальність причин)
                                        try:
                                            if decision == "REJECT" and callable(inc_scenario_reject):  # type: ignore[arg-type]
                                                inc_scenario_reject(symbol.upper(), "no_candidate")  # type: ignore[misc]
                                        except Exception:
                                            pass
                                        logger.info(
                                            "[SCENARIO_TRACE] symbol=%s candidate=%s pred=%s decision=%s reason=%s",
                                            lower_symbol,
                                            (scn_name or "-"),
                                            pred,
                                            decision,
                                            reason,
                                        )
                            except Exception:
                                pass
                        except Exception:
                            # не ламаємо основний шлях
                            pass
                        # Prometheus: оновлюємо HTF та presence (якщо доступні)
                        try:
                            htf_val = 0.0
                            try:
                                htf_val = float(
                                    (stats_for_phase or {}).get("htf_strength", 0.0)
                                    or 0.0
                                )
                            except Exception:
                                htf_val = 0.0
                            if callable(set_htf_strength):  # type: ignore[arg-type]
                                set_htf_strength(symbol.upper(), float(htf_val))  # type: ignore[misc]
                            # presence: лише якщо є в state; лог рейт-обмежений
                            emit_prom_presence(stats_for_phase, symbol)
                        except Exception:
                            pass
                        try:
                            logger.info(
                                "[STRICT_PHASE] %s phase=%s score=%.2f reasons=%s strategy_hint=%s",
                                lower_symbol,
                                phase_info.get("name"),
                                float(phase_info.get("score", 0.0) or 0.0),
                                "+".join(phase_info.get("reasons", []) or []),
                                phase_info.get("strategy_hint"),
                            )
                            try:
                                # Базовий payload події фази
                                _stats_for_payload = normalized.get("stats") or {}
                                _payload: dict[str, Any] = {
                                    "phase": phase_info.get("name"),
                                    "score": float(phase_info.get("score", 0.0) or 0.0),
                                    "reasons": phase_info.get("reasons"),
                                    "strategy_hint": phase_info.get("strategy_hint"),
                                    "htf_ok": _stats_for_payload.get("htf_ok"),
                                    "htf_score": _stats_for_payload.get("htf_score"),
                                    "htf_strength": _stats_for_payload.get(
                                        "htf_strength"
                                    ),
                                }

                                # За потреби збагачуємо мінімальним дашборд‑набором
                                if TELEM_ENRICH_PHASE_PAYLOAD:
                                    try:
                                        # Базові Stage1 статки
                                        _payload.update(
                                            {
                                                "band_pct": _stats_for_payload.get(
                                                    "band_pct"
                                                ),
                                                "near_edge": _stats_for_payload.get(
                                                    "near_edge"
                                                ),
                                                "atr_ratio": _stats_for_payload.get(
                                                    "atr_ratio"
                                                ),
                                                "vol_z": _stats_for_payload.get(
                                                    "volume_z"
                                                ),
                                                "rsi": _stats_for_payload.get("rsi"),
                                                "atr": _stats_for_payload.get("atr"),
                                                "current_price": _stats_for_payload.get(
                                                    "current_price"
                                                ),
                                                # Directional
                                                "dvr": _stats_for_payload.get(
                                                    K_DIRECTIONAL_VOLUME_RATIO
                                                ),
                                                "cd": _stats_for_payload.get(
                                                    K_CUMULATIVE_DELTA
                                                ),
                                                "slope_atr": _stats_for_payload.get(
                                                    K_PRICE_SLOPE_ATR
                                                ),
                                                # Джерело vol_z із phase_detector (якщо доступне)
                                                "volz_source": phase_info.get(
                                                    "volz_source"
                                                ),
                                            }
                                        )
                                        # Контекстні причини та теги (якщо є у stats)
                                        try:
                                            if isinstance(
                                                _stats_for_payload.get(
                                                    "context_only_reasons"
                                                ),
                                                list,
                                            ):
                                                _payload["context_only_reasons"] = (
                                                    _stats_for_payload.get(
                                                        "context_only_reasons"
                                                    )
                                                )
                                        except Exception:
                                            pass
                                        try:
                                            if isinstance(
                                                _stats_for_payload.get("tags"), list
                                            ):
                                                _payload["tags"] = (
                                                    _stats_for_payload.get("tags")
                                                )
                                        except Exception:
                                            pass
                                        # Whale (якщо вбудовано)
                                        _wh = (
                                            _stats_for_payload.get("whale")
                                            if isinstance(
                                                _stats_for_payload.get("whale"), dict
                                            )
                                            else {}
                                        )
                                        if isinstance(_wh, dict) and _wh:
                                            _payload.update(
                                                {
                                                    "whale_presence": _wh.get(
                                                        "presence"
                                                    ),
                                                    "whale_bias": _wh.get("bias"),
                                                    "whale_vwap_dev": _wh.get(
                                                        "vwap_dev"
                                                    ),
                                                    "whale_vol_regime": _wh.get(
                                                        "vol_regime"
                                                    ),
                                                    "whale_age_ms": _wh.get("age_ms"),
                                                    "whale_stale": _wh.get("stale"),
                                                }
                                            )
                                            _zones = (
                                                _wh.get("zones_summary")
                                                if isinstance(
                                                    _wh.get("zones_summary"), dict
                                                )
                                                else {}
                                            )
                                            _payload.update(
                                                {
                                                    "whale_accum_cnt": _zones.get(
                                                        "accum_cnt"
                                                    ),
                                                    "whale_dist_cnt": _zones.get(
                                                        "dist_cnt"
                                                    ),
                                                }
                                            )
                                        # TRAP score (із stats.trap.score або прямий trap_score)
                                        _trap = (
                                            _stats_for_payload.get("trap")
                                            if isinstance(
                                                _stats_for_payload.get("trap"), dict
                                            )
                                            else {}
                                        )
                                        if isinstance(_trap, dict) and "score" in _trap:
                                            _payload["trap_score"] = _trap.get("score")
                                        elif "trap_score" in _stats_for_payload:
                                            _payload["trap_score"] = (
                                                _stats_for_payload.get("trap_score")
                                            )
                                    except Exception:
                                        # Best‑effort: збагачення не повинно ламати пайплайн
                                        logger.debug(
                                            "[TELEM] %s: enrich payload failed",
                                            lower_symbol,
                                        )

                                await log_stage1_event(
                                    event="phase_detected",
                                    symbol=lower_symbol,
                                    payload=_payload,
                                )
                            except Exception:
                                logger.debug(
                                    "[TELEM] %s: phase_detected log failed",
                                    lower_symbol,
                                )
                        except Exception:
                            logger.debug(
                                "[STRICT_PHASE] %s: логування не вдалося", lower_symbol
                            )
                except Exception as exc:
                    logger.error(
                        "[STRICT_PHASE] %s: помилка фазування: %s", lower_symbol, exc
                    )

                # signal_v2: невтручальний роутер на базі whale + directional контексту
                try:
                    if STAGE2_SIGNAL_V2_ENABLED:
                        st = normalized.setdefault("stats", {})
                        profile_cfg = STAGE2_SIGNAL_V2_PROFILES.get(
                            STAGE2_PROFILE
                        ) or STAGE2_SIGNAL_V2_PROFILES.get("strict", {})
                        sig, conf, reasons = router_signal_v2(st, profile_cfg)
                        st["signal_v2"] = sig
                        st["signal_v2_conf"] = conf
                        if reasons:
                            st["signal_v2_reasons"] = reasons
                        try:
                            await log_stage1_event(
                                event="signal_v2_emitted",
                                symbol=lower_symbol,
                                payload={
                                    "sig": sig,
                                    "conf": conf,
                                },
                            )
                        except Exception:
                            logger.debug(
                                "[TELEM] %s: signal_v2_emitted log failed", lower_symbol
                            )
                except Exception as exc:
                    logger.error(
                        "[SIGNAL_V2] %s: помилка роутера: %s", lower_symbol, exc
                    )

                # Policy Engine v2: керовані сигнали у market_context.meta (контракти не змінюємо)
                try:
                    from config import (
                        config as _cfg,  # локальний import, щоби уникати циклів
                    )

                    if (
                        getattr(_cfg, "POLICY_V2_ENABLED", False)
                        and policy_engine_v2 is not None
                    ):
                        st = normalized.setdefault("stats", {})
                        ds_ctx = normalized.get("datastore") or {}
                        dec = policy_engine_v2.decide(st, ds_ctx, flags={})  # type: ignore[attr-defined]
                        if dec is not None:
                            mc_meta = normalized.setdefault(
                                "market_context", {}
                            ).setdefault("meta", {})
                            mc_meta["signal_v2"] = asdict(dec)
                            # Метрики (best-effort)
                            try:
                                symbol_up = lower_symbol.upper()
                                from telemetry import prom_gauges as prom

                                # Гейджі (без дебаунсу)
                                prom.set_signal_pwin(symbol_up, float(dec.p_win))
                                prom.set_ramp(
                                    symbol_up, float(dec.features.get("rs", 0.0))
                                )
                                prom.set_absorption(
                                    symbol_up, float(dec.features.get("abs", 0.0))
                                )
                                # one-hot позначення активного режиму (без обнулення інших)
                                regimes = policy_engine_v2.infer_regime(st)
                                if regimes:
                                    regime_arg = max(
                                        regimes.items(), key=lambda kv: kv[1]
                                    )[0]
                                    prom.set_regime_onehot(symbol_up, regime_arg)
                                # Лічильники: завжди позначаємо readiness, emit — під cooldown
                                prom.inc_signal_ready(symbol_up, dec.class_)
                                try:
                                    cool_s = float(
                                        getattr(
                                            _cfg,
                                            "COOLDOWN_S",
                                            getattr(_cfg, "FSM_COOLDOWN_S", 90),
                                        )
                                    )
                                except Exception:
                                    cool_s = 90.0
                                now_ts = time.time()
                                key = (symbol_up, dec.class_)
                                last_ts = _POLICY_V2_LAST_SIGNAL_TS.get(key)
                                if not last_ts or (now_ts - float(last_ts)) >= cool_s:
                                    prom.inc_signal(symbol_up, dec.class_)
                                    _POLICY_V2_LAST_SIGNAL_TS[key] = now_ts
                                    # Опційно: проброс до Stage3 paper‑раннера під прапорами
                                    try:
                                        if bool(
                                            getattr(
                                                _cfg,
                                                "POLICY_V2_TO_STAGE3_ENABLED",
                                                False,
                                            )
                                        ) and bool(
                                            getattr(_cfg, "STAGE3_PAPER_ENABLED", False)
                                        ):
                                            try:
                                                from tools.run_stage3_canary import (
                                                    handle_scenario_alert as _paper_emit,
                                                )

                                                direction = (
                                                    "BUY"
                                                    if str(dec.side).lower() == "long"
                                                    else "SELL"
                                                )
                                                meta = {
                                                    "source": "policy_v2",
                                                    "class": dec.class_,
                                                    "p_win": float(dec.p_win),
                                                    "tp_atr": float(
                                                        getattr(dec, "tp_atr", 0.0)
                                                        or 0.0
                                                    ),
                                                    "sl_atr": float(
                                                        getattr(dec, "sl_atr", 0.0)
                                                        or 0.0
                                                    ),
                                                    "ttl_s": int(
                                                        getattr(dec, "ttl_s", 0) or 0
                                                    ),
                                                    "ts_ms": int(
                                                        getattr(dec, "ts_ms", 0) or 0
                                                    ),
                                                }
                                                _paper_emit(
                                                    symbol_up,
                                                    str(dec.class_),
                                                    float(dec.p_win),
                                                    direction,
                                                    reason="policy_v2",
                                                    meta=meta,
                                                )
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                except Exception as exc:
                    logger.error(
                        "[SIGNAL_V2] %s: помилка policy_v2: %s", lower_symbol, exc
                    )

                # ── Manipulation Insight (OBSERVE/WAIT only, не впливає на Stage2/3) ──
                try:
                    from config import config as _cfg

                    if bool(getattr(_cfg, "MANIPULATION_OBSERVER_ENABLED", True)):
                        # Легка інтеграція спостережного шару: будуємо фічі зі stats і додаємо concise‑пояснення в market_context.meta
                        from stage2.manipulation_detector import (
                            Features as _ManipFeatures,
                        )
                        from stage2.manipulation_detector import (
                            attach_to_market_context as _attach_manip,
                        )
                        from stage2.manipulation_detector import (
                            detect_manipulation as _detect_manip,
                        )

                        st = normalized.setdefault("stats", {})
                        mc = normalized.setdefault("market_context", {})

                        # Базові ціни (fallback на current_price, якщо немає OHLC у stats)
                        try:
                            c = float(st.get("current_price") or 0.0)
                        except Exception:
                            c = 0.0
                        try:
                            open_ = float(st.get("open") or c)
                        except Exception:
                            open_ = c
                        try:
                            h = float(st.get("high") or c)
                        except Exception:
                            h = c
                        try:
                            low_ = float(st.get("low") or c)
                        except Exception:
                            low_ = c
                        try:
                            atr = float(st.get("atr") or 0.0)
                        except Exception:
                            atr = 0.0

                        # Вторинні фічі (best‑effort)
                        volume_z = st.get("volume_z")
                        trade_count_z = st.get("trade_count_z")
                        near_edge = st.get("near_edge")
                        band_pct = st.get("band_pct")
                        htf_strength = st.get("htf_strength")
                        cumulative_delta = st.get("cumulative_delta") or st.get("cd")
                        price_slope_atr = st.get("price_slope_atr")
                        wh = st.get("whale") or {}
                        whale_presence = (wh or {}).get("presence")
                        whale_bias = (wh or {}).get("bias")
                        whale_stale = (
                            bool((wh or {}).get("stale"))
                            if isinstance((wh or {}).get("stale"), bool)
                            else (wh or {}).get("stale")
                        )

                        # range_z: якщо відсутній у stats — розрахунок (h‑l)/ATR
                        try:
                            rng_z = st.get("range_z")
                            if rng_z is None:
                                rng_z = (
                                    ((h - low_) / max(atr, 1e-9))
                                    if atr and atr > 0
                                    else None
                                )
                        except Exception:
                            rng_z = None

                        manip_f = _ManipFeatures(
                            symbol=lower_symbol.upper(),
                            o=float(open_),
                            h=float(h),
                            low=float(low_),
                            c=float(c),
                            atr=float(atr),
                            volume_z=(
                                volume_z if isinstance(volume_z, (int, float)) else None
                            ),
                            range_z=(
                                rng_z if isinstance(rng_z, (int, float)) else None
                            ),
                            trade_count_z=(
                                trade_count_z
                                if isinstance(trade_count_z, (int, float))
                                else None
                            ),
                            near_edge=(
                                near_edge
                                if near_edge in ("upper", "lower", "none")
                                else None
                            ),
                            band_pct=(
                                band_pct if isinstance(band_pct, (int, float)) else None
                            ),
                            htf_strength=(
                                htf_strength
                                if isinstance(htf_strength, (int, float))
                                else None
                            ),
                            cumulative_delta=(
                                cumulative_delta
                                if isinstance(cumulative_delta, (int, float))
                                else None
                            ),
                            price_slope_atr=(
                                price_slope_atr
                                if isinstance(price_slope_atr, (int, float))
                                else None
                            ),
                            whale_presence=(
                                whale_presence
                                if isinstance(whale_presence, (int, float))
                                else None
                            ),
                            whale_bias=(
                                whale_bias
                                if isinstance(whale_bias, (int, float))
                                else None
                            ),
                            whale_stale=(
                                bool(whale_stale)
                                if isinstance(whale_stale, bool)
                                else (
                                    whale_stale
                                    if whale_stale in (True, False)
                                    else None
                                )
                            ),
                        )
                        _attach_manip(mc, _detect_manip(manip_f))
                except Exception:
                    # Шар спостереження не повинен ламати пайплайн
                    pass

                # Публікація
                signal_val = str(normalized.get(K_SIGNAL, "")).upper()
                prev_entry = state_manager.state.get(lower_symbol, {})
                prev_signal = str(prev_entry.get(K_SIGNAL, "")).upper()
                was_alert = prev_signal.startswith("ALERT")
                is_alert = signal_val.startswith("ALERT")

                stats_for_session = normalized.setdefault(K_STATS, stats_container)
                price_now = None
                atr_pct_now = None
                try:
                    price_now = float(stats_for_session.get("current_price"))
                except Exception:
                    price_now = None
                try:
                    atr_val = float(stats_for_session.get("atr"))
                    if atr_val and price_now:
                        atr_pct_now = atr_val / price_now
                except Exception:
                    atr_pct_now = stats_for_session.get("atr_pct")
                try:
                    rsi_now = float(stats_for_session.get("rsi"))
                except Exception:
                    rsi_now = None
                htf_ok_now = stats_for_session.get("htf_ok")
                try:
                    band_pct_now = float(stats_for_session.get("band_pct"))
                except Exception:
                    band_pct_now = None
                near_edge_now = stats_for_session.get("near_edge")
                low_gate_now = None
                try:
                    low_gate_now = float(
                        (normalized.get("thresholds") or {}).get("low_gate")
                    )
                except Exception:
                    low_gate_now = None

                try:
                    if is_alert:
                        if lower_symbol not in state_manager.alert_sessions:
                            side_hint = None
                            if signal_val.endswith("BUY"):
                                side_hint = "BUY"
                            elif signal_val.endswith("SELL"):
                                side_hint = "SELL"
                            state_manager.start_alert_session(
                                lower_symbol,
                                price_now,
                                atr_pct_now,
                                rsi_now,
                                side_hint,
                                band_pct_now,
                                low_gate_now,
                                (
                                    near_edge_now
                                    if isinstance(near_edge_now, str)
                                    else None
                                ),
                            )
                        state_manager.update_alert_session(
                            lower_symbol,
                            price_now,
                            atr_pct_now,
                            rsi_now,
                            htf_ok_now if isinstance(htf_ok_now, bool) else None,
                            band_pct_now,
                            low_gate_now,
                            near_edge_now if isinstance(near_edge_now, str) else None,
                        )
                    elif was_alert:
                        downgrade_reason = f"stage1_to_{signal_val.lower() or 'none'}"
                        # TODO: перевірити дублювання фіналізації зі Stage3 при одночасному запуску
                        state_manager.finalize_alert_session(
                            lower_symbol,
                            downgrade_reason,
                        )
                except Exception:
                    logger.debug(
                        "[TELEM] %s: alert lifecycle instrumentation failed",
                        lower_symbol,
                        exc_info=True,
                    )
                allowed_keys = {
                    "symbol",
                    K_SIGNAL,
                    K_STATS,
                    "thresholds",
                    "trigger_reasons",
                    "raw_trigger_reasons",
                    "hints",
                    "state",
                }
                normalized = {k: v for k, v in normalized.items() if k in allowed_keys}
                normalized.setdefault(K_STATS, stats_container)
                state_manager.update_asset(lower_symbol, normalized)
                try:
                    await log_stage1_event(
                        event="state_updated",
                        symbol=lower_symbol,
                        payload={"signal": signal_val},
                    )
                except Exception:
                    logger.debug("[TELEM] %s: state_updated log failed", lower_symbol)
                logger.info(
                    "[STATE] %s: оновлено стан, сигнал %s", lower_symbol, signal_val
                )
            except Exception as exc:
                logger.error("[STAGE1] %s: помилка Stage1: %s", symbol, exc)
                try:
                    if lower_symbol in state_manager.alert_sessions:
                        state_manager.finalize_alert_session(
                            lower_symbol,
                            "stage1_exception",
                        )
                except Exception:
                    logger.debug(
                        "[TELEM] %s: finalize after exception failed",
                        lower_symbol,
                        exc_info=True,
                    )
                state_manager.update_asset(
                    lower_symbol, create_error_signal(symbol, str(exc))
                )
        logger.info("[BATCH] symbols=%s: завершено", symbols)
        # Опційно: запуск суворого пре-фільтра над зібраною статистикою батчу та публікація
        # результатів у Redis для UI/діагностики. Це керується фіче-флагом, тому
        # не впливає на пайплайн без явного увімкнення.
        # при ввімкненому фіче‑флагу моніторинг символів поточного циклу обмежується строгим пре-фільтром
        try:
            if STAGE1_PREFILTER_STRICT_ENABLED:
                try:
                    from config.config import PREFILTER_STRICT_PROFILES
                    from config.flags import STAGE1_PREFILTER_STRICT_SOFT_PROFILE
                    from stage1.prefilter_strict import (
                        StrictThresholds,
                        prefilter_symbols,
                    )

                    # snapshots: list of stats dicts collected during this batch
                    snapshots = list(monitor.asset_stats.values())
                    if snapshots:
                        # Єдиний уніфікований Redis: використовуємо UnifiedDataStore.redis (без .r)
                        redis_cli = getattr(store, "redis", None)
                        # Вибір профілю порогів через фіче‑флаг (rollback‑дружньо)
                        profile_name = (
                            "soft"
                            if STAGE1_PREFILTER_STRICT_SOFT_PROFILE
                            else "default"
                        )
                        profile = PREFILTER_STRICT_PROFILES.get(profile_name, {})
                        thresholds = (
                            StrictThresholds(**profile)
                            if profile
                            else StrictThresholds()
                        )
                        # Best-effort: do not raise on failure
                        try:
                            res = prefilter_symbols(
                                snapshots,
                                thresholds,
                                redis_client=redis_cli,
                            )
                            syms = [r.get("symbol") for r in res]
                            logger.debug(
                                "[PREFILTER_STRICT] kept=%d symbols=%s",
                                len(syms),
                                syms,
                            )
                        except Exception:
                            logger.exception(
                                "[PREFILTER_STRICT] execution failed",
                            )
                except Exception:
                    logger.exception("[PREFILTER_STRICT] import failed")
        except Exception:
            # Defensive: do not allow prefilter logging to affect main pipeline
            logger.debug("[PREFILTER_STRICT] guard failed", exc_info=True)
