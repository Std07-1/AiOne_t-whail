"""Stage3 helper: відкриття угод на базі Stage2 сигналів.

Сортує сигнали за впевненістю, застосовує поріг і делегує відкриття
`TradeLifecycleManager`. Стиль уніфіковано (короткі секції, guard logger).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import (
    FEATURE_STAGE3_RECENT_DOWNTREND_BLOCK,
    PACK_EXHAUSTION_REVERSAL_ENABLED_FLAG,
    STAGE3_DIRECTIONAL_GUARD_CANARY_ONLY,
    STAGE3_HEALTH_CONFIDENCE_DAMPING,
    STAGE3_HEALTH_LOCK_CFG,
    STAGE3_SHADOW_CANARY_SYMBOLS,
    STAGE3_SHADOW_DUAL_WRITE_ENABLED,
    STAGE3_SHADOW_GLOBAL_ROLLOUT,
    STAGE3_SHADOW_PIPELINE_ENABLED,
    STAGE3_SHADOW_STREAM_CFG,
    STAGE3_SHADOW_STREAMS,
    STAGE3_TRADE_PARAMS,
    get_stage3_param,
)
from data.redis_connection import acquire_redis
from monitoring.telemetry_sink import log_stage3_event
from stage3.trade_manager import TradeLifecycleManager
from utils.utils import map_reco_to_signal, safe_float

try:
    from config import config as _config_module
except Exception:  # pragma: no cover - fallback на дефолти
    _config_module = None


def _cfg_attr(name: str, default: Any) -> Any:
    """Дістає значення з config.config з безпечним дефолтом."""

    if _config_module is not None and hasattr(_config_module, name):
        return getattr(_config_module, name)
    return default


if TYPE_CHECKING:  # pragma: no cover
    from app.asset_state_manager import AssetStateManager

# ── Logger ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("stage3.open_trades")
if not logger.handlers:  # guard від повторної ініціалізації
    logger.setLevel(logging.DEBUG)
    try:  # optional rich
        logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    except Exception:  # broad except: rich необов'язковий
        logger.addHandler(logging.StreamHandler())
    logger.propagate = False

# Мінімальна впевненість для відкриття угоди (централізована в config)
MIN_CONFIDENCE_TRADE = float(STAGE3_TRADE_PARAMS.get("min_confidence_trade", 0.75))
LOW_VOL_CONF_OVERRIDE = float(STAGE3_TRADE_PARAMS.get("low_vol_conf_override", 0.82))
MAX_BAND_PCT: float | None = float(STAGE3_TRADE_PARAMS.get("max_band_pct", 5.0))
NARROW_BAND_PCT = float(STAGE3_TRADE_PARAMS.get("narrow_band_pct", 1.2))
_raw_scenarios = STAGE3_TRADE_PARAMS.get(
    "narrow_allowed_scenarios",
    ["BULLISH_BREAKOUT", "BEARISH_REVERSAL"],
)
NARROW_ALLOWED_SCENARIOS = {
    str(item).upper() for item in _raw_scenarios if isinstance(item, str)
}
if not NARROW_ALLOWED_SCENARIOS:
    NARROW_ALLOWED_SCENARIOS = {"BULLISH_BREAKOUT", "BEARISH_REVERSAL"}
WIDE_CONTINUATION_ENABLED = bool(
    STAGE3_TRADE_PARAMS.get("wide_continuation_enabled", False)
)
WIDE_CONTINUATION_MIN_TREND = float(
    STAGE3_TRADE_PARAMS.get("wide_continuation_min_trend", 0.65)
)
WIDE_CONTINUATION_MIN_VOLUME = float(
    STAGE3_TRADE_PARAMS.get("wide_continuation_min_volume", 1.2)
)
DEBOUNCE_WINDOW_SEC = float(STAGE3_TRADE_PARAMS.get("debounce_window_sec", 90.0))
# Додаткові параметри гейту впевненості та швидкого re-entry
CONF_DIR_BOOST = float(STAGE3_TRADE_PARAMS.get("conf_directional_boost", 0.05))
CONF_LOW_VOL_GATE_RELAX = float(
    STAGE3_TRADE_PARAMS.get("conf_low_vol_gate_relax", 0.15)
)
MIN_RR_RATIO = float(STAGE3_TRADE_PARAMS.get("min_rr_ratio", 1.2))
_LAST_OPEN_ATTEMPTS: dict[str, tuple[float, str]] = {}
_HEALTH_LOCK_UNTIL: float = 0.0
_HEALTH_LOCK_REASON: str | None = None
_STREAMING_ACTIVE: bool = STAGE3_SHADOW_PIPELINE_ENABLED
_SHADOW_DUAL_WRITE_ACTIVE: bool = _STREAMING_ACTIVE and STAGE3_SHADOW_DUAL_WRITE_ENABLED
_SHADOW_STRATEGY_STREAM: str = STAGE3_SHADOW_STREAMS["STRATEGY_OPEN"]
_SHADOW_STREAM_MAXLEN: int = int(STAGE3_SHADOW_STREAM_CFG["MAXLEN"])
_shadow_redis: Any | None = None
_shadow_redis_lock: asyncio.Lock | None = None


STRATEGY_HINT_EXHAUSTION_REVERSAL = "exhaustion_reversal_long"


def _extract_intent_overrides(signal: dict[str, Any]) -> dict[str, Any] | None:
    """Витягає пороги з trade_intent (якщо є) для локального гейтингу.

    Returns:
        dict | None: {"min_required_conf": float, "min_rr": float}
    """
    try:
        intent = signal.get("trade_intent")
        if not isinstance(intent, dict):
            return None
        out: dict[str, Any] = {}
        mrc = safe_float(intent.get("min_required_conf"))
        if mrc is not None:
            out["min_required_conf"] = float(mrc)
        mr = safe_float(intent.get("min_rr"))
        if mr is not None:
            out["min_rr"] = float(mr)
        # Якщо у сигналу немає явної confidence — використаємо intent.composite_conf
        if (safe_float(signal.get("confidence")) is None) and safe_float(
            intent.get("composite_conf")
        ) is not None:
            signal["confidence"] = float(intent.get("composite_conf"))  # type: ignore[arg-type]
        return out if out else None
    except Exception:
        return None


def should_open(
    intent: dict[str, Any],
    now_price: float | None = None,
    limits: dict[str, Any] | None = None,
    *,
    price_getter: Any | None = None,
    portfolio_getter: Any | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Оцінює, чи варто відкривати угоду на основі TradeIntent.

    Args:
        intent: Словник TradeIntent (див. stage2.contracts.TradeIntent).
        now_price: Поточна ціна (float).
        limits: Додаткові обмеження (необов'язково), наприклад {"min_rr": 1.3}.

    Returns:
        (allowed, meta): allowed — True/False, meta — деталі прийнятого рішення.
    """
    meta: dict[str, Any] = {
        "reason": None,
        "min_conf": MIN_CONFIDENCE_TRADE,
        "min_rr": MIN_RR_RATIO,
    }
    if not isinstance(intent, dict):
        meta["reason"] = "invalid_intent"
        return False, meta

    # 0) Спроба отримати актуальну ціну якщо не передали now_price
    symbol = str(intent.get("symbol") or "").upper()
    if (now_price is None or now_price <= 0) and callable(price_getter) and symbol:
        try:
            fetched = safe_float(price_getter(symbol))
            if fetched is not None and fetched > 0:
                now_price = fetched
        except Exception:
            pass

    comp_conf = safe_float(intent.get("composite_conf")) or 0.0
    min_conf = max(
        MIN_CONFIDENCE_TRADE, safe_float(intent.get("min_required_conf")) or 0.0
    )
    meta["min_conf"] = min_conf

    # Low-vol адаптація (якщо доступні atr_pct/low_gate у gates/context)
    adapt_gate = 1.0
    try:
        context = intent.get("context") or {}
        atr_pct = safe_float((context or {}).get("atr_pct"))
        low_gate = safe_float((context or {}).get("low_gate")) or None
        if atr_pct is not None and low_gate is not None and atr_pct < low_gate:
            adapt_gate = max(0.7, 1.0 - CONF_LOW_VOL_GATE_RELAX)
    except Exception:
        pass

    if comp_conf < (min_conf * adapt_gate):
        meta["reason"] = "low_confidence"
        meta.update({"conf": comp_conf, "adapt_gate": adapt_gate})
        return False, meta

    # Портфельні ліміти (за бажанням через portfolio_getter)
    if limits is None and callable(portfolio_getter):
        try:
            limits = portfolio_getter()  # очікується dict
        except Exception:
            limits = None
    if isinstance(limits, dict):
        slots_rem = safe_float(limits.get("open_slots_remaining"))
        if slots_rem is not None and slots_rem <= 0:
            meta["reason"] = "portfolio_full"
            meta.update({"open_slots_remaining": slots_rem})
            return False, meta

    # Перевірка R:R — спираємось на оцінку з intent; враховуємо limits
    local_min_rr = max(MIN_RR_RATIO, safe_float(intent.get("min_rr")) or 0.0)
    if isinstance(limits, dict):
        lim_rr = safe_float(limits.get("min_rr"))
        if lim_rr is not None:
            local_min_rr = max(local_min_rr, lim_rr)
    meta["min_rr"] = local_min_rr

    rr_est = safe_float(intent.get("rr_estimate")) or 0.0
    if rr_est <= 0 and now_price and now_price > 0:
        # Фолбек: якщо intent не містить rr_estimate — порахувати по tp/sl
        try:
            sl_level = safe_float(intent.get("sl_level"))
            tp_targets = intent.get("tp_targets") or []
            tp0 = None
            if isinstance(tp_targets, list) and tp_targets:
                tp0 = safe_float(tp_targets[0])
            if sl_level and tp0:
                reward = abs(tp0 - float(now_price))
                risk = abs(float(now_price) - sl_level)
                rr_est = (reward / risk) if (risk and risk > 0) else 0.0
        except Exception:
            rr_est = rr_est or 0.0
    if rr_est < local_min_rr:
        meta["reason"] = "low_rr"
        meta.update({"rr": rr_est})
        return False, meta

    # HTF gate (якщо надано)
    htf_ok = intent.get("gates", {}).get("htf_ok")
    if isinstance(htf_ok, bool) and not htf_ok:
        meta["reason"] = "htf_gate"
        return False, meta

    meta["reason"] = "ok"
    return True, meta


def _log_stage3_skip(signal: dict[str, Any], reason: str) -> None:
    """Записати причину пропуску відкриття угоди у JSONL (best-effort).

    Формат рядка: {
        ts, symbol, reason, confidence, signal_type,
        htf_ok, atr_pct, low_gate
    }
    """
    try:
        symbol = signal.get("symbol")
        ctx_meta = signal.get("context_metadata") or {}
        if not ctx_meta and isinstance(signal.get("market_context"), dict):
            ctx_meta = (signal.get("market_context", {}) or {}).get("meta", {})
        if not isinstance(ctx_meta, dict):
            ctx_meta = {}
        corridor_meta: dict[str, Any] = _extract_corridor_meta(signal, ctx_meta)
        rec = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "symbol": symbol,
            "reason": reason,
            "confidence": safe_float(signal.get("confidence")),
            "signal_type": signal.get("signal"),
            "htf_ok": ctx_meta.get("htf_ok"),
            "atr_pct": ctx_meta.get("atr_pct"),
            "low_gate": ctx_meta.get("low_gate"),
            "corridor_band_pct": safe_float(corridor_meta.get("band_pct")),
            "corridor_dist_to_edge_pct": safe_float(
                corridor_meta.get("dist_to_edge_pct")
            ),
            "corridor_dist_to_edge_ratio": safe_float(
                corridor_meta.get("dist_to_edge_ratio")
            ),
            "corridor_near_edge": corridor_meta.get("near_edge"),
            "corridor_is_near_edge": corridor_meta.get("is_near_edge"),
            "corridor_within": corridor_meta.get("within_corridor"),
        }
        # Централізоване зберігання у TELEMETRY_BASE_DIR
        from pathlib import Path

        try:
            from config.config import STAGE3_SKIPS_LOG, TELEMETRY_BASE_DIR
        except Exception:
            telemetry_base_dir, stage3_skips_log = "./telemetry", "stage3_skips.jsonl"
        else:
            telemetry_base_dir, stage3_skips_log = TELEMETRY_BASE_DIR, STAGE3_SKIPS_LOG
        out_path = Path(telemetry_base_dir) / stage3_skips_log
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        # ігноруємо будь-які помилки (не критично для пайплайну)
        pass


def _extract_corridor_meta(
    signal: dict[str, Any], ctx_meta: dict[str, Any] | None
) -> dict[str, Any]:
    """Витягує corridor_meta з signal у канонічному порядку джерел.

    Порядок:
    1) context_metadata.corridor
    2) market_context.key_levels_meta
    3) market_context.meta.corridor (як резерв)

    Args:
        signal: Stage2 сигнал/кандидат для Stage3.
        ctx_meta: Вже виділена context_metadata (може бути None).

    Returns:
        dict: corridor метадані або порожній dict.
    """
    try:
        if isinstance(ctx_meta, dict):
            cm = ctx_meta.get("corridor")
            if isinstance(cm, dict):
                logger.debug(
                    "corridor_meta знайдено у context_metadata для %s",
                    signal.get("symbol"),
                )
                return cm

        market_ctx = signal.get("market_context") if isinstance(signal, dict) else None
        if isinstance(market_ctx, dict):
            km = market_ctx.get("key_levels_meta")
            if isinstance(km, dict):
                logger.debug(
                    "corridor_meta знайдено у market_context.key_levels_meta для %s",
                    signal.get("symbol"),
                )
                return km
            meta = market_ctx.get("meta")
            if isinstance(meta, dict):
                corr2 = meta.get("corridor")
                if isinstance(corr2, dict):
                    logger.debug(
                        "corridor_meta знайдено у market_context.meta.corridor для %s",
                        signal.get("symbol"),
                    )
                    return corr2
    except Exception as exc:
        logger.warning(
            "Помилка при витяганні corridor_meta для %s: %s", signal.get("symbol"), exc
        )
    logger.debug("corridor_meta не знайдено для %s", signal.get("symbol"))
    return {}


def _optional_float(value: Any) -> float | None:
    """Повертає float якщо можливо, інакше None.

    Args:
        value: Значення для конвертації.

    Returns:
        float або None.
    """
    try:
        if value is None:
            logger.debug("optional_float: value=None")
            return None
        result = float(value)
        logger.debug("optional_float: value=%s -> %s", value, result)
        return result
    except Exception as exc:
        logger.debug("optional_float: не вдалося конвертувати %s (%s)", value, exc)
        return None


def _pick_float(*values: Any) -> float | None:
    """Повертає перший валідний float із переданих значень.

    Args:
        *values: Кандидати для вибору.

    Returns:
        float або None.
    """
    for candidate in values:
        val = safe_float(candidate)
        if val is not None:
            logger.debug("pick_float: вибрано %s з %s", val, candidate)
            return val
    logger.debug("pick_float: не знайдено валідного значення серед %s", values)
    return None


def _resolve_stage3_float(symbol: str, key: str, fallback: float) -> float:
    """Витягує float-параметр Stage3 для символу, з fallback.

    Args:
        symbol: Символ.
        key: Ключ параметра.
        fallback: Значення за замовчуванням.

    Returns:
        float: Витягнуте або fallback.
    """
    value = get_stage3_param(symbol, key, fallback)
    resolved = _optional_float(value)
    logger.debug(
        "resolve_stage3_float: %s[%s]=%s (fallback=%s)", symbol, key, resolved, fallback
    )
    return resolved if resolved is not None else float(fallback)


def _resolve_stage3_optional_float(
    symbol: str, key: str, fallback: float | None
) -> float | None:
    """Витягує float-параметр Stage3 для символу, допускає None.

    Args:
        symbol: Символ.
        key: Ключ параметра.
        fallback: Значення за замовчуванням (може бути None).

    Returns:
        float або None.
    """
    value = get_stage3_param(symbol, key, fallback)
    if value is None:
        logger.debug(
            "resolve_stage3_optional_float: %s[%s]=None (fallback=%s)",
            symbol,
            key,
            fallback,
        )
        return None
    resolved = _optional_float(value)
    logger.debug(
        "resolve_stage3_optional_float: %s[%s]=%s (fallback=%s)",
        symbol,
        key,
        resolved,
        fallback,
    )
    if resolved is None:
        return fallback
    return resolved


def _parse_bool(value: Any, default: bool) -> bool:
    """Парсить булеве значення з різних типів.

    Args:
        value: Вхідне значення.
        default: Значення за замовчуванням.

    Returns:
        bool: Результат парсингу.
    """
    if isinstance(value, bool):
        logger.debug("parse_bool: %s -> %s", value, value)
        return value
    if isinstance(value, (int, float)):
        result = bool(value)
        logger.debug("parse_bool: %s -> %s", value, result)
        return result
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            logger.debug("parse_bool: %s -> True", value)
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            logger.debug("parse_bool: %s -> False", value)
            return False
    logger.debug("parse_bool: %s -> default %s", value, default)
    return default


def _resolve_stage3_bool(symbol: str, key: str, fallback: bool) -> bool:
    """Витягує булевий параметр Stage3 для символу.

    Args:
        symbol: Символ.
        key: Ключ параметра.
        fallback: Значення за замовчуванням.

    Returns:
        bool: Витягнуте або fallback.
    """
    value = get_stage3_param(symbol, key, fallback)
    result = _parse_bool(value, bool(fallback))
    logger.debug(
        "resolve_stage3_bool: %s[%s]=%s (fallback=%s)", symbol, key, result, fallback
    )
    return result


def _resolve_stage3_scenarios(symbol: str, key: str, fallback: set[str]) -> set[str]:
    """Витягує множину сценаріїв Stage3 для символу.

    Args:
        symbol: Символ.
        key: Ключ параметра.
        fallback: Множина за замовчуванням.

    Returns:
        set[str]: Множина сценаріїв.
    """
    value = get_stage3_param(symbol, key, list(fallback))
    items: set[str] = set()
    if isinstance(value, (list, tuple, set)):
        for item in value:
            if isinstance(item, str):
                items.add(item.upper())
    logger.debug(
        "resolve_stage3_scenarios: %s[%s]=%s (fallback=%s)",
        symbol,
        key,
        items,
        fallback,
    )
    if items:
        return items
    return set(fallback)


def _map_signal_from_recommendation(signal: dict[str, Any]) -> None:
    """Перетворює general ALERT на напрямок згідно з recommendation.

    Args:
        signal: Stage2 сигнал.

    Returns:
        None. Модифікує signal in-place.
    """
    sig_raw = str(signal.get("signal", "")).upper()
    if sig_raw in {"ALERT_BUY", "ALERT_SELL"}:
        logger.debug(
            "map_signal_from_recommendation: signal вже має напрямок %s", sig_raw
        )
        return
    mapped = map_reco_to_signal(signal.get("recommendation"))
    if mapped in {"ALERT_BUY", "ALERT_SELL"}:
        logger.info(
            "map_signal_from_recommendation: %s → %s",
            signal.get("recommendation"),
            mapped,
        )
        signal["signal"] = mapped
    else:
        logger.debug(
            "map_signal_from_recommendation: recommendation %s не мапиться на напрямок",
            signal.get("recommendation"),
        )


def _pre_open_directional_guard(symbol: str, signal: dict[str, Any]) -> bool:
    """Перевіряє, чи дозволено відкривати LONG на основі directional guard.

    Активується лише коли FEATURE_STAGE3_RECENT_DOWNTREND_BLOCK = True.
    Умова блокування: помітний нещодавній спад (slope ≤ -thr_s) і DVR ≥ thr_d,
    опціонально з урахуванням cumulative delta, якщо FEATURE_STAGE3_USE_CUM_DELTA = True.

    Args:
        symbol: Символ активу (наприклад, "BTCUSDT").
        signal: Словник сигналу Stage2 з ключами 'stats', 'market_context' тощо.

    Returns:
        bool: True, якщо відкриття дозволено; False, якщо заблоковано через recent downtrend.

    Raises:
        None: Функція не викликає виключень, обробляє помилки внутрішньо.
    """
    if not FEATURE_STAGE3_RECENT_DOWNTREND_BLOCK:
        logger.debug("Directional guard вимкнено для %s", symbol)
        return True

    # Канарейкове обмеження: застосовувати guard лише до певних символів
    if STAGE3_DIRECTIONAL_GUARD_CANARY_ONLY:
        canary_raw = _cfg_attr("CANARY_SYMBOLS", ())
        if isinstance(canary_raw, Iterable):
            _canary = {str(item).upper() for item in canary_raw if item is not None}
        else:
            _canary = set()
        if _canary and str(symbol).upper() not in _canary:
            logger.debug("Directional guard пропущено для %s (не канарейка)", symbol)
            return True

    # Витягуємо метрики з сигналу
    st = signal.get("stats", {}) or {}
    mc = signal.get("market_context", {}) or {}
    dir_ctx = (mc.get("directional", {}) if isinstance(mc, dict) else {}) or {}
    slope = (
        safe_float(st.get("price_slope_atr") or dir_ctx.get("price_slope_atr")) or 0.0
    )
    dvr = (
        safe_float(
            st.get("directional_volume_ratio")
            or dir_ctx.get("directional_volume_ratio")
        )
        or 1.0
    )
    cd = (
        safe_float(st.get("cumulative_delta") or dir_ctx.get("cumulative_delta")) or 0.0
    )

    # Завантажуємо пороги з конфігу
    cfg_dir_params = _cfg_attr("DIRECTIONAL_PARAMS", {})
    if isinstance(cfg_dir_params, Mapping):
        thr_s = float(
            cfg_dir_params.get(
                "slope_thresh_atr", cfg_dir_params.get("slope_thresh", 0.25)
            )
        )
        thr_d = float(cfg_dir_params.get("dv_ratio_thresh", 1.2))
        thr_cd = float(cfg_dir_params.get("cum_delta_thresh", 0.30))
    else:
        thr_s, thr_d, thr_cd = 0.25, 1.2, 0.30
    use_cd_flag = bool(_cfg_attr("FEATURE_STAGE3_USE_CUM_DELTA", False))

    # Обчислюємо умови блокування
    cond_base = slope <= -thr_s and dvr >= thr_d
    cond = cond_base if not use_cd_flag else (cond_base and abs(cd) >= thr_cd)

    logger.debug(
        "Directional guard для %s: slope=%.4f, dvr=%.2f, cd=%.4f, cond_base=%s, use_cd=%s, cond=%s",
        symbol,
        slope,
        dvr,
        cd,
        cond_base,
        use_cd_flag,
        cond,
    )

    if cond:
        # Телеметрія та локальний лог пропуску
        reason = "recent_downtrend_block"
        _log_stage3_skip(signal, reason)
        logger.info(
            "⛔️ Directional guard: блокування LONG для %s через recent downtrend (slope=%.4f, dvr=%.2f, cd=%.4f)",
            symbol,
            slope,
            dvr,
            cd,
        )
        try:
            asyncio.create_task(
                log_stage3_event(
                    "open_skipped",
                    symbol,
                    {
                        "reason": reason,
                        "price_slope_atr": slope,
                        "directional_volume_ratio": dvr,
                        "cumulative_delta": cd,
                        "use_cd": bool(use_cd_flag),
                        "thr_cd": thr_cd,
                    },
                )
            )
        except Exception:
            logger.debug(
                "Не вдалося відправити телеметрію для directional guard %s", symbol
            )
        return False

    logger.debug("Directional guard дозволено для %s", symbol)
    return True


def _ensure_tp_sl(signal: dict[str, Any]) -> None:
    """Гарантує наявність базових TP/SL у сигналі, якщо Stage2 їх не надав.

    Якщо TP або SL відсутні чи невалідні, обчислює дефолтні значення на основі ATR:
    - Для BUY: TP = price + ATR * 1.0, SL = price - ATR * 0.8
    - Для SELL: TP = price - ATR * 1.0, SL = price + ATR * 0.8

    Args:
        signal: Словник сигналу Stage2, модифікується in-place.

    Returns:
        None: Функція не повертає значення, лише модифікує сигнал.

    Raises:
        None: Функція не викликає виключень, обробляє помилки внутрішньо.
    """
    stats = signal.get("stats") if isinstance(signal, dict) else {}
    if not isinstance(stats, dict):
        stats = {}
    rp = signal.get("risk_parameters") if isinstance(signal, dict) else {}
    if not isinstance(rp, dict):
        rp = {}

    # Витягуємо ціну та ATR
    price = _pick_float(
        signal.get("current_price"),
        stats.get("current_price"),
    )
    atr_val = _pick_float(signal.get("atr"), stats.get("atr"))

    if price is None or atr_val is None or price <= 0 or atr_val <= 0:
        logger.debug(
            "Недостатньо даних для TP/SL у сигналі: price=%s, atr=%s", price, atr_val
        )
        return

    sig_type = str(signal.get("signal", "")).upper()
    if sig_type not in {"ALERT_BUY", "ALERT_SELL"}:
        logger.debug("TP/SL не потрібні для сигналу типу %s", sig_type)
        return

    # Перевіряємо наявність TP
    tp_candidates: list[Any] = [signal.get("tp"), rp.get("take_profit"), rp.get("tp")]
    targets = rp.get("tp_targets")
    if isinstance(targets, (list, tuple)):
        tp_candidates.extend(targets)
    existing_tp = _pick_float(*tp_candidates) if tp_candidates else None

    # Перевіряємо наявність SL
    sl_candidates: list[Any] = [
        signal.get("sl"),
        rp.get("stop_loss"),
        rp.get("sl"),
        rp.get("sl_level"),
    ]
    existing_sl = _pick_float(*sl_candidates) if sl_candidates else None

    if (
        existing_tp is not None
        and existing_tp > 0
        and existing_sl is not None
        and existing_sl > 0
    ):
        logger.debug("TP/SL вже присутні у сигналі для %s", signal.get("symbol"))
        return

    # Обчислюємо дефолтні значення
    tp_step = 1.0
    sl_step = 0.8
    if sig_type == "ALERT_BUY":
        default_tp = price + atr_val * tp_step
        default_sl = max(1e-12, price - atr_val * sl_step)
    else:  # ALERT_SELL
        default_tp = max(1e-12, price - atr_val * tp_step)
        default_sl = price + atr_val * sl_step

    # Додаємо відсутні TP/SL
    if existing_tp is None or existing_tp <= 0:
        signal["tp"] = default_tp
        logger.debug(
            "Додано дефолтний TP=%.4f для %s", default_tp, signal.get("symbol")
        )
    if existing_sl is None or existing_sl <= 0:
        signal["sl"] = default_sl
        logger.debug(
            "Додано дефолтний SL=%.4f для %s", default_sl, signal.get("symbol")
        )


def _ensure_alert_session(
    state_manager: AssetStateManager,
    symbol: str,
    signal: dict[str, Any],
) -> None:
    """Гарантує наявність активної сесії ALERT для символу.

    Якщо сесія вже існує, повертається без змін. Інакше ініціалізує нову
    сесію на основі даних сигналу (ціна, ATR%, RSI, сторона, коридор тощо).

    Args:
        state_manager: Менеджер стану активів для керування сесіями.
        symbol: Символ активу (наприклад, "BTCUSDT").
        signal: Словник сигналу Stage2 з ключами 'stats', 'market_context' тощо.

    Returns:
        None: Функція не повертає значення, лише модифікує стан.

    Raises:
        None: Функція не викликає виключень, обробляє помилки внутрішньо.
    """
    if symbol in state_manager.alert_sessions:
        logger.debug("Сесія ALERT для %s вже існує, пропускаємо ініціалізацію", symbol)
        return

    # Витягуємо метрики з сигналу
    stats = signal.get("stats") or {}
    price_val = _optional_float(stats.get("current_price"))
    rsi_val = _optional_float(stats.get("rsi"))
    market_ctx_raw = signal.get("market_context")
    market_ctx = market_ctx_raw if isinstance(market_ctx_raw, dict) else {}
    meta = signal.get("context_metadata") or {}
    if not meta:
        meta = market_ctx.get("meta", {}) or {}
    if not isinstance(meta, dict):
        meta = {}
    atr_pct_val = _optional_float(meta.get("atr_pct"))
    low_gate_val = _optional_float(meta.get("low_gate"))

    # Витягуємо corridor метадані
    corridor_meta: dict[str, Any] = {}
    corridor_candidate = meta.get("corridor")
    if isinstance(corridor_candidate, dict):
        corridor_meta = corridor_candidate
    else:
        key_levels_candidate = market_ctx.get("key_levels_meta")
        if isinstance(key_levels_candidate, dict):
            corridor_meta = key_levels_candidate
    band_pct_val = _optional_float(corridor_meta.get("band_pct"))
    near_edge_val = corridor_meta.get("near_edge")
    if not isinstance(near_edge_val, str):
        nearest_edge_candidate = corridor_meta.get("nearest_edge")
        is_near_edge_val = corridor_meta.get("is_near_edge")
        if isinstance(nearest_edge_candidate, str) and bool(is_near_edge_val):
            near_edge_val = nearest_edge_candidate

    # Визначаємо сторону сигналу
    sig_type = str(signal.get("signal", "")).upper()
    side_val: str | None = None
    if sig_type.startswith("ALERT_BUY"):
        side_val = "BUY"
    elif sig_type.startswith("ALERT_SELL"):
        side_val = "SELL"

    logger.debug(
        "Ініціалізація сесії ALERT для %s: price=%.4f, atr_pct=%.4f, rsi=%.2f, side=%s, band_pct=%.2f, near_edge=%s",
        symbol,
        price_val or 0.0,
        atr_pct_val or 0.0,
        rsi_val or 0.0,
        side_val,
        band_pct_val or 0.0,
        near_edge_val,
    )

    try:
        state_manager.start_alert_session(
            symbol,
            price_val,
            atr_pct_val,
            rsi_val,
            side_val,
            band_pct_val,
            low_gate_val,
            near_edge_val,
        )
        logger.info("Ініціалізовано сесію ALERT для %s", symbol)
    except Exception as exc:
        logger.warning("Не вдалося ініціалізувати сесію ALERT для %s: %s", symbol, exc)


def _finalize_alert_session(
    state_manager: AssetStateManager | None,
    signal: dict[str, Any],
    reason: str,
) -> None:
    """Фіналізує сесію ALERT для символу з вказаною причиною.

    Якщо сесія не існує та сигнал є ALERT, спочатку ініціалізує її.
    Потім фіналізує сесію та оновлює стан активу на NORMAL.

    Args:
        state_manager: Менеджер стану активів (може бути None).
        signal: Словник сигналу Stage2 з ключем 'symbol'.
        reason: Причина фіналізації (наприклад, "stage3_health_lock").

    Returns:
        None: Функція не повертає значення, лише модифікує стан.

    Raises:
        None: Функція не викликає виключень, обробляє помилки внутрішньо.
    """
    if state_manager is None:
        logger.debug("state_manager відсутній, пропускаємо фіналізацію сесії")
        return

    symbol = signal.get("symbol")
    if not isinstance(symbol, str):
        logger.debug("Невалідний символ у сигналі: %s", symbol)
        return

    sig_type = str(signal.get("signal", "")).upper()
    if symbol not in state_manager.alert_sessions and sig_type.startswith("ALERT"):
        logger.debug(
            "Сесія ALERT для %s не існує, ініціалізуємо перед фіналізацією", symbol
        )
        _ensure_alert_session(state_manager, symbol, signal)

    if symbol in state_manager.alert_sessions:
        logger.debug("Фіналізація сесії ALERT для %s з причиною: %s", symbol, reason)
        try:
            state_manager.finalize_alert_session(symbol, reason)
            logger.info("Фіналізовано сесію ALERT для %s (%s)", symbol, reason)
        except Exception as exc:
            logger.warning(
                "Не вдалося фіналізувати сесію ALERT для %s: %s", symbol, exc
            )
            return

        try:
            state_manager.update_asset(symbol, {"signal": "NORMAL"})
            logger.debug("Оновлено стан активу %s на NORMAL", symbol)
        except Exception as exc:
            logger.warning("Не вдалося оновити стан активу %s: %s", symbol, exc)


async def _get_shadow_redis() -> Any | None:
    if not _SHADOW_DUAL_WRITE_ACTIVE:
        return None

    global _shadow_redis
    if _shadow_redis is not None:
        return _shadow_redis

    global _shadow_redis_lock
    if _shadow_redis_lock is None:
        _shadow_redis_lock = asyncio.Lock()

    async with _shadow_redis_lock:
        if _shadow_redis is not None:
            return _shadow_redis
        try:
            _shadow_redis = await acquire_redis(decode_responses=False)
        except Exception:  # pragma: no cover - best effort
            logger.exception("Shadow dual-write: не вдалося отримати Redis клієнт")
            return None
    return _shadow_redis


def _signal_side(signal: dict[str, Any]) -> str:
    sig_type = str(signal.get("signal", "")).upper()
    if sig_type == "ALERT_BUY":
        return "LONG"
    if sig_type == "ALERT_SELL":
        return "SHORT"
    return "UNKNOWN"


async def _publish_shadow_signal(
    signal: dict[str, Any],
    trade_payload: dict[str, Any],
) -> None:
    if not _SHADOW_DUAL_WRITE_ACTIVE:
        return

    symbol = str(signal.get("symbol", "UNKNOWN")).upper()
    if not STAGE3_SHADOW_GLOBAL_ROLLOUT and symbol not in STAGE3_SHADOW_CANARY_SYMBOLS:
        return

    redis = await _get_shadow_redis()
    if redis is None:
        return

    now_ts = time.time()
    entry_id: str | None = None
    try:
        fields = {
            "symbol": symbol,
            "side": _signal_side(signal),
            "trade_payload": json.dumps(trade_payload, ensure_ascii=False, default=str),
            "raw_signal": json.dumps(signal, ensure_ascii=False, default=str),
            "ts": f"{now_ts:.6f}",
            "dual_write": "0",
            "source": "stage3_shadow",
        }
        entry_id = await redis.xadd(
            _SHADOW_STRATEGY_STREAM,
            fields,
            maxlen=_SHADOW_STREAM_MAXLEN,
            approximate=True,
        )
    except Exception:  # pragma: no cover - захист від Redis збоїв
        logger.exception("Shadow dual-write: не вдалося записати сигнал %s", symbol)
        await log_stage3_event(
            "strategy_shadow_error",
            symbol,
            {"reason": "dual_write_failed"},
        )
        return

    lag_ms = 0
    try:
        candidates = [
            signal.get("ingested_ts"),
            signal.get("ts"),
            signal.get("timestamp"),
        ]
        stats = signal.get("stats")
        if isinstance(stats, dict):
            candidates.append(stats.get("ts"))
            candidates.append(stats.get("ingested_ts"))
        meta = signal.get("context_metadata")
        if isinstance(meta, dict):
            candidates.append(meta.get("ingested_ts"))
        base_ts = next(
            (
                safe_float(value)
                for value in candidates
                if safe_float(value) is not None
            ),
            None,
        )
        if base_ts is not None and base_ts > 0:
            lag_ms = int(max(now_ts - float(base_ts), 0.0) * 1000)
    except Exception:  # pragma: no cover - лише для телеметрії
        lag_ms = 0

    payload = {
        "entry_id": entry_id or "unknown",
        "lag_ms": lag_ms,
        "stream": _SHADOW_STRATEGY_STREAM,
    }
    await log_stage3_event("strategy_shadow_open_enqueued", symbol, payload)


def _get_health_snapshot(
    trade_manager: TradeLifecycleManager,
) -> dict[str, Any] | None:
    snapshot = getattr(trade_manager, "health_snapshot", None)
    if isinstance(snapshot, dict):
        return snapshot
    return None


def _health_lock_state(
    snapshot: dict[str, Any] | None,
) -> tuple[bool, dict[str, Any]]:
    """Повертає стан lock'у Stage3 та метадані для логів."""

    global _HEALTH_LOCK_UNTIL, _HEALTH_LOCK_REASON

    now = time.monotonic()
    info: dict[str, Any] = {
        "reason": _HEALTH_LOCK_REASON,
        "busy_level": None,
        "busy_trigger": None,
        "pending": None,
        "cooldown_left": 0.0,
    }

    if snapshot:
        busy_level = int(snapshot.get("busy_level", 0))
        busy_reason = snapshot.get("busy_reason")
        stream_info = snapshot.get("stream")
        pending = 0
        if isinstance(stream_info, dict):
            pending_val = safe_float(stream_info.get("pending"))
            if pending_val is not None:
                pending = int(max(pending_val, 0))
        info.update(
            {
                "busy_level": busy_level,
                "busy_trigger": busy_reason,
                "pending": pending,
            }
        )

        lock_level = int(STAGE3_HEALTH_LOCK_CFG.get("LOCK_BUSY_LEVEL", 3))
        pending_threshold = int(STAGE3_HEALTH_LOCK_CFG.get("PENDING_THRESHOLD", 0))
        critical_cd = float(
            STAGE3_HEALTH_LOCK_CFG.get(
                "CRITICAL_ERROR_COOLDOWN_SEC",
                STAGE3_HEALTH_LOCK_CFG.get("LOCK_COOLDOWN_SEC", 30.0),
            )
        )
        cooldown = float(STAGE3_HEALTH_LOCK_CFG.get("LOCK_COOLDOWN_SEC", 30.0))
        if busy_reason == "pending_severe":
            cooldown = max(cooldown, critical_cd)

        should_lock = busy_level >= lock_level or pending >= pending_threshold
        if should_lock:
            expiry = now + max(cooldown, 0.0)
            if expiry > _HEALTH_LOCK_UNTIL:
                _HEALTH_LOCK_UNTIL = expiry
                _HEALTH_LOCK_REASON = busy_reason or "health_lock"

    active = now < _HEALTH_LOCK_UNTIL
    if active:
        info["reason"] = (
            _HEALTH_LOCK_REASON or info.get("busy_trigger") or "health_lock"
        )
        info["cooldown_left"] = max(_HEALTH_LOCK_UNTIL - now, 0.0)
    else:
        _HEALTH_LOCK_REASON = None
        _HEALTH_LOCK_UNTIL = 0.0
        info["cooldown_left"] = 0.0

    return active, info


def _health_confidence_profile(
    snapshot: dict[str, Any] | None, max_parallel: int
) -> tuple[float, int]:
    multiplier = 1.0
    effective_parallel = max(1, int(max_parallel))
    if not snapshot:
        return multiplier, effective_parallel

    busy_level = int(snapshot.get("busy_level", 0))
    try:
        threshold = int(
            STAGE3_HEALTH_CONFIDENCE_DAMPING.get("BUSY_LEVEL_THRESHOLD", 99)
        )
    except Exception:
        threshold = 99

    if busy_level < threshold:
        return multiplier, effective_parallel

    try:
        multiplier = float(
            STAGE3_HEALTH_CONFIDENCE_DAMPING.get("CONFIDENCE_MULTIPLIER", 1.0)
        )
    except Exception:
        multiplier = 1.0

    try:
        ratio = float(STAGE3_HEALTH_CONFIDENCE_DAMPING.get("MAX_PARALLEL_RATIO", 1.0))
    except Exception:
        ratio = 1.0

    ratio = max(0.0, min(ratio, 1.0))
    if ratio == 0.0:
        effective_parallel = 1
    else:
        effective_parallel = max(1, min(effective_parallel, int(max_parallel * ratio)))

    return multiplier, effective_parallel


def _remember_attempt(symbol: str, reason: str) -> None:
    try:
        _LAST_OPEN_ATTEMPTS[symbol] = (time.monotonic(), reason)
    except Exception:
        _LAST_OPEN_ATTEMPTS[symbol] = (0.0, reason)


def _should_debounce(symbol: str, window_sec: float) -> bool:
    entry = _LAST_OPEN_ATTEMPTS.get(symbol)
    if not entry:
        return False
    ts, reason = entry
    # Дозволяємо повторний шанс, якщо попередня причина була low_confidence
    if reason == "low_confidence":
        return False
    try:
        return (time.monotonic() - ts) < max(window_sec, 0.0)
    except Exception:
        return False


def _evaluate_wide_continuation(
    scenario: str,
    band_pct: float,
    ctx_meta: dict[str, Any] | None,
    market_ctx: dict[str, Any] | None,
    *,
    wide_enabled: bool,
    max_band_pct: float | None,
    allowed_scenarios: set[str],
    min_trend: float,
    min_volume: float,
) -> tuple[bool, float | None, float | None]:
    if not wide_enabled:
        return False, None, None
    if max_band_pct is not None and band_pct > max_band_pct:
        return False, None, None
    if scenario not in allowed_scenarios:
        return False, None, None

    trend_val: float | None = None
    micro: dict[str, Any] | None = None
    meso: dict[str, Any] | None = None
    if isinstance(market_ctx, dict):
        micro_candidate = market_ctx.get("micro")
        meso_candidate = market_ctx.get("meso")
        micro = micro_candidate if isinstance(micro_candidate, dict) else None
        meso = meso_candidate if isinstance(meso_candidate, dict) else None

    if meso:
        trend_val = safe_float(meso.get("trend_strength") or meso.get("trend_score"))
    if trend_val is None and isinstance(ctx_meta, dict):
        trend_val = safe_float(
            ctx_meta.get("trend_strength")
            or ctx_meta.get("trend_score")
            or ctx_meta.get("trend_alignment")
        )

    volume_val: float | None = None
    if micro:
        volume_val = safe_float(
            micro.get("volume_anomaly")
            or micro.get("volume_impulse")
            or micro.get("volume_strength")
        )
    if volume_val is None and isinstance(ctx_meta, dict):
        volume_val = safe_float(
            ctx_meta.get("volume_anomaly")
            or ctx_meta.get("volume_impulse")
            or ctx_meta.get("volume_strength")
        )

    if trend_val is None or volume_val is None:
        return False, trend_val, volume_val

    allowed = trend_val >= min_trend and volume_val >= min_volume
    return allowed, trend_val, volume_val


async def open_trades(
    signals: list[dict[str, Any]],
    trade_manager: TradeLifecycleManager,
    max_parallel: int,
    state_manager: AssetStateManager | None = None,
) -> None:
    """
    Відкриває угоди для найперспективніших сигналів.

    Алгоритм (коротко):
    1. Сортує сигнали за впевненістю
    2. Застосовує health damping (залежно від snapshot від TradeLifecycleManager)
    3. Дозволяє/відкидає кандидата через набір guard'ів
    4. Якщо shadow pipeline активний — enqueue/dual-write, інакше — fallback open через trade_manager

    TODO:
    - Додати unit-тести: happy-path + edge-cases + псевдо-стрім (TDD-light).
    - Перевірити race conditions при паралельних інстансах (можливо Redis lock).
    - Додати метрики counts for each skipped reason (Prometheus/Telemetry).
    """
    # Швидкі валідації вхідних параметрів
    if not trade_manager or not signals:
        return

    # Отримуємо health snapshot і обчислюємо демпфінг впевненості / effective_parallel
    health_snapshot = _get_health_snapshot(trade_manager)
    confidence_multiplier, effective_parallel = _health_confidence_profile(
        health_snapshot, max_parallel
    )

    # Підписи/логи для моніторингу змін поведінки
    if logger.isEnabledFor(logging.DEBUG) and confidence_multiplier != 1.0:
        logger.debug(
            "Health confidence damping активовано: multiplier=%.2f, busy_level=%s",
            confidence_multiplier,
            (health_snapshot or {}).get("busy_level"),
        )
    if effective_parallel != max_parallel and logger.isEnabledFor(logging.INFO):
        logger.info(
            "Stage3 health: обмежено паралельні відкриття %d → %d (busy_level=%s)",
            max_parallel,
            effective_parallel,
            (health_snapshot or {}).get("busy_level"),
        )

    # Групуємо за символом — беремо найкращий сигнал per-symbol
    best_per_symbol: dict[str, dict[str, Any]] = {}
    for raw_signal in signals:
        if not isinstance(raw_signal, dict):
            continue
        # Підписи: перетворюємо recommendation → directed signal, додаємо TP/SL дефолти
        _map_signal_from_recommendation(raw_signal)
        _ensure_tp_sl(raw_signal)
        if raw_signal.get("validation_passed") is False:
            try:
                logger.info(
                    "⛔️ Пропуск %s: validation_passed=False", raw_signal.get("symbol")
                )
            except Exception:  # pragma: no cover - захист від рідкісних збоїв
                pass
            continue
        sym_key = str(raw_signal.get("symbol", "")).upper()
        if not sym_key:
            continue
        prev = best_per_symbol.get(sym_key)
        new_conf = safe_float(raw_signal.get("confidence")) or 0.0
        prev_conf = safe_float(prev.get("confidence")) if prev else None
        # Вибираємо найвищу впевненість (tie → останній)
        if prev is None or (prev_conf is None or new_conf >= prev_conf):
            best_per_symbol[sym_key] = raw_signal

    candidates = list(best_per_symbol.values())
    if not candidates:
        logger.info("Stage3: немає кандидатів для відкриття (raw=%d)", len(signals))
        # TODO: emit telemetry metric 'stage3.no_candidates'
        return

    # Сортування кандидатів за (демпфованою) впевненістю і відбір top-N
    base_multiplier = confidence_multiplier

    def _damped_key(payload: dict[str, Any]) -> float:
        raw_conf = safe_float(payload.get("confidence")) or 0.0
        return raw_conf * base_multiplier

    sorted_signals = sorted(
        candidates,
        key=_damped_key,
        reverse=True,
    )[:effective_parallel]

    # Лічильник причин пропусків — агрегований для логів/метрик
    skipped_by_reason: dict[str, int] = {}

    # ── Ітерація по відсортованих сигналах ───────────────────────────────────
    for signal in sorted_signals:
        # Підписи: базові дані сигналу
        symbol = signal["symbol"]
        confidence = safe_float(signal.get("confidence", 0))
        if confidence is None:
            confidence = 0.0

        sym_key = str(symbol).upper()
        is_shadow_candidate = STAGE3_SHADOW_GLOBAL_ROLLOUT or (
            sym_key in STAGE3_SHADOW_CANARY_SYMBOLS
        )

        # Локальні helper-и для shadow telemetry — тільки для читабельності
        async def _shadow_skip(
            reason: str,
            extra: dict[str, Any] | None = None,
            *,
            _sym_key: str = sym_key,
            _is_shadow: bool = is_shadow_candidate,
        ) -> None:
            if not _is_shadow:
                return
            payload: dict[str, Any] = {"reason": reason}
            if extra:
                payload.update(extra)
            await log_stage3_event("shadow_candidate_skip", _sym_key, payload)

        async def _shadow_open(
            extra: dict[str, Any] | None = None,
            *,
            _sym_key: str = sym_key,
            _is_shadow: bool = is_shadow_candidate,
        ) -> None:
            if not _is_shadow:
                return
            await log_stage3_event(
                "shadow_candidate_open",
                _sym_key,
                extra or {},
            )

        async def _shadow_error(
            reason: str,
            extra: dict[str, Any] | None = None,
            *,
            _sym_key: str = sym_key,
            _is_shadow: bool = is_shadow_candidate,
        ) -> None:
            if not _is_shadow:
                return
            payload: dict[str, Any] = {"reason": reason}
            if extra:
                payload.update(extra)
            await log_stage3_event("shadow_candidate_error", _sym_key, payload)

        # Оновлюємо health snapshot локально, якщо з'явилися свіжі дані
        current_snapshot = health_snapshot or _get_health_snapshot(trade_manager)
        if current_snapshot is not None:
            health_snapshot = current_snapshot
            updated_multiplier, _ = _health_confidence_profile(
                current_snapshot, max_parallel
            )
            confidence_multiplier = updated_multiplier

        # Демпфована/ефективна впевненість — для гейтів
        gate_confidence = confidence
        if confidence_multiplier != 1.0:
            gate_confidence = confidence * confidence_multiplier

        def _with_health(
            extra: dict[str, Any] | None = None,
            *,
            _confidence: float = confidence,
            _damped: float = gate_confidence,
            _multiplier: float = confidence_multiplier,
            _effective: int = effective_parallel,
        ) -> dict[str, Any]:
            payload: dict[str, Any] = {
                "confidence": _confidence,
                "confidence_damped": _damped,
                "health_multiplier": _multiplier,
                "effective_parallel": _effective,
            }
            if extra:
                payload.update(extra)
            return payload

        # Telemetry: коли health вплинув на gating
        if confidence_multiplier != 1.0:
            await log_stage3_event(
                "shadow_confidence_gate",
                sym_key,
                _with_health(
                    {
                        "busy_level": (current_snapshot or {}).get("busy_level"),
                        "busy_reason": (current_snapshot or {}).get("busy_reason"),
                    }
                ),
            )

        # Health lock — якщо активний, skip candidate
        is_locked, lock_info = _health_lock_state(current_snapshot)
        if is_locked:
            remaining = float(lock_info.get("cooldown_left", 0.0))
            busy_level = lock_info.get("busy_level")
            pending = lock_info.get("pending")
            lock_reason = lock_info.get("reason") or lock_info.get("busy_trigger")
            logger.info(
                "⛔️ Пропуск відкриття %s: health lock (%s, busy=%s, pending=%s, left=%.1fs)",
                sym_key,
                lock_reason,
                busy_level,
                pending,
                remaining,
            )
            skipped_by_reason["health_lock"] = (
                skipped_by_reason.get("health_lock", 0) + 1
            )
            _log_stage3_skip(signal, "health_lock")
            _finalize_alert_session(state_manager, signal, "stage3_health_lock")
            _remember_attempt(sym_key, "health_lock")
            await _shadow_skip(
                "health_lock",
                _with_health(
                    {
                        "busy_level": busy_level,
                        "pending": pending,
                        "cooldown_left": round(remaining, 3),
                        "lock_reason": lock_reason,
                    }
                ),
            )
            continue

        # Перевірки на наявність meta/eligibility
        elite_meta = signal.get("elite_signal_extra")
        if not isinstance(elite_meta, dict):
            logger.info(
                "⛔️ Пропуск відкриття %s: відсутня elite_signal_extra",
                sym_key,
            )
            skipped_by_reason["elite_meta_missing"] = (
                skipped_by_reason.get("elite_meta_missing", 0) + 1
            )
            _log_stage3_skip(signal, "elite_meta_missing")
            _finalize_alert_session(
                state_manager,
                signal,
                "stage3_elite_meta_missing",
            )
            _remember_attempt(sym_key, "elite_meta_missing")
            await _shadow_skip("elite_meta_missing", _with_health())
            continue

        if not _parse_bool(elite_meta.get("eligible"), True):
            # Підпис/логування причин відмови elite
            reasons_raw = elite_meta.get("reasons")
            if isinstance(reasons_raw, (list, tuple)):
                reasons_list = [str(item) for item in reasons_raw]
                reasons_repr = ",".join(reasons_list)
            elif reasons_raw is None:
                reasons_repr = "not_approved"
            else:
                reasons_repr = str(reasons_raw)
            logger.info(
                "⛔️ Пропуск відкриття %s: elite_signal_extra.eligible=False (%s)",
                sym_key,
                reasons_repr,
            )
            skipped_by_reason["elite_not_approved"] = (
                skipped_by_reason.get("elite_not_approved", 0) + 1
            )
            _log_stage3_skip(signal, "elite_not_approved")
            _finalize_alert_session(
                state_manager,
                signal,
                "stage3_elite_not_approved",
            )
            _remember_attempt(sym_key, "elite_not_approved")
            await _shadow_skip(
                "elite_not_approved",
                _with_health({"reasons": reasons_repr}),
            )
            continue

        # Локальні параметри конфігу/override для символа
        min_confidence_trade = _resolve_stage3_float(
            sym_key, "min_confidence_trade", MIN_CONFIDENCE_TRADE
        )
        low_vol_conf_override = _resolve_stage3_float(
            sym_key, "low_vol_conf_override", LOW_VOL_CONF_OVERRIDE
        )
        debounce_window = _resolve_stage3_float(
            sym_key, "debounce_window_sec", DEBOUNCE_WINDOW_SEC
        )

        # Intent overrides (per-signal) — локальні мінімальні значення
        local_min_rr_ratio: float = MIN_RR_RATIO
        try:
            _intent_overrides = _extract_intent_overrides(signal)
            if isinstance(_intent_overrides, dict):
                mrc = _intent_overrides.get("min_required_conf")
                if isinstance(mrc, (int, float)):
                    min_confidence_trade = max(min_confidence_trade, float(mrc))
                mrr = _intent_overrides.get("min_rr")
                if isinstance(mrr, (int, float)):
                    local_min_rr_ratio = max(local_min_rr_ratio, float(mrr))
        except Exception:
            local_min_rr_ratio = MIN_RR_RATIO

        # Підсилення local_min_rr з hints, якщо Stage2 підказав rr_hint
        try:
            rp = signal.get("risk_parameters") if isinstance(signal, dict) else None
            rr_hint = None
            if isinstance(rp, dict):
                hints = rp.get("hints")
                if isinstance(hints, dict):
                    val = hints.get("rr_hint")
                    if isinstance(val, (int, float)):
                        rr_hint = float(val)
            if rr_hint is not None and rr_hint > 0:
                local_min_rr_ratio = max(local_min_rr_ratio, rr_hint)
        except Exception:
            pass

        # Адаптація debounce в залежності від DEFAULT_TIMEFRAME
        try:
            from config.config import DEFAULT_TIMEFRAME

            default_tf_local = DEFAULT_TIMEFRAME
        except Exception:
            default_tf_local = "1m"
        if (
            isinstance(default_tf_local, str)
            and default_tf_local.strip().lower() == "1m"
        ):
            debounce_window = min(debounce_window, 10.0)
        else:
            debounce_window = max(debounce_window, 30.0)

        # Опціональний bypass debounce для дуже високої впевненості
        debounce_bypass_conf = _resolve_stage3_optional_float(
            sym_key, "debounce_bypass_conf", None
        )
        max_band_limit = _resolve_stage3_optional_float(
            sym_key, "max_band_pct", MAX_BAND_PCT
        )
        narrow_band_pct = _resolve_stage3_optional_float(
            sym_key, "narrow_band_pct", NARROW_BAND_PCT
        )
        narrow_allowed = _resolve_stage3_scenarios(
            sym_key, "narrow_allowed_scenarios", NARROW_ALLOWED_SCENARIOS
        )
        wide_enabled = _resolve_stage3_bool(
            sym_key, "wide_continuation_enabled", WIDE_CONTINUATION_ENABLED
        )
        wide_min_trend = _resolve_stage3_float(
            sym_key,
            "wide_continuation_min_trend",
            WIDE_CONTINUATION_MIN_TREND,
        )
        wide_min_volume = _resolve_stage3_float(
            sym_key,
            "wide_continuation_min_volume",
            WIDE_CONTINUATION_MIN_VOLUME,
        )

        # Debounce logic: даємо повторну спробу тільки якщо попередня причина enqueued
        if _should_debounce(sym_key, debounce_window):
            # TODO: можливо зберігати last_attempt_reason->timestamp в Redis для крос-інстанс debounce
            if debounce_bypass_conf is not None and confidence >= float(
                debounce_bypass_conf
            ):
                logger.info(
                    "Debounce bypass для %s: conf %.3f ≥ %.3f",
                    sym_key,
                    confidence,
                    float(debounce_bypass_conf),
                )
                logger.debug(
                    "debounce bypass debug %s: window=%.1fs bypass_conf=%.3f conf=%.3f",
                    sym_key,
                    debounce_window,
                    float(debounce_bypass_conf),
                    confidence,
                )
            else:
                logger.info(
                    "⛔️ Пропуск відкриття %s: debounce-вікно %.0fс ще активне",
                    sym_key,
                    debounce_window,
                )
                last_attempt = _LAST_OPEN_ATTEMPTS.get(sym_key)
                last_ts = None
                last_reason = None
                if last_attempt:
                    last_ts, last_reason = last_attempt
                if last_reason == "enqueued":
                    # Швидкий re-entry після enqueued — дозволяємо, не оновлюємо last_attempt
                    logger.info("Debounce fast re-entry для %s після enqueued", sym_key)
                else:
                    logger.debug(
                        "debounce skip %s: window=%.1fs bypass_conf=%s conf=%.3f last_ts=%s last_reason=%s now=%s",
                        sym_key,
                        debounce_window,
                        debounce_bypass_conf,
                        confidence,
                        (
                            f"{last_ts:.2f}"
                            if isinstance(last_ts, (int, float))
                            else last_ts
                        ),
                        last_reason,
                        time.time(),
                    )
                    skipped_by_reason["debounce"] = (
                        skipped_by_reason.get("debounce", 0) + 1
                    )
                    _log_stage3_skip(signal, "debounce")
                    _finalize_alert_session(state_manager, signal, "stage3_debounce")
                    await _shadow_skip(
                        "debounce",
                        _with_health({"window_sec": debounce_window}),
                    )
                    continue

        # Далі: легкі нуджі/корекції effective_conf, RSI penalty і ін.
        ctx_stats = signal.get("stats") or {}
        mc = signal.get("market_context") or {}
        dir_ctx = (mc.get("directional", {}) if isinstance(mc, dict) else {}) or {}
        slope_norm = (
            safe_float(
                (ctx_stats or {}).get("price_slope_atr")
                or dir_ctx.get("price_slope_atr")
            )
            or 0.0
        )
        dvr = (
            safe_float(
                (ctx_stats or {}).get("directional_volume_ratio")
                or dir_ctx.get("directional_volume_ratio")
            )
            or 1.0
        )
        cd = (
            safe_float(
                (ctx_stats or {}).get("cumulative_delta")
                or dir_ctx.get("cumulative_delta")
            )
            or 0.0
        )
        directional_strength = (
            max(0.0, slope_norm) + max(0.0, (dvr - 1.0) / 2) + max(0.0, cd / 2)
        )
        effective_conf = confidence * (1.0 + CONF_DIR_BOOST * directional_strength)
        # RSI penalty: легке зниження впевненості при екстремальних значеннях
        rsi_local = safe_float((signal.get("stats") or {}).get("rsi"))
        if rsi_local is not None:
            if rsi_local > 70 or rsi_local < 30:
                effective_conf *= 0.9

        # Додаткові фактори нуджів (breadth, vwap, htf alignment, band confidence)
        mc_local = signal.get("market_context") or {}
        breadth_val = safe_float(
            (mc_local.get("macro", {}) or {}).get("market_breadth")
        )
        if breadth_val is not None and breadth_val < 0.3:
            effective_conf *= 0.97
        vwap_dev_val = safe_float(
            (signal.get("thresholds", {}) or {}).get("vwap_deviation")
        )
        if vwap_dev_val is not None and abs(vwap_dev_val) > 2.5:
            effective_conf *= 1.02
        htf_align_val = safe_float(
            (mc_local.get("meso", {}) or {}).get("htf_alignment")
        )
        if htf_align_val is not None:
            if htf_align_val < 0:
                effective_conf *= 0.98
            elif htf_align_val > 0:
                effective_conf *= 1.01
        band_conf_val = safe_float(
            (mc_local.get("key_levels_meta", {}) or {}).get("confidence")
        )
        if band_conf_val is not None:
            band_conf_norm = max(-1.0, min(1.0, band_conf_val))
            effective_conf *= 1.0 + 0.02 * band_conf_norm

        # Low-vol адаптація: знижуємо мін. поріг на RELAX частку при низькому ATR%
        ctx_meta = signal.get("context_metadata") or {}
        if not ctx_meta and isinstance(signal.get("market_context"), dict):
            ctx_meta = (signal.get("market_context", {}) or {}).get("meta", {})
        atr_pct = safe_float((ctx_meta or {}).get("atr_pct"))
        low_gate = safe_float((ctx_meta or {}).get("low_gate"))
        adapt_conf_gate = 1.0
        if atr_pct is not None and low_gate is not None and atr_pct < low_gate:
            adapt_conf_gate = max(0.7, 1.0 - CONF_LOW_VOL_GATE_RELAX)

        # Приймаємо рішення по conf threshold
        if effective_conf < (min_confidence_trade * adapt_conf_gate):
            if confidence_multiplier != 1.0:
                logger.info(
                    "⛔️ Не відкриваємо угоду для %s: conf_eff %.3f (raw=%.3f, damped=%.3f, adapt=%.2f) < поріг %.3f",
                    symbol,
                    effective_conf,
                    confidence,
                    gate_confidence,
                    adapt_conf_gate,
                    min_confidence_trade,
                )
            else:
                logger.info(
                    "⛔️ Не відкриваємо угоду для %s: conf_eff %.3f < поріг %.3f",
                    symbol,
                    effective_conf,
                    min_confidence_trade,
                )
            logger.debug(
                f"Деталі сигналу: {json.dumps(signal, ensure_ascii=False, default=str)}"
            )
            logger.debug(
                "low_confidence skip %s: conf_raw=%.3f conf_eff=%.3f gate=%.3f thr=%.3f adapt=%.2f multiplier=%.3f",
                sym_key,
                confidence,
                effective_conf,
                gate_confidence,
                min_confidence_trade,
                adapt_conf_gate,
                confidence_multiplier,
            )
            skipped_by_reason["low_confidence"] = (
                skipped_by_reason.get("low_confidence", 0) + 1
            )
            _log_stage3_skip(signal, "low_confidence")
            _finalize_alert_session(state_manager, signal, "stage3_low_confidence")
            # low_confidence: не перезапускаємо debounce-вікно — дозволяємо майбутнім сигналам
            await _shadow_skip(
                "low_confidence",
                _with_health({"threshold": min_confidence_trade}),
            )
            continue

        # Перевірка чи вже є відкрита угода на символі
        if await trade_manager.has_open(sym_key):
            logger.info("⛔️ Пропуск відкриття %s: активна угода вже існує", sym_key)
            skipped_by_reason["already_open"] = (
                skipped_by_reason.get("already_open", 0) + 1
            )
            _log_stage3_skip(signal, "already_open")
            _finalize_alert_session(state_manager, signal, "stage3_already_open")
            await _shadow_skip("already_open", _with_health())
            continue

        # Пропускаємо не-ALERT сигнали
        sig_type = str(signal.get("signal", "NONE")).upper()
        if sig_type not in ["ALERT_BUY", "ALERT_SELL"]:
            logger.info(
                f"⛔️ Не відкриваємо угоду для {symbol}: тип сигналу {signal.get('signal')} "
                "не є ALERT_BUY/ALERT_SELL"
            )
            logger.debug(
                f"Деталі сигналу: {json.dumps(signal, ensure_ascii=False, default=str)}"
            )
            skipped_by_reason["not_alert"] = skipped_by_reason.get("not_alert", 0) + 1
            _log_stage3_skip(signal, "not_alert")
            _finalize_alert_session(state_manager, signal, "stage3_not_alert")
            _remember_attempt(sym_key, "not_alert")
            await _shadow_skip(
                "not_alert",
                _with_health({"signal": sig_type}),
            )
            continue

        # Вимагаємо явний стан 'alert' якщо передається
        state_val = signal.get("state") or signal.get("status")
        if isinstance(state_val, dict):
            state_val = state_val.get("status") or state_val.get("state")
        if isinstance(state_val, str) and state_val.lower() != "alert":
            logger.info(f"⛔️ Пропуск відкриття {symbol}: state='{state_val}' ≠ 'alert'")
            skipped_by_reason["not_state_alert"] = (
                skipped_by_reason.get("not_state_alert", 0) + 1
            )
            _log_stage3_skip(signal, "not_state_alert")
            _finalize_alert_session(state_manager, signal, "stage3_not_state_alert")
            _remember_attempt(sym_key, "not_state_alert")
            await _shadow_skip(
                "not_state_alert",
                _with_health({"state": state_val}),
            )
            continue

        # Підготовка контекстних метаданих
        ctx_meta = signal.get("context_metadata") or {}
        if not ctx_meta and isinstance(signal.get("market_context"), dict):
            ctx_meta = (signal.get("market_context", {}) or {}).get("meta", {})

        try:
            # HTF та ATR guard — фінальна перевірка перед відкриттям
            try:
                htf_ok = ctx_meta.get("htf_ok")
                atr_pct = ctx_meta.get("atr_pct")
                low_gate = ctx_meta.get("low_gate")
                if isinstance(htf_ok, bool) and not htf_ok:
                    logger.info(
                        f"⛔️ Пропуск відкриття {symbol}: 1h не підтверджує (htf_ok=False)"
                    )
                    skipped_by_reason["htf_block"] = (
                        skipped_by_reason.get("htf_block", 0) + 1
                    )
                    _log_stage3_skip(signal, "htf_block")
                    _finalize_alert_session(state_manager, signal, "stage3_htf_block")
                    _remember_attempt(sym_key, "htf_block")
                    await _shadow_skip(
                        "htf_block",
                        _with_health({"htf_ok": htf_ok}),
                    )
                    continue
                if isinstance(atr_pct, (int, float)) and isinstance(
                    low_gate, (int, float)
                ):
                    if float(atr_pct) < float(low_gate):
                        if gate_confidence >= low_vol_conf_override:
                            if logger.isEnabledFor(logging.INFO):
                                logger.info(
                                    f"⚠️ Low ATR {float(atr_pct)*100:.2f}% < {float(low_gate)*100:.2f}% для {symbol}, але застосовано override (conf={confidence:.3f})"
                                )
                        else:
                            logger.info(
                                f"⛔️ Пропуск відкриття {symbol}: ATR%% {float(atr_pct)*100:.2f}% нижче порогу {float(low_gate)*100:.2f}%"
                            )
                            skipped_by_reason["low_atr"] = (
                                skipped_by_reason.get("low_atr", 0) + 1
                            )
                            _log_stage3_skip(signal, "low_atr")
                            _finalize_alert_session(
                                state_manager, signal, "stage3_low_atr"
                            )
                            _remember_attempt(sym_key, "low_atr")
                            await _shadow_skip(
                                "low_atr",
                                _with_health(
                                    {
                                        "atr_pct": float(atr_pct),
                                        "low_gate": float(low_gate),
                                    }
                                ),
                            )
                            continue

                # Corridor / band checks (narrow/wide)
                band_pct = None
                corridor_meta = None
                if isinstance(ctx_meta, dict):
                    corridor_meta = ctx_meta.get("corridor")
                market_ctx_raw = (
                    signal.get("market_context") if isinstance(signal, dict) else {}
                )
                market_ctx = market_ctx_raw if isinstance(market_ctx_raw, dict) else {}
                if (not isinstance(corridor_meta, dict)) and market_ctx:
                    corridor_meta = market_ctx.get("key_levels_meta")
                if isinstance(corridor_meta, dict):
                    band_pct = safe_float(corridor_meta.get("band_pct"))
                scenario = str(market_ctx.get("scenario") or "").upper()
                if not scenario:
                    scenario = str(signal.get("scenario") or "").upper()

                # Narrow corridor + scenario check
                if (
                    band_pct is not None
                    and narrow_band_pct is not None
                    and band_pct < narrow_band_pct
                    and scenario not in narrow_allowed
                ):
                    allowed_repr = ", ".join(sorted(narrow_allowed))
                    logger.info(
                        "⛔️ Пропуск відкриття %s: вузький коридор %.2f%% та сценарій %s не у {%s}",
                        symbol,
                        band_pct,
                        scenario or "UNKNOWN",
                        allowed_repr,
                    )
                    skipped_by_reason["narrow_scenario"] = (
                        skipped_by_reason.get("narrow_scenario", 0) + 1
                    )
                    _log_stage3_skip(signal, "narrow_scenario")
                    _finalize_alert_session(
                        state_manager, signal, "stage3_narrow_scenario"
                    )
                    _remember_attempt(sym_key, "narrow_scenario")
                    await _shadow_skip(
                        "narrow_scenario",
                        _with_health(
                            {
                                "band_pct": band_pct,
                                "scenario": scenario,
                            }
                        ),
                    )
                    continue

                # Wide band maximum limit
                if (
                    band_pct is not None
                    and max_band_limit is not None
                    and band_pct > max_band_limit
                ):
                    logger.info(
                        "⛔️ Пропуск відкриття %s: широкий коридор %.2f%% > %.2f%%",
                        symbol,
                        band_pct,
                        max_band_limit,
                    )
                    skipped_by_reason["wide_band"] = (
                        skipped_by_reason.get("wide_band", 0) + 1
                    )
                    _log_stage3_skip(signal, "wide_band")
                    _finalize_alert_session(state_manager, signal, "stage3_wide_band")
                    _remember_attempt(sym_key, "wide_band")
                    await _shadow_skip(
                        "wide_band",
                        _with_health(
                            {
                                "band_pct": band_pct,
                                "max_band": max_band_limit,
                            }
                        ),
                    )
                    continue

                # Wide continuation evaluation (якщо band >= narrow threshold)
                if (
                    band_pct is not None
                    and narrow_band_pct is not None
                    and band_pct >= narrow_band_pct
                ):
                    allowed, trend_val, volume_val = _evaluate_wide_continuation(
                        scenario,
                        band_pct,
                        ctx_meta if isinstance(ctx_meta, dict) else {},
                        market_ctx,
                        wide_enabled=wide_enabled,
                        max_band_pct=max_band_limit,
                        allowed_scenarios=narrow_allowed,
                        min_trend=wide_min_trend,
                        min_volume=wide_min_volume,
                    )
                    if not allowed:
                        reason_msg = (
                            "continuation вимкнено"
                            if not wide_enabled
                            else "недостатні метрики"
                        )
                        if logger.isEnabledFor(logging.INFO):
                            logger.info(
                                "⛔️ Пропуск %s: %.2f%% коридор (%s, trend=%.2f, volume=%.2f)",
                                symbol,
                                band_pct,
                                reason_msg,
                                trend_val if trend_val is not None else float("nan"),
                                volume_val if volume_val is not None else float("nan"),
                            )
                        skipped_by_reason["wide_band"] = (
                            skipped_by_reason.get("wide_band", 0) + 1
                        )
                        _log_stage3_skip(signal, "wide_band_gate")
                        _finalize_alert_session(
                            state_manager, signal, "stage3_wide_band_gate"
                        )
                        _remember_attempt(sym_key, "wide_band_gate")
                        await _shadow_skip(
                            "wide_band_gate",
                            _with_health(
                                {
                                    "band_pct": band_pct,
                                    "scenario": scenario,
                                    "trend": trend_val,
                                    "volume": volume_val,
                                }
                            ),
                        )
                        continue
                    logger.debug(
                        "Wide continuation дозволено для %s: band=%.2f%% trend=%.2f volume=%.2f",
                        symbol,
                        band_pct,
                        trend_val if trend_val is not None else float("nan"),
                        volume_val if volume_val is not None else float("nan"),
                    )
            except Exception as exc:  # pragma: no cover - захист від рідкісних збоїв
                # TODO: виділити окремий metric/error counter для guard exceptions
                logger.exception("Stage3 guard exception для %s", symbol)
                await _shadow_error(
                    "guard_exception",
                    {"error": str(exc)},
                )

            # Directional pre-open guard: блокувати LONG у нещодавньому даунтренді
            try:
                sig_type_local = str(signal.get("signal", "NONE")).upper()
                if sig_type_local == "ALERT_BUY":
                    # Observe-only logging: якщо в конфігу observe=true, логувати would_block
                    cfg_observe = bool(
                        _cfg_attr("STAGE3_DIRECTIONAL_GUARD_OBSERVE", False)
                    )
                    cfg_canary_only = bool(
                        _cfg_attr("STAGE3_DIRECTIONAL_GUARD_CANARY_ONLY", True)
                    )
                    canary_raw = _cfg_attr("CANARY_SYMBOLS", ())
                    if isinstance(canary_raw, Iterable) and not isinstance(
                        canary_raw, (str, bytes)
                    ):
                        canary_set = {
                            str(item).upper() for item in canary_raw if item is not None
                        }
                    else:
                        canary_set = set()
                    dir_params_raw = _cfg_attr("DIRECTIONAL_PARAMS", {})
                    if isinstance(dir_params_raw, Mapping):
                        dir_params = dict(dir_params_raw)
                    else:
                        dir_params = {}
                    st_obs = signal.get("stats", {}) or {}
                    mc_obs = signal.get("market_context", {}) or {}
                    dir_obs = (
                        mc_obs.get("directional", {})
                        if isinstance(mc_obs, dict)
                        else {}
                    ) or {}
                    slope_ob = (
                        safe_float(
                            st_obs.get("price_slope_atr")
                            or dir_obs.get("price_slope_atr")
                        )
                        or 0.0
                    )
                    dvr_ob = (
                        safe_float(
                            st_obs.get("directional_volume_ratio")
                            or dir_obs.get("directional_volume_ratio")
                        )
                        or 1.0
                    )
                    thr_s_ob = (
                        float(
                            dir_params.get(
                                "slope_thresh_atr", dir_params.get("slope_thresh", 0.25)
                            )
                        )
                        if dir_params
                        else 0.25
                    )
                    thr_d_ob = (
                        float(dir_params.get("dv_ratio_thresh", 1.2))
                        if dir_params
                        else 1.2
                    )
                    is_canary = (not canary_set) or (sym_key in canary_set)
                    if cfg_observe and (not cfg_canary_only or is_canary):
                        if slope_ob <= -thr_s_ob and dvr_ob >= thr_d_ob:
                            try:
                                await log_stage3_event(
                                    "would_block_recent_downtrend",
                                    sym_key,
                                    {
                                        "price_slope_atr": slope_ob,
                                        "directional_volume_ratio": dvr_ob,
                                    },
                                )
                            except Exception:
                                pass
                    # Call the actual pre-open directional guard (which also logs & telemetry)
                    if not _pre_open_directional_guard(sym_key, signal):
                        logger.info(
                            "⛔️ Пропуск відкриття %s: recent_downtrend_block",
                            sym_key,
                        )
                        skipped_by_reason["recent_downtrend_block"] = (
                            skipped_by_reason.get("recent_downtrend_block", 0) + 1
                        )
                        _finalize_alert_session(
                            state_manager, signal, "stage3_recent_downtrend_block"
                        )
                        _remember_attempt(sym_key, "recent_downtrend_block")
                        await _shadow_skip(
                            "recent_downtrend_block",
                            _with_health(
                                {
                                    "price_slope_atr": safe_float(
                                        (signal.get("stats") or {}).get(
                                            "price_slope_atr"
                                        )
                                    ),
                                    "directional_volume_ratio": safe_float(
                                        (signal.get("stats") or {}).get(
                                            "directional_volume_ratio"
                                        )
                                    ),
                                }
                            ),
                        )
                        continue
            except Exception:
                # Не блокуємо, якщо guard несподівано впав (defensive)
                pass

            # Загальна підготовка: ATR захист та TP/SL consolidation
            stats = signal.get("stats") if isinstance(signal, dict) else {}
            if not isinstance(stats, dict):
                stats = {}

            # Захист від нульових/дуже малих ATR (запобігаємо діленням на нуль)
            atr = _pick_float(signal.get("atr"), stats.get("atr"))
            if atr is None or atr < 0.0001:
                atr = 0.01
                logger.warning(
                    f"Коригування ATR для {symbol}: {signal.get('atr')} -> 0.01"
                )
            signal["atr"] = atr

            # Збираємо TP/SL кандидати з різних джерел (risk_parameters priority)
            rp = signal.get("risk_parameters") or {}
            if not isinstance(rp, dict):
                rp = {}

            tp_candidates: list[Any] = [
                rp.get("take_profit"),
                rp.get("tp"),
            ]
            targets = rp.get("tp_targets")
            if isinstance(targets, (list, tuple)):
                tp_candidates.extend(targets)
            tp_candidates.append(signal.get("tp"))
            tp_val = _pick_float(*tp_candidates)

            sl_candidates: list[Any] = [
                rp.get("stop_loss"),
                rp.get("sl"),
                rp.get("sl_level"),
                signal.get("sl"),
            ]
            sl_val = _pick_float(*sl_candidates)
            if tp_val is None and logger.isEnabledFor(logging.DEBUG):
                logger.debug("Stage3 TP не визначено для %s (ризик=%s)", symbol, rp)
            if sl_val is None and logger.isEnabledFor(logging.DEBUG):
                logger.debug("Stage3 SL не визначено для %s (ризик=%s)", symbol, rp)

            # Валідна ціна — з контексту/статів
            current_price = _pick_float(
                signal.get("current_price"),
                stats.get("current_price"),
                ctx_meta.get("current_price"),
            )
            if current_price is None or current_price <= 0:
                logger.info(
                    "⛔️ Пропуск відкриття %s: відсутня валідна ціна",
                    symbol,
                )
                skipped_by_reason["no_price"] = skipped_by_reason.get("no_price", 0) + 1
                _log_stage3_skip(signal, "no_price")
                _finalize_alert_session(state_manager, signal, "stage3_no_price")
                _remember_attempt(sym_key, "no_price")
                await _shadow_skip(
                    "no_price",
                    _with_health(
                        {
                            "context_price": ctx_meta.get("current_price"),
                        }
                    ),
                )
                continue
            signal["current_price"] = current_price
            rsi_val = _pick_float(signal.get("rsi"), stats.get("rsi"))
            volume_val = _pick_float(
                signal.get("volume"),
                signal.get("volume_mean"),
                stats.get("volume_mean"),
                stats.get("volume"),
            )

            # TP/SL повинні бути валідними
            if tp_val is None or sl_val is None:
                skipped_by_reason["bad_targets"] = (
                    skipped_by_reason.get("bad_targets", 0) + 1
                )
                logger.info(
                    "⛔️ Пропуск відкриття %s: відсутні валідні TP/SL",
                    symbol,
                )
                _log_stage3_skip(signal, "bad_targets")
                _finalize_alert_session(state_manager, signal, "stage3_bad_targets")
                _remember_attempt(sym_key, "bad_targets")
                await _shadow_skip(
                    "bad_targets",
                    _with_health(
                        {
                            "tp": tp_val,
                            "sl": sl_val,
                        }
                    ),
                )
                continue

            # Інваріанти TP/SL відносно ціни
            if sig_type == "ALERT_BUY":
                invariant_ok = tp_val > current_price > sl_val
            else:
                invariant_ok = sl_val > current_price > tp_val

            if not invariant_ok:
                skipped_by_reason["bad_targets"] = (
                    skipped_by_reason.get("bad_targets", 0) + 1
                )
                logger.info(
                    "⛔️ Пропуск відкриття %s: цілі порушують інваріанти",
                    symbol,
                )
                _log_stage3_skip(signal, "bad_targets")
                _finalize_alert_session(state_manager, signal, "stage3_bad_targets")
                _remember_attempt(sym_key, "bad_targets")
                await _shadow_skip(
                    "bad_targets",
                    _with_health(
                        {
                            "tp": tp_val,
                            "sl": sl_val,
                            "price": current_price,
                        }
                    ),
                )
                continue

            # Мінімальний R:R фільтр (якщо немає trade_intent)
            rr_ratio = 0.0
            try:
                reward = abs(tp_val - current_price)
                risk = abs(current_price - sl_val)
                rr_ratio = (reward / risk) if risk > 0 else 0.0
            except Exception:
                rr_ratio = 0.0

            # Якщо trade_intent є — делегуємо рішення should_open
            intent: dict[str, Any] | None = None
            try:
                if isinstance(signal.get("trade_intent"), dict):
                    intent = signal.get("trade_intent")
            except Exception:
                intent = None
            if intent is not None:
                # Формуємо portfolio_limits (best-effort) і викликаємо should_open
                portfolio_limits: dict[str, Any] | None = None
                try:
                    active_trades_snapshot = await trade_manager.get_active_trades()
                    open_slots_remaining = max(
                        0,
                        trade_manager.max_parallel_trades - len(active_trades_snapshot),
                    )
                    portfolio_limits = {
                        "open_slots_remaining": open_slots_remaining,
                        "min_rr": local_min_rr_ratio,
                    }
                except Exception:
                    portfolio_limits = {"min_rr": local_min_rr_ratio}

                ok, meta = should_open(
                    intent,
                    current_price,
                    portfolio_limits,
                )
                if not ok:
                    reason = str(meta.get("reason") or "intent_gate")
                    logger.info("⛔️ Пропуск відкриття %s: intent.%s", symbol, reason)
                    skipped_by_reason[reason] = skipped_by_reason.get(reason, 0) + 1
                    _log_stage3_skip(signal, reason)
                    _finalize_alert_session(state_manager, signal, f"stage3_{reason}")
                    _remember_attempt(sym_key, reason)
                    await _shadow_skip(
                        reason,
                        _with_health(
                            {
                                "rr_ratio": rr_ratio,
                                "min_rr": meta.get("min_rr", local_min_rr_ratio),
                                "min_conf": meta.get("min_conf", min_confidence_trade),
                            }
                        ),
                    )
                    continue
            elif rr_ratio < local_min_rr_ratio:
                skipped_by_reason["low_rr"] = skipped_by_reason.get("low_rr", 0) + 1
                logger.info(
                    "⛔️ Пропуск відкриття %s: низький R:R=%.2f < %.2f",
                    symbol,
                    rr_ratio,
                    local_min_rr_ratio,
                )
                _log_stage3_skip(signal, "low_rr")
                _finalize_alert_session(state_manager, signal, "stage3_low_rr")
                _remember_attempt(sym_key, "low_rr")
                await _shadow_skip(
                    "low_rr",
                    _with_health({"rr_ratio": rr_ratio, "min_rr": local_min_rr_ratio}),
                )
                continue

            # Підготовка фінального payload'у для відкриття
            context_meta = {}
            signal_ctx_meta = signal.get("context_metadata")
            if isinstance(signal_ctx_meta, dict):
                context_meta = dict(signal_ctx_meta)

                stats_block = signal.get("stats") if isinstance(signal, dict) else {}
                if isinstance(stats_block, dict):
                    trap_block = stats_block.get("trap")
                    if isinstance(trap_block, dict):
                        trap_meta = context_meta.setdefault("trap", {})
                        if (
                            trap_block.get("score") is not None
                            and "trap_score" not in trap_meta
                        ):
                            try:
                                trap_meta["trap_score"] = float(trap_block.get("score"))
                            except Exception:
                                trap_meta["trap_score"] = trap_block.get("score")
                        ratios_block = trap_block.get("ratios")
                        if isinstance(ratios_block, dict):
                            if (
                                ratios_block.get("atr_spike_ratio") is not None
                                and "atr_spike_ratio" not in trap_meta
                            ):
                                spike_val = safe_float(
                                    ratios_block.get("atr_spike_ratio")
                                )
                                if spike_val is not None:
                                    trap_meta["atr_spike_ratio"] = spike_val
                        if trap_block.get("cooldown_override") and not trap_meta.get(
                            "cooldown_override"
                        ):
                            trap_meta["cooldown_override"] = True

                    vol_block = stats_block.get("volatility_regime")
                    if isinstance(vol_block, dict):
                        vol_meta = context_meta.setdefault("volatility", {})
                        if vol_block.get("regime") and "regime" not in vol_meta:
                            vol_meta["regime"] = vol_block.get("regime")
                        atr_ratio_val = safe_float(vol_block.get("atr_ratio"))
                        if atr_ratio_val is not None and "atr_ratio" not in vol_meta:
                            vol_meta["atr_ratio"] = atr_ratio_val
                        crisis_val = safe_float(vol_block.get("crisis_vol_score"))
                        if (
                            crisis_val is not None
                            and "crisis_vol_score" not in vol_meta
                        ):
                            vol_meta["crisis_vol_score"] = crisis_val
                        spike_val = safe_float(vol_block.get("atr_spike_ratio"))
                        if spike_val is not None and "atr_spike_ratio" not in vol_meta:
                            vol_meta["atr_spike_ratio"] = spike_val
                        if (
                            vol_block.get("crisis_reason")
                            and "crisis_reason" not in vol_meta
                        ):
                            vol_meta["crisis_reason"] = vol_block.get("crisis_reason")

            trade_data = {
                "symbol": symbol,
                "current_price": current_price,
                "atr": float(atr),
                "rsi": rsi_val,
                "volume": volume_val,
                "tp": float(tp_val),
                "sl": float(sl_val),
                "confidence": confidence,
                "hints": signal.get("hints", []),
                "cluster_factors": signal.get("cluster_factors", []),
                "context_metadata": context_meta,
                # strategy буде оновлено нижче за даними з Stage2, якщо доступно
                "strategy": "stage2_cluster",
            }

            # Спроба встановити стратегію від Stage2 (meta.strategy / intent.hints)
            try:
                strategy_name: str | None = None
                mc = signal.get("market_context") if isinstance(signal, dict) else None
                if isinstance(mc, dict):
                    meta = mc.get("meta")
                    if isinstance(meta, dict):
                        hint_val = meta.get("strategy_hint")
                        if (
                            PACK_EXHAUSTION_REVERSAL_ENABLED_FLAG
                            and isinstance(hint_val, str)
                            and hint_val == STRATEGY_HINT_EXHAUSTION_REVERSAL
                        ):
                            strategy_name = hint_val
                        else:
                            s = meta.get("strategy")
                            if isinstance(s, str) and s:
                                strategy_name = s
                        if (
                            isinstance(hint_val, str)
                            and hint_val
                            and hint_val == STRATEGY_HINT_EXHAUSTION_REVERSAL
                        ):
                            context_meta.setdefault("strategy_hint", hint_val)
                if strategy_name is None:
                    intent = (
                        signal.get("trade_intent") if isinstance(signal, dict) else None
                    )
                    if isinstance(intent, dict):
                        hints = intent.get("hints")
                        if isinstance(hints, dict):
                            s2 = hints.get("strategy")
                            if isinstance(s2, str) and s2:
                                strategy_name = s2
                        if strategy_name is None:
                            style = intent.get("style")
                            if isinstance(style, str) and style:
                                strategy_name = style
                        if strategy_name is None:
                            entry = intent.get("entry")
                            if isinstance(entry, dict):
                                et = entry.get("type")
                                if isinstance(et, str) and et:
                                    strategy_name = et
                if isinstance(strategy_name, str) and strategy_name:
                    trade_data["strategy"] = strategy_name
                    if strategy_name == STRATEGY_HINT_EXHAUSTION_REVERSAL:
                        logger.info(
                            "[STRICT_PHASE] %s застосовує StrategyPack %s",
                            symbol,
                            strategy_name,
                        )
            except Exception:
                # Невеликий захист — стратегія необов'язкова
                pass

            # Anti-false guard для exhaustion StrategyPack: пропускаємо слабкі обсяги
            if PACK_EXHAUSTION_REVERSAL_ENABLED_FLAG:
                strategy_name_local = (
                    str(trade_data.get("strategy", "")).strip().lower()
                )
                if strategy_name_local == STRATEGY_HINT_EXHAUSTION_REVERSAL:
                    stats_block = (
                        signal.get("stats") if isinstance(signal, dict) else {}
                    )
                    volz_local = None
                    dvr_local = None
                    if isinstance(stats_block, dict):
                        volz_local = safe_float(stats_block.get("volume_z"))
                        dvr_local = safe_float(
                            stats_block.get("directional_volume_ratio")
                        )
                    if dvr_local is None and isinstance(signal, dict):
                        market_ctx = signal.get("market_context") or {}
                        if isinstance(market_ctx, dict):
                            directional_meta = market_ctx.get("directional")
                            if isinstance(directional_meta, dict):
                                dvr_local = safe_float(
                                    directional_meta.get("directional_volume_ratio")
                                )
                    if (
                        volz_local is not None
                        and dvr_local is not None
                        and volz_local < -1.2
                        and dvr_local < 0.6
                    ):
                        logger.info(
                            "[STRICT_PHASE] %s пропуск StrategyPack exhaustion: volz=%.2f dvr=%.2f",
                            symbol,
                            volz_local,
                            dvr_local,
                        )
                        skipped_by_reason["exhaustion_guard"] = (
                            skipped_by_reason.get("exhaustion_guard", 0) + 1
                        )
                        _log_stage3_skip(signal, "exhaustion_guard")
                        _finalize_alert_session(
                            state_manager, signal, "stage3_exhaustion_guard"
                        )
                        await _shadow_skip(
                            "exhaustion_guard",
                            _with_health(
                                {
                                    "volz": volz_local,
                                    "directional_volume_ratio": dvr_local,
                                }
                            ),
                        )
                        continue

            # Якщо streaming/shadow активний — публикація/dual-write (stage3 shadow pipeline)
            if _STREAMING_ACTIVE and is_shadow_candidate:
                if _SHADOW_DUAL_WRITE_ACTIVE:
                    # Записуємо факт enqueue у local attempts: enqueued → дає fast re-entry право
                    _remember_attempt(sym_key, "enqueued")
                    await _publish_shadow_signal(signal, dict(trade_data))
                    await _shadow_open(
                        _with_health(
                            {
                                "price": current_price,
                                "tp": float(tp_val),
                                "sl": float(sl_val),
                            }
                        )
                    )
                    logger.debug(
                        "🟦 Stage3 shadow enqueue %s (conf: %.2f)",
                        symbol,
                        confidence,
                    )
                    # TODO: можливо додати callback/wait-for-confirmation механізм для dual-write
                    continue
                # Якщо dual-write вимкнено — все одно логувати як open attempt у shadow
                await _shadow_open(
                    _with_health(
                        {
                            "price": current_price,
                            "tp": float(tp_val),
                            "sl": float(sl_val),
                        }
                    )
                )
                logger.info(
                    "🟦 Stage3 shadow dual-write вимкнено для %s — fallback open",
                    symbol,
                )

            # Пряме відкриття через TradeLifecycleManager (fallback, коли без shadow або shadow вимкнено)
            try:
                # Маркуємо фактичну спробу перед викликом open_trade (локальний debounce + audit)
                _remember_attempt(sym_key, "open_attempt")
                trade_id = await trade_manager.open_trade(
                    trade_data, strategy=str(trade_data.get("strategy", "default"))
                )
            except Exception as exc:
                # TODO: розглянути retry with backoff або пом'якшений fallback (наприклад, enqueue на Redis)
                await _shadow_error(
                    "open_trade_exception",
                    {"error": str(exc)},
                )
                logger.error("Помилка відкриття угоди для %s: %s", symbol, exc)
                continue

            # Якщо open_trade повернув id — успіх
            if trade_id:
                await _shadow_open(
                    _with_health(
                        {
                            "price": current_price,
                            "tp": float(tp_val),
                            "sl": float(sl_val),
                        }
                    )
                )
                logger.info(
                    "✅ Відкрито угоду для %s (conf: %.2f) у fallback-режимі",
                    symbol,
                    confidence,
                )
                _remember_attempt(sym_key, "opened")
        except Exception as e:  # broad except: відкриття угоди не критичне
            # Defensive: логування і телеметрія про несподівану помилку на одному кандидатові
            await _shadow_error(
                "open_trade_exception",
                {"error": str(e)},
            )
            logger.error(f"Помилка відкриття угоди для {symbol}: {str(e)}")

    # Після обробки всіх кандидатів — агрегований лог причин пропусків
    if skipped_by_reason:
        try:
            logger.info(
                "Stage3 пропуски: %s", json.dumps(skipped_by_reason, ensure_ascii=False)
            )
            # TODO: emit skipped_by_reason as telemetry metric / persist to JSONL for audits
        except Exception:
            logger.info("Stage3 пропуски: %s", skipped_by_reason)


__all__ = ["open_trades"]
