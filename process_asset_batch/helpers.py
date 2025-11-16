"""Допоміжні функції для process_asset_batch (policy engine)."""

import logging
import sys
import time
from collections import OrderedDict, deque
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config_whale import STAGE2_WHALE_TELEMETRY
from config.constants import K_DIRECTIONAL_VOLUME_RATIO
from process_asset_batch.global_state import (
    _ACCUM_MONITOR_STATE,
    _DVR_EMA_STATE,
    _PROM_PRES_LAST_TS,
    _SCEN_EXPLAIN_BATCH_COUNTER,
    _SCEN_EXPLAIN_FORCE_ALL,
    _SCEN_EXPLAIN_LAST_TS,
)

# Спроба імпортувати реальну функцію (опційно); якщо немає — використовуємо None
try:
    from telemetry.prom_gauges import set_presence as _set_presence  # type: ignore
except Exception:  # pragma: no cover
    _set_presence = None  # type: ignore[assignment]
set_presence = _set_presence  # re-export (імена зберігаємо для monkeypatch у тестах)


logger = logging.getLogger("process_asset_batch.helpers")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    # Пишемо у stdout, щоб tee у tools.run_window коректно дзеркалив у файл UTF-8
    logger.addHandler(RichHandler(console=Console(file=sys.stdout), show_path=True))
    logger.propagate = False


def active_alt_keys(*alt_dicts: dict[str, bool] | None) -> set[str]:
    """Формує множину активних alt-прапорців з кількох джерел (дедуп).

    Повертає множину ключів із булевим значенням True у будь-якому джерелі.
    """
    active: set[str] = set()
    for d in alt_dicts:
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            try:
                if bool(v):
                    active.add(str(k))
            except Exception:
                continue
    return active


def should_confirm_pre_breakout(
    *,
    accept_ok: bool,
    alt_confirms_count: int,
    thr_alt_min: int,
    dominance_buy: bool,
    vwap_dev: float,
    slope_atr: float | None,
) -> tuple[bool, OrderedDict[str, bool]]:
    """Перевіряє суворі гейти для chop_pre_breakout → breakout."""

    cfg = STAGE2_WHALE_TELEMETRY or {}
    try:
        mid_dev_thr = float(cfg.get("mid_dev_thr", 0.0) or 0.0)
    except Exception:
        mid_dev_thr = 0.0
    slope_thr = 0.8
    alt_floor = max(int(thr_alt_min or 0), 0)
    alt_gate = alt_confirms_count >= alt_floor
    alt_trigger_min = max(2, alt_floor)
    alt_trigger = alt_confirms_count >= alt_trigger_min
    slope_val = float(slope_atr or 0.0)

    flags: OrderedDict[str, bool] = OrderedDict()
    flags["pre_breakout_accept"] = bool(accept_ok)
    flags["pre_breakout_alt_gate"] = bool(alt_gate)
    flags["pre_breakout_alt_trigger"] = bool(alt_trigger)
    flags["pre_breakout_dom"] = bool(dominance_buy)
    flags["pre_breakout_vdev_mid"] = bool(abs(float(vwap_dev or 0.0)) >= mid_dev_thr)
    flags["pre_breakout_slope_strict"] = bool(slope_val >= slope_thr)

    confirm_ready = (
        flags["pre_breakout_dom"]
        and flags["pre_breakout_vdev_mid"]
        and flags["pre_breakout_slope_strict"]
        and flags["pre_breakout_alt_gate"]
        and (flags["pre_breakout_accept"] or flags["pre_breakout_alt_trigger"])
    )

    return confirm_ready, flags


def score_scale_for_class(market_class: str) -> float:
    """Множник для score за класом ринку (мінімальний диф, без конфігу).

    BTC=1.0, ETH=0.95, ALTS=0.85 (консервативніші score для альтів).
    """
    mc = (market_class or "ALTS").upper()
    if mc == "BTC":
        return 1.0
    if mc == "ETH":
        return 0.95
    return 0.85


async def read_profile_cooldown(symbol: str) -> float | None:
    """Best‑effort читання cooldown_until_ts з Redis (TTL ~120с).

    Ключ: ai_one:ctx:{symbol}:cooldown (значення — epoch seconds). Повертає None при помилці.
    """
    try:
        from data.redis_connection import acquire_redis, release_redis  # type: ignore

        key = f"ai_one:ctx:{symbol.lower()}:cooldown"
        client = await acquire_redis()
        try:
            raw = await client.get(key)
            if raw is None:
                return None
            return float(raw)
        finally:
            await release_redis(client)
    except Exception:
        return None


async def write_profile_cooldown(
    symbol: str, until_ts: float, ttl_s: int = 120
) -> None:
    """Best‑effort запис cooldown_until_ts у Redis з TTL (не ламає пайплайн при збої)."""
    try:
        from data.redis_connection import acquire_redis, release_redis  # type: ignore

        key = f"ai_one:ctx:{symbol.lower()}:cooldown"
        client = await acquire_redis()
        try:
            # setex(key, ttl, value)
            await client.setex(key, int(ttl_s), str(float(until_ts)))
        finally:
            await release_redis(client)
    except Exception:
        return


def explain_should_log(
    symbol: str,
    now_ts: float,
    min_period_s: float = 10.0,
    *,
    every_n: int | None = None,
    force_all: bool | None = None,
) -> bool:
    """Рейт-ліміт для explain-логів із опцією «кожен N-й батч».

    Args:
        symbol: символ, для якого відстежуємо ка́денс.
        now_ts: поточний timestamp (sec).
        min_period_s: базовий мін-інтервал між explain-рядками.
        every_n: якщо >0 — додатково дозволяє логувати кожен N-й батч, навіть
            якщо min_period ще не минув (тонкий heartbeat).
        force_all: явний оверрайд (True → логувати завжди, False → ігнорувати
            глобальний `_SCEN_EXPLAIN_FORCE_ALL`).
    """

    sym = str(symbol or "").lower()

    def _reset_counter() -> None:
        _SCEN_EXPLAIN_BATCH_COUNTER[sym] = 0

    try:
        suppressed = int(_SCEN_EXPLAIN_BATCH_COUNTER.get(sym, 0) or 0)
    except Exception:
        suppressed = 0

    try:
        now_val = float(now_ts)
    except Exception:
        now_val = time.time()
    min_period = max(0.0, float(min_period_s or 0.0))
    effective_force = _SCEN_EXPLAIN_FORCE_ALL if force_all is None else bool(force_all)

    def _commit_log() -> bool:
        _SCEN_EXPLAIN_LAST_TS[sym] = now_val
        _reset_counter()
        return True

    if effective_force:
        return _commit_log()

    had_last = sym in _SCEN_EXPLAIN_LAST_TS
    try:
        last = float(_SCEN_EXPLAIN_LAST_TS.get(sym) or 0.0)
    except Exception:
        last = 0.0
    if not had_last:
        return _commit_log()
    if (now_val - last) >= min_period:
        return _commit_log()

    suppressed += 1
    _SCEN_EXPLAIN_BATCH_COUNTER[sym] = suppressed
    every = int(every_n or 0)
    if every > 0 and suppressed % every == 0:
        return _commit_log()
    return False


def normalize_and_cap_dvr(
    symbol: str, value: Any, *, span: int = 10, cap: float = 5.0
) -> float:
    """Нормалізує DVR: EMA(span=10) і обмежує до [-cap, cap].

    - Неграничні, нечислові значення → ігноруються (повертається 0.0 або попередня EMA).
    - При обрізанні логуємо маркер [STRICT_SANITY] DVR_CAPPED.
    """
    try:
        x = float(value)
    except Exception:
        x = 0.0
    if not (x == x and abs(x) != float("inf")):  # NaN/Inf захист
        x = 0.0
    alpha = 2.0 / (float(span) + 1.0)
    prev = float(_DVR_EMA_STATE.get(symbol, x))
    ema = alpha * x + (1.0 - alpha) * prev
    _DVR_EMA_STATE[symbol] = ema
    capped = max(-float(cap), min(float(cap), float(ema)))
    if capped != ema:
        try:
            logger.info(
                "[STRICT_SANITY] %s DVR_CAPPED raw=%.3f ema=%.3f capped=%.3f",
                symbol,
                x,
                ema,
                capped,
            )
        except Exception:
            pass
    return float(capped)


def is_heavy_compute_override(stats: dict[str, Any]) -> bool:
    """Override‑умова для heavy_compute whitelist: near_edge=True, band_pct<0.04, DVR≥0.6."""
    try:
        near_edge = stats.get("near_edge")
        near_flag = bool(near_edge in (True, "upper", "lower"))
    except Exception:
        near_flag = False
    try:
        band_pct = float(stats.get("band_pct") or 1.0)
    except Exception:
        band_pct = 1.0
    try:
        dvr_val = float(
            stats.get(K_DIRECTIONAL_VOLUME_RATIO) or stats.get("dvr") or 0.0
        )
    except Exception:
        dvr_val = 0.0
    return bool(near_flag and band_pct < 0.04 and dvr_val >= 0.6)


def extract_last_h1(stats: Any) -> dict[str, Any] | None:
    """Безпечне діставання останнього h1/agg_h1/last_h1 зрізу HTF."""

    try:
        st = stats if isinstance(stats, dict) else {}
        htf = st.get("htf") if isinstance(st.get("htf"), dict) else None
        for key in ("h1", "agg_h1", "last_h1"):
            if isinstance(htf, dict) and isinstance(htf.get(key), dict):
                return htf.get(key)  # type: ignore[return-value]
        for key in ("h1", "agg_h1", "last_h1"):
            candidate = st.get(key)
            if isinstance(candidate, dict):
                return candidate  # type: ignore[return-value]
        return None
    except Exception:
        return None


def compute_profile_hint_direction_and_score(
    whale_embedded: dict[str, Any],
    *,
    presence_min: float,
    bias_min: float,
    vdev_min: float,
    thr: dict[str, Any] | None,
    alt_flags: dict[str, bool] | None,
) -> tuple[str | None, float, bool, bool]:
    """Допоміжна pure‑функція для юніт‑тестів: визначає dir і score.

    Параметри відповідають полям, що вже обчислені у основному коді.
    Повертає: (dir or None, score, dom_ok, alt_ok).
    """
    try:
        presence_f = float(whale_embedded.get("presence") or 0.0)
        bias_f = float(whale_embedded.get("bias") or 0.0)
        vdev_f = float(whale_embedded.get("vwap_dev") or 0.0)
    except Exception:
        presence_f, bias_f, vdev_f = 0.0, 0.0, 0.0
    dom = (
        (whale_embedded.get("dominance") or {})
        if isinstance(whale_embedded, dict)
        else {}
    )
    dom_buy = bool((dom or {}).get("buy"))
    dom_sell = bool((dom or {}).get("sell"))
    candidate_up = bias_f >= 0.0
    require_dom = bool((thr or {}).get("require_dominance", False))
    dom_ok = bool(dom_buy if candidate_up else dom_sell) if require_dom else True
    alt_min = int((thr or {}).get("alt_confirm_min", 0))
    ac = (
        int(sum(1 for v in (alt_flags or {}).values() if bool(v)))
        if isinstance(alt_flags, dict)
        else 0
    )
    alt_ok = ac >= max(0, alt_min)

    dir_hint: str | None = None
    if (
        abs(vdev_f) >= float(vdev_min)
        and abs(bias_f) >= float(bias_min)
        and presence_f >= float(presence_min)
        and dom_ok
        and alt_ok
    ):
        dir_hint = "long" if (bias_f > 0.0 or dom_buy) else "short"

    # score
    def _clip01(x: float) -> float:
        return 0.0 if x < 0 else (1.0 if x > 1 else x)

    pres_n = max(0.0, min(1.0, presence_f))
    bias_n = max(0.0, min(1.0, abs(bias_f)))
    vdev_n = max(0.0, min(1.0, abs(vdev_f) / max(float(vdev_min), 1e-6)))
    agrees = (
        1.0
        if ((bias_f >= 0 and dom_buy) or (bias_f <= 0 and dom_sell) or (bias_f == 0))
        else 0.0
    )
    w1, w2, w3, w4, w5, w6 = 0.35, 0.25, 0.10, 0.10, 0.10, 0.10
    score = _clip01(
        w1 * pres_n
        + w2 * bias_n * (1.0 if agrees else 0.8)
        + w3 * vdev_n
        + w4 * (1.0 if dom_ok else 0.0)
        + w5 * 0.0  # conf додається у основному коді
        + w6 * (1.0 if alt_ok else 0.0)
    )
    return dir_hint, float(score), bool(dom_ok), bool(alt_ok)


def emit_prom_presence(stats_for_phase: dict[str, Any], symbol: str) -> bool:
    """Оновити гейдж presence з Redis/state, якщо доступний.

    Повертає True, якщо set_presence було викликано; інакше False.
    """
    try:
        wh = stats_for_phase.get("whale") if isinstance(stats_for_phase, dict) else {}
        presence_raw = (wh or {}).get("presence") if isinstance(wh, dict) else None
    except Exception:
        presence_raw = None
    if presence_raw is None:
        return False
    try:
        pres_val = float(presence_raw)
    except Exception:
        return False
    # Викликаємо Prometheus-клієнт лише якщо доступний
    try:
        if callable(set_presence):  # type: ignore[arg-type]
            set_presence(symbol.upper(), float(pres_val))  # type: ignore[misc]
            # Рейт-обмежений лог
            now = time.time()
            last = float(_PROM_PRES_LAST_TS.get(symbol.upper(), 0.0) or 0.0)
            if now - last >= 30.0:
                logger.info(
                    "[PROM] presence set symbol=%s value=%.3f", symbol.upper(), pres_val
                )
                _PROM_PRES_LAST_TS[symbol.upper()] = now
            return True
    except Exception:
        return False
    return False


# ── HTF influence helper (Stage C, pure) ─────────────────────────────────────
def htf_adjust_alt_min(
    market_class: str,
    last_h1: dict[str, Any] | None,
    thr: dict[str, Any] | None,
    *,
    enabled: bool,
) -> dict[str, Any]:
    """Зменшує alt_confirm_min на 1 (мінімум 1) для BTC/ETH при h1 up+confluence.

    Без побічних ефектів: повертає копію thr або {}.
    """
    try:
        if not (enabled and market_class in ("BTC", "ETH") and isinstance(thr, dict)):
            return dict(thr or {})
        try:
            from context.htf_context import h1_ctx as _h1_ctx  # type: ignore
        except Exception:
            return dict(thr or {})
        if not isinstance(last_h1, dict):
            return dict(thr or {})
        try:
            trend, conf = _h1_ctx(last_h1)
        except Exception:
            trend, conf = ("unknown", False)
        if trend == "up" and bool(conf):
            out = dict(thr or {})
            base = int(out.get("alt_confirm_min", 0) or 0)
            out["alt_confirm_min"] = max(1, base - 1)
            return out
        return dict(thr or {})
    except Exception:
        return dict(thr or {})


def compute_ctx_persist(
    buf: list[dict[str, Any]] | deque[dict[str, Any]], pres_thr: float
) -> tuple[float | None, float | None]:
    """Обчислити near_edge_persist та presence_sustain з буфера.

    near_edge_persist: частка записів із near_edge ∈ {True,"upper","lower"}.
    presence_sustain: частка записів із presence ≥ pres_thr (None → 0.0).
    Повертає (edge_persist, presence_sustain) у [0,1] або (None, None), якщо buf порожній.
    """
    try:
        n = len(buf)  # type: ignore[arg-type]
    except Exception:
        n = 0
    if not n:
        return (None, None)
    edge_cnt = 0
    pres_cnt = 0
    for e in list(buf)[-min(n, 12) :]:
        ne = e.get("near_edge")
        if ne is True or (isinstance(ne, str) and ne.lower() in {"upper", "lower"}):
            edge_cnt += 1
        p = e.get("presence")
        try:
            pval = float(p) if p is not None else 0.0
        except Exception:
            pval = 0.0
        if pval >= float(pres_thr):
            pres_cnt += 1
    edge_persist = round(edge_cnt / float(n), 3)
    pres_sustain = round(pres_cnt / float(n), 3)
    return (edge_persist, pres_sustain)


def update_accum_monitor(
    symbol: str, *, accum_cnt: int, dist_cnt: int
) -> dict[str, Any]:
    """Простий постпроцесор для відстеження динаміки зон акумуляції/дистрибуції."""

    state = _ACCUM_MONITOR_STATE.setdefault(
        symbol,
        {
            "last_accum": None,
            "last_dist": None,
            "streak_up": 0,
            "streak_down": 0,
        },
    )
    prev_accum = state.get("last_accum")
    prev_dist = state.get("last_dist")
    delta_accum = (accum_cnt - int(prev_accum)) if isinstance(prev_accum, int) else None
    delta_dist = (dist_cnt - int(prev_dist)) if isinstance(prev_dist, int) else None

    streak_up = int(state.get("streak_up", 0))
    streak_down = int(state.get("streak_down", 0))
    if isinstance(delta_accum, int):
        if delta_accum > 0:
            streak_up += 1
            streak_down = 0
        elif delta_accum < 0:
            streak_down += 1
            streak_up = 0
        else:
            streak_up = max(streak_up - 1, 0)
            streak_down = max(streak_down - 1, 0)
    else:
        streak_up = 0
        streak_down = 0

    state.update(
        {
            "last_accum": accum_cnt,
            "last_dist": dist_cnt,
            "streak_up": streak_up,
            "streak_down": streak_down,
            "updated_ms": int(time.time() * 1000),
        }
    )

    trend = "init"
    if isinstance(delta_accum, int):
        if delta_accum > 0:
            trend = "up"
        elif delta_accum < 0:
            trend = "down"
        else:
            trend = "flat"

    return {
        "accum": accum_cnt,
        "dist": dist_cnt,
        "delta_accum": delta_accum,
        "delta_dist": delta_dist,
        "streak_up": streak_up,
        "streak_down": streak_down,
        "trend": trend,
        "updated_ms": state["updated_ms"],
    }


def flag_htf_enabled(default: bool = False) -> bool:
    """Безпечне читання прапора HTF_CONTEXT_ENABLED із config.flags.

    Повертає default при помилці або відсутності модуля.
    """
    try:  # lazy import, щоб уникати жорсткої залежності
        from config.flags import HTF_CONTEXT_ENABLED  # type: ignore

        return bool(HTF_CONTEXT_ENABLED)
    except Exception:
        return bool(default)


# Юніт-тести очікують збереження старих підкреслених аліасів


def _explain_should_log(symbol: str, now_ts: float, min_period_s: float = 10.0) -> bool:
    """Спрощена версія для тестів: примусово force_all=False."""

    return explain_should_log(
        symbol,
        now_ts,
        min_period_s=min_period_s,
        every_n=None,
        force_all=False,
    )


_should_confirm_pre_breakout = should_confirm_pre_breakout
_normalize_and_cap_dvr = normalize_and_cap_dvr
_is_heavy_compute_override = is_heavy_compute_override
_active_alt_keys = active_alt_keys
_htf_adjust_alt_min = htf_adjust_alt_min
_emit_prom_presence = emit_prom_presence
