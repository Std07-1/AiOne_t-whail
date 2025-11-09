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

    BTC=1.0, ETH=0.95, ALTS=0.80 (консервативніші score для альтів).
    """
    mc = (market_class or "ALTS").upper()
    if mc == "BTC":
        return 1.0
    if mc == "ETH":
        return 0.95
    return 0.80


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


def explain_should_log(symbol: str, now_ts: float, min_period_s: float = 10.0) -> bool:
    """Чи варто логувати [SCEN_EXPLAIN] зараз для символу.

    Повертає True, якщо минуло ≥ ``min_period_s`` від останнього логування для
    ``symbol``. Окремо в коді також логуватимемо кожен N-й батч (тонкий слід).
    """
    # У режимі офлайн‑реплею або за явним оверрайдом — логувати завжди
    if _SCEN_EXPLAIN_FORCE_ALL:
        return True
    try:
        last = float(_SCEN_EXPLAIN_LAST_TS.get(symbol.lower()) or 0.0)
    except Exception:
        last = 0.0
    return bool((now_ts - last) >= float(min_period_s))


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


def build_stage2_hint_from_whale(
    whale_embedded: dict[str, Any] | None,
    hint_cfg: dict[str, Any] | None = None,
    *,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Формує stage2_hint на основі whale‑телеметрії (pure-функція)."""

    cfg = hint_cfg or (STAGE2_WHALE_TELEMETRY or {})
    if not isinstance(whale_embedded, dict):
        now_value = int(now_ms if now_ms is not None else time.time() * 1000)
        cooldown_cfg = (cfg.get("hint_cooldown") or {}) if isinstance(cfg, dict) else {}
        base_cd = float(cooldown_cfg.get("base", 120.0))
        return {
            "dir": "NEUTRAL",
            "score": 0.0,
            "reasons": [],
            "ts": now_value,
            "cooldown_s": int(round(base_cd)),
        }

    def _clip01(val: float) -> float:
        return 0.0 if val < 0.0 else (1.0 if val > 1.0 else val)

    presence_f = float(whale_embedded.get("presence") or 0.0)
    bias_f = float(whale_embedded.get("bias") or 0.0)
    vdev_f = float(whale_embedded.get("vwap_dev") or 0.0)
    features_map = (
        whale_embedded.get("features")
        if isinstance(whale_embedded.get("features"), dict)
        else {}
    )
    slope_f = float(features_map.get("slope_twap") or 0.0)
    dom_state = (
        whale_embedded.get("dominance")
        if isinstance(whale_embedded.get("dominance"), dict)
        else {}
    )
    dom_meta = (
        whale_embedded.get("dominance_meta")
        if isinstance(whale_embedded.get("dominance_meta"), dict)
        else {}
    )

    dir_hint = "NEUTRAL"
    if bool(dom_state.get("buy")) and bias_f > 0:
        dir_hint = "UP"
    elif bool(dom_state.get("sell")) and bias_f < 0:
        dir_hint = "DOWN"

    weights = (cfg.get("hint_weights") or {}) if isinstance(cfg, dict) else {}
    presence_norm = _clip01(presence_f)
    bias_norm = _clip01(abs(bias_f))
    strong_dev = float(cfg.get("strong_dev_thr", 0.02) or 0.02)
    vdev_norm = min(1.0, abs(vdev_f) / max(strong_dev, 1e-6))
    score = (
        presence_norm * float((weights or {}).get("presence", 0.45))
        + bias_norm * float((weights or {}).get("bias", 0.35))
        + vdev_norm * float((weights or {}).get("vwap_dev", 0.20))
    )
    score = _clip01(score)

    reasons: list[str] = []
    if vdev_f > 0:
        reasons.append("vwap_dev↑")
    elif vdev_f < 0:
        reasons.append("vwap_dev↓")

    slope_thr = float((cfg.get("dominance") or {}).get("slope_abs_min", 0.0))
    if slope_f > slope_thr:
        reasons.append("slope_twap↑")
    elif slope_f < -slope_thr:
        reasons.append("slope_twap↓")

    zones_summary = (
        whale_embedded.get("zones_summary")
        if isinstance(whale_embedded.get("zones_summary"), dict)
        else {}
    )
    try:
        dist_cnt = int(zones_summary.get("dist_cnt", 0))
        reasons.append(f"zones.dist={dist_cnt}")
    except Exception:
        pass

    if whale_embedded.get("stale"):
        penalty = float(cfg.get("stale_score_penalty", 0.5))
        score *= _clip01(penalty)
        reasons.append("stale")

    if dir_hint == "NEUTRAL":
        neutral_penalty = float(cfg.get("neutral_score_penalty", 0.6))
        score *= _clip01(neutral_penalty)
        buy_candidate = bool(dom_meta.get("buy_candidate"))
        sell_candidate = bool(dom_meta.get("sell_candidate"))
        buy_confirmed = bool(dom_meta.get("buy_confirmed"))
        sell_confirmed = bool(dom_meta.get("sell_confirmed"))
        if buy_candidate and not buy_confirmed:
            reasons.append("alt_confirm=0")
        if sell_candidate and not sell_confirmed and "alt_confirm=0" not in reasons:
            reasons.append("alt_confirm=0")

    score = _clip01(score)
    cooldown_cfg = (cfg.get("hint_cooldown") or {}) if isinstance(cfg, dict) else {}
    base_cd = float(cooldown_cfg.get("base", 120.0))
    impulse_cd = float(cooldown_cfg.get("impulse", 45.0))
    cooldown_s = impulse_cd if dir_hint != "NEUTRAL" else base_cd

    now_value = int(now_ms if now_ms is not None else time.time() * 1000)
    return {
        "dir": dir_hint,
        "score": float(round(score, 4)),
        "reasons": reasons,
        "ts": now_value,
        "cooldown_s": int(round(cooldown_s)),
    }


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


def ensure_whale_default(stats: dict[str, Any]) -> None:
    """Гарантує наявність stats.whale із дефолтами (presence=0.0, stale=True)."""
    try:
        w = stats.get("whale")
        if not isinstance(w, dict):
            stats["whale"] = {
                "version": "v2",
                "stale": True,
                "presence": 0.0,
                "bias": 0.0,
                "vwap_dev": None,
                "zones_summary": {"accum_cnt": 0, "dist_cnt": 0},
                "vol_regime": "unknown",
            }
            try:
                logger.info(
                    "[STRICT_SANITY] %s: whale default presence=0.00 stale=True",
                    str(stats.get("symbol") or "-"),
                )
            except Exception:
                pass

        def extract_last_h1(stats: Any) -> dict[str, Any] | None:
            """Безпечне діставання останнього h1/agg_h1/last_h1 зрізу HTF.

            Порядок:
            1. Перевіряємо stats['htf'][key] для ключів ('h1','agg_h1','last_h1').
            2. Якщо немає — шукаємо прямо у stats[key].
            Повертає dict або None. Не кидає винятків.
            """
            try:
                st = stats if isinstance(stats, dict) else {}
                htf = st.get("htf") if isinstance(st.get("htf"), dict) else None
                for k in ("h1", "agg_h1", "last_h1"):
                    if isinstance(htf, dict) and isinstance(htf.get(k), dict):
                        return htf.get(k)  # type: ignore[return-value]
                for k in ("h1", "agg_h1", "last_h1"):
                    v = st.get(k)
                    if isinstance(v, dict):
                        return v  # type: ignore[return-value]
                return None
            except Exception:
                return None

    except Exception:
        return

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
            if (
                (bias_f >= 0 and dom_buy) or (bias_f <= 0 and dom_sell) or (bias_f == 0)
            )
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
