"""
prefilter_strict.py

Strict-префільтр для AiOne_t:
  • Жорсткі гейти: ліквідність/обіг, участь (vol_z, DVR), структура (band_pct, CD),
    HTF-узгодженість, анти-патерни (false_breakout, trap_high_risk), stale-whale штраф.
  • Скоринг для пріоритезації: participation + structure + HTF − penalties.
  • Публікація результатів у Redis: ai_one:prefilter:strict:list (+TTL).
  • Прометеєві метрики: prefilter_kept_total, prefilter_runtime_ms, prefilter_score_avg.

Очікуваний snapshot на вхід (dict per symbol) — поля Stage1/Stage2:
{
  "symbol": "BTCUSDT",
  "turnover_usd": float,      # обіг за сесію/вікно
  "atr_pct": float,           # ATR / price
  "rsi": float,
  "band_pct": float,
  "near_edge": "upper|lower|none",
  "vol_z": float,
  "dvr": float,
  "cd": float,                # cumulative delta нормалізований [-1..+1]
  "slope_atr": float,         # схил у ATR
  "htf_ok": bool,
  "htf_strength": float,
  "phase": str|None,          # e.g. "momentum", "post_breakout", "false_breakout"
  "trap_score": float|None,   # 0..1
  "whale": {                  # v2 агрегат
      "stale": bool,
      "presence": float,      # 0..1
      "bias": float           # -1..+1
  }
}
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

try:
    # Допускаємо відсутність Prometheus у мінімальних стендах
    from prometheus_client import Counter, Gauge
except Exception:  # pragma: no cover
    Counter = Gauge = None  # type: ignore

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

# Фіче-флаги: relaxed HTF-strength
try:  # без жорсткої залежності
    from config import flags as flags  # type: ignore

    _RELAX_ALLOW_HTF_STRENGTH = bool(
        getattr(flags, "STRICT_PREFILTER_RELAXED_ALLOW_HTF_STRENGTH", False)
    )
    _RELAX_MIN_HTF_STRENGTH = float(
        getattr(flags, "STRICT_PREFILTER_RELAXED_MIN_HTF_STRENGTH", 0.02)
    )
except Exception:  # pragma: no cover
    _RELAX_ALLOW_HTF_STRENGTH = False
    _RELAX_MIN_HTF_STRENGTH = 0.02

# ── Логування ──────────────────────────────────────────────────────────────────
from rich.logging import RichHandler  # type: ignore

logger = logging.getLogger("prefilter_strict")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=False, markup=True)],
    )

# ── Метрики ────────────────────────────────────────────────────────────────────
if Gauge and Counter:
    M_KEPT = Counter("aione_prefilter_kept_total", "Symbols kept by strict prefilter")
    M_KEPT_MAIN = Counter(
        "aione_prefilter_kept_main_total",
        "Symbols kept in main lane by strict prefilter",
    )
    M_KEPT_RELAXED = Counter(
        "aione_prefilter_kept_relaxed_total",
        "Symbols kept in relaxed lane by strict prefilter",
    )
    M_REJECT = Counter(
        "aione_prefilter_reject_reason_total",
        "Reject reasons in strict prefilter",
        ["reason"],
    )
    M_RUNTIME = Gauge("aione_prefilter_runtime_ms", "Strict prefilter runtime in ms")
    M_CYCLE_SEC = Gauge(
        "aione_prefilter_cycle_seconds", "Strict prefilter cycle seconds"
    )
    M_RELAXED_RATIO = Gauge(
        "aione_prefilter_relaxed_ratio", "Ratio of relaxed lane in final kept"
    )
    M_SCORE = Gauge("aione_prefilter_score_avg", "Average score of kept symbols")
else:  # pragma: no cover
    M_KEPT = M_KEPT_MAIN = M_KEPT_RELAXED = M_REJECT = M_RUNTIME = M_CYCLE_SEC = (
        M_RELAXED_RATIO
    ) = M_SCORE = None


# ── Пороги та ваги ─────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class StrictThresholds:
    # Жорсткі відсікання
    min_turnover_usd: float = 2_000.0  # уникнути мікро-лоудів
    min_band_pct: float = 0.006  # ≥0.6% коридор
    min_participation_volz: float = 1.8  # базова участь за vol_z
    alt_participation_volz: float = 0.8  # альтернативна участь при DVR (трохи м'якше)
    alt_min_dvr: float = 1.0  # мінімальний DVR для альтернативи
    min_cd: float = 0.0  # без переваги продавця
    max_phase_deny: tuple[str, ...] = ("false_breakout",)
    max_trap_score: float = 0.85  # жорсткий анти-ризик
    cd_floor: float = -0.05  # підлога для CD (None→0.0, далі max(cd, floor))

    # Мʼякі штрафи
    whale_stale_penalty: float = 0.15
    low_dvr_penalty: float = 0.1  # якщо 0.8≤DVR<1.0
    htf_required: bool = False  # м'якший режим за замовчуванням
    min_htf_score: float = 0.00  # поріг на htf_score (0.0 = вимкнено)

    # Скоринг
    w_volz: float = 0.45
    w_dvr: float = 0.20
    w_cd: float = 0.15
    w_slope: float = 0.10
    w_htf: float = 0.10

    # Бонус за широкий діапазон (навіть при низькому vol_z)
    band_wide_bonus_thr: float = 0.03  # 3%
    band_wide_bonus: float = 1.05

    # Розмір вибірки
    top_k: int = 24
    relaxed_top_k: int = 10  # додаткові місця для relaxed-рейтингу (розширено)
    relaxed_w: float = 0.55  # зниження бала для relaxed-кейсів
    # Поріг band для relaxed-гейта
    relaxed_min_band_pct: float = 0.008  # 0.8%


# ── Нормалізації ──────────────────────────────────────────────────────────────
def _nz(v: float | None, default: float = 0.0) -> float:
    return float(v) if v is not None else default


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _norm_volz(v: float) -> float:
    # vol_z 0..5+ → 0..1
    return _clip(v / 3.0, 0.0, 1.0)


def _norm_dvr(v: float) -> float:
    # DVR 0..3 → 0..1
    return _clip(v / 2.0, 0.0, 1.0)


def _norm_cd(v: float) -> float:
    # CD -1..+1 → 0..1, але нагорода лише за позитив
    return _clip((v + 1.0) / 2.0, 0.0, 1.0) if v > 0 else 0.0


def _norm_slope(v: float) -> float:
    # slope@ATR 0..4 → 0..1
    return _clip(v / 3.0, 0.0, 1.0)


def _norm_htf(strength: float, ok: bool) -> float:
    return _clip(strength / 0.05, 0.0, 1.0) if ok else 0.0


# ── Основна логіка ────────────────────────────────────────────────────────────
def score_symbol(s: dict[str, Any], th: StrictThresholds) -> tuple[float, list[str]]:
    """Повертає (score, reasons). Score=0 означає відсікання."""
    reasons: list[str] = []

    # Жорсткі гейти
    if _nz(s.get("turnover_usd")) < th.min_turnover_usd:
        reasons.append("deny_turnover")
        logger.debug(
            "[STRICT_PHASE] symbol=%s deny=turnover turnover_usd=%.2f min=%.2f",
            s.get("symbol"),
            _nz(s.get("turnover_usd")),
            th.min_turnover_usd,
        )
        if M_REJECT:  # прометей: причина відсікання
            try:
                M_REJECT.labels(reason="deny_turnover").inc()
            except Exception:
                pass
        return 0.0, reasons

    if _nz(s.get("band_pct")) < th.min_band_pct:
        reasons.append("deny_band")
        logger.debug(
            "[STRICT_PHASE] symbol=%s deny=band band_pct=%.4f min=%.4f",
            s.get("symbol"),
            _nz(s.get("band_pct")),
            th.min_band_pct,
        )
        if M_REJECT:
            try:
                M_REJECT.labels(reason="deny_band").inc()
            except Exception:
                pass
        return 0.0, reasons

    if s.get("phase") in th.max_phase_deny:
        reasons.append("deny_phase_false_breakout")
        logger.debug(
            "[STRICT_PHASE] symbol=%s deny=phase phase=%s",
            s.get("symbol"),
            s.get("phase"),
        )
        if M_REJECT:
            try:
                M_REJECT.labels(reason="deny_phase_false_breakout").inc()
            except Exception:
                pass
        return 0.0, reasons

    if _nz(s.get("trap_score")) > th.max_trap_score:
        reasons.append("deny_trap_high")
        logger.debug(
            "[STRICT_PHASE] symbol=%s deny=trap trap_score=%.3f max=%.3f",
            s.get("symbol"),
            _nz(s.get("trap_score")),
            th.max_trap_score,
        )
        if M_REJECT:
            try:
                M_REJECT.labels(reason="deny_trap_high").inc()
            except Exception:
                pass
        return 0.0, reasons

    if th.htf_required:
        htf_ok_flag = bool(s.get("htf_ok", False))
        htf_score_val = _nz(s.get("htf_score"))
        if (not htf_ok_flag) and (htf_score_val < th.min_htf_score):
            reasons.append("deny_htf")
            logger.debug(
                "[STRICT_PHASE] symbol=%s deny=htf htf_ok=%s htf_score=%.3f min=%.3f",
                s.get("symbol"),
                s.get("htf_ok"),
                htf_score_val,
                th.min_htf_score,
            )
            if M_REJECT:
                try:
                    M_REJECT.labels(reason="deny_htf").inc()
                except Exception:
                    pass
            return 0.0, reasons
    else:
        # Якщо htf_required=False, але задано мінімальний htf_score — застосовуємо його як м'який гейт
        htf_score_val = _nz(s.get("htf_score"))
        if th.min_htf_score > 0 and htf_score_val < th.min_htf_score:
            reasons.append("deny_htf_score")
            logger.debug(
                "[STRICT_PHASE] symbol=%s deny=htf_score htf_score=%.3f min=%.3f",
                s.get("symbol"),
                htf_score_val,
                th.min_htf_score,
            )
            if M_REJECT:
                try:
                    M_REJECT.labels(reason="deny_htf_score").inc()
                except Exception:
                    pass
            return 0.0, reasons

    vol_z = _nz(s.get("vol_z"))
    dvr = _nz(s.get("dvr"))
    cd_raw = s.get("cd")
    # CD: None→0.0, далі застосовуємо підлогу cd_floor
    cd = max(_nz(cd_raw, 0.0), th.cd_floor)

    part_ok = (vol_z >= th.min_participation_volz) or (
        vol_z >= th.alt_participation_volz and dvr >= th.alt_min_dvr
    )

    deny_part = not part_ok
    deny_cd = cd < th.min_cd

    relaxed_flag = False
    if deny_part or deny_cd:
        # relaxed-lane: шанс для майже-ок випадків із ознаками імпульсу
        htf_ok_flag = bool(s.get("htf_ok", False))
        htf_strength_val = _nz(s.get("htf_strength"))
        relaxed_htf_ok = htf_ok_flag or (
            _RELAX_ALLOW_HTF_STRENGTH and htf_strength_val >= _RELAX_MIN_HTF_STRENGTH
        )
        relaxed = (
            _nz(s.get("slope_atr")) >= 0.8
            and _nz(s.get("band_pct")) >= th.relaxed_min_band_pct
            and relaxed_htf_ok
            and (
                cd >= -0.10
                or (
                    s.get("near_edge") in ("upper", "lower")
                    and _nz(s.get("trap_score"), 0.0) <= 0.5
                )
            )
        )
        if not relaxed:
            r: list[str] = []
            if deny_part:
                r.append("deny_participation")
            if deny_cd:
                r.append("deny_cd")
            logger.debug(
                "[STRICT_SANITY] symbol=%s denied reasons=%s",
                s.get("symbol"),
                r,
            )
            if M_REJECT:
                for rr in r:
                    try:
                        M_REJECT.labels(reason=rr).inc()
                    except Exception:
                        pass
            return 0.0, r
        relaxed_flag = True

    # Скоринг
    slope = _nz(s.get("slope_atr"))
    htf_score = _norm_htf(_nz(s.get("htf_strength")), bool(s.get("htf_ok", False)))

    base = (
        th.w_volz * _norm_volz(vol_z)
        + th.w_dvr * _norm_dvr(dvr)
        + th.w_cd * _norm_cd(cd)
        + th.w_slope * _norm_slope(slope)
        + th.w_htf * htf_score
    )

    # Штрафи
    whale = s.get("whale") or {}
    if whale.get("stale", False):
        base *= 1.0 - th.whale_stale_penalty
        logger.debug(
            "[STRICT_WHALE] symbol=%s whale_stale_penalty=%.3f",
            s.get("symbol"),
            th.whale_stale_penalty,
        )

    if th.alt_min_dvr <= dvr < 1.0:
        base *= 1.0 - th.low_dvr_penalty
        logger.debug(
            "[STRICT_PHASE] symbol=%s low_dvr_penalty=%.3f dvr=%.3f",
            s.get("symbol"),
            th.low_dvr_penalty,
            dvr,
        )

    # Дрібний бонус за near_edge у бік імпульсу
    ne = s.get("near_edge")
    if ne in ("upper", "lower") and slope > 0:
        base *= 1.05
        logger.debug(
            "[STRICT_PHASE] symbol=%s near_edge_bonus=1.05 edge=%s slope=%.3f",
            s.get("symbol"),
            ne,
            slope,
        )
    # Додатковий бонус за широкий діапазон (band_pct)
    band_val = _nz(s.get("band_pct"))
    if band_val >= th.band_wide_bonus_thr:
        base *= th.band_wide_bonus
        logger.debug(
            "[STRICT_PHASE] symbol=%s band_wide_bonus=%.2f band_pct=%.4f thr=%.4f",
            s.get("symbol"),
            th.band_wide_bonus,
            band_val,
            th.band_wide_bonus_thr,
        )

    if relaxed_flag:
        base *= th.relaxed_w

    logger.debug(
        "[STRICT_PHASE] symbol=%s score=%.4f reasons=%s",
        s.get("symbol"),
        float(base),
        ["ok_relaxed" if relaxed_flag else "ok"],
    )
    return float(base), ["ok_relaxed" if relaxed_flag else "ok"]


def prefilter_symbols(
    snapshots: Iterable[dict[str, Any]],
    thresholds: StrictThresholds | None = None,
    *,
    redis_client: Any = None,
    redis_key: str = "ai_one:prefilter:strict:list",
    redis_debug_key: str | None = "ai_one:prefilter:last:strict",
    ttl_sec: int = 30,
    debug_ttl_sec: int = 120,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    """
    Відбирає та ранжує символи для моніторингу strict-логікою.
    Повертає список dict: {symbol, score, reasons, snapshot}.
    """
    t0 = time.time()
    kept_main: list[tuple[str, float, list[str], dict[str, Any]]] = []
    kept_relaxed: list[tuple[str, float, list[str], dict[str, Any]]] = []
    reject_reasons: dict[str, int] = {}

    if thresholds is None:
        thresholds = StrictThresholds()

    for s in snapshots:
        sym = s.get("symbol")
        sc, rs = score_symbol(s, thresholds)
        if sc > 0:
            if "ok_relaxed" in rs:
                kept_relaxed.append((str(sym), sc, rs, s))
            else:
                kept_main.append((str(sym), sc, rs, s))
            logger.debug(
                "[STRICT_PHASE] symbol=%s PASSED score=%.4f reasons=%s", sym, sc, rs
            )
        else:
            logger.debug("[STRICT_PHASE] symbol=%s REJECTED reasons=%s", sym, rs)
            for rr in rs:
                reject_reasons[rr] = reject_reasons.get(rr, 0) + 1

    kept_main.sort(key=lambda x: x[1], reverse=True)
    kept_relaxed.sort(key=lambda x: x[1], reverse=True)

    ranked = kept_main[: thresholds.top_k] + kept_relaxed[: thresholds.relaxed_top_k]
    ranked.sort(key=lambda x: x[1], reverse=True)
    top = ranked[: thresholds.top_k]

    result = [
        {"symbol": sym, "score": round(score, 4), "reasons": rs, "snapshot": snap}
        for sym, score, rs, snap in top
    ]

    # Метрики
    dt_ms = int((time.time() - t0) * 1000)
    kept_total = len(result)
    kept_main_n = len(kept_main[: thresholds.top_k])
    kept_relaxed_n = len(kept_relaxed[: thresholds.relaxed_top_k])
    relaxed_ratio = (kept_relaxed_n / max(1, kept_total)) if kept_total else 0.0

    if M_RUNTIME:
        M_RUNTIME.set(dt_ms)
    if M_CYCLE_SEC:
        try:
            M_CYCLE_SEC.set(dt_ms / 1000.0)
        except Exception:
            pass
    if M_KEPT:
        M_KEPT.inc(kept_total)
    if M_KEPT_MAIN and kept_main_n:
        M_KEPT_MAIN.inc(kept_main_n)
    if M_KEPT_RELAXED and kept_relaxed_n:
        M_KEPT_RELAXED.inc(kept_relaxed_n)
    if M_RELAXED_RATIO:
        try:
            M_RELAXED_RATIO.set(relaxed_ratio)
        except Exception:
            pass
    if M_SCORE and result:
        M_SCORE.set(sum(r["score"] for r in result) / len(result))

    logger.info(
        "[STRICT_SANITY] [prefilter] kept=%d kept_main=%d kept_relaxed=%d relaxed_ratio=%.2f dt=%dms avg_score=%.3f",
        kept_total,
        kept_main_n,
        kept_relaxed_n,
        relaxed_ratio,
        dt_ms,
        (sum(r["score"] for r in result) / max(1, len(result))),
    )

    # Публікація у Redis (опційно)
    if redis_client is not None:
        payload = {
            "ts": now_ms or int(time.time() * 1000),
            "top_k": thresholds.top_k,
            "items": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "lane": ("relaxed" if ("ok_relaxed" in r["reasons"]) else "main"),
                    # компактна картка для UI/дебага
                    "card": {
                        "vol_z": _nz(r["snapshot"].get("vol_z")),
                        "dvr": _nz(r["snapshot"].get("dvr")),
                        "cd": _nz(r["snapshot"].get("cd")),
                        "band": _nz(r["snapshot"].get("band_pct")),
                        "htf": {
                            "ok": bool(r["snapshot"].get("htf_ok", False)),
                            "str": _nz(r["snapshot"].get("htf_strength")),
                        },
                        "phase": r["snapshot"].get("phase"),
                        "whale": {
                            "stale": bool(
                                (r["snapshot"].get("whale") or {}).get("stale", False)
                            ),
                            "presence": _nz(
                                (r["snapshot"].get("whale") or {}).get("presence")
                            ),
                            "bias": _nz((r["snapshot"].get("whale") or {}).get("bias")),
                        },
                    },
                }
                for r in result
            ],
        }
        try:
            # Підтримка як sync Redis, так і redis.asyncio або RedisAdapter.jset
            publish_done = False
            # 1) Якщо це RedisAdapter з jset()
            jset = getattr(redis_client, "jset", None)
            if callable(jset):
                try:
                    coro = jset(redis_key, value=payload, ttl=ttl_sec)
                    if asyncio.iscoroutine(coro):
                        try:
                            asyncio.get_running_loop().create_task(coro)
                        except RuntimeError:
                            # немає активного loop — виконуємо синхронно
                            asyncio.run(coro)
                    publish_done = True
                except Exception:
                    publish_done = False

            if not publish_done:
                # 2) Спроба через .set() (може бути sync або async)
                setter = getattr(redis_client, "set", None)
                if callable(setter):
                    res = setter(redis_key, json.dumps(payload), ex=ttl_sec)
                    if asyncio.iscoroutine(res):
                        try:
                            asyncio.get_running_loop().create_task(res)
                        except RuntimeError:
                            asyncio.run(res)
                    publish_done = True

            if publish_done:
                logger.debug(
                    "[STRICT_GUARD] [prefilter] published to Redis key=%s ttl=%ds",
                    redis_key,
                    ttl_sec,
                )
        except Exception as e:  # pragma: no cover
            logger.warning("[STRICT_GUARD] Redis publish failed: %s", e)

        # Debug payload: зберігаємо окремо агрегати
        if redis_debug_key:
            dbg = {
                "ts": now_ms or int(time.time() * 1000),
                "kept_main": [sym for sym, _, _, _ in kept_main[: thresholds.top_k]],
                "kept_relaxed": [
                    sym for sym, _, _, _ in kept_relaxed[: thresholds.relaxed_top_k]
                ],
                "reasons": reject_reasons,
                "relaxed_ratio": relaxed_ratio,
            }
            try:
                publish_done_dbg = False
                jset = getattr(redis_client, "jset", None)
                if callable(jset):
                    try:
                        coro = jset(redis_debug_key, value=dbg, ttl=debug_ttl_sec)
                        if asyncio.iscoroutine(coro):
                            try:
                                asyncio.get_running_loop().create_task(coro)
                            except RuntimeError:
                                asyncio.run(coro)
                        publish_done_dbg = True
                    except Exception:
                        publish_done_dbg = False

                if not publish_done_dbg:
                    setter = getattr(redis_client, "set", None)
                    if callable(setter):
                        res = setter(redis_debug_key, json.dumps(dbg), ex=debug_ttl_sec)
                        if asyncio.iscoroutine(res):
                            try:
                                asyncio.get_running_loop().create_task(res)
                            except RuntimeError:
                                asyncio.run(res)
                        publish_done_dbg = True

                if publish_done_dbg:
                    logger.debug(
                        "[STRICT_GUARD] [prefilter] published debug key=%s ttl=%ds",
                        redis_debug_key,
                        debug_ttl_sec,
                    )
            except Exception as e:  # pragma: no cover
                logger.warning("[STRICT_GUARD] Redis debug publish failed: %s", e)

    return result
