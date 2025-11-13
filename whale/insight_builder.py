"""Market Insight (Phase 1.5) — телеметрійний builder.

Призначення:
    • Обчислює простий opportunity/risk скор на основі overlay полів Stage2.
    • Класифікує і пропонує наступні дії (людинозрозуміло).
    • Записує JSONL у TELEMETRY_BASE_DIR (best‑effort, без винятків назовні).

Важливо:
    • Телеметрія‑only: не впливає на Stage3/рішення.
    • Усі шляхи/імена файлів — через config.config (STAGE2_INSIGHT, TELEMETRY_BASE_DIR).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config_stage2 import STAGE2_INSIGHT, TELEMETRY_BASE_DIR  # type: ignore
from config.config_whale import STAGE2_WHALE_TELEMETRY  # type: ignore

try:
    from config import config as _live_config  # type: ignore
except Exception:  # pragma: no cover - дефолт на випадок збою імпорту
    _live_config = None

logger = logging.getLogger("monitoring.insight_builder")
if not logger.handlers:  # guard
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


def _insight_cfg() -> Mapping[str, Any]:
    cfg = getattr(_live_config, "STAGE2_INSIGHT", None)
    if isinstance(cfg, Mapping):
        return cfg
    if isinstance(cfg, dict):
        return cfg
    return STAGE2_INSIGHT or {}


def opportunity_risk_score(overlay: Mapping[str, Any] | None) -> float:
    """Компактний скор 0..1 на базі overlay: імпульс / кризова волатильність / пастка.

    Аргументи:
        overlay: Optional[Mapping] — очікується словник з ключами:
            - "impulse_up" (float|int|str) — сила імпульсу вгору [0..1]
            - "impulse_down" (float|int|str) — сила імпульсу вниз [0..1]
            - "crisis_vol_score" (float|int|str) — індикатор кризової волатильності [0..1]
            - "trap_score" (float|int|str) — ймовірність TRAP [0..1]
        Якщо overlay відсутній або має некоректні типи — функція повертає 0.0 (best-effort).

    Повертає:
        float: скор у діапазоні [0.0, 1.0], округлений до 3 знаків після коми.

    Евристика (поточна реалізація):
        base = max(impulse_up, impulse_down)
        penalty = w_crisis * crisis_vol_score + w_trap * trap_score
        score = clamp(base - penalty)

    Logging:
        - DEBUG: розшифровка вхідних значень, обчислених проміжних величин (base, penalty, raw score).
        - INFO: коли скор дуже високий (>= 0.9) або нульовий (== 0.0) — корисно для телеметрії.

    TODOs (чому/навіщо):
        - TODO: Винести ваги (w_crisis, w_trap) та обмеження (clamp) у config/config.py.
                Навіщо: централізоване A/B тестування порогів без змін коду.
        - TODO: Замінити ad-hoc парсинг overlay на Pydantic модель (Schema) для валідації вхідних даних.
                Навіщо: зменшить ризик silent-fail і полегшить тести, особливо для псевдо-стрімів.
        - TODO: Додати unit-тести (happy-path + edge-cases: None, invalid types, extremes).
        - TODO: Розглянути кешування (memoize) для одного ts/symbol, якщо Stage2 викликає часто.
                Навіщо: зменшення CPU при високочастотних викликах.
    """
    if not isinstance(overlay, Mapping):
        logger.debug("opportunity_risk_score: overlay is not a Mapping - returning 0.0")
        return 0.0

    # Ваги/коефіцієнти — можна винести у конфіг
    w_crisis = 0.4
    w_trap = 0.2

    try:
        # Безпечна коерсія в float з дефолтами
        imp_up = float((overlay or {}).get("impulse_up") or 0.0)
        imp_dn = float((overlay or {}).get("impulse_down") or 0.0)
        crisis = float((overlay or {}).get("crisis_vol_score") or 0.0)
        trap = float((overlay or {}).get("trap_score") or 0.0)
    except Exception:
        logger.debug(
            "opportunity_risk_score: failed to parse overlay values, returning 0.0",
            exc_info=True,
        )
        return 0.0

    # Кліппінг вхідних метрик до очікуваного інтервалу [0,1]
    imp_up_c = max(0.0, min(1.0, imp_up))
    imp_dn_c = max(0.0, min(1.0, imp_dn))
    crisis_c = max(0.0, min(1.0, crisis))
    trap_c = max(0.0, min(1.0, trap))

    base = max(imp_up_c, imp_dn_c)
    penalty = w_crisis * crisis_c + w_trap * trap_c
    raw_score = base - penalty
    score = float(max(0.0, min(1.0, round(raw_score, 3))))

    logger.debug(
        "opportunity_risk_score parsed: imp_up=%.3f imp_dn=%.3f crisis=%.3f trap=%.3f",
        imp_up_c,
        imp_dn_c,
        crisis_c,
        trap_c,
    )
    logger.debug(
        "opportunity_risk_score computed: base=%.3f penalty=%.3f raw_score=%.3f final_score=%.3f",
        base,
        penalty,
        raw_score,
        score,
    )

    # INFO для крайніх випадків — корисно в телеметрії
    if score >= 0.9:
        logger.info(
            "opportunity_risk_score high score=%.3f (base=%.3f penalty=%.3f)",
            score,
            base,
            penalty,
        )
    # Якщо whale-метрики позначені як stale — застосовуємо пенальті/стелю (телеметрія‑only)
    try:
        whale_stale_flag = bool((overlay or {}).get("whale_stale", False))
    except Exception:
        whale_stale_flag = False
    if whale_stale_flag:
        ins_cfg = _insight_cfg()
        try:
            stale_penalty = float((ins_cfg or {}).get("stale_hint_penalty", 0.5))
            stale_cap = float((ins_cfg or {}).get("stale_hint_cap", 0.35))
        except Exception:
            stale_penalty = 0.5
            stale_cap = 0.35
        penalized = float(score) * stale_penalty
        score = float(min(stale_cap, max(0.0, round(penalized, 3))))
        logger.info(
            "opportunity_risk_score stale penalty applied: score=%.3f cap=%.2f",
            score,
            stale_cap,
        )

    if score == 0.0:
        # Спробуємо застосувати фолу-бек з китових метрик (телеметрія-only), якщо дозволено конфігом
        ins_cfg = _insight_cfg()
        try:
            fb_cfg = (ins_cfg or {}).get("fallback_from_whales") or {}
            fb_enabled = bool(fb_cfg.get("enabled", False))
            presence_min = float(fb_cfg.get("presence_min", 0.20) or 0.20)
            bias_min = float(fb_cfg.get("bias_min", 0.40) or 0.40)
            weight = float(fb_cfg.get("weight", 0.75) or 0.75)
        except Exception:
            fb_enabled = False
            presence_min = 0.20
            bias_min = 0.40
            weight = 0.75

        if fb_enabled:
            try:
                wp = float((overlay or {}).get("whale_presence") or 0.0)
                wb = float((overlay or {}).get("whale_bias") or 0.0)
                vwap_dev = None
                try:
                    vwap_dev = float((overlay or {}).get("vwap_dev"))
                except Exception:
                    vwap_dev = None
                accum_cnt = (
                    int((overlay or {}).get("accum_zones_count"))
                    if (overlay or {}).get("accum_zones_count") is not None
                    else None
                )
                dist_cnt = (
                    int((overlay or {}).get("dist_zones_count"))
                    if (overlay or {}).get("dist_zones_count") is not None
                    else None
                )
                vol_reg = str((overlay or {}).get("vol_regime") or "")
            except Exception:
                wp = 0.0
                wb = 0.0
                vwap_dev = None
                accum_cnt = None
                dist_cnt = None
                vol_reg = ""

            # Зони як фільтр: вимагаємо або dist>=min, або accum>=min; якщо немає даних — блокуємо скор
            try:
                _zf = ((ins_cfg or {}).get("fallback_from_whales") or {}).get(
                    "zones_min_filter", {}
                )
                accum_min = int(_zf.get("accum_min", 3))
                dist_min = int(_zf.get("dist_min", 3))
                policy_any = str(_zf.get("policy", "any")).lower() == "any"
            except Exception:
                accum_min = 3
                dist_min = 3
                policy_any = True

            zones_ok = False
            if accum_cnt is not None or dist_cnt is not None:
                if policy_any:
                    zones_ok = (accum_cnt or 0) >= accum_min or (
                        dist_cnt or 0
                    ) >= dist_min
                else:
                    zones_ok = (accum_cnt or 0) >= accum_min and (
                        dist_cnt or 0
                    ) >= dist_min
            else:
                # Якщо немає даних про зони — вважаємо, що фільтр НЕ пройдено (консервативно)
                zones_ok = False

            # Стабільність: якщо prev-значення є, вимагаємо дрейф ≤ drift_max; якщо їх немає — дозволяємо
            try:
                drift_max = float(
                    ((ins_cfg or {}).get("fallback_from_whales") or {})
                    .get("stability_window", {})
                    .get("drift_max", 0.10)
                )
            except Exception:
                drift_max = 0.10

            wp_prev = (
                overlay.get("whale_presence_prev")
                if isinstance(overlay, Mapping)
                else None
            )
            wb_prev = (
                overlay.get("whale_bias_prev") if isinstance(overlay, Mapping) else None
            )
            stable_ok = True
            try:
                if wp_prev is not None and wb_prev is not None:
                    d_wp = abs(float(wp) - float(wp_prev))
                    d_wb = abs(abs(float(wb)) - abs(float(wb_prev)))
                    stable_ok = (d_wp <= drift_max) and (d_wb <= drift_max)
            except Exception:
                stable_ok = True

            # Кеп ваги за волатильнісним режимом
            try:
                caps = ((ins_cfg or {}).get("fallback_from_whales") or {}).get(
                    "weight_cap_by_volregime", {}
                )
                if isinstance(caps, Mapping) and vol_reg:
                    cap = float(caps.get(vol_reg, weight) or weight)
                else:
                    cap = weight
            except Exception:
                cap = weight

            if (
                zones_ok
                and stable_ok
                and (wp >= presence_min)
                and (abs(wb) >= bias_min)
            ):
                eff_weight = float(min(weight, cap))
                fb_score_raw = eff_weight

                fb_local_cfg: Mapping[str, Any] = (
                    (overlay or {}).get("whale_fallback")
                    if isinstance(overlay, Mapping)
                    else {}
                )
                if not isinstance(fb_local_cfg, Mapping):
                    fb_local_cfg = {}
                try:
                    abs_min = float(fb_local_cfg.get("abs_min", 0.04) or 0.04)
                except Exception:
                    abs_min = 0.04
                try:
                    bonus = float(fb_local_cfg.get("bonus", 0.05) or 0.05)
                except Exception:
                    bonus = 0.05

                if (vwap_dev is not None) and (abs(vwap_dev) >= abs_min):
                    same_direction = (vwap_dev >= 0 and wb > 0) or (
                        vwap_dev < 0 and wb < 0
                    )
                    if same_direction:
                        fb_score_raw += bonus

                fb_score_raw = min(fb_score_raw, float(cap))
                fb_score = float(max(0.0, min(1.0, round(fb_score_raw, 3))))
                if fb_score > 0.0:
                    logger.info(
                        "opportunity_risk_score fallback used: score=%.3f presence=%.3f bias=%.3f weight=%.2f regime=%s vwap_dev=%s zones_ok=%s stable=%s",
                        fb_score,
                        wp,
                        wb,
                        eff_weight,
                        vol_reg,
                        None if vwap_dev is None else f"{vwap_dev:.3f}",
                        zones_ok,
                        stable_ok,
                    )
                    return fb_score

        # Якщо фолу-бек не застосовано — лог нульового скору для телеметрії
        logger.info(
            "opportunity_risk_score zero score for overlay=%s",
            {
                k: overlay.get(k)
                for k in (
                    "impulse_up",
                    "impulse_down",
                    "crisis_vol_score",
                    "trap_score",
                )
            },
        )

    return score


def classify(score: float) -> str:
    """Просте бінування скору у категорії: 'low' | 'medium' | 'high'.

    Аргументи:
        score: float — очікується в діапазоні 0..1; функція безпечна до вхідних поза межами.

    Повертає:
        str: одна з рядкових констант: "low", "medium", "high".

    Правила (поточні пороги — TODO винести в конфіг):
        - high: score >= 0.6
        - medium: 0.3 <= score < 0.6
        - low: score < 0.3

    TODOs:
        - TODO: Винести пороги в config/config.py як константи (наприклад, INSIGHT_THRESHOLDS).
                Навіщо: дозволить тонке налаштування класифікації без змін коду.
        - TODO: Повернути структурований тип (enum або TypedDict) замість простого рядка.
                Навіщо: зменшить ризик typo-багів у споживачів (UI/Stage3) та полегшить тестування.
        - TODO: Додати unit-тести на граничні значення (0.3, 0.6) та нетипові вхідні значення.
    """
    try:
        s = float(score)
    except Exception:
        logger.debug(
            "classify: invalid score=%r, coercing to 0.0", score, exc_info=True
        )
        s = 0.0

    # Нормалізація на випадок вхідних значень поза [0,1]
    s = max(0.0, min(1.0, s))

    if s >= 0.6:
        logger.debug("classify: score=%.3f -> high", s)
        return "high"
    if s >= 0.3:
        logger.debug("classify: score=%.3f -> medium", s)
        return "medium"

    logger.debug("classify: score=%.3f -> low", s)
    return "low"


def classify_enriched(inputs: Mapping[str, Any]) -> str:
    """Збагачена класифікація інсайту з урахуванням контексту.

    Повертає один з: "TRADEABLE" | "OBSERVE" | "AVOID".

    Логіка (коротко):
        - Беруться до уваги: scenario, confidence, rr_live, atr_ratio, trap_score, whale_bias, overlay.
        - Політика консервативна — функція тільки для телеметрії/аудиту і не впливає на Stage3.
        - Best-effort: ніколи не піднімає виключення назовні; усі помилки логуються.

    Args:
        inputs: Mapping з можливими ключами:
            - "scenario" (str|None): сценарій Stage2 (напр., "BULLISH_BREAKOUT").
            - "confidence" (float|None): composite confidence [0..1].
            - "rr_live" (float|None): поточне risk/reward (може бути None).
            - "atr_ratio" (float|None): співвідношення ATR до очікуваного (може бути None).
            - "trap_score" (float|None): ймовірність TRAP [0..1].
            - "whale_bias" (float|None): [-1..1], позитивне → buy bias.
            - "overlay" (Mapping|None): додаткові метрики (impulse_up/down, crisis_vol_score тощо).

    Returns:
        str: одна з констант "TRADEABLE" / "OBSERVE" / "AVOID".

    Logging:
        - DEBUG: деталі парсингу та причин рішення.
        - INFO: коли категорія є "TRADEABLE" або "AVOID" (важлива телеметрія).

    TODOs (чому/навіщо):
        - TODO: Виносити пороги (rr_min, atr_min, impulse_thr, trap_thr, confidence_thr) у config/config.py.
                Навіщо: централізована політика порогів, A/B тестування без деплою.
        - TODO: Замінити ad-hoc парсинг inputs на Pydantic модель для валідації/документації.
                Навіщо: зменшить кількість boilerplate парсингу і підвищить надійність / тестованість.
        - TODO: Змінити повернення на структурований тип (dict з code + reason + meta) замість рядка.
                Навіщо: спростить споживання телеметрії у UI/аналітиці і дозволить фільтрувати за severity.
        - TODO: Додати unit‑тести + псевдо‑стрім тести на edge-cases (None, invalid types, extreme values).
        - TODO: Розглянути кешування результатів для одного ts/symbol при високій частоті викликів.
                Навіщо: зменшення CPU при повторних викликах Stage2.
    """
    try:
        logger.debug(
            "classify_enriched called with inputs keys=%s", list(inputs.keys())
        )

        # Безпечні допоміжні конвертери
        def _safe_str(k: str) -> str:
            try:
                return str(inputs.get(k) or "").upper()
            except Exception:
                logger.debug(
                    "classify_enriched: safe_str failed for key=%s", k, exc_info=True
                )
                return ""

        def _safe_float_val(v: Any, default: float = 0.0) -> float:
            try:
                return float(v)
            except Exception:
                return default

        # Парсинг вхідних полів (best-effort)
        scenario = _safe_str("scenario")
        conf = _safe_float_val(inputs.get("confidence"), 0.0)
        rr_live_raw = inputs.get("rr_live")
        rr_live = (
            _safe_float_val(rr_live_raw, default=None)
            if rr_live_raw is not None
            else None
        )
        # Якщо rr_live_raw є нечислом, rr_live залишиться None (відсутність даних)
        if isinstance(rr_live_raw, (int, float)):
            rr_live = float(rr_live_raw)
        atr_ratio = _safe_float_val(inputs.get("atr_ratio"), 0.0)
        trap_score = _safe_float_val(inputs.get("trap_score"), 0.0)
        whale_bias = _safe_float_val(inputs.get("whale_bias"), 0.0)

        # Overlay — optional, мінімальний парсинг імпульсу і кризової волатильності
        overlay = inputs.get("overlay") or {}
        imp_up = _safe_float_val(
            overlay.get("impulse_up") if isinstance(overlay, Mapping) else None, 0.0
        )
        imp_dn = _safe_float_val(
            overlay.get("impulse_down") if isinstance(overlay, Mapping) else None, 0.0
        )
        crisis = _safe_float_val(
            overlay.get("crisis_vol_score") if isinstance(overlay, Mapping) else None,
            0.0,
        )
        dom_up = _safe_float_val(
            overlay.get("dominance_up") if isinstance(overlay, Mapping) else None,
            0.0,
        )
        dom_dn = _safe_float_val(
            overlay.get("dominance_down") if isinstance(overlay, Mapping) else None,
            0.0,
        )
        dom_ready = _safe_float_val(
            (
                overlay.get("dominance_alt_ready")
                if isinstance(overlay, Mapping)
                else None
            ),
            0.0,
        )

        logger.debug(
            "classify_enriched parsed: scenario=%s conf=%.3f rr_live=%r atr_ratio=%.3f trap=%.3f whale_bias=%.3f imp_up=%.3f imp_dn=%.3f crisis=%.3f dom_up=%.1f dom_dn=%.1f dom_ready=%.1f",
            scenario,
            conf,
            rr_live,
            atr_ratio,
            trap_score,
            whale_bias,
            imp_up,
            imp_dn,
            crisis,
            dom_up,
            dom_dn,
            dom_ready,
        )

        # Пороги: намагаємось завантажити з конфігу, fallback на жорсткі значення
        try:
            wb_warn = float(
                ((STAGE2_WHALE_TELEMETRY or {}).get("bias_warn_thr", 0.4)) or 0.4
            )
        except Exception:
            wb_warn = 0.4
            logger.debug(
                "classify_enriched: couldn't load whale thresholds, using default wb_warn=%.2f",
                wb_warn,
            )

        # Пороги ризику/якісної перевірки (можна винести в config)
        rr_min = 1.6  # TODO -> config: мінімальне RR для tradeable
        atr_min = 0.8  # TODO -> config: мінімальний atr_ratio
        impulse_strong = 0.7  # TODO -> config: поріг вважати імпульс сильним
        conf_tradeable = 0.6  # TODO -> config: мінімальна довіра для tradeable
        conf_observe = 0.4  # TODO -> config

        # -----------------------
        # Gate 1: явний ризик або низька якість → AVOID
        # -----------------------
        if (
            (rr_live is not None and rr_live < rr_min)
            or (atr_ratio and atr_ratio < atr_min)
            or trap_score >= 0.67
        ):
            logger.info(
                "classify_enriched decision=AVOID reason=%s",
                (
                    "low_rr"
                    if (rr_live is not None and rr_live < rr_min)
                    else (
                        "low_atr"
                        if (atr_ratio and atr_ratio < atr_min)
                        else "trap_score"
                    )
                ),
            )
            return "AVOID"

        # -----------------------
        # Gate 2: бичий сценарій з кризою або sell‑bias → OBSERVE
        # -----------------------
        if scenario in {"BULLISH_BREAKOUT", "BULLISH_CONTROL", "BUY_IN_DIPS"}:
            if crisis >= 0.6:
                logger.debug(
                    "classify_enriched decision=OBSERVE reason=crisis_high crisis=%.3f",
                    crisis,
                )
                return "OBSERVE"
            if whale_bias <= -wb_warn:
                logger.debug(
                    "classify_enriched decision=OBSERVE reason=whale_sell_warn whale_bias=%.3f wb_warn=%.3f",
                    whale_bias,
                    wb_warn,
                )
                return "OBSERVE"

        # -----------------------
        # Gate 3: сильний імпульс + достатня впевненість → TRADEABLE
        # -----------------------
        if max(imp_up, imp_dn) >= impulse_strong and conf >= conf_tradeable:
            logger.info(
                "classify_enriched decision=TRADEABLE reason=strong_impulse conf=%.3f imp=%.3f",
                conf,
                max(imp_up, imp_dn),
            )
            return "TRADEABLE"

        dominance_score = max(dom_up, dom_dn)
        if dominance_score >= 0.99 and conf >= max(conf_tradeable - 0.05, 0.5):
            logger.info(
                "classify_enriched decision=TRADEABLE reason=whale_dominance conf=%.3f dom=%.1f alt_ready=%.1f",
                conf,
                dominance_score,
                dom_ready,
            )
            return "TRADEABLE"

        # HIGH_VOLATILITY сценарій → за замовчанням OBSERVE
        if scenario == "HIGH_VOLATILITY":
            logger.debug("classify_enriched decision=OBSERVE reason=high_volatility")
            return "OBSERVE"

        # Дефолтне правило: якщо достатня довіра → OBSERVE, інакше AVOID
        if conf >= conf_observe:
            logger.debug(
                "classify_enriched decision=OBSERVE reason=confidence_ok conf=%.3f",
                conf,
            )
            return "OBSERVE"

        logger.debug(
            "classify_enriched decision=AVOID reason=low_confidence conf=%.3f", conf
        )
        return "AVOID"

    except Exception:
        # Best‑effort: ніколи не піднімаємо виняток у телеметрії
        logger.debug("classify_enriched failed unexpectedly", exc_info=True)
        return "OBSERVE"


def next_actions(score: float, overlay: Mapping[str, Any] | None) -> list[str]:
    """Генерує прості, людино‑читабельні поради за коротким скором (телеметрія‑only).

    Args:
        score: Значення opportunity_risk_score в діапазоні 0..1.
        overlay: Допоміжні метрики (наприклад: impulse_up, impulse_down, crisis_vol_score, trap_score).
                 Якщо None або невірного типу — трактуються як відсутні.

    Returns:
        list[str]: Список коротких порад (може бути порожнім). Функція ніколи не піднімає виняток —
                   усі помилки логуються і повертається пустий список.

    Logging policy:
        - DEBUG: вхідні значення та деталі парсингу overlay, кожне додавання поради.
        - INFO: коли згенеровано хоча б одну поради (корисна телеметрія).
        - Exceptions: ловляться та логуються на DEBUG (best‑effort).

    TODOs:
        - TODO: Винести тексти повідомлень у config/i18n для можливості локалізації та A/B тестування.
                Чому: змінювати політику підказок без ребілда/деплой.
        - TODO: Замінити ad‑hoc парсинг overlay на Pydantic модель для валідації/документації.
                Чому: підвищить надійність і спростить тести (особливо для псевдо‑стрімів).
        - TODO: Можливість кешування результатів (memoize) для одного ts/symbol при частих викликах.
                Чому: зменшить CPU при високочастотних оновленнях Stage2.
        - TODO: Розглянути асинхронну чергу запису/генерації порад при високому навантаженні.
                Чому: не блокувати критичний шлях Stage2.
        - TODO: Винести пороги (наприклад, IMPULSE_THRESHOLD) у config/config.py.
                Чому: централізувати політику і дозволити швидке налаштування.
    """
    try:
        logger.debug("next_actions called with score=%.3f overlay=%r", score, overlay)

        advice: list[str] = []
        # Класифікація за скором (low/medium/high)
        cls = classify(score)
        logger.debug("next_actions: classified score=%.3f as %s", score, cls)

        # Базова порада залежно від класу скору
        if cls == "high":
            msg = "Стежити за підтвердженням; готувати сценарій входу."
            advice.append(msg)
            # INFO: важлива телеметрія — high priority insight
            logger.info(
                "next_actions: high priority advice added: %s score=%.3f", msg, score
            )
        elif cls == "medium":
            msg = "Моніторити; очікувати додаткових підтверджень."
            advice.append(msg)
            logger.debug("next_actions: medium priority advice added: %s", msg)
        else:
            msg = "Низький пріоритет; без активних дій."
            advice.append(msg)
            logger.debug("next_actions: low priority advice added: %s", msg)

        # Пороги (TODO: винести в config)
        impulse_threshold = 0.5  # TODO -> config: поріг виявлення значного імпульсу

        # Безпечне парсування overlay‑метрик
        try:
            imp_up = (
                float((overlay or {}).get("impulse_up") or 0.0)
                if isinstance(overlay, Mapping)
                else 0.0
            )
            imp_dn = (
                float((overlay or {}).get("impulse_down") or 0.0)
                if isinstance(overlay, Mapping)
                else 0.0
            )
            crisis = (
                float((overlay or {}).get("crisis_vol_score") or 0.0)
                if isinstance(overlay, Mapping)
                else 0.0
            )
            trap = (
                float((overlay or {}).get("trap_score") or 0.0)
                if isinstance(overlay, Mapping)
                else 0.0
            )
            logger.debug(
                "next_actions parsed overlay: impulse_up=%.3f impulse_down=%.3f crisis=%.3f trap=%.3f",
                imp_up,
                imp_dn,
                crisis,
                trap,
            )
        except Exception:
            # Егер парсинг провалився — використовуємо безпечні дефолти та логируемо помилку на DEBUG
            imp_up = imp_dn = crisis = trap = 0.0
            logger.debug(
                "next_actions: overlay parse failed, using defaults (0.0)",
                exc_info=True,
            )

        # Додаткові поради за напрямком імпульсу
        try:
            if imp_up > imp_dn and imp_up >= impulse_threshold:
                msg = "Імпульс вгору: придивитись до pullback‑входів."
                advice.append(msg)
                logger.debug(
                    "next_actions: impulse up advice added: %s imp_up=%.3f imp_dn=%.3f",
                    msg,
                    imp_up,
                    imp_dn,
                )
            if imp_dn > imp_up and imp_dn >= impulse_threshold:
                msg = "Імпульс вниз: придивитись до корекцій для short."
                advice.append(msg)
                logger.debug(
                    "next_actions: impulse down advice added: %s imp_up=%.3f imp_dn=%.3f",
                    msg,
                    imp_up,
                    imp_dn,
                )
        except Exception:
            logger.debug(
                "next_actions: failed to evaluate impulse direction", exc_info=True
            )

        # Додаткові застереження при кризовій волатильності або TRAP
        try:
            if crisis >= 0.6:
                msg = "Кризова волатильність: знизити агресію."
                advice.append(msg)
                logger.info(
                    "next_actions: crisis warning added: %s crisis=%.3f", msg, crisis
                )
            if trap >= 0.67:
                msg = "Висока ймовірність TRAP: уникати ризикованих входів."
                advice.append(msg)
                logger.debug(
                    "next_actions: trap warning added: %s trap=%.3f", msg, trap
                )
        except Exception:
            logger.debug(
                "next_actions: failed to evaluate crisis/trap indicators", exc_info=True
            )

        # Підсумкове логування — INFO коли є корисні поради
        if advice:
            logger.info(
                "next_actions produced %d advice item(s) for score=%.3f class=%s",
                len(advice),
                score,
                cls,
            )
            logger.debug("next_actions items: %s", advice)
        else:
            logger.debug("next_actions produced no advice for score=%.3f", score)

        return advice

    except Exception:
        # Best‑effort: телеметрія не має ламати пайплайн — лог на DEBUG з exc_info
        logger.debug("next_actions failed unexpectedly", exc_info=True)
        return []


def next_actions_enriched(
    *,
    scenario: str | None,
    strategy: str | None,
    whale_bias: float | None,
    overlay: Mapping[str, Any] | None,
    rr_live: float | None,
    atr_ratio: float | None,
) -> list[str]:
    """Нюансовані дії з урахуванням стратегії/whale_bias (телеметрія‑only).

    Пояснення / призначення:
        Формує список коротких порад (людинозрозумілих рядків) для аналітиків
        або UI‑телеметрії, базуючись на контексті сценaрію, стратегії,
        поведінці китів (whale_bias) та overlay‑метриках. Функція — тільки
        для телеметрії/аудиту і ніколи не впливає на Stage3.

    Args:
        scenario: Сценарій Stage2 (напр., "BULLISH_BREAKOUT"). None → "".
        strategy: Підтип стратегії ("retest" або "breakout"). None → "".
        whale_bias: Ступінь buy/sell‑упередженості китів у діапазоні [-1; 1].
                    Якщо парсинг неможливий → використовується 0.0.
        overlay: Допоміжні метрики (impulse_up/down, crisis_vol_score тощо).
                 Якщо не Mapping — трактуються як відсутні.
        rr_live: Поточне співвідношення ризик/прибуток (якщо є).
                 None означає відсутність даних.
        atr_ratio: Відношення ATR до очікуваного (якщо є). None → відсутність.

    Returns:
        Список рядків з порадами (може бути порожнім). Функція ніколи не
        піднімає виняток — помилки логуються та повертається пустий список.

    Logging policy:
        - DEBUG — деталі парсингу вхідних даних та кожне додавання поради.
        - INFO  — коли згенеровано хоча б одну важливу пораду (ключова телеметрія).
        - Exceptions — ловляться і логуються на DEBUG (best‑effort).

    TODOs (чому/навіщо):
        - TODO: Виносити текста повідомлень у config/i18n, щоб змінювати
                політику без ребілду (потрібно для A/B тестування та локалізації).
        - TODO: Замінити ad‑hoc парсинг на Pydantic модель для валідації input:
                підвищить надійність та читабельність коду.
        - TODO: Додати unit‑тести та псевдо‑стрім тести (edge cases: None, invalid types).
        - TODO: Розглянути кешування (memoize) результатів для одного ts/symbol,
                коли Stage2 генерує багато повторних викликів — зменшить CPU.
        - TODO: Розглянути асинхронний варіант або чергу запису порад у разі
                високих навантажень, щоб не блокувати основний потік Stage2.
    """
    try:
        out: list[str] = []
        logger.debug(
            "next_actions_enriched called with scenario=%r strategy=%r whale_bias=%r rr_live=%r atr_ratio=%r",
            scenario,
            strategy,
            whale_bias,
            rr_live,
            atr_ratio,
        )

        # Нормалізація вхідних значень
        scn = (scenario or "").upper()
        strat = (strategy or "").lower()
        try:
            wb = float(whale_bias or 0.0)
        except Exception:
            wb = 0.0
            logger.debug(
                "next_actions_enriched: whale_bias parse failed, defaulted to 0.0",
                exc_info=True,
            )

        # Безпечне читання overlay‑метрик (min work)
        try:
            imp_up = (
                float((overlay or {}).get("impulse_up") or 0.0)
                if isinstance(overlay, Mapping)
                else 0.0
            )
            imp_dn = (
                float((overlay or {}).get("impulse_down") or 0.0)
                if isinstance(overlay, Mapping)
                else 0.0
            )
            crisis = (
                float((overlay or {}).get("crisis_vol_score") or 0.0)
                if isinstance(overlay, Mapping)
                else 0.0
            )
        except Exception:
            imp_up = 0.0
            imp_dn = 0.0
            crisis = 0.0
            logger.debug(
                "next_actions_enriched: overlay parse failed, using defaults (0.0)",
                exc_info=True,
            )

        logger.debug(
            "parsed overlay: imp_up=%.3f imp_dn=%.3f crisis=%.3f whale_bias=%.3f",
            imp_up,
            imp_dn,
            crisis,
            wb,
        )

        # -----------------------
        # Загальні ризикові фільтри
        # -----------------------
        if rr_live is not None and rr_live < 1.8:
            msg = "RR низький: зменшити розмір або утриматись."
            out.append(msg)
            logger.debug("advice added (rr_live): %s rr_live=%r", msg, rr_live)

        if atr_ratio is not None and atr_ratio < 0.8:
            msg = "ATR‑ratio низький: почекати кращої структури."
            out.append(msg)
            logger.debug("advice added (atr_ratio): %s atr_ratio=%r", msg, atr_ratio)

        if crisis >= 0.6:
            msg = "Кризова волатильність: знизити агресію, уникати середини діапазону."
            out.append(msg)
            logger.info(
                "advice (crisis) added: %s crisis=%.3f scenario=%s", msg, crisis, scn
            )

        # -----------------------
        # Специфіка ретестів
        # -----------------------
        if strat == "retest" or scn in {
            "POST_BREAKOUT",
            "BUY_IN_DIPS",
            "SELL_ON_RALLIES",
        }:
            logger.debug(
                "handling retest logic for scenario=%s strategy=%s", scn, strat
            )
            if wb <= -0.4:
                msg1 = "Short‑biased ретести: шукати слабкі відскоки до опору."
                msg2 = (
                    "План 'retest_short': дочекатись торкання рівня X знизу, DVR≥0.5 і vol≤1.0; "
                    "частковий вхід; інвалідація — закриття вище X+ε."
                )
                out.extend([msg1, msg2])
                logger.debug("retest advice (short bias) added: %s | %s", msg1, msg2)
            elif wb >= 0.4:
                msg = "Long‑biased ретести: шукати покупку на підтверджених підтримках."
                out.append(msg)
                logger.debug("retest advice (long bias) added: %s", msg)
            else:
                msg = "Ретест: чекати підтвердження на рівнях."
                out.append(msg)
                logger.debug("retest advice (neutral bias) added: %s", msg)

        # -----------------------
        # Специфіка breakout
        # -----------------------
        elif strat == "breakout" or scn in {"BULLISH_BREAKOUT", "BEARISH_BREAKOUT"}:
            logger.debug(
                "handling breakout logic for scenario=%s strategy=%s", scn, strat
            )
            if max(imp_up, imp_dn) >= 0.7:
                msg = "Breakout: працювати від імпульсу; підтвердження обсягом."
                out.append(msg)
                logger.debug("breakout advice (strong impulse) added: %s", msg)
            else:
                msg = "Breakout: чекати імпульс/обсяг для підтвердження."
                out.append(msg)
                logger.debug("breakout advice (wait for impulse) added: %s", msg)

        # -----------------------
        # Whale bias — окремі застереження
        # -----------------------
        if wb <= -0.6:
            msg = "Whale sell‑bias: уникати контр‑трендових long‑входів."
            out.append(msg)
            logger.debug("whale bias (strong sell) advice added: %s wb=%.3f", msg, wb)
        elif wb >= 0.6:
            msg = "Whale buy‑bias: уникати контр‑трендових short‑входів."
            out.append(msg)
            logger.debug("whale bias (strong buy) advice added: %s wb=%.3f", msg, wb)

        # -----------------------
        # Базові підказки (fallback)
        # -----------------------
        if not out:
            if imp_up > imp_dn and imp_up >= 0.5:
                msg = "Імпульс вгору: фокус на pullback‑входи."
                out.append(msg)
                logger.debug("default advice (imp_up) added: %s", msg)
            elif imp_dn > imp_up and imp_dn >= 0.5:
                msg = "Імпульс вниз: фокус на корекційні short."
                out.append(msg)
                logger.debug("default advice (imp_dn) added: %s", msg)
            else:
                # Якщо взагалі немає явних сигналів — корисно додати нейтральну пораду
                msg = "Немає явного сигналу: моніторити і чекати підтверджень."
                out.append(msg)
                logger.debug("default neutral advice added: %s", msg)

        # Підсумкове логування: INFO якщо є поради, DEBUG — деталі
        if out:
            logger.info(
                "next_actions_enriched produced %d advice item(s) for scenario=%s strategy=%s",
                len(out),
                scn,
                strat,
            )
            logger.debug("advice items: %s", out)
        else:
            logger.debug(
                "next_actions_enriched produced no advice for scenario=%s strategy=%s",
                scn,
                strat,
            )

        return out

    except Exception:
        # Best‑effort: телеметрія не має ламати пайплайн — лог на DEBUG з exc_info
        logger.debug("next_actions_enriched failed unexpectedly", exc_info=True)
        return []


def build_invalidation(
    *, scenario: str | None, overlay: Mapping[str, Any] | None, whale_bias: float | None
) -> list[str]:
    """Побудова простих правил інвалідації (телеметрія‑only).

    Пояснення:
        Функція формує набір текстових правил‑інвалідацій, які допомагають
        пояснити, за яких умов поточний сценарій/рекомендація має бути
        переоцінена або відхилена. Це лише для телеметрії/аудиту — не
        впливає на прийняття рішень у Stage3.

    Args:
        scenario: рядок сценарію (напр., "BULLISH_BREAKOUT"). Обробляється
            безпечним upper() — None → "".
        overlay: допоміжні метрики (можуть бути None). Очікувані поля:
            - crisis_vol_score: float-призначеність для кризової волатильності.
            - trap_score: float‑ймовірність "trap" сигналу.
        whale_bias: float у діапазоні [-1; 1] або None — оцінка buy/sell упередженості
            великих учасників (китів). Якщо парсинг не вдається — використовується 0.0.

    Returns:
        Список рядків (може бути порожнім). Функція ніколи не піднімає виняток.

    Logging:
        - DEBUG — деталі парсингу/порогів, кожне додавання правила.
        - INFO — коли отримано хоча б одне правило (ключова телеметрія).
        - EXCEPTION — непередбачені помилки (повертається []).

    TODOs / місця для покращення (чому й навіщо):
        - TODO: Винести текстові повідомлення та пороги у config/config.py або i18n,
                щоб змінювати політику без ребілду. Навіщо: централізація політики
                та можливість A/B тестування порогів.
        - TODO: Формувати структуровані інвалідації (dict з полями: code, severity, msg),
                замість плоских рядків. Навіщо: полегшить індексацію, фільтрацію
                та подальше автоматичне застосування (Stage3 або дашборди).
        - TODO: Додати unit‑тести та псевдо‑стрім тести (coverage для edge cases).
        - TODO: Розглянути кешування результатів build_invalidation для одного
                ts/symbol при високій частоті Stage2 (з TTL). Навіщо: зменшення CPU/IO.
        - TODO: Міграція до Pydantic для валідації overlay/inputs — підвищить надійність.
    """
    try:
        rules: list[str] = []

        # Нормалізація вхідних даних — без винятків назовні
        scn = (scenario or "").upper()
        # Безпечний парсинг overlay полів (виконуємо minimal work — float coercion)
        crisis = (
            float((overlay or {}).get("crisis_vol_score") or 0.0)
            if isinstance(overlay, Mapping)
            else 0.0
        )
        trap = (
            float((overlay or {}).get("trap_score") or 0.0)
            if isinstance(overlay, Mapping)
            else 0.0
        )
        try:
            wb = float(whale_bias or 0.0)
        except Exception:
            wb = 0.0
            logger.debug(
                "build_invalidation: whale_bias parse failed, defaulting to 0.0",
                exc_info=True,
            )

        logger.debug(
            "build_invalidation called: scenario=%s crisis=%.3f trap=%.3f whale_bias=%.3f",
            scn,
            crisis,
            trap,
            wb,
        )

        # Отримуємо пороги whale‑bias з конфігу, з дефолтами при помилці
        try:
            wb_warn = float(
                ((STAGE2_WHALE_TELEMETRY or {}).get("bias_warn_thr", 0.4)) or 0.4
            )
            wb_conf = float(
                ((STAGE2_WHALE_TELEMETRY or {}).get("bias_confirm_thr", 0.6)) or 0.6
            )
        except Exception:  # pragma: no cover
            wb_warn, wb_conf = 0.4, 0.6
            logger.debug(
                "build_invalidation: could not load STAGE2_WHALE_TELEMETRY, using defaults: warn=%.2f confirm=%.2f",
                wb_warn,
                wb_conf,
                exc_info=True,
            )

        logger.debug("whale thresholds: warn=%.2f confirm=%.2f", wb_warn, wb_conf)

        # -----------------------
        # Правила для бичих сценаріїв
        # -----------------------
        if scn in {"BULLISH_BREAKOUT", "BULLISH_CONTROL"}:
            # Якщо кит‑сигнал підтверджений на продаж → уникати long
            if wb <= -wb_conf:
                msg = "Whale sell‑bias (confirm): уникати контр‑трендових long‑входів."
                rules.append(msg)
                logger.debug("added invalidation rule (bull, sell confirm): %s", msg)
            elif wb <= -wb_warn:
                msg = "Whale sell‑bias (warn): обережність із long."
                rules.append(msg)
                logger.debug("added invalidation rule (bull, sell warn): %s", msg)

            # Якщо кит‑buy підтверджений → уникати short
            if wb >= wb_conf:
                msg = "Whale buy‑bias (confirm): уникати контр‑трендових short‑входів."
                rules.append(msg)
                logger.debug("added invalidation rule (bull, buy confirm): %s", msg)
            elif wb >= wb_warn:
                msg = "Whale buy‑bias (warn): обережність із short."
                rules.append(msg)
                logger.debug("added invalidation rule (bull, buy warn): %s", msg)

            # TRAP: сигнал проти довгої позиції
            if trap >= 0.67:
                msg = "TRAP сигнал проти long‑сценарію."
                rules.append(msg)
                logger.debug("added invalidation rule (bull, trap): %s", msg)

        # -----------------------
        # Правила для ведмежих/різних режимів
        # -----------------------
        if scn in {
            "BEARISH_BREAKOUT",
            "BEARISH_CONTROL",
            "HIGH_VOLATILITY",
            "RANGE_BOUND",
        }:
            # Buy bias у китів може інвалідовувати short‑план
            if wb >= wb_warn:
                msg = "Стійкий buy‑bias китів."
                rules.append(msg)
                logger.debug("added invalidation rule (bear-ish, buy bias): %s", msg)

            # Висока кризова волатильність → не стабільно
            if crisis >= 0.6:
                msg = "Кризова волатильність не спадає."
                rules.append(msg)
                logger.debug("added invalidation rule (bear-ish, crisis): %s", msg)

            # TRAP проти short
            if trap >= 0.67:
                msg = "TRAP сигнал проти short‑сценарію."
                rules.append(msg)
                logger.debug("added invalidation rule (bear-ish, trap): %s", msg)

            # Стратегічна інвалідація для ретест‑шортів (детальний текст для телеметрії)
            msg = "Інвалідація ретест‑шорт: закриття вище найближчого опору або верхньої межі діапазону."
            rules.append(msg)
            logger.debug("added invalidation rule (bear-ish, generic retest): %s", msg)

        # Підсумкове логування: INFO якщо є правила (ключова телеметрія)
        if rules:
            logger.info(
                "build_invalidation produced %d rule(s) for scenario=%s",
                len(rules),
                scn,
            )
            # DEBUG: детальний список правил для аналітики
            logger.debug("invalidation rules: %s", rules)
        else:
            logger.debug(
                "build_invalidation: no invalidation rules for scenario=%s", scn
            )

        return rules

    except Exception:
        # Best‑effort: не ламати пайплайн — повертаємо порожній список і логуюємо помилку
        logger.exception("build_invalidation failed unexpectedly")
        return []


def explain(symbol: str, score: float, overlay: Mapping[str, Any] | None) -> str:
    """Коротка людино-зрозуміла довідка для JSONL.

    Args:
        symbol: Тікер інструменту (наприклад, "BTCUSDT").
        score: Значення opportunity_risk_score у діапазоні 0..1.
        overlay: Допоміжний словник з метриками (impulse_up/down, crisis_vol_score, trap_score).

    Returns:
        Строка з консолідованим коротким описом (без винятків).
    """

    # Безпечне перетворення значень overlay на float для коректного форматування
    def _safe(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    try:
        iu = _safe((overlay or {}).get("impulse_up", 0))
        idn = _safe((overlay or {}).get("impulse_down", 0))
        crisis = _safe((overlay or {}).get("crisis_vol_score", 0))
        trap = _safe((overlay or {}).get("trap_score", 0))
        parts = [
            f"{symbol}: insight_score={score:.2f}",
            f"impulse_up={iu:.2f}",
            f"impulse_down={idn:.2f}",
            f"crisis_vol={crisis:.2f}",
            f"trap={trap:.2f}",
        ]
        # Опційна примітка: якщо скор сформовано фолу-беком за китами
        try:
            note_fb = bool(
                (_insight_cfg() or {}).get("note_fallback_in_explain", False)
            )
        except Exception:
            note_fb = False
        if note_fb:
            try:
                wp = _safe((overlay or {}).get("whale_presence", 0))
                wb = _safe((overlay or {}).get("whale_bias", 0))
                # Якщо імпульс/криза/пастка ≈ 0, але whale-проксі присутні — вважаємо fallback
                if (
                    iu <= 0.01 and idn <= 0.01 and crisis <= 0.01 and trap <= 0.01
                ) and (wp > 0.0 or wb != 0.0):
                    parts.append("note=fallback_whales")
            except Exception:
                pass
        # Опційна позначка: якщо whale-метрики застарілі і застосовано пенальті
        try:
            if bool((overlay or {}).get("whale_stale", False)):
                parts.append("note=whale_stale_penalty")
        except Exception:
            pass
        out = " | ".join(parts)
        logger.debug("explain produced: %s", out)
        return out
    except Exception:
        # Best-effort: ніколи не піднімаємо виняток з телеметрії
        logger.debug(
            "explain failed for symbol=%s score=%.3f overlay=%r",
            symbol,
            score,
            overlay,
            exc_info=True,
        )
        return f"{symbol}: insight_score={score:.2f}"


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    """Апендить рядок у JSONL файл (best-effort).

    Args:
        path: Повний шлях до файлу JSONL.
        row: Словник, який буде серіалізований у JSON.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # TODO: (performance) Розглянути батчинг записів або асинхронну чергу
        # навіщо: зменшити I/O на високих частотах оновлень (Stage2 може генерувати багато записів).
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        logger.info(
            "Appended telemetry row to %s (symbol=%s ts=%s)",
            path,
            row.get("symbol"),
            row.get("ts"),
        )
        logger.debug("Appended row keys: %s", list(row.keys()))
    except Exception:
        # Не ламаємо пайплайн — лог на DEBUG з трастуванням exc_info
        logger.debug("Insight JSONL append failed: %s", path, exc_info=True)


def emit_asset_card(
    symbol: str,
    overlay: Mapping[str, Any] | None,
    extras: Mapping[str, Any] | None = None,
) -> None:
    """Додає рядок у insight_asset_cards.jsonl (best-effort).

    Формат:
        Базовий: { ts, symbol, score, class, overlay, actions, explain }
        Розширений (якщо extras): додаються scenario/strategy/rr_live/atr_ratio/whale_bias/insight_class/actions_ext/invalidation/regime/edges.

    Args:
        symbol: Тікер інструменту (наприклад, "BTCUSDT").
        overlay: Словник overlay-метрик (impulse_up/down, crisis_vol_score, trap_score).
        extras: Додаткові поля, що дозволяють збагачувати запис (scenario, confidence, rr_live, atr_ratio, whale_bias тощо).

    Notes / TODOs:
        - TODO: Валідація schema (pydantic) щоб гарантувати контракт Stage2Output у телеметрії;
               навіщо: підвищує довіру до аналітики та запобігає непередбаченим поламкам у downstream.
        - TODO: Розглянути обмеження розміру 'overlay' або анонімізацію чутливих полів;
               навіщо: уникнути зберігання великих/чутливих об'єктів у логах.
        - TODO: Подумати про асинхронну версію (emit_asset_card_async) або пропуск при високому навантаженні;
               навіщо: мінімізувати latency у критичних шляхах Stage2.
    """
    if not bool((STAGE2_INSIGHT or {}).get("enabled", False)):
        logger.debug(
            "emit_asset_card skipped: STAGE2_INSIGHT disabled for symbol=%s", symbol
        )
        return
    try:
        base = Path(TELEMETRY_BASE_DIR)
        file = str(
            (STAGE2_INSIGHT or {}).get("asset_cards_file", "insight_asset_cards.jsonl")
        )
        out = base / file

        score = opportunity_risk_score(overlay)
        cls = classify(score)
        row: dict[str, Any] = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "symbol": symbol,
            "score": score,
            "class": cls,
            "overlay": dict(overlay or {}),
            "actions": next_actions(score, overlay),
            "explain": explain(symbol, score, overlay),
        }

        logger.debug(
            "emit_asset_card preparing row for %s: score=%.3f class=%s",
            symbol,
            score,
            cls,
        )

        if isinstance(extras, Mapping):
            # Збагачена класифікація/дії/інвалідації
            enriched_inputs = {
                "scenario": extras.get("scenario"),
                "confidence": extras.get("confidence"),
                "rr_live": extras.get("rr_live"),
                "atr_ratio": extras.get("atr_ratio"),
                "trap_score": (
                    (overlay or {}).get("trap_score")
                    if isinstance(overlay, Mapping)
                    else None
                ),
                "whale_bias": extras.get("whale_bias"),
                "overlay": dict(overlay or {}),
            }

            # TODO: (compat) Переконатися, що ключі та типи extras сумісні з Stage2 contract;
            # навіщо: щоб Stage3/UI не очікували інших полів і не ламалися при споживанні телеметрії.
            insight_class = classify_enriched(enriched_inputs)
            row["insight_class"] = insight_class
            row["scenario"] = extras.get("scenario")
            row["strategy"] = extras.get("strategy")
            row["rr_live"] = extras.get("rr_live")
            row["atr_ratio"] = extras.get("atr_ratio")
            row["whale_bias"] = extras.get("whale_bias")
            row["regime"] = extras.get("regime")
            row["edges"] = extras.get("edges")
            # Розширені дії з логуванням для аналітики
            row["actions_ext"] = next_actions_enriched(
                scenario=extras.get("scenario"),
                strategy=extras.get("strategy"),
                whale_bias=extras.get("whale_bias"),
                overlay=overlay,
                rr_live=extras.get("rr_live"),
                atr_ratio=extras.get("atr_ratio"),
            )
            row["invalidation"] = build_invalidation(
                scenario=extras.get("scenario"),
                overlay=overlay,
                whale_bias=extras.get("whale_bias"),
            )

            logger.debug(
                "emit_asset_card enriched fields set: insight_class=%s scenario=%s strategy=%s",
                insight_class,
                extras.get("scenario"),
                extras.get("strategy"),
            )

        # Write out (best-effort)
        _append_jsonl(out, row)
    except Exception:
        # Best‑effort: телеметрія не має ламати пайплайн — докладний DEBUG лог
        logger.debug("emit_asset_card failed for %s", symbol, exc_info=True)


def update_market_now(summary: Mapping[str, Any]) -> None:
    """Додає агрегаційний запис у insight_market_now.jsonl (best‑effort)."""
    if not bool((STAGE2_INSIGHT or {}).get("enabled", False)):
        return
    try:
        base = Path(TELEMETRY_BASE_DIR)
        file = str(
            (STAGE2_INSIGHT or {}).get("market_now_file", "insight_market_now.jsonl")
        )
        out = base / file
        row = {"ts": datetime.utcnow().isoformat() + "Z", **dict(summary)}
        _append_jsonl(out, row)
    except Exception:
        logger.debug("update_market_now failed", exc_info=True)
