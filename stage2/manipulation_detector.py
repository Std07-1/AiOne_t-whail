"""
Модуль stage2/manipulation_detector.py

Призначення:
  • Виявляти «маніпулятивні» / «аномально-тонкі» рухи, коли система у strict-профілі
    свідомо не рекомендує входи (WAIT/OBSERVE), але трейдеру потрібен зрозумілий контекст:
      - liquidity grab / stop-run (верхній або нижній),
      - thin-liquidity pump/dump (великий діапазон при слабкому обсязі/тіку),
      - delta-divergence (ціна вгору, а кумулятивна дельта ~0 або від’ємна),
      - beta-drag (BTC імпульс «тягне» альт без локальних китів),
      - news microburst (обсяг + діапазон, але whale stale/absent),
      - laddering (сходинки малих тіл з кроком, слабкий vol_z).

  • Публікувати зрозумілі пояснення та підказку напряму (up/down/none) у
    market_context.meta.manipulation, + Prometheus-метрики для моніторингу.

Політика:
  • НЕ змінює рішень Stage2/Stage3 (без buy/sell), лише додає «контекст-підказку».
  • TTL для підказки → автообнулення gauge в метриках.
  • Пороги керуються з config.py (якщо є); інакше — безпечні дефолти.

Логування: RichHandler → stdout.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from typing import Literal

from rich.console import Console
from rich.logging import RichHandler

from utils.utils import safe_float

# ── Логування (RichHandler у stdout) ───────────────────────────────────────────
logger = logging.getLogger("manipulation_detector")
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

# ── Опційні Prometheus-хелпери (no-op, якщо немає) ────────────────────────────
try:
    # Рекомендація: додати ці метрики в prom_gauges.py
    #   Counter: ai_one_manipulation_pattern_total{symbol,pattern,dir}
    #   Gauge:   ai_one_manipulation_hint{symbol,dir}
    from telemetry.prom_gauges import inc_manipulation_pattern, set_manipulation_hint
except Exception:

    def inc_manipulation_pattern(symbol: str, pattern: str, direction: str) -> None:  # type: ignore
        pass

    def set_manipulation_hint(symbol: str, direction: str, val: int, ttl_s: int = 90) -> None:  # type: ignore
        pass


# ── Конфіг/пороги (підхоплюємо з config.py, інакше дефолти) ───────────────────
try:
    # В ideї проєкту є єдиний config.py — не ламаємо контракти
    import config  # type: ignore

    MANIP_HINT_TTL_S: int = getattr(config, "MANIP_HINT_TTL_S", 90)
    MANIP_WICK_RATIO_MIN: float = getattr(config, "MANIP_WICK_RATIO_MIN", 0.55)
    MANIP_RANGE_Z_MIN: float = getattr(config, "MANIP_RANGE_Z_MIN", 2.2)
    MANIP_VOL_Z_MAX_THIN: float = getattr(config, "MANIP_VOL_Z_MAX_THIN", 0.55)
    MANIP_TICK_Z_MAX_THIN: float = getattr(config, "MANIP_TICK_Z_MAX_THIN", 0.6)
    MANIP_DELTA_DIVERGENCE_ALLOW: bool = getattr(
        config, "MANIP_DELTA_DIVERGENCE_ALLOW", True
    )
    MANIP_STRONG_WHALE_PRES_MIN: float = getattr(
        config, "MANIP_STRONG_WHALE_PRES_MIN", 0.75
    )
    MANIP_STRONG_WHALE_BIAS_MIN: float = getattr(
        config, "MANIP_STRONG_WHALE_BIAS_MIN", 0.6
    )
    MANIP_NEWS_BURST_VOLZ: float = getattr(config, "MANIP_NEWS_BURST_VOLZ", 2.0)
    MANIP_NEWS_BURST_RANGEZ: float = getattr(config, "MANIP_NEWS_BURST_RANGEZ", 2.5)
except Exception:
    # Безпечні дефолти
    MANIP_HINT_TTL_S = 90
    MANIP_WICK_RATIO_MIN = 0.55
    MANIP_RANGE_Z_MIN = 2.2
    MANIP_VOL_Z_MAX_THIN = 0.55
    MANIP_TICK_Z_MAX_THIN = 0.6
    MANIP_DELTA_DIVERGENCE_ALLOW = True
    MANIP_STRONG_WHALE_PRES_MIN = 0.75
    MANIP_STRONG_WHALE_BIAS_MIN = 0.6
    MANIP_NEWS_BURST_VOLZ = 2.0
    MANIP_NEWS_BURST_RANGEZ = 2.5

Direction = Literal["up", "down", "none"]


@dataclass(frozen=True)
class Features:
    """Агреговані фічі останнього кадру (або невеликого вікна)."""

    symbol: str
    o: float
    h: float
    low: float
    c: float
    atr: float  # ATR(14) або інший базовий ATR
    volume_z: float | None  # z-скор обсягу; може бути proxy
    range_z: float | None  # (high-low)/ATR як z або нормалізований коеф.
    trade_count_z: float | None  # z-скор кількості тік-трейдів (якщо є)
    near_edge: Literal["upper", "lower", "none"] | None
    band_pct: float | None  # вузькість діапазону 0..1 (менше → тісніше)
    htf_strength: float | None
    cumulative_delta: float | None  # cd за бар/вікно
    price_slope_atr: float | None  # нахил ціни у ATR/бар
    whale_presence: float | None  # 0..1
    whale_bias: float | None  # -1..+1
    whale_stale: bool | None  # True якщо age_ms>порогу


@dataclass(frozen=True)
class ManipulationInsight:
    pattern: str
    direction: Direction
    confidence: float  # 0..1
    recommendation: Literal["OBSERVE", "AVOID", "WAIT"]
    reasons: list[str]  # короткі тексти «чому»


# ── Хелпери ───────────────────────────────────────────────────────────────────
def _wick_up_ratio(o: float, h: float, low: float, c: float) -> float:
    r = h - low
    if r <= 0:
        return 0.0
    upper = h - max(o, c)
    return max(0.0, min(1.0, upper / r))


def _wick_down_ratio(o: float, h: float, low: float, c: float) -> float:
    r = h - low
    if r <= 0:
        return 0.0
    lower = min(o, c) - low
    return max(0.0, min(1.0, lower / r))


# ── ДЕТЕКТОРИ ПАТЕРНІВ ────────────────────────────────────────────────────────
def _detect_liquidity_grab(f: Features) -> ManipulationInsight | None:
    """Stop-run над/під рівнем: велика тінь у бік пробою, слабка підтримка китів."""
    logger.debug("[STRICT_MANIP] перевірка liquidity_grab для символу=%s", f.symbol)
    wu = _wick_up_ratio(f.o, f.h, f.low, f.c)
    wd = _wick_down_ratio(f.o, f.h, f.low, f.c)
    pres = safe_float(f.whale_presence)
    bias = safe_float(f.whale_bias)

    # Верхній stop-run: довга верхня тінь, закриття в діапазоні, слабкий whale-сигнал
    if wu >= MANIP_WICK_RATIO_MIN and (pres < 0.35 or abs(bias) < 0.35):
        logger.debug(
            "[STRICT_MANIP] виявлено liquidity_grab_up: wu=%.2f pres=%.2f bias=%.2f для символу=%s",
            wu,
            pres,
            bias,
            f.symbol,
        )
        return ManipulationInsight(
            pattern="liquidity_grab_up",
            direction="up",
            confidence=min(1.0, 0.5 + 0.5 * wu),
            recommendation="OBSERVE",
            reasons=[
                f"довга верхня тінь wu={wu:.2f}",
                f"слабкі whales pres={pres:.2f} bias={bias:.2f}",
                "ймовірний знім стопів без підтвердження",
            ],
        )

    # Нижній stop-run
    if wd >= MANIP_WICK_RATIO_MIN and (pres < 0.35 or abs(bias) < 0.35):
        logger.debug(
            "[STRICT_MANIP] виявлено liquidity_grab_down: wd=%.2f pres=%.2f bias=%.2f для символу=%s",
            wd,
            pres,
            bias,
            f.symbol,
        )
        return ManipulationInsight(
            pattern="liquidity_grab_down",
            direction="down",
            confidence=min(1.0, 0.5 + 0.5 * wd),
            recommendation="OBSERVE",
            reasons=[
                f"довга нижня тінь wd={wd:.2f}",
                f"слабкі whales pres={pres:.2f} bias={bias:.2f}",
                "ймовірний знім ліквідності без підтвердження",
            ],
        )

    logger.debug("[STRICT_MANIP] liquidity_grab не виявлено для символу=%s", f.symbol)
    return None


def _detect_thin_liquidity_pump(f: Features) -> ManipulationInsight | None:
    """Діапазон великий при слабких vol/tick → «тонкий» рух (часто з відкатом)."""
    logger.debug(
        "[STRICT_MANIP] перевірка thin_liquidity_pump для символу=%s", f.symbol
    )
    rz = safe_float(f.range_z)
    vz = safe_float(f.volume_z)
    tz = safe_float(f.trade_count_z)

    if rz >= MANIP_RANGE_Z_MIN and (
        vz <= MANIP_VOL_Z_MAX_THIN or tz <= MANIP_TICK_Z_MAX_THIN
    ):
        # напрям за тілом свічки
        direction: Direction = "up" if f.c >= f.o else "down"
        logger.debug(
            "[STRICT_MANIP] виявлено thin_liquidity_move: rz=%.2f vz=%.2f tz=%.2f напрям=%s для символу=%s",
            rz,
            vz,
            tz,
            direction,
            f.symbol,
        )
        return ManipulationInsight(
            pattern="thin_liquidity_move",
            direction=direction,
            confidence=min(1.0, 0.6 + 0.1 * (rz - MANIP_RANGE_Z_MIN)),
            recommendation="OBSERVE",
            reasons=[
                f"range_z={rz:.2f} при слабкому vol/tick (vol_z={vz:.2f}, tick_z={tz:.2f})",
                "тонкий рух ймовірно без сталого попиту/пропозиції",
            ],
        )
    logger.debug(
        "[STRICT_MANIP] thin_liquidity_pump не виявлено для символу=%s", f.symbol
    )
    return None


def _detect_delta_divergence(f: Features) -> ManipulationInsight | None:
    """Ціна в один бік, кумулятивна дельта не підтримує → «fade»/спуф-фліп проксі."""
    logger.debug("[STRICT_MANIP] перевірка delta_divergence для символу=%s", f.symbol)
    if not MANIP_DELTA_DIVERGENCE_ALLOW:
        logger.debug(
            "[STRICT_MANIP] delta_divergence заборонено конфігом для символу=%s",
            f.symbol,
        )
        return None
    cd = safe_float(f.cumulative_delta)
    # напрям ціни за body
    up = f.c > f.o
    down = f.c < f.o
    if up and cd <= 0:
        logger.debug(
            "[STRICT_MANIP] виявлено delta_divergence_up: cd=%.3f напрям=up для символу=%s",
            cd,
            f.symbol,
        )
        return ManipulationInsight(
            pattern="delta_divergence_up",
            direction="up",
            confidence=0.6,
            recommendation="OBSERVE",
            reasons=[f"ціна ↑, cd={cd:.3f} ≤ 0 → розбіжність попиту"],
        )
    if down and cd >= 0:
        logger.debug(
            "[STRICT_MANIP] виявлено delta_divergence_down: cd=%.3f напрям=down для символу=%s",
            cd,
            f.symbol,
        )
        return ManipulationInsight(
            pattern="delta_divergence_down",
            direction="down",
            confidence=0.6,
            recommendation="OBSERVE",
            reasons=[f"ціна ↓, cd={cd:.3f} ≥ 0 → розбіжність пропозиції"],
        )
    logger.debug("[STRICT_MANIP] delta_divergence не виявлено для символу=%s", f.symbol)
    return None


def _detect_btc_beta_drag(f: Features) -> ManipulationInsight | None:
    """Альт рухається за BTC при слабких локальних whales → beta-drag."""
    logger.debug("[STRICT_MANIP] перевірка btc_beta_drag для символу=%s", f.symbol)
    # Не застосовувати до самого BTC (базовий актив для beta‑контексту)
    try:
        sym_up = str(f.symbol or "").strip().upper()
    except Exception:
        sym_up = ""
    if sym_up in {"BTCUSDT", "BTCUSD", "BTC-USD", "XBTUSD"}:
        logger.debug(
            "[STRICT_MANIP] beta_drag пропущено для базового інструменту: %s",
            f.symbol,
        )
        return None
    # Спрощено: якщо присутність низька, а htf сильний (або near_edge «upper»), вважаємо тягарем контексту
    pres = safe_float(f.whale_presence)
    if pres < 0.30 and (f.htf_strength or 0.0) >= 0.15:
        direction: Direction = "up" if f.c >= f.o else "down"
        logger.debug(
            "[STRICT_MANIP] виявлено beta_drag: pres=%.2f htf=%.2f напрям=%s",
            pres,
            safe_float(f.htf_strength),
            direction,
        )
        return ManipulationInsight(
            pattern="beta_drag",
            direction=direction,
            confidence=0.5,
            recommendation="WAIT",
            reasons=[
                f"локальні whales слабкі pres={pres:.2f}",
                "ймовірний рух «за ринком/BTC» без локального попиту",
            ],
        )
    logger.debug("[STRICT_MANIP] btc_beta_drag не виявлено для символу=%s", f.symbol)
    return None


def _detect_news_microburst(f: Features) -> ManipulationInsight | None:
    """Великий обсяг + діапазон при stale whales → новинний/зовнішній імпульс."""
    logger.debug("[STRICT_MANIP] перевірка news_microburst для символу=%s", f.symbol)
    rz = safe_float(f.range_z)
    vz = safe_float(f.volume_z)
    if (
        rz >= MANIP_NEWS_BURST_RANGEZ
        and vz >= MANIP_NEWS_BURST_VOLZ
        and (f.whale_stale is True)
    ):
        direction: Direction = "up" if f.c >= f.o else "down"
        logger.info(
            "[STRICT_MANIP] виявлено news_microburst: rz=%.2f vz=%.2f whale_stale=%s напрям=%s",
            rz,
            vz,
            f.whale_stale,
            direction,
        )
        return ManipulationInsight(
            pattern="news_microburst",
            direction=direction,
            confidence=0.7,
            recommendation="OBSERVE",
            reasons=[
                f"range_z={rz:.2f} vol_z={vz:.2f} при whale_stale",
                "імовірно зовнішній фактор / новина",
            ],
        )
    logger.debug("[STRICT_MANIP] news_microburst не виявлено для символу=%s", f.symbol)
    return None


def _detect_laddering(f: Features) -> ManipulationInsight | None:
    """Сходинки малих тіл, крокове підняття/опускання при низькому vol_z."""
    logger.debug("[STRICT_MANIP] перевірка laddering для символу=%s", f.symbol)
    # Проксі: маленький body vs range + слабкий vol_z → «ліфт» без попиту
    r = f.h - f.low
    body = abs(f.c - f.o)
    if r > 0 and (body / r) <= 0.2 and safe_float(f.volume_z) <= 0.5:
        direction: Direction = "up" if f.c >= f.o else "down"
        logger.debug(
            "[STRICT_MANIP] виявлено laddering: body_ratio=%.2f vol_z=%.2f напрям=%s",
            body / r,
            safe_float(f.volume_z),
            direction,
        )
        return ManipulationInsight(
            pattern="laddering",
            direction=direction,
            confidence=0.5,
            recommendation="OBSERVE",
            reasons=[
                f"маленьке тіло/body_ratio={(body/r):.2f} при слабкому vol_z={safe_float(f.volume_z):.2f}",
                "кроковий рух без явних китів",
            ],
        )
    logger.debug("[STRICT_MANIP] laddering не виявлено для символу=%s", f.symbol)
    return None


# ── ПУБЛІЧНИЙ API ─────────────────────────────────────────────────────────────
def _detect_controlled_distribution(f: Features) -> ManipulationInsight | None:
    """
    Виявляє керований низхідний зсув ціни без ознак маніпуляції чи виносу (controlled distribution).

    Умови (спостережні, без трейду):
      - whale_presence ≥ 0.5
      - whale_bias ≤ -0.6
      - 1.0 ≤ vol_z ≤ 1.8 (помірний тиск, не мікроберст)
      - htf_strength < 0.25 (слабкий HTF)
      - near_edge == 'lower'
    Вихід: direction=down, recommendation=OBSERVE
    """
    logger.debug(
        "[STRICT_MANIP] перевірка controlled_distribution для символу=%s", f.symbol
    )

    pres = safe_float(f.whale_presence)
    bias = safe_float(f.whale_bias)
    vz = safe_float(f.volume_z)
    htf = safe_float(f.htf_strength)
    edge_ok = f.near_edge == "lower"

    # Перевірка умов з поясненням
    if pres < 0.50:
        logger.debug(
            "[STRICT_MANIP] controlled_distribution не виявлено: whale_presence=%.2f < 0.50 (потрібно ≥0.50 для сильної присутності китів)",
            pres,
        )
        return None
    if bias > -0.60:
        logger.debug(
            "[STRICT_MANIP] controlled_distribution не виявлено: whale_bias=%.2f > -0.60 (потрібно ≤-0.60 для негативного ухилу)",
            bias,
        )
        return None
    if not (1.0 <= vz <= 1.8):
        logger.debug(
            "[STRICT_MANIP] controlled_distribution не виявлено: vol_z=%.2f не в [1.0,1.8] (потрібно помірний обсяг для тиску без спайку)",
            vz,
        )
        return None
    if htf >= 0.25:
        logger.debug(
            "[STRICT_MANIP] controlled_distribution не виявлено: htf_strength=%.2f ≥ 0.25 (потрібно <0.25 для слабкого HTF)",
            htf,
        )
        return None
    if not edge_ok:
        logger.debug(
            "[STRICT_MANIP] controlled_distribution не виявлено: near_edge='%s' != 'lower' (потрібно біля нижнього краю)",
            f.near_edge,
        )
        return None

    # Усі умови виконані
    # Розрахунок впевненості: базове значення 0.55, додається бонус за whale_presence понад 0.5,
    # та невелика поправка за негативний whale_bias (чим менше, тим вище confidence).
    confidence = min(1.0, 0.55 + 0.05 * (pres - 0.5) + 0.05 * min(0.0, bias + 0.6))
    reasons = [
        f"whale_presence={pres:.2f} ≥ 0.50",
        f"whale_bias={bias:.2f} ≤ -0.60",
        f"vol_z={vz:.2f} у [1.0,1.8]; htf={htf:.2f} < 0.25",
        "керований низхідний тиск без ознак виносу/спайку",
    ]
    logger.info(
        "[STRICT_MANIP] виявлено controlled_distribution: напрям=down, конф=%.2f, причини=%s",
        confidence,
        "; ".join(reasons),
    )

    return ManipulationInsight(
        pattern="controlled_distribution",
        direction="down",
        confidence=confidence,
        recommendation="OBSERVE",
        reasons=reasons,
    )


def detect_manipulation(f: Features) -> ManipulationInsight | None:
    """
    Основний вхід: повертає перший найбільш виразний інсайт (OBSERVE/WAIT + up/down),
    або None, якщо маніпулятивного патерну не виявлено.
    Порядок — від більш «сигнатурних» до загальніших.
    """
    logger.debug("[STRICT_MANIP] початок детекції для символу=%s", f.symbol)
    # Порядок детекторів визначає пріоритет: спочатку більш «сигнатурні» патерни, потім загальніші.
    # 1. liquidity_grab — стоп-рани, ключові для трейдерів.
    # 2. thin_liquidity_pump — тонкі рухи, часто з відкатом.
    # 3. news_microburst — імпульси на новинах, важливо для ризик-менеджменту.
    # 4. delta_divergence — розбіжність ціни та кумулятивної дельти.
    # 5. btc_beta_drag — альт рухається за BTC без локальних китів.
    # 6. controlled_distribution — керований низхідний тиск.
    # 7. laddering — сходинки малих тіл при слабкому vol_z.
    detectors = (
        _detect_liquidity_grab,
        _detect_thin_liquidity_pump,
        _detect_news_microburst,
        _detect_delta_divergence,
        _detect_btc_beta_drag,
        _detect_controlled_distribution,
        _detect_laddering,
    )
    for fn in detectors:
        logger.debug(
            "[STRICT_MANIP] перевірка детектора %s для символу=%s",
            fn.__name__,
            f.symbol,
        )
        ins = fn(f)
        if ins:
            # Метрики + короткий explain
            try:
                inc_manipulation_pattern(f.symbol, ins.pattern, ins.direction)
                set_manipulation_hint(
                    f.symbol, ins.direction, 1, ttl_s=MANIP_HINT_TTL_S
                )
            except Exception:
                pass
            logger.info(
                "[STRICT_MANIP] символ=%s паттерн=%s напрям=%s конф=%.2f причини=%s",
                f.symbol,
                ins.pattern,
                ins.direction,
                ins.confidence,
                "; ".join(ins.reasons[:3]),
            )
            return ins
        else:
            logger.debug(
                "[STRICT_MANIP] детектор %s не виявив патерну для символу=%s (умови не виконані)",
                fn.__name__,
                f.symbol,
            )
    logger.debug(
        "[STRICT_MANIP] символ=%s маніпуляція не виявлена після перевірки всіх детекторів",
        f.symbol,
    )
    return None


def attach_to_market_context(
    market_context: dict, ins: ManipulationInsight | None
) -> None:
    """
    Додає concise-пояснення у market_context.meta.manipulation (не змінює інших полів).
    TTL для gauge в метриках обробляється Prom-хелперами; тут лише публікація в state.
    """
    if "meta" not in market_context:
        market_context["meta"] = {}
    if ins is None:
        return
    market_context["meta"]["manipulation"] = {
        "direction": ins.direction,
        "pattern": ins.pattern,
        "conf": round(ins.confidence, 3),
        "reco": ins.recommendation,
        "reasons": ins.reasons[:3],
    }
