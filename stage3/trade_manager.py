"""Stage3 TradeLifecycleManager.

Управління життєвим циклом угод:
    • відкриття та оновлення за правилами;
    • trailing (trail);
    • дострокові виходи;
    • контекстні адаптації.

Стиль:
    • короткі секційні хедери;
    • guard для логера;
    • коментарі до broad except.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

import pandas as pd

from config.config import (
    PACK_EXHAUSTION_REVERSAL_ENABLED_FLAG,
    STAGE3_PREDICTED_PROFIT_SCALE,
    STAGE3_STRATEGY_PROFILES,
    STAGE3_TRADE_PARAMS,
    get_stage3_param,
)
from monitoring.telemetry_sink import log_stage3_event
from utils.utils import safe_float

# ── Logger ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("stage3.trade_manager")
if not logger.handlers:  # guard від дублювання
    logger.setLevel(logging.DEBUG)
    try:
        from rich.console import Console
        from rich.logging import RichHandler

        logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    except Exception:  # broad except: rich опціональний
        logger.addHandler(logging.StreamHandler())
    logger.propagate = False

    STRATEGY_HINT_EXHAUSTION_REVERSAL = "exhaustion_reversal_long"


# ── Політика ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TradePolicy:
    """Налаштування Stage3 правил (entry/exit/trail)."""

    min_hold_seconds: float
    adverse_move_atr: float
    trail_arm_atr: float
    trail_break_even_atr: float
    trail_buffer_atr: float
    trail_buffer_atr_low: float
    trail_low_atr_threshold: float
    symbol_cooldown_sec: float
    trade_timeout_sec: float
    trade_timeout_sec_low: float
    trade_timeout_sec_mid: float
    trade_timeout_atr_low: float
    trade_timeout_atr_high: float
    debounce_window_sec: float


def _load_trade_policy() -> TradePolicy:
    params = STAGE3_TRADE_PARAMS or {}

    def _get(key: str, default: float) -> float:
        try:
            return float(params.get(key, default))
        except Exception:
            return default

    return TradePolicy(
        min_hold_seconds=_get("min_hold_seconds", 120.0),
        adverse_move_atr=_get("adverse_move_atr", 0.7),
        trail_arm_atr=_get("trail_arm_atr", 1.3),
        trail_break_even_atr=_get("trail_break_even_atr", 1.6),
        trail_buffer_atr=_get("trail_buffer_atr", 0.5),
        trail_buffer_atr_low=_get("trail_buffer_atr_low", 0.6),
        trail_low_atr_threshold=_get("trail_low_atr_threshold", 0.002),
        symbol_cooldown_sec=_get("symbol_cooldown_sec", 1200.0),
        trade_timeout_sec=_get("trade_timeout_sec", 3600.0),
        trade_timeout_sec_low=_get("trade_timeout_sec_low", 1200.0),
        trade_timeout_sec_mid=_get("trade_timeout_sec_mid", 2400.0),
        trade_timeout_atr_low=_get("trade_timeout_atr_low", 0.0015),
        trade_timeout_atr_high=_get("trade_timeout_atr_high", 0.0045),
        debounce_window_sec=_get("debounce_window_sec", 90.0),
    )


TRADE_POLICY = _load_trade_policy()


# ── Локальні хелпери ────────────────────────────────────────────────────────
def as_float(value: object, default: float = 0.0) -> float:
    """Приводить значення до float через safe_float, підставляє default, якщо None.

    Args:
        value: Вхідне значення (будь-що, що може бути float).
        default: Значення за замовчуванням, якщо конвертація неможлива.

    Returns:
        float: Коректний float (або default).
    """
    v = safe_float(value)
    if v is None:
        return default
    return v


def _resolve_trade_float(symbol: str, key: str, fallback: float) -> float:
    """Повертає параметр Stage3 для символа з float-конверсією."""

    value = get_stage3_param(symbol, key, fallback)
    resolved = safe_float(value)
    if resolved is None:
        try:
            return float(fallback)
        except Exception:
            return fallback
    return resolved


def _compose_policy_snapshot(symbol: str, defaults: TradePolicy) -> dict[str, float]:
    """Готує знімок політики Stage3 для символа на момент відкриття."""

    return {
        "min_hold_seconds": _resolve_trade_float(
            symbol, "min_hold_seconds", defaults.min_hold_seconds
        ),
        "adverse_move_atr": _resolve_trade_float(
            symbol, "adverse_move_atr", defaults.adverse_move_atr
        ),
        "trail_arm_atr": _resolve_trade_float(
            symbol, "trail_arm_atr", defaults.trail_arm_atr
        ),
        "trail_break_even_atr": _resolve_trade_float(
            symbol, "trail_break_even_atr", defaults.trail_break_even_atr
        ),
        "trail_buffer_atr": _resolve_trade_float(
            symbol, "trail_buffer_atr", defaults.trail_buffer_atr
        ),
        "trail_buffer_atr_low": _resolve_trade_float(
            symbol, "trail_buffer_atr_low", defaults.trail_buffer_atr_low
        ),
        "trail_low_atr_threshold": _resolve_trade_float(
            symbol, "trail_low_atr_threshold", defaults.trail_low_atr_threshold
        ),
        "symbol_cooldown_sec": _resolve_trade_float(
            symbol, "symbol_cooldown_sec", defaults.symbol_cooldown_sec
        ),
        "trade_timeout_sec": _resolve_trade_float(
            symbol, "trade_timeout_sec", defaults.trade_timeout_sec
        ),
        "trade_timeout_sec_low": _resolve_trade_float(
            symbol, "trade_timeout_sec_low", defaults.trade_timeout_sec_low
        ),
        "trade_timeout_sec_mid": _resolve_trade_float(
            symbol, "trade_timeout_sec_mid", defaults.trade_timeout_sec_mid
        ),
        "trade_timeout_atr_low": _resolve_trade_float(
            symbol, "trade_timeout_atr_low", defaults.trade_timeout_atr_low
        ),
        "trade_timeout_atr_high": _resolve_trade_float(
            symbol, "trade_timeout_atr_high", defaults.trade_timeout_atr_high
        ),
    }


# ───── Статуси угод ─────
TRADE_STATUS: dict[str, str] = {
    "OPEN": "open",
    "CLOSED_TP": "closed_tp",
    "CLOSED_SL": "closed_sl",
    "CLOSED_MANUAL": "closed_manual",
    "CLOSED_TIMEOUT": "closed_timeout",
    "CLOSED_BY_SIGNAL": "closed_by_signal",
    "CLOSED_BY_CLUSTER": "closed_by_cluster",
}


def utc_now() -> str:
    """Повертає поточний час в UTC у форматі ISO із суфіксом 'Z'."""
    return datetime.utcnow().isoformat() + "Z"


class Trade:
    """
    Модель торгової угоди.

    Attributes:
        id: Унікальний ідентифікатор.
        symbol: Торговий інструмент.
        entry_price: Ціна входу.
        tp: Take Profit.
        sl: Stop Loss.
        status: Поточний статус.
        open_time: Час відкриття.
        close_time: Час закриття.
        exit_reason: Причина закриття.
        result: Фінальний P&L (%).
        strategy: Ім'я стратегії.
        confidence: Рівень впевненості сигналу.
        indicators: ATR, RSI, Volume на вході.
        updates: Історія подій (open, update, trailing_stop тощо).
        current_price: Остання відома ціна.
        close_price: Ціна закриття.
    predicted_profit: Прогнозований профіт (%) на момент відкриття (масштабований).
    predicted_profit_raw: Базовий прогнозований профіт (%) без масштабування.
    """

    def __init__(self, signal: dict[str, Any], strategy: str = "default") -> None:
        # Унікальний ідентифікатор угоди
        self.id: str = f"{signal.get('symbol','?')}_{uuid.uuid4().hex}"
        # Основні атрибути
        self.symbol: str = signal.get("symbol", "")
        self.entry_price: float = as_float(signal.get("current_price"), 0.0)
        self.tp: float = as_float(signal.get("tp"), 0.0)
        self.sl: float = as_float(signal.get("sl"), 0.0)
        self.strategy: str = strategy
        self.confidence: float = as_float(signal.get("confidence", 0.0), 0.0)
        # Кластерні фактори, знайдені патерни та підтвердження контексту
        self.cluster_factors: list[str] = signal.get("cluster_factors", [])
        self.patterns: list[str] = signal.get("patterns", [])
        self.context_confirmations: list[str] = signal.get("context_confirmations", [])
        # Статус та часові мітки
        self.status: str = TRADE_STATUS["OPEN"]
        self.open_time: str = utc_now()
        self.close_time: str | None = None
        self.exit_reason: str | None = None
        # Контекст, що може оновлювати EnhancedContextAwareTradeManager
        self.context: dict[str, Any] = {"trail": {"armed": False}}
        # Фіксована політика на момент відкриття (для стабільності правил)
        self.policy_snapshot: dict[str, float] = {}
        # Ціни та індикатори
        self.current_price: float = self.entry_price
        self.close_price: float | None = None
        self.indicators: dict[str, float] = {
            "atr": as_float(signal.get("atr"), 0.0),
            "rsi": as_float(signal.get("rsi"), 0.0),
            "volume": as_float(signal.get("volume"), 0.0),
        }
        self.mfe_pct: float = 0.0
        self.mae_pct: float = 0.0
        self.trail_armed_ts: str | None = None
        self.first_trail_sl: float | None = None
        # Прогнозований прибуток (%) на момент відкриття
        if self.entry_price == 0:
            raw_predicted = 0.0
        elif self.tp >= self.entry_price:
            raw_predicted = (self.tp - self.entry_price) / self.entry_price * 100
        else:
            raw_predicted = (self.entry_price - self.tp) / self.entry_price * 100
        scale = float(STAGE3_PREDICTED_PROFIT_SCALE)
        self.predicted_profit_raw: float = raw_predicted
        self.predicted_profit = raw_predicted * scale

        # Фінальний P&L (%) — спочатку None, встановиться при закритті
        self.result: float | None = None

        # Історія подій (open, update, trailing_stop тощо)
        self.updates: list[dict[str, Any]] = []
        self._log_event("open", self._snapshot())
        logger.info(
            "🔔 Відкрито угоду %s: factors=%s patterns=%s conf=%.2f TP=%.4f SL=%.4f predicted_raw=%.2f%% scaled=%.2f%%",
            self.id,
            self.cluster_factors,
            self.patterns,
            self.confidence,
            self.tp,
            self.sl,
            self.predicted_profit_raw,
            self.predicted_profit,
        )

    def _snapshot(self) -> dict[str, Any]:
        """Поточний зріз стану угоди (для логування)."""
        return {
            "symbol": self.symbol,
            "side": self.side,
            "entry_price": self.entry_price,
            "tp": self.tp,
            "sl": self.sl,
            "status": self.status,
            "open_time": self.open_time,
            "current_price": self.current_price,
            "max_profit": self.max_profit,
            "mfe_pct": self.mfe_pct,
            "mae_pct": self.mae_pct,
            "cluster_factors": self.cluster_factors,
            "patterns": self.patterns,
            "context_confirmations": self.context_confirmations,
        }

    @property
    def side(self) -> str:
        """Напрямок угоди (buy якщо TP>=entry, інакше sell)."""
        return "buy" if self.tp >= self.entry_price else "sell"

    @property
    def max_profit(self) -> float:
        """Максимальний профіт (%) від відкриття до теперішньої ціни."""
        if self.entry_price == 0:
            return 0.0
        if self.side == "buy":
            return (self.current_price - self.entry_price) / self.entry_price * 100
        return (self.entry_price - self.current_price) / self.entry_price * 100

    @property
    def risk_reward_ratio(self) -> float:
        """Поточне співвідношення ризик/прибуток (R:R)."""
        reward = abs(self.tp - self.entry_price)
        risk = abs(self.entry_price - self.sl)
        if risk <= 0:
            return 0.0
        return reward / risk

    def _log_event(self, event: str, data: dict[str, Any]) -> None:
        """Додає запис в історію подій, фіксує поточний SL/TP."""
        data["sl"] = self.sl  # Фіксуємо поточний SL
        data["tp"] = self.tp  # Фіксуємо поточний TP
        data["mfe_pct"] = self.mfe_pct
        data["mae_pct"] = self.mae_pct
        trail_state = self.context.get("trail", {})
        data["trail_armed"] = bool(trail_state.get("armed"))
        if trail_state.get("armed_ts"):
            data.setdefault("trail_armed_ts", trail_state.get("armed_ts"))
        if trail_state.get("first_sl") is not None:
            data.setdefault("trail_first_sl", trail_state.get("first_sl"))
        record = {"event": event, "timestamp": utc_now(), **data}
        self.updates.append(record)

    def to_dict(self) -> dict[str, Any]:
        """Повертає повне представлення угоди для запису в лог."""
        base = self._snapshot()
        base.update(
            {
                "id": self.id,
                "strategy": self.strategy,
                "confidence": self.confidence,
                "predicted_profit_raw": self.predicted_profit_raw,
                "predicted_profit": self.predicted_profit,
                "close_time": self.close_time,
                "exit_reason": self.exit_reason,
                "result": self.result,
                "close_price": self.close_price,
                "indicators": self.indicators,
                "updates": self.updates,
                "context": self.context,
                "mfe_pct": self.mfe_pct,
                "mae_pct": self.mae_pct,
                "trail_armed_ts": self.trail_armed_ts,
                "first_trail_sl": self.first_trail_sl,
                "rr_ratio": self.risk_reward_ratio,
            }
        )
        return base


class TradeRule:
    """Інтерфейс правила для оновлення угоди."""

    async def __call__(self, trade: Trade, market: dict[str, Any]) -> None:
        raise NotImplementedError


class ContextExitRule(TradeRule):
    """Правило закриття при зміні ринкового контексту."""

    async def __call__(self, trade: Trade, market: dict[str, Any]) -> None:
        # Якщо market містить прапорець контр-тренду → закрити
        if market.get("context_break", False):
            trade.status = TRADE_STATUS["CLOSED_BY_SIGNAL"]
            trade.exit_reason = "context_break"
            trade._log_event("exit_context", {"reason": "context_break"})
            logger.info("❌ Угода %s закрита через контекст (context_break)", trade.id)


class TrailingStopRule(TradeRule):
    """Trail-stop із фазою активації та адаптивним буфером."""

    def __init__(self, policy: TradePolicy) -> None:
        self.policy = policy
        self.logger = logging.getLogger(f"{__name__}.TrailingStopRule")
        self.logger.setLevel(logging.DEBUG)

    async def __call__(self, trade: Trade, market: dict[str, Any]) -> None:
        if trade.status != TRADE_STATUS["OPEN"]:
            return

        price = as_float(market.get("price"), 0.0)
        if price <= 0:
            return
        atr = trade.indicators.get("atr", 0.0)
        if atr <= 0:
            return
        entry = trade.entry_price
        if entry <= 0:
            return

        policy_snapshot = trade.policy_snapshot or {}
        trail_arm_atr = policy_snapshot.get("trail_arm_atr", self.policy.trail_arm_atr)
        move = price - entry if trade.side == "buy" else entry - price
        if move < atr * trail_arm_atr:
            return

        trail_state = trade.context.setdefault("trail", {"armed": False})
        was_armed = bool(trail_state.get("armed"))
        trail_break_even_atr = policy_snapshot.get(
            "trail_break_even_atr", self.policy.trail_break_even_atr
        )
        allow_break_even = was_armed and move >= atr * trail_break_even_atr

        atr_pct = atr / entry if entry > 0 else 0.0
        trail_low_atr_threshold = policy_snapshot.get(
            "trail_low_atr_threshold", self.policy.trail_low_atr_threshold
        )
        trail_buffer_atr_low = policy_snapshot.get(
            "trail_buffer_atr_low", self.policy.trail_buffer_atr_low
        )
        trail_buffer_atr = policy_snapshot.get(
            "trail_buffer_atr", self.policy.trail_buffer_atr
        )
        buffer_mult = (
            trail_buffer_atr_low
            if atr_pct <= trail_low_atr_threshold
            else trail_buffer_atr
        )
        buffer_size = atr * buffer_mult
        if buffer_size <= 0:
            return

        old_sl = trade.sl
        if trade.side == "buy":
            target_sl = max(price - buffer_size, 1e-12)
            new_sl = max(old_sl, target_sl)
            if not allow_break_even and new_sl > entry:
                new_sl = entry
            new_sl = min(new_sl, price - buffer_size)
        else:
            target_sl = price + buffer_size
            new_sl = min(old_sl, target_sl)
            if not allow_break_even and new_sl < entry:
                new_sl = entry
            new_sl = max(new_sl, price + buffer_size)

        # Не дозволяємо опускати SL нижче 0
        if new_sl <= 0:
            return

        if new_sl != old_sl:
            trail_state["armed"] = True
            now_iso = utc_now()
            if not trail_state.get("armed_ts"):
                trail_state["armed_ts"] = now_iso
                trade.trail_armed_ts = now_iso
            trail_state["allow_break_even"] = allow_break_even
            trail_state["last_buffer"] = buffer_size
            trail_state["last_move_atr"] = move / atr if atr > 0 else None
            if trade.first_trail_sl is None:
                trail_state["first_sl"] = new_sl
                trade.first_trail_sl = new_sl
            trade.trail_armed_ts = trail_state.get("armed_ts", trade.trail_armed_ts)
            trade.context["trail"] = trail_state
            trade.sl = new_sl
            trade._log_event(
                "trailing_stop",
                {
                    "old_sl": old_sl,
                    "new_sl": new_sl,
                    "buffer": buffer_size,
                    "armed_atr": trail_arm_atr,
                    "allow_break_even": allow_break_even,
                },
            )
            self.logger.debug(
                "🛡 TRAIL %s: %.6f → %.6f (price=%.6f, atr=%.6f, buffer=%.6f)",
                trade.id,
                old_sl,
                new_sl,
                price,
                atr,
                buffer_size,
            )


class EarlyExitRule(TradeRule):
    """Контр-трендовий вихід із мінімальним холдом та ATR фільтрами."""

    def __init__(self, policy: TradePolicy) -> None:
        self.policy = policy

    async def __call__(self, trade: Trade, market: dict[str, Any]) -> None:
        if trade.status != TRADE_STATUS["OPEN"]:
            return

        try:
            opened_at = datetime.fromisoformat(trade.open_time.rstrip("Z"))
        except Exception:
            opened_at = datetime.utcnow()
        hold_seconds = (datetime.utcnow() - opened_at).total_seconds()
        policy_snapshot = trade.policy_snapshot or {}
        min_hold_seconds = policy_snapshot.get(
            "min_hold_seconds", self.policy.min_hold_seconds
        )
        if hold_seconds < min_hold_seconds:
            return

        price = as_float(market.get("price"), 0.0)
        if price <= 0:
            return
        atr = trade.indicators.get("atr", 0.0)
        if atr <= 0:
            return

        move_abs = abs(price - trade.entry_price)
        if move_abs < atr * 0.5:
            return

        rsi = as_float(market.get("rsi"), 0.0)
        adverse_move_atr = policy_snapshot.get(
            "adverse_move_atr", self.policy.adverse_move_atr
        )
        adverse = atr * adverse_move_atr
        if trade.side == "buy":
            if price <= trade.entry_price - adverse and rsi < 50:
                trade.status = TRADE_STATUS["CLOSED_BY_SIGNAL"]
                trade.exit_reason = "early_exit_contra"
                trade._log_event(
                    "early_exit",
                    {"hold_s": hold_seconds, "move": move_abs, "rsi": rsi},
                )
                logger.info(
                    "🔻 Early exit %s (contra move %.4f, rsi=%.2f)",
                    trade.id,
                    move_abs,
                    rsi,
                )
        else:  # sell
            if price >= trade.entry_price + adverse and rsi > 50:
                trade.status = TRADE_STATUS["CLOSED_BY_SIGNAL"]
                trade.exit_reason = "early_exit_contra"
                trade._log_event(
                    "early_exit",
                    {"hold_s": hold_seconds, "move": move_abs, "rsi": rsi},
                )
                logger.info(
                    "🔺 Early exit %s (contra move %.4f, rsi=%.2f)",
                    trade.id,
                    move_abs,
                    rsi,
                )


class TradeLifecycleManager:
    """
    Асинхронний менеджер життєвого циклу угоди.

    Використовує asyncio.Lock для потокобезпечності,
    cooldown для повторного відкриття,
    збирає підсумкову статистику кожної угоди,
    та веде наскрізне логування всіх ключових подій E2E.

    Логування:
        • INFO — відкриття, оновлення, закриття, summary;
        • DEBUG — деталі розрахунків, зміни статусу, причини пропуску;
        • WARNING — спроби закриття неіснуючих угод;
        • ERROR — помилки запису/логіки.
    """

    def __init__(
        self,
        log_file: str | None = None,
        summary_file: str | None = None,
        reopen_cooldown: float | None = None,
        max_parallel_trades: int = get_stage3_param("", "max_parallel_trades", 3),
        policy: TradePolicy | None = None,
    ) -> None:
        self.policy = policy or TRADE_POLICY
        self.active_trades: dict[str, Trade] = {}
        self.closed_trades: list[dict[str, Any]] = []
        if reopen_cooldown is None:
            reopen_cooldown = self.policy.symbol_cooldown_sec
        self.reopen_cooldown = float(reopen_cooldown)
        self.max_parallel_trades = max_parallel_trades
        self.recently_closed: dict[str, str] = {}  # symbol → ISO close_time
        # Централізовані шляхи журналів у TELEMETRY_BASE_DIR за замовчуванням
        if log_file is None or summary_file is None:
            try:
                from pathlib import Path as _Path

                from config.config import (  # type: ignore
                    STRATEGY_METRICS_LOG,
                    SUMMARY_LOG_FILE,
                    TELEMETRY_BASE_DIR,
                    TRADE_LOG_FILE,
                )

                base = _Path(TELEMETRY_BASE_DIR)
                base.mkdir(parents=True, exist_ok=True)
                self.log_file = (
                    str(base / TRADE_LOG_FILE) if log_file is None else log_file
                )
                self.summary_file = (
                    str(base / SUMMARY_LOG_FILE)
                    if summary_file is None
                    else summary_file
                )
                # Централізований шлях для пер-стратегічних метрик
                self.strategy_metrics_file = str(base / STRATEGY_METRICS_LOG)
            except Exception:
                self.log_file = log_file or "trade_log.jsonl"
                self.summary_file = summary_file or "summary_log.jsonl"
                self.strategy_metrics_file = "strategy_metrics.jsonl"
        else:
            self.log_file = log_file
            self.summary_file = summary_file
            # Визначаємо файл метрик поряд із summary
            try:
                from pathlib import Path as _Path

                from config.config import STRATEGY_METRICS_LOG  # type: ignore

                self.strategy_metrics_file = str(
                    _Path(self.summary_file).parent / STRATEGY_METRICS_LOG
                )
            except Exception:
                self.strategy_metrics_file = "strategy_metrics.jsonl"
        self.health_snapshot: dict[str, Any] | None = None
        # Оновлені правила включають контекстний вихід
        self.rules: list[TradeRule] = [
            ContextExitRule(),
            TrailingStopRule(self.policy),
            EarlyExitRule(self.policy),
        ]
        self.lock = asyncio.Lock()
        logger.info(
            "Ініціалізовано TradeLifecycleManager: log=%s, summary=%s, cooldown=%.1fs, max_parallel=%d",
            log_file,
            summary_file,
            self.reopen_cooldown,
            self.max_parallel_trades,
        )

    async def open_trade(
        self, signal: dict[str, Any], strategy: str = "default"
    ) -> str | None:
        """
        Відкриває угоду, якщо для символа нема open-угоди
        і якщо не в cooldown після останнього закриття.
        Додає обмеження на кількість одночасних угод.

        Returns:
            id відкритої або існуючої угоди, або None якщо пропущено.
        """
        async with self.lock:
            sym = signal["symbol"]
            logger.debug("Спроба відкриття угоди для %s зі сигналом: %s", sym, signal)

            policy_snapshot = _compose_policy_snapshot(sym, self.policy)

            # Strategy-specific overrides (дані-only з конфігу)
            strat_key = str(strategy or "").strip().lower()
            overrides = None
            try:
                normalized_key = strat_key.replace(" ", "_")
                if (
                    strat_key == STRATEGY_HINT_EXHAUSTION_REVERSAL
                    and not PACK_EXHAUSTION_REVERSAL_ENABLED_FLAG
                ):
                    logger.info(
                        "[STRICT_PHASE] %s стратегія %s вимкнена фіче-флагом",
                        sym,
                        strat_key,
                    )
                else:
                    if strat_key in STAGE3_STRATEGY_PROFILES:
                        overrides = STAGE3_STRATEGY_PROFILES[strat_key]
                    elif normalized_key in STAGE3_STRATEGY_PROFILES:
                        overrides = STAGE3_STRATEGY_PROFILES[normalized_key]
            except Exception:
                overrides = None

            if isinstance(overrides, dict):
                for key, value in overrides.items():
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        policy_snapshot[key] = float(value)
                    else:
                        policy_snapshot[key] = value
                logger.info(
                    "[STRICT_PHASE] %s застосовано StrategyPack overrides %s",
                    sym,
                    strat_key,
                )

            # 0) обмеження на кількість одночасних угод
            if len(self.active_trades) >= self.max_parallel_trades:
                logger.info(
                    "SKIP OPEN ❌ %s: досягнуто ліміту одночасних угод (%d)",
                    sym,
                    self.max_parallel_trades,
                )
                logger.debug(
                    "Причина SKIP: перевищено max_parallel_trades (%d)",
                    self.max_parallel_trades,
                )
                await log_stage3_event(
                    "open_rejected",
                    sym,
                    {
                        "reason": "max_parallel_trades",
                        "active_trades": len(self.active_trades),
                        "limit": self.max_parallel_trades,
                    },
                )
                return None

            # 1) cooldown після закриття
            last = self.recently_closed.get(sym)
            cooldown_limit = policy_snapshot.get(
                "symbol_cooldown_sec", self.reopen_cooldown
            )
            if last:
                t0 = datetime.fromisoformat(last.rstrip("Z"))
                elapsed = (datetime.utcnow() - t0).total_seconds()
                if elapsed < max(cooldown_limit, 0.0):
                    logger.info(
                        "SKIP OPEN ❌ %s: в cooldown %.0fs (закрита %s)",
                        sym,
                        cooldown_limit,
                        last,
                    )
                    logger.debug(
                        "Причина SKIP: cooldown active (elapsed=%.1fs < limit=%.1fs)",
                        elapsed,
                        cooldown_limit,
                    )
                    await log_stage3_event(
                        "open_rejected",
                        sym,
                        {
                            "reason": "cooldown",
                            "cooldown_limit": cooldown_limit,
                            "elapsed": elapsed,
                            "last_close": last,
                        },
                    )
                    return None

            # 2) якщо вже є open-угода — не відкриваємо нову
            for tr in self.active_trades.values():
                if tr.symbol == sym and tr.status == TRADE_STATUS["OPEN"]:
                    logger.info(
                        "SKIP OPEN ❌ %s: вже має відкриту угоду id=%s",
                        sym,
                        tr.id,
                    )
                    logger.debug(
                        "Причина SKIP: вже існує активна угода для %s (id=%s)",
                        sym,
                        tr.id,
                    )
                    await log_stage3_event(
                        "open_rejected",
                        sym,
                        {
                            "reason": "already_open",
                            "active_trade_id": tr.id,
                        },
                    )
                    return tr.id

            # 2.1) Перевірка інваріантів TP/SL/ціни
            entry_price = as_float(signal.get("current_price"), 0.0)
            tp = as_float(signal.get("tp"), 0.0)
            sl = as_float(signal.get("sl"), 0.0)
            min_entry = 0.0001  # мінімальна валідна ціна
            min_distance = 0.00001  # мінімальна відстань між TP/SL та entry

            invariant_errors = []
            if entry_price < min_entry:
                invariant_errors.append(
                    f"entry_price < {min_entry} (%.6f)" % entry_price
                )
            if tp < min_entry:
                invariant_errors.append(f"tp < {min_entry} (%.6f)" % tp)
            if sl < min_entry:
                invariant_errors.append(f"sl < {min_entry} (%.6f)" % sl)
            if abs(tp - entry_price) < min_distance:
                invariant_errors.append(
                    f"|tp-entry| < {min_distance} (%.6f)" % abs(tp - entry_price)
                )
            if abs(entry_price - sl) < min_distance:
                invariant_errors.append(
                    f"|entry-sl| < {min_distance} (%.6f)" % abs(entry_price - sl)
                )
            if tp == sl:
                invariant_errors.append("tp == sl ({:.6f})".format(tp))  # noqa: UP032

            if invariant_errors:
                logger.info("⛔️ Пропуск відкриття %s: цілі порушують інваріанти", sym)
                logger.debug(
                    "Причина SKIP [%s]: інваріанти не виконані для %s: %s",
                    datetime.now().isoformat(),
                    sym,
                    "; ".join(invariant_errors),
                )
                await log_stage3_event(
                    "open_rejected",
                    sym,
                    {
                        "reason": "invariant_violation",
                        "errors": invariant_errors,
                    },
                )
                return None

            # 3) інакше відкриваємо
            trade = Trade(signal, strategy)
            trade.policy_snapshot = policy_snapshot
            self.active_trades[trade.id] = trade

            # Лог файлу
            await self._persist(self.log_file, trade.to_dict())

            logger.info(
                "OPENED ✅ %s: id=%s, entry_price=%.6f, tp=%.6f, sl=%.6f",
                sym,
                trade.id,
                trade.entry_price,
                trade.tp,
                trade.sl,
            )
            logger.debug("OPEN DETAIL ▶ %s", trade.to_dict())

            await self._emit_stage3_trade_event(
                "trade_opened",
                trade,
                {
                    "entry_source": strategy,
                    "signal_confidence": trade.confidence,
                },
            )

            return trade.id

    async def has_open(self, symbol: str) -> bool:
        """Перевіряє, чи є активна угода для символа."""
        async with self.lock:
            for tr in self.active_trades.values():
                if (
                    tr.symbol.upper() == symbol.upper()
                    and tr.status == TRADE_STATUS["OPEN"]
                ):
                    logger.debug(
                        "has_open: знайдено відкриту угоду %s для %s", tr.id, symbol
                    )
                    return True
        logger.debug("has_open: немає відкритої угоди для %s", symbol)
        return False

    async def update_trade(self, trade_id: str, market: dict[str, Any]) -> bool:
        """
        Оновлює стан угоди: індикатори, правила, TP/SL, timeout.

        Returns:
            True якщо угода закрилася в цьому оновленні.
        """
        async with self.lock:
            tr = self.active_trades.get(trade_id)
            if not tr or tr.status != TRADE_STATUS["OPEN"]:
                logger.debug(
                    "UPDATE SKIP 🔄 %s: не знайдено відкриту угоду або status≠OPEN",
                    trade_id,
                )
                logger.debug(
                    "Причина SKIP: угода не знайдена або status≠OPEN (trade_id=%s)",
                    trade_id,
                )
                return False

            market_price = as_float(market.get("price"), 0.0)
            if market_price <= 0:
                logger.debug(
                    "UPDATE SKIP 🔄 %s: невалідна ціна оновлення %.6f",
                    trade_id,
                    market_price,
                )
                logger.debug(
                    "Причина SKIP: порушено інваріант — ціна <= 0 (trade_id=%s, price=%.6f)",
                    trade_id,
                    market_price,
                )
                return False

            tr.current_price = market_price
            logger.debug(
                "UPDATE ► %s: нова поточна ціна = %.6f",
                trade_id,
                tr.current_price,
            )

            entry = tr.entry_price
            if entry > 0:
                if tr.side == "buy":
                    favorable = max(0.0, (market_price - entry) / entry * 100)
                    adverse = max(0.0, (entry - market_price) / entry * 100)
                else:
                    favorable = max(0.0, (entry - market_price) / entry * 100)
                    adverse = max(0.0, (market_price - entry) / entry * 100)

                tr.mfe_pct = max(tr.mfe_pct, favorable)
                tr.mae_pct = max(tr.mae_pct, adverse)
                metrics = tr.context.setdefault("metrics", {})
                metrics.update({"mfe_pct": tr.mfe_pct, "mae_pct": tr.mae_pct})

            # застосування кожного правила
            for rule in self.rules:
                logger.debug(
                    "Застосовуємо правило %s до %s", rule.__class__.__name__, trade_id
                )
                await rule(tr, market)

            closed = await self._check_exit(tr)
            logger.debug(
                "RESULT ▶ %s: status=%s, closed=%s",
                trade_id,
                tr.status,
                closed,
            )

            # Дедуплікація: якщо останнє оновлення теж було 'update' і
            # price та status не змінилися — пропускаємо лог/запис у файл
            last_update = tr.updates[-1] if tr.updates else None
            if (
                last_update
                and last_update.get("event") == "update"
                and last_update.get("price") == tr.current_price
                and last_update.get("status") == tr.status
            ):
                logger.debug(
                    "UPDATE SKIP (no-change) 🔕 %s: остання подія має ту ж ціну/статус",
                    trade_id,
                )
            else:
                # лог події
                tr._log_event(
                    "update", {"price": tr.current_price, "status": tr.status}
                )
                await self._persist(self.log_file, tr.to_dict())
            # logger.debug("UPDATED ► %s", tr.to_dict()) тимчасово закоментував щоб зниизити шум логів але видаляти повністю не потрібно.

            if closed:
                # прибираємо з активних
                self.active_trades.pop(trade_id, None)
                logger.info(
                    "TRADE CLOSED ✅ %s: причина='%s'", trade_id, tr.exit_reason
                )
                logger.debug(
                    "TRADE CLOSED DETAIL: trade_id=%s, причина=%s",
                    trade_id,
                    tr.exit_reason,
                )
                await self._emit_stage3_trade_event(
                    "trade_closed",
                    tr,
                    {
                        "source": "auto_update",
                        "close_price": tr.close_price,
                    },
                )

            return closed

    async def close_trade(self, trade_id: str, price: float, reason: str) -> None:
        """
        Ручне закриття угоди.

        Args:
            price: Ціна закриття.
            reason: Причина закриття.
        """
        async with self.lock:
            tr = self.active_trades.pop(trade_id, None)
            if not tr:
                logger.warning("CLOSE SKIP ⚠️ %s: угода не знайдена", trade_id)
                logger.debug(
                    "Причина SKIP: спроба закрити неіснуючу угоду (trade_id=%s)",
                    trade_id,
                )
                return

            tr.status = TRADE_STATUS["CLOSED_MANUAL"]
            normalized_reason = (reason or "manual").lower()
            tr.exit_reason = normalized_reason if normalized_reason else "manual"
            tr.close_price = price
            tr.close_time = utc_now()
            tr.result = TradeLifecycleManager.calculate_profit(tr, price)

            # зберігаємо час для cooldown
            self.recently_closed[tr.symbol] = tr.close_time

            logger.info(
                "CLOSE ◀ %s: price=%.6f, reason=%s, result=%.2f%%",
                trade_id,
                price,
                reason,
                tr.result,
            )
            logger.debug("CLOSE DETAIL ◀ %s", tr.to_dict())

            # запис повного логу
            await self._persist(self.log_file, tr.to_dict())

            # запис summary
            summary = self._make_summary(tr)
            await self._persist(self.summary_file, summary)
            logger.info("SUMMARY ✍️ %s", summary)

            # Запис пер-стратегічних метрик (best-effort)
            await self._persist_strategy_metrics(tr)

            await self._emit_stage3_trade_event(
                "trade_closed_manual",
                tr,
                {
                    "source": "manual_close",
                    "close_price": price,
                    "reason": tr.exit_reason,
                },
            )

    async def _check_exit(self, tr: Trade) -> bool:
        """
        Перевіряє TP/SL та інші автоматичні статуси,
        застосовує timeout і записує summary при закритті.
        """
        p = tr.current_price
        now = datetime.utcnow()

        try:
            opened_at = datetime.fromisoformat(tr.open_time.rstrip("Z"))
        except Exception:
            opened_at = now
        hold_seconds = (now - opened_at).total_seconds()

        atr_val = tr.indicators.get("atr", 0.0)
        atr_ratio = 0.0
        if tr.entry_price > 0:
            atr_ratio = atr_val / tr.entry_price

        policy_snapshot = tr.policy_snapshot or {}
        min_hold_seconds = max(
            policy_snapshot.get("min_hold_seconds", self.policy.min_hold_seconds),
            0.0,
        )
        if tr.status != TRADE_STATUS["OPEN"]:
            tr.exit_reason = (tr.exit_reason or "manual").lower()
        else:
            trail_state = tr.context.get("trail", {})
            trail_armed = bool(trail_state.get("armed"))
            trail_arm_atr_value = policy_snapshot.get(
                "trail_arm_atr", self.policy.trail_arm_atr
            )
            timeout_sec = max(
                policy_snapshot.get("trade_timeout_sec", self.policy.trade_timeout_sec),
                0.0,
            )
            low_timeout = max(
                policy_snapshot.get(
                    "trade_timeout_sec_low", self.policy.trade_timeout_sec_low
                ),
                0.0,
            )
            mid_timeout = max(
                policy_snapshot.get(
                    "trade_timeout_sec_mid", self.policy.trade_timeout_sec_mid
                ),
                0.0,
            )
            atr_low = policy_snapshot.get(
                "trade_timeout_atr_low", self.policy.trade_timeout_atr_low
            )
            atr_high = policy_snapshot.get(
                "trade_timeout_atr_high", self.policy.trade_timeout_atr_high
            )
            if atr_ratio <= atr_low:
                timeout_sec = low_timeout or timeout_sec
                logger.debug(
                    "Інваріант: atr_ratio <= atr_low (%.6f <= %.6f), timeout_sec=%.1f",
                    atr_ratio,
                    atr_low,
                    timeout_sec,
                )
            elif atr_ratio <= atr_high:
                timeout_sec = mid_timeout or timeout_sec
                logger.debug(
                    "Інваріант: atr_ratio <= atr_high (%.6f <= %.6f), timeout_sec=%.1f",
                    atr_ratio,
                    atr_high,
                    timeout_sec,
                )

            allow_tp_sl = hold_seconds >= min_hold_seconds
            tp_hit = (tr.side == "buy" and p >= tr.tp) or (
                tr.side == "sell" and p <= tr.tp
            )
            tp_hold = tp_hit and (
                (trail_arm_atr_value > 0 and not trail_armed) or not allow_tp_sl
            )
            if tp_hit:
                if tp_hold:
                    logger.debug(
                        "TP HOLD ▶ %s: hold=%.1fs < %.1fs або трейл не активований (armed=%s, arm_atr=%.3f)",
                        tr.id,
                        hold_seconds,
                        min_hold_seconds,
                        trail_armed,
                        trail_arm_atr_value,
                    )
                    logger.debug(
                        "Причина: TP досягнуто, але інваріант hold_seconds < min_hold_seconds або трейл не активований"
                    )
                else:
                    logger.debug(
                        "TP TOUCH ▶ %s: залишаємо відкритою, трейл обробляє рух (armed=%s)",
                        tr.id,
                        trail_armed,
                    )
                    logger.debug(
                        "Причина: TP досягнуто, але трейл обробляє рух, угода не закривається"
                    )
                return False
            # SL
            if allow_tp_sl and tr.side == "buy" and p <= tr.sl:
                tr.status, tr.exit_reason = TRADE_STATUS["CLOSED_SL"], "sl"
                logger.info("SL CLOSE ▶ %s: ціна=%.6f, SL=%.6f", tr.id, p, tr.sl)
                logger.debug("Причина: ціна <= SL для buy (%.6f <= %.6f)", p, tr.sl)
            elif allow_tp_sl and tr.side == "sell" and p >= tr.sl:
                tr.status, tr.exit_reason = TRADE_STATUS["CLOSED_SL"], "sl"
                logger.info("SL CLOSE ▶ %s: ціна=%.6f, SL=%.6f", tr.id, p, tr.sl)
                logger.debug("Причина: ціна >= SL для sell (%.6f >= %.6f)", p, tr.sl)
            # timeout
            elif hold_seconds > timeout_sec:
                tr.status, tr.exit_reason = TRADE_STATUS["CLOSED_TIMEOUT"], "timeout"
                logger.info(
                    "TIMEOUT CLOSE ▶ %s: hold=%.1fs > %.1fs",
                    tr.id,
                    hold_seconds,
                    timeout_sec,
                )
                logger.debug(
                    "Причина: hold_seconds > timeout_sec (%.1fs > %.1fs)",
                    hold_seconds,
                    timeout_sec,
                )
            else:
                logger.debug(
                    "Інваріанти не порушено, угода залишається відкритою (id=%s)", tr.id
                )
                return False

        # заповнюємо поля закриття
        tr.close_price = p
        tr.close_time = utc_now()
        tr.result = TradeLifecycleManager.calculate_profit(tr, p)
        # cooldown
        self.recently_closed[tr.symbol] = tr.close_time

        # запис full-detail
        await self._persist(self.log_file, tr.to_dict())
        # запис summary
        await self._persist(self.summary_file, self._make_summary(tr))

        # пер-стратегічні метрики стратегії (win/rr) — best effort
        await self._persist_strategy_metrics(tr)

        # додаємо в closed_trades для внутрішнього зберігання
        self.closed_trades.append(tr.to_dict())
        logger.info(
            "TRADE E2E LOG ▶ %s: закрито, причина=%s, результат=%.2f%%",
            tr.id,
            tr.exit_reason,
            tr.result,
        )
        logger.debug(
            "TRADE EXIT DETAIL: id=%s, причина=%s, результат=%.2f%%",
            tr.id,
            tr.exit_reason,
            tr.result,
        )
        return True

    @staticmethod
    def calculate_profit(tr: Trade, price: float) -> float:
        """Profit (%) для buy/sell."""
        if tr.entry_price == 0:
            logger.debug(
                "Порушено інваріант: entry_price == 0 при розрахунку прибутку (id=%s)",
                tr.id,
            )
            return 0.0
        if tr.side == "buy":
            return (price - tr.entry_price) / tr.entry_price * 100
        return (tr.entry_price - price) / tr.entry_price * 100

    def _make_summary(self, tr: Trade) -> dict[str, Any]:
        """
        Формує підсумковий запис для summary_log.jsonl
        """
        return {
            "id": tr.id,
            "symbol": tr.symbol,
            "strategy": tr.strategy,
            "confidence": tr.confidence,
            "open_time": tr.open_time,
            "entry_price": tr.entry_price,
            "predicted_profit_raw": tr.predicted_profit_raw,
            "predicted_profit": tr.predicted_profit,
            "close_time": tr.close_time,
            "close_price": tr.close_price,
            "exit_reason": tr.exit_reason,
            "realized_profit": tr.result,
            "events_count": len(tr.updates),
            "mfe_pct": tr.mfe_pct,
            "mae_pct": tr.mae_pct,
            "trail_armed": bool(tr.context.get("trail", {}).get("armed")),
            "trail_armed_ts": tr.trail_armed_ts,
            "first_trail_sl": tr.first_trail_sl,
            "rr_ratio": tr.risk_reward_ratio,
        }

    async def _persist(self, file_path: str, data: dict[str, Any]) -> None:
        """Асинхронно записує JSONL у вказаний файл. Логування помилок українською."""
        loop = asyncio.get_event_loop()
        line = json.dumps(data, ensure_ascii=False) + "\n"
        try:
            await loop.run_in_executor(None, self._write_sync, file_path, line)
            logger.debug("Записано у файл %s: %s", file_path, data.get("id", ""))
        except Exception as e:
            logger.error("Помилка запису у файл %s: %s", file_path, e)
            logger.debug(
                "Причина ERROR: виняток при записі у файл %s: %s", file_path, e
            )

    async def _persist_strategy_metrics(self, tr: Trade) -> None:
        """Пише запис у strategy_metrics.jsonl. Best-effort, не впливає на логіку.

        Args:
            tr: Закрита угода із заповненими полями закриття.
        """
        try:
            rec = {
                "id": tr.id,
                "symbol": tr.symbol,
                "strategy": tr.strategy,
                "open_time": tr.open_time,
                "close_time": tr.close_time,
                "realized_profit": tr.result,
                "rr_ratio": tr.risk_reward_ratio,
                "exit_reason": tr.exit_reason,
            }
            await self._persist(self.strategy_metrics_file, rec)
        except Exception:
            # Телеметрія не повинна ламати життєвий цикл угоди
            pass

    def _write_sync(self, file_path: str, line: str) -> None:
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            logger.error("Помилка синхронного запису у файл %s: %s", file_path, e)
            logger.debug(
                "Причина ERROR: виняток при синхронному записі у файл %s: %s",
                file_path,
                e,
            )

    @staticmethod
    def _compute_hold_seconds(tr: Trade) -> float | None:
        """Обчислює тривалість утримання угоди у секундах."""

        if not tr.open_time or not tr.close_time:
            return None
        try:
            opened = datetime.fromisoformat(tr.open_time.rstrip("Z"))
            closed = datetime.fromisoformat(tr.close_time.rstrip("Z"))
        except Exception:
            return None
        hold = (closed - opened).total_seconds()
        return max(0.0, hold)

    def _build_trade_event_payload(self, tr: Trade) -> dict[str, Any]:
        """Готує словник із ключовими полями угоди для телеметрії."""

        payload: dict[str, Any] = {
            "trade_id": tr.id,
            "strategy": tr.strategy,
            "confidence": tr.confidence,
            "entry_price": tr.entry_price,
            "tp": tr.tp,
            "sl": tr.sl,
            "side": tr.side,
            "predicted_profit_raw": tr.predicted_profit_raw,
            "predicted_profit": tr.predicted_profit,
            "prediction_scale": float(STAGE3_PREDICTED_PROFIT_SCALE),
            "open_time": tr.open_time,
            "mfe_pct": tr.mfe_pct,
            "mae_pct": tr.mae_pct,
            "policy_snapshot": tr.policy_snapshot,
        }
        indicators = {
            name: value for name, value in tr.indicators.items() if value is not None
        }
        if indicators:
            payload["indicators"] = indicators
        if tr.close_time:
            payload["close_time"] = tr.close_time
        if tr.exit_reason:
            payload["exit_reason"] = tr.exit_reason
        if tr.result is not None:
            payload["realized_profit"] = tr.result
        hold_seconds = self._compute_hold_seconds(tr)
        if hold_seconds is not None:
            payload["hold_seconds"] = hold_seconds
        return payload

    async def _emit_stage3_trade_event(
        self, event: str, tr: Trade, extra: dict[str, Any] | None = None
    ) -> None:
        """Надсилає подію у телеметрію Stage3 (best-effort)."""

        payload = self._build_trade_event_payload(tr)
        if extra:
            payload.update(extra)
        payload.setdefault("active_trades_total", len(self.active_trades))
        payload.setdefault("max_parallel_limit", self.max_parallel_trades)
        payload.setdefault(
            "open_slots_remaining",
            max(0, self.max_parallel_trades - len(self.active_trades)),
        )
        await log_stage3_event(event, tr.symbol, payload)

    async def get_active_trades(self) -> list[dict[str, Any]]:
        """Повертає копію активних угод. Логування кількості українською."""
        async with self.lock:
            # Прибираємо шум на INFO: короткий DEBUG-лог із кількістю
            # logger.debug("get_active_trades: %d", len(self.active_trades))
            return [tr.to_dict() for tr in self.active_trades.values()]

    async def get_closed_trades(self) -> list[dict[str, Any]]:
        """Повертає копію закритих угод. Логування кількості українською."""
        async with self.lock:
            # Аналогічно — тільки DEBUG для уникнення спаму
            # logger.debug("get_closed_trades: %d", len(self.closed_trades))
            return list(self.closed_trades)


class EnhancedContextAwareTradeManager(TradeLifecycleManager):
    class _ContextEngineProto(Protocol):
        async def evaluate_context(self, symbol: str) -> dict[str, Any]: ...

        def get_last_bar(self, symbol: str) -> dict[str, object]: ...

        def load_data(self, symbol: str, interval: str = ...) -> object: ...

    def __init__(
        self,
        context_engine: _ContextEngineProto,
        *,
        log_file: str = "trade_log.jsonl",
        summary_file: str = "summary_log.jsonl",
        reopen_cooldown: float = 60.0,
        max_parallel_trades: int = 3,
    ) -> None:
        super().__init__(
            log_file=log_file,
            summary_file=summary_file,
            reopen_cooldown=reopen_cooldown,
            max_parallel_trades=max_parallel_trades,
        )
        self.context_engine: EnhancedContextAwareTradeManager._ContextEngineProto = (
            context_engine
        )
        # Додаткові параметри для керування чутливістю
        self.volatility_threshold = 0.005
        self.phase_change_threshold = 0.5

    async def manage_active_trades(self) -> None:
        """Періодична перевірка активних угод з урахуванням контексту"""
        while True:
            for trade_id in list(self.active_trades.keys()):
                trade = self.active_trades[trade_id]
                try:
                    # Отримання контексту з обробкою помилок
                    context = await self.context_engine.evaluate_context(trade.symbol)

                    # Перевірка зміни контексту
                    if await self.has_context_changed_significantly(trade, context):
                        await self.close_trade(
                            trade_id, trade.current_price, "context_change"
                        )
                        continue

                    # Адаптація параметрів угоди
                    self.adapt_trade_parameters(trade, context)

                    # Оновлення ринковими даними
                    market_data = self.get_market_data(trade.symbol)
                    await self.update_trade(trade_id, market_data)
                except Exception as e:  # broad except: ізоляція однієї угоди
                    logger.error(f"Error managing trade {trade_id}: {e}")

            await asyncio.sleep(60)

    async def has_context_changed_significantly(
        self, trade: Trade, new_context: dict[str, Any]
    ) -> bool:
        """Визначає чи зміна контексту вимагає закриття угоди"""
        old_context = getattr(trade, "context", {})
        old_phase = old_context.get("market_phase", "")
        new_phase = new_context["market_phase"]

        # Критичні зміни між протилежними станами
        critical_changes = {
            ("strong_uptrend", "strong_downtrend"),
            ("strong_downtrend", "strong_uptrend"),
            ("accumulation_phase", "distribution"),
            ("volatility_compression", "volatility_expansion"),
            ("price_compression", "price_expansion"),
        }

        # Перевірка критичних переходів
        if (old_phase, new_phase) in critical_changes:
            return True

        # Перевірка зсуву ключових рівнів
        old_levels = set(old_context.get("key_levels", []))
        new_levels = set(new_context["key_levels"])

        if old_levels and new_levels:
            # Розрахунок середньої зміни рівнів
            avg_change = sum(
                abs(new - old)
                for new, old in zip(sorted(new_levels), sorted(old_levels), strict=True)
            ) / len(old_levels)

            if avg_change / trade.entry_price > 0.03:
                return True

        # Перевірка різкої зміни волатильності
        old_volatility = old_context.get("volatility", 0)
        new_volatility = new_context["volatility"]
        if abs(new_volatility - old_volatility) > self.volatility_threshold * 3:
            return True

        return False

    def adapt_trade_parameters(self, trade: Trade, context: dict[str, Any]) -> None:
        """Адаптація параметрів угоди до нового контексту"""
        new_volatility = context["volatility"]
        old_context = getattr(trade, "context", {})
        old_volatility = old_context.get("volatility", 0)
        phase = context["market_phase"]

        # Корекція тільки при значній зміні волатильності
        if abs(new_volatility - old_volatility) > self.volatility_threshold:
            # Розраховуємо коефіцієнт коригування
            volatility_ratio = (
                new_volatility / old_volatility if old_volatility > 0 else 1.0
            )

            # Для трендових станів - більш агресивна корекція
            if "trend" in phase:
                tp_adjust = volatility_ratio**0.8
                sl_adjust = volatility_ratio**1.2
            # Для консолідації - консервативна корекція
            else:
                tp_adjust = volatility_ratio**0.5
                sl_adjust = volatility_ratio**0.8

            # Застосовуємо корекцію до TP/SL
            trade.tp = trade.entry_price + (trade.tp - trade.entry_price) * tp_adjust
            trade.sl = trade.entry_price - (trade.entry_price - trade.sl) * sl_adjust

            trade._log_event(
                "parameters_adjusted",
                {
                    "reason": "volatility_change",
                    "new_volatility": new_volatility,
                    "old_volatility": old_volatility,
                    "tp_adjust": tp_adjust,
                    "sl_adjust": sl_adjust,
                },
            )

        # Оновлення контексту в угоді
        trade.context = {
            "market_phase": phase,
            "key_levels": context["key_levels"],
            "volatility": new_volatility,
            "cluster_indicators": context["cluster_indicators"],
            "sentiment": context.get("sentiment", 0),
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Додаткова корекція для стиснених ринків
        if "compression" in phase:
            # Зменшуємо TP та розширюємо SL для більш консервативної стратегії
            trade.tp = trade.entry_price + (trade.tp - trade.entry_price) * 0.8
            trade.sl = trade.entry_price - (trade.entry_price - trade.sl) * 1.2
            trade._log_event(
                "compression_adjust",
                {
                    "reason": "market_compression",
                    "new_tp": trade.tp,
                    "new_sl": trade.sl,
                },
            )

    def get_market_data(self, symbol: str) -> dict[str, float]:
        """Покращене отримання ринкових даних з реального контексту/буфера/біржі"""
        # Спробуємо отримати останній бар з context_engine (якщо є метод)
        try:
            if hasattr(self.context_engine, "get_last_bar"):
                raw = self.context_engine.get_last_bar(symbol)
                bar: Mapping[str, object] = raw  # очікуємо мапу із числовими значеннями
                price = as_float(bar.get("close", 0.0), 0.0)
                volume = as_float(bar.get("volume", 0.0), 0.0)
                rsi = as_float(bar.get("rsi", 0.0), 0.0)
                ask = as_float(bar.get("ask", 0.0), 0.0)
                bid = as_float(bar.get("bid", 0.0), 0.0)
                spread = abs(ask - bid) if (ask and bid) else 0.0
                return {
                    "price": price,
                    "volume": volume,
                    "rsi": rsi,
                    "bid_ask_spread": spread,
                }
            else:
                df_obj = self.context_engine.load_data(symbol, "1m")
                if isinstance(df_obj, pd.DataFrame) and not df_obj.empty:
                    row = df_obj.iloc[-1]
                    price = float(row.get("close", 0.0))
                    volume = float(row.get("volume", 0.0))
                    rsi = float(row.get("rsi", 0.0))
                    ask = float(row.get("ask", 0.0)) if "ask" in row else 0.0
                    bid = float(row.get("bid", 0.0)) if "bid" in row else 0.0
                    spread = abs(ask - bid) if (ask and bid) else 0.0
                    return {
                        "price": price,
                        "volume": volume,
                        "rsi": rsi,
                        "bid_ask_spread": spread,
                    }
                return {"price": 0.0, "volume": 0.0, "rsi": 0.0, "bid_ask_spread": 0.0}
        except Exception as e:
            logger.error(
                f"[TradeManager] Не вдалося отримати ринкові дані для {symbol}: {e}"
            )
            return {"price": 0.0, "volume": 0.0, "rsi": 0.0, "bid_ask_spread": 0.0}

    def get_current_price(self, symbol: str) -> float:
        """Отримання поточної ціни з context_engine (остання ціна close)"""
        try:
            df_obj = self.context_engine.load_data(symbol)
            if isinstance(df_obj, pd.DataFrame) and not df_obj.empty:
                return float(df_obj.iloc[-1]["close"])
        except Exception as e:  # broad except: тільки лог діагностики
            logger.error(f"get_current_price error for {symbol}: {e}")
        return 0.0

    def get_current_volume(self, symbol: str) -> float:
        """Отримання поточного обсягу з context_engine (остній bar volume)"""
        try:
            df_obj = self.context_engine.load_data(symbol)
            if isinstance(df_obj, pd.DataFrame) and not df_obj.empty:
                return float(df_obj.iloc[-1]["volume"])
        except Exception as e:  # broad except: тільки лог діагностики
            logger.error(f"get_current_volume error for {symbol}: {e}")
        return 0.0

    def get_current_rsi(self, symbol: str) -> float:
        """Отримання поточного RSI з context_engine (остній bar rsi)"""
        try:
            df_obj = self.context_engine.load_data(symbol)
            if (
                isinstance(df_obj, pd.DataFrame)
                and not df_obj.empty
                and "rsi" in df_obj.columns
            ):
                return float(df_obj.iloc[-1]["rsi"])
        except Exception as e:  # broad except: тільки лог діагностики
            logger.error(f"get_current_rsi error for {symbol}: {e}")
        return 0.0

    def get_bid_ask_spread(self, symbol: str) -> float:
        """Отримання спреду з context_engine (bid/ask якщо є, інакше 0)"""
        try:
            df_obj = self.context_engine.load_data(symbol)
            if (
                isinstance(df_obj, pd.DataFrame)
                and not df_obj.empty
                and "bid" in df_obj.columns
                and "ask" in df_obj.columns
            ):
                bid = float(df_obj.iloc[-1]["bid"])
                ask = float(df_obj.iloc[-1]["ask"])
                return abs(ask - bid)
        except Exception as e:  # broad except: тільки лог діагностики
            logger.error(f"get_bid_ask_spread error for {symbol}: {e}")
        return 0.0
