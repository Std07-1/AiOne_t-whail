"""Централізовані специфікації телеметрії для Stage3 shadow-пайплайна.

Файл описує канонічні події, які мають генерувати сервіси Stage3.
Мета: уникнути розсинхрону під час міграції та гарантувати стабільний контракт.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

__all__ = [
    "TelemetryEventSpec",
    "SHADOW_PRICE_EVENTS",
    "SHADOW_STRATEGY_EVENTS",
    "STAGE3_CORE_EVENTS",
    "STAGE3_TELEMETRY_EVENTS",
]


@dataclass(frozen=True)
class TelemetryEventSpec:
    """Опис однієї телеметричної події.

    Args:
        name: Назва події (як логікує telemetry_sink).
        description: Людинозрозумілий опис події.
        required_fields: Кортеж ключів, які обов'язково мають бути у payload.
        optional_fields: Кортеж додаткових ключів (best-effort).
    """

    name: str
    description: str
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...] = ()


SHADOW_PRICE_EVENTS: Mapping[str, TelemetryEventSpec] = {
    "price_shadow_ingest": TelemetryEventSpec(
        name="price_shadow_ingest",
        description=(
            "Стрімінгове оновлення ціни потрапило у shadow pipeline і записане в"
            " Redis Stream. Використовується для контролю latency Stage3."
        ),
        required_fields=(
            "ingest_ts",
            "source",
            "latency_ms",
        ),
        optional_fields=(
            "symbol",
            "redis_stream",
            "redis_id",
            "mode",
        ),
    ),
    "price_shadow_failover": TelemetryEventSpec(
        name="price_shadow_failover",
        description=(
            "Ціновий продюсер не зміг записати оновлення в Redis і зафіксував"
            " деградацію. Викликає health алерти та блокування Stage3."
        ),
        required_fields=("reason",),
        optional_fields=(
            "symbol",
            "attempt",
        ),
    ),
    "price_shadow_recovered": TelemetryEventSpec(
        name="price_shadow_recovered",
        description=(
            "Після деградації price-stream відновився: з'єднання Redis успішно"
            " перепідключено, latency нормалізована."
        ),
        required_fields=("downtime_sec",),
        optional_fields=("attempts", "redis_stream"),
    ),
    "price_shadow_pending_warn": TelemetryEventSpec(
        name="price_shadow_pending_warn",
        description=(
            "Попередження: у shadow price stream накопичилось забагато pending"
            " повідомлень. Сигнал для health-оркестратора і trade-throttle."
        ),
        required_fields=("pending",),
        optional_fields=("threshold", "redis_stream"),
    ),
    "price_shadow_health": TelemetryEventSpec(
        name="price_shadow_health",
        description=(
            "Попередження: shadow price stream не отримував оновлень довше за"
            " допустимий поріг. Використовується для моніторингу лагу цін."
        ),
        required_fields=("lag_sec", "threshold"),
        optional_fields=("symbol", "redis_stream"),
    ),
}

SHADOW_STRATEGY_EVENTS: Mapping[str, TelemetryEventSpec] = {
    "strategy_shadow_open_enqueued": TelemetryEventSpec(
        name="strategy_shadow_open_enqueued",
        description=(
            "Stage3 open candidate записаний у shadow strategy stream."
            " Використовується для контролю latency перед фактичним виконанням."
        ),
        required_fields=("entry_id", "lag_ms"),
        optional_fields=("symbol", "stream"),
    ),
    "strategy_shadow_error": TelemetryEventSpec(
        name="strategy_shadow_error",
        description=(
            "Помилка роботи shadow strategy pipeline (enqueue/redis/декодування)."
        ),
        required_fields=("reason",),
        optional_fields=("symbol", "entry_id"),
    ),
    "shadow_candidate_open": TelemetryEventSpec(
        name="shadow_candidate_open",
        description=(
            "Stage3 кандидат на відкриття переданий у shadow stream (після фільтрів)."
        ),
        required_fields=(),
        optional_fields=(
            "symbol",
            "confidence",
            "confidence_damped",
            "health_multiplier",
            "effective_parallel",
            "price",
            "tp",
            "sl",
        ),
    ),
    "shadow_candidate_skip": TelemetryEventSpec(
        name="shadow_candidate_skip",
        description=(
            "Stage3 кандидат пропущений, але подія зафіксована у shadow telemetry."
        ),
        required_fields=("reason",),
        optional_fields=(
            "symbol",
            "confidence",
            "confidence_damped",
            "health_multiplier",
            "effective_parallel",
            "tp",
            "sl",
            "price",
            "threshold",
            "window_sec",
            "state",
            "signal",
            "band_pct",
            "scenario",
            "trend",
            "volume",
            "atr_pct",
            "low_gate",
            "context_price",
            "reasons",
            "busy_level",
            "busy_reason",
            "cooldown_left",
            "lock_reason",
            "pending",
            "max_band",
            "htf_ok",
        ),
    ),
    "shadow_candidate_error": TelemetryEventSpec(
        name="shadow_candidate_error",
        description=(
            "Помилка під час підготовки shadow candidate (серіалізація, Redis тощо)."
        ),
        required_fields=("reason",),
        optional_fields=("symbol", "error"),
    ),
    "shadow_confidence_gate": TelemetryEventSpec(
        name="shadow_confidence_gate",
        description=(
            "Застосовано демпфування впевненості Stage3 через health-мультиплікатор."
        ),
        required_fields=(),
        optional_fields=(
            "symbol",
            "confidence",
            "confidence_damped",
            "health_multiplier",
            "effective_parallel",
            "busy_level",
            "busy_reason",
        ),
    ),
}

STAGE3_CORE_EVENTS: Mapping[str, TelemetryEventSpec] = {
    "price_update_processed": TelemetryEventSpec(
        name="price_update_processed",
        description=(
            "TradeUpdateService обробив оновлення ціни та доставив його до"
            " активних угод. Містить latency pipeline і pending метрики."
        ),
        required_fields=(
            "symbol",
            "latency_ms",
            "delivery_latency_ms",
            "pending_count",
        ),
        optional_fields=("redis_id", "source", "sequence_id"),
    ),
    "stage3_health": TelemetryEventSpec(
        name="stage3_health",
        description=(
            "Агрегована метрика стану Stage3: тиск на shadow stream, "
            "навантаження TradeLifecycleManager та рівень busy."
        ),
        required_fields=(
            "status",
            "busy_level",
            "stream_pending",
            "stream_depth",
            "active_trades",
        ),
        optional_fields=(
            "buffer_depth",
            "pending_ratio",
            "open_slots",
            "confidence_avg",
            "confidence_max",
            "confidence_min",
            "redis_stream",
            "timestamp",
            "busy_reason",
        ),
    ),
    "open_executed": TelemetryEventSpec(
        name="open_executed",
        description=(
            "TradeExecutionService відкрив угоду (stream → TradeLifecycleManager)."
        ),
        required_fields=("symbol", "trade_id"),
        optional_fields=("entry_price", "strategy", "queued"),
    ),
    "open_rejected": TelemetryEventSpec(
        name="open_rejected",
        description="TradeExecutionService відхилив відкриття угоди.",
        required_fields=("symbol", "reason"),
        optional_fields=("trade_id",),
    ),
    "open_failed": TelemetryEventSpec(
        name="open_failed",
        description="Не вдалося відкрити угоду через внутрішню помилку Stage3.",
        required_fields=("symbol", "reason"),
        optional_fields=("trade_id",),
    ),
}

STAGE3_TELEMETRY_EVENTS: Mapping[str, TelemetryEventSpec] = {
    **SHADOW_PRICE_EVENTS,
    **SHADOW_STRATEGY_EVENTS,
    **STAGE3_CORE_EVENTS,
}
