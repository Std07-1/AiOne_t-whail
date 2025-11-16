from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import NAMESPACE
from config.keys import build_key

try:  # pragma: no cover - optional redis dependency
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:  # pragma: no cover - optional settings dependency
    from app.settings import settings  # type: ignore
except Exception:  # pragma: no cover
    settings = None  # type: ignore

logger = logging.getLogger("stage2.phase_state")
if not logger.handlers:  # pragma: no cover - guard repeated init
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

PHASE_STATE_MIN_SCORE = 0.45
# Максимальний вік (сек), протягом якого дозволяємо carry-forward
PHASE_STATE_MAX_AGE = 120.0
PHASE_STATE_HARD_MAX_AGE = 600.0
PHASE_STATE_BIAS_FLIP_DELTA = 0.25
PHASE_STATE_BIAS_STRONG_MIN = 0.20
PHASE_STATE_PRES_MIN_FOR_HOLD = 0.08
PHASE_STATE_PRES_MIN_RESET = 0.04
PHASE_STATE_REDIS_TTL = 1800
SOFT_PHASE_REASONS = {
    "presence_cap_no_bias_htf",
    "htf_gray_low",
    "volz_too_low",
}
SOFT_REASON_CODES = set(SOFT_PHASE_REASONS)
HARD_REASON_CODES = {
    "htf_conflict",
    "trend_reversal",
    "risk_block",
    "no_zones",
    "low_atr_guard",
    "anti_breakout_whale_guard",
    "htf_gate",
}


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        val = float(value)
        if val != val:  # NaN guard
            return None
        return val
    except Exception:
        return None


def _normalize_ts(ts: float | int | str | None) -> float:
    if ts is None:
        return time.time()
    try:
        val = float(ts)
    except Exception:
        return time.time()
    if val > 1_000_000_000_000:  # ms → s
        val = val / 1000.0
    if val <= 0:
        return time.time()
    return val


def _bias_conflict(new_bias: float | None, last_bias: float | None) -> bool:
    if new_bias is None or last_bias is None:
        return False
    try:
        if abs(float(new_bias) - float(last_bias)) < PHASE_STATE_BIAS_FLIP_DELTA:
            return False
        return float(new_bias) * float(last_bias) < 0.0
    except Exception:
        return False


def _bias_sign_conflict(new_bias: float | None, last_bias: float | None) -> bool:
    """Перевіряє протилежні знаки whale-bias за достатньої величини."""

    if new_bias is None or last_bias is None:
        return False
    try:
        new_val = float(new_bias)
        last_val = float(last_bias)
        if abs(new_val) < PHASE_STATE_BIAS_STRONG_MIN:
            return False
        if abs(last_val) < PHASE_STATE_BIAS_STRONG_MIN:
            return False
        return new_val * last_val < 0.0
    except Exception:
        return False


@dataclass
class PhaseState:
    current_phase: str | None = None
    last_detected_phase: str | None = None
    phase_score: float = 0.0
    age_s: float = 0.0
    last_reason: str | None = None
    last_whale_presence: float | None = None
    last_whale_bias: float | None = None
    last_htf_strength: float | None = None
    updated_ts: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> PhaseState:
        try:
            return cls(
                current_phase=payload.get("current_phase"),
                last_detected_phase=payload.get("last_detected_phase"),
                phase_score=float(payload.get("phase_score", 0.0) or 0.0),
                age_s=float(payload.get("age_s", 0.0) or 0.0),
                last_reason=payload.get("last_reason"),
                last_whale_presence=_safe_float(payload.get("last_whale_presence")),
                last_whale_bias=_safe_float(payload.get("last_whale_bias")),
                last_htf_strength=_safe_float(payload.get("last_htf_strength")),
                updated_ts=float(payload.get("updated_ts", 0.0) or 0.0),
            )
        except Exception:
            return cls()


class PhaseStateManager:
    def __init__(
        self,
        redis_client: Any | None = None,
        namespace: str = "ai_one:phase_state",
    ) -> None:
        if namespace == "ai_one:phase_state":
            namespace = f"{NAMESPACE}:phase_state"
        self.namespace = namespace
        self.redis_client = redis_client
        self._local_cache: dict[str, PhaseState] = {}
        self._redis_cache: Any | None = None

    def _ensure_client(self) -> Any | None:
        if self.redis_client is not None:
            return self.redis_client
        if self._redis_cache is not None:
            return self._redis_cache
        if redis is None or settings is None:
            return None
        try:
            self._redis_cache = redis.StrictRedis(  # type: ignore[attr-defined]
                host=settings.redis_host,
                port=settings.redis_port,
                decode_responses=True,
                encoding="utf-8",
            )
        except Exception:
            self._redis_cache = None
        return self._redis_cache

    def _build_key(self, symbol: str, granularity: str) -> str:
        sym = (symbol or "").upper()
        gran = granularity or "1m"
        if ":" in self.namespace:
            prefix = self.namespace.rstrip(":")
            return f"{prefix}:{sym}:{gran}"
        return build_key(NAMESPACE, self.namespace, symbol=sym, granularity=gran)

    def load(self, symbol: str, granularity: str) -> PhaseState:
        key = self._build_key(symbol, granularity)
        cached = self._local_cache.get(key)
        if cached is not None:
            return cached
        client = self._ensure_client()
        if client is None:
            state = PhaseState()
            self._local_cache[key] = state
            return state
        try:
            raw = client.get(key)
        except Exception:
            raw = None
        if not raw:
            state = PhaseState()
            self._local_cache[key] = state
            return state
        try:
            payload = json.loads(raw)
        except Exception:
            payload = {}
        state = PhaseState.from_dict(payload if isinstance(payload, Mapping) else {})
        self._local_cache[key] = state
        return state

    def save(self, symbol: str, granularity: str, state: PhaseState) -> None:
        key = self._build_key(symbol, granularity)
        self._local_cache[key] = state
        client = self._ensure_client()
        if client is None:
            return
        try:
            payload = json.dumps(state.to_dict())
            if hasattr(client, "setex"):
                client.setex(key, PHASE_STATE_REDIS_TTL, payload)
            else:
                client.set(key, payload)
        except Exception:
            return

    def _presence_conflict(
        self,
        presence: float | None,
    ) -> bool:
        p = _safe_float(presence)
        if p is None:
            return False
        return p < PHASE_STATE_PRES_MIN_RESET

    def update_from_detector(
        self,
        *,
        symbol: str,
        granularity: str,
        raw_phase: str | None,
        raw_score: float | None,
        phase_reason: str | None,
        whale_presence: float | None,
        whale_bias: float | None,
        htf_strength: float | None,
        now_ts: float,
    ) -> PhaseState:
        state = self.load(symbol, granularity)
        now = _normalize_ts(now_ts)
        delta = max(0.0, now - (state.updated_ts or 0.0)) if state.updated_ts else 0.0
        reason = (phase_reason or "").strip().lower() or None
        score = float(raw_score or 0.0)
        new_presence = _safe_float(whale_presence)
        new_bias = _safe_float(whale_bias)
        hard_reason = reason in HARD_REASON_CODES
        soft_reason = reason in SOFT_PHASE_REASONS
        bias_flip = _bias_conflict(new_bias, state.last_whale_bias)
        strong_bias_conflict = _bias_sign_conflict(new_bias, state.last_whale_bias)
        last_reason_soft = (
            state.last_reason is None or state.last_reason in SOFT_PHASE_REASONS
        )
        presence_conflict = self._presence_conflict(new_presence)
        age = state.age_s or 0.0
        hold_presence = (
            state.last_whale_presence
            if state.last_whale_presence is not None
            else new_presence
        )

        if raw_phase is not None and score >= PHASE_STATE_MIN_SCORE and not hard_reason:
            state.current_phase = raw_phase
            state.last_detected_phase = raw_phase
            state.phase_score = score
            state.age_s = 0.0
            state.last_reason = None
            state.last_whale_presence = new_presence
            state.last_whale_bias = new_bias
            state.last_htf_strength = _safe_float(htf_strength)
        elif (
            state.current_phase
            and soft_reason
            and last_reason_soft
            and age < PHASE_STATE_MAX_AGE
            and not bias_flip
            and not strong_bias_conflict
            and not presence_conflict
            and (hold_presence or 0.0) >= PHASE_STATE_PRES_MIN_FOR_HOLD
        ):
            state.age_s = min(PHASE_STATE_HARD_MAX_AGE, age + delta)
            state.last_reason = reason
            state.last_detected_phase = None
            if new_presence is not None:
                state.last_whale_presence = new_presence
            if new_bias is not None:
                state.last_whale_bias = new_bias
            if htf_strength is not None:
                state.last_htf_strength = _safe_float(htf_strength)
        else:
            if (
                hard_reason
                or bias_flip
                or presence_conflict
                or age >= PHASE_STATE_HARD_MAX_AGE
                or raw_phase is None
            ):
                state.current_phase = None
                state.phase_score = 0.0
                state.age_s = 0.0
                state.last_reason = reason
                state.last_detected_phase = raw_phase
                state.last_whale_presence = new_presence
                state.last_whale_bias = new_bias
                state.last_htf_strength = _safe_float(htf_strength)

        state.updated_ts = now
        self.save(symbol, granularity, state)
        return state


__all__ = [
    "PhaseState",
    "PhaseStateManager",
    "PHASE_STATE_MAX_AGE",
    "SOFT_REASON_CODES",
]
