from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any


@dataclass
class _CtxPoint:
    near_edge: str | None = None  # "upper" | "lower" | "near" | None
    band_expand: float | None = None
    vol_z: float | None = None
    dvr: float | None = None
    cd: float | None = None
    slope_atr: float | None = None
    rsi: float | None = None
    htf_strength: float | None = None
    whale_presence: float | None = None
    whale_bias: float | None = None
    low_atr_override_active: bool | None = None


class ContextMemory:
    """
    ContextMemory — це кільцевий буфер для зберігання останніх контекстних подій по символу,
    збирає легкі фічі для подальшої агрегації або гістерезису.

    Аргументи:
        n (int, опціонально): Максимальна кількість точок у буфері. За замовчуванням 60.

    Атрибути:
        N (int): Розмір буфера.
        _ring (deque[_CtxPoint]): Кільцевий буфер з контекстними точками.

    Методи:
        update(**features: Any) -> None:
            Додає нову точку контексту з переданими фічами.
        near_edge_persist(w: int) -> float:
            Частка останніх w точок, де 'near_edge' сигналізує близькість до краю.
        presence_sustain(w: int, thr: float = 0.6) -> float:
            Частка останніх w точок, де 'whale_presence' ≥ thr.
        htf_sustain(w: int, thr: float = 0.2) -> float:
            Частка останніх w точок, де 'htf_strength' ≥ thr.
        band_expand_score(w: int) -> float:
            Середнє значення 'band_expand' за останні w точок.
        export_meta(w: int = 3) -> dict[str, float]:
            Експортує компактний словник агрегованих метрик для market_context.meta.
    """

    def __init__(self, n: int = 60) -> None:
        self.N = int(n)
        self._ring: deque[_CtxPoint] = deque(maxlen=max(1, self.N))

    def update(self, **features: Any) -> None:
        p = _CtxPoint(
            near_edge=features.get("near_edge"),
            band_expand=_f(features.get("band_expand")),
            vol_z=_f(features.get("vol_z")),
            dvr=_f(features.get("dvr")),
            cd=_f(features.get("cd")),
            slope_atr=_f(features.get("slope_atr")),
            rsi=_f(features.get("rsi")),
            htf_strength=_f(features.get("htf_strength")),
            whale_presence=_f(features.get("whale_presence")),
            whale_bias=_f(features.get("whale_bias")),
            low_atr_override_active=_b(features.get("low_atr_override_active")),
        )
        self._ring.append(p)

    # ── Агрегати ──────────────────────────────────────────────────────────
    def near_edge_persist(self, w: int) -> float:
        """Частка останніх w точок, де ми біля краю (upper/lower/near/True)."""
        w = max(1, int(w))
        tail = list(self._ring)[-w:]
        if not tail:
            return 0.0

        def _is_near(v: str | None) -> bool:
            if v is True:  # type: ignore[truthy-bool]
                return True
            if isinstance(v, str):
                return v.lower() in {"upper", "lower", "near", "true"}
            return False

        cnt = sum(1 for p in tail if _is_near(p.near_edge))
        return cnt / float(len(tail))

    def presence_sustain(self, w: int, thr: float = 0.6) -> float:
        """Частка останніх w точок із whale_presence ≥ thr."""
        w = max(1, int(w))
        tail = list(self._ring)[-w:]
        if not tail:
            return 0.0
        cnt = 0
        for p in tail:
            try:
                if (p.whale_presence or 0.0) >= float(thr):
                    cnt += 1
            except Exception:
                pass
        return cnt / float(len(tail))

    def htf_sustain(self, w: int, thr: float = 0.2) -> float:
        """Частка останніх w точок із htf_strength ≥ thr."""
        w = max(1, int(w))
        tail = list(self._ring)[-w:]
        if not tail:
            return 0.0
        cnt = 0
        for p in tail:
            try:
                if (p.htf_strength or 0.0) >= float(thr):
                    cnt += 1
            except Exception:
                pass
        return cnt / float(len(tail))

    def band_expand_score(self, w: int) -> float:
        """Середнє band_expand за останні w точок (0.0 якщо немає даних)."""
        w = max(1, int(w))
        tail = list(self._ring)[-w:]
        if not tail:
            return 0.0
        vals = [float(p.band_expand or 0.0) for p in tail]
        return sum(vals) / float(len(vals)) if vals else 0.0

    # ── Експорт ───────────────────────────────────────────────────────────
    def export_meta(self, w: int = 3) -> dict[str, float]:
        """Підготовка компактного dict для market_context.meta.context."""
        return {
            "near_edge_persist": float(self.near_edge_persist(w)),
            "presence_sustain": float(self.presence_sustain(w)),
            "htf_sustain": float(self.htf_sustain(w)),
            "band_expand_score": float(self.band_expand_score(w)),
        }


def _f(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _b(x: Any) -> bool | None:
    try:
        if isinstance(x, bool):
            return x
        if x is None:
            return None
        s = str(x).strip().lower()
        return s in {"1", "true", "yes", "on"}
    except Exception:
        return None
