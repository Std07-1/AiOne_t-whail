from __future__ import annotations

Bar = dict[str, float]


def _sf(x: float | int | None, default: float = 0.0) -> float:
    try:
        return float(x)  # type: ignore[arg-type]
    except Exception:
        return float(default)


def band_width(atr_pct: float, tick: float, k: float) -> float:
    """Обчислює ширину діапазону (band) у цінах.

    - atr_pct: частка ATR від ціни (наприклад 0.012 для 1.2%).
    - tick: мінімальний крок ціни.
    - k: масштабатор для ATR.

    Правило: w = max(atr_pct * k, tick * 3).
    """
    try:
        atr = float(atr_pct or 0.0)
        t = float(tick or 0.0)
        kk = float(k or 0.0)
    except Exception:
        atr, t, kk = 0.0, 0.0, 0.0
    w = max(atr * kk, t * 3.0)
    return float(max(0.0, w))


def edge_features(bars: list[Bar], band: tuple[float, float]) -> dict[str, float]:
    """Оцінює прості фічі відносно band=(lo, hi) для останніх 15 барів.

    Повертає:
      - edge_hits: кількість барів, де high >= hi
      - accept_in_band: частка барів, де close ∈ [lo, hi]
      - band_squeeze: частка барів, де low >= lo і high <= hi (усі складові бару всередині діапазону)
      - max_pullback_in_band: max(hi - close) по барах з close ∈ [lo, hi]
    """
    if not isinstance(bars, (list, tuple)) or not bars:
        return {
            "edge_hits": 0.0,
            "accept_in_band": 0.0,
            "band_squeeze": 0.0,
            "max_pullback_in_band": 0.0,
        }
    lo, hi = float(band[0]), float(band[1])
    window = list(bars)[-15:]
    n = float(len(window))
    if n <= 0:
        return {
            "edge_hits": 0.0,
            "accept_in_band": 0.0,
            "band_squeeze": 0.0,
            "max_pullback_in_band": 0.0,
        }

    edge_hits = 0
    accept_cnt = 0
    squeeze_cnt = 0
    max_pullback = 0.0

    for b in window:
        h = _sf(b.get("high") or b.get("h"))
        low = _sf(b.get("low") or b.get("l"))
        c = _sf(b.get("close") or b.get("c"))
        if h >= hi:
            edge_hits += 1
        if lo <= c <= hi:
            accept_cnt += 1
            pb = max(0.0, hi - c)
            if pb > max_pullback:
                max_pullback = pb
        if (low >= lo) and (h <= hi):
            squeeze_cnt += 1

    return {
        "edge_hits": float(edge_hits),
        "accept_in_band": round(accept_cnt / n, 4),
        "band_squeeze": round(squeeze_cnt / n, 4),
        "max_pullback_in_band": float(max_pullback),
    }
