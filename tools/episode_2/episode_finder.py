"""Episode detection logic (extracted from monolithic episodes.py).

Overview:
    Інкапсулює алгоритм пошуку напрямлених «епізодів» (великих рухів) у ціновому ряді.
    Епізод визначається як відрізок між локальним екстремумом (min/max) та протилежним екстремумом,
    де накопичений рух (up або down) перевищив відсотковий поріг і (опційно) відбувся retrace.

Algorithm (спрощено):
    1. Ітеруємо послідовно індекс i.
    2. Вікно росте до max_bars або поки не виконано умову руху.
    3. direction встановлюється, коли up_move >= move_pct_up або down_move >= move_pct_down.
    4. Для напрямку чекаємо retrace (якщо require_retrace=True) або кінець вікна.
    5. Фіксуємо епізод між екстремумами (min->max для UP, max->min для DOWN), якщо тривалість >= min_bars.
    6. Переходимо за кінець епізоду, інакше i++.

Complexities:
    * Гірший випадок O(N * W) де W ~ max_bars, але на практиці селективний break скорочує час.
    * Пам'ять O(1) + список епізодів.

Edge Cases:
    * Всі NaN → повертає порожній список.
    * Вкінці немає retrace → можлива фіксація при досягненні max_bars.
    * adaptive_threshold наразі тільки пропагується (місце для розширення).

Extension Points:
    * Можна додати adaptive_threshold зміну move_pct_* в процесі.
    * Можна інтегрувати волатильність або ATR для динаміки порогу.

Outputs:
    * Список Episode із заповненими метриками (move_pct, max_runup_pct, max_drawdown_pct, duration_bars, індекси/час).
"""

from __future__ import annotations

import logging
import time
from typing import List, Optional

import pandas as pd
from ep_2.core import Direction, Episode, calculate_max_runup_drawdown  # type: ignore

try:
    from rich.console import Console  # type: ignore
    from rich.logging import RichHandler  # type: ignore

    _HAS_RICH = True
except ImportError:  # pragma: no cover
    _HAS_RICH = False

logger = logging.getLogger("ep2.episode_finder")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    if _HAS_RICH:
        _h = RichHandler(console=Console(stderr=True), show_path=False)  # type: ignore[arg-type]
    else:
        _h = logging.StreamHandler()
        _h.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        )
    logger.addHandler(_h)
    logger.propagate = False


def log_params(params, title="Параметри пошуку епізодів:"):
    symbol = params.get("symbol")
    tf = params.get("timeframe")
    lines = [f"{title}", f"  symbol={symbol}", f"  timeframe={tf}"]
    for k, v in params.items():
        if k in ("symbol", "timeframe"):
            continue
        lines.append(f"  {k}: {v}")
    logger.info("\n".join(lines))


def find_episodes_v2(
    df: pd.DataFrame,
    move_pct_up: float = 0.02,
    move_pct_down: float = 0.02,
    min_bars: int = 20,
    max_bars: int = 750,
    close_col: str = "close",
    retrace_ratio: float = 0.33,
    require_retrace: bool = True,
    min_gap_bars: int = 0,
    merge_adjacent: bool = True,
    max_episodes: Optional[int] = None,
    adaptive_threshold: bool = False,
    config_context=None,
) -> List[Episode]:
    """Find directional price episodes in a OHLCV DataFrame.

    Args:
        df: DataFrame із колонкою close_col та DatetimeIndex.
        move_pct_up: Відсотковий поріг (фракція, 0.02=2%) для встановлення напрямку UP.
        move_pct_down: Аналогічно для DOWN.
        min_bars: Мінімальна кількість барів у епізоді.
        max_bars: Максимальна довжина (жорсткий обріз) розгляду при пошуку одного епізоду.
        close_col: Назва колонки з ціною close.
        retrace_ratio: Частка початкового move_pct, яку повинен пройти відкат (retrace) для фіксації епізоду.
        require_retrace: Якщо False – епізод можна закріпити без retrace.
        min_gap_bars: (Плейсхолдер) Мінімальний розрив між епізодами (ще не застосовано у фільтрі).
        merge_adjacent: (Плейсхолдер) Злиття сусідніх епізодів (ще не реалізовано тут – делеговано вищому рівню).
        max_episodes: Ліміт кількості епізодів (необов'язково; не застосовано якщо None).
        adaptive_threshold: Прапорець для майбутнього динамічного порогу (поки не використовується).

    Returns:
        Список Episode (може бути порожнім).
    """

    def _ctx_get(key):
        if config_context is None:
            return None
        if isinstance(config_context, dict):
            return config_context.get(key)
        return getattr(config_context, key, None)

    params_log = {
        "symbol": _ctx_get("symbol"),
        "timeframe": _ctx_get("timeframe"),
        "move_pct_up": move_pct_up,
        "move_pct_down": move_pct_down,
        "min_bars": min_bars,
        "max_bars": max_bars,
        "close_col": close_col,
        "retrace_ratio": retrace_ratio,
        "require_retrace": require_retrace,
        "min_gap_bars": min_gap_bars,
        "merge_adjacent": merge_adjacent,
        "max_episodes": max_episodes if max_episodes is not None else "None",
        "adaptive_threshold": adaptive_threshold,
    }
    log_params(params_log, title="Параметри пошуку епізодів:")
    t0 = time.perf_counter()
    if df.isna().any().any():
        nan_map = {c: int(df[c].isna().sum()) for c in df.columns if df[c].isna().any()}
        logger.warning(
            "NaN у даних:\n" + "\n".join([f"  {k}: {v}" for k, v in nan_map.items()])
        )
    assert close_col in df.columns, f"'{close_col}' column is required"
    n = len(df)
    if n == 0:
        logger.info("Порожній DF → епізодів: 0")
        return []
    close = df[close_col].astype(float).values
    logger.info("Запуск пошуку епізодів (n=%d)", n)
    i = 0
    max_iterations = n
    iteration = 0
    episodes: List[Episode] = []
    # episode_scan_start debug removed
    last_progress_log = 0
    while i < n - 1 and iteration < max_iterations:
        iteration += 1
        start = i
        base_price = close[start]
        local_min = base_price
        local_max = base_price
        local_min_idx = start
        local_max_idx = start
        direction: Optional[Direction] = None
        end = start
        episode_created = False
        upper_bound = min(n, start + max_bars + 1)
        for j in range(start + 1, upper_bound):
            end = j
            p = close[j]
            if p < local_min:
                local_min = p
                local_min_idx = j
            if p > local_max:
                local_max = p
                local_max_idx = j
            up_move = (p - local_min) / max(local_min, 1e-12)
            down_move = (local_max - p) / max(local_max, 1e-12)
            if direction is None:
                if up_move >= move_pct_up:
                    direction = Direction.UP
                    # direction_set debug removed (UP)
                elif down_move >= move_pct_down:
                    direction = Direction.DOWN
                    # direction_set debug removed (DOWN)
            if direction is not None:
                if direction == Direction.UP:
                    peak_price = close[local_max_idx]
                    retrace = (peak_price - p) / max(peak_price, 1e-12)
                    if (
                        (not require_retrace)
                        or (retrace >= retrace_ratio * move_pct_up)
                        or (j == upper_bound - 1)
                    ):
                        ep_start = local_min_idx
                        ep_end = local_max_idx
                        if ep_end - ep_start + 1 >= min_bars:
                            seg = close[ep_start : ep_end + 1]
                            move = (close[ep_end] - close[ep_start]) / max(
                                close[ep_start], 1e-12
                            )
                            runup, dd = calculate_max_runup_drawdown(
                                pd.Series(seg), Direction.UP.value
                            )
                            episodes.append(
                                Episode(
                                    start_idx=ep_start,
                                    end_idx=ep_end,
                                    direction=Direction.UP,
                                    peak_idx=local_max_idx,
                                    move_pct=float(move),
                                    duration_bars=int(ep_end - ep_start + 1),
                                    max_drawdown_pct=float(dd),
                                    max_runup_pct=float(runup),
                                    t_start=pd.Timestamp(df.index[ep_start]),
                                    t_end=pd.Timestamp(df.index[ep_end]),
                                )
                            )
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(
                                    "[finder] episode UP start=%d end=%d dur=%d move=%.4f runup=%.4f dd=%.4f",
                                    ep_start,
                                    ep_end,
                                    ep_end - ep_start + 1,
                                    move,
                                    runup,
                                    dd,
                                )
                            episode_created = True
                        i = max(ep_end, j)
                        break
                else:  # DOWN
                    trough_price = close[local_min_idx]
                    retrace = (p - trough_price) / max(trough_price, 1e-12)
                    if (
                        (not require_retrace)
                        or (retrace >= retrace_ratio * move_pct_down)
                        or (j == upper_bound - 1)
                    ):
                        ep_start = local_max_idx
                        ep_end = local_min_idx
                        if ep_end - ep_start + 1 >= min_bars:
                            seg = close[ep_start : ep_end + 1]
                            move = (close[ep_end] - close[ep_start]) / max(
                                close[ep_start], 1e-12
                            )
                            runup, dd = calculate_max_runup_drawdown(
                                pd.Series(seg), Direction.DOWN.value
                            )
                            episodes.append(
                                Episode(
                                    start_idx=ep_start,
                                    end_idx=ep_end,
                                    direction=Direction.DOWN,
                                    peak_idx=local_min_idx,
                                    move_pct=float(move),
                                    duration_bars=int(ep_end - ep_start + 1),
                                    max_drawdown_pct=float(dd),
                                    max_runup_pct=float(runup),
                                    t_start=pd.Timestamp(df.index[ep_start]),
                                    t_end=pd.Timestamp(df.index[ep_end]),
                                )
                            )
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(
                                    "[finder] episode DOWN start=%d end=%d dur=%d move=%.4f runup=%.4f dd=%.4f",
                                    ep_start,
                                    ep_end,
                                    ep_end - ep_start + 1,
                                    move,
                                    runup,
                                    dd,
                                )
                            episode_created = True
                        i = max(ep_end, j)
                        break
        if (
            logger.isEnabledFor(logging.DEBUG) and iteration - last_progress_log >= 5000
        ):  # large datasets
            last_progress_log = iteration
            logger.debug(
                "[finder] progress i=%d episodes=%d (%.2f%%)",
                i,
                len(episodes),
                100 * i / max(n - 1, 1),
            )
        if not episode_created:
            i += 1
    episodes_before_filter = len(episodes)
    episodes = [ep for ep in episodes if ep.duration_bars >= min_bars]
    if episodes_before_filter > 0 and len(episodes) == 0:
        logger.warning(
            f"[find_episodes_v2] Всі епізоди відфільтровані як занадто короткі (min_bars={min_bars})"
        )
    episodes.sort(key=lambda e: e.start_idx)
    if len(episodes) == 0:
        logger.info("[find_episodes_v2] Не знайдено жодного епізоду")
    # episode_scan_done debug removed
    total_time = (time.perf_counter() - t0) * 1000
    if episodes:
        durations = [ep.duration_bars for ep in episodes]
        avg_d = sum(durations) / len(durations)
        logger.info(
            "[finder] episodes=%d avg_dur=%.1f min=%d max=%d total_time=%.1fms",
            len(episodes),
            avg_d,
            min(durations),
            max(durations),
            total_time,
        )
        # Diagnostics: move statistics & gaps
        moves_abs = [abs(ep.move_pct) for ep in episodes]
        mean_move = sum(moves_abs) / len(moves_abs)
        min_move = min(moves_abs)
        max_move = max(moves_abs)
        # Gaps between episodes (bars not covered)
        gaps = [
            max(0, b.start_idx - a.end_idx - 1) for a, b in zip(episodes, episodes[1:])
        ]
        if gaps:
            avg_gap = sum(gaps) / len(gaps)
            max_gap = max(gaps)
            zero_gap_pct = 100 * sum(1 for g in gaps if g == 0) / len(gaps)
        else:
            avg_gap = max_gap = 0
            zero_gap_pct = 0.0
        logger.debug(
            "[finder.diag] move_mean=%.4f move_min=%.4f move_max=%.4f gap_avg=%.1f gap_max=%d zero_gap_pct=%.1f%%",  # noqa: E501
            mean_move,
            min_move,
            max_move,
            avg_gap,
            max_gap,
            zero_gap_pct,
        )
        # Coverage ratio (union of episode spans / total bars)
        covered = 0
        merged_spans = []
        for ep in sorted(episodes, key=lambda e: e.start_idx):
            if not merged_spans:
                merged_spans.append([ep.start_idx, ep.end_idx])
            else:
                last = merged_spans[-1]
                if ep.start_idx <= last[1] + 1:
                    last[1] = max(last[1], ep.end_idx)
                else:
                    merged_spans.append([ep.start_idx, ep.end_idx])
        for s, e in merged_spans:
            covered += e - s + 1
        coverage_ratio = covered / n if n else 0.0
        logger.debug(
            "[finder.diag] coverage_spans=%d covered_bars=%d coverage_ratio=%.4f",
            len(merged_spans),
            covered,
            coverage_ratio,
        )
        # Top 3 episodes by |move|
        top = sorted(episodes, key=lambda ep: abs(ep.move_pct), reverse=True)[:3]
        top_lines = [
            f"  #{i} dir={ep.direction.value} {ep.start_idx}->{ep.end_idx} move={ep.move_pct:.4f} dur={ep.duration_bars}"  # noqa: E501
            for i, ep in enumerate(top, 1)
        ]
        logger.debug("[finder.diag] top_moves:\n%s", "\n".join(top_lines))
    else:
        logger.info("[finder] episodes=0 total_time=%.1fms", total_time)
    return episodes


__all__ = ["find_episodes_v2"]
