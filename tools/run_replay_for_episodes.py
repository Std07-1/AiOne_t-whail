"""Run Stage1→Stage2 replay for top episodes found by the simplified ep_2 pipeline.

Потік:
1) Завантажуємо OHLCV через ep_2.data_loader.fetch_bars (за EPISODE_CONFIG_5M).
2) Знаходимо епізоди (find_episodes_v2) і беремо топ-2 за |move_pct|.
3) Для кожного епізоду обчислюємо вікно [start_ms-буфер; end_ms+буфер] і запускаємо replay.
4) Аналізуємо summary.csv і друкуємо стислий звіт.

Без зміни контрактів; лише утиліта для швидкого бектесту обраних епізодів.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import pandas as pd
from tools.analyze_replay import analyze_summary, format_report
from tools.episode_2.config_episodes import EPISODE_CONFIG_1M, EPISODE_CONFIG_5M
from tools.episode_2.core import EpisodeConfig
from tools.episode_2.data_loader import fetch_bars
from tools.episode_2.episode_finder import find_episodes_v2
from tools.replay_stream import ReplayConfig, run_replay


def _to_ms(ts: pd.Timestamp) -> int:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.timestamp() * 1000)


async def _replay_for_window(
    symbol: str, start_ms: int, end_ms: int, *, interval: str = "5m", dump_dir: Path
) -> Path:
    cfg = ReplayConfig(
        symbol=symbol,
        interval=interval,
        source="binance",
        start_ms=start_ms,
        end_ms=end_ms,
        dump_dir=dump_dir,
    )
    stats = await run_replay(cfg)
    return stats.dump_dir / "summary.csv"


def _pick_config(symbol: str, timeframe: str) -> EpisodeConfig:
    """Повертає конфіг епізодів за символом/таймфреймом.

    Args:
        symbol: Символ, напр. "TONUSDT".
        timeframe: Таймфрейм ("1m" або "5m").

    Returns:
        EpisodeConfig: Об'єкт конфігурації епізодів.

    Raises:
        SystemExit: Якщо таймфрейм не підтримується.
    """
    tf = timeframe.lower().strip()
    if tf == "1m":
        cfg_map = dict(EPISODE_CONFIG_1M)
    elif tf == "5m":
        cfg_map = dict(EPISODE_CONFIG_5M)
    else:
        raise SystemExit(f"Непідтримуваний таймфрейм: {timeframe}")
    cfg_map["symbol"] = symbol.upper()
    cfg_map["timeframe"] = tf
    return EpisodeConfig(**cfg_map)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Епізодні реплеї Stage1→Stage2 для обраного символу/таймфрейму."
        )
    )
    parser.add_argument("--symbol", default="TONUSDT", help="Символ, напр. TONUSDT")
    parser.add_argument(
        "--timeframe",
        default="5m",
        choices=["1m", "5m"],
        help="Таймфрейм свічок для пошуку епізодів",
    )
    parser.add_argument(
        "--dump-dir",
        default="./replay_dump_ep",
        help="Каталог базовий для результатів",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    # 1) Config → DF
    cfg = _pick_config(args.symbol, args.timeframe)
    df = fetch_bars(cfg.symbol, cfg.timeframe, cfg.limit)
    if df.empty:
        raise SystemExit("Порожній DataFrame для епізодів")

    # 2) Episodes
    episodes = find_episodes_v2(
        df,
        move_pct_up=cfg.move_pct_up,
        move_pct_down=cfg.move_pct_down,
        min_bars=cfg.min_bars,
        max_bars=cfg.max_bars,
        close_col=cfg.close_col,
        retrace_ratio=cfg.retrace_ratio,
        require_retrace=cfg.require_retrace,
        min_gap_bars=cfg.min_gap_bars,
        merge_adjacent=cfg.merge_adjacent,
        max_episodes=10,
        config_context=cfg,
    )
    if not episodes:
        raise SystemExit("Епізоди не знайдені")
    episodes_sorted = sorted(episodes, key=lambda e: abs(e.move_pct), reverse=True)
    pick = episodes_sorted[:2]

    # 3) Windows for replay
    pre_bars = 20
    post_bars = 20
    tasks = []
    base_dump = Path(args.dump_dir)
    base_dump.mkdir(parents=True, exist_ok=True)

    for i, ep in enumerate(pick, 1):
        start_idx = max(0, ep.start_idx - pre_bars)
        end_idx = min(len(df) - 1, ep.end_idx + post_bars)
        start_ms = _to_ms(df.index[start_idx])
        end_ms = _to_ms(df.index[end_idx])
        dump_dir = (
            base_dump
            / f"{cfg.symbol.lower()}_{cfg.timeframe}_ep{i}_{start_idx}_{end_idx}"
        )
        tasks.append(
            _replay_for_window(
                cfg.symbol, start_ms, end_ms, interval=cfg.timeframe, dump_dir=dump_dir
            )
        )

    # 4) Run replays sequentially (safer for rate limits)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results: list[Path] = []
    try:
        for coro in tasks:
            results.append(loop.run_until_complete(coro))
    finally:
        loop.close()

    # 5) Analyze reports
    for summary_path in results:
        analysis = analyze_summary(summary_path)
        print("\n=== REPLAY REPORT:")
        print(format_report(analysis, top_triggers=10))
if __name__ == "__main__":  # pragma: no cover
    main()
