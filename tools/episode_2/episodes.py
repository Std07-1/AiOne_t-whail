"""Minimal episode pipeline (спрощена версія).

За замовчуванням: система НІЧОГО не генерує – ви передаєте ВЖЕ сформовані зовнішньою логікою сигнали (або не передаєте зовсім).

Основні кроки:
    * Завантаження даних
    * (Опціонально) прийом зовнішніх сигналів через параметр `signals`
    * Побудова простої маски входів (1 сигнал на бар)
    * Пошук епізодів (`episode_finder.find_episodes_v2`)
    * Збереження снапшотів (три окремі LZ4 JSON: meta / episodes / signals; якщо доступний пакет lz4)
    * Спрощена візуалізація (close + епізоди + точки сигналів)

Додатковий режим (опція для бекстесту Stage1):
    * `generate_system_signals` – проходить по історії ковзним вікном і викликає Stage1 `AssetMonitorStage1.check_anomalies`.
      НЕ активується автоматично; лише якщо викликати `run_pipeline(..., system_generate=True)`.

Видалено: adaptive/dynamic thresholds, coverage, кластеризація, фічі, audit / tuning / debug.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from ep_2.config_episodes import EPISODE_CONFIG_1M, EPISODE_CONFIG_5M
from ep_2.core import Episode, EpisodeConfig
from ep_2.data_loader import load_bars_auto
from ep_2.episode_finder import find_episodes_v2

try:  # visualization optional
    from ep_2.visualize_episodes import visualize_episodes
except Exception:  # pragma: no cover

    def visualize_episodes(*args, **kwargs):  # type: ignore
        pass


logger = logging.getLogger("ep2.episodes")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    h = logging.StreamHandler()
    h.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(h)
    logger.propagate = False


def build_entry_mask(df: pd.DataFrame, signals: Optional[pd.DataFrame]) -> pd.Series:
    if signals is None or signals.empty:
        return pd.Series(False, index=df.index, dtype=bool)
    s = signals.copy()
    if "ts" in s.columns:
        s["ts"] = pd.to_datetime(s["ts"], utc=True, errors="coerce")
        s = s.dropna(subset=["ts"])
    if s.empty:
        return pd.Series(False, index=df.index, dtype=bool)
    s = s.sort_values(["ts"]).drop_duplicates(subset=["ts"], keep="first")
    mask = pd.Series(False, index=df.index, dtype=bool)
    if isinstance(df.index, pd.DatetimeIndex):
        common = df.index.intersection(s["ts"])  # align on exact timestamp
        if len(common):
            mask.loc[common] = True
    return mask


# ---------------------------------------------------------------------------
# OPTIONAL Stage1 backtest signal generation (NOT used by default)
# ---------------------------------------------------------------------------
try:  # ізолюємо імпорти, щоб не ламати мінімальний режим якщо Stage1 недоступний
    from app.asset_state_manager import AssetStateManager  # type: ignore
    from config.config import STAGE1_MONITOR_PARAMS  # type: ignore
    from stage1.asset_monitoring import AssetMonitorStage1  # type: ignore
except Exception:  # pragma: no cover
    AssetMonitorStage1 = None  # type: ignore
    AssetStateManager = None  # type: ignore
    STAGE1_MONITOR_PARAMS = {}  # type: ignore


def generate_system_signals(
    df: pd.DataFrame,
    symbol: str = "BTCUSDT",
    *,
    lookback: int = 120,
    alerts_only: bool = True,
    progress_every: int = 1000,
) -> pd.DataFrame:
    """Емуляція реального часу для Stage1 по історичних барах (опціонально).

    ВАЖЛИВО: Основний дизайн – прийом зовнішніх сигналів. Ця функція – лише допоміжний
    інструмент для порівняння/бектесту Stage1. Повертає DataFrame зі стовпцями
    [ts, price, side, type, reasons].
    """
    if AssetMonitorStage1 is None or AssetStateManager is None:
        raise RuntimeError(
            "Stage1 компоненти недоступні (можливо видалені або неінстальовані)"
        )
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("df must have DatetimeIndex")
    req = {"close", "high", "low", "volume"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Відсутні колонки для Stage1 backtest: {miss}")
    state_manager = AssetStateManager([symbol])
    monitor = AssetMonitorStage1(
        cache_handler=None,
        state_manager=state_manager,
        **{
            k: v
            for k, v in STAGE1_MONITOR_PARAMS.items()
            if not isinstance(v, (dict, list))
        },
    )
    rows: List[Dict[str, Any]] = []
    total = len(df)
    import asyncio

    for i in range(lookback, total):
        window = df.iloc[i - lookback : i + 1]
        try:

            async def _run():
                return await monitor.check_anomalies(symbol, window.copy())

            res = asyncio.run(_run())
            if not res:
                continue
            sig = res.get("signal")
            if alerts_only and sig != "ALERT":
                continue
            rows.append(
                {
                    "ts": window.index[-1],
                    "price": float(window["close"].iloc[-1]),
                    "side": sig,
                    "type": "SYSTEM_SIGNAL",
                    "reasons": res.get("trigger_reasons", []),
                }
            )
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Stage1-backtest] error i={i}: {e}")
        if progress_every and (i % progress_every == 0):
            logger.info(
                "[Stage1-backtest] progress %.1f%% (%d/%d)", i / total * 100, i, total
            )
    if not rows:
        return pd.DataFrame(columns=["ts", "price", "side", "type", "reasons"])
    out = pd.DataFrame(rows).sort_values("ts").reset_index(drop=True)
    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    return out


__all_optional = ["generate_system_signals"]


def episodes_to_df(episodes: list[Episode]) -> pd.DataFrame:
    if not episodes:
        return pd.DataFrame(columns=["start_idx", "end_idx", "direction", "move_pct"])
    rows = []
    for e in episodes:
        rows.append(
            {
                "start_idx": e.start_idx,
                "end_idx": e.end_idx,
                "direction": getattr(e, "direction", None),
                "move_pct": getattr(e, "move_pct", None),
            }
        )
    return pd.DataFrame(rows)


def run_pipeline(
    config_dict: Dict[str, Any],
    *,
    signals: Optional[pd.DataFrame] = None,
    save_snapshots: bool = False,
    visualize: bool = True,
    system_generate: bool = False,
    system_symbol: Optional[str] = None,
    system_lookback: int = 120,
) -> Dict[str, Any]:
    """Основний мінімальний сценарій.

    Parameters
    ----------
    config_dict : dict
        Параметри для `EpisodeConfig`.
    save_snapshots : bool
        Зберігати три LZ4 JSON файли (meta, episodes, signals). Потрібен пакет lz4.
    visualize : bool
        Побудувати простий графік (якщо доступний matplotlib).
    """
    # Ігноруємо спискові альтернативи (alt варіанти) якщо вони були у повнішій версії
    cfg = EpisodeConfig(
        **{k: v for k, v in config_dict.items() if not isinstance(v, list)}
    )
    t0 = time.perf_counter()
    logger.info(
        "[pipeline] start symbol=%s tf=%s limit=%s",
        cfg.symbol,
        cfg.timeframe,
        cfg.limit,
    )
    df = load_bars_auto(cfg)
    t_load = time.perf_counter()
    if not isinstance(df.index, pd.DatetimeIndex):  # захист
        raise ValueError("Очікується DatetimeIndex у DataFrame")

    # 1) ЗОВНІШНІ сигнали (основний варіант)
    if signals is not None:
        logger.info(
            "[pipeline] external signals received rows=%d cols=%s",
            len(signals),
            list(signals.columns),
        )
        signals_df = signals.copy()
    # 2) Опціональний бекстест Stage1
    elif system_generate:
        sym1 = system_symbol or cfg.symbol
        try:
            signals_df = generate_system_signals(
                df, symbol=sym1, lookback=system_lookback
            )
            logger.info(
                "[pipeline] system_generate=True produced %d signals", len(signals_df)
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("[pipeline] system_generate failed: %s", e)
            signals_df = None
    else:
        logger.info("[pipeline] no external signals provided and system_generate=False")
        signals_df = None
    t_signals = time.perf_counter()
    entry_mask = build_entry_mask(df, signals_df)

    # Діагностика індексу та базових цін перед пошуком епізодів
    try:
        _index_diagnostics(df)
        _price_diagnostics(df)
    except Exception as _e_diag:  # pragma: no cover
        logger.warning("[pipeline] diagnostics error: %s", _e_diag)

    # Пошук епізодів (оновлений виклик – передаємо DataFrame, а не Series)
    episodes = find_episodes_v2(
        df,
        move_pct_up=cfg.move_pct_up,
        move_pct_down=cfg.move_pct_down,
        min_bars=cfg.min_bars,
        max_bars=cfg.max_bars,
        close_col=cfg.close_col,
        retrace_ratio=cfg.retrace_ratio,
        require_retrace=cfg.require_retrace,
        min_gap_bars=cfg.min_gap_bars if isinstance(cfg.min_gap_bars, int) else 0,
        merge_adjacent=cfg.merge_adjacent,
        max_episodes=cfg.max_episodes,
        config_context=cfg,
    )
    t_episodes = time.perf_counter()
    logger.info("[pipeline] episodes=%d", len(episodes))

    # Snapshots (опціонально, три окремі LZ4 JSON)
    if save_snapshots:
        out_dir = Path("results/")
        out_dir.mkdir(parents=True, exist_ok=True)
        has_lz4 = importlib.util.find_spec("lz4") is not None
        if not has_lz4:
            logger.info("Пропуск збереження (нема пакету lz4: pip install lz4)")
        else:
            try:  # pragma: no cover
                import lz4.frame  # type: ignore

                def _json_default(o):  # локальний серіалайзер
                    if isinstance(o, (pd.Timestamp, datetime)):
                        # приводимо до UTC ISO8601
                        if isinstance(o, pd.Timestamp):
                            if o.tzinfo is None:
                                o = o.tz_localize("UTC")
                            return (
                                o.tz_convert("UTC").isoformat().replace("+00:00", "Z")
                            )
                        return o.isoformat() + "Z"
                    if isinstance(o, (np.integer,)):
                        return int(o)
                    if isinstance(o, (np.floating,)):
                        return float(o)
                    if isinstance(o, (np.ndarray,)):
                        return o.tolist()
                    return str(o)

                def _write_lz4(obj: dict, name: str) -> None:
                    data_bytes = json.dumps(
                        obj, ensure_ascii=False, default=_json_default
                    ).encode("utf-8")
                    out_file = out_dir / name
                    with lz4.frame.open(out_file, mode="wb", compression_level=9) as f:
                        f.write(data_bytes)
                    logger.info(
                        "[snapshot] %s size=%.1fKB",
                        out_file.name,
                        len(data_bytes) / 1024,
                    )

                base = f"{cfg.symbol}_{cfg.timeframe}"
                meta = {
                    "symbol": cfg.symbol,
                    "timeframe": cfg.timeframe,
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                    "counts": {
                        "episodes": len(episodes),
                        "signals": 0 if signals_df is None else int(len(signals_df)),
                        "bars": len(df),
                    },
                    "params": {
                        "move_pct_up": cfg.move_pct_up,
                        "move_pct_down": cfg.move_pct_down,
                        "min_bars": cfg.min_bars,
                        "max_bars": cfg.max_bars,
                        "retrace_ratio": cfg.retrace_ratio,
                        "require_retrace": cfg.require_retrace,
                    },
                }
                episodes_payload = {
                    "episodes": episodes_to_df(episodes).to_dict(orient="records")
                }
                if signals_df is None or signals_df.empty:
                    signals_list: list[dict[str, Any]] = []
                else:
                    s = signals_df.copy().reset_index(drop=True)
                    if "ts" in s.columns:
                        # Перевести у ISO строки для надійності серіалізації
                        s["ts"] = pd.to_datetime(s["ts"], utc=True, errors="coerce")
                        s["ts"] = (
                            s["ts"]
                            .dt.tz_convert("UTC")
                            .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                        )
                    signals_list = s.to_dict(orient="records")
                signals_payload = {"signals": signals_list}
                _write_lz4(meta, f"meta_{base}.json.lz4")
                _write_lz4(episodes_payload, f"episodes_{base}.json.lz4")
                _write_lz4(signals_payload, f"signals_{base}.json.lz4")
            except Exception as e:
                logger.warning("Не вдалось зберегти LZ4 snapshots: %s", e)

    # Візуалізація (опціональна)
    if visualize:
        try:  # pragma: no cover
            visualize_episodes(df, episodes, signals_df, entry_mask)
        except Exception as e:  # noqa: BLE001
            logger.warning("Візуалізація не вдалася: %s", e)
    t_end = time.perf_counter()
    if logger.isEnabledFor(logging.INFO):
        logger.info(
            "[pipeline] timings: load=%.1fms signals=%.1fms episodes=%.1fms total=%.1fms",
            (t_signals - t_load) * 1000,
            (t_episodes - t_signals) * 1000,
            (t_end - t_episodes) * 1000,
            (t_end - t0) * 1000,
        )

        logger.debug(
            "[pipeline] df_rows=%d ext_signals=%d entry_true=%d first_ts=%s last_ts=%s",
            len(df),
            0 if signals_df is None else len(signals_df),
            int(entry_mask.sum()),
            df.index[0] if len(df) else None,
            df.index[-1] if len(df) else None,
        )
        if episodes:
            logger.debug(
                "[pipeline] sample_episode=%s",
                episodes_to_df(episodes).head(1).to_dict(orient="records"),
            )
        if signals_df is not None and not signals_df.empty:
            logger.debug(
                "[pipeline] sample_signals=%s",
                signals_df.head(3).to_dict(orient="records"),
            )

    return {
        "config": cfg,
        "df": df,
        "signals": signals_df,
        "entry_mask": entry_mask,
        "episodes": episodes,
    }


def main():  # pragma: no cover
    # Можна швидко переключати між 1m / 5m
    configs = [EPISODE_CONFIG_5M]
    for cfg in configs:
        # Варіант 1: чисто зовнішні або без сигналів
        run_pipeline(cfg, signals=None, save_snapshots=True, visualize=True)
        # Варіант 2 (закоментовано): емулювати Stage1 і отримати системні сигнали
        # run_pipeline(cfg, system_generate=True, system_lookback=150, visualize=True)


if __name__ == "__main__":  # pragma: no cover
    main()


# ---------------- Diagnostics Helpers -----------------
def _index_diagnostics(df: pd.DataFrame) -> None:
    idx = df.index
    monotonic = idx.is_monotonic_increasing
    tz = getattr(idx, "tz", None) is not None
    dup_count = int(idx.duplicated().sum())
    if len(idx) > 1:
        diffs = (idx[1:] - idx[:-1]).astype("timedelta64[s]").astype(int)
        if len(diffs):
            import numpy as _np

            base = int(pd.Series(diffs).mode().iloc[0]) if len(diffs) else 0
            max_gap_s = int(diffs.max()) if len(diffs) else 0
            nonstandard = int((diffs != base).sum()) if base > 0 else 0
            nonstandard_pct = (nonstandard / len(diffs) * 100) if len(diffs) else 0.0
            max_gap_mult = (max_gap_s / base) if base > 0 else 0.0
        else:
            base = max_gap_s = nonstandard = 0
            nonstandard_pct = max_gap_mult = 0.0
    else:
        base = max_gap_s = nonstandard = 0
        nonstandard_pct = max_gap_mult = 0.0
    logger.debug(
        "[index] monotonic=%s tz_aware=%s duplicates=%d base_interval_s=%s nonstandard_gaps=%d(%.3f%%) max_gap_mult=%.2f",
        monotonic,
        tz,
        dup_count,
        base,
        nonstandard,
        nonstandard_pct,
        max_gap_mult,
    )
    if (not monotonic) or dup_count > 0:
        logger.warning(
            "[index] integrity issue: monotonic=%s duplicates=%d", monotonic, dup_count
        )
    if nonstandard_pct > 1.0:
        logger.info(
            "[index] notice: %.2f%% non-standard gaps (base=%ss) max_gap_mult=%.2f",
            nonstandard_pct,
            base,
            max_gap_mult,
        )


def _price_diagnostics(df: pd.DataFrame) -> None:
    if "close" not in df.columns:
        return
    closes = pd.to_numeric(df["close"], errors="coerce")
    zero_ct = int((closes == 0).sum())
    neg_ct = int((closes < 0).sum())
    rets = closes.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    ret_std = float(rets.std()) if len(rets) else 0.0
    ret_mean_abs = float(rets.abs().mean()) if len(rets) else 0.0
    logger.debug(
        "[price] zero_close=%d neg_close=%d ret_std=%.6f ret_mean_abs=%.6f",
        zero_ct,
        neg_ct,
        ret_std,
        ret_mean_abs,
    )
