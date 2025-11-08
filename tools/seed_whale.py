"""One-shot whale.v2 seeder for Redis.

Призначення:
- Одноразово записати whale‑пейлоуди у Redis під ключами ai_one:whale:{SYMBOL_UPPER}:1m
- Формат значення відповідає очікуваному споживачем (whale.v2):
  {"ts": int(ms), "presence_score": float, "bias": float, "vwap_deviation": float,
   "dominance": {"buy": bool, "sell": bool}}

Джерело даних: локальні снапшоти у ./datastore/*_bars_1m_snapshot.jsonl

Використання (Windows PowerShell):
  .\.venv\Scripts\python.exe -m tools.seed_whale --symbols BTCUSDT,ETHUSDT,TONUSDT,SNXUSDT --ttl 1200

Примітка:
- Символ у ключі лишається UPPERCASE, щоби відповідати lookup у пайплайні.
- TTL задавайте із запасом ≥900с, щоб уникати stale під час канарейки.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from app.settings import settings
from config.config import NAMESPACE
from data.unified_store import k as build_key_raw
from utils.utils import sanitize_ohlcv_numeric

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore[assignment]


logger = logging.getLogger("tools.seed_whale")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

DATASTORE_DIR = Path("./datastore")


def _parse_symbols(s: str) -> list[str]:
    return [x.strip().upper() for x in s.split(",") if x.strip()]


def _load_m1_df(symbol: str) -> pd.DataFrame:
    fname = f"{symbol.lower()}_bars_1m_snapshot.jsonl"
    path = DATASTORE_DIR / fname
    if not path.exists():
        raise FileNotFoundError(f"Відсутній снапшот: {path}")
    df = pd.read_json(path, lines=True)
    if "timestamp" in df.columns and "open_time" not in df.columns:
        df = df.rename(columns={"timestamp": "open_time"})
    cols = ["open_time", "open", "high", "low", "close", "volume"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Відсутні колонки у {path}: {missing}")
    df = df[cols].copy()
    df = sanitize_ohlcv_numeric(df)
    return df


def _rolling_vwap(close: pd.Series, volume: pd.Series, window: int = 60) -> pd.Series:
    price_vol = close * volume
    num = price_vol.rolling(window, min_periods=max(3, window // 3)).sum()
    den = volume.rolling(window, min_periods=max(3, window // 3)).sum()
    with np.errstate(divide="ignore", invalid="ignore"):
        vwap = num / den
    return vwap.bfill().ffill()


def build_whale_v2(df_m1: pd.DataFrame) -> dict:
    tail = df_m1.tail(200).copy()
    close = tail["close"].astype(float)
    vol = tail["volume"].astype(float)
    vwap = _rolling_vwap(close, vol, window=60)
    vdev = float(
        ((close.iloc[-1] - vwap.iloc[-1]) / vwap.iloc[-1]) if vwap.iloc[-1] else 0.0
    )
    # bias ∈ [-1,1] через поріг 2%
    mag = min(1.0, abs(vdev) / 0.02) if np.isfinite(vdev) else 0.0
    bias = mag if vdev > 0 else (-mag if vdev < 0 else 0.0)
    # presence_score: простий комбінатор z(volume) та |vdev|
    zv = (vol - vol.mean()) / (vol.std(ddof=0) if vol.std(ddof=0) else 1.0)
    z_last = float(zv.iloc[-1]) if np.isfinite(zv.iloc[-1]) else 0.0
    presence = max(0.0, min(1.0, 0.2 + 0.15 * z_last + 6.0 * abs(vdev)))
    # dominance евристика: за знаком vdev і мін. модулем
    dom_gate = 0.01
    dominance = {
        "buy": bool(vdev >= dom_gate),
        "sell": bool(vdev <= -dom_gate),
    }
    now_ms = int(time.time() * 1000)
    return {
        "version": "v2",
        "ts": now_ms,
        "presence_score": float(presence),
        "bias": float(bias),
        "vwap_deviation": float(vdev),
        "dominance": dominance,
    }


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="One-shot whale.v2 seeder")
    p.add_argument(
        "--symbols",
        required=True,
        help="Кома‑список символів: BTCUSDT,ETHUSDT,TONUSDT,SNXUSDT",
    )
    p.add_argument(
        "--namespace",
        required=False,
        help=(
            "Необов'язково: явний namespace для ключів Redis (за замовчуванням береться з STATE_NAMESPACE або 'ai_one')."
        ),
    )
    p.add_argument(
        "--ttl", type=int, default=1200, help="TTL для ai_one:whale:KEY, сек (>=900)"
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_cli().parse_args(argv)
    symbols = _parse_symbols(args.symbols)
    ttl = max(1, int(args.ttl))
    # Визначаємо namespace: явний з CLI або з конфігурації
    namespace = (
        str(args.namespace).strip(":")
        if getattr(args, "namespace", None)
        else NAMESPACE
    )
    logger.info(
        "[WHALE_SEED] Символи: %s; TTL=%ds; namespace=%s",
        ", ".join(symbols),
        ttl,
        namespace,
    )
    if redis is None:
        logger.error("[WHALE_SEED] Бібліотека redis недоступна")
        return 2
    try:
        r = redis.StrictRedis(
            host=settings.redis_host,
            port=settings.redis_port,
            decode_responses=True,
        )
    except Exception as e:  # pragma: no cover
        logger.error("[WHALE_SEED] Не вдалося підключитися до Redis: %s", e)
        return 3

    ok = 0
    for sym in symbols:
        try:
            df = _load_m1_df(sym)
            payload = build_whale_v2(df)
            whale_key = build_key_raw(namespace, "whale", sym.upper(), "1m")
            j = json.dumps(payload, ensure_ascii=False)
            # setex: атомарний set+expire
            setex = getattr(r, "setex", None)
            if callable(setex):
                setex(whale_key, ttl, j)
            else:  # pragma: no cover
                r.set(whale_key, j)
                try:
                    r.expire(whale_key, ttl)
                except Exception:
                    pass
            logger.info("[WHALE_SEED] OK %s → %s", sym, whale_key)
            ok += 1
        except Exception as e:
            logger.warning("[WHALE_SEED] %s: помилка сідингу: %s", sym, e)

    logger.info("[WHALE_SEED] Готово: %d/%d", ok, len(symbols))
    return 0 if ok == len(symbols) else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
