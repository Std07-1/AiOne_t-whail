import argparse
import asyncio
import math
import re
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from data.redis_connection import acquire_redis, release_redis
from data.unified_store import StoreConfig, UnifiedDataStore


def _parse_ts_ms(ln: str) -> int | None:
    """Парсить timestamp (ms) з лог-рядка: підтримує ts=epoch(10/13), ISO '...Z' або [MM/DD/YY HH:MM:SS]."""
    # ts=1681234567890 або ts=1681234567
    m = re.search(r"\bts=(\d{10,13})\b", ln)
    if m:
        val = m.group(1)
        try:
            ts = int(val)
            # seconds → ms
            if len(val) == 10:
                ts *= 1000
            return ts
        except Exception:
            return None
    # ISO 8601: 2025-11-04T12:34:56Z (UTC)
    m2 = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)", ln)
    if m2:
        try:
            dt = datetime.strptime(m2.group(1), "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=UTC
            )
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
    # Бракетований формат: [MM/DD/YY HH:MM:SS]
    m3 = re.search(r"\[(\d{2})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})\]", ln)
    if m3:
        try:
            mm, dd, yy, HH, MM, SS = map(int, m3.groups())
            # yy у форматі 25 → 2025
            year = 2000 + yy
            dt = datetime(year, mm, dd, HH, MM, SS, tzinfo=UTC)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
    return None


def _parse_symbol(ln: str) -> str | None:
    """Парсить символ із лог-рядка. Спершу symbol=..., інакше шукаємо токен типу .*USDT.

    Друга гілка нечутлива до регістру (btcusdt → BTCUSDT).
    """
    m = re.search(r"\bsymbol=([A-Za-z0-9_:\-/.]+)", ln, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # Ev: BTCUSDT, ETHUSDT тощо (case-insensitive)
    m2 = re.search(r"\b([A-Za-z0-9]{3,}USDT)\b", ln, flags=re.IGNORECASE)
    if m2:
        return m2.group(1).upper()
    return None


async def _load_dataframes(
    symbols: list[str], interval: str = "1m", limit: int = 6000
) -> dict[str, pd.DataFrame]:
    """Завантажує OHLCV для набору символів з UnifiedDataStore (RAM/Redis/Disk)."""
    client = await acquire_redis()
    try:
        uds = UnifiedDataStore(redis=client, cfg=StoreConfig())
        out: dict[str, pd.DataFrame] = {}
        for sym in sorted(set(symbols)):
            try:
                df = await uds.get_df(sym, interval, limit=limit)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    out[sym] = df
            except Exception:
                # best-effort: пропускаємо, якщо Redis недоступний або даних немає
                continue
        return out
    finally:
        await release_redis(client)


def _find_bar_index(df: pd.DataFrame, ts_ms: int) -> int | None:
    """Знаходить індекс бара за ts: беремо бар, чий open_time <= ts < next.open_time."""
    try:
        ot = pd.to_numeric(df["open_time"], errors="coerce").astype("Int64")
    except Exception:
        return None
    if ot.isna().all():
        return None
    # позиція останнього ot <= ts_ms
    arr = ot.astype("int64").to_numpy()
    import numpy as np

    pos = int(np.searchsorted(arr, ts_ms, side="right") - 1)
    if 0 <= pos < len(df):
        return pos
    return None


def _agree_sign(bias: float, ret: float) -> bool:
    if not (math.isfinite(bias) and math.isfinite(ret)):
        return False
    if ret == 0.0:
        return False
    return (bias > 0 and ret > 0) or (bias < 0 and ret < 0)


async def run(a: argparse.Namespace) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--log", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--k", nargs="+", type=int, required=True)
    p.add_argument("--presence-min", type=float, default=0.75)
    p.add_argument("--bias-abs-min", type=float, default=0.6)
    p.add_argument("--whale-max-age-sec", type=int, default=600)
    # Якщо скрипт викликаний через main(), a вже розпарсено вище; залишимо p для довідки

    alerts: list[dict[str, Any]] = []
    # Зберігаємо останні [STRICT_WHALE] за символом: ts_ms, presence, bias
    last_whale: dict[str, dict[str, Any]] = {}
    # Поточний ts з логів (оновлюється при зустрічі будь-якого розпізнаного часу)
    current_ts_ms: int | None = None
    # Лічильники для QA
    whales_seen = 0
    alerts_seen = 0
    with open(a.log, encoding="utf-8", errors="ignore") as f:
        for ln in f:
            # Оновлюємо контекстний час (наприклад, з рядків виду [MM/DD/YY HH:MM:SS])
            ts_any = _parse_ts_ms(ln)
            if ts_any is not None:
                current_ts_ms = ts_any
            # Захоплюємо WHALE-рядки
            m_wh = re.search(
                r"\[STRICT_WHALE\].*presence=([0-9.]+).*bias=([\-0-9.]+)", ln
            )
            if m_wh:
                sym_wh = _parse_symbol(ln)
                ts_wh = _parse_ts_ms(ln) or current_ts_ms
                if sym_wh and ts_wh:
                    try:
                        presence_wh = float(m_wh.group(1))
                        bias_wh = float(m_wh.group(2))
                    except Exception:
                        presence_wh = float("nan")
                        bias_wh = float("nan")
                    last_whale[sym_wh] = {
                        "ts_ms": int(ts_wh),
                        "presence": presence_wh,
                        "bias": bias_wh,
                    }
                    whales_seen += 1

            # Витягуємо активації сценаріїв
            if "[SCENARIO_ALERT]" in ln and re.search(r"\bactivate\b", ln):
                sym_al = _parse_symbol(ln)
                ts_al = _parse_ts_ms(ln) or current_ts_ms
                if not (sym_al and ts_al):
                    continue
                alerts_seen += 1
                w = last_whale.get(sym_al)
                if not w:
                    continue
                # Перевіряємо, що whale достатньо свіжий
                if int(ts_al) < int(w.get("ts_ms", 0)):
                    # На випадок невідсортованих логів — пропускаємо, якщо майбутнє
                    continue
                age_ms = int(ts_al) - int(w.get("ts_ms", 0))
                if age_ms > int(a.whale_max_age_sec) * 1000:
                    continue
                presence = float(w.get("presence", float("nan")))
                bias = float(w.get("bias", float("nan")))
                if not (math.isfinite(presence) and math.isfinite(bias)):
                    continue
                if presence >= a.presence_min and abs(bias) >= a.bias_abs_min:
                    alerts.append(
                        {
                            "symbol": sym_al.upper(),
                            "ts_ms": int(ts_al),
                            "bias": bias,
                            "presence": presence,
                        }
                    )

    # Якщо не вдалось витягнути символ/час — немає сенсу рахувати
    if not alerts:
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(
                f"# Forward filtered (presence>={a.presence_min:.2f} & |bias|>={a.bias_abs_min:.2f})\n"
                f"# whales_seen={whales_seen} alerts_seen={alerts_seen} alerts_matched=0\n\n"
            )
            for k in a.k:
                f.write(f"K={k}: N=0 hit≈nan\n")
        return

    symbols = [al["symbol"] for al in alerts if "symbol" in al]
    dfs = await _load_dataframes(symbols, interval="1m", limit=6000)

    results: list[dict[str, Any]] = [{"k": int(k), "agree": 0, "n": 0} for k in a.k]

    for al in alerts:
        sym = al["symbol"]
        ts_ms = int(al["ts_ms"])  # at or within bar
        bias = float(al["bias"])
        df = dfs.get(sym)
        if df is None or df.empty:
            continue
        idx = _find_bar_index(df, ts_ms)
        if idx is None:
            continue
        try:
            c0 = float(df["close"].iloc[idx])
        except Exception:
            continue
        if not (math.isfinite(c0) and c0 > 0):
            continue
        for r in results:
            k = int(r["k"])  # bars ahead
            j = idx + k
            if 0 <= j < len(df):
                try:
                    ck = float(df["close"].iloc[j])
                except Exception:
                    continue
                if not math.isfinite(ck):
                    continue
                ret = (ck / c0) - 1.0
                r["n"] += 1
                if _agree_sign(bias, ret):
                    r["agree"] += 1

    with open(a.out, "w", encoding="utf-8") as f:
        f.write(
            f"# Forward filtered (presence>={a.presence_min:.2f} & |bias|>={a.bias_abs_min:.2f})\n"
            f"# whales_seen={whales_seen} alerts_seen={alerts_seen} alerts_matched={len(alerts)}\n\n"
        )
        for r in results:
            hit = (r["agree"] / r["n"]) if r["n"] else float("nan")
            f.write(f"K={r['k']}: N={r['n']} hit≈{hit:.2f}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--k", nargs="+", type=int, required=True)
    parser.add_argument("--presence-min", type=float, default=0.75)
    parser.add_argument("--bias-abs-min", type=float, default=0.6)
    parser.add_argument("--whale-max-age-sec", type=int, default=600)
    args = parser.parse_args()
    asyncio.run(run(args))
