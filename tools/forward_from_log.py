import argparse
import asyncio
import math
import re
from datetime import UTC, datetime
from pathlib import Path  # noqa: F401 - зарезервовано для майбутнього dedup
from typing import Any

import pandas as pd

from data.redis_connection import acquire_redis, release_redis
from data.unified_store import StoreConfig, UnifiedDataStore

try:  # noqa: WPS433 - динамічний імпорт під фіче-флаг
    from config.flags import FORWARD_SOFT_THRESH, FORWARD_SOFT_THRESH_ENABLED
except Exception:  # pragma: no cover
    FORWARD_SOFT_THRESH_ENABLED = False
    FORWARD_SOFT_THRESH = {}

# Пороги для обчислення time-to-fulfil (у відносних відсотках)
TTF_THRESHOLDS: dict[str, float] = {"ttf05": 0.005, "ttf10": 0.010}


def _parse_ts_ms(ln: str, *, strict: bool = False) -> int | None:
    """Парсить timestamp (ms) з лог-рядка: підтримує ts=epoch(10/13), ISO '...Z' або [MM/DD/YY HH:MM:SS].

    Коли strict=True — у випадку відсутності коректного часу піднімає ValueError.
    """
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
            mm, dd, yy, hh, mm_time, ss = map(int, m3.groups())
            # yy у форматі 25 → 2025
            year = 2000 + yy
            dt = datetime(year, mm, dd, hh, mm_time, ss, tzinfo=UTC)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
    if strict:
        raise ValueError("timestamp not found")
    return None


def _parse_symbol(ln: str, *, strict: bool = False) -> str | None:
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
    if strict:
        raise ValueError("symbol not found")
    return None


def _dedup_key(symbol: str, ts_ms: int, bias: float) -> str:
    """Ключ для дедупа: SYMBOL|ts_ms|(+|-)"""
    dir_token = "+" if bias > 0 else "-"
    return f"{symbol.upper()}|{int(ts_ms)}|{dir_token}"


def _passes_thresholds(
    presence: float, bias: float, presence_min: float, bias_abs_min: float
) -> bool:
    """Порогова перевірка для алерту (>= для presence і |bias|)."""
    if not (math.isfinite(presence) and math.isfinite(bias)):
        return False
    return presence >= presence_min and abs(bias) >= bias_abs_min


def _resolve_thresholds(
    source: str,
    presence_min: float | None,
    bias_abs_min: float | None,
    whale_max_age_sec: int | None,
    explain_ttl_sec: int | None,
) -> tuple[float, float, int, int, bool]:
    """Повертає (presence_min, bias_abs_min, whale_max_age_sec, explain_ttl_sec, soft_applied)."""
    # Жорсткі дефолти (історична поведінка)
    hard = {
        "presence_min": 0.75,
        "bias_abs_min": 0.60,
        "whale_max_age_sec": 600,
        "explain_ttl_sec": 600,
    }
    soft_applied = False
    # Якщо флаг увімкнений і користувач не задав явні значення — беремо м'які
    if FORWARD_SOFT_THRESH_ENABLED:
        prof = (
            FORWARD_SOFT_THRESH.get(source, {})
            if isinstance(FORWARD_SOFT_THRESH, dict)
            else {}
        )
        use_soft = (
            presence_min is None
            and bias_abs_min is None
            and (
                (source == "whale" and whale_max_age_sec is None)
                or (source == "explain" and explain_ttl_sec is None)
            )
        )
        if use_soft and prof:
            presence_min = prof.get("presence_min", presence_min)
            bias_abs_min = prof.get("bias_abs_min", bias_abs_min)
            if source == "whale":
                whale_max_age_sec = prof.get("whale_max_age_sec", whale_max_age_sec)
            else:
                explain_ttl_sec = prof.get("explain_ttl_sec", explain_ttl_sec)
            soft_applied = True
    # Заповнити відсутнє жорсткими дефолтами
    if presence_min is None:
        presence_min = hard["presence_min"]
    if bias_abs_min is None:
        bias_abs_min = hard["bias_abs_min"]
    if whale_max_age_sec is None:
        whale_max_age_sec = hard["whale_max_age_sec"]
    if explain_ttl_sec is None:
        explain_ttl_sec = hard["explain_ttl_sec"]
    return (
        float(presence_min),
        float(bias_abs_min),
        int(whale_max_age_sec),
        int(explain_ttl_sec),
        bool(soft_applied),
    )


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
    # Параметри (очікуємо, що a вже розпарсено у виклику)
    source = str(getattr(a, "source", "whale"))
    # Розв'язання порогів та TTL з урахуванням флагів і CLI (None — «не задано явно»)
    presence_min, bias_abs_min, whale_age_sec, explain_ttl_sec, soft_applied = (
        _resolve_thresholds(
            source=source,
            presence_min=getattr(a, "presence_min", None),
            bias_abs_min=getattr(a, "bias_abs_min", None),
            whale_max_age_sec=getattr(a, "whale_max_age_sec", None),
            explain_ttl_sec=getattr(a, "explain_ttl_sec", None),
        )
    )
    dedup_file = getattr(a, "dedup_file", None)
    max_lines = getattr(a, "max_lines", None)

    # Попередження про взаємовиключні параметри
    warnings: list[str] = []
    if source == "explain" and hasattr(a, "whale_max_age_sec"):
        msg = "ignore whale_max_age_sec for source=explain"
        warnings.append(msg)
        try:
            print(f"[forward_from_log] WARN: {msg}")
        except Exception:
            pass
    if source == "whale" and hasattr(a, "explain_ttl_sec"):
        msg = "ignore explain_ttl_sec for source=whale"
        warnings.append(msg)
        try:
            print(f"[forward_from_log] WARN: {msg}")
        except Exception:
            pass

    alerts: list[dict[str, Any]] = []
    # Зберігаємо останні [STRICT_WHALE] за символом: ts_ms, presence, bias
    last_whale: dict[str, dict[str, Any]] = {}
    # Зберігаємо останні [SCEN_EXPLAIN] за символом: ts_ms, presence, bias
    last_explain: dict[str, dict[str, Any]] = {}
    # Поточний ts з логів (оновлюється при зустрічі будь-якого розпізнаного часу)
    current_ts_ms: int | None = None
    # Лічильники для QA
    whales_seen = 0
    alerts_seen = 0
    ttl_rejected = 0
    skew_dropped_total = 0

    # Дедуп-стан між профілями (strong > soft > explain)
    dedup_keys: set[str] = set()
    dedup_path: Path | None = None
    if isinstance(dedup_file, str) and dedup_file.strip():
        dedup_path = Path(dedup_file)
        if dedup_path.exists():
            try:
                for ln in dedup_path.read_text(
                    encoding="utf-8", errors="ignore"
                ).splitlines():
                    ln = ln.strip()
                    if ln:
                        dedup_keys.add(ln)
            except Exception:
                pass
    dedup_dropped_total = 0
    lines_read = 0
    early_stop = False
    with open(a.log, encoding="utf-8", errors="ignore") as f:
        for ln in f:
            lines_read += 1
            if isinstance(max_lines, int) and max_lines > 0 and lines_read > max_lines:
                early_stop = True
                break
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

            # Захоплюємо SCEN_EXPLAIN-рядки (explain payload)
            if "[SCEN_EXPLAIN]" in ln:
                sym_ex = _parse_symbol(ln)
                ts_ex = _parse_ts_ms(ln) or current_ts_ms
                if sym_ex and ts_ex:
                    # explain="... presence=0.12 ... bias=-0.7 ..."
                    exp = None
                    m_exp = re.search(r"explain=\"([^\"]+)\"", ln)
                    if m_exp:
                        exp = m_exp.group(1)
                    presence_ex = float("nan")
                    bias_ex = float("nan")
                    if exp:
                        m_p = re.search(r"\bpresence=([\-0-9.]+)\b", exp)
                        if m_p:
                            try:
                                presence_ex = float(m_p.group(1))
                            except Exception:
                                presence_ex = float("nan")
                        m_b = re.search(r"\bbias=([\-0-9.]+)\b", exp)
                        if m_b:
                            try:
                                bias_ex = float(m_b.group(1))
                            except Exception:
                                bias_ex = float("nan")
                    # Fallback: інколи bias присутній поза explain=...
                    if not math.isfinite(bias_ex):
                        m_b2 = re.search(r"\bbias=([\-0-9.]+)\b", ln)
                        if m_b2:
                            try:
                                bias_ex = float(m_b2.group(1))
                            except Exception:
                                bias_ex = float("nan")
                    last_explain[sym_ex] = {
                        "ts_ms": int(ts_ex),
                        "presence": presence_ex,
                        "bias": bias_ex,
                    }

            # Витягуємо активації сценаріїв
            if "[SCENARIO_ALERT]" in ln and re.search(r"\bactivate\b", ln):
                sym_al = _parse_symbol(ln)
                ts_al = _parse_ts_ms(ln) or current_ts_ms
                if not (sym_al and ts_al):
                    continue
                alerts_seen += 1
                if source == "whale":
                    w = last_whale.get(sym_al)
                    if not w:
                        continue
                    # Перевіряємо, що whale достатньо свіжий
                    if int(ts_al) < int(w.get("ts_ms", 0)):
                        # На випадок невідсортованих логів — пропускаємо, якщо майбутнє
                        continue
                    age_ms = int(ts_al) - int(w.get("ts_ms", 0))
                    if age_ms > whale_age_sec * 1000:
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
                else:  # source=explain
                    e = last_explain.get(sym_al)
                    if not e:
                        continue
                    # TTL-перевірка explain
                    if int(ts_al) < int(e.get("ts_ms", 0)):
                        # clock-skew: алерт раніше explain → відсікати, рахувати
                        skew_dropped_total += 1
                        continue
                    age_ms = int(ts_al) - int(e.get("ts_ms", 0))
                    if age_ms > explain_ttl_sec * 1000:
                        ttl_rejected += 1
                        continue
                    presence = float(e.get("presence", float("nan")))
                    bias = float(e.get("bias", float("nan")))
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

    # Сортування та дедуп (ключ: SYMBOL|ts_ms|dir)
    alerts.sort(key=lambda d: int(d.get("ts_ms", 0)))
    filtered: list[dict[str, Any]] = []
    for al in alerts:
        sym = str(al.get("symbol", "")).upper()
        ts_ms = int(al.get("ts_ms", 0))
        bias = float(al.get("bias", 0.0))
        key = _dedup_key(sym, ts_ms, bias)
        if key in dedup_keys:
            dedup_dropped_total += 1
            continue
        dedup_keys.add(key)
        filtered.append(al)
    alerts = filtered

    # Якщо не вдалось витягнути символ/час — все одно створюємо звіт
    if not alerts:
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(
                f"# Forward filtered (presence>={presence_min:.2f} & |bias|>={bias_abs_min:.2f})\n"
            )
            # Додаємо службову нотатку для whale-джерела
            if source == "whale" and whales_seen == 0:
                f.write(f"# whales_seen=0 alerts_seen={alerts_seen} alerts_matched=0\n")
                f.write("# note: whales_seen=0 → N=0 (даних недостатньо)\n\n")
            else:
                f.write(
                    f"# whales_seen={whales_seen} alerts_seen={alerts_seen} alerts_matched=0\n\n"
                )
            for k in a.k:
                f.write(f"K={k}: N=0 hit≈nan\n")
            # Footer з метаданими (вікно/білд/ttf медіани як nan)
            f.write("\n# footer: ")
            import os

            git_sha = os.environ.get("GIT_SHA", "unknown")
            f.write(
                f"build={git_sha} | window=[-,-] | source={source} | params={{presence_min:{presence_min:.2f}, bias_abs_min:{bias_abs_min:.2f}}} | dedup_dropped={dedup_dropped_total} | N=0 | ttf05_median=nan | ttf10_median=nan | ttf_thresholds={TTF_THRESHOLDS}"
            )
            if source == "explain":
                ttl_ratio = (ttl_rejected / max(1, alerts_seen)) if alerts_seen else 0.0
                f.write(
                    f" | explain_ttl_sec={explain_ttl_sec} | ttl_rejected={ttl_rejected} | ttl_reject_ratio={ttl_ratio:.2f} | skew_dropped=0"
                )
            else:
                f.write(f" | whale_max_age_sec={whale_age_sec}")
            if soft_applied:
                f.write(" | note=soft_thresholds")
            if early_stop:
                f.write(" | note=early_stop")
            if warnings:
                f.write(" | warnings=" + ";".join(warnings))
            f.write("\n")
        return

    symbols = [al["symbol"] for al in alerts if "symbol" in al]
    dfs = await _load_dataframes(symbols, interval="1m", limit=6000)

    results: list[dict[str, Any]] = [{"k": int(k), "agree": 0, "n": 0} for k in a.k]
    # Колектори TTF
    ttf05_list: list[int] = []
    ttf10_list: list[int] = []
    max_k = max(int(k) for k in a.k) if a.k else 0

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
        # Обчислення TTF (0.5% та 1.0%) у барах для кожного кейсу
        ttf05: int | None = None
        ttf10: int | None = None
        if max_k > 0:
            for b in range(1, max_k + 1):
                j2 = idx + b
                if 0 <= j2 < len(df):
                    try:
                        c_b = float(df["close"].iloc[j2])
                    except Exception:
                        continue
                    if not math.isfinite(c_b) or c_b <= 0:
                        continue
                    rrel = abs((c_b / c0) - 1.0)
                    if ttf05 is None and rrel >= TTF_THRESHOLDS["ttf05"]:
                        ttf05 = b
                    if ttf10 is None and rrel >= TTF_THRESHOLDS["ttf10"]:
                        ttf10 = b
                    if ttf05 is not None and ttf10 is not None:
                        break
        if ttf05 is not None:
            ttf05_list.append(int(ttf05))
            # Prometheus бакети (best-effort)
            try:
                from tools.metrics_forward import inc_ttf05_bucket

                inc_ttf05_bucket(int(ttf05))
            except Exception:
                pass
        if ttf10 is not None:
            ttf10_list.append(int(ttf10))

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
            f"# Forward filtered (presence>={presence_min:.2f} & |bias|>={bias_abs_min:.2f})\n"
            f"# whales_seen={whales_seen} alerts_seen={alerts_seen} alerts_matched={len(alerts)}\n\n"
        )
        for r in results:
            hit = (r["agree"] / r["n"]) if r["n"] else float("nan")
            f.write(f"K={r['k']}: N={r['n']} hit≈{hit:.2f}\n")
        # Footer з метаданими
        total_n = sum(r["n"] for r in results)
        f.write("\n# footer: ")

        # Медіани TTF
        def _median(vals: list[int]) -> float:
            if not vals:
                return float("nan")
            s = sorted(vals)
            m = len(s) // 2
            if len(s) % 2 == 1:
                return float(s[m])
            return float((s[m - 1] + s[m]) / 2.0)

        ttf05_med = _median(ttf05_list)
        ttf10_med = _median(ttf10_list)

        # Вікно часу на основі алертів
        if alerts:
            tmin = min(int(al["ts_ms"]) for al in alerts)
            tmax = max(int(al["ts_ms"]) for al in alerts)
        else:
            tmin = tmax = 0

        def _iso(ms: int) -> str:
            if ms <= 0:
                return "-"
            return datetime.fromtimestamp(ms / 1000.0, tz=UTC).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        import os

        git_sha = os.environ.get("GIT_SHA", "unknown")

        f.write(
            f"build={git_sha} | window=[{_iso(tmin)},{_iso(tmax)}] | source={source} | params={{presence_min:{presence_min:.2f}, bias_abs_min:{bias_abs_min:.2f}}} | dedup_dropped={dedup_dropped_total} | N_total={total_n} | ttf05_median={ttf05_med if ttf05_med==ttf05_med else 'nan'} | ttf10_median={ttf10_med if ttf10_med==ttf10_med else 'nan'} | ttf_thresholds={TTF_THRESHOLDS}"
        )
        # Позначка надто короткого вікна (<60s)
        if tmax - tmin < 60_000 and tmax > 0 and tmin > 0:
            f.write(" | note=too_short_window")
        if source == "explain":
            ttl_ratio = (ttl_rejected / max(1, alerts_seen)) if alerts_seen else 0.0
            f.write(
                f" | explain_ttl_sec={explain_ttl_sec} | ttl_rejected={ttl_rejected} | ttl_reject_ratio={ttl_ratio:.2f} | skew_dropped={skew_dropped_total}"
            )
        else:
            # Для whale-дджерела фіксуємо вік валідації
            f.write(f" | whale_max_age_sec={whale_age_sec}")
        if soft_applied:
            f.write(" | note=soft_thresholds")
        if early_stop:
            f.write(" | note=early_stop")
        if warnings:
            f.write(" | warnings=" + ";".join(warnings))
        f.write("\n")

    # Prometheus (опційно)
    try:
        from tools.metrics_forward import inc_dedup_dropped, inc_profile_emitted

        inc_dedup_dropped(dedup_dropped_total)
        inc_profile_emitted(source, len(alerts))
    except Exception:
        pass

    # Оновити dedup state
    if dedup_path is not None:
        try:
            dedup_path.parent.mkdir(parents=True, exist_ok=True)
            dedup_path.write_text(
                "\n".join(sorted(dedup_keys)) + "\n", encoding="utf-8"
            )
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--k", nargs="+", type=int, required=True)
    # None означає «не задано явно», буде розв'язано з урахуванням флагів/дефолтів
    parser.add_argument("--presence-min", type=float, default=None)
    parser.add_argument("--bias-abs-min", type=float, default=None)
    parser.add_argument("--whale-max-age-sec", type=int, default=None)
    parser.add_argument("--source", choices=["whale", "explain"], default="whale")
    parser.add_argument("--explain-ttl-sec", type=int, default=None)
    parser.add_argument("--dedup-file", default=None)
    parser.add_argument("--max-lines", type=int, default=None)
    args = parser.parse_args()
    asyncio.run(run(args))
