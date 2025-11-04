"""HTF Publisher: агрегує M1 → M5/M15 та публікує у Redis.

Використання (Windows PowerShell):
    venv\Scripts\python.exe -m tools.htf_publisher \
        --symbols BTCUSDT,TONUSDT,SNXUSDT --htf M5,M15 --publish-redis

Особливості:
- Джерело даних:
  1) спроба прочитати останній снапшот M1 із локального datastore/*_bars_1m_snapshot.jsonl
     (простий та надійний варіант для демо);
  2) у майбутньому можна додати читання з Redis ai_one:bars:{SYMBOL}:M1.
- Агрегація: pandas resample з індексу datetime (UTC) з OHLCV-правилами.
- Публікація:
  - Ключ батчу: ai_one:bars:{SYMBOL}:{TF} (TF ∈ {M5, M15}).
  - TTL: беремо з INTERVAL_TTL_MAP за відповідним інтервалом ("5m"/"15m").
  - Оновлюємо ai_one:state:{SYMBOL} (hash): htf:strength, htf_strength (дубль для сумісності), htf_stale_ms=0.
- Логування: кожні ~30 с маркер [HTF_PUBLISH] symbol=... tf=... n=...

Примітка: це утиліта best-effort для відновлення контексту HTF; не змінює контракти Stage1/2/3.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Iterable
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from app.settings import settings
from config.config import INTERVAL_TTL_MAP, NAMESPACE

# Опційні метрики Prometheus (клієнтські set_*, без старту HTTP експортеру)
try:  # best-effort: не ламаємо інструмент без залежностей
    from config.config import PROM_GAUGES_ENABLED  # type: ignore
except Exception:  # pragma: no cover
    PROM_GAUGES_ENABLED = False  # type: ignore[assignment]
try:
    if PROM_GAUGES_ENABLED:
        from telemetry.prom_gauges import (
            set_htf_strength as _set_htf_strength,  # type: ignore
        )

        _SET_HTF = _set_htf_strength
    else:  # pragma: no cover - вимкнено прапором
        _SET_HTF = None  # type: ignore[assignment]
except Exception:  # pragma: no cover - немає prometheus_client або інша помилка
    _SET_HTF = None  # type: ignore[assignment]
from config.keys import build_key
from utils.utils import sanitize_ohlcv_numeric

try:  # синхронний клієнт (простий для інструмента)
    import redis  # type: ignore
except Exception:  # pragma: no cover - дозволяє запуск без redis у тестах
    redis = None  # type: ignore[assignment]


logger = logging.getLogger("tools.htf_publisher")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

DATASTORE_DIR = Path("./datastore")


def _parse_symbols(s: str) -> list[str]:
    return [x.strip().upper() for x in s.split(",") if x.strip()]


def _parse_htf(s: str) -> list[str]:
    """
    Розібрати рядок, розділений комами, для вилучення дійсних вищих таймфреймів.

    Ця функція приймає рядковий вхід, розділяє його комами, видаляє пробіли,
    перетворює на верхній регістр та фільтрує за дійсними ідентифікаторами таймфреймів ("M5" або "M15").
    Якщо жодного дійсного таймфрейму не знайдено, за замовчуванням повертає ["M5"].

    Параметри:
        s (str): Рядок, розділений комами, із потенційними ідентифікаторами таймфреймів.

    Повертає:
        list[str]: Список дійсних рядків таймфреймів ("M5" або "M15"), або ["M5"], якщо жодного не знайдено.
    """
    out: list[str] = []
    for part in s.split(","):
        p = part.strip().upper()
        if p in {"M5", "M15"}:
            out.append(p)
    return out or ["M5"]


def _load_m1_df(symbol: str) -> pd.DataFrame:
    """
    Завантажує 1-хвилинні OHLCV дані з локального сховища даних у форматі JSONL.
    Функція завантажує дані для вказаного символу (symbol) з файлу снапшоту,
    проводить санітизацію числових значень OHLCV та встановлює індекс як datetime
    на основі часу відкриття (open_time).
    Параметри:
    ----------
    symbol : str
        Символ активу (наприклад, 'btc' або 'eth'), для якого завантажуються дані.
        Назва файлу формується як '{symbol.lower()}_bars_1m_snapshot.jsonl'.
    Повертає:
    -------
    pd.DataFrame
        Датафрейм із колонками: 'open_time', 'open', 'high', 'low', 'close', 'volume'.
        Індекс встановлено як pd.DatetimeIndex у UTC на основі 'open_time' (у мілісекундах).
    Викликає:
    -------
    FileNotFoundError
        Якщо файл снапшоту для вказаного символу не існує у директорії DATASTORE_DIR.
    ValueError
        Якщо у завантаженому датафреймі відсутні необхідні колонки ('open_time', 'open', 'high', 'low', 'close', 'volume').
    """
    fname = f"{symbol.lower()}_bars_1m_snapshot.jsonl"
    path = DATASTORE_DIR / fname
    if not path.exists():
        raise FileNotFoundError(f"Відсутній снапшот: {path}")
    df = pd.read_json(path, lines=True)
    # Санітизація та приведення назв
    if "timestamp" in df.columns and "open_time" not in df.columns:
        df = df.rename(columns={"timestamp": "open_time"})
    cols = ["open_time", "open", "high", "low", "close", "volume"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Відсутні колонки у {path}: {missing}")
    df = df[cols].copy()
    df = sanitize_ohlcv_numeric(df)
    # до datetime індексу
    try:
        # open_time у снапшотах у мілісекундах
        idx = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    except Exception:
        idx = pd.to_datetime(df["open_time"], utc=True)
    df.index = idx
    return df


def _resample_ohlcv(df_m1: pd.DataFrame, tf: str) -> pd.DataFrame:
    """
    Перетворює OHLCV дані з 1-хвилинного інтервалу на вищий часовий інтервал (наприклад, 5 хвилин або 15 хвилин).
    Ця функція виконує ресемплінг даних OHLCV (відкриття, максимум, мінімум, закриття, об'єм) з DataFrame,
    що містить 1-хвилинні дані, на заданий часовий інтервал. Вона обчислює агреговані значення для кожного інтервалу
    та повертає новий DataFrame з колонками: open_time (час відкриття в мілісекундах), open, high, low, close, volume.
    Args:
        df_m1 (pd.DataFrame): DataFrame з 1-хвилинними даними OHLCV. Має містити колонки: 'open', 'high', 'low', 'close', 'volume'.
        tf (str): Часовий інтервал для ресемплінгу. Підтримувані значення: 'M5' (5 хвилин) або 'M15' (15 хвилин).
    Returns:
        pd.DataFrame: Новий DataFrame з ресемпленими даними OHLCV. Колонки: 'open_time' (int64, час відкриття в мілісекундах),
                      'open', 'high', 'low', 'close', 'volume'. Дані очищені від NaN значень.
    Raises:
        ValueError: Якщо заданий часовий інтервал (tf) не підтримується
    """
    rule = "5min" if tf == "M5" else "15min"
    ohlc = (
        df_m1[["open", "high", "low", "close"]]
        .resample(rule, label="left", closed="left")
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
            }
        )
    )
    vol = df_m1[["volume"]].resample(rule, label="left", closed="left").sum()
    out = pd.concat([ohlc, vol], axis=1).dropna(how="any")
    out = out.reset_index().rename(columns={"index": "open_time"})
    # Зворотне перетворення open_time в ms для серіалізації
    out["open_time"] = (
        pd.to_datetime(out["open_time"], utc=True).astype("int64") // 1_000_000
    ).astype("int64")
    return out[["open_time", "open", "high", "low", "close", "volume"]]


def _strength_from_htf(df_htf: pd.DataFrame, window: int = 48) -> float:
    """
    Обчислює евристику сили для вищого часового фрейму (HTF): нормовану ширину діапазону за N барів.
    Сила розраховується як clamp((max(high) - min(low)) / ema(close, N), 0..1), де ema - експоненціальна ковзна середня.
    Параметри:
        df_htf (pd.DataFrame): DataFrame з даними HTF, що містить стовпці 'high', 'low', 'close'.
        window (int, за замовчуванням 48): Кількість барів для розрахунку діапазону та EMA.
    Повертає:
        float: Значення сили в діапазоні [0.0, 1.0]. Повертає 0.0, якщо DataFrame порожній або виникає помилка.
    """
    if df_htf.empty:
        return 0.0
    tail = df_htf.tail(window)
    try:
        rng = float(tail["high"].max() - tail["low"].min())
        close = tail["close"].astype(float)
        # проста EMA для масштабу
        alpha = 2.0 / (min(len(close), window) + 1.0)
        ema = close.ewm(alpha=alpha, adjust=False).mean().iloc[-1]
        base = float(ema) if ema and float(ema) != 0.0 else float(close.iloc[-1])
        val = (rng / base) if base else 0.0
        return max(0.0, min(1.0, float(val)))
    except Exception:
        return 0.0


def _publish(redis_cli, symbol: str, tf: str, bars: list[dict], *, ttl: int) -> None:
    """
    Публікує HTF-батч барів у Redis та оновлює хеш стану.
    Ця функція зберігає список барів (bars) у Redis під ключем, побудованим для заданого символу (symbol)
    та гранулярності (tf), з встановленням часу життя (TTL). Потім оновлює хеш стану для символу,
    включаючи значення сили (strength) з останнього бара батчу та інші метадані.
    Параметри:
    - redis_cli: Клієнт Redis для виконання операцій (наприклад, redis.Redis).
    - symbol (str): Символ фінансового інструменту (наприклад, "EURUSD"). Перетворюється на верхній регістр.
    - tf (str): Гранулярність часу (time frame), наприклад "1m", "5m".
    - bars (list[dict]): Список словників, що представляють бари даних. Кожен бар містить інформацію,
      таку як "htf_strength".
    - ttl (int): Час життя ключа в секундах для батчу барів.
    Повертає:
    None
    Винятки:
    Функція обробляє винятки внутрішньо, логуючи помилки при невдачі запису або оновлення.
    """
    """Публікує HTF-батч і оновлює state hash."""
    sym = symbol.upper()
    # Ключ батчу барів
    bars_key = build_key(NAMESPACE, "bars", symbol=sym, granularity=tf)
    payload = json.dumps(bars, ensure_ascii=False)
    try:
        # setex якщо доступний, інакше set+expire
        setex = getattr(redis_cli, "setex", None)
        if callable(setex):
            setex(bars_key, int(ttl), payload)
        else:
            redis_cli.set(bars_key, payload)
            try:
                redis_cli.expire(bars_key, int(ttl))
            except Exception:
                pass
    except Exception:
        logger.debug("Не вдалося записати %s", bars_key, exc_info=True)

    # Оновити state hash
    state_key = build_key(NAMESPACE, "state", symbol=sym)
    try:
        hset = getattr(redis_cli, "hset", None)
        expire = getattr(redis_cli, "expire", None)
        if callable(hset):
            # сила та свіжість
            # strength беремо з останнього бара батчу: очікуємо, що bars[-1]["strength"] встановлено вище
            last_strength = 0.0
            try:
                last_strength = float(bars[-1].get("htf_strength", 0.0))  # type: ignore[index]
            except Exception:
                last_strength = 0.0
            mapping = {
                "htf:strength": last_strength,
                "htf_strength": last_strength,  # дубль для читачів без двокрапки
                "htf_stale_ms": 0,
            }
            hset(state_key, mapping={k: v for k, v in mapping.items() if v is not None})
            if callable(expire):
                # легкий TTL для hash, щоб уникати протухлих станів у демо
                expire(state_key, int(INTERVAL_TTL_MAP.get("1m", 120)))
    except Exception:
        logger.debug("Не вдалося оновити state hash %s", state_key, exc_info=True)


def run(
    symbols: Iterable[str],
    htf_list: list[str],
    publish_redis: bool = True,
    loop_sleep_s: float = 5.0,
) -> int:
    """
    Запускає нескінченний цикл публікації HTF (Higher Time Frame) батчів для заданих символів та таймфреймів.
    Функція завантажує дані M1 для кожного символу, передискретизує їх на вищі таймфрейми (HTF),
    обчислює силу (strength) для кожного HTF, додає її до останнього рядка батчу, опціонально публікує
    метрики у Prometheus, серіалізує батч у список словників та публікує у Redis (якщо ввімкнено).
    Цикл працює постійно з паузами між ітераціями, логуючи прогрес кожні 30 секунд.
        symbols (Iterable[str]): Ітерований об'єкт зі списком символів (наприклад, валютних пар) для обробки.
        publish_redis (bool, за замовчуванням True): Якщо True, публікує серіалізовані батчі у Redis.
    Повертає:
        int: Загальна кількість опублікованих барів (для діагностики, хоча цикл нескінченний).
    Викидає:
        Не викидає винятків безпосередньо, але обробляє внутрішні помилки (наприклад, при завантаженні даних
        або публікації) за допомогою логування та продовження циклу.
    """
    # Підключення до Redis (синхронний клієнт для простоти інструмента)
    r = None
    if publish_redis and redis is not None:
        try:
            r = redis.StrictRedis(
                host=settings.redis_host,
                port=settings.redis_port,
                decode_responses=True,
            )
        except Exception:
            r = None

    last_log = 0.0
    while True:
        total = 0
        for sym in symbols:
            try:
                df_m1 = _load_m1_df(sym)
            except Exception as e:
                logger.warning("[HTF] symbol=%s завантаження M1 невдале: %s", sym, e)
                continue
            for tf in htf_list:
                df_htf = _resample_ohlcv(df_m1, tf)
                if df_htf.empty:
                    logger.debug(
                        "[HTF] symbol=%s tf=%s порожній батч після ресемплінгу", sym, tf
                    )
                    continue
                # strength по HTF
                s_val = _strength_from_htf(df_htf)
                # додамо strength у останній рядок батчу (службово для публікації state)
                try:
                    df_htf.loc[df_htf.index[-1], "htf_strength"] = s_val
                except Exception:
                    logger.debug(
                        "[HTF] symbol=%s tf=%s не вдалося додати strength", sym, tf
                    )
                # Prometheus: легка публікація (опційно)
                try:
                    if callable(_SET_HTF):  # type: ignore[arg-type]
                        _SET_HTF(sym.upper(), float(s_val))  # type: ignore[misc]
                except Exception:
                    logger.debug("[HTF] symbol=%s tf=%s Prometheus невдалий", sym, tf)
                # серіалізація батчу у list[dict]
                bars = df_htf.tail(200).to_dict(orient="records")  # обмежимо розмір
                # TTL з мапи
                ttl = int(INTERVAL_TTL_MAP.get("5m" if tf == "M5" else "15m", 600))
                if r is not None and publish_redis:
                    _publish(r, sym, tf, bars, ttl=ttl)
                    logger.info(
                        "[HTF_PUBLISH] symbol=%s tf=%s n=%d", sym, tf, len(bars)
                    )
                total += len(bars)

        now = time.time()
        if now - last_log >= 30.0:
            logger.info(
                "[HTF_PUBLISH] symbols=%d tf=%s n≈%d",
                len(symbols),
                ",".join(htf_list),
                total,
            )
            last_log = now
        time.sleep(loop_sleep_s)


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="HTF Publisher (M1→M5/M15)")
    p.add_argument(
        "--symbols", required=True, help="Кома‑список символів: BTCUSDT,TONUSDT"
    )
    p.add_argument(
        "--source", default="m1", help="Джерело (зарезервовано, не використовується)"
    )
    p.add_argument("--htf", default="M5,M15", help="Кома‑список HTF: M5,M15")
    p.add_argument("--publish-redis", action="store_true", help="Публікувати у Redis")
    return p


def main(argv: list[str] | None = None) -> int:
    logger.info("[HTF] Запуск HTF Publisher")
    try:
        args = _build_cli().parse_args(argv)
        symbols = _parse_symbols(args.symbols)
        htf_list = _parse_htf(args.htf)
        logger.info(
            "[HTF] Символи: %s, HTF: %s, Redis: %s",
            symbols,
            htf_list,
            args.publish_redis,
        )
        run(symbols, htf_list, publish_redis=bool(args.publish_redis))
        logger.info("[HTF] HTF Publisher завершено успішно")
        return 0
    except Exception as e:
        logger.error("[HTF] Помилка у HTF Publisher: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
