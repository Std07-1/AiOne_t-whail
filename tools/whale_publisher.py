"""Whale Publisher: best‑effort оновлювач стану whale у Redis.

Використання (Windows PowerShell):
    venv\Scripts\python.exe -m tools.whale_publisher \
        --symbols BTCUSDT,TONUSDT,SNXUSDT --cadence-ms 3000

Дані/логіка:
- Джерело: локальний снапшот datastore/*_bars_1m_snapshot.jsonl (демо‑режим).
- Евристика:
  * vwap_dev = (close - vwap(window=60)) / vwap
  * bias = sign(vwap_dev) * min(1, |vwap_dev| / 0.02)
  * presence ∈ [0,1] як поєднання нормованого zscore(volume, 200) та |vwap_dev|
- Публікація кожні cadence_ms:
  * HSET ai_one:state:{SYMBOL} поле "whale" (JSON), наприклад:
      {"presence":0.45,"bias":0.14,"vwap_dev":0.0012,"stale":false,"ts_ms":...}
  * SET ai_one:metrics:whale:last_publish_ms = now_ms
- Лог кожні ~15 с: [WHALE_PUBLISH] symbol=... presence=... bias=...

Примітка: інструмент не змінює контракти Stage1/2/3 та працює окремо від основного пайплайна.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from app.settings import settings
from config.config import INTERVAL_TTL_MAP, NAMESPACE

# Опційні метрики Prometheus (клієнтські set_*, без старту HTTP експортеру)
try:
    from config.config import PROM_GAUGES_ENABLED  # type: ignore
except Exception:  # pragma: no cover
    PROM_GAUGES_ENABLED = False  # type: ignore[assignment]
try:
    if PROM_GAUGES_ENABLED:
        from telemetry.prom_gauges import set_presence as _set_presence  # type: ignore

        _SET_PRESENCE = _set_presence
    else:  # pragma: no cover - вимкнено прапором
        _SET_PRESENCE = None  # type: ignore[assignment]
except Exception:  # pragma: no cover - залежність відсутня або інша помилка
    _SET_PRESENCE = None  # type: ignore[assignment]
from config.keys import build_key
from utils.utils import sanitize_ohlcv_numeric

try:  # синхронний клієнт
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore[assignment]


logger = logging.getLogger("tools.whale_publisher")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

DATASTORE_DIR = Path("./datastore")


def _parse_symbols(s: str) -> list[str]:
    return [x.strip().upper() for x in s.split(",") if x.strip()]


def _load_m1_df(symbol: str) -> pd.DataFrame:
    """
    Завантажує DataFrame з 1-хвилинними барами для заданого символу з JSONL файлу.

    Функція шукає файл з іменем '{symbol.lower()}_bars_1m_snapshot.jsonl' у директорії DATASTORE_DIR.
    Якщо файл не існує, виникає помилка FileNotFoundError.
    Дані зчитуються як JSON рядки, після чого перевіряються та обробляються необхідні колонки OHLCV.

    Параметри:
        symbol (str): Символ активу (наприклад, 'BTCUSDT'), для якого завантажуються дані.

    Повертає:
        pd.DataFrame: DataFrame з колонками ['open_time', 'open', 'high', 'low', 'close', 'volume'],
                      де 'open_time' - час відкриття бару, а інші - відповідні OHLCV значення.
                      Дані очищені від некоректних числових значень за допомогою sanitize_ohlcv_numeric.

    Виникає:
        FileNotFoundError: Якщо файл з даними не знайдено за вказаним шляхом.
        ValueError: Якщо у DataFrame відсутні необхідні колонки ('open_time', 'open', 'high', 'low', 'close', 'volume').
    """
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
    """
    Обчислює ковзну середню вагову ціну за обсягом (VWAP) для заданих серій цін закриття та обсягів.

    VWAP (Volume Weighted Average Price) - це індикатор, який обчислює середню ціну актива, зважену за обсягом торгівлі протягом заданого періоду.
    Ця функція використовує ковзне вікно для розрахунку VWAP, що дозволяє отримати динамічну середню ціну, яка враховує обсяги.

    Параметри:
        close (pd.Series): Серія цін закриття актива. Повинна бути pandas Series з числовими значеннями.
        volume (pd.Series): Серія обсягів торгівлі актива. Повинна бути pandas Series з числовими значеннями, відповідати індексу close.
        window (int, за замовчуванням 60): Розмір ковзного вікна для розрахунку VWAP. Визначає кількість періодів, які використовуються для обчислення середнього.

    Повертає:
        pd.Series: Серія значень ковзної VWAP. Значення заповнюються назад (bfill) і вперед (ffill) для обробки відсутніх даних.

    Примітки:
        - Функція використовує min_periods=max(3, window // 3) для забезпечення мінімальної кількості періодів у вікні.
        - Ділення обробляється з ігноруванням помилок ділення на нуль або некоректних значень за допомогою np.errstate.
        - Результат заповнюється для уникнення NaN у кінцевих значеннях.
    """
    price_vol = close * volume
    num = price_vol.rolling(window, min_periods=max(3, window // 3)).sum()
    den = volume.rolling(window, min_periods=max(3, window // 3)).sum()
    with np.errstate(divide="ignore", invalid="ignore"):
        vwap = num / den
    return vwap.bfill().ffill()


def build_whale_payload(df_m1: pd.DataFrame) -> dict:
    """
    Будує корисне навантаження (payload) для аналізу "whale" активності на основі даних хвилинного графіка.

    Функція обробляє останні 200 рядків DataFrame df_m1, обчислюючи індикатори, такі як VWAP (Volume Weighted Average Price),
    відхилення від VWAP (vdev), bias (упередження) та presence (присутність). Ці індикатори допомагають оцінити
    вплив великих гравців (whales) на ринок, враховуючи ціну закриття, обсяг торгів та їхні відхилення.

    Args:
        df_m1 (pd.DataFrame): DataFrame з хвилинними даними, що містить стовпці "close" (ціна закриття) та "volume" (обсяг).
                              Очікується, що дані є числовими та придатними для обчислень.

    Returns:
        dict: Словник з обчисленими індикаторами:
            - "presence" (float): Індикатор присутності whales, обчислений як комбінація z-score обсягу та абсолютного відхилення від VWAP.
                                  Значення в діапазоні [0, 1], де вищі значення вказують на сильнішу активність.
            - "bias" (float): Упередження, масштабоване через 2% відхилення від VWAP. Значення в діапазоні [-1, 1],
                              де позитивні значення означають зростання, негативні - падіння.
            - "vwap_dev" (float): Відносне відхилення поточної ціни від VWAP (Volume Weighted Average Price).
            - "stale" (bool): Завжди False, вказує на актуальність даних.
            - "ts_ms" (int): Часова мітка в мілісекундах (поточний час).

    Примітки:
        - VWAP обчислюється як ковзне середнє з вагою обсягу за вікном 60 періодів.
        - Відхилення vdev = (close[-1] - vwap[-1]) / vwap[-1], якщо vwap не нульове.
        - Bias масштабується до [-1, 1] на основі vdev, з порогом 2%.
        - Presence поєднує z-score обсягу (стандартизований обсяг) та абсолютне vdev для оцінки активності.
        - Функція використовує numpy для перевірки скінченності значень та обробки NaN.
    """
    tail = df_m1.tail(200).copy()
    close = tail["close"].astype(float)
    vol = tail["volume"].astype(float)
    vwap = _rolling_vwap(close, vol, window=60)
    vdev = float(
        ((close.iloc[-1] - vwap.iloc[-1]) / vwap.iloc[-1]) if vwap.iloc[-1] else 0.0
    )
    # bias ∈ [-1,1], масштабуємо через 2%
    mag = min(1.0, abs(vdev) / 0.02) if np.isfinite(vdev) else 0.0
    bias = mag if vdev > 0 else (-mag if vdev < 0 else 0.0)
    # presence: поєднання zscore(volume) і |vdev|
    zv = (vol - vol.mean()) / (vol.std(ddof=0) if vol.std(ddof=0) else 1.0)
    z_last = float(zv.iloc[-1]) if np.isfinite(zv.iloc[-1]) else 0.0
    presence = max(0.0, min(1.0, 0.2 + 0.15 * z_last + 6.0 * abs(vdev)))
    now_ms = int(time.time() * 1000)
    payload = {
        "presence": float(presence),
        "bias": float(bias),
        "vwap_dev": float(vdev),
        "stale": False,
        "ts_ms": now_ms,
    }
    return payload


def run(symbols: Iterable[str], cadence_ms: int = 3000) -> int:
    """
    Основна функція запуску публікатора whale-даних.

    Ця функція виконує цикл публікації індикаторів whale-активності для заданих символів.
    Вона завантажує хвилинні дані з локальних снапшотів, обчислює payload (присутність, bias тощо),
    публікує їх у Redis (як у state hash, так і у окремий ключ для читання пайплайном),
    опціонально оновлює метрики Prometheus та логуює кожні ~15 секунд.

    Параметри:
        symbols (Iterable[str]): Ітерабельний об'єкт з символами активів (наприклад, ['BTCUSDT', 'TONUSDT']).
        cadence_ms (int, за замовчуванням 3000): Інтервал між оновленнями в мілісекундах.

    Повертає:
        int: Код виходу (завжди 0 при успішному виконанні, але функція працює в циклі).

    Примітки:
        - Використовує Redis для публікації, якщо доступний.
        - Логування включає [WHALE_PUBLISH] з presence та bias.
        - Prometheus-метрики оновлюються опціонально, якщо увімкнено.
        - Цикл працює безкінечно з паузою cadence_ms.
    """
    logger.info(
        "[WHALE_PUBLISH] Початок роботи з символами: %s, каденс: %d мс",
        ", ".join(symbols),
        cadence_ms,
    )
    # Ініціалізація Redis-клієнта, якщо доступний
    r = None
    if redis is not None:
        try:
            r = redis.StrictRedis(
                host=settings.redis_host,
                port=settings.redis_port,
                decode_responses=True,
            )
            logger.info("[WHALE_PUBLISH] Redis-клієнт ініціалізований успішно")
        except Exception as e:
            logger.warning(
                "[WHALE_PUBLISH] Не вдалося ініціалізувати Redis-клієнт: %s", e
            )
            r = None
    else:
        logger.warning("[WHALE_PUBLISH] Redis недоступний")

    # Змінні для контролю логування (щоб уникнути надлишкових логів)
    last_log = 0.0
    last_prom_log = 0.0
    while True:
        logger.debug("[WHALE_PUBLISH] Початок ітерації циклу")
        # Ітерація по кожному символу для обробки
        for sym in symbols:
            try:
                # Завантаження DataFrame з хвилинними барами для символу
                df = _load_m1_df(sym)
                logger.debug(
                    "[WHALE_PUBLISH] Завантажено M1 дані для %s: %d рядків",
                    sym,
                    len(df),
                )
            except Exception as e:
                # Логування помилки завантаження, але продовження для інших символів
                logger.warning(
                    "[WHALE_PUBLISH] %s: не вдалося завантажити M1: %s", sym, e
                )
                continue
            # Побудова payload з індикаторами whale-активності
            try:
                payload = build_whale_payload(df)
                logger.debug(
                    "[WHALE_PUBLISH] Побудовано payload для %s: presence=%.3f, bias=%.3f",
                    sym,
                    payload.get("presence", 0.0),
                    payload.get("bias", 0.0),
                )
            except Exception as e:
                logger.error(
                    "[WHALE_PUBLISH] %s: помилка при побудові payload: %s", sym, e
                )
                continue
            # Публікація у Redis, якщо клієнт ініціалізований
            if r is not None:
                try:
                    # Ключ для state hash: ai_one:state:{SYMBOL}
                    state_key = build_key(NAMESPACE, "state", symbol=sym.upper())
                    # Серіалізація payload у JSON
                    whale_json = json.dumps(payload, ensure_ascii=False)
                    # Збереження у hash як поле "whale"
                    r.hset(state_key, mapping={"whale": whale_json})
                    # Додатковий write-through у окремий ключ для читання пайплайном: ai_one:whale:{SYMBOL}:1m
                    whale_key = build_key(
                        NAMESPACE, "whale", symbol=sym.upper(), granularity="1m"
                    )
                    # TTL з INTERVAL_TTL_MAP (за замовчуванням 180 сек для 1m)
                    ttl = int(INTERVAL_TTL_MAP.get("1m", 180))
                    # Використання setex для атомічного set з TTL, якщо доступний
                    setex = getattr(r, "setex", None)
                    if callable(setex):
                        setex(whale_key, ttl, whale_json)
                    else:
                        # Альтернатива: set окремо, потім expire
                        r.set(whale_key, whale_json)
                        try:
                            r.expire(whale_key, ttl)
                        except Exception:
                            pass
                    # Окремий ключ для метрики останньої публікації: ai_one:metrics:whale:last_publish_ms
                    met_key = build_key(
                        NAMESPACE, "metrics", extra=["whale", "last_publish_ms"]
                    )
                    r.set(met_key, str(payload["ts_ms"]))
                    logger.debug("[WHALE_PUBLISH] Опубліковано у Redis для %s", sym)
                except Exception:
                    # Логування помилки публікації
                    logger.warning(
                        "[WHALE_PUBLISH] Не вдалося опублікувати whale для %s",
                        sym,
                        exc_info=True,
                    )
            # Опціональне оновлення Prometheus-метрики presence
            try:
                if callable(_SET_PRESENCE):  # type: ignore[arg-type]
                    _SET_PRESENCE(sym.upper(), float(payload.get("presence", 0.0)))  # type: ignore[misc]
                    logger.debug("[WHALE_PUBLISH] Оновлено Prometheus для %s", sym)
            except Exception as e:
                logger.debug(
                    "[WHALE_PUBLISH] Помилка оновлення Prometheus для %s: %s", sym, e
                )

            # Логування кожні ~15 секунд для уникнення спаму
            now = time.time()
            if now - last_log >= 15.0:
                logger.info(
                    "[WHALE_PUBLISH] symbol=%s presence=%.3f bias=%.3f",
                    sym,
                    payload.get("presence", 0.0),
                    payload.get("bias", 0.0),
                )
                last_log = now

        # Логування підтвердження Prometheus-оновлень кожні ~30 секунд
        if time.time() - last_prom_log >= 30.0:
            try:
                logger.info("[PROM] presence set for %s", ",".join(symbols))
            except Exception:
                logger.info("[PROM] presence set (symbols logged)")
            last_prom_log = time.time()

        # Пауза між ітераціями циклу
        logger.debug("[WHALE_PUBLISH] Пауза %d мс", cadence_ms)
        time.sleep(max(0.0, cadence_ms / 1000.0))


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Whale Publisher (best‑effort)")
    p.add_argument(
        "--symbols", required=True, help="Кома‑список символів: BTCUSDT,TONUSDT"
    )
    p.add_argument("--cadence-ms", type=int, default=3000, help="Каденс оновлення, мс")
    return p


def main(argv: list[str] | None = None) -> int:
    logger.info("[WHALE_PUBLISH] Початок роботи скрипта")
    args = _build_cli().parse_args(argv)
    symbols = _parse_symbols(args.symbols)
    logger.info(
        "[WHALE_PUBLISH] Розібрано символи: %s, каденс: %d мс",
        ", ".join(symbols),
        args.cadence_ms,
    )
    run(symbols, cadence_ms=int(args.cadence_ms))
    logger.info("[WHALE_PUBLISH] Завершення роботи скрипта")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
