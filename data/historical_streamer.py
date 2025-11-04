"""
Модуль data/historical_streamer.py

Призначення:
  • Локальний «історичний стрімер» свічок для інтеграційних e2e-тестів AiOne_t.
  • Завантажує обраний діапазон даних (CSV/JSONL/Parquet), відтворює як стрім.
  • Публікує події у:
      1) unified_store.push_bar(symbol, timeframe, bar) — якщо доступно,
      2) Redis pub/sub канал ai_one:bars:{symbol}:{tf} — fallback,
      3) локальні asyncio.Queue — для детермінованих тестів без зовнішніх залежностей.
  • Керує швидкістю: realtime, speed-множник, throttle інтервал, pause/resume/seek.
  • Зберігає позицію програвання у Redis ключі ai_one:streamer:pos:{symbol}:{tf} з TTL.
  • Prometheus-метрики з мінімальними накладними витратами.
  • Логування через RichHandler.

Використання:
  from stadatage1.historical_streamer import HistoricalStreamer, StreamConfig
  cfg = StreamConfig(
      path="data/EURUSD_M1.parquet",
      symbol="EURUSD",
      timeframe="1m",
      start_ts=None,  # ms
      end_ts=None,    # ms
      realtime=False,
      speed=0.0,      # 0 → миттєво, 1.0 → realtime, 2.0 → вдвічі швидше
      throttle_ms=0,  # ≥0 штучна пауза між барами
      redis_url="redis://localhost:6379/0",
      save_position=True,
      pos_ttl_sec=24*3600,
  )
  streamer = HistoricalStreamer(cfg, unified_store=unified_store)
  await streamer.start()
  ...
  await streamer.pause()
  await streamer.seek_ts(ts_ms)
  await streamer.resume()
  ...
  await streamer.stop()

Політика ключів Redis:
  • Публікація свічок: ai_one:bars:{symbol}:{timeframe}
  • Позиція програвання: ai_one:streamer:pos:{symbol}:{timeframe} → {"ts": <ms>}
"""

from __future__ import annotations

# ── Стандартна бібліотека ──
import asyncio
import csv
import json
import logging
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ── Опціональні залежності ──
try:
    import pandas as pd  # для Parquet
except Exception:  # pragma: no cover
    pd = None  # type: ignore

try:
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore

try:
    from prometheus_client import Counter, Gauge
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    Gauge = None  # type: ignore

from rich.logging import RichHandler

from config.config import NAMESPACE

# ── Логування ──
LOG = logging.getLogger("ai_one.historical_streamer")
if not LOG.handlers:
    _handler = RichHandler(rich_tracebacks=True)
    _fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    _handler.setFormatter(logging.Formatter(_fmt))
    LOG.addHandler(_handler)
LOG.setLevel(logging.INFO)


# ── Метрики Prometheus ──
BARS_PUBLISHED = (
    Counter(
        "ai_one_stream_bars_total", "Кількість опублікованих барів історичного стріму"
    )
    if Counter
    else None
)
STREAM_ACTIVE = (
    Gauge("ai_one_stream_active", "Статус стріма (1=active,0=idle)") if Gauge else None
)
STREAM_LAG_MS = (
    Gauge("ai_one_stream_lag_ms", "Лаг між баром та системним часом, ms")
    if Gauge
    else None
)
STREAM_LEAD_MS = (
    Gauge("ai_one_stream_lead_ms", "Випередження (lead) відносно плану, ms")
    if Gauge
    else None
)
BARS_DROPPED = (
    Counter(
        "ai_one_stream_dropped_total", "Кількість скинутих подій через backpressure"
    )
    if Counter
    else None
)


# ── Типи даних ──
@dataclass
class Bar:
    ts: int  # epoch ms
    open: float
    high: float
    low: float
    close: float
    volume: float
    symbol: str
    timeframe: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": int(self.ts),
            "open": float(self.open),
            "high": float(self.high),
            "low": float(self.low),
            "close": float(self.close),
            "volume": float(self.volume),
            "symbol": self.symbol,
            "timeframe": self.timeframe,
        }


@dataclass
class StreamConfig:
    # Джерело
    path: str
    symbol: str
    timeframe: str = "1m"
    # Фільтри діапазону [start_ts, end_ts]
    start_ts: int | None = None  # ms
    end_ts: int | None = None  # ms
    # Режим програвання
    realtime: bool = False
    speed: float = 0.0  # 0 → миттєво
    throttle_ms: int = 0  # ≥0 штучна пауза між барами
    # Redis
    redis_url: str | None = None
    save_position: bool = True
    pos_ttl_sec: int = 24 * 3600
    # Поведінка
    validate_bars: bool = True  # інваріанти high≥low, no NaN
    # Буфер підписників
    local_queue_maxsize: int = 0  # 0 = без ліміту
    # Обмеження навантаження
    max_events_per_sec: int = 0  # 0 = без ліміту (soft-cap)


# ── Основний клас стрімера ──
class HistoricalStreamer:
    """
    Стрімер для історичних свічок.

    Чому так:
      • Виносимо I/O читання у генератори, щоб мінімізувати паузи в публікації.
      • Реалізуємо три рівні доставки: unified_store → Redis → локальні Queue.
      • Позиція в Redis дозволяє перезапускати довгі тести без втрати контексту.
    """

    def __init__(self, cfg: StreamConfig, unified_store: Any | None = None) -> None:
        self.cfg = cfg
        self.unified_store = unified_store
        self._path = Path(cfg.path)
        self._stop = asyncio.Event()
        self._paused = asyncio.Event()
        self._paused.set()  # set → НЕ пауза; clear → пауза
        self._task: asyncio.Task | None = None
        self._subscribers: list[asyncio.Queue] = []
        self._redis: Any | None = None
        self._anchor: tuple[int, float] | None = (
            None  # (first_bar_ts_ms, loop_monotonic_s)
        )
        self._rate_anchor: tuple[float, int] | None = None  # (mono_s, sent_count)

    # ── Публічний API ──
    async def start(self) -> None:
        if STREAM_ACTIVE:
            STREAM_ACTIVE.set(1)
        self._stop.clear()
        self._paused.set()
        if self.cfg.redis_url and aioredis:
            try:
                self._redis = aioredis.from_url(self.cfg.redis_url)
                # перевірити з'єднання
                await self._redis.ping()
            except Exception:
                LOG.warning("Не вдалося підключитись до Redis: %s", self.cfg.redis_url)
                self._redis = None
        LOG.info(
            "HistoricalStreamer start: %s %s tf=%s realtime=%s speed=%.2f throttle=%dms",
            self.cfg.symbol,
            self._path.name,
            self.cfg.timeframe,
            self.cfg.realtime,
            self.cfg.speed,
            self.cfg.throttle_ms,
        )
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await self._task
            finally:
                self._task = None
        if self._redis:
            try:
                await self._redis.close()
            except Exception:
                pass
            self._redis = None
        if STREAM_ACTIVE:
            STREAM_ACTIVE.set(0)
        LOG.info("HistoricalStreamer stopped: %s %s", self.cfg.symbol, self._path.name)

    async def pause(self) -> None:
        self._paused.clear()
        # Скидаємо якір часу для коректного перерахунку після відновлення
        self._anchor = None
        LOG.info("HistoricalStreamer paused")

    async def resume(self) -> None:
        self._paused.set()
        LOG.info("HistoricalStreamer resumed")

    async def seek_ts(self, ts_ms: int) -> None:
        # просте seek: перезапуск задачі із новим start_ts
        if not isinstance(ts_ms, int):
            raise ValueError("ts_ms має бути цілим у мілісекундах")
        LOG.info("Seek to ts=%d", ts_ms)
        await self.pause()
        self.cfg.start_ts = ts_ms
        await self._restart()

    def subscribe_queue(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self.cfg.local_queue_maxsize)
        self._subscribers.append(q)
        return q

    def unsubscribe_queue(self, q: asyncio.Queue) -> None:
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    # ── Внутрішня логіка ──
    async def _restart(self) -> None:
        # контрольований перезапуск без втрати ресурсу Redis
        await self.stop()
        await self.start()

    async def _run(self) -> None:
        if not self._path.exists():
            LOG.error("Файл не знайдено: %s", self._path)
            return

        # 1) Позиція у Redis (optional)
        if self.cfg.save_position and self._redis:
            last_ts = await self._load_position()
            if last_ts and (self.cfg.start_ts is None or last_ts > self.cfg.start_ts):
                # якщо в Redis позиція новіша за локальний start_ts — продовжимо з неї
                self.cfg.start_ts = last_ts

        # 2) Ітератор барів
        try:
            async for bar in self._iter_bars_filtered():
                if self._stop.is_set():
                    break
                await self._paused.wait()  # пауза
                await self._playback_wait(bar.ts)
                await self._publish(bar)
                await self._persist_position(bar.ts)
                await self._throttle()
                await self._rate_limit()
        except Exception as exc:
            LOG.exception("Помилка стрімера: %s", exc)
        finally:
            LOG.info("HistoricalStreamer finished file: %s", self._path.name)

    async def _iter_bars_filtered(self) -> AsyncIterator[Bar]:
        async for bar in self._iter_bars():
            if self.cfg.start_ts is not None and bar.ts < self.cfg.start_ts:
                continue
            if self.cfg.end_ts is not None and bar.ts > self.cfg.end_ts:
                break
            if self.cfg.validate_bars and not self._valid_bar(bar):
                # інваріанти захищають від битих даних у тестах
                continue
            yield bar

    async def _iter_bars(self) -> AsyncIterator[Bar]:
        suffix = self._path.suffix.lower()
        if suffix == ".csv":
            async for b in _async_csv_iter(
                self._path, self.cfg.symbol, self.cfg.timeframe
            ):
                yield b
        elif suffix in {".jsonl", ".ndjson"}:
            async for b in _async_jsonl_iter(
                self._path, self.cfg.symbol, self.cfg.timeframe
            ):
                yield b
        elif suffix in {".parquet", ".pq"}:
            if pd is None:
                raise RuntimeError("Для Parquet потрібен pandas")
            # читаємо синхронно, але безпечно і лінійно
            for b in _sync_parquet_iter(
                self._path, self.cfg.symbol, self.cfg.timeframe
            ):
                yield b
        else:
            # універсальна спроба CSV
            async for b in _async_csv_iter(
                self._path, self.cfg.symbol, self.cfg.timeframe
            ):
                yield b

    async def _publish(self, bar: Bar) -> None:
        payload = bar.to_dict()

        # 1) Спроба unified_store через адаптер put_bars (інкрементально)
        delivered = False
        if self.unified_store is not None:
            try:
                await _store_put_bar(self.unified_store, payload)
                delivered = True
            except Exception:
                LOG.debug(
                    "unified_store.push_bar помилка, fallback → Redis/Queue",
                    exc_info=True,
                )

        # 2) Redis pub/sub fallback
        if not delivered and self._redis is not None:
            channel = f"{NAMESPACE}:bars:{self.cfg.symbol}:{self.cfg.timeframe}"
            try:
                await self._redis.publish(channel, json.dumps(payload))
                delivered = True
            except Exception:
                LOG.debug("Redis publish помилка", exc_info=True)

        # 3) Локальні черги — завжди дублюємо для тестів
        for q in list(self._subscribers):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                # drop-oldest політика для стабільності інтеграцій
                try:
                    _ = q.get_nowait()
                except Exception:
                    pass
                try:
                    q.put_nowait(payload)
                except Exception:
                    pass
                if BARS_DROPPED:
                    try:
                        BARS_DROPPED.inc()
                    except Exception:
                        pass

        if BARS_PUBLISHED:
            try:
                BARS_PUBLISHED.inc()
            except Exception:
                pass

    async def _persist_position(self, ts_ms: int) -> None:
        if not (self.cfg.save_position and self._redis):
            return
        key = _pos_key(self.cfg.symbol, self.cfg.timeframe)
        try:
            await self._redis.set(
                key, json.dumps({"ts": ts_ms}), ex=self.cfg.pos_ttl_sec
            )
        except Exception:
            LOG.debug("Збереження позиції у Redis не вдалося", exc_info=True)

    async def _load_position(self) -> int | None:
        key = _pos_key(self.cfg.symbol, self.cfg.timeframe)
        try:
            raw = await self._redis.get(key)
            if not raw:
                return None
            obj = json.loads(raw)
            ts = int(obj.get("ts", 0))
            return ts if ts > 0 else None
        except Exception:
            return None

    async def _playback_wait(self, bar_ts_ms: int) -> None:
        # Реалізуємо три сценарії:
        #  • speed == 0 → без очікування (миттєво).
        #  • realtime True → реальне відтворення за часовими мітками.
        #  • speed > 0 → масштабування часу (вдвічі швидше тощо).
        if self.cfg.speed == 0.0 and not self.cfg.realtime:
            return

        now_mon = asyncio.get_running_loop().time()
        if self._anchor is None:
            self._anchor = (bar_ts_ms, now_mon)
            return

        first_ts, first_mon = self._anchor
        delta_ms = max(0, bar_ts_ms - first_ts)
        if self.cfg.realtime and self.cfg.speed <= 0.0:
            scale = 1.0
        else:
            scale = 1.0 / max(self.cfg.speed, 1e-6)

        target_mon = first_mon + (delta_ms / 1000.0) * scale
        delay = target_mon - now_mon
        if delay > 0:
            await asyncio.sleep(delay)
        # Окремі метрики lead/lag
        try:
            if STREAM_LAG_MS:
                STREAM_LAG_MS.set(int(max(0.0, -delay) * 1000.0))
            if STREAM_LEAD_MS:
                STREAM_LEAD_MS.set(int(max(0.0, delay) * 1000.0))
        except Exception:
            pass

    async def _throttle(self) -> None:
        if self.cfg.throttle_ms > 0:
            await asyncio.sleep(self.cfg.throttle_ms / 1000.0)

    async def _rate_limit(self) -> None:
        m = int(self.cfg.max_events_per_sec or 0)
        if m <= 0:
            return
        now = asyncio.get_running_loop().time()
        if self._rate_anchor is None:
            self._rate_anchor = (now, 1)
            return
        t0, n = self._rate_anchor
        elapsed = now - t0
        if elapsed >= 1.0:
            self._rate_anchor = (now, 1)
            return
        if n >= m:
            await asyncio.sleep(1.0 - elapsed)
            self._rate_anchor = (asyncio.get_running_loop().time(), 1)
        else:
            self._rate_anchor = (t0, n + 1)

    @staticmethod
    def _valid_bar(b: Bar) -> bool:
        # Чому: захист від битих джерел у тестах, що ламають Stage2/3.
        if b.high < b.low:
            return False
        if not (b.open == b.open and b.close == b.close):  # NaN check
            return False
        if not (b.high == b.high and b.low == b.low and b.volume == b.volume):
            return False
        return True


# ── Допоміжні функції читання ──
async def _async_csv_iter(
    path: Path, default_symbol: str, default_tf: str
) -> AsyncIterator[Bar]:
    # Чому асинхронно: дозволяє залишати цикл подій чутливим до pause/stop.
    # прочитуємо потоково, але парсинг рядків мінімальний
    with path.open("r", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            return
        header = [h.strip().lower() for h in header]
        idx = {k: i for i, k in enumerate(header)}
        for i, row in enumerate(reader, start=1):
            # дозволяємо іншим таскам працювати при великих файлах
            if (i % 1024) == 0:
                await asyncio.sleep(0)
            bar = _row_to_bar(row, idx, default_symbol, default_tf)
            if bar:
                yield bar


async def _async_jsonl_iter(
    path: Path, default_symbol: str, default_tf: str
) -> AsyncIterator[Bar]:
    with path.open("r", encoding="utf-8") as fh:
        for i, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            obj = json.loads(line)
            bar = _dict_to_bar(obj, default_symbol, default_tf)
            if bar:
                yield bar
            if (i % 2048) == 0:
                await asyncio.sleep(0)  # кооперативність


def _sync_parquet_iter(
    path: Path, default_symbol: str, default_tf: str
) -> Iterable[Bar]:
    df = pd.read_parquet(path)  # type: ignore
    # очікувані колонки: ts, open, high, low, close, [volume]
    cols = set(map(str, df.columns))
    ts_col = (
        "ts"
        if "ts" in cols
        else (
            "open_time"
            if "open_time" in cols
            else ("timestamp" if "timestamp" in cols else None)
        )
    )
    if ts_col is None:
        # немає відомої часової колонки — пропускаємо
        return []
    for r in df.itertuples(index=False):
        ts_val = getattr(r, ts_col, None)
        if ts_val is None:
            continue
        ts = int(ts_val)
        ts = ts * 1000 if ts < 10**12 else ts
        yield Bar(
            ts=ts,
            open=float(r.open),
            high=float(r.high),
            low=float(r.low),
            close=float(r.close),
            volume=float(getattr(r, "volume", 0.0)),
            symbol=str(getattr(r, "symbol", default_symbol)),
            timeframe=str(getattr(r, "timeframe", default_tf)),
        )


def _row_to_bar(
    row: list[str], idx: dict[str, int], default_symbol: str, default_tf: str
) -> Bar | None:
    try:
        ts_key = idx.get("ts")
        if ts_key is None:
            ts_key = idx.get("open_time")
        if ts_key is None:
            ts_key = idx.get("timestamp")
        if ts_key is None:
            return None
        ts_raw = row[ts_key]
        ts = int(float(ts_raw))
        ts = ts * 1000 if ts < 10**12 else ts
        o = float(row[idx["open"]])
        h = float(row[idx["high"]])
        low_v = float(row[idx["low"]])
        c = float(row[idx["close"]])
        v = float(row[idx.get("volume", -1)]) if "volume" in idx else 0.0
        symbol = row[idx["symbol"]] if "symbol" in idx else default_symbol
        tf = row[idx["timeframe"]] if "timeframe" in idx else default_tf
        return Bar(
            ts=ts,
            open=o,
            high=h,
            low=low_v,
            close=c,
            volume=v,
            symbol=symbol,
            timeframe=tf,
        )
    except Exception:
        LOG.debug("Пропуск некоректного CSV рядка: %s", row)
        return None


def _dict_to_bar(
    obj: dict[str, Any], default_symbol: str, default_tf: str
) -> Bar | None:
    try:
        # підтримуємо кілька варіантів часових ключів
        if "ts" in obj:
            ts = int(obj["ts"])
        elif "open_time" in obj:
            ts = int(obj["open_time"])
        elif "timestamp" in obj:
            ts = int(obj["timestamp"])
        else:
            return None
        ts = ts * 1000 if ts < 10**12 else ts
        open_v = float(obj["open"]) if "open" in obj else float(obj.get("o", 0.0))
        high_v = float(obj["high"]) if "high" in obj else float(obj.get("h", 0.0))
        low_v = float(obj["low"]) if "low" in obj else float(obj.get("l", 0.0))
        close_v = float(obj["close"]) if "close" in obj else float(obj.get("c", 0.0))
        vol_v = float(obj.get("volume", obj.get("v", 0.0)))
        return Bar(
            ts=ts,
            open=open_v,
            high=high_v,
            low=low_v,
            close=close_v,
            volume=vol_v,
            symbol=str(obj.get("symbol", default_symbol)),
            timeframe=str(obj.get("timeframe", default_tf)),
        )
    except Exception:
        LOG.debug("Пропуск некоректного JSONL рядка: %s", obj)
        return None


# ── Утиліти ──
def _pos_key(symbol: str, timeframe: str) -> str:
    return f"{NAMESPACE}:streamer:pos:{symbol}:{timeframe}"


# ── Адаптер з UnifiedDataStore.put_bars ──
async def _store_put_bar(store: Any, payload: dict[str, Any]) -> None:
    """Адаптує одиничний бар у формат DataFrame та викликає put_bars.

    Args:
        store: Інстанс UnifiedDataStore (очікує метод put_bars).
        payload: Dict з ключами ts, open, high, low, close, volume, symbol, timeframe.
    """
    # Ліниво імпортуємо pandas, якщо доступний
    global pd  # використовуємо вже імпортований псевдо-модуль вище
    if pd is None:
        raise RuntimeError("pandas потрібен для запису у UnifiedDataStore")
    ts = int(payload["ts"])
    # Формуємо мінімально необхідні колонки UnifiedDataStore
    df_row = pd.DataFrame(
        [
            {
                "open_time": ts,
                "open": float(payload["open"]),
                "high": float(payload["high"]),
                "low": float(payload["low"]),
                "close": float(payload["close"]),
                "volume": float(payload.get("volume", 0.0)),
                "close_time": ts + 60_000 - 1,
                "is_closed": True,
            }
        ]
    )
    sym = str(payload.get("symbol"))
    tf = str(payload.get("timeframe", "1m"))
    put = getattr(store, "put_bars", None)
    if put is None:
        raise AttributeError("UnifiedDataStore.put_bars відсутній")
    await put(sym, tf, df_row)
