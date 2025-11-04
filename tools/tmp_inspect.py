"""Інспекція тимчасових даних із аналітичними етапами.
Інспекція тимчасових даних із аналітичними етапами.
Цей модуль призначений для асинхронної інспекції часових рядів, представлених у форматі JSON Lines (JSONL).
Він дозволяє завантажити дані, обробити їх через два аналітичні етапи (stage 1 та stage 2), а також отримати
статистику щодо виявлених сигналів, включаючи розподіл типів сигналів та приклади аномальних подій.
Основні можливості:
- Завантаження часових рядів із JSONL-файлу у pandas DataFrame.
- Підтримка ковзного вікна для аналізу даних із warmup-періодом.
- Інтеграція з кастомним кешем та state manager для імітації робочого середовища.
- Використання двох аналітичних етапів:
    - Stage 1: Виявлення аномалій та генерація сигналів на основі статистики.
    - Stage 2: Подальша обробка сигналів для отримання фінального payload.
- Збір та вивід статистики по типах сигналів.
- Виявлення та вивід прикладів сигналів, що починаються з "ALERT".
    path (str): Шлях до JSONL-файлу для інспекції. Кожен рядок файлу — окремий запис із часовими мітками та іншими полями.
    1. Завантаження даних із файлу у DataFrame.
    2. Ініціалізація in-memory кешу та dummy state manager.
    3. Ітерування по даних із наростаючим вікном (ковзне вікно).
    4. Для кожного вікна після warmup:
        - Оновлення статистики через stage 1.
        - Перевірка на аномалії та генерація сигналу.
        - Обробка сигналу через stage 2.
        - Збереження результату (часова мітка, сигнал, результат stage 2).
    5. Підрахунок розподілу типів сигналів.
    6. Вивід до 5 прикладів сигналів, що починаються з "ALERT".
    - Вхідний файл повинен містити стовпець "open_time" (часова мітка).
    - Підтримується асинхронний запуск через asyncio.
Приклад структури вхідного JSONL-файлу:
    {"open_time": 1698451200000, "open": 1.23, "high": 1.25, "low": 1.22, "close": 1.24, "volume": 100}
    {"open_time": 1698451260000, "open": 1.24, "high": 1.26, "low": 1.23, "close": 1.25, "volume": 110}
    ...
Приклад використання:
    # Виклик інспекції для конкретного файлу
    asyncio.run(inspect("datastore/replay/melaniausdt_2025-10-28T02-16-00Z_replay_1m.jsonl"))
    # Очікуваний вивід:
    # Signal distribution: {'ALERT_SPIKE': 3, 'NORMAL': 97}
    # Total ALERT-like signals: 3
    # {
    #   "ts": 1698451320000,
    #   "signal": {"signal": "ALERT_SPIKE", ...},
    #   "stage2": {...}
    # }
    # ...
Виводить статистику щодо виявлених сигналів.
"""
import asyncio
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stage1.asset_monitoring import AssetMonitorStage1  # noqa: E402
from stage2.processor import Stage2Processor  # noqa: E402
from tools.replay_stream import DummyStateManager, InMemoryCache  # noqa: E402


async def inspect(path: str) -> None:
    """
    Асинхронно інспектує JSONL-файл із часовими рядами, обробляє його через два аналітичні етапи
    та виводить статистику щодо виявлених сигналів.

    Аргументи:
        path (str): Шлях до JSONL-файлу для інспекції.

    Робочий процес:
        1. Завантажує дані з вказаного JSONL-файлу у DataFrame pandas.
        2. Ініціалізує in-memory кеш та dummy state manager для обробки.
        3. Ітерує дані, підтримуючи ковзне вікно для аналізу.
        4. Для кожного вікна (після warmup) оновлює статистику та перевіряє аномалії через stage 1.
        5. Обробляє отриманий сигнал через stage 2.
        6. Збирає результати: часові мітки, сигнали та вихід stage 2.
        7. Обчислює та виводить розподіл типів сигналів.
        8. Виявляє та виводить до 5 прикладів сигналів, що починаються з "ALERT".

    Примітки:
        - Передбачає наявність класів/функцій: InMemoryCache, DummyStateManager, AssetMonitorStage1, Stage2Processor.
        - Вхідний файл має бути у форматі JSON Lines, кожен рядок — окремий запис.
        - DataFrame повинен містити стовпець "open_time" для часових міток.
    """
    df = pd.read_json(path, lines=True)
    cache = InMemoryCache()
    state_manager = DummyStateManager()
    stage1 = AssetMonitorStage1(cache_handler=cache, state_manager=state_manager)
    stage2 = Stage2Processor(state_manager=state_manager)
    warmup = max(stage1.atr_manager.period + 1, stage1.volumez_manager.window + 1)
    results: list[dict] = []
    for idx in range(len(df)):
        window = df.iloc[: idx + 1].copy()
        if len(window) < warmup:
            continue
        for bucket in (stage2.bars_1m, stage2.bars_5m, stage2.bars_1d):
            bucket["MELANIAUSDT"] = window
        stats = await stage1.update_statistics("MELANIAUSDT", window)
        signal = await stage1.check_anomalies("MELANIAUSDT", window, stats)
        stage2_payload = await stage2.process(signal)
        ts_raw = window["open_time"].iloc[-1]
        if hasattr(ts_raw, "timestamp"):
            ts_ms = int(ts_raw.timestamp() * 1000)
        else:
            ts_ms = int(ts_raw)
        results.append(
            {
                "ts": ts_ms,
                "signal": signal,
                "stage2": stage2_payload,
            }
        )
    counts: dict[str, int] = {}
    for row in results:
        sig_type = str(row.get("signal", {}).get("signal", "UNKNOWN"))
        counts[sig_type] = counts.get(sig_type, 0) + 1
    print("Signal distribution:", counts)
    alerts = [
        row
        for row in results
        if str(row.get("signal", {}).get("signal", "")).upper().startswith("ALERT")
    ]
    print(f"Total ALERT-like signals: {len(alerts)}")
    for row in alerts[:5]:
        print(json.dumps(row, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    target = Path("datastore/replay/melaniausdt_2025-10-28T02-16-00Z_replay_1m.jsonl")
    asyncio.run(inspect(str(target)))
