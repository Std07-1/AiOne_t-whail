# ep_2 (спрощений модуль)

ПОТОЧНИЙ СТАТУС (2025-09): ВНУТРІШНЯ ГЕНЕРАЦІЯ СИГНАЛІВ ПОВНІСТЮ ВИДАЛЕНА.

Мінімальний робочий конвеєр:
```
load_bars_auto -> (external signals OPTIONAL) -> find_episodes_v2 -> (snapshots LZ4) -> visualize_episodes
```

Тобто модуль більше НЕ створює жодних Stage1 сигналів. Якщо передати DataFrame сигналів через параметр `signals` у `run_pipeline`, вони будуть використані (побудується entry_mask та будуть показані на графіку). Якщо ні – епізоди шукаються лише за ціною.

Залишені: завантаження OHLCV, пошук епізодів, простий графік, опціональні снапшоти, діагностика індексу та цін.
Видалено: генерація сигналів, coverage, alignment, clustering, adaptive thresholds, audit, складні метрики.

## Основні файли
- `data_loader.py` – отримання OHLCV.
- `episode_finder.py` – детектор рухів (UP/DOWN) з опційним retrace.
- `episodes.py` – оркестрація конвеєра (приймає зовнішні сигнали, але не генерує).
- `visualize_episodes.py` – спрощена візуалізація.
- `core.py` – моделі / конфіг / утиліти runup/drawdown.
- `signal_utils.py` – залишено лише для історичної довідки (не використовується).

## Локальне логування
Кожний модуль має власний локальний logger:
```python
logger = logging.getLogger("ep2.<module>")
if not logger.handlers:
  logger.setLevel(logging.DEBUG)  # або INFO
  h = logging.StreamHandler()
  h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
  logger.addHandler(h)
  logger.propagate = False
```
Жодних централізованих менеджерів або фабрик – лише ізольовані блоки. Якщо присутній `rich` – може бути використаний `RichHandler`, але це не обов'язково.

## Snapshots
Опційно три LZ4 JSON файли (`meta_*`, `episodes_*`, `signals_*`) – зберігаються тільки якщо встановлено `lz4`.


## Запуск
```bash
python -m ep_2.episodes
```

## Можливі наступні кроки (не реалізовано)
- Версія схеми снапшотів
- Plain JSON дубль без компресії
- Простий CLI аргументи (symbol, timeframe)

Документ свідомо мінімальний, щоб відповідати скороченій кодовій базі.

## Потік Даних (Data Flow)
```
raw_data snapshot / REST -> data_loader.load_bars_auto -> DataFrame
  -> (optional) external_signals (ваша система) -> entry_mask
    -> episode_finder.find_episodes_v2 -> episodes
      -> (optional) snapshots LZ4
        -> visualize_episodes (опціонально)
```
Будь-яка логіка побудови сигналів винесена назовні. Очікується колонка `ts` (UTC) що збігається з індексом барів.

## Snapshot Caching (raw_data.py)
- 24h snapshot локально (JSONL/LZ4 або plain) використовується для швидкого старту без повторних REST викликів.
- Backward pagination: витягуємо нові свічки поки не досягнута глибина або ліміт часу.
- Retry/Backoff: експоненційна затримка при помилках мережі.
- Механізм розширює snapshot назад у часі й оновлює передні (свіжі) бари при потребі.
- Rationale: мінімізувати REST-запити, забезпечити стабільність та детерміновану точку відліку.

## Сигнали (зовнішні)
Alignment не виконується всередині. Ви подаєте вже нормалізовані позначки часу. Буде використано точне перетинання індексу з колонкою `ts` для побудови маски.

## Coverage / Alignment / Clustering
Повністю видалено з мінімальної версії.

## Extension Points
- Adaptive thresholds (у майбутньому)
- Додаткові діагностичні метрики (volatility regime)
- Подальша оптимізація продуктивності

## Приклад Оркестрації
Виконання: `python -m ep_2.episodes`
1. Читає конфіг (`EPISODE_CONFIG_1M`, `EPISODE_CONFIG_5M`).
2. Завантажує дані.
3. (Опційно) отримує зовнішній DataFrame сигналів.
4. Запускає пошук епізодів.
5. (Опційно) зберігає снапшоти та візуалізує.

## Продуктивність
Пошук епізодів: лінійний з внутрішньою ітерацією до `max_bars`. Додано DEBUG діагностику: середній/мін/макс рух, gaps, coverage_ratio, top moves.

## Логування (поточний принцип)
Тільки локальні логгери з мінімальним шаблоном. Ніяких rate-limiter / dispatcher / registry.

## Константи
Ключові константи у `config_episodes.py`:
- `DEFAULT_SIGNAL_ALIGN_TOLERANCE_RATIO`
- `DEFAULT_GLOBAL_COVERAGE_WARN_RATIO`
- `DEFAULT_INSIDE_BUFFER_MINUTES`
- `DEFAULT_HIGHLIGHT_EDGE_MINUTES`
- `SYSTEM_SIGNAL_CONFIG`

## Безпека / Надійність
- Перевірка NaN перед алгоритмами.
- Обмеження `max_bars` запобігає нескінченним пошукам.
- Мережеві виклики із retry у fetch шарі.

## TODO / Майбутні Ідеї
- SELL сигналізація (детальні умови).
- Повна async генерація Stage1 статистик.
- Додаткові класифікаційні фічі (volatility regime, trend slope).
- Візуалізація heatmap для сигналів поза епізодами.

## Cleanup / Changes (2025-09)
Останні зміни:
* Видалено внутрішню генерацію сигналів.
* Видалено coverage / alignment / clustering.
* Додано діагностику індексу (`_index_diagnostics`) і цін (`_price_diagnostics`).
* Додано епізодні метрики (gaps, coverage_ratio, top moves).

## Видалені секції (історія)
Старі розділи (coverage, alignment, adaptive tuning, audit, external tuning YAML приклади) вилучені з активної документації для ясності.

---
Автор: Std07-1
