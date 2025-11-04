# Архітектура AiOne_t (огляд)

Цей документ описує потоки даних та ключові модулі системи AiOne_t з фокусом на контракти, ключі Redis та телеметрію.

## Пайплайн Stage1 → Stage2 → Stage3

- Stage1 (Тригери):
  - Зчитує ринкові дані (свічки), рахує статистики (ATR, volume Z‑score, corridor band %, dist_to_edge, rsi, тощо).
  - Формує Stage1Signal: `{ symbol, signal: ALERT|NORMAL, trigger_reasons[], stats{...}, thresholds? }`.
  - Контракти сталi. Канонічні ключі — у `config/constants.py`.

- Stage2 (Агрегація/контекст/фази):
  - Агрегує Stage1 + контекст (levels, HTF alignment, volatility regime).
  - Легкий фазний детектор (для реплею/телеметрії): визначає `market_context.phase` і `phase_score`.
  - Нові дані пишуться лише в `market_context.meta.*` (наприклад: `meta.htf_ok`, `meta.volatility_regime`, `meta.insights.quiet_mode`).
  - Фіче‑флаги — у `config/flags.py` (rollback‑дружні).

- Stage3 (Угоди/життєвий цикл):
  - Використовує Stage2Output для прийняття рішень (поза рамками цього документа).

## Телеметрія і аналіз

- Реплей (`tools/replay_stream.py`): офлайн прогін Stage1→Stage2 з збереженням:
  - `stage1_signals.jsonl`, `stage2_outputs.jsonl`, `summary.csv`, `kpi.json`.
  - Інсайти у `telemetry/replay_insights_*.jsonl`.
- Аналізатор (`tools/analyze_telemetry.py`):
  - Готує звіти Markdown/JSON по телеметрії, включно з розділом «Symbol last phases».
- Live‑стан UI у Redis:
  - Публікація здійснюється UI‑publisherʼом у канонічні ключі (`ai_one:channel:ui:*` та state‑HSET за символом) з TTL.
  - Для ручної перевірки використовуйте `config/keys.py` як єдине джерело формування ключів і стандартні інструменти Redis (CLI/GUI) для читання HSET.

## Контракти даних (важливо)

- Stage1Signal — обовʼязкові: `symbol`, `signal`, `trigger_reasons`, `stats`.
- Stage2Output — обовʼязкові: `symbol`, `market_context`, `recommendation`, `narrative`, `confidence_metrics`, `risk_parameters`, `anomaly_detection`.
- Будь‑які нові поля додаємо ТІЛЬКИ в `market_context.meta.*` або `confidence_metrics`.

## Redis‑ключі

- Формуються лише через `config/keys.py`. Формат: `ai_one:{domain}:{symbol}:{granularity}`.
  - Приклади: `ai_one:stats:btcusdt:1m`, `ai_one:levels:ethusdt:1m`, канали `ai_one:channel:ui:asset_state`.
- TTL беруться з `config/config.py` (`INTERVAL_TTL_MAP`, `REDIS_CACHE_TTL`). Ніколи не хардкодимо ключі/TTL у коді.

## Фіче‑флаги (вибірка)

- `FEATURE_PARTICIPATION_LIGHT` — вмикає класифікацію «тихий тренд (drift_trend)» у фазному детекторі (телеметрія/реплей/UI‑інсайти).
- Directional/Whale/Crisis — див. `docs/FEATURE_FLAGS.md` та статус‑доки.

## Продуктивність і логи

- Бюджет latency: ≤ +20% до пікових `avg_wall_ms`. Важкі розрахунки — у `cpu_pool`.
- Логи суворі та інформативні (RichHandler):
  - `[STRICT_PHASE]`, `[STRICT_WHALE]`, `[STRICT_GUARD]`, `[VOL_REGIME]` тощо.
  - Жодних чутливих даних у логах.

## UI та пейлоад

- Схему див. `docs/ui_payload_schema_v2.md`.
- Новий інсайт для «тихого тренду»: `market_context.meta.insights.quiet_mode(+quiet_score)` — опційний, для візуалізації.

## Швидкі посилання

- Реплей — `docs/README_historical_replay.md`.
- Телеметрія/аналіз — `docs/TELEMETRY.md`.
- Фіче‑флаги — `docs/FEATURE_FLAGS.md`.
- Redis‑ключі — `docs/REDIS_KEYS.md`.
- Directional/Whale/Crisis — див. статус‑документи у `docs/`.
