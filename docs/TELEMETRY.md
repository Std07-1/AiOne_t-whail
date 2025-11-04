# Телеметрія та аналітика

Опис інструментів для офлайн/онлайн аналізу телеметрії, дашбордів і live‑перевірок стану.

## 1) Аналізатор телеметрії (tools.analyze_telemetry)

Призначення:
- Збір і агрегація метрик із JSONL у `telemetry/`.
- Звіт Markdown/JSON (`--out report.md --json report.json`).
- Розділ «Symbol last phases»: останні виявлені фази з ключовими показниками і підказками Stage2 (hints).

Запуск (приклад):
```
python -m tools.analyze_telemetry --symbol BTCUSDT --out telemetry_report_btc.md --json telemetry_report_btc.json
```

Вихід:
- Markdown із секціями (огляд, ключові аномалії, Symbol last phases, тощо).
- JSON для подальшої обробки/дашбордів.

## 2) Live‑стан у Redis (ручна перевірка)

UI‑publisher публікує агрегований стан у канонічні ключі/канали Redis з TTL. Для ручної перевірки:
- Формуйте ключі через `config/keys.py` (наприклад, state‑HSET для конкретного символу).
- Перевіряйте HSET у Redis CLI/GUI: наявність полів (`phase`, `vol_z`, `dvr`, `cd`, `htf_ok`, тощо) та актуальний TTL.
- Fallback поля обробляються на стороні читачів (UI/аналітика), тож у сховищі зберігаються канонічні назви.

## 3) Реплей інсайти

У процесі реплею створюється файл `telemetry/replay_insights_*.jsonl` із рядками формату:
- `ts`, `ts_ms`, `symbol`, `interval`,
- `signal` (Stage1), `whale_presence`, `whale_bias`, `vwap_deviation`, `watch_tags`,
- наближеність до краю коридору (`near_edge`, `dist_to_edge_pct`).

Ці інсайти зручно переглядати для швидкого sanity‑чеків без читання повних Stage2Output.

## 4) Фазний детектор та «тихий тренд» (participation‑light)

- Легке визначення фаз (momentum, exhaustion, false_breakout, pre/post_breakout, drift_trend) здійснюється у Stage2‑lite на базі полів `stats` + контекст (HTF, рівні, волатильність).
- «Тихий тренд» (`drift_trend`) активується фіче‑флагом `FEATURE_PARTICIPATION_LIGHT` і телеметрійно позначається у UI через `market_context.meta.insights.quiet_mode` (`quiet_score`).
- Пороги зберігаються у конфігурації Stage2 (див. `config/config_stage2.py`: `PARTICIPATION_LIGHT_THRESHOLDS`).

## 5) Найкращі практики

- Завжди вказуйте достатній прогрів для Stage1 (див. `tools.replay_stream.py` — `warmup`).
- Додавайте нові поля лише в `market_context.meta.*`, щоб не ламати контракти Stage1/Stage2.
- Всі ключі Redis формуйте через `config/keys.py`; TTL — із `config/config.py`.

---

## 6) Prometheus-метрики та лічильники (HTTP /metrics)

Єдиний HTTP-експортер /metrics піднімається всередині пайплайна (див. `tools.run_window`, прапор `PROM_GAUGES_ENABLED=True`). Паблішери (HTF/Whale) не стартують HTTP — лише оновлюють Redis.

Доступні гейджі (best-effort, no-op фолбек якщо відсутній пакет `prometheus_client`):
- `htf_strength{symbol}` — сила HTF 0..1.
- `presence{symbol}` — whale presence 0..1 (виставляється пайплайном на основі Redis/state; лог `[PROM] presence set ...` рейт-обмежений).
- `ai_one_phase{symbol,phase}` — оцінка фази 0..1.
- `ai_one_scenario{symbol,scenario}` — впевненість стабільного сценарію 0..1 (канарейка вимкнена за замовч.).
- `ai_one_context_near_edge_persist{symbol}` — контекстна метрика (0..1).
- `ai_one_context_presence_sustain{symbol}` — контекстна метрика (0..1).
- `ai_one_btc_regime{state}` — one-hot стан BTC режиму.
- `low_atr_override_active{symbol}`, `low_atr_override_ttl{symbol}` — стани Low-ATR override.
 - `ai_one_explain_lines_total{symbol}` — лічильник фактично записаних explain-рядків.

Лічильники:
- `ai_one_scn_reject_total{symbol,reason}` — лічильник відмов/деактивацій сценарію з причиною низької кардинальності:
	- У TRACE-відмові кандидата: `reason="no_candidate"`.
	- При деактивації стабільного сценарію: `reason∈{"htf_weak","presence_drop","phase_change","confidence_decay"}` — визначається під час аналізу останніх барів у гістерезисі сценарію.

Примітки реалізації:
- Ініціалізація метрик виконується ледачо, збій/відсутність клієнта — no-op і не впливає на пайплайн.
- Лічильники інкрементуються без винятків (best-effort), усі виклики захищені try/except.

Приклад швидкої перевірки в PowerShell:

```powershell
(Invoke-WebRequest -UseBasicParsing http://localhost:9108/metrics).Content |
	Select-String -SimpleMatch -Pattern 'ai_one_scn_reject_total{', 'ai_one_phase{phase=', 'presence{symbol='
```

## EvidenceBus та пояснення сценарію

У процесі селекції сценарію збираємо “докази” у буфер EvidenceBus на символ:

- market_context.meta.evidence: список пунктів {key,value,weight,note}.
- market_context.meta.scenario_explain: короткий рядок із топ-3 пунктів за вагою.

Додатково раз на ≤10 с/символ (і кожен N-й батч) логуються пояснення у компактному форматі:

```
[SCEN_EXPLAIN] symbol=btcusdt scenario=pullback_continuation explain="near_edge_persist=0.67 (w=1.00); compression.index=0.12 (w=0.70); btc_regime_v2=flat (w=0.60)"
```

Керування логуванням explain:

- `config/config.py` прапори:
	- `SCEN_EXPLAIN_ENABLED=True`
	- `SCEN_EXPLAIN_VERBOSE_EVERY_N=20` — кожен N-й батч для символу, навіть якщо ще не минуло 10 с.
- Рейт-ліміт реалізовано в `app/process_asset_batch.py` через `_explain_should_log` + пер-символьний лічильник батчів.

Аналітика explain у CSV:

- Парсер `tools/scenario_quality_report.py` використовує ці рядки для збагачення CSV колонки: `in_zone`, `compression_level`, `persist_score`, `btc_regime_at_activation`, `quality_proxy`.
- Додано метрики покриття explain: `explain_coverage_rate` (частка активацій із explain у ±30 с) та `explain_latency_ms` (від першого TRACE кандидата до найближчого explain).

Швидка верифікація explain у логах:

- Утиліта `tools.verify_explain.py`:
	- `--last-min 60 --logs-dir ./logs`
	- Виводить `explain_lines_total`, `symbols_seen`, `coverage_by_symbol` та `recent_examples` (3 останні explain-рядки).
