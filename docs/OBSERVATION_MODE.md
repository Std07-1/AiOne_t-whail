# Режим спостереження (без торгів)

> TL;DR (як запустити на 60 хв):

```powershell
# 1) (опційно) паблішери, щоб заповнювати Redis
 .\.venv\Scripts\python.exe -m tools.htf_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --htf M5,M15 --publish-redis
 .\.venv\Scripts\python.exe -m tools.whale_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --cadence-ms 3000

# 2) пайплайн — єдиний власник /metrics:9108 (60–62 хв + лог у файл)
 .\.venv\Scripts\python.exe -m tools.run_window `
   --duration 3720 `                          # 62 хв (рекомендовано ≥ monitor)
   --set PROM_GAUGES_ENABLED=true `
   --set SCENARIO_TRACE_ENABLED=true `
   --log reports\run_window.log

# 3) монітор — кожні 15 хв знімає /metrics і Redis у reports/
 .\.venv\Scripts\python.exe -m tools.monitor_observation `
   --symbols BTCUSDT,TONUSDT,SNXUSDT `
   --interval-min 15 `
   --duration-min 62 `                        # +2 хв буфер для останнього тіку
   --out-dir reports
```

Примітка: тримайте `run_window --duration` ≥ тривалості монітора, щоб endpoint `/metrics` був доступний під час фінального знімка.


Мета: керовано запустити пайплайн на вікно часу, знімати живі метрики Prometheus, дампити Redis‑стан, збирати forward‑validation артефакти (quality.csv) — без змін контрактів Stage1/2/3.

Цей гайд описує, як:
- запускати пайплайн через `tools.run_window` (керована тривалість, прапори, лог‑файл),
- запускати зовнішній монітор `tools.monitor_observation` (кожні 15 хв знімає /metrics, Redis‑стани, перегенерує quality.csv),
- перевіряти живі метрики (`presence`, `htf_strength`, `ai_one_phase`, `ai_one_scenario`),
- де шукати артефакти у `reports/` і як їх читати,
- як зупинятися/відкотитися.

---

## 1) Прапори й залежності

Джерело правди — `config/config.py` та `config/flags.py` (не env):
- `PROM_GAUGES_ENABLED=True`, `PROM_HTTP_PORT=9108`
- `SCENARIO_TRACE_ENABLED=True`
- `SCEN_EXPLAIN_ENABLED=True`, `SCEN_EXPLAIN_VERBOSE_EVERY_N=20`
- `STRICT_SCENARIO_HYSTERESIS_ENABLED=True`
- `SCENARIO_CANARY_PUBLISH_CANDIDATE=False`

Додаткові процеси:
- `tools.htf_publisher` — M1→M5/M15 агрегатор із публікацією HTF у Redis/метрики.
- `tools.whale_publisher` — оновлювач whale‑presence у Redis/метрики.

> Контракти Stage1/2/3 не змінюються. Нові дані — лише у `market_context.meta.*` та Prometheus‑метрики.

Важливо про Prometheus (/metrics):
- Єдиний HTTP‑експортер стартує всередині пайплайна (`app.main`, зручно через `tools.run_window`).
- Зовнішні інструменти (HTF/Whale publishers) не стартують власний HTTP‑експортер; вони лише оновлюють Redis‑стан. Видимі на `/metrics` значення `presence` та `htf_strength` виставляє саме пайплайн під час обробки батчів, зчитуючи Redis/state.

---

## 2) Запуск паблішерів (PowerShell)

Запустіть в окремих вкладках термінала (адаптуйте шлях venv за потреби):

```powershell
# HTF Publisher (M5,M15)
.\.venv\Scripts\python.exe -m tools.htf_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --htf M5,M15 --publish-redis

# Whale Publisher (cadence 3s)
.\.venv\Scripts\python.exe -m tools.whale_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --cadence-ms 3000
```

Очікування:
- у Redis з’являються ключі стану `ai_one:state:{SYMBOL}` та батчі барів `ai_one:bars:{SYMBOL}:{TF}`;
- на `/metrics` метрики з’являться після запуску п.3 (їх виставляє пайплайн, не паблішери).

---

## 3) Керований запуск основи (run_window)

`tools.run_window` запускає пайплайн на фіксований час і завершує його коректно.

```powershell
.\.venv\Scripts\python.exe -m tools.run_window `
  --duration 3600 `                           # 60 хвилин
  --set PROM_GAUGES_ENABLED=true `            # вмикаємо метрики
  --set SCENARIO_TRACE_ENABLED=true `         # вмикаємо TRACE
  --set STRICT_SCENARIO_HYSTERESIS_ENABLED=true `
  --set SCENARIO_CANARY_PUBLISH_CANDIDATE=false `
  --log reports\run_window.log                # дзеркалити stdout/stderr у файл
```

Під час роботи endpoint метрик: http://localhost:9108/metrics (активний, доки процес живий).

---

## 4) Зовнішній монітор (кожні 15 хв)

`tools.monitor_observation` — окремий процес, що кожні N хвилин:
- знімає `/metrics` → `reports/metrics_YYYYmmdd_HHMM.txt`,
- дампить `ai_one:state:{SYMBOL}` → `reports/state_{SYMBOL}_YYYYmmdd_HHMM.txt`,
- оновлює `reports/quality.csv` за останні 4 години.

```powershell
\.venv\Scripts\python.exe -m tools.monitor_observation `
  --symbols BTCUSDT,TONUSDT,SNXUSDT `
  --interval-min 15 `
  --duration-min 62 `      # +2 хв буфер; або 2880 для 48 год
  --out-dir reports
```

Монітор пише `reports/monitor.pid` і heartbeat у stdout: `MONITOR_TICK ts=... symbols=...`.

---

## 5) Перевірка /metrics

Поки процеси працюють, перевірте наявність мінімальних гейджів:

```powershell
# Перші рядки /metrics (швидка перевірка доступності)
(Invoke-WebRequest -UseBasicParsing http://localhost:9108/metrics).Content | Select-Object -First 50

# Вибірково фільтруємо ключові гейджі
(Invoke-WebRequest -UseBasicParsing http://localhost:9108/metrics).Content |
  Select-String -SimpleMatch -Pattern 'htf_strength{symbol=', 'presence{symbol=', 'ai_one_phase{phase=', 'ai_one_scn_reject_total{', 'ai_one_context_near_edge_persist{', 'ai_one_context_presence_sustain{'
```

Очікування (метрики на сторінці — лише від пайплайна):
- `htf_strength{symbol="..."}` — числові значення 0..1
- `presence{symbol="..."}` — числові значення 0..1
- `ai_one_phase{phase=...,symbol=...}` — фаза/оцінка кожен цикл
- `ai_one_scenario{scenario=...,symbol=...}` — лише для стабільних сценаріїв (канарейка вимкнена).
 - `ai_one_scn_reject_total{symbol=...,reason=...}` — лічильник відмов (`no_candidate`) і деактивацій (`htf_weak`, `presence_drop`, `phase_change`, `confidence_decay`).
 - `ai_one_context_near_edge_persist{symbol=...}` — агрегат «біля зони × персистентність» (Observability v1).
 - `ai_one_context_presence_sustain{symbol=...}` — агрегат «сталий presence за кілька циклів» (Observability v1).

> Якщо endpoint недоступний — перевірте, що `run_window`/`app.main` запущені з `PROM_GAUGES_ENABLED=true` і порт не зайнятий.

---

## 6) Де лежать артефакти і як читати

Каталог `reports/` заповнюється автоматично:
- `run_window.log` — сумарний лог керованого запуску.
- `metrics_YYYYmmdd_HHMM.txt` — зрізи /metrics (по одному файлу на кожен тік монітора).
- `state_{SYMBOL}_YYYYmmdd_HHMM.txt` — дампи Redis‑станів (поля типу `htf:strength`, `vol_z`, `whale=...`).
- `quality.csv` — forward‑validation за останні 4 години:
 - `quality.csv` — forward‑validation за останні 4 години:
  - базові: `symbol, scenario, activations, mean_conf, p75_conf, mean_time_to_stabilize_s, btc_gate_effect_rate`;
  - explain‑розширення (за наявності логів `[SCEN_EXPLAIN]`):
    - `in_zone`, `compression_level`, `persist_score`, `btc_regime_at_activation`, `quality_proxy`;
  - `explain_coverage_rate`, `explain_latency_ms`;
    - `reject_top1, reject_top2, reject_top3`, а також `reject_rate_top1, reject_rate_top2, reject_rate_top3`.
- (Після завершення `--duration-min`) `quality_summary.json` — агрегований підсумок по символах/сценаріях.

Підсвіт BTC‑gate:
- `btc_gate_effect_rate` — частка активацій, біля яких у TRACE фіксувався `penalty=0.2` (вікно ±30с).

### 6.1) Пояснювальні логи [SCEN_EXPLAIN]

Під час роботи пайплайн рейт‑обмежено пише пояснення до рішень селектора у лог (і дублює короткий підсумок у `market_context.meta.scenario_explain`). Приклад:

```
[SCEN_EXPLAIN] symbol=BTCUSDT scenario=pullback_continuation conf=0.62 explain="in_zone=1; compression=0.74; persist=0.68; htf=0.41; dvr=1.35; volz=2.6; btc_regime=trend_up"
```

Інструмент `tools.scenario_quality_report` споживає ці рядки для заповнення explain‑колонок у `quality.csv` (див. перелік вище). Шукайте їх у `reports\\run_window.log` і stdout `run_window`.

Порада: для швидкої перевірки explain‑покриття використайте `tools.verify_explain`:

```powershell
\.venv\Scripts\python.exe -m tools.verify_explain --last-min 60 --logs-dir .\logs
```

---

## 7) Зупинка та відкат

- Зупинка: Ctrl+C у відповідних вікнах термінала.
- Монітор можна завершити, використавши PID із `reports/monitor.pid`.
- Відкат фіч: у `config/config.py`:
  - `SCEN_BTC_SOFT_GATE=False`
  - `PROM_GAUGES_ENABLED=False`
  - Канарейку не вмикати: `SCENARIO_CANARY_PUBLISH_CANDIDATE=False`.

---

## 8) Траблшутинг

- `/metrics` порожній або недоступний:
  - Переконайтесь, що `PROM_GAUGES_ENABLED=True` і `run_window` (пайплайн) працює — тільки він тримає HTTP‑експортер 9108.
  - Перевірте порт 9108 (блокування фаєрволом/конфлікт портів).
- Немає `presence{symbol=...}` або `htf_strength{symbol=...}`:
  - Переконайтесь, що `tools.whale_publisher` та `tools.htf_publisher` працюють і оновлюють Redis‑стан (`ai_one:state:{SYMBOL}` поля `whale`, `htf_strength`).
  - Дайте пайплайну 1–2 цикли: саме він прочитає Redis/state та виставить метрики. У логах пайплайна з’являється рейт‑обмежений запис: `[PROM] presence set symbol=... value=...`.
- `quality.csv` порожній:
  - Немає стабільних `[SCENARIO_ALERT] activate=...` в останні 4 години — зачекайте або розширте вікно.

---

## 9) Критерії прийняття (операційні)

- `/metrics` містить живі рядки:
  - `^presence{symbol=`, `^htf_strength{symbol=`, `^ai_one_phase{phase=`
  - `^ai_one_scn_reject_total{symbol=` (після перших TRACE/деактивацій)
  - `^ai_one_context_near_edge_persist{symbol=` і `^ai_one_context_presence_sustain{symbol=` (контекстні гейджі)
- У `reports/` є:
  - щонайменше один `metrics_*.txt`,
  - `state_{BTCUSDT,TONUSDT,SNXUSDT}_*.txt`,
  - актуальний `quality.csv` (за можливості — з explain‑колонками).
 - У `reports\\run_window.log` присутні рядки `[SCEN_EXPLAIN]` під час активності селектора.
- Контракти Stage1/2/3 — без змін.

---

## 10) Приклад швидкого старту (62 хв, 3 символи)

```powershell
# 1) Паблішери (оновлюють Redis; HTTP /metrics не стартують)
.\.venv\Scripts\python.exe -m tools.htf_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --htf M5,M15 --publish-redis
.\.venv\Scripts\python.exe -m tools.whale_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --cadence-ms 3000

# 2) Керований запуск пайплайна (рекомендовано 62 хв) — єдиний власник /metrics:9108
.\.venv\Scripts\python.exe -m tools.run_window `
  --duration 3720 `
  --set PROM_GAUGES_ENABLED=true `
  --set SCENARIO_TRACE_ENABLED=true `
  --set STRICT_SCENARIO_HYSTERESIS_ENABLED=true `
  --set SCENARIO_CANARY_PUBLISH_CANDIDATE=false `
  --log reports\run_window.log

# 3) Зовнішній монітор (62 хв, тік=15 хв)
.\.venv\Scripts\python.exe -m tools.monitor_observation --symbols BTCUSDT,TONUSDT,SNXUSDT --interval-min 15 --duration-min 60 --out-dir reports

> Порада: монітор краще запускати на 62 хв, а пайплайн — на 62–65 хв, щоб гарантовано зняти фінальний 15‑хв тик з активним `/metrics`.

# 4) Перевірка метрик (видимі, доки працює п.2 – пайплайн)
(Invoke-WebRequest -UseBasicParsing http://localhost:9108/metrics).Content |
  Select-String -SimpleMatch -Pattern 'htf_strength{symbol=', 'presence{symbol=', 'ai_one_phase{phase='
```

---

## 11) Глибокі снапшоти стану і підсумкові звіти (180 хв)

Для довшого керованого прогону (≈3 години) з єдиним експортером /metrics:9108 і розширеними снапшотами `market_context.meta.*` використовуйте:

1) Пайплайн (без зміни контрактів), із пояснювальними логами та гістерезисом; контекст‑ваги вимкнено на ітерацію:

```powershell
.\.venv\Scripts\python.exe -m tools.run_window `
  --duration 10815 `
  --set PROM_GAUGES_ENABLED=true `
  --set SCENARIO_TRACE_ENABLED=true `
  --set STRICT_SCENARIO_HYSTERESIS_ENABLED=true `
  --set SCEN_EXPLAIN_ENABLED=true `
  --set SCEN_CONTEXT_WEIGHTS_ENABLED=false `
  --log reports\run_window.log
```

2) Паралельно — монітор із глибоким станом (кожні 15 хв):

```powershell
.\.venv\Scripts\python.exe -m tools.monitor_observation `
  --symbols BTCUSDT,TONUSDT,SNXUSDT `
  --interval-min 15 `
  --duration-min 180 `
  --out-dir reports `
  --deep-state
```

Монітор додатково створює:
- `reports/state_{SYMBOL}_YYYYmmdd_HHMM_context.json` (context_frame / context)
- `reports/state_{SYMBOL}_YYYYmmdd_HHMM_structure.json`
- `reports/state_{SYMBOL}_YYYYmmdd_HHMM_evidence.json`
- `reports/state_{SYMBOL}_YYYYmmdd_HHMM_explain.txt`

3) Після завершення процесів згенеруйте підсумкові звіти:

```powershell
.\.venv\Scripts\python.exe -m tools.scenario_quality_report --last-hours 4 --out reports\quality.csv
.\.venv\Scripts\python.exe -m tools.verify_explain --last-min 180 --logs-dir .\logs | Out-File -FilePath reports\verify_explain.txt -Encoding UTF8
.\.venv\Scripts\python.exe -m tools.quality_snapshot --csv reports\quality.csv --metrics-dir reports --out reports\quality_snapshot.md
.\.venv\Scripts\python.exe -m tools.quality_dashboard --csv reports\quality.csv --out reports\quality_dashboard.md
```

Все це працює без змін Stage1/Stage2/Stage3; нові дані — тільки у `market_context.meta.*`, логи й Prometheus‑метрики.

