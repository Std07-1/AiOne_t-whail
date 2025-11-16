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
- «Тихий тренд» (`drift_trend`) активується фіче-флагом `FEATURE_PARTICIPATION_LIGHT` і телеметрійно позначається у UI через `market_context.meta.insights.quiet_mode` (`quiet_score`).
- Пороги зберігаються у конфігурації Stage2 (див. `config/config_stage2.py`: `PARTICIPATION_LIGHT_THRESHOLDS`).
- Памʼять фаз (`PhaseState`) додає мʼякі підказки: адаптер фаз читає `stats.phase_state` і публікує `market_context.meta.phase_state_hint`, тож Explain/QA бачать carry-forward без зміни контрактів Stage1/Stage2.

### PhaseState hint

- **Джерело.** `phase_adapter.detect_phase_from_stats()` формує `phase_state_hint` і вкладає його у `market_context.meta.phase_state` під ключем `phase_state_hint`; `app/process_asset_batch.py` дублює блок у Stage1 `meta`.
- **Поля.** `{phase_state_current (поле `phase`), age_s, score, reason, presence, bias, htf_strength, updated_ts, direction_hint}`. Значення presence/bias/htf_strength перетягуються з останнього PhaseState snapshot, `direction_hint` обчислюється через `utils.direction_hint.infer_direction_hint` як мʼякий натяк long/short (не трейдовий сигнал).
- **Використання.**
	- `SCENARIO_TRACE` та `market_context.meta` показують hint разом із кандидатом, щоб видно було carry-forward віком/причиною.
	- `[SCEN_EXPLAIN]` додає `direction_hint`, коли Explain проходить rate-limit.
	- UI/Prometheus читають блок для QA/спостереження, Stage3 не залежить від нього й контракти не змінені.

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
- `ai_one_whale_signal_v1_confidence{symbol}` — confidence мін-сигналу v1.
- `ai_one_whale_signal_v1_enabled{symbol}` — поточний стан enabled (0/1).
- `ai_one_whale_signal_v1_profile_total{symbol,profile}` — частота профілів (strong/soft/explain/none).
- `ai_one_whale_signal_v1_direction_total{symbol,direction}` — напрямок long/short/unknown.
- `ai_one_whale_signal_v1_disabled_total{symbol,reason}` — причини фолбеку (dominance_missing, age_exceeded, profile:none тощо).

Лічильники:
- `ai_one_scn_reject_total{symbol,reason}` — лічильник відмов/деактивацій сценарію з причиною низької кардинальності:
	- У TRACE-відмові кандидата: `reason="no_candidate"`.
	- При деактивації стабільного сценарію: `reason∈{"htf_weak","presence_drop","phase_change","confidence_decay"}` — визначається під час аналізу останніх барів у гістерезисі сценарію.
- `ai_one_phase_presence_cap_delta_total{symbol}` — к-сть разів гвард `presence_cap_no_bias_htf` зрізав сирий presence; допомагає відстежити, наскільки часто clamp блокує фазу.
- `ai_one_phase_reject_total{symbol,reason}` — усі відмови фазового детектора. Причина береться з Stage2 reason-коду (`htf_gray_low`, `htf_not_ok`, `volz_missing`, `volz_too_low`, `no_zones`, `trend_weak`, `unknown`).

Фазний детектор також зберігає `stats.phase_debug.presence_cap_guard` із полями `before`, `after`, `htf_ok`, `htf_strength` (та ім'ям гварда). Це поле не змінює бізнес-логіку, але дозволяє бачити фактичний clamp прямо у Phase Diagnostics/market_context.meta.stats.

Додатково у `stats.phase_debug.reason` записується останній reason-код відмови (той самий, що й у лічильнику `ai_one_phase_reject_total`). Це допомагає дивитися причину прямо з Redis/UI без читання логів, а в логах з'являється рядок `[STRICT_PHASE_REASON]` із упорядкованими полями `symbol ts phase scenario reasons presence bias rr gates`.

### PhaseState QA та `[PHASE_STATE_UPDATE]`

- Прапори: `PHASE_STATE_ENABLED` (config) активує менеджер, а `PHASE_STATE_ENABLED_FLAG` (реекспорт у `stage2/phase_detector`) дозволяє Stage2 використовувати carry-forward фазу. Stage2 читає цей прапор безпосередньо з конфіга під час ініціалізації.
- `[PHASE_STATE_UPDATE] symbol=... enabled=... raw_phase=... current_phase=... age_s=... reason=...` лог відображає весь життєвий цикл оновлень. Дані паралельно пишуться у `stats["phase_state"]`:
	`{current_phase, phase_score, age_s, last_reason, last_whale_presence, last_whale_bias, last_htf_strength, updated_ts}`.
- Soft причини carry-forward: `presence_cap_no_bias_htf`, `htf_gray_low`, `volz_too_low`. Лише вони дозволяють PhaseState утримати фазу (якщо `age_s ≤ PHASE_STATE_MAX_AGE` та немає конфлікту bias). Жорсткі причини (`htf_conflict`, `trend_reversal`, `risk_block`, `low_atr_guard`, `anti_breakout_whale_guard`) миттєво скидають state.
- Цей snapshot стає джерелом для `phase_state_hint` та Prometheus (через Meta/Redis), тому QA бачить реальний вік, причину й останні whale-поля без розкриття приватного PhaseState namespace.

### Whale signal v1: телеметрія та Prometheus

- Payload збирається у `stage3.whale_signal_telemetry.build_whale_signal_v1_payload` → `market_context.meta.whale_signal_v1` → Stage3 `_enforce_whale_signal_v1`. Поля: `enabled`, `direction`, `profile`, `confidence`, `phase_reason`, `reasons`, `presence`, `bias`, `vwap_dev`, `vol_regime`, `age_s`, `missing`, `stale`, `dominance`, `zones_summary`.
- Профілі: `strong` (високі presence/|bias|), `soft` (observe/watchlist) та `explain_only` (UI/QA-only; активується при `phase_reason ∈ {volz_too_low, presence_cap_no_bias_htf}` коли conf < 0.5). Якщо профіль `explain_only` або `enabled=False`, Stage3 лише логуватиме snapshot (`whale_signal_profile`, `whale_phase_reason`) і не впливає на позиції.
- Prometheus: `ai_one_whale_signal_v1_confidence`, `ai_one_whale_signal_v1_enabled`, `ai_one_whale_signal_v1_profile_total`, `ai_one_whale_signal_v1_direction_total`, `ai_one_whale_signal_v1_disabled_total`. Перші два — гейджі, решта — лічильники подій.

Примітки реалізації:
- Ініціалізація метрик виконується ледачо, збій/відсутність клієнта — no-op і не впливає на пайплайн.
- Лічильники інкрементуються без винятків (best-effort), усі виклики захищені try/except.

Приклад швидкої перевірки в PowerShell:

```powershell
(Invoke-WebRequest -UseBasicParsing http://localhost:9108/metrics).Content |
	Select-String -SimpleMatch -Pattern 'ai_one_scn_reject_total{', 'ai_one_phase{phase=', 'presence{symbol='
```

Додано окремий запис телеметрії `whale_signal_v1` у `stage1_events.jsonl`. Payload містить `enabled`, `profile`, `confidence`, `direction`, `presence`, `bias`, `vol_regime`, `age_s`, `dominance`, `zones_summary` та `reasons`. Дані дублюються у `market_context.meta.whale_signal_v1`, тож Analyzer або canary-репорти можуть читати їх без зміни Stage1/Stage2 контрактів.

## 7) Forward-зрізи для whale_signal_v1

Щоб квантифікувати вплив фіче-флагу `WHALE_MINSIGNAL_V1_ENABLED`, додана CLI-утиліта `tools.whale_signal_forward`. Вона читає JSONL `stage1_events.jsonl`, відфільтровує події `whale_signal_v1`, розділяє їх на ON/OFF-зрізи (за полем `enabled`) і будує агрегати для швидкої QA:

```
python -m tools.whale_signal_forward \
	--events telemetry/stage1_events.jsonl \
	--out-dir reports/whale_forward_2025-11-12 \
	--symbols BTCUSDT,ETHUSDT \
	--since "2025-11-12T00:00:00Z" \
	--json reports/whale_forward_2025-11-12.json \
	--markdown reports/whale_forward_2025-11-12.md
```

Експорт:
- `whale_forward_on.csv` / `whale_forward_off.csv` — плоскі таблиці зі snapshot`ами подій (confidence, presence, bias, reasons, dominance, zones_summary тощо).
- JSON/Markdown огляд із підсумковими лічильниками: enabled ratio, профілі, сторони (direction), найчастіші причини вимкнення, top symbols за активністю.

Скрипт навмисно працює тільки з уже зібраними Stage1 подіями, тож forward-зрізи отримуємо без змін Stage3. Для справжнього A/B потрібно прогнати пайплайн двічі (ON/OFF) з тим самим `stage1_events.jsonl` output і порівняти CSV/Markdown-репорти.

## EvidenceBus та пояснення сценарію

У процесі селекції сценарію збираємо “докази” у буфер EvidenceBus на символ:

- market_context.meta.evidence: список пунктів {key,value,weight,note}.
- market_context.meta.scenario_explain: короткий рядок із топ-3 пунктів за вагою.

Додатково раз на ≤10 с/символ (і кожен N-й батч) логуються пояснення у компактному форматі:

```
[SCEN_EXPLAIN] symbol=btcusdt scenario=pullback_continuation explain="near_edge_persist=0.67 (w=1.00); compression.index=0.12 (w=0.70); btc_regime_v2=flat (w=0.60)"
```

Керування логуванням explain (`process_asset_batch.helpers.explain_should_log`):

- Прапори `config/config.py`:
	- `SCEN_EXPLAIN_ENABLED=True` — включає explain-пайплайн.
	- `SCEN_EXPLAIN_VERBOSE_EVERY_N=20` — heartbeat: навіть якщо 10-секундний мін-інтервал не пройшов, кожен N-й батч матиме explain.
- Параметри функції `_explain_should_log(symbol, now_ts, min_period_s, every_n, force_all)`:
	- `min_period_s` — пауза між explain (за замовчуванням 10 с).
	- `every_n` — лічильник `_SCEN_EXPLAIN_BATCH_COUNTER`; після `every_n` пропущених батчів логування примусово спрацьовує і скидає лічильник.
	- `force_all` — форсований режим (`SCEN_EXPLAIN_FORCE_ALL`), який знехтовує rate-limit (використовується для реплеїв/QA). Значення можна перевизначити per-call.
- `_SCEN_EXPLAIN_LAST_TS` тримає останній timestamp; `_SCEN_EXPLAIN_BATCH_COUNTER` показує, скільки батчів було «приглушено». Обидва значення доступні з `process_asset_batch.global_state` для діагностики.
- Лог `[SCEN_EXPLAIN] ... direction_hint=...` тепер включає мʼякий напрямок із `market_context.meta.phase_state_hint.direction_hint` (тільки для Explain/UI; Stage3 і трейдинг його не використовують).

Аналітика explain у CSV:

- Парсер `tools/scenario_quality_report.py` використовує ці рядки для збагачення CSV колонки: `in_zone`, `compression_level`, `persist_score`, `btc_regime_at_activation`, `quality_proxy`.
- Додано метрики покриття explain: `explain_coverage_rate` (частка активацій із explain у ±30 с) та `explain_latency_ms` (від першого TRACE кандидата до найближчого explain).

Швидка верифікація explain у логах:

- Утиліта `tools.verify_explain.py`:
	- `--last-min 60 --logs-dir ./logs`
	- Виводить `explain_lines_total`, `symbols_seen`, `coverage_by_symbol` та `recent_examples` (3 останні explain-рядки).

## PhaseState QA режим (safe)

Псевдостріми на ≥1 годину з carry-forward вимагають окремого профілю, щоб не торкнутися Stage3. Рекомендований набір прапорів для QA/diagnostic запусків через `python -m tools.unified_runner --duration ... --set ...`:

- `STAGE3_PAPER_ENABLED=true` — Stage3 повністю паперовий.
- `WHALE_MINSIGNAL_V1_ENABLED=true` та `SCEN_CONTEXT_WEIGHTS_ENABLED=false` — телеметрія мін-сигналу активна, контексти фіксуємо.
- `PHASE_STATE_ENABLED=true` і `PHASE_STATE_ENABLED_FLAG=true` — включають PhaseState manager + використання carry-forward.
- `SCEN_EXPLAIN_ENABLED=true`, `SCEN_EXPLAIN_VERBOSE_EVERY_N=20`, `SCEN_EXPLAIN_FORCE_ALL=false` — explain активний із heartbeat.
- `PROM_GAUGES_ENABLED=true`, `PROM_HTTP_PORT=9108` — Prometheus /metrics доступний для QA.

Приклад (PowerShell):

```powershell
cd C:\Aione_projects\AiOne_t-whail
.\.venv\Scripts\python.exe -m tools.unified_runner `
	--duration 28800 `
	--set STATE_NAMESPACE=ai_one_phaseqa `
	--set PHASE_STATE_ENABLED=true `
	--set PHASE_STATE_ENABLED_FLAG=true `
	--set STAGE3_PAPER_ENABLED=true `
	--set WHALE_MINSIGNAL_V1_ENABLED=true `
	--set SCEN_EXPLAIN_ENABLED=true `
	--set SCEN_EXPLAIN_VERBOSE_EVERY_N=20 `
	--set PROM_GAUGES_ENABLED=true `
	--log reports\phase_state_8h_on\run.log
```

Цей профіль потрібен лише для діагностики PhaseState / whale_signal_v1 (довгі псевдостріми, forward-зрізи, metrics) і не дає Stage3 права відкривати ордери: `Stage3_PAPER_ENABLED` + профіль `explain_only` гарантують відсутність змін напрямку.
