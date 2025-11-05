# Збір метрик Prometheus і TRACE (Windows PowerShell)

Цей документ описує, як знімати метрики Prometheus і збирати TRACE/quality артефакти під час коротких прогонів пайплайна.

## Передумови

- Прапори у `config/config.py`:
  - `PROM_GAUGES_ENABLED=True` — вмикає легкий HTTP `/metrics` на порту `PROM_HTTP_PORT` (за замовчуванням 9108).
  - `SCENARIO_TRACE_ENABLED=True` — вмикає рейт-лімітовані логи `[SCENARIO_TRACE]`.
- Важливо: `/metrics` існує лише поки процес працює. Після завершення вікна запуску ендпоінт закривається.

## Крок 1. Запустити вікно пайплайна

В одному терміналі запустіть керований прогін на 15–30 хв із прапорами:

- Приклад для 30 хв (1800 с):

(приклад команд див. у заголовку модуля `tools/run_window.py` — блок із `--duration 1800`)

## Крок 2. Знімати /metrics у паралельному вікні

В іншому (паралельному) терміналі періодично знімайте `/metrics` під час роботи першого вікна.

- Приклад збереження у файл `reports/metrics.txt` (повторюйте за потреби):

```
(Invoke-WebRequest -UseBasicParsing http://localhost:9108/metrics).Content | Out-File -Encoding utf8 reports/metrics.txt
```

Порада: робіть знімок наприкінці вікна (за 5–10с до завершення), щоб отримати найсвіжіші значення.

Для швидкої фільтрації ключових гейджів і лічильників у консолі використайте:

```powershell
(Invoke-WebRequest -UseBasicParsing http://localhost:9108/metrics).Content |
  Select-String -SimpleMatch -Pattern 'htf_strength{symbol=', 'presence{symbol=', 'ai_one_phase{phase=', 'ai_one_scn_reject_total{'
```

Додаткові профільні метрики (за `PROM_GAUGES_ENABLED=true` і ввімкненим профільним двигуном):

- `ai_one_profile_active{symbol,profile,market_class}` — one‑hot індикатор активного профілю.
- `ai_one_profile_confidence{symbol}` — остання впевненість профілю (0..1).
- `ai_one_profile_switch_total{from,to}` — лічильник перемикань профілю (очікувана частота ≤ 1 раз/30с).
- `ai_one_profile_hint_emitted_total{dir}` — кількість емітованих хінтів за напрямом (`long`/`short`/`neutral`).
- `ai_one_stage1_latency_ms{symbol}` — латентність гарячого шляху (ціль p95 ≤ 200 мс).

## Крок 3. Звіт якості (quality.csv)

Після прогона згенеруйте звіт якості за останні N годин логів:

```
python -m tools.scenario_quality_report --last-hours 4 --out reports/quality.csv
```

Колонки:
- `scenario`
- `count_activations_by_scenario`
- `mean_conf`
- `time_to_stabilize`
- `btc_regime`
- `btc_gate_effect` (1, якщо застосовано flat‑penalty 0.2)

Додатково (live-метрика):
- `ai_one_scn_reject_total{symbol,reason}` — інкрементується при TRACE-відмові кандидата (`no_candidate`) та при деактивації стабільного сценарію (`htf_weak`, `presence_drop`, `phase_change`, `confidence_decay`).

## Крок 4. Огляд TRACE (опційно)

Для швидкого огляду останніх TRACE‑рядків скористайтеся пошуком у `logs/app.log` або зробіть короткий підсумок власним скриптом (tail → фільтр `[SCENARIO_TRACE]`).

Зауваження: TRACE для символів відображає `btc_regime` (v2: `flat`/`trend_up`/`trend_down`) і, якщо застосовано, `penalty=0.2`.

## Поширені питання

- Чому отримую помилку «endpoint not available»?
  — Ви опитуєте `/metrics` після завершення вікна запуску. Знімайте метрики у паралельному вікні ПІД ЧАС роботи пайплайна.

- На якому порту працює `/metrics`?
  — Порт задається `PROM_HTTP_PORT` у `config/config.py` (за замовчуванням 9108).

- Як вимкнути всю телеметрію швидко?
  — Встановіть `PROM_GAUGES_ENABLED=False` і не передавайте `--set SCENARIO_TRACE_ENABLED=true`.
