# PhaseState QA Checklist

План перевірок перед довгими (≥1h) псевдострімами з carry-forward.

## 1. Артефакти для ON/OFF ран-ів

Для двох запусків (`reports/phase_state_base`, `reports/phase_state_cf_on`) зберігаємо:

- `logs/run_phase_state_*.log` — повний лог пайплайна.
- `reports/phase_state_*/quality_snapshot.md` — готовий snapshot.
- `reports/phase_state_*/signals_truth.csv` — результат `tools.signals_truth`.
- `reports/phase_state_*/forward_*` (за потреби) — forward-аналітика.
- `reports/phase_state_*/prom_metrics.txt` — дамп `/metrics`, якщо `PROM_GAUGES_ENABLED=true`.

## 2. Commands

Signals truth для carry-forward ON:

```powershell
python -m tools.signals_truth \
  --logs logs/run_phase_state_cf_on.log \
  --last-hours 2 \
  --ds-dir datastore \
  --out reports/phase_state_cf_on/signals_truth.csv \
  --emit-prom true
```

Prometheus дамп:

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:9108/metrics \
  | Out-File -Encoding utf8 reports/phase_state_cf_on/prom_metrics.txt
```

## 3. Метрики для порівняння ON/OFF

- Частка `phase=None` (з логів або `quality_snapshot`).
- Кількість `[PHASE_STATE_CARRY]` подій та їх reason-коди (новий скрипт `tools.analyze_phase_state`).
- FP/TP із `signals_truth.csv` (навіть у грубому форматі).
- `p75`/`p95` latency `phase_detector` / `Stage2` (лог `[KPI] avg_wall_ms` / `prom_metrics`).

## 4. analyze_phase_state

Після запуску:

```powershell
python -m tools.analyze_phase_state \
  --log logs/run_phase_state_cf_on.log \
  --out reports/phase_state_cf_on/phase_state_qa.md
```

Скрипт збирає кількість carry-подій, розклад reason-кодів, базові статистики `age_s`.

## Запуск tools.signals_truth для PhaseState 8h

### ON-ран (PhaseState увімкнено)

```powershell
.\.venv\Scripts\python.exe -m tools.signals_truth \
  --last-hours 8 \
  --logs reports/phase_state_8h_on/run.log \
  --ds-dir datastore \
  --out reports/phase_state_8h_on/signals_qa_on.csv
```

### OFF-ран (PhaseState вимкнено)

```powershell
.\.venv\Scripts\python.exe -m tools.signals_truth \
  --last-hours 8 \
  --logs reports/phase_state_8h_off/run.log \
  --ds-dir datastore \
  --out reports/phase_state_8h_off/signals_qa_off.csv
```

> `tools.signals_truth` створює валідний CSV навіть без сигналів (у файлі лишається заголовок).

## Вибірка епізодів для overlay

Приклад для BTC (carry + конфіденс ≥0.35):

```powershell
.\.venv\Scripts\python.exe -m tools.extract_phase_episodes ^
  --log reports/phase_state_8h_on/run.log ^
  --symbol btcusdt ^
  --out reports/phase_state_8h_on/episodes_btc.md ^
  --phase-state-carried-only ^
  --min-conf 0.35 ^
  --max-episodes 10
```

Для ETH/TON/SNX змінюємо `--symbol`, `--out` і пороги за потреби; результат — Markdown-таблиця з часовими мітками для TradingView overlay.

---

Перевірки вважаються пройденими, якщо carry-forward зменшує частку `phase=None`, не збільшує FP і не погіршує latency >20% проти baseline.
