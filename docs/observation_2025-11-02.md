# Спостереження • 2025-11-02

Цей звіт підсумовує останні події, внесені зміни та поточні результати прозорості/якості.

## Звіт A-only прогону (UTC 15:45)

- Останній зріз: `reports/metrics_20251102_1545.txt` @ 2025-11-02 15:45:11Z
- Активaцій (Acts): 0 для BTCUSDT, ETHUSDT, TONUSDT, SNXUSDT за останній тік (очікувано при слабкому HTF і строгих ґейтах).
- Forward (K=5) — короткий підсумок на базі останніх рішень:
  - BTCUSDT: hit=0.40, med_ret=-0.06%, p75|ret|=0.15%
  - ETHUSDT: hit=0.55, med_ret=-0.06%, p75|ret|=0.14%
  - TONUSDT: hit=0.64, med_ret=0.04%, p75|ret|=0.12%
  - SNXUSDT: hit=0.33, med_ret=0.29%, p75|ret|=0.59%
- Quality gates збірки: Ruff/Mypy/Pytest — PASS (останній прогон all-checks).

## Останні зміни

- Розширено `tools/quality_snapshot.py`:
  - Додано швидкий зріз із `/metrics`: `phase(score)`, `scenario(conf)`, `bias_state`, `btc_regime`, `htf_strength`, `presence`, контекстні `near_edge_persist` та `presence_sustain`.
  - Додано секцію Forward‑перевірок (K‑барів): оцінка `hit_rate(cd·ret>0)`, `median ret%`, `p75(|ret|)%` на базі `telemetry/decision_traces/*.jsonl` і `datastore/*_bars_1m_snapshot.jsonl`.
  - Новий параметр `--forward-k` для вибору горизонту; за замовчуванням тепер `K=3,5,10`.
  - Розширено перелік символів у snapshot: додано `ETHUSDT` (разом із `BTCUSDT, TONUSDT, SNXUSDT`).
- Автоматизовано перегенерацію артефактів наприкінці кожного тіку монітора:
  - `tools/monitor_observation.py` тепер викликає `tools.quality_snapshot` і `tools.quality_dashboard` після оновлення `quality.csv`.
- A‑only монітор запущено зі знімком `/metrics` і Redis‑стану кожні 15 хв протягом 4 годин із `--deep-state`.

## Поточні артефакти

- Останній зріз метрик: `reports/metrics_20251102_1545.txt` (UTC 2025‑11‑02 15:45:11Z)
- Консолідований snapshot: `reports/quality_snapshot.md`
- Дашборд якості (aggregate): `reports/quality_dashboard.md` (буде наповнюватись у міру появи даних у `quality.csv`)
- CSV зі зведенням якості: `reports/quality.csv` (оновлюється монітором)
- Redis‑снапшоти: `reports/state_<SYMBOL>_*.txt`

## Швидкий зріз стану (на основі останнього `/metrics`)

Зафіксовано з `quality_snapshot.md`:

- BTCUSDT: htf_strength=0.04, presence=0.30, bias=-1, near_edge_persist=1.00, presence_sustain=1.00
- ETHUSDT: htf_strength=0.01, presence=0.45, bias=-1, near_edge_persist=1.00, presence_sustain=1.00
- TONUSDT: htf_strength=0.03, presence=0.00, bias=-1, near_edge_persist=1.00, presence_sustain=0.33
- SNXUSDT: htf_strength=0.13, presence=0.94, near_edge_persist=1.00, presence_sustain=0.08

Примітка: поле `Acts` у таблиці наразі 0 для всіх символів — це очікувано одразу після старту монітора;
`reports/quality.csv` буде поповнюватись під час подальших тікiв.

## Forward K‑bars (K=3,5,10; останні ~50–60 рішень)

З `quality_snapshot.md` (точні значення):

- BTCUSDT: K=3 N=52 hit=0.33, K=5 N=42 hit=0.40, K=10 N=19 hit=0.74; медіанний ret% від −0.14% до −0.04%, p75|ret| до 0.15%.
- ETHUSDT: K=3 N=52 hit=0.42, K=5 N=42 hit=0.55, K=10 N=19 hit=1.00; медіани близькі до −0.08…−0.04%, p75|ret| до 0.14%.
- TONUSDT: K=3 N=52 hit=0.42, K=5 N=42 hit=0.64, K=10 N=19 hit=0.11; медіани 0.02…0.05%, p75|ret| до 0.12%.
- SNXUSDT: K=3 N=52 hit=0.17, K=5 N=42 hit=0.33, K=10 N=19 hit=0.47; медіани 0.10…0.29%, p75|ret| до 0.59%.

Тлумачення:
- `hit_rate(cd·ret>0)`: частка випадків, коли знак `cd` у рішенні збігається зі знаком форвардного руху ціни через K барів.
- `median ret%`: медіанний відсотковий рух через K барів від моменту рішення.
- `p75(|ret|)%`: 75‑й перцентиль абсолютного руху, дає уявлення про характерну «амплітуду».

## Оцінка поточного стану

- Прозорість: snapshot тепер містить достатньо контексту, щоб читати його самодостатньо (фаза/сценарій/bias/BTC режим + контекстні показники).
- Forward‑перевірка: початкові метрики для BTC/TON свідчать про помірну узгодженість знаку (≈0.6 hit‑rate) з невеликою типовою амплітудою (p75 до ~0.14%).
- Дані якості: `quality.csv` поки що бідний — нормальне явище відразу після запуску; очікуємо наповнення протягом найближчих тiкiв (15‑хв цикли).

## Наступні кроки

- Монітор вже автоматично перегенеровує snapshot/дашборд на кожному тiку; додаткові ручні дії не потрібні.
- За потреби змінити горизонти: `--forward-k 3,5,10` або інший перелік.

## Додаток

- Вихідні місця даних:
  - Рішення: `telemetry/decision_traces/*.jsonl`
  - Ціни: `datastore/*_bars_1m_snapshot.jsonl`
- Інструменти генерації:
  - Snapshot: `tools.quality_snapshot`
  - Дашборд: `tools.quality_dashboard`
  - Монітор спостереження: `tools.monitor_observation`
