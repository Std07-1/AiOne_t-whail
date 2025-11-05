# Тюнінг Stage2: cpu_pool і perf-метрики

Цей документ допомагає інженерам оперативно підбирати `STAGE2_RUNTIME.cpu_pool.max_workers` і читати perf-логи Stage2.

## Як читати рядок perf-логу

Приклад:

INFO     [Stage2][perf] hippousdt wall=217.6ms cache_hits={fast:False,levels:True} hit_ratio=0.50 offload=False

Що це означає:
- symbol: hippousdt — для якого інструменту виконано цикл Stage2.
- wall=217.6ms — «стіночний» час обробки одного айтема Stage2 від входу до готового результату. 200–300 мс — нормальний діапазон для навантажених моментів; >400–600 мс частими серіями потребує тюнінгу.
- cache_hits — по яких кешах була економія обчислень:
  - fast:False — швидкий кеш/fast‑path не спрацював, виконувались розрахунки.
  - levels:True — рівні/коридор взяті з TTL‑кешу, важкий крок не обчислювався.
- hit_ratio=0.50 — частка спрацювань серед наявних кешів (у прикладі 1 з 2).
- offload=False — важкий крок НЕ відвантажувався у ProcessPool. У даному випадку це очікувано, бо `levels=True`: коли рівні вже з кешу, CPU‑важкий обчислювач не запускається, отже й відвантажувати нічого.

Корисні примітки:
- Якщо `levels=False` і `offload=False`, перевірте, чи увімкнено `cpu_pool.enabled` та чи не сталося фолбеку на синхронний шлях (у логах має бути помітно причину фолбеку/виняток).
- На low‑черзі ми можемо навмисно пропускати частину повільних кроків (fast‑only), що знижує `wall` без втрати реакції на хай‑сигнали.

## Підбір max_workers: симптом → дія

Початкові налаштування:
- Стартуйте з `max_workers = 2` на типовій 8‑ядерній машині (Windows). Підвищуйте поступово по +1.
- Верхня межа — орієнтовно 0.5–0.75× кількість фізичних ядер для CPU‑інтенсивних задач.

Типові ситуації:

- Симптом: `wall` часто > 400–600 мс, черги зростають (qdepth hi ↑), CPU завантаження низьке/середнє (<50%).
  Дія: збільшити `max_workers` на +1, спостерігати 5–10 хв. Якщо qdepth стабілізується і `wall` падає — зафіксувати; інакше ще +1.

- Симптом: CPU 90–100% постійно, `wall` високий, qdepth не росте помітно (система насичена).
  Дія: не збільшувати воркери. Оптимізувати кеші/гейти: підняти `levels_ttl_sec`, `fast_ttl_sec`, вимкнути/дебаунсити narrative для low‑черги, перевірити проксі‑політики.

- Симптом: `offload=True` часто, але `wall` не зменшується або росте; видно конкуренцію/контеншн.
  Дія: зменшити `max_workers` на −1. Можливо, завдання надто дрібні — витрати IPC > вигоди. Підвищіть TTL рівнів/HTF, об’єднайте дрібні кроки або залиште синхронно.

- Симптом: `cache_hits.levels` низький (<0.3), `wall` високий.
  Дія: підняти `STAGE2_RUNTIME.levels_ttl_sec`; перевірити, що ключі кешу відповідають шаблонам з `config`, немає зайвої інвалідації.

- Симптом: `cache_hits.fast` низький, багато повторних перерахунків на low‑черзі.
  Дія: збільшити `STAGE2_RUNTIME.fast_ttl_sec`; відкоригувати пороги fast‑гейтів, перевірити soft‑sупресію (suppress_ttl_sec).

- Симптом: Очікували `offload=True` при `levels=False`, але бачимо `offload=False`.
  Дія: перевірити `STAGE2_RUNTIME.cpu_pool.enabled`, `max_workers > 0`, семафор/таймаути, логи фолбеку. На Windows перший запуск процесів повільний — дочекайтесь прогріву.

- Симптом: qdepth і `wall` стабільно низькі, CPU завантаження низьке.
  Дія: можна зменшити `max_workers` (економія ресурсів), або залишити як є для запасу по піках.

## Нюанси Windows

- ProcessPool на Windows використовує spawn: перший прогрів повільніший. Можна «підігріти» пул коротким завданням при старті додатку.
- Уникайте надто великого `max_workers`: створення процесів і IPC мають відчутний оверхед.

## Де міняти параметр

- Файл: `config/config.py`
- Поле: `STAGE2_RUNTIME["cpu_pool"]["max_workers"]` і прапорець `enabled`.
- Після зміни — перезапустити застосунок. Рекомендується спостерігати за:
  - perf‑логами (`[Stage2][perf]`): `wall`, `cache_hits`, `offload`;
  - логами планувальника (qdepths);
  - системними метриками CPU/RAM.

## RANGE light‑mode, heartbeat і suppress

Ціль: мінімізувати CPU у флеті без втрати реакції на подієві виходи.

- У сценарії RANGE_BOUND Stage2 працює у «легкому» режимі: лише швидкі фічі, без важких блоків. Рекомендація — `light_reco` (типово WAIT_FOR_CONFIRMATION), `market_context.meta.range_light_mode = True`.
- Вихід із light‑mode подієвий: `exit_triggers_any` (band_break/vol_spike/delta_flip/slope_atr_jump) або overshoot по `price_slope_atr` відносно порогу × `slope_overshoot_factor`.
- Heartbeat: процесор додає `result.hints.low_priority_tick_sec` — продюсер/шедулер знижує частоту low‑перевірок адресно для символів у RANGE. Значення дефолтного heartbeat — `STAGE2_RANGE_LIGHT_MODE.heartbeat_sec` (типово 15с).
- Soft‑suppress: коли `atr_pct/low_gate < suppress.min_ratio` або `band_pct < suppress.min_band_pct`, ставиться TTL‑мітка у Redis `suppress:{symbol}` на `suppress.ttl_sec` (типово 300с) — шедулер пропускає такі символи в low‑черзі.

Налаштування у `config/config.py` (блок `STAGE2_RANGE_LIGHT_MODE`):

- `enabled`: вмикає light‑mode у RANGE.
- `exit_triggers_any`: список тригерів виходу.
- `heartbeat_sec`: бажаний інтервал перевірки у light‑mode.
- `hysteresis_ttl_sec`, `slope_overshoot_factor`: параметри гістерезису виходу.
- `suppress.{ttl_sec,min_ratio,min_band_pct}`: межі для soft‑suppress.

Спостереження ефекту:
- У заголовку UI стежать за `wall`/CPU/MEM; очікуємо падіння `avg_wall_ms` і зменшення qdepth.
- У counters з core:stats з’являються `suppress_set`/`suppress_active` (див. нижче telemetry.md).

## Швидкий ролбек

- Вимкнути пул: `STAGE2_RUNTIME["cpu_pool"]["enabled"] = False`.
- Або знизити `max_workers` до безпечного значення (1–2).# Тюнінг фаз/гейтів (Stage2→Stage3)

Ціль: обережно зменшити кількість слабких (низький RR) проходів у Stage3 та
краще відсікати угоди у надто тонкому ринку, не ламаючи контракти даних.

## Що змінено (параметрично)

- STAGE2_LOW_VOL_ADAPT.rolling_atr_p50_factor: 0.90 → 0.95
  - Ефект: ефективний low_gate частіше підтягується до rolling ATR p50, що
    уникає відкриття у «надто тихих» ділянках. Менше low_atr overrides.
- STAGE2_GATE_PARAMS.rr_hint_factor: 0.88 (новий ключ)
  - Ефект: мʼякий pre‑gate у Stage2 підвищує поріг RR на основі rr_hint.
    Менше записів зі skip‑причиною low_rr у Stage3.

Контракти Stage1Signal/Stage2Output не змінювалися.

## Як моніторити ефект

- Перевірити розподіл причин у `telemetry/stage3_skips.jsonl`:
  - `low_rr`, `low_atr`, `wide_band_gate`, `low_confidence`.
- Додатково подивитись `telemetry/stage2_decisions.jsonl`:
  - поля `atr_vs_low_gate_ratio`, `low_volatility_flag`, `market_context.meta.rr_hint`.

Швидкий аналіз:

```powershell
# Приклад: сумарна статистика (Windows PowerShell)
C:\Users\vikto\Desktop\AiOne_t_v2-main\venv\Scripts\python.exe -m tools.analyze_telemetry --all --since "1d"
```

Ключові метрики успіху:
- Зменшення частки skip `low_rr` у Stage3 на ≥10–20% при стабільній/кращій win‑rate.
- Відсутність зростання skip `low_confidence` або `wide_band_gate`.

## Rollback

- Повернути `rolling_atr_p50_factor` до 0.90.
- Змінити `rr_hint_factor` на 0.80 (або вилучити ключ — код має дефолт 0.8).

## Примітки

- Всі ключі/TTL/формати лишаються канонічними; новий параметр доданий в `STAGE2_GATE_PARAMS`.
- Зміни не вимагають міграцій і можуть бути вимкнені гаряче через patch.

## Кольори для perf‑метрик у UI

У заголовку UI та у персигнальних "Perf:" підказках використовується колірне кодування Rich для швидкої діагностики навантаження:

- wall (затримка Stage2 на один айтем):
  - зелений: ≤ 200 мс
  - жовтий: ≤ 400 мс
  - червоний: > 400 мс
- CPU (system):
  - зелений: < 40%
  - жовтий: < 70%
  - червоний: ≥ 70%
- Proc (CPU процесу):
  - зелений: < 50%
  - жовтий: < 80%
  - червоний: ≥ 80%
- MEM (зайнятість ОЗП, %):
  - зелений: < 70%
  - жовтий: < 85%
  - червоний: ≥ 85%

Поради:
- Перші 1–2 цикли значення Proc можуть бути 0% через «розігрів» вимірювача CPU в psutil.
- Якщо wall тримається у жовтій/червоній зоні при зеленому CPU — перевірте кеші/гейти (fast/levels), дебаунс narrative, або профілюйте важкі кроки.
- Якщо CPU/MEM стабільно у червоній зоні — зменшіть навантаження (зниження qdepth, вимкнення DEBUG‑логів) або обмежте кількість активів; альтернативно — підсиліть інфраструктуру.

---

## NEW • Нормалізація *_IN_* у підтриману стратегію виконання

Сигнали типу "pullback" мапляться на ф’ючерсну стратегію виконання `momentum_pullback`.

- Якщо селектор стратегії не задав `market_context.meta.strategy`, процесор встановлює fallback `"momentum_pullback"`.
- Це значення дублюється у `trade_intent.hints.strategy` для Stage3, який підключає профіль з `STAGE3_STRATEGY_PROFILES`.

Мета: уніфікувати виконання *_IN_* без зміни контрактів і зберегти керованість через конфіг.

## NEW • Жорсткі WAIT‑гейти після оцінки ризику (Stage2)

Щоб зменшити слабкі проходи у Stage3, введені два демоут‑гейти після розрахунку ризику:

1) Низький live‑RR:
  - Умова: `rr_live < max(min_rr_cfg, rr_hint * STAGE2_GATE_PARAMS.rr_hint_factor)`.
  - Дія: зниження до `WAIT_FOR_CONFIRMATION`, очищення `risk_parameters`, причина `reco_gate_reason+=low_rr_stage2`.

2) Низький atr_ratio:
  - Розрахунок: `atr_ratio = atr_pct / low_gate_effective` (із контексту або дефолт `STAGE2_CONFIG.low_gate`).
  - Поріг: `< STAGE2_FAST_GATES.min_atr_ratio` (дефолт 0.8 у config).
  - Дія: `WAIT_FOR_CONFIRMATION`, очищення `risk_parameters`, причина `reco_gate_reason+=low_atr_ratio`.

Рекомендації з тюнінгу:
- Тримайте `min_atr_ratio` в межах 0.8–0.9 залежно від ринку; підвищення зменшить кількість угод у «тонких» ділянках.
- `rr_hint_factor` впливає на мінімально допустимий RR — збільшуйте обережно, щоб не задушити сигнал.

---

## Додаток: тюнінг фільтра китових рівнів (QUALITY_THRESHOLDS)

Мета: зменшити шум у телеметрії зон попиту/пропозиції, залишаючи найближчі та «сильніші» рівні. Працює лише як телеметрія Stage2 (observe‑only), без впливу на Stage3.

Де налаштовувати: `config/config.py`, словник `QUALITY_THRESHOLDS`.

Ключі:
- `MIN_ACCUMULATION_VOLUME` — мінімальний сумарний обсяг усередині зони накопичення.
- `MIN_DISTRIBUTION_VOLUME` — мінімальний сумарний обсяг усередині зони розподілу.
- `MAX_DISTANCE_FROM_PRICE` — макс. відстань центру зони до поточної ціни (частка від ціни, наприклад 0.03 = 3%).
- `STOP_HUNT_MAX_DISTANCE` — макс. відстань рівня stop‑hunt до поточної ціни.
- `MAX_LEVELS_PER_TYPE` — верхня межа кількості зон одного типу у виході (наприклад, 2 для accum, 3 для dist).
- `STOP_HUNT_TOP_N` — кількість найближчих рівнів stop‑hunt.

Поради з тюнінгу:
- Для високоволатильних/низьколіквідних активів підвищуйте `MAX_DISTANCE_FROM_PRICE` (0.05–0.08) і масштабуйте `MIN_*_VOLUME` до середніх обсягів символа.
- Для великих капів зменшуйте `MAX_DISTANCE_FROM_PRICE` (0.02–0.03) і підвищуйте `MIN_*_VOLUME`, щоб уникнути «тонких» зон.
- Якщо бачите часто 0 у `whale_quality_*_cnt` — пороги надто суворі; якщо занадто багато рівнів — навпаки, посиліть пороги.

Моніторинг ефекту:
- У `telemetry/stage2_decisions.jsonl` зʼявляються:
  - `whale_presence_score`, `whale_quality_score`;
  - `whale_quality_accum_cnt`, `whale_quality_dist_cnt`, `whale_quality_stop_cnt`.
- Запустіть `tools/analyze_telemetry.py` — у секції «Whale telemetry (observe‑only)» дивіться середні/діапазони значень і корегуйте пороги.
