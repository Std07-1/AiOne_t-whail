# Config v2 — реорганізація (оновлено)

## Мета і цілі
- Зменшити складність `config/config.py`, підвищити читабельність і швидкість ревʼю.
- Централізувати фіче‑флаги і канонічні ключі (K_*), уніфікувати публічний API.
- Підготуватися до поступового винесення великих карт у YAML без зміни контрактів.

## Архітектурні принципи (SoT)

- Тонкий конфіг: лише дані, легкі структури, без бізнес‑логіки і гарячого I/O.
- Модульність:
  - `config/constants.py` — канонічні ключі/статуси (ре‑експорт у `config/config.py`).
  - `config/flags.py` — фіче‑флаги (перемикачі з докстрінгами, rollback‑дружні).
  - `config/config.py` — ядро конфігурації (TTL, директорії, семафори, дрібні карти).
  - `config/keys.py` — Єдиний спосіб формувати Redis‑ключі/канали (KeyBuilder).
  - `config/loaders.py` — кешовані YAML‑лоадери для великих карт у `config/data/*`.
- Контракти Stage1/Stage2/Stage3 незмінні. Нові дані — лише у `market_context.meta.*` або `confidence_metrics`.

## Redis‑ключі та канали

- Будувати ключі тільки через `config/keys.py`:
  - `build_key(NAMESPACE, "stats", symbol="btcusdt", granularity="1m")`
    → `ai_one:stats:btcusdt:1m`
  - `build_channel(NAMESPACE, "ui", "asset_state")` → `ai_one:channel:ui:asset_state`
- Символи нормалізуються до нижнього регістру. Нові домени додавайте як рядок `domain`.
- TTL інтервалів зберігаються у `INTERVAL_TTL_MAP`.

## YAML‑профілі та оверлеї середовищ

- Великі карти/пороги — у `config/data/*.yaml`, читання через `load_yaml(name)` з кешем.
- Вмикати YAML‑джерела тільки під флагом (приклад: `FEATURE_STAGE2_THRESHOLDS_YAML`).
- Допускаються env‑оверлеї на рівні споживачів (не у `config/*`), з явним перетворенням типів.

## Gap‑recovery WS (сумісність із ws_worker)

- `WS_GAP_BACKFILL` має обовʼязково містити:
  - `enabled: bool`
  - `lookback_minutes: int`
  - `max_parallel: int`
  - `max_minutes: int` — верхня межа хвилин у бекфіл за один прохід
  - `status_ttl: int` — TTL публікації статусу ресинхронізації у Redis (секунди)

## Чек‑листи

### Додавання нового Redis‑ключа
- [ ] Використати `build_key(NAMESPACE, domain, symbol?, granularity?, extra?)`.
- [ ] Додати/використати TTL з `INTERVAL_TTL_MAP` або доречний локальний TTL.
- [ ] Логи без секретів; уникати «вічних» ключів без потреби.

### Додавання нової карти конфігу
- [ ] Для великих обсягів даних — YAML у `config/data/` + `load_yaml()`.
- [ ] За потреби — фіче‑флаг для мʼякого ввімкнення.
- [ ] Короткий докстрінг у `config/config.py`/`flags.py`/`constants.py`.
- [ ] Мінімальні тести (smoke): лоадер + валідний тип/формат.

### Фіче‑флаг з впливом на рішення
- [ ] Додати прапорець у `config/flags.py` з описом.
- [ ] Повністю вимикає вплив при `False` (rollback за 1 клік).
- [ ] Оцінити перф‑наслідки; уникати I/O у гарячій петлі.

Див. також: `docs/FEATURE_FLAGS.md` — огляд ключових прапорів і правила rollback.

## Внесені зміни (поточна ітерація)
- Додано файли:
  - `config/flags.py` — фіче‑флаги (STRICT/INSIGHT/WHALE/DIRECTIONAL/NARRATIVE).
  - `config/constants.py` — K_* ключі, `STAGE2_STATUS`, `ASSET_STATE`.
  - `config/keys.py` — KeyBuilder для формування Redis‑ключів/каналів.
  - `config/config_stage{1,2,3}.py`, `config/config_{main,produsser,processor,whale}.py` — каркаси.
  - `config/data/symbol_whitelist.yaml`, `config/data/stage2_thresholds.yaml` — приклади YAML.
  - `config/loaders.py` — safe YAML loader з кешем.
- Оновлено `config/config.py`: додано re‑export з `flags.py` і `constants.py` (wildcard у кінці файлу).
 - Уточнено `WS_GAP_BACKFILL`: додано `max_minutes`, `status_ttl` для повної сумісності з `ws_worker`.

### Нове: профіль Strict Prefilter (soft)

- Додано фіче‑флаг `STAGE1_PREFILTER_STRICT_SOFT_PROFILE` у `config/flags.py`.
  - `True` → використовується м'який профіль префільтра (нижчі пороги участі, HTF не обов'язковий, бонус за широкий діапазон).
  - `False` → жорсткий профіль (поведінка ближча до початкової).
- Додано мапу профілів `PREFILTER_STRICT_PROFILES` у `config/config.py` з двома наборами параметрів: `default`, `soft`.
  - Споживачі можуть створити пороги так: `StrictThresholds(**PREFILTER_STRICT_PROFILES["soft"])`.
  - Пайплайн у `app/screening_producer.py` і утиліта `tools/run_prefilter_strict.py` обирають профіль автоматично на базі флагу.

## Сумісність
- Публічні імена з `config/config.py` збережені. Інші модулі нічого не змінюють.
- Бізнес‑логіки у `config/` немає. Жодних зовнішніх I/O у гарячому шляху.
- Redis‑ключі/TTL/контракти Stage1/2/3 не змінені.

## Rollback
- Видалити нові файли і прибрати re‑export‑блок у кінці `config/config.py`.
- Старий код продовжить працювати.

## Наступні кроки (Етап 2+)
- Поступово переносити великі мапи Stage2/Stage1/Stage3 у відповідні модулі або YAML.
- Для YAML – підключати через `config/loaders.py` і кешувати, без синхронного I/O в критичних ділянках.
- Розширити документацію кожного підмодуля з чітким переліком публічних змінних і відповідних тестів.
 - Замінювати поетапно хардкод ключів на `build_key(...)` у робочих модулях (малими дифами).

## Нотатки для Copilot
- Не змінювати контракти `Stage1Signal/Stage2Output` — нові поля лише в `market_context.meta.*` або `confidence_metrics`.
- Нові Redis‑ключі — лише через константи і у форматі `ai_one:{domain}:{symbol}:{granularity}`.
- Будь‑які зміни флагів — у `flags.py` із коротким докстрінгом.
- Уся нова конфігурація для whale‑телеметрії — у `config/config_whale.py`, не в `config/config.py`.

## Low-ATR override (спайк)

- Флаг: `STRICT_LOW_ATR_OVERRIDE_ON_SPIKE` (вмикає/вимикає усю поведінку override).
- Redis ключ: `ai_one:overrides:low_atr_spike:{symbol}` для UI/споживачів; внутрішній TTL-стан також кешується під `ai_one:phase:low_atr_override:{symbol}`.
- Умова: `band_expand≥0.013 ∧ spike_ratio≥0.60 ∧ |vol_z|≥2.5 ∧ DVR≥1.2` (див. `LOW_ATR_SPIKE_OVERRIDE`).
- TTL: `bars_ttl` барів.
- Поведінка: override лише знімає low-ATR guard; промоція додатково потребує `presence≥0.60 ∧ htf≥0.20 ∧ bias≠0` + тригер.

## Flat policy

- Presence cap: `PRESENCE_CAPS.accum_only_cap` при `dist=0 ∧ liq=0` (флаг `STRICT_ACCUM_CAPS_ENABLED`).
- Risk clamp: `STAGE2_PRESENCE_PROMOTION_GUARDS.cap_without_confirm` при `dist=0 ∧ liq=0` (флаг `STRICT_ZONE_RISK_CLAMP_ENABLED`).

## Метрики Prometheus (опційно)

- Ендпоінт: `http://localhost:{PROM_HTTP_PORT}/metrics` (див. `PROM_HTTP_PORT`).
- Гейджі: `low_atr_override_active{symbol}`, `low_atr_override_ttl{symbol}`, `htf_strength{symbol}`, `presence{symbol}`.

## HTF checks (Windows)

Запуск мінімального HTF‑паблішера і перевірки ключів у Redis:

```bat
rem Запуск (в іншій консолі має працювати Redis)
venv\Scripts\python.exe -m tools.htf_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --htf M5,M15 --publish-redis

rem Перевірка наявності ключів M5
redis-cli KEYS ai_one:bars:*:M5

rem Перевірка TTL (має бути > 0)
redis-cli TTL ai_one:bars:BTCUSDT:M5

rem Перевірка state: витягнемо hash та знайдемо поля HTF
redis-cli HGETALL ai_one:state:BTCUSDT > state.txt
findstr "htf:strength htf_strength htf_stale_ms" state.txt

rem Метрики (опційно)
curl -s http://localhost:9108/metrics > metrics.txt
findstr ai_one_scenario metrics.txt
findstr htf_strength metrics.txt
```

## Whale checks (Windows)

Запуск мінімального whale‑паблішера і перевірки:

```bat
venv\Scripts\python.exe -m tools.whale_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --cadence-ms 3000

rem Оновлення каденсу
redis-cli GET ai_one:metrics:whale:last_publish_ms

rem Перевірка наявності поля whale у state
redis-cli HGET ai_one:state:BTCUSDT whale
```
- Флаг: `PROM_GAUGES_ENABLED` — якщо `True`, ендпоінт стартує на початку аплікації (best-effort, no-op якщо відсутній `prometheus_client`).

## CLI перемикачі (A/B)

У скрипті `tools/bench_pseudostream.py` додано прапори:

- `--enable-accum-caps` / `--disable-accum-caps` → `STRICT_ACCUM_CAPS_ENABLED`.
- `--enable-htf-gray` / `--disable-htf-gray` → `STRICT_HTF_GRAY_GATE_ENABLED`.
- `--enable-low-atr-spike-override` / `--disable-low-atr-spike-override` → `STRICT_LOW_ATR_OVERRIDE_ON_SPIKE`.
- `--prometheus` → вмикає `PROM_GAUGES_ENABLED` та стартує ендпоінт метрик.

Приклади:

```
python tools/bench_pseudostream.py --prometheus \
  --enable-accum-caps --enable-htf-gray --enable-low-atr-spike-override \
  --symbols BTCUSDT,ETHUSDT,TRXUSDT --limit 300 --interval 1m

curl -s localhost:9108/metrics | rg "low_atr_override|htf_strength|presence"
```
