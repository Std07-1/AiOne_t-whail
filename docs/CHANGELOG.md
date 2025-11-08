# Зміни та оновлення (спостереження / explain) — 2025-11-02

Цей файл підсумовує останні зміни, пов’язані з прозорістю селектора, explain‑логами та якісними звітами. Контракти Stage1/Stage2/Stage3 не змінювалися; усі нові дані — у `market_context.meta.*`, логи та Prometheus.

## Нове

- SCEN_EXPLAIN логування з рейт‑лімітом і "тонким" слідом:
  - Прапори в `config/config.py`: `SCEN_EXPLAIN_ENABLED=True`, `SCEN_EXPLAIN_VERBOSE_EVERY_N=20`.
  - Хелпер `_explain_should_log` і пер-символьний лічильник батчів у `app/process_asset_batch.py`.
  - Формат лишився сталим: `[SCEN_EXPLAIN] symbol=... scenario=... explain="..."`.
- EvidenceBus: збір ключових "доказів" рішення селектора з підсумком у `market_context.meta.scenario_explain`.
- Якісний звіт (tools/scenario_quality_report.py):
  - Додано `explain_coverage_rate` (частка активацій із explain у ±30 с).
  - Додано `explain_latency_ms` (середній час від першого TRACE кандидата до найближчого explain).
  - Збережені раніше колонки: `in_zone`, `compression_level`, `persist_score`, `btc_regime_at_activation`, `quality_proxy`, `reject_top{1..3}`, `reject_top{1..3}_rate`.
- Перевірка explain (tools/verify_explain.py):
  - Параметри: `--last-min 60`, `--logs-dir ./logs`.
  - Вивід: `explain_lines_total`, `symbols_seen`, `coverage_by_symbol`, `recent_examples` (останні 3 explain‑рядки).

## Документація

- `docs/OBSERVATION_MODE.md` — додано прапори SCEN_EXPLAIN, колонки coverage/latency у `quality.csv`, приклад запуску `tools.verify_explain`.
- `docs/TELEMETRY.md` — описано керування explain‑логами (рейт‑ліміт + кожен N‑й батч), нові метрики у `quality.csv` і утиліту верифікації.

## Примітки

- Єдиний /metrics експортер — у пайплайні; паблішери не стартують HTTP.
- Продуктивність — без регресії; логи explain рейт‑обмежені та безпечні.
- Всі зміни підлягають вимкненню прапорами (rollback‑friendly).

---

# Профільний двигун (T0) — 2025-11-05

Мета: керовані профільні сигнали в Stage2‑lite без зміни контрактів, під фіче‑флагом.

## Нове

- Прапор `PROFILE_ENGINE_ENABLED` — вмикає телеметрійні Stage2‑хінти на базі профілів (контракти стабільні).
- Гейтинг: `require_dominance`, `alt_confirm_min`, `hysteresis`, `cooldown`, `atr_scale`; у `market_context.meta.profile` зберігаємо `name`, `conf`, `k_range`.
- Dedup alt‑прапорців: обʼєднання з кількох джерел у множину; `alt_confirms_count=len(set)`.
- Причини у hint: компактні коди `profile=...,dom_ok=0/1,alt_ok=0/1,hysteresis=0/1,cooldown=0/1,stale=0/1` + базові `presence_ok,bias_ok,vwap_dev_ok`.
- Персист кулдауну (best‑effort): `ai_one:ctx:{symbol}:cooldown` із TTL≈120 с; фолбек — ін‑меморі.
- Пер‑класне масштабування `score`: BTC=1.0, ETH=0.95, ALTS=0.85.
- Метрики Prometheus: `ai_one_profile_active{symbol,profile,market_class}`, `ai_one_profile_confidence{symbol}`, `ai_one_profile_switch_total{from,to}`, `ai_one_profile_hint_emitted_total{dir}`; а також `ai_one_stage1_latency_ms` для латентності.

## Документація

- Оновлено: `docs/FEATURE_FLAGS.md`, `docs/REDIS_KEYS.md`, `docs/metrics_capture.md`, `docs/ui_payload_schema_v2.md`.

## Примітки

- Контракти Stage1/Stage2/Stage3 не змінено — усі нові дані лише у `market_context.meta.*` і телеметрії.
- Усі write‑/read‑операції Redis для кулдаунів — best‑effort; збої не впливають на гарячий шлях.

---

# Helpers / Whale embedding refactor — 2025-11-07

Мета: зменшити дублювання та усунути приховану логічну помилку в побудові whale‑метрик без зміни контрактів.

## Виправлено

- Індентація блоку формування `whale_embedded`: раніше створювався лише коли `vol_regime == "unknown"`. Тепер метрики будуються завжди після визначення режиму волатильності.
- Carry‑forward presence (фіче‑флаг `WHALE_PRESENCE_STALE_CARRY_FORWARD_ENABLED`) доповнено діагностичним логом `[STRICT_WHALE] ... carry-forward presence=...`.

## Додано

- Helper `extract_last_h1(stats)` для безпечного вилучення останнього HTF зрізу (`h1|agg_h1|last_h1`) замість двох дублюючих циклів.
- Helper `flag_htf_enabled()` для централізованого читання прапора `HTF_CONTEXT_ENABLED` (уникаємо повторних try/except імпортів).

## Очікуваний вплив

- Продуктивність: нейтральний (заміна цикла на O(1) перевірки + одна точка імпорту).
- Прозорість: легше QA — лог carry-forward показує reuse stale presence.
- Надійність: профільні та Stage2-lite хінти тепер завжди мають фактичні whale‑метрики (раніше могли підмінятися дефолтами).

## Rollback

- Повернення до попередньої поведінки можливе видаленням нових helperів та відновленням оригінальної індентації (не потрібне; змінені рядки локальні ≤30 LOC).

## Контракти

- Жодних змін у Stage1/Stage2/Stage3 структурах; лише внутрішня побудова `stats.whale`.
