# Інструкція для GitHub Copilot (AiOne_t) • v2.4

---
# Інструкція для GitHub Copilot (AiOne_t) • v2.3

## Правило №1

* Усе українською: чат, коментарі, докстрінги, логи.
* Регулярно звіряй `.github/copilot-memory.md` та roadmap.

## Цілі

* Бізнес: PnL↑, winrate↑, FP↓, latency ≤200 мс/бар, прозорість↑.
* Техніка: **мінімальний диф**, без зміни контрактів, фіче-флаги для всього.

## Контракти

* **Stage1/Stage2/Stage3** не змінювати. Нові дані лише в:
## Policy Engine v2 (T0) — сигнали під фіче‑флагом

Мета T0: керовані сигнали без зміни контрактів Stage1/2/3, з мінімальним ризиком і прозорою QA‑оцінкою.


  * `market_context.meta.*`
  * `confidence_metrics.*`
* API/подписи функцій не чіпати.

## BTC gate v2 (hint + v1 penalty)

* BTC‑режим визначається як `flat` / `trend_up` / `trend_down`.
* Правила v2: якщо `htf_strength≥SCEN_HTF_MIN` і `phase∈{"momentum","drift_trend"}` —
  тоді `trend_up` при `cd≥0` або `slope_atr≥0`; `trend_down` при `cd<0` або `slope_atr<0`; інакше `flat`.
* Вплив на conf наразі лише як у v1: для альтів у `flat` і низькому HTF (`btc_htf_strength<SCEN_HTF_MIN`) застосовується м’який штраф −20%.
* Нові дані залишаються лише в `market_context.meta.*`: `btc_regime`, `btc_htf_strength`. Прапор: `SCEN_BTC_SOFT_GATE=True`.

## Фіче-флаги (config/config.py)

Тримай як джерело правди. Усі зміни лише під цими прапорами.

```
STRICT_ACCUM_CAPS_ENABLED: bool = True
STRICT_HTF_GRAY_GATE_ENABLED: bool = True
STRICT_ZONE_RISK_CLAMP_ENABLED: bool = True
STRICT_LOW_ATR_OVERRIDE_ON_SPIKE: bool = True
PROM_GAUGES_ENABLED: bool = False
PROM_HTTP_PORT: int = 9108
PRESENCE_CAPS = {"accum_only_cap": 0.30}
HTF_GATES = {"gray_low": 0.10, "ok": 0.20, "gray_penalty": 0.20}
STAGE2_PRESENCE_PROMOTION_GUARDS = {"cap_without_confirm": 0.20}
LOW_ATR_SPIKE_OVERRIDE = {
  "band_expand_min": 0.013, "spike_ratio_min": 0.60,
  "abs_volz_min": 2.50, "dvr_min": 1.20, "bars_ttl": 3
}
```

## Redis-ключі

* Лише через `config`-утиліти. Формат: `ai_one:{domain}:{symbol}:{granularity}`.
* Для override: `ai_one:overrides:low_atr_spike:{symbol}` з TTL `bars_ttl * interval_sec`.

## Перф-бюджет
# v2.4 • Оновлено: 2025-11-03
* Дозволено: ≤ +20 % до `avg_wall_ms` у піках.
* Важке в `cpu_pool`. I/O — best-effort, без блокувань.

## Логування

* Однорядкові маркери: `[STRICT_PHASE] [STRICT_SANITY] [STRICT_WHALE] [STRICT_GUARD] [STRICT_RR] [STRICT_VOLZ] [LOW_ATR_OVERRIDE] [HTF]`.
* Порядок полів: `symbol ts phase scenario reasons presence bias rr gates`.

## Процес прийняття рішень (CCP v1.2)

1. **SCENARIOS** 2–3 варіанти.
2. **IMPACT** latency/PnL/winrate/ризики.
3. **GO/NO-GO**.
4. **PLAN**: файли, тести, rollback.
   Немає чіткої мети → **PLAN-ONLY** без коду.

## Коли не кодимо

* Немає RCA. Потрібна міграція. Ламаються контракти. Нові залежності без плану. → **PLAN-ONLY**.

## Atomic Patch

Design → Код (в межах Scope) → Тести (happy+edge+stream) → Логи → Док → Rollback.

## Тести

* Unit + smoke (псевдострім).
* TDD-light: спочатку «червоний» контракт-тест, потім фікс.

## TODO-формат (довгі задачі)

```
- [ ] META/ціль
- [ ] SCENARIOS/GO-NO-GO
- [ ] FILES/DIFF (≤30 LOC, 1–2 файли)
- [ ] TESTS (unit+smoke)
- [ ] LOGS/METRICS
- [ ] ROLLBACK/FLAGS
- [ ] KPI/Acceptance
```

---

# Політики 2025-10-22 (ядро)

### 1. Whale v2 / Flat policy

* **Presence cap**: якщо `dist_zones=0 ∧ liquidity_pools=0` → `presence ≤ PRESENCE_CAPS.accum_only_cap` під `STRICT_ACCUM_CAPS_ENABLED`.
* **Risk clamp**: та ж умова → `risk_from_zones ≤ STAGE2_PRESENCE_PROMOTION_GUARDS.cap_without_confirm` під `STRICT_ZONE_RISK_CLAMP_ENABLED`.
* Відсутній orderbook → не штрафувати, просто не додавати компонент.

### 2. HTF Gate

* `htf_strength < HTF_GATES.gray_low` → `htf_ok=False`.
* `gray_low ≤ htf_strength < ok` → `score -= gray_penalty` (не нижче 0).
* `htf_stale_ms > settings.stage1_htf_stale_ms` → `htf_ok=False` (якщо обидва значення є).

### 3. Low-ATR Spike Override

* Умова (Stage1):
  `band_expand ≥ 0.013 ∧ spike_ratio ≥ 0.60 ∧ |vol_z| ≥ 2.5 ∧ DVR ≥ 1.2`.
* Дія: `low_atr_override.active=True` на `bars_ttl` барів, TTL у Redis-ключі вище.
* Stage2: спершу читає Redis, потім `stats.overrides`, потім none. Override **лише** знімає `low_atr_guard`.

### 4. ACCUM_MONITOR постпроцесор

* Якщо `phase is None ∧ atr_ratio < 1.0 ∧ not low_atr_override.active` → `phase="ACCUM_MONITOR"`, `score=0`, reason `low_atr_guard`.
* Реалізація централізовано у `_apply_phase_postprocessors`, усі `return PhaseResult(...)` проходять через нього.

### 5. Сценарії: гістерезис і канарейка

* Гістерезис сценаріїв увімкнено прапором `STRICT_SCENARIO_HYSTERESIS_ENABLED=True` і застосовується у `app/process_asset_batch.py` (2 послідовні бари або сумарна conf ≥ 0.30; для pullback — додаткові умови presence/htf).
* Для коротких перевірок дозволено `SCENARIO_CANARY_PUBLISH_CANDIDATE=True`: кандидат (`resolve_scenario`) публікується у `market_context.meta.scenario_*` і в Prometheus `ai_one_scenario` навіть без підтвердження гістерезисом. Контракти Stage1/Stage2 не змінюються; вимикати після канарейки.

---

# Стандарти змін (що саме робити копілоту)

## A. Санітизація чисел

* Використовуй `sanitize_ohlcv_numeric(df)` **до** обчислень і перед `to_csv/to_parquet`.
* Порівняння `presence/bias/vwap_dev` тільки через `float()` + `math.isfinite()`.

## B. Місця застосування капів/клампів

* `whale_telemetry_scoring.py` — cap presence у flat.
* `supply_demand_zones.py` і `whale_detection.py` — clamp risk у flat.

## C. Override читання

* `process_asset_batch.py` або адаптер перед фазуванням: Redis → stats → none.
* Не створюй нових структур; мерж у `normalized["stats"]["overrides"]["low_atr_spike"]`.

## D. Метрики Prometheus (опційно, під прапором)

* `low_atr_override_active{symbol}`, `low_atr_override_ttl{symbol}`, `htf_strength{symbol}`, `presence{symbol}`.
* Додатково: `ai_one_scenario{symbol,scenario}`, `ai_one_phase{symbol,phase}`.
* `PROM_GAUGES_ENABLED=True` вмикає легкий HTTP на `PROM_HTTP_PORT`.
* Операційна примітка: `/metrics` доступний лише поки процес працює. Для знімання метрик відкривайте окреме вікно термінала і опитуйте ендпоінт паралельно до прогона (`tools/run_window`).

## E. CLI-прапори (опційно)

* `--enable/--disable-accum-caps`
* `--enable/--disable-htf-gray`
* `--enable/--disable-low-atr-spike-override`
* `--prometheus`

---

# Епізоди та реплей (операційний шаблон)

## Маніфест

```json
[
  {"symbol":"BTCUSDT","scenario":"breakout_confirmation","start":"YYYY-MM-DDTHH:MM:00Z","end":"..."},
  {"symbol":"TONUSDT","scenario":"pullback_continuation","start":"...","end":"..."}
]
```

## Команди

```bash
python -m tools.generate_and_replay_episodes \
  --symbols BTCUSDT,TONUSDT,SNXUSDT \
  --interval 1m \
  --lookback 360 --limit 900 \
  --scenarios breakout_confirmation,pullback_continuation \
  --per-symbol 1 --require-both-scenarios \
  --out-dir replay_bench

python -m tools.bench_pseudostream \
  --episodes-manifest replay_bench/episodes_manifest.jsonl \
  --strict-datetime-match \
  --enable-accum-caps --enable-htf-gray --enable-low-atr-spike-override \
  --out-dir replay_bench/run
```

## Інструкції для запуску Python-кодів у PowerShell (Windows)

Коли генеруєш команди для запуску невеликих Python-фрагментів у VS Code (Windows, PowerShell), дотримуйся таких правил:

1. Використовуй PowerShell-нотацію та явну зміну каталогу:
  ```powershell
  cd C:\Aione_projects\AiOne_t-whail
  .\.venv\Scripts\python.exe -c "<однорядковий Python-код; інструкції розділяй ; >"
  ```

2. Правила всередині аргумента для -c:
  - Увесь код має бути в одному рядку.
  - Окремі інструкції розділяй ;.
  - Не вставляй переносів рядка, heredoc або конструкції, що залежать від bash/zsh.

3. Заборонено (для мого середовища PowerShell):
  - Bash heredoc та подібний синтаксис:
    ```bash
    python - <<'PY'
    # такий формат не можна використовувати в PowerShell
    PY
    ```
  - Команди, які припускають, що термінал — це bash/zsh (Linux/WSL/macOS).

Приклад (еталон) для тесту explain_should_log:
  ```powershell
  cd C:\Aione_projects\AiOne_t-whail
  .\.venv\Scripts\python.exe -c "from process_asset_batch.helpers import explain_should_log; from process_asset_batch.global_state import _SCEN_EXPLAIN_LAST_TS, _SCEN_EXPLAIN_BATCH_COUNTER; _SCEN_EXPLAIN_LAST_TS.clear(); _SCEN_EXPLAIN_BATCH_COUNTER.clear(); print(explain_should_log('ethusdt', 0.0, min_period_s=10.0, every_n=3)); print(_SCEN_EXPLAIN_LAST_TS, _SCEN_EXPLAIN_BATCH_COUNTER); print(explain_should_log('ethusdt', 1.0, min_period_s=10.0, every_n=3)); print(_SCEN_EXPLAIN_LAST_TS, _SCEN_EXPLAIN_BATCH_COUNTER)"
  ```

## Acceptance

* `episodes_manifest.jsonl` містить обидва сценарії.
* `replay_summary.csv`: `scenario_detected ∈ {breakout_confirmation,pullback_continuation}`, `confidence>0`.
* Логи мають `[LOW_ATR_OVERRIDE]` лише на спайках; у флєті — `phase=ACCUM_MONITOR`.
* Часи реплею збігаються 1:1 з маніфестом.

---

# PR-чек-лист

* Ціль/вплив описані. Фіче-флаг є.
* Диф ≤30 LOC, 1–2 файли. Контракти незмінні.
* Логи `[STRICT_*]` додані.
* Тести: unit + псевдострім. `pytest -q` PASS.
* Rollback: вимкнути прапор. Немає залишкових side-effects.
* Док: README/docs оновлені, якщо змінилася публічна поведінка.

# Заборонено

* Літери в Redis-ключах, хардкод часу/таймзони.
* Блокуючі I/O у критичному шляху без пулів.
* Рефакторинг «заради краси» без KPI й тестів.
* Зміни в Stage-контрактах та сигнатурах.

---

# KPI перед увімкненням у прод

* TRX flat: `presence≤0.30`, `risk≤0.20`, `ACCUM_MONITOR`, без рекомендацій.
* BTC/ETH тренди: з’являються `EXPANSION_START` у відповідних епізодах, FP не росте.
* Немає `RuntimeWarning` у серіалізації.
* Перф: без регресу `avg_wall_ms`.

> Формат відповіді Copilot: **PLAN-ONLY / PATCH / REVIEW**. Якщо даних не вистачає — **PLAN-ONLY**.

# v2.3 • Оновлено: 2025-10-31
# Автор: [Std07-1]
