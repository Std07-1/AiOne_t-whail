# PhaseState v2 — дизайн

## 1. Мета

Проблема: нинішній детектор фаз часто повертає `phase=None` через суворий `presence_cap_no_bias_htf`, сіру зону HTF або нестачу довіри до volz, навіть коли сирі WhaleTelemetry показують сталу активність. У результаті Stage2/Stage3 бачать «дірки» в фазах, зникає тяглість сценаріїв і деградують контекстні гейти.

Ціль: запровадити керовану памʼять фази (PhaseState), яка дозволяє тимчасово утримувати останню валідну фазу в сірих умовах без зняття жорстких гейтів. Ми хочемо зменшити частку `phase=None`, не відкривши нових ризиків: якщо відбувається справжній конфлікт (HTF розвернувся, bias змінив знак, ризик-прапор), PhaseState має негайно скидатись.

## 2. Вхідні дані та наявний контекст

PhaseState спирається на вже доступні поля Stage2:

- `phase`, `phase_score` від детектора.
- `phase_debug.htf_ok`, `phase_debug.htf_strength`.
- `phase_debug.reason` (наприклад, `presence_cap_no_bias_htf`, `htf_gray_low`, `no_zones`, `trend_weak`).
- Метрики обʼєму: `vol`, `volz`, `vol_z_samples` (для перевірки стабільності).
- Whale-поля: `whale_presence`, `whale_bias`, `whale_ctx.dominance_*`.
- Часові атрибути: `now_ts`, `age_s` (від попереднього PhaseState), порядковий `batch_id` / `bar_index`.

## 3. Структура PhaseState

Логічна схема:

```
PhaseState:
  current_phase: str | None          # що бачать Stage2/Stage3
  last_detected_phase: str | None    # сирий результат детектора
  phase_score: float                 # останній підтверджений score
  age_s: float                       # час з моменту підтвердженої фази
  last_reason: str | None            # останній phase_debug.reason, якщо примусове None
  last_whale_presence: float | None  # для перевірки знаку/амплітуди
  last_whale_bias: float | None
  last_htf_strength: float | None
  updated_ts: float                  # штамп для TTL / синхронізації
```

Де зберігаємо:

1. `stats.phase_state` — легка ін’єкція у Stage2 stats (для подальших етапів).
2. Redis namespace `ai_one:phase_state:{symbol}:{granularity}` — дає durability між батчами й можливість перегляду з інструментів QA.
3. Локальний кеш у Stage2 процесі (наприклад, `PhaseStateManager` із LRU). Початковий варіант — «stats + Redis», далі можна оптимізувати.

## 4. Правила оновлення PhaseState

### Нормальне оновлення
- Детектор повернув фазу `phase != None`.
- `phase_score >= PHASE_STATE_MIN_SCORE`.
- `phase_debug.reason` не містить жорстких кодів (див. нижче).
- Дії: `current_phase = phase`, `phase_score = score`, `last_detected_phase = phase`, `age_s = 0`, `last_reason = None`, оновити `last_whale_*`, `last_htf_strength`.

### Carry-forward (мʼяке продовження)
- Детектор повернув `phase=None`.
- Поточна `current_phase` існує і `age_s < PHASE_STATE_MAX_AGE` (наприклад, 180 с).
- `phase_debug.reason` ∈ мʼяких кодів: `{presence_cap_no_bias_htf, htf_gray_low, volz_too_low, volz_missing, trend_weak}`.
- Whale-сигнал не показує конфлікту: знак bias не змінився понад `BIAS_FLIP_DELTA`, dominance/ presence не впали нижче `PRES_MIN_FOR_HOLD`.
- Дії: зберегти `current_phase`, `phase_score` не оновлюємо, лише `age_s += delta_t`, `last_reason = reason`, `last_detected_phase = None`.

### Примусовий скидання
- `phase_debug.reason` ∈ жорстких кодів: `{htf_conflict, trend_reversal, risk_block, no_zones, low_atr_guard, anti_breakout_whale_guard}`.
- Або `age_s >= PHASE_STATE_HARD_MAX_AGE` (наприклад, 600 с).
- Або whale-сигнал показує конфлікт: `abs(new_bias - last_whale_bias) >= BIAS_FLIP_DELTA` із зміною знаку; `presence` < `PRES_MIN_RESET`; `dominance_up/down` змінилися на протилежні прапори.
- Дії: `current_phase = None`, `phase_score = 0`, `age_s = 0`, `last_reason = reason`, `last_detected_phase = None`.

## 5. Інтеграційні точки

- `stage2/phase_detector.py`: після обчислення phase/result викликаємо `PhaseStateManager.update(symbol, stats, context)`, отримуємо `phase_state.current_phase` та кладемо в `PhaseResult`.
- `market_context.meta.phase` тепер бере значення з PhaseState (коли прапор увімкнений). Сирий результат детектора зберігаємо в `phase_state.last_detected_phase` для діагностики.
- Stage2 експортує стислий хінт `phase_state_hint`: адаптер фаз (`utils.phase_adapter.detect_phase_from_stats`) зчитує `stats.phase_state` і повертає словник {phase, age_s, score, reason, presence, bias, htf_strength, updated_ts, direction_hint}. `direction_hint` генерується чистою функцією `utils.direction_hint.infer_direction_hint`: вона дозволяє лише `pullback_continuation` сценарії, вимагає |whale_bias| ≥0.25 і узгодження фази (`trend_up`, `trend_down`, `false_breakout`) з bias. `app/process_asset_batch.py` дублює цей блок у `market_context.meta.phase_state_hint`, щоб Stage3/Explain/QA бачили стан памʼяті й одразу мали мʼяку підказку для explain-логів без зміни контрактів.
- Stage3 / `trade_manager.py`: контракти не змінюємо; Stage3 просто споживає `market_context.meta.phase`, але з меншою кількістю `None`.
- Telemetry / Prometheus: додаємо серію `phase_state_age_seconds`, `phase_state_carry_forward_total{reason}` для QA.

## 6. Фіче-флаги та безпека

- Новий прапор `PHASE_STATE_ENABLED = False` у `config/config.py`.
- При `False`: PhaseState пише тільки лог/Redis, а Stage2 вихід залишається сирим.
- При `True`: Stage2/Stage3 отримують `PhaseState.current_phase` замість сирого при мʼяких reason-кодах.
- Додатковий прапор `PHASE_STATE_FORCE_LOG=True` для раннього QA — логувати таблицю `raw_phase → phase_state` кожні N батчів.

## 7. План інтеграції (чернетка)

1. **Імплементація шару** — клас `PhaseStateManager` (+ Redis адаптер), логувати оновлення й причини carry-forward без зміни Stage3.
2. **Експериментальний режим** — під `PHASE_STATE_ENABLED` віддавати у пайплайн `current_phase` для reason-кодів зі списку мʼяких. Паралельно писати Prometheus-лічильники `phase_state_carry_forward_total`.
3. **QA/forward** — прогнати `tools.unified_runner` (мін. 1 год) ON/OFF, виміряти: частка `phase=None`, вплив на ScenarioTrace/Act, наявність конфліктів з whale bias/HTF. `tools.unified_runner` піднімає той самий пайплайн, але дозволяє одразу задавати прапори PhaseState, telemetry та whale_signal_v1 в одному місці. Лише після цього рухатись до прод-канарейки.

## QA-критерії перед канарейкою

- Дотримуємось чекліста з `docs/phase_state_qa_checklist.md`: для кожного запуску зберігаємо логи, Prometheus дамп, `signals_truth.csv`, snapshot-и як артефакти.
- Скрипт `python -m tools.analyze_phase_state --log logs/run_phase_state_cf_on.log --out reports/phase_state_cf_on/phase_state_qa.md` має показати: (а) зниження частки `phase=None` проти baseline; (б) перерахунок reason-кодів без домінування жорстких кодів (`low_atr_guard`, `htf_conflict`).
- Порівнюємо `signals_truth.csv` (FP/TP, latency) між ON/OFF сценаріями — допустима деградація ≤20% по latency й відсутність новых FP.
- Якщо хоча б один критерій не виконується, залишаємо прапор `PHASE_STATE_ENABLED=False`, аналізуємо причини carry-forward через звіт `phase_state_qa.md` і повторюємо запуск.

## Статус імплементації (крок 1)

- Реалізовано PhaseStateManager (stats + Redis).
- PhaseState оновлюється після phase_detector і логиться у `stats.phase_state` та `[PHASE_STATE_UPDATE]` логах.
- Контракти Stage2/Stage3 поки не змінюються — `current_phase` використовується тільки для QA.
- Покрито базовими юніт-тестами (`tests/test_phase_state_manager.py`) для нормального, carry-forward та reset-потоків.
