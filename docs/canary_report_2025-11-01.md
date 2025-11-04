# Канарейка сценаріїв • Звіт (2025-11-01)

## Резюме

- Мета: тимчасово (10–15 хв) увімкнути публікацію кандидата сценарію з пом’якшеними порогами, зібрати факти з /metrics і Redis, не змінюючи контракти Stage1/2/3 і не редагуючи env.
- Підсумок: публікація кандидатів у Prometheus успішна — `ai_one_scenario{symbol,scenario}` має ненульові значення для низки символів. Запис `scenario_*` у Redis-стан на момент відбору знімків траплявся не стабільно (наявні state_* файли без полів).
- Висновок: підтверджено працездатність телеметрії та canary‑path. Порогові пропозиції наразі відсутні (немає домінантної причини REJECT >50%).

## Джерела/артефакти

- Prometheus:
  - `metrics_scenario.txt` — містить, зокрема:
    - BTCUSDT pullback_continuation ≈ 0.415
    - ICPUSDT pullback_continuation ≈ 0.59
    - TONUSDT, SNXUSDT pullback_continuation ≈ 0.31
  - `metrics_phase.txt` — фази/бали (momentum/drift_trend/false_breakout/accum_monitor).
- Redis snapshot-и:
  - `state_BTCUSDT.txt`, `state_TONUSDT.txt`, `state_SNXUSDT.txt` — на момент зняття не містили `scenario_detected/confidence`.
- Агрегований звіт колектора: `report_canary.txt`
  - `ai_one_scenario (scenario!=none) present: YES`
  - Top REJECT reasons: (no data)
  - Proposed threshold tweaks: No dominant single cause (>50%).
- `scenario_trace_summary.txt` — порожня категоризація (недостатньо даних у вікні).

## Конфіг/фіче‑флаги (актуальні для канарейки)

- Публікація кандидатів: `SCENARIO_CANARY_PUBLISH_CANDIDATE=True` (тимчасово).
- Гістерезис ввімкнено: `STRICT_SCENARIO_HYSTERESIS_ENABLED=True` (для стабільних публікацій).
- Пороги сценаріїв — лише з `config/config.py` (без env): SCEN_*.
- Prometheus: `PROM_GAUGES_ENABLED=True`, порт `PROM_HTTP_PORT=9108`.

## Прийняття (Acceptance)

- [x] У /metrics присутні рядки `ai_one_scenario` із сценаріями ≠ "none".
- [x] Prometheus endpoint активний, метрики фаз/HTF/override доступні.
- [~] Redis `ai_one:state:{symbol}`: `scenario_detected/confidence` — не зафіксовано у знімку часу (залежить від таймінгу публікації UI і гістерезису).
- [ ] Збір причин REJECT — недостатньо подій у вікні канарейки.

## Висновки і пропозиції

- Транспорт і телеметрія для сценаріїв працюють за планом.
- Зараз немає підстав змінювати пороги: «No dominant single cause (>50%)». Пропонується залишити SCEN_* без змін.
- Для отримання стабільних `scenario_*` у Redis-стані:
  - Запустити додаток на довший період або збільшити вікно відбору колектора.
  - Перевірити каденс UI‑паблішера відносно моментів встановлення `market_context.meta.scenario_*`.

## План наступних кроків

1. Дати канарейці попрацювати довше (≥30–60 хв) і повторити збір артефактів.
2. За наявності достатньої кількості SCENARIO_TRACE — побудувати reject‑breakdown (>100 подій).
3. Якщо `state_*` досі нестабільні — звірити публішинг в UI прошарку й умови гістерезису.
4. Завершити: вимкнути канарейку і повернути SCEN_* до базових значень у конфігу.

## Rollback

- Вимкнути публікацію кандидатів: `SCENARIO_CANARY_PUBLISH_CANDIDATE=False`.
- Повернути тимчасово пом’якшені `SCEN_*` до базових значень у `config/config.py`.
- Prometheus можна залишити увімкненим; за потреби — `PROM_GAUGES_ENABLED=False`.

## Додаток: уривки метрик

```
# HELP ai_one_scenario Scenario confidence (0..1)
ai_one_scenario{scenario="pullback_continuation",symbol="BTCUSDT"} 0.415
ai_one_scenario{scenario="pullback_continuation",symbol="ICPUSDT"} 0.59
ai_one_scenario{scenario="pullback_continuation",symbol="TONUSDT"} 0.31
ai_one_scenario{scenario="pullback_continuation",symbol="SNXUSDT"} 0.31
```
