# Crisis Mode: інтегровані кроки, статус і наступні етапи (2025‑10‑22)

Цей документ консолідує виконані зміни навколо прискорення Stage2 та окреслює безпечний план впровадження кризового режиму (Crisis Mode) з чіткими фіче‑флагами, конфігами, тестами та метриками. Мета — щоб система не «сліпла» під час екстремальних рухів (knife, liquidation cascades), зберігала низьку затримку та не ламала чинні контракти Stage1/Stage2/Stage3/UI.

## 1. Що вже інтегровано в Stage2 (готово)

- Fast‑path → `stage2/processor_v2/run_fast_path.py` з уніфікованим вихідним контрактом і валідацією.
- Debounce і перф → `stage2/processor_v2/narrative_debounce.py`, `stage2/processor_v2/perf_helpers.py`.
- Corridor v2 + CPU офлоад → `stage2/processor_v2/levels.py` + `stage2/processor_v2/load_or_compute_corridor.py` (через `Stage2CorridorLoader._compute_corridor_sync`).
- Whale Engine консолідовано → `whale/engine.py` (імпорти `whale.*`).
- Планувальник ризику → `stage2/processor_v2/maybe_apply_risk_planner.py` (делегується приватним `_maybe_apply_risk_planner`).
- Мета/агрегація/HTF → `stage2/processor_v2/meta_and_agg.py`.
- Агрегації сценаріїв/рекомендацій → `stage2/processor_v2/agg_counts.py` (rr_hist, perf/system publish).
- Інтеграційний псевдо‑стрім тест → `test/test_integration_stage2_smoke.py` (datastore JSONL).

Інваріанти: контракти Stage1Signal/Stage2Output незмінні; додаткові поля — лише у `market_context.meta.*`.

## 2. Crisis Mode — що потрібно додати (дизайн і конфіг)

Нижче — модулі й параметри, що впроваджуються поетапно. Усі пороги та фіче‑флаги тільки через `config/config.py` (без хардкодів).

### 2.1 Детектори кризових умов (telemetry‑first)

- `monitoring/cascade_detector.py` → `LiquidationCascadeDetector`:
  - Параметри: `liquidation_spike_threshold` (≈3.0×), `volume_spike_threshold` (≈2.5×), `time_window` (≈5 хв).
  - Повертає: `{cascade_detected: bool, severity: float∈[0,1], type: 'bearish'|'bullish'}`.
  - Запис: `market_context.meta.crisis.cascade_detected` (+ пояснення).
- `stage1/trap_detector.py` → `detect_trap_signals(price_data, volume_data) -> bool`:
  - Умови: прорив рівнів, волатильність `short/long ATR`, обсяг (з/без тик‑дельти), прискорення ціни; активується при ≥3 умовах.
  - Запис: `market_context.meta.trap_detected=True` + причини.
- `stage2/indicators.py` → `MultiTimeframeTrend`:
  - Метод: `calculate_trend_confluence()` → `{confluence: bool, direction: 1|-1, strength: 0..1}`.
  - Поріг `trend_strength_threshold` у `config`.
  - Запис: `market_context.meta.trend_confluence`.
- `stage2/volatility.py` → `detect_volatility_regime(atr_values) -> (label, atr_ratio)`:
  - Класи: `hyper_volatile`, `high_volatile`, `normal` за `shortATR/longATR`.
  - Пороги: `STAGE2_VOLATILITY_REGIME.{hyper_threshold, high_threshold}`.
  - Запис: `market_context.meta.volatility_regime`.

Конфіг: `STAGE2_CRISIS_MODE.{cascade_threshold, vol_spike_threshold, ttl_sec}`;
`STAGE2_VOLATILITY_REGIME.{hyper_threshold, high_threshold}`.

### 2.2 Менеджер кризового режиму і ризик‑адаптер

- `stage2/crisis.py` → `CrisisModeManager`:
  - Вхідні індикатори: `cascade_detected`, `trap_detected`, `trend_confluence`, `volatility_regime`, `key_level_break`.
  - Вихід: `{crisis_mode: bool, severity: 0..1, override_filters: bool, boost_aggression: float}`.
  - Активується, якщо істинні ≥ `min_indicators` (див. конфіг).
- `stage2/risk_dynamic.py` → `DynamicRiskManager`:
  - Коригує `rr_threshold`, `position_size_multiplier`, `trend_boost_multiplier`, `disable_conservative_filters` за `severity`.
  - Приклад: `rr_threshold_new = rr_threshold_base × (1 − 0.5×severity)`; `position_size = 1 + severity`; `trend_boost = 1 + 3×severity`.

Конфіг: `STAGE2_CRISIS_MODE.{min_indicators, aggression_base, aggression_per_indicator}`.

### 2.3 Кризові стратегії і мапінг

- Нові профілі: `bearish_liquidation_cascade`, `bullish_reversal_from_extreme`.
- Умови активації: `crisis_mode=True` + напрямок `trend_confluence` + `key_level_break` / `support`.
- Мапінг: доповнити `PHASE_TO_STRATEGY` і `STAGE3_STRATEGY_PROFILES` (RR мінімальні значення, короткі SL на 1м, cooldown≈1 хв).

### 2.4 Інтеграційний порядок (Stage2 → Stage3)

1) `LiquidationCascadeDetector.detect_cascade()`
2) `trap_detector.detect_trap_signals()`
3) `MultiTimeframeTrend.calculate_trend_confluence()`
4) `detect_volatility_regime()`
5) `CrisisModeManager.activate_crisis_mode()` + (опційно) `DynamicRiskManager.adjust_parameters_crisis()`
6) Фаза/стратегія (поточний пайплайн) з умовним перекиданням у кризові профілі

Гейти: при `crisis_mode=True` допустиме тимчасове послаблення `low_rr_stage2`/`low_atr_ratio`/`proxy_volz_policy` (лише за фіче‑флагами).

### 2.5 Телеметрія та тестування

- Додати поля телеметрії: `crisis_mode`, `cascade_severity`, `trap_detected`, `trend_confluence`, `volatility_regime`, `whale_activity`.
- Інтеграційні тести: реплеї 9–10 жовтня 2025 (очікування: вибір `bearish_liquidation_cascade` при перших падіннях за умов).
- Перф бюджет: +≤20% до `avg_wall_ms` у піках; CPU‑офлоад для важких кроків; кеші з TTL.

## 3. Фіче‑флаги і Rollout

- `crisis_mode_eval` — лише оцінює й логгує.
- `crisis_mode_override_filters` — дозоляє послаблення гейтів.
- `proactive_scenarios_enabled` — увімкнення проактивних рекомендацій (UI‑only на першому етапі).
- Rollback: вимкнути флаги у конфігу — система повертається до стабільного шляху без зміни контрактів.

## 4. Acceptance та контрольні метрики

- FAST_SKIP частка в кризові вікна зменшується без росту FP; не втрачаємо продуктивність (>20% ліміту).
- В JSONL зʼявляються нові meta‑поля; агрегати публікуються у `REDIS_DOC_CORE/REDIS_CORE_PATH_STATS` (best‑effort, TTL=`CORE_TTL_SEC`).
- Smoke‑реплей 9–10 жовтня 2025 проходить без падінь; при наявності умов обираються кризові профілі.

## 5. Нотатки сумісності

- Контракти Stage1/Stage2/Stage3 не змінюються; усе нове — в `market_context.meta.*`.
- Ключі Redis — лише через `config/config.py` (шаблон `ai_one:{domain}:{symbol}:{granularity}`).
- Логування через RichHandler, без чутливих даних; INFO — події, DEBUG — розрахунки.
