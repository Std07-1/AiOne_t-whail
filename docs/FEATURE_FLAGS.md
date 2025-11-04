# Фіче‑флаги (огляд і правила)

Фіче‑флаги використовуються для безпечного ввімкнення/вимкнення функціоналу без зміни контрактів і з можливістю миттєвого rollback.

## Загальні принципи
- Усі флаги зберігаються у `config/flags.py` із короткими докстрінгами.
- Вплив на рішення має зникати повністю при `False` (roll‑back за 1 клік).
- Важкі обчислення не повинні вмикатись у гарячому шляху без контролю перф‑бюджету.

## Перелік важливих фіче‑прапорів

- `FEATURE_PARTICIPATION_LIGHT`
  - Опис: вмикає «тихий тренд» (drift_trend) у фазному детекторі Stage2‑lite для реплею/телеметрії/UI‑інсайтів.
  - Контракти: незмінні; додає опційні `market_context.meta.insights.quiet_mode[,_score]`.
  - Тюнінг: `config/config_stage2.py: PARTICIPATION_LIGHT_THRESHOLDS`.

- Stage1 тригери (централізовані прапори)
  - `STAGE1_TRIGGER_VOLUME_SPIKE_ENABLED` — дозволяє/вимикає тригер volume_spike.
  - `STAGE1_TRIGGER_BREAKOUT_ENABLED` — дозволяє/вимикає тригер breakout/near_*.
  - `STAGE1_TRIGGER_VOLATILITY_SPIKE_ENABLED` — дозволяє/вимикає тригер volatility_spike.
  - `STAGE1_TRIGGER_RSI_ENABLED` — дозволяє/вимикає RSI/дивергенції.
  - `STAGE1_TRIGGER_VWAP_DEVIATION_ENABLED` — дозволяє/вимикає тригер відхилення від VWAP.
  - Примітка: ці прапори є дефолтами; їх можна тимчасово перевизначити у рантаймі через `feature_switches={"triggers": {name: bool}}` при створенні `AssetMonitorStage1`.

- Directional:
  - `FEATURE_DIRECTIONAL_GATING` — вмикає залежні фільтри/гейтинг для рекомендацій.
  - `FEATURE_DIRECTIONAL_REWEIGHT` — змінює ваги підсистем при конфлікті напрямків.
  - `FEATURE_RAPID_MOVE_HYSTERESIS` — гістерезис швидких рухів.

- Crisis Mode:
  - `crisis_mode_eval`, `crisis_mode_override_filters`, `proactive_scenarios_enabled` — управління кризовими сценаріями.

- Insight/Whale:
  - `INSIGHT_LAYER_ENABLED`, `PROCESSOR_INSIGHT_WIREUP` — дозволи на інсайт‑шар та відповідну маршрутизацію.
  - Нотатка: «soft» рекомендації/сигнали формуються тонким адаптером поверх `whale.insight_builder` без дублювання логіки. Усі пороги/ваги беруться з конфігів (див. `config/config_whale.py`). Rollback = вимкнути відповідні фіче‑флаги — контракти не змінюються.

## Rollback
- Вимкніть відповідний прапор у `config/flags.py` — вплив зникне негайно, без зміни кодової бази.
- Для телеметрійних інсайтів (напр., `quiet_mode`) — відсутність поля в UI означає «неактивно/вимкнено».

## Перф‑бюджет
- Допускається ≤ +20% до пікових `avg_wall_ms` у гарячих ділянках.
- Важкі блоки переносити у `cpu_pool` або обмежувати семафорами.
