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

- Profile Engine v2 (T0):
  - `PROFILE_ENGINE_ENABLED` — вмикає профільні сигнали у Stage2‑lite під телеметрійний хінт `stats.stage2_hint` (контракти незмінні).
  - Семантика (коротко): `require_dominance`, `alt_confirm_min`, `hysteresis`, `cooldown`, `atr_scale`, `k_range(meta)`.
  - Причини у `reasons`: компактні коди `profile=...,dom_ok=0/1,alt_ok=0/1,hysteresis=0/1,cooldown=0/1,stale=0/1` + базові `presence_ok,bias_ok,vwap_dev_ok`.
  - Персист кулдауну (необовʼязково): `ai_one:ctx:{symbol}:cooldown` TTL≈120 с, best‑effort.
  - Пер‑класне масштабування `score` (консервативніше для ALTS): BTC=1.0, ETH=0.95, ALTS=0.85.
  - Метрики Prometheus (best‑effort): `ai_one_profile_active{symbol,profile,market_class}`, `ai_one_profile_confidence{symbol}`,
    `ai_one_profile_switch_total{from,to}`, `ai_one_profile_hint_emitted_total{dir}`.

## Rollback
- Вимкніть відповідний прапор у `config/flags.py` — вплив зникне негайно, без зміни кодової бази.
- Для телеметрійних інсайтів (напр., `quiet_mode`) — відсутність поля в UI означає «неактивно/вимкнено».

## Перф‑бюджет
- Допускається ≤ +20% до пікових `avg_wall_ms` у гарячих ділянках.
- Важкі блоки переносити у `cpu_pool` або обмежувати семафорами.
