# План реорганізації whale‑логіки

## 1. Інвентаризація поточного стану

| Модуль / файл | Роль і функціональність | Стан | Коментарі |
| ------------- | ----------------------- | ---- | --------- |
| `whale/engine.py` | Поточний двигун китової телеметрії: обчислює presence, whale_bias, dominance, VWAP‑відхилення, інʼєктує у `ctx.meta.whale`. Має локальний EMA стан `_PRESENCE_EMA_STATE`. | Мутує стан (глобальний `_PRESENCE_EMA_STATE`, in‑place ctx). | Немає I/O. Скоринг вже винесено у `whale_telemetry_scoring.py`. |
| `whale/whale_telemetry_scoring.py` | Статичні pure‑функції для presence, bias, dominance. Використовує `config_whale.py`. | Pure. | Викликається з engine та Stage2. |
| `app/whale_worker.py` | Асинхронний воркер Stage1/Stage2: читає 1m бари з Redis, рахує VWAP‑dev, slope, iceberg, zones, викликає скоринг, формує `whale.v2` payload. | Мутує стан (`last_tick_ts`, Redis). | Часткове дублювання presence/dominance з engine. |
| `process_asset_batch/helpers.py` | Canonical helpers для Stage2: `_active_alt_keys`, `should_confirm_pre_breakout`, `score_scale_for_class`, `normalize_and_cap_dvr`, тощо. | Мутує глобальні стани: `_DVR_EMA_STATE`, `_ACCUM_MONITOR_STATE`, `_PROM_PRES_LAST_TS`. | Експортує `_alias` для сумісності. |
| `app/process_asset_batch.py` | Головний Stage2 процесор: отримує whale‑метрики, фази, хинти, сценарії, DVR, згладжує та капує, записує у state manager. | Мутує глобальні контексти, кеші, cooldown. | Дублює pre‑breakout/alt‑confirm логіку зі Stage3. |
| `stage3/trade_manager.py` | Менеджер життєвого циклу угод (створення, оновлення, закриття, trailing/early exit). | Мутує стан (словник угод, локи). | Споживає Stage2 сигнали, майбутні мінімальні адаптери. |
| `stage3/open_trades.py` | Оркеструє відкриття угод за сигналами Stage2 (health damping, debounce, directional gating, volatility/corridor). | Мутує стан (через TradeLifecycleManager). | Дублює pre‑breakout/alt‑confirm гейти. |
| `config/config_whale.py` | Центр конфігурації: ваги presence, пороги dominance, EMA параметри. | Статичний. | Потрібно зібрати всі константи whale‑підсистеми. |
| `config/flags.py` | Feature‑flags: `WHALE_SCORING_V2_ENABLED`, `ENABLE_LARGE_ORDER_DETECTION`, `ENABLE_WHALE_BEHAVIOR_PREDICTOR`. | Статичний. | Використовувати для міграції на нову архітектуру. |
| `Stage1 helpers.py` | Допоміжні функції Stage1 (ATR, order book depth, open interest). | Мутує стан (Redis кеші). | Можемо успадкувати патерн кеш‑контролю. |
| `stage3/min_signal_adapter.py` | Відсутній. | — | Потрібно реалізувати. |
| Інші (`metrics/whale_*`, `whale_stage2.py`, `embed_whale_metrics.py`) | Не знайдені. | — | Ймовірно архівовані / інтегровані. |

### Чисті (Pure) компоненти
- `whale_telemetry_scoring.py` (усі score‑функції)
- Частина helpers: `score_scale_for_class`, `compute_profile_hint_direction_and_score`

### Станові (Stateful) компоненти
- `whale/engine.py` (EMA presence)
- `process_asset_batch/helpers.py` (`normalize_and_cap_dvr`, `update_accum_monitor`, `emit_prom_presence`)
- `app/whale_worker.py`
- `app/process_asset_batch.py`
- `stage3/trade_manager.py`

### Тестові/зворотні шими
- `_active_alt_keys`, `_normalize_and_cap_dvr` та інші `_alias` — прибрати після рефакторингу.

## 2. Дублювання та класифікація логіки

#### EMA згладжування та капування
- Presence EMA в `whale/engine.py` через `_PRESENCE_EMA_STATE`.
- DVR EMA + cap в `normalize_and_cap_dvr` через `_DVR_EMA_STATE`.
- Подібні підходи у Stage1 helpers та `whale_worker` (VWAP deviation).
Класифікація: критична телеметрична логіка → винести в ядро; глобальні словники → замінити параметризованим станом.

#### Dominance та bias
- Формули в `whale_telemetry_scoring.py`.
- Часткові дублікати / fallback у `engine.py` та `whale_worker.py`.
Класифікація: стандартизувати одну формулу в ядрі.

#### Гейти (pre_breakout, alt_confirm)
- Є в `helpers.py` та `open_trades.py`.
Класифікація: виділити універсальні pure‑гейти, використовувати через адаптери Stage2/Stage3.

#### Active alt‑keys
- `active_alt_keys` + дублікати `_active_alt_keys`.
Класифікація: залишити один pure‑інтерфейс.

#### Prometheus метрики
- `emit_prom_presence` в helpers + логіка публікації у `whale_worker`.
Класифікація: винести в `whale/adapters/metrics.py`.

## 3. Цільова архітектура

### 3.1 Ядро `whale/core.py`

Функції (усі pure):
```python
compute_presence(strats: dict[str, Any], cfg: WhaleConfig, prev_state: float | None) -> tuple[float, float]
compute_bias(strats: dict[str, Any], cfg: WhaleConfig) -> float
compute_dominance(strats: dict[str, Any], cfg: WhaleConfig) -> dict[str, Any]
should_confirm_pre_breakout(...)
should_confirm_alt_confirm(...)
smooth_dvr(value: float, prev: float | None, cfg) -> float
```

Типи:
```python
@dataclass
class WhaleSnapshot:
    presence_smoothed: float
    presence_raw: float
    bias: float
    dominance: dict[str, Any]
    vwap_deviation: float
    dvr: float
    timestamp: int
    alt_flags: set[str]
    version: str
    stale: bool
```

```python
class WhaleTelemetry(TypedDict):
    version: str
    stale: bool
    snapshot: WhaleSnapshot
    analytics: dict[str, Any]
```

Стан передається явним параметром (`prev_state`) — без глобальних словників.

### 3.2 Адаптери

- Stage2: `whale/adapters/stage2.py` — конструює `strats`, викликає ядро, повертає `WhaleSnapshot`; backward‑сумісність через тонку обгортку в `engine.py`.
- Stage1: опційно для джерел order book / latency.
- Stage3 Minimal Signal: `whale/adapters/stage3_min_signal.py` — формує мінімальний сигнал (наприклад, `presence_sustain`, `edge_persist`).
- Metrics: `whale/adapters/metrics.py` — Prometheus (без EMA станів).
- UI: формує payload для `insight.card.v2`.

### 3.3 Контракти та конфігурації
- Конфіг лише в `config/config_whale.py` (ваги, пороги, alpha).
- Feature‑flags у `config/flags.py`: `NEW_WHALE_CORE_ENABLED`, `STAGE3_MIN_SIGNAL_ENABLED`.
- Передача даних — виключно через `WhaleSnapshot`.

## 4. План міграції

### M0 (База)
- Створити `whale/core.py` (presence/bias/dominance).
- Переписати presence EMA → pure з `prev_presence`.
- Додати датакласи / TypedDict.
- `whale/engine.py` → тонкий делегатор (`enrich_ctx`).
- Юніт‑тести: порівняння старого vs нового.

### M1 (Адаптери)
- Реалізувати `whale/adapters/stage2.py`.
- Інтегрувати у `app/whale_worker.py` під флагом.
- Винести метрики → `whale/adapters/metrics.py`.

### M2 (Stage2 допрацювання)
- Замінити `normalize_and_cap_dvr` на `core.smooth_dvr`.
- Видалити `_DVR_EMA_STATE`, `_ACCUM_MONITOR_STATE`.
- Імпортувати гейти з ядра.
- Прибрати `_alias` поступово.

### M3 (Stage3 інтеграція)
- Реалізувати `stage3_min_signal.py`.
- Додати флаг `STAGE3_MIN_SIGNAL_ENABLED`.
- Канарейкові shadow‑тести.

### M4 (Чистка та документація)
- Вимкнути старі модулі / дублікати.
- Видалити legacy `_alias`.
- Оновити `ARCHITECTURE.md` (схема, контракти, приклади).
- Оновити `config/flags.py` (прибрати `WHALE_SCORING_V2_ENABLED`).

### M5 (Комунікація та підтримка)
- Узгодити з командою структуру.
- Маленькі PR (30–90 LOC) під фіче‑флагами.
- Smoke‑тести + метрики після кожного етапу.
- Моніторинг presence / точності.

## 5. Висновки

Реорганізація:
- Відділяє pure‑ядро від адаптерів.
- Прибирає дублювання та глобальний стан.
- Уніфікує контракти даних (`WhaleSnapshot`).
- Підвищує тестованість і передбачуваність.
- Дозволяє незалежний розвиток Stage1/2/3 поверх єдиного ядра.

