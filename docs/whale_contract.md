# Whale Telemetry Contract (Stage2 SoT)

> Мета: зафіксувати існуючий контракт між Stage2 → whale core (Redis snapshot → `stats.whale`), а також Stage2 → UI (`ctx.meta.whale` та дзеркало у `meta.insight_overlay`). Цей документ не змінює код, а лише формалізує поточні поля для майбутнього `whale/core.py`.

## 1. Stage2 → whale core (`stats.whale.*`)

| Source | Field path | Type | Required? | Consumers | Transformations | Flags / Config | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Redis snapshot (`presence`) | `stats.whale.presence` | float [0,1] | Y (default 0) | Stage2, Stage3 min-signal, policy gates | `_clip()` до [0,1]; carry-forward зберігає значення | `WHALE_CARRY_FORWARD_TTL_S`, `WHALE_MISSING_STALE_MODEL_ENABLED` | Raw presence з Redis; 0.0 якщо немає даних. |
| Redis snapshot (`bias`) | `stats.whale.bias` | float [-1,1] | Y (default 0) | Stage2 risk clamps, Stage3 | `_clip()` до [-1,1]; carry-forward | same as above | Positive → buying, negative → selling. |
| Redis snapshot (`vwap_dev`) | `stats.whale.vwap_dev` | float | Y (default 0) | Stage2 fallback dominance, Stage3 | `_clip()` до ±1e9 | same | Відхилення від VWAP; 0 при помилках. |
| Redis snapshot (`dominance`) | `stats.whale.dominance` | dict {buy: bool, sell: bool} | N | UI, Stage3 heuristics | Validate bools; fallback із ATR/зон | залежить від `price_slope_atr`, thresholds зон | Якщо Redis відсутній/некоректний, Stage2 створює евристику: min VWAP dev, slope, zones cnt. |
| Redis snapshot (`ts`) | `stats.whale.ts` | int (ms) | Y | Monitoring, soft-stale | `_to_seconds` нормалізує; age обчислюється | `_SNAPSHOT_TTL_S`, `WHALE_SOFT_STALE_TTL_S` | Зберігається і в секундах (`ts_s`). |
| Derived | `stats.whale.age_ms`, `age_s` | int | Y | QA, telemetry | `now_ts - ts_s` | same | 0 якщо немає ts. |
| Redis snapshot (`missing`) | `stats.whale.missing` | bool | Y | Stage2 tags, monitoring | Forced False якщо модель вимкнена | `WHALE_MISSING_STALE_MODEL_ENABLED` | Carry-forward скидає прапори. |
| Redis snapshot (`stale`) | `stats.whale.stale` | bool | Y | Stage2 tags, logging | same | same + `WHALE_SOFT_STALE_TTL_S` | Використовується для `tags` і soft-stale. |
| Redis snapshot (`reasons`) | `stats.whale.reasons[]` | list[str] | N | QA/logs | dedup; додається reason `carry_forward` | carry-forward TTL | Порожній список якщо немає. |
| Redis snapshot (`zones`) | `stats.whale.zones_summary.accum_cnt/dist_cnt` | ints ≥0 | Y (default 0) | `update_accum_monitor`, dominance fallback, Stage3 | Якщо Redis не дає — беремо зі state carry-forward | state manager: `stats.whale_last` | Кількість акум/дист зон. |
| Redis snapshot (`explain.dev_level`) | `stats.whale.dev_level` | str or None | N | UI, Stage3 heuristics | fallback на попередній снепшот | state manager | Немає строгого словника; Stage3 трактує «flat/imbalance». |
| Stage2 derived (ATR %) | `stats.whale.vol_regime` | enum {hyper, high, normal, unknown} | Y | Prom dashboards, policy toggles | ATR/price *100 → bucket | thresholds fixed у Stage2 | Thres: ≥3.5 hyper, ≥2 high. |
| Stage2 tagging | `stats.tags[]` | list[str] | N | Observability, QA | Append `whale_stale`, `whale_missing`, `whale_soft_stale` | `WHALE_SOFT_STALE_TTL_S` | Теги додаються лише при true-прапорах. |
| Stage2 postprocessor | `stats.postprocessors.accum_monitor` | dict {accum_cnt, dist_cnt, ts} | Y | Stage2 flat-policy, Stage3 clamp | Output `update_accum_monitor()` | `STRICT_ACCUM_CAPS_ENABLED`, `STRICT_ZONE_RISK_CLAMP_ENABLED` | Дзеркалиться у `market_context.meta.postprocessors`. |
| Carry-forward marker | `stats.whale.reasons[]` contains `carry_forward` | N | QA | Added when fallback uses cached snapshot | `WHALE_CARRY_FORWARD_TTL_S` | Ідентифікація кешованого прогнозу. |
| State snapshot | `state.stats.whale_last` (not part of ctx) | - | - | Stage2 carry-forward | write-through копія | same TTL | Не експортується назовні, але важливо для контракту. |

### Семантика Required

- Required (Y) означає, що поле має існувати в `stats.whale` навіть якщо значення дефолтне (0, `False`, `unknown`).
- Optional (N) може бути відсутнє або `None`; Stage3/UI повинні мати захист.

### Stage3 мінімальний сигнал

Stage3 сьогодні очікує принаймні: `stats.whale.presence`, `bias`, `dominance`, `vol_regime`, `zones_summary`, `missing/stale`. Будь-яка еволюція цих полів — під фіче-флагом і відображена в цій таблиці перед релізом.

## 2. Stage2 → UI (`ctx.meta.whale`, `meta.insight_overlay`)

| Source | Field path | Type | Required? | Consumers | Transformations | Flags / Config | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `stats.vwap`, `stats.current_price` | `ctx.meta.whale.vwap_deviation` | float | Y (default None) | UI, overlay, WhaleTelemetryScoring | `(price - vwap) / vwap` if vwap>0 | - | Розраховується тут; не дублюємо `stats.whale.vwap_dev`. |
| `WhaleTelemetryScoring.presence_score_from` | `ctx.meta.whale.presence_score` | float [0,1] | Y (default None) | UI, strategy selector | EMA згладжування (optional) | `STAGE2_WHALE_TELEMETRY.ema.enabled`, `.alpha` | `_PRESENCE_EMA_STATE` локальний; `presence_score_raw` зберігає сирий інпут. |
| same as above | `ctx.meta.whale.presence_score_raw` | float | N | QA, Prometheus | лише коли EMA ввімкнено | same | Відсутнє, якщо EMA вимкнено. |
| `WhaleTelemetryScoring.whale_bias_from` | `ctx.meta.whale.whale_bias` | float [-1,1] | Y (default None) | UI, strategy selector | прямий запис | - | Дзеркалиться в overlay. |
| Derived | `ctx.meta.whale.dominance` | dict {buy/sell} | N | UI, scenario rules | з `WhaleTelemetryScoring.dominance_flags` | depends on directional metrics, traps | Якщо `WhaleTelemetryScoring` недоступна — поле відсутнє. |
| Derived | `ctx.meta.insight_overlay.whale_presence` | float | N | UI overlay | дзеркало presence_score | same flag | Лише для візуалізації. |
| Derived | `ctx.meta.insight_overlay.whale_bias` | float | N | UI overlay | дзеркало whale_bias | - | - |
| Input | `ctx.directional.price_slope_atr` | float | Y для dominance | WhaleTelemetryScoring | `safe_float` | - | Нуль/None → dominance fallback False. |
| Input | `ctx.directional.directional_volume_ratio` | float | N | same | `safe_float` | - | 0 при відсутності. |
| Input | `ctx.meta.insight_overlay.trap` | any | N | dominance flags | пряме використання | - | Використовується лише якщо overlay — dict. |

### Семантика UI контракту

- `whale/engine.py` — єдине місце, що змінює `ctx.meta.whale.*` та `meta.insight_overlay.whale_*`.
- Якщо `WhaleTelemetryScoring` недоступний, всі поля лишаються `None`/відсутні — це grace-фолбек; зберегти при рефакторингу.

## 3. Acceptance цього етапу

- Додано документ `docs/whale_contract.md`; код Stage1/2/3 не змінювався.
- Таблиця покриває `stats.whale.*`, `ctx.meta.whale.*`, `meta.insight_overlay.whale_*`, а також контекст Stage3 мінімального сигналу.
- На основі цієї таблиці наступним кроком буде проектування `WhaleInput` / `WhaleTelemetry` та `whale/core.py` під контракт.

## 4. WhaleInput (вхідний контракт для `whale/core.py`)

WhaleInput — логічний контейнер, який Stage2 формує перед викликом нового ядра. Структура не є Python‑класом, але має чіткі групи полів:

```
WhaleInput:
    whale_snapshot: { ... }
    stage2_derived: { ... }
    directional: { ... }
    time_context: { ... }
```

### whale_snapshot (джерело: Redis snapshot + carry-forward)

- `presence: float [0,1]` — Required; raw із Redis, переноситься carry-forward у межах `WHALE_CARRY_FORWARD_TTL_S`.
- `bias: float [-1,1]` — Required; позитив → buy, негатив → sell.
- `vwap_dev: float` — Required; відхилення від VWAP; 0 за відсутності даних.
- `dominance: dict{buy: bool, sell: bool}` — Optional; або з Redis, або fallback Stage2 (використовує `price_slope_atr`, `zones_summary`).
- `ts_ms: int`, `ts_s: int`, `age_ms: int`, `age_s: int` — Required; нормалізовані часові мітки; `age_*` рахується від `now_ts`.
- `missing: bool`, `stale: bool` — Required; якщо модель вимкнена прапором — примусово `False`.
- `reasons: list[str]` — Optional; включає `carry_forward`, якщо використаний кеш.
- `zones_summary: {accum_cnt: int, dist_cnt: int}` — Required; з Redis або кешу `stats.whale_last`.
- `dev_level: str | None` — Optional; з `explain.dev_level` чи останнього снепшоту.
- `vol_regime: enum{'hyper','high','normal','unknown'}` — Required; Stage2 рахує за ATR%.
- `carry_forward_ttl_s: float` — Optional; прозорість щодо походження snapshot’а.

### stage2_derived (джерело: локальні постпроцесори)

- `accum_monitor: {accum_cnt: int, dist_cnt: int, ts: int}` — Required; вихід `update_accum_monitor`, синхронізований зі stats/postprocessors.
- `tags: list[str]` — Optional; `whale_missing`, `whale_stale`, `whale_soft_stale`.
- `whale_last_snapshot: dict | None` — Optional; write-through копія для усвідомлення carry-forward.
- `vol_regime_thresholds: dict` — Optional; значення з конфіга (для майбутніх clamp’ів).

### directional (джерело: `ctx.directional`, `meta.insight_overlay`)

- `price_slope_atr: float` — Required для dominance fallback; `safe_float`/0.0 при відсутності.
- `directional_volume_ratio: float` — Optional; 0.0 якщо немає.
- `trap_signal: dict | None` — Optional; з `meta.insight_overlay.trap`, впливає на dominance.
- `iceberg_orders: bool | None`, `twap_accumulation: bool | None` — Optional; приймаємо true у майбутньому.

### time_context

- `now_ts: float` — Required; глобальний час Stage2 (секунди).
- `snapshot_age_budget_ms: int` — Optional; поріг прийнятності снепшота.
- `htf_state: dict | None` — Optional; для майбутніх правил (наприклад, `btc_regime`).

Усі поля WhaleInput мають лишатися сумісними з розділом 1; core не додає нових джерел, лише споживає нормалізований SoT.

## 5. WhaleTelemetry (вихідний контракт `whale/core.py`)

WhaleTelemetry — результат чистої функції ядра. Мета — одночасно оновити `stats.whale.*` (для Stage3/політик) і `ctx.meta.whale.*`/`insight_overlay` (для UI).

### scores

- `presence_score: float [0,1]` — Required; може бути згладжений EMA (керується `STAGE2_WHALE_TELEMETRY.ema`).
- `presence_score_raw: float` — Optional; зберігається, якщо EMA активний.
- `whale_bias: float [-1,1]` — Required; прямий вихід телеметрії, без додаткових clamp’ів.
- `vwap_deviation: float` — Required; обчислюється за `stats.vwap`/`current_price`, не дублює `stats.whale.vwap_dev`.
- `dominance: dict{buy: bool, sell: bool}` — Optional; через `dominance_flags`, fallback → False при браку directional сигналів.

### state_flags

- `missing: bool`, `stale: bool` — Required; ядро не змінює семантику, лише передає значення зі snapshot’а.
- `soft_stale: bool` — Optional; true, якщо `age_s <= WHALE_SOFT_STALE_TTL_S`.
- `tags: list[str]` — Optional; `whale_missing`, `whale_stale`, `whale_soft_stale`.
- `reasons: list[str]` — Optional; включає `carry_forward`, `ema_applied` тощо.

### context

- `vol_regime: enum` — Required; копіюється з Entry (Stage3 потребує ATR-режиму).
- `zones_summary: {accum_cnt: int, dist_cnt: int}` — Required; для flat-policy та dominance fallback.
- `dev_level: str | None` — Optional; з Redis/prev snapshot.
- `age_ms: int`, `age_s: int` — Required; для моніторингу/soft-stale.
- `postprocessors: {accum_monitor: {...}}` — Required; Stage3 clamps очікують ці лічильники.
- `carry_forward: bool` — Optional; позначає дані зі state cache.

### ui_projection

- `meta_whale: {presence_score, presence_score_raw, whale_bias, vwap_deviation, dominance}` — напряму в `ctx.meta.whale`.
- `insight_overlay: {whale_presence, whale_bias}` — дзеркало для UI; синхронізоване з `meta_whale`.

Примітка щодо згладжувань: єдине допустиме — EMA над `presence_score` (під прапором). Поля мінімального сигналу для Stage3 — Required у WhaleTelemetry.

## 6. План інтеграції `whale/core.py`

- Крок 1: Розгорнути `whale/core.py`, що приймає `WhaleInput` і повертає `WhaleTelemetry`, не змінюючи зовнішніх контрактів Stage2/Stage3. Побудувати адаптер у Stage2, який формує `WhaleInput` з наявного `stats.whale` snapshot’а.
- Крок 2: Поступово винести обчислення з `whale/engine.py` та `app/whale_worker.py` до нового ядра, залишивши старі модулі тонкими обгортками, що викликають core і оновлюють `ctx.meta.whale` / `insight_overlay`. Жодна семантика полів у `stats.whale` чи `ctx.meta.whale` не змінюється без фіче-флагів.
- Крок 3: Підключити Stage3 мінімальний сигнал до `WhaleTelemetry` під фіче-флаг і канарейкові псевдостріми (`tools.bench_pseudostream`). Переконатися, що Required-поля для Stage3 (див. розділ 1) надходять із core без додаткових адаптерів.
- Загальні вимоги: будь-які кодові зміни виконуються лише після затвердження цього документа, під керуванням конфіг-прапорів і в малих PR. Rollback — вимкненням фіча-флагу.

## 7. Нотатки для майбутнього PR (PLAN-ONLY)

- Додати `whale/core.py`: чисте ядро з `WhaleInput`/`WhaleTelemetry`; нормалізує snapshot до канонічного `stats.whale`, рулить dominance fallback, генерує теги та формує UI-проєкцію з єдиного EMA-стану.
- Оновити `whale_stage2.py`: будувати `WhaleInput`, викликати `WhaleCore.compute`, розносити результат у `stats.whale`, `market_context.meta.whale`, `insight_overlay`; carry-forward і стейт-менеджер використовують новий payload.
- Спрощення `engine.py`: тонкий адаптер до `WhaleCore`, без дублювання скорингової логіки; `__init__.py` експортує `WhaleCore`.
- Додати юніт `tests/test_whale_core.py`: фіксує базову поведінку (clamp, теги, presence_score, dominance fallback). Псевдострім smoke — окремо.
- Критерії проходження: `pytest -q` PASS; без змін у Stage-контрактах; вимикається флагом без залишкових side-effects.

- Зроблено ( 2025.11.13 )

## 8. Stage3 Whale View (мінімальний сигнал)

- Мета: підготувати Stage3 до роботи з `WhaleTelemetry`, не змінюючи бізнес-логіку трейд-менеджера.
- Дія: створити утиліту `stage3.whale_min_signal_view.whale_min_signal_view(stats)`, яка повертає нормалізований зріз `stats.whale` з дефолтами для Stage3 (`presence`, `bias`, `vwap_dev`, `dominance`, `vol_regime`, `zones_summary`, `missing`, `stale`, `age_s`, `tags`, `phase_reason`).
- Вимоги: жодних нових порогів або правил — лише безпечне читання канонічного payload’у; дефолти відповідають розділу 1.
- Наступний крок: адаптувати Stage3 мінімальний сигнал до поверненого словника під фіче-флагом; після тестів — поетапно переносити решту Stage3 логіки на `WhaleTelemetry`.

## Stage3 Whale MinSignal v1 (дизайн)

- **Вхідні поля** (взяти з `WhaleTelemetry` / `whale_min_signal_view`, дефолти як у розділі 1): `presence`, `bias`, `vwap_dev`, `dominance.buy`, `dominance.sell`, `vol_regime`, `zones_summary.accum_cnt`, `zones_summary.dist_cnt`, `missing`, `stale`, `age_s`, `tags`, `phase_reason`. `phase_reason` береться з каскаду `_extract_phase_reason`: спочатку `stats.phase_debug.reason`, далі `stats.phase_state.last_reason`, і лише потім `market_context.meta.phase_state_hint.reason`. Таким чином навіть carry-forward з PhaseState потрапляє в мін-сигнал без зміни контрактів Stage1/Stage2.
- **Hard-deny гейти**: одразу вимикати сигнал, якщо `missing=True`, `stale=True` або `age_s > whale_max_age_sec` (порог підтягується з конфіга). У такому стані `direction="unknown"`, `profile="none"`, додаємо відповідні причини.
- **Профілі присутності/упередженості**:
    - `strong`: обов’язкові високі `presence` і `|bias|` (точні числа задаємо у коді; upper-поріг > soft). Це кандидати для реальних входів у майбутньому, коли Stage3 навчиться довіряти цьому сигналу.
    - `soft`: нижчі пороги для `presence` та `|bias|`; профіль рекомендаційний/observe — публікуємо як попередження, але Stage3 додатково перевіряє напрямок і довіру.
    - `explain_only`: суто explain/UI статус. Активується, якщо `phase_reason ∈ {volz_too_low}` або `phase_reason == "presence_cap_no_bias_htf"` із `confidence < 0.5`. Такий сигнал не впливає на Stage3 рішення — лише зберігається в снапшоті/телеметрії для QA.
    - Менш ніж soft → `profile="none"`, `enabled=False`.
- **Напрямок**: визначається тільки домінансом — `dominance.buy=True` → `direction="long"`, `dominance.sell=True` → `direction="short"`. Якщо жоден прапор не активний, фіксуємо `direction="unknown"` навіть за високих presence/bias.
- **Бонуси/штрафи**: допускається м’яке коригування `confidence`/`reasons` за `vol_regime` (наприклад, `hyper` → штраф, `normal` → базова вага) та співвідношенням `zones_summary.accum_cnt/dist_cnt` (накопичення підтримує long, надлишок dist → обережність або short). Жодних нових капів без прапорів Stage2 flat-policy.
- **Forward-артефакти**: поточна статистика `forward_*` — `whales_seen=116`, `alerts_seen=4`, `alerts_matched=0`. Цього горизонту замало для якісного тюнінгу, але дизайн гейтів треба зафіксувати вже зараз, щоб збирати додаткові дані на довших вікнах.

## Stage3 Whale Signal Telemetry (market_context.meta)

- Під прапором `WHALE_MINSIGNAL_V1_ENABLED` пайплайн записує у `market_context.meta.whale_signal_v1` поточний стан мін-сигналу. Контракти Stage1/Stage2 не змінюються: дані лише для телеметрії та аналізу.
- Payload містить: `enabled`, `direction`, `profile`, `confidence`, `reasons`, а також дзеркало вхідного зрізу (`presence`, `bias`, `vwap_dev`, `vol_regime`, `age_s`, `missing`, `stale`, `dominance`, `zones_summary`).
- Ті самі дані логуються як event `whale_signal_v1` (`stage1_events.jsonl`) і в Prometheus-метрики `ai_one_whale_signal_v1_*`, що дозволяє будувати forward-звіти без змін Stage3 трейд-менеджера.
