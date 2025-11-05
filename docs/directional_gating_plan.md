# Directional gating & re-weighting: план, контракти, фіче‑флаги

Мета — зменшити хибні входи проти домінуючого напрямку та швидше реагувати на «rapids» без порушення контрактів і з можливістю повного відкату через фіче‑флаги.

## 1. Контракти й сумісність

- Stage1Signal: допускаються додаткові поля у `stats` (через константи з `config.config`, без хардкодів).
- Stage2Output: базові ключі незмінні; розширення тільки у:
  - `confidence_metrics`: `dir_score: float`, `directional_conflict: bool`, `rapid_move_flag: bool`.
  - `market_context.meta.directional`: довідкові `directional_volume_ratio`, `cumulative_delta`, `price_slope_atr` тощо.
- Stage3: `pre-open guard` — додатковий запобіжник перед відкриттям, без зміни API (під флагом).
- Redis/UI: сумісність назад — UI ігнорує невідомі поля; публікації йдуть у наявні канали/документи.

## 2. Stage1 — нові метрики у stats

- `directional_volume_ratio (DVR)`: `(sum down-vol / sum up-vol)` на вікнах `W_short` (≈3) і `W_mid` (≈5..10).
- `cumulative_delta (CD)`: за наявності тик‑даних; інакше апроks через signed volume per bar (менша вага).
- `price_slope_atr`: `(price_now − price_t_minus_W) / ATR_now`.
- `up_vol_z`, `down_vol_z`: `z-score` для напрямних обсягів.

Усі вікна/пороги — у `config`, з докстрінгами.

## 3. Stage2 — gating і re-weighting

- `Directional_conflict`:
  - `sign_check = sign(price_slope_atr)` при `|slope| > slope_thresh` (≈0.25 ATR).
  - Конфлікт, якщо `sign_check` суперечить buy/short‑сценарію і `DVR > 1.2` і `|CD| > 0.35`.
  - Дія:
    - `soft`: `composite_confidence *= gamma_conflict (≈0.6)`; `confidence_metrics.directional_conflict = True`; `gate_reasons += ['directional_conflict']`.
    - `strict`: downgrade `BUY → WAIT` або `BUY_IN_DIPS → WAIT_FOR_CONFIRMATION`.
- `Re-weighting (dir_score)`:
  - `dir_score ∈ [−1..1]` з вагами `w_slope, w_cd, w_dvr, w_side`.
  - `conf_buy/conf_sell = base_conf × (0.5 × (1 ± dir_score))`; вибір recommendation за максимумом.
- `Rapid_move hysteresis`:
  - Якщо `price_shock > shock_thresh` і `volume_z > vol_z_thresh` → `rapid_move_flag=True`.
  - Для BUY вимагати: `conf_buy ≥ rapid_override_conf` або HTF alignment; додати `gate_reason=rapid_move_hysteresis`.

Усе вище — під фіче‑флагами й окремими порогами у `config`.

## 4. Stage3 — pre‑open guard

- `refuse_long_on_recent_downtrend` (і симетрично для short):
  - Якщо `price_slope_atr_last_W < −slope_block_threshold` і `DVR > dv_block_thresh` → skip open; reason=`recent_downtrend_block`.
  - Під флагом `FEATURE_STAGE3_RECENT_DOWNTREND_BLOCK` (+ пер‑символьні overrides).

## 5. Телеметрія

- Лічильники:
  - `telemetry.stage2.directional_conflict.rate`
  - `telemetry.stage2.rapid_move.rate`
  - `telemetry.stage2.confidence_side_discrepancy.rate`
  - `telemetry.stage3.open_skips.recent_downtrend_block`
- Звіти для replay‑comparator:
  - Частка BUY при `CD < −0.4` та `slope < −0.25` → очікувано ~0.

## 6. Інтеграційні точки

- Stage1: `asset_monitoring.py` — обчислення вікон/метрик; `breakout_level_trigger.py` — доповнення `stats` після формування сигналу.
- Stage2: `processor.py` — вставка directional gating/re‑weighting перед фінальним `recommendation`; серіалізація нових полів у `confidence_metrics`/`market_context`.
- Stage3: `open_trades.py` / `trade_manager.py` — `pre-open guard`.

## 7. Параметри за замовчуванням (у config)

- `W_short=3`, `W_mid=5`
- `slope_thresh=0.25 (ATR units)`
- `dv_ratio_thresh=1.2`, `cum_delta_thresh=0.35`
- `gamma_conflict=0.6`
- `shock_thresh=1.5`, `vol_z_thresh=1.8`, `rapid_override_conf=0.92`
- `min_volume_for_directional` — символ‑залежний поріг
- Stage3 block: `slope_block_threshold=0.25`, `dv_block_thresh=1.1`

Все — через `config`; у коді без «магічних» чисел.

## 8. Тести (TDD‑light)

- Псевдо‑стрім:
  - Rapid downmove + високий DVR: переконатися, що `BUY_IN_DIPS` не видається або є `gate_reason=directional_conflict`.
  - Rapid upmove: симетричний кейс (валідний BUY не блокується).
  - Без тик‑даних: fallback на signed volume → стабільна поведінка.
  - Низький обсяг: DVR високий, але `abs(volume)` нижче порогу → gate OFF.
- Replay‑comparator:
  - Частка BUY при `CD < −0.4` і `slope < −0.25` < 0.5% (тривожний поріг).

Інваріанти: контракти незмінні; `feature flags OFF` → поведінка тотожна поточній.

## 9. Ризики і мітігації

- Немає тик‑дельти → зменшена вага `CD`, EWMA згладження; fallback на signed volume.
- Перекалібровка → спочатку `observe‑mode` (ON без впливу), далі `soft`, потім `strict` на canary.
- Недовідкриття у V‑розворотах → не вмикати hysteresis або вимагати HTF alignment.

## 10. Rollout / rollback

- Фази: Observe → Analysis → Soft gate → Strict (canary) → Global.
- Rollback: вимкнути відповідний фіче‑флаг; повернути пороги до дефолтів.

## 11. Пропозиція перших безпечних кроків (PLAN‑ONLY)

1) Додати фіче‑флаги у `config`:
   - `FEATURE_DIRECTIONAL_GATING`, `FEATURE_DIRECTIONAL_REWEIGHT`, `FEATURE_RAPID_MOVE_HYSTERESIS`, `FEATURE_STAGE3_RECENT_DOWNTREND_BLOCK` (усі False).
2) Додати Stage1‑метрики у `asset_monitoring.py` і включити їх у `stats` (вплив OFF).
3) Розширити `Stage2Output.confidence_metrics` потрібними полями (без зміни reco).
4) Додати 2 тести: happy‑path (без конфлікту) і directional_conflict у observe‑mode.

