# Whale Detection v2 — поточний стан, KPI та правила (2025‑10‑22)

Мета: покращити детекцію впливу великих гравців, зменшити шум від слабких сетапів і підвищити якість рішень Stage2/Stage3, не ламаючи контракти та зберігаючи контрольований латенсі‑бюджет.

## 1. Що вже працює (з логів)

- Strict‑фази: `pre/post_breakout`, `momentum_pullback`, `exhaustion` — активуються лише за жорстких умов; інакше `phase=None` + теги watch.
- Sanity‑корекції: системно гасять хибні `BULLISH_*` у невідповідних умовах (типово BREAKOUT→RANGE_BOUND, CONTROL→HIGH_VOLATILITY).
- Directional‑guard: при конфлікті напряму (`slope_atr`/`CD` проти сценарію) — демоут до WAIT; прозоре логування причин.
- Whale v2 (пакет `whale/`): обчислює `presence_score` (сила) і `bias` (напрям), використовує VWAP/TWAP/iceberg/зони. Медіанне presence стабільне (~0.6–0.7).
- RR‑gate: low‑RR переводить у WAIT_FOR_CONFIRMATION.
- FAST‑gate: пропускає дрібʼязок без побічних ефектів, latency стабільна.
- Proxy‑vol безпека: при `volz_source=proxy` блокуються ризикові фази (exhaustion), що знижує FP.

Інваріанти: контракти `Stage1Signal`/`Stage2Output` незмінні; усе нове — під `market_context.meta.*`.

## 2. Де ще слабко

- `vol_z` із proxy: без реального обсягу strict‑тригери часто не добирають умови → `phase=None/WAIT`.
- Пост‑події: `post_breakout` спрацьовує, але RR/vol‑гейти утримують від входу (правильно, але виглядає пасивно).
- Whale presence tail при proxy: іноді завищення — застосовано cap/EMA згладжування, потребує докрутки.
- Пороги глобальні: бракує калібрування per‑asset.
- Insight‑layer (AssetStateCard): готовий, але вимкнений у проді для поступової валідації.
- Бракує еталонних історичних контрольок — оцінка за replay/ручним аналізом; потрібні KPI.

## 3. KPI «готовності» (definition of done для строгої відповіді)

- Precision breakout/retest ≥ 0.7 на реплеї з реальним `vol_z`.
- Guard‑узгодженість: ≥95% кейсів із `directional_conflict` → WAIT/OBSERVE (без HOLD).
- Whale‑узгодженість: знак `bias` збігається з наступним 1–3×ATR зсувом ≥ 60%.

## 4. Точкові дотиски (мінімальні патчі, дизайн‑наміри)

- Phase strict: при `presence≥0.85` і `|VWAP-dev|≥0.05` — мʼякий overlay impulse‑gate (не фаза) → дозволяти post_breakout/retest лише якщо `vol≤1.0` і `DVR≥0.6` на ретесті.
- Ваги whale: підняти `w_dist` до 0.18 при `VWAP-dev≤−0.05` (sell‑trend) і `w_accum` до 0.18 при `VWAP-dev≥+0.05` (buy‑trend).
- Proxy‑volume: зберегти заборону для `exhaustion`, але дозволити теги `momentum_overbought/oversold` при `RSI>72/<28` і `slope_atr>1.5`.
- Insight‑Layer (канарейка): увімкнути на 2–3 активах → AssetStateCard (phase/tags, whale presence/bias, used_thresholds, next‑actions, invalidation‑лейни).
- Per‑asset профілі: зафіксувати `band_pct/vol_z/DVR/CD` для топ‑5; автокалібрація (EWMA) раз/добу.

Приклади фрагментів (до реалізації тільки через PR/фіче‑флаги):

- `phase_detector.py` — anti‑breakout guard за whale контекстом;
- `whale_scoring` — cap presence при proxy та EMA згладжування;
- `strategy_selector` — sanity гілка для RSI≪20 зі `slope_atr<0` → HIGH_VOLATILITY;
- `processor` — `meta.directional_guard_trip`, пріоритетний `reco_gate_reason`, лог‑маркери `[STRICT_*]`;
- `config` — `used_thresholds="strict"`, ваги whale.

## 5. Правила й інваріанти (щоб не «перекручувати налаштування»)

- Контракти незмінні: базові ключі `Stage2Output` не чіпаємо; розширення тільки в `market_context.meta.*` і `confidence_metrics`.
- Фіче‑флаги обовʼязково: жодних змішаних режимів у проді без явного флагу й доку; вимкнення = повний rollback.
- Конфіг лише у `config/config.py`: пороги, ваги, TTL — без «магічних» чисел у коді; Redis‑ключі тільки через константи (`ai_one:{domain}:{symbol}:{granularity}`).
- Логи: маркери `[STRICT_PHASE]`, `[STRICT_SANITY]`, `[STRICT_WHALE]`, `[STRICT_GUARD]`, `[STRICT_RR]`, `[STRICT_VOLZ]`; один рядок — ключові поля (`symbol ts phase scenario reasons presence bias rr gates`).
- Тести перед вмиканням: mini‑replay (10–15 хв SNX + 5 хв FART + 5 хв BAS) і KPI‑чек; у CI — smoke без мережі, локально — з реплеями.
- Minimal‑diff PR: ≤ ~30 LOC і 1–2 файли, коли можливо; якщо більше — дробити та флагувати вплив.
- Продуктивність: бюджет ≤ +20% `avg_wall_ms` у піках; важке — в cpu_pool; best‑effort I/O.

## 6. Готовність до «Кроку 2: Insight»

Умови перед увімкненням канарейки:

- Частка `BULLISH_* → HIGH_VOLATILITY/RANGE_BOUND` стабілізувалась (<5% хибних після дотисків).
- `presence` μ∈[0.60, 0.75], хвіст ≤ 0.90 при proxy.
- `directional_conflict` завжди корелює з WAIT/демоут (без HOLD).

Увімкнення:

- `INSIGHT_LAYER_ENABLED=True`, `PROCESSOR_INSIGHT_WIREUP=True`.
- В картці показувати: `used_thresholds="strict"`, `reco_gate_reason` (єдиний, пріоритетний), `directional_guard_trip`, `whale{presence,bias,dev}`, `tags` при `phase=None` (e.g., `momentum_overbought_watch`, `whipsaw_risk_watch`).

## 7. Контрольний чек‑лист комітів (рекап)

- [ ] phase_detector: `anti_breakout_whale_guard`, `volz_eff<1.2` у wide_band_or_low_vol.
- [ ] whale_scoring: proxy cap 0.85, EMA згладжування, лог `[STRICT_WHALE]`.
- [ ] strategy_selector: RSI<20 і slope<0 → HIGH_VOLATILITY sanity.
- [ ] processor: `meta.directional_guard_trip`, пріоритетний `reco_gate_reason`, `[STRICT_*]` маркери.
- [ ] config: `used_thresholds="strict"` і ваги для whale.

Примітка: це документ політик і KPI, а не автозміни коду. Будь‑яка імплементація — через окремі PR із фіче‑флагами та вимірюванням ефекту.
