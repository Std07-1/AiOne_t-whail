# Пам'ять Copilot для AiOne_t • v2025-10-29

## Місія

* Win-rate↑, PnL↑, FP↓. Latency ≤200 мс/бар.
* Контракти Stage1→Stage3 незмінні. Нові дані лише в `market_context.meta.*` і `confidence_metrics.*`.
* Спочатку інструментація, потім зміни порогів.

## Принципи

* Усе українською: код, логи, доки.
* Фіче-флаги для кожної зміни. Rollback = вимкнути флаг.
* Redis-ключі тільки через `config`, формат `ai_one:{domain}:{symbol}:{granularity}`.
* Мінімальний диф, без нових залежностей, без блокуючих I/O у гарячому шляху.

## Останні дії (2025-10-31 → 2025-11-01)

* HTF publisher: оновлення `htf_strength` у Prometheus (no-op, якщо вимкнено `PROM_GAUGES_ENABLED`).
* Whale publisher: відновлено провідку whale-метрик (Redis → локальний fallback з write‑through); `stats.whale = {presence,bias,vwap_dev,zones_summary,vol_regime}` + Prometheus `presence`.
* Канарейкове публішення сценарію: додано фіче‑флаг `SCENARIO_CANARY_PUBLISH_CANDIDATE` (False за замовч.) — дозволяє тимчасово публікувати кандидат у `market_context.meta.scenario_*` і `ai_one_scenario` без підтвердження гістерезисом.
* Prometheus `/metrics`: публікуємо `ai_one_scenario{symbol,scenario}=confidence` і `ai_one_phase{symbol,phase}=score` поряд із `htf_strength`, `presence`, `low_atr_override_*` (коли `PROM_GAUGES_ENABLED=True`).
* Проведено коротку канарейку (10–15 хв) із пом’якшеними SCEN_* у `config/config.py`. Результат: у `/metrics` присутні ненульові `ai_one_scenario` для BTCUSDT (~0.415), ICPUSDT (~0.59), TONUSDT/SNXUSDT (~0.31). `state_*` знімки без `scenario_*` (таймінг/каденс UI), пропозицій щодо порогів немає (немає домінантної причини REJECT >50%). Звіт: `docs/canary_report_2025-11-01.md`.
* SCENARIO_TRACE: рейт‑лімітовані логи кандидата/предикторів/рішення (1 раз на ~10с/символ) активні при `SCENARIO_TRACE_ENABLED=True`.

* Forward‑validation: додано інструмент `tools/scenario_quality_report.py`; згенеровано `reports/quality.csv` за останні 4 години. Результат на поточних логах: 0 активацій для `breakout_confirmation` та `pullback_continuation` (за цей період у логах відсутні [SCENARIO_TRACE]/публікації сценаріїв).
* Конфіг: додано фіче‑прапори `PROM_GAUGES_ENABLED=False` і `PROM_HTTP_PORT=9108` у `config/config.py` як джерело правди для метрик. Лінтери/тайпчек/тести — PASS.

## Останні дії (2025-11-03)

* D5 (A‑only, 3 години) — PASS: p95(avg_wall_ms)=535 мс (≤800), avg=421 мс, p99=636 мс; Explain coverage ≈0.98–1.00 для 4 символів; Forward‑K секції увімкнено (включно з K=20/30; N для довгих K ще мале). Підсумок у `reports/Finale_D5.md`.
* Стандартизація виходів: усі `--out` без каталогу автоматично у `reports/…` (tools: quality_snapshot, quality_dashboard, scenario_quality_report). Рун‑логи `--log` без каталогу автоматично у `logs/runs/…` (tools.run_window). Існуючі артефакти з кореня перенесено у `reports/`, run_*.log → `logs/runs/`.
* Дефолти закріплено: `SCEN_CONTEXT_WEIGHTS_ENABLED=False` як джерело правди у `config/config.py`. FSM sweep→retest→bias — без змін. Forward‑K=3/5/10/20/30 увімкнено у snapshot/dashboard.
* D6 запущено у фоні: 4h A‑only ран (`tools.run_window`, лог → `logs/runs/run_A_D6_4h.log`) + монітор 4h (`tools.monitor_observation --deep-state --symbols BTC,ETH,TON,SNX,SOL,TRX,XRP,LINK`, артефакти → `reports/`).

### Policy Engine v2 (T0) інтеграція

* Реалізовано `stage2/policy_engine_v2.py`: гейти для IMPULSE/ACCUM_BREAKOUT/MEAN_REVERT, сторони за `near_edge/slope_atr`, TP/SL per class, p_win (формула T0), фолбек `compute_5m_and_median20` (None → skip), `[SIGNAL_V2]` логи. Контракти незмінні: дані лише у `market_context.meta.signal_v2` через `asdict(dec)`.
* Метрики Prometheus: `ai_one_signal_pwin`, `ai_one_ramp_score`, `ai_one_absorption_score`, `ai_one_regime{symbol,regime}` (one-hot із зануленням попереднього); лічильники `ai_one_signal_ready_total{symbol,class}`, `ai_one_signal_emitted_total{symbol,class}`, `ai_one_signal_tp_hit_total{symbol,class}`, `ai_one_signal_fp_total{symbol,class}`.
* Кулдаун: у `process_asset_batch.py` додано `_POLICY_V2_LAST_SIGNAL_TS[(symbol,class)]`; `inc_signal_ready` — завжди, `inc_signal` — тільки поза `COOLDOWN_S`.
* QA: `tools/signals_truth` — реальні TP/FP за 1m‑серіями (проксі time-align), CSV `reports/signals_qa.csv`, опція `--emit-prom=true` інкрементує TP/FP лічильники.
* VS Code tasks: `run-policy-T0-smoke` (1h), `signals-qa`, `snapshot-now`.

Acceptance T0: у `reports/signals_qa.csv` має бути ≥1 TP/FP; у `/metrics` видно зростання `ai_one_signal_*_total`; під час емісій присутній `market_context.meta.signal_v2`.


## Канарейка Stage3 A/B (2025‑11‑02 → 2025‑11‑30)

* Обсяг: paper‑execution Stage3 з A/B на 4 символах: `BTCUSDT, ETHUSDT, TONUSDT, SNXUSDT`. Сценарії: `pullback_continuation`, `breakout_confirmation`.
* Ризик/ліміти: `RISK_PER_TRADE_R=0.25`, `MAX_TRADES_PER_SYMBOL=4/доба`, `MAX_TRADES_PER_DAY=12`; стоп — `invalidator` або `bias` проти позиції.
* A/B:
  * A: `SCEN_CONTEXT_WEIGHTS_ENABLED=false`, `STATE_NAMESPACE=default`, `PROM_HTTP_PORT=9108`.
  * B: `SCEN_CONTEXT_WEIGHTS_ENABLED=true`, ваги `α=β=γ=0.10`, `STATE_NAMESPACE=ai_one_b`, `PROM_HTTP_PORT=9118`.
* Ритуал (UTC): 09:00 прогін 60–90 хв (snap-every=900), 10:45 огляд `quality_snapshot.md/quality_dashboard.md`, 16:00 оновлення `docs/observation_YYYY-MM-DD.md` і розсилка.
* KPI тижня 1: ≥20 активацій сумарно; FP ≤15%; latency p95 ≤800мс; explain_coverage ≥60%; `no_candidate_rate_B ≤ 0.8 × A`.
* Критерії прийняття (28 днів): hit‑rate@K=5 ≥0.55 і p75(|ret|) ≥0.4% на ≥2 символах; winrate ≥52%; PF ≥1.1; drawdown ≤3R; explain_coverage ≥70%; `verify_explain` стабільний; `no_candidate_rate_B < A` без росту `presence_drop/htf_weak`.
* Snapshot Forward: дефолт K=3,5,10; на тижні 2 додати K=20/30 (довші хвости).
* BTC‑gate v2: м’який штраф для мейджорів при `htf_strength ≥ 0.12` (під прапором, без зміни контрактів; вплив як у v1 для альтів у flat).
* Довідкові команди: `tools.run_window` (A/B), `tools.ab_orchestrator` (A/B + авто‑звіти), `tools.scenario_quality_report`, `tools.quality_snapshot`, `tools.quality_dashboard`.

## Активні механіки (обов’язкові до врахування)

* **Low-ATR Spike Override**: Stage1 озброює `low_atr_override` за умовою
  `band_expand≥0.013 ∧ spike_ratio≥0.60 ∧ |vol_z|≥2.5 ∧ DVR≥1.2`, `TTL=bars_ttl`.
  Stage2 читає override: Redis → `stats.overrides` → none. Override лише знімає `low_atr_guard`.
  Флаги: `STRICT_LOW_ATR_OVERRIDE_ON_SPIKE`, `LOW_ATR_SPIKE_OVERRIDE.{…}`
* **ACCUM_MONITOR постпроцесор**: будь-який `PhaseResult(None, …)` проходить `_apply_phase_postprocessors`;
  якщо `atr_ratio<1.0` і override неактивний → `phase="ACCUM_MONITOR"`, `score=0`, reason `low_atr_guard`.
* **HTF “сіра” зона + stale**:
  `htf_strength<0.10` або `htf_stale_ms>stage1_htf_stale_ms` → `htf_ok=False`.
  `0.10≤htf_strength<0.20` → штраф `score -= 0.20` (не нижче 0).
  Флаг: `STRICT_HTF_GRAY_GATE_ENABLED`, пороги `HTF_GATES`.
* **Flat-policy**:
  `dist_zones=0 ∧ liquidity_pools=0` →
  `presence ≤ PRESENCE_CAPS.accum_only_cap` (в `whale_telemetry_scoring.py`) і
  `risk_from_zones ≤ STAGE2_PRESENCE_PROMOTION_GUARDS.cap_without_confirm`
  (в `supply_demand_zones.py` і `whale_detection.py`).
  Orderbook missing не штрафується.
  Флаги: `STRICT_ACCUM_CAPS_ENABLED`, `STRICT_ZONE_RISK_CLAMP_ENABLED`.
* **Санітизація чисел**: `sanitize_ohlcv_numeric` перед обчисленнями і перед записом CSV/Parquet.
* **Опційно**: Prometheus-гейджі (`PROM_GAUGES_ENABLED`, порт `PROM_HTTP_PORT`):
  `low_atr_override_active`, `low_atr_override_ttl`, `htf_strength`, `presence`,
  `ai_one_scenario{symbol,scenario}`, `ai_one_phase{symbol,phase}`.
  CLI-прапори для A/B: `--enable/disable-accum-caps|htf-gray|low-atr-spike-override|--prometheus`.

* **BTC gate v2 (hint + v1 penalty)**:
  Детекція режиму BTC: `flat` | `trend_up` | `trend_down`.
  Правила v2: якщо `htf_strength≥SCEN_HTF_MIN` і `phase∈{"momentum","drift_trend"}` —
  тоді `trend_up` при `cd≥0` або `slope_atr≥0`; `trend_down` при `cd<0` або `slope_atr<0`; інакше `flat`.
  Вплив на conf не змінено порівняно з v1: м’який штраф −20% лише у `flat` при `btc_htf_strength<SCEN_HTF_MIN` (для альтів).
  У TRACE додається `btc_regime` (v2‑тег) та, за потреби, `penalty=0.2`. Поля у `market_context.meta.*`: `btc_regime`, `btc_htf_strength`. Прапор: `SCEN_BTC_SOFT_GATE=True`.

## Прапори сценаріїв (поточні)

* `SCENARIO_TRACE_ENABLED=True`
* `STRICT_SCENARIO_HYSTERESIS_ENABLED=True`
* `SCENARIO_CANARY_PUBLISH_CANDIDATE=False` (на час канарейки тимчасово вмикаємо True)
* Усі пороги сценаріїв (`SCEN_*`) беруться лише з `config/config.py` (заборонено через env)

## Фокусні напрями

1. Stage2 консервативність: baseline ≥50 `alerts_quality`; потім Probe→Promote.
2. Stage1 якість: тригери, microstructure_score, авто-тюн порогів.
3. Stage3 ризик: Probe→Promote, динамічний SL, regression harness.
4. Observability: latency, JSONL rotation, Redis backlog, Prom/OTel.
5. Аналітика: AOF, Survival, Follow-through, PnL симулятор.
6. Інновації: ML-фільтр, regime detection, Optuna (після стабільного baseline).

## Roadmap

* Етап 0: збір даних (alerts_quality, stage3_skips, stage2_decisions) — активний.
* Етап 1: Telemetry розширення + microstructure + ротація JSONL.
* Етап 2: Dashboard якості, PnL симулятор, Stage2 grid backtest.
* Етап 3: Trigger weighting, microstructure_score, Stage3 Probe→Promote.
* Етап 4: Regression harness, Redis resilience, observability stack, cache warmup/Parallel Stage2.
* Етап 5: ML-фільтр, regime detection, Optuna.

## Поточний TODO (P0→P2)

### P0 — стабілізація ядра

* [ ] Перевірити, що **обидва** клампи у flat активні (whale_detection + supply_demand_zones).
* [ ] Stage2 читає override з Redis пріоритетно. Ключ: `ai_one:overrides:low_atr_spike:{symbol}`.
* [ ] Санітизація numeric перед усіма `.to_csv/.to_parquet`. Усунути pandas warnings.
* [ ] Тести `pytest -q`: presence_cap_flat, risk_clamp_flat, accum_monitor_fallback, htf_gray_stale, override_ttl.

### P1 — спостережність і A/B
* [x] Опційні Prometheus-гейджі (лінива ініціалізація, no-op без залежності).
* [x] Policy v2: метрики pwin/ramp/absorption/regime, лічильники ready/emitted/tp/fp, кулдаун (per symbol×class).
* [x] tools/signals_truth: TP/FP на 1m, CSV, пром-емісія.
* [ ] CLI-прапори для фіче-флагів у bench/runner.
* [ ] README: override, ключ Redis, пороги, умови промоції.

### P2 — епізоди та сценарна валідація

* [ ] `generate_and_replay_episodes.py`: маніфест із **двома різними** сценаріями
  (`breakout_confirmation`, `pullback_continuation`), `--strict-datetime-match`.
* [ ] Артефакти: `episodes_manifest.jsonl`, `replay_summary.csv`, `phase_logs.jsonl`, `kpi.json`.
* [ ] Acceptance: по кожному епізоду є `scenario_detected` і `confidence>0`; у флєті `ACCUM_MONITOR`.
* [ ] `quality.csv` має додаткову колонку `btc_gate_effect` (1/0) — ознака застосування soft penalty у вікні для сценарію.

Доповнення (стан на 2025‑11‑01):

* [x] Forward‑validation (останні 4 години): `tools/scenario_quality_report.py` → `reports/quality.csv` (проксі‑метрики активацій сценаріїв).
* [ ] Acceptance епізодів: тулчейн для епізодів/реплею відсутній у цьому репо — потрібна мінімальна імплементація (лайт) або підключення зовнішнього інструментарію перед запуском acceptance.

## Ритуал перевірки

* Перед зміною: звірити з цією пам’яттю.
* Після злиття: оновити TODO і статуси.
* Щотижня: ревізія KPI і latency.

## Політики (нагадування)

* Без змін контрактів Stage1/2/3. Нові поля — тільки `market_context.meta.*`, `confidence_metrics.*`.
* Фіче-флаг на все, що впливає на рішення.
* Логи-маркери: `[STRICT_PHASE] [STRICT_SANITY] [STRICT_WHALE] [STRICT_GUARD] [STRICT_RR] [STRICT_VOLZ] [LOW_ATR_OVERRIDE] [HTF]`.
* Формат лог-рядка: `symbol ts phase scenario reasons presence bias rr gates`.

## Додаткові елементи якості сигналів

* Alert lifecycle + `alerts_quality.jsonl` (PAE, MFE/MAE, Survival, Follow-through, AOF).
* Microstructure: `wick_ratio`, `body_fraction`, `post_bar_retrace`, `micro_time_clustering`;
  агрегувати в `microstructure_score` і використовувати як **hint/penalty**, не як hard-block.

## Backtest Stage2 (сітка параметрів)

* `low_gate ∈ {0.004,0.006,0.008}`, `high_gate ∈ {0.012,0.015,0.018}`, `min_confidence ∈ {0.68,0.72,0.75,0.78}`.
* Метрики: Pass Rate, Quality (MFE/ATR), Survival, Composite Score.
* Вибір оптимуму без ручного overtune.

## Rollback

* Вимкнути відповідний флаг. Без міграцій. Контракти незмінні.

---

### Що прибрано з попередньої версії як неактуальне

* Будь-які натяки на зміну підписів Stage-функцій.
* Жорсткі штрафи за відсутній orderbook. Лишається “no-add, no-penalty”.
* Розрізнені ручні обхідні `ACCUM_MONITOR` — тепер тільки централізований постпроцесор.

