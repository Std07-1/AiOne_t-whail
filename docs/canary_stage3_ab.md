# Канарейка Stage3 (paper) + A/B • 28 днів

Мета: перевести прозорий PnL‑proxy у фактичний PnL під жорсткими ризик‑гейтами, знизити no_candidate і не підняти FP.

- Обсяг: Stage3 paper‑execution на 4 символах: BTCUSDT, ETHUSDT, TONUSDT, SNXUSDT.
- Сценарії: pullback_continuation, breakout_confirmation.
- Ризик/ліміти: RISK_PER_TRADE_R=0.25; MAX_TRADES_PER_SYMBOL=4/доба; MAX_TRADES_PER_DAY=12; стоп = invalidator або bias‑flip.
- A/B:
  - A: SCEN_CONTEXT_WEIGHTS_ENABLED=false; STATE_NAMESPACE=default; PROM_HTTP_PORT=9108.
  - B: SCEN_CONTEXT_WEIGHTS_ENABLED=true; ваги α=β=γ=0.10; STATE_NAMESPACE=ai_one_b; PROM_HTTP_PORT=9118.
- Щоденний ритуал (UTC): 09:00 прогін 60–90 хв (snap‑every=900) → 10:45 огляд snapshot/dashboard → 16:00 observation‑звіт.
- KPI тижня 1: ≥20 активацій; FP ≤15%; p95 latency ≤800мс; explain_coverage ≥60%; no_candidate_rate_B ≤ 0.8 × A.
- Acceptance (28 днів): hit‑rate@K=5 ≥0.55 і p75(|ret|) ≥0.4% ≥2 symbols; winrate ≥52%; PF ≥1.1; DD ≤3R; explain_coverage ≥70%; verify_explain стабільний; no_candidate_rate_B < A без росту presence_drop/htf_weak.
- Snapshot Forward: дефолт K=3,5,10; на тижні 2 додати K=20/30.
- BTC‑gate v2 (м’який): послаблення для мейджорів при htf_strength ≥0.12 (під прапором; без зміни контрактів).
- Контракти Stage1/2/3 не змінювати. Усі зміни — під фіче‑флагами у config/config.py.

## Команди (довідково)

- Канарейка A: `python -m tools.run_window --duration 5400 --set PROM_GAUGES_ENABLED=true --set SCEN_CONTEXT_WEIGHTS_ENABLED=false --log run_A.log`
- Канарейка B: `STATE_NAMESPACE=ai_one_b; python -m tools.run_window --duration 5400 --set PROM_GAUGES_ENABLED=true --set PROM_HTTP_PORT=9118 --set SCEN_CONTEXT_WEIGHTS_ENABLED=true --log run_B.log`
- Оркестратор A/B: `python -m tools.ab_orchestrator --ports 9108,9118 --snap-every 900 --duration 5400 --out ab_runs`
- Щоденні звіти: `python -m tools.scenario_quality_report --last-hours 24 --out reports/quality.csv`; `python -m tools.quality_snapshot --csv reports/quality.csv --metrics-dir reports --out reports/quality_snapshot.md`; `python -m tools.quality_dashboard --csv reports/quality.csv --out reports/quality_dashboard.md`
