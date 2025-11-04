# D6 — Стрес‑прогін A‑only (6–8 активів, 3–4 години)

Мета: p95(avg_wall_ms) ≤ 800 мс при збільшенні кількості активів; зібрати снапшоти стану, пояснень та forward‑оцінки.

## Запуск (фон)

```powershell
# 4 години A‑only; лог автоматично буде у logs/runs/run_A_D6_4h.log
python -m tools.run_window --duration 14400 `
  --set STATE_NAMESPACE=ai_one `
  --set PROM_GAUGES_ENABLED=true --set PROM_HTTP_PORT=9108 `
  --set SCEN_CONTEXT_WEIGHTS_ENABLED=false `
  --log run_A_D6_4h.log

# Моніторинг 4 години з deep‑state (артефакти у reports/)
python -m tools.monitor_observation `
  --interval-min 15 `
  --duration-min 240 `
  --deep-state `
  --symbols "BTCUSDT,ETHUSDT,TONUSDT,SNXUSDT,SOLUSDT,TRXUSDT,XRPUSDT,LINKUSDT" `
  --out-dir reports
```

## Під час ранну — контрольні метрики

- p95(avg_wall_ms) із run‑логу (див. one‑liner у D5 прикладі)
- Prometheus `/metrics`: `ai_one_phase`, `ai_one_bias_state`, `ai_one_scn_reject_total`, `ai_one_explain_lines_total`
- Системні: CPU ≤ 70%, MEM ≤ 80%

## Пост‑обробка (3 кроки)

```powershell
# 1) Якість за 4 години (лог з logs/runs/)
python -m tools.scenario_quality_report --last-hours 4 `
  --logs logs/runs/run_A_D6_4h.log `
  --out quality.csv

# 2) Дашборд з forward‑K (3/5/10/20/30)
python -m tools.quality_dashboard --csv quality.csv `
  --forward-k 3,5,10,20,30 `
  --out quality_dashboard.md

# 3) Снапшот + forward‑K (метрики з reports/)
python -m tools.quality_snapshot --csv quality.csv `
  --metrics-dir reports `
  --forward-k 3,5,10,20,30 `
  --forward-enable-long true `
  --out quality_snapshot.md
```

Опційно:

```powershell
python -m tools.verify_explain --last-min 240 --logs-dir ./logs `
  | Out-File reports\verify_explain_D6.txt -Encoding UTF8
```

## Acceptance (PASS якщо ≥4/5)

- p95(avg_wall_ms) ≤ 800 мс
- Explain coverage ≥ 0.60 у `reports/quality.csv`
- Σ(FSM sweep_then_breakout | sweep_reject) ≥ 3
- Δ no_candidate/hour не вище D4/D5
- Snapshot/dashboard консистентні, Forward‑K секція заповнена (K=20/30 можуть мати низькі N)

## Якщо p95 > 800 мс

- Зменшити список до 6 активів
- Додати sleep у батчі +100–150 мс
- Зменшити деталізацію логів (залишити [KPI] і [SCEN_EXPLAIN] раз на N‑й батч≈20)
