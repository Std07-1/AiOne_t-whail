# Прозорість та метрики (Observability)

Цей файл консолідує публічні метрики, журнали пояснень і новий JSONL‑слід рішень для максимальної прозорості системи.

## Prometheus‑метрики (/metrics)

Увімкнення: `config.config.PROM_GAUGES_ENABLED=True` (порт `PROM_HTTP_PORT`, типово 9108).

Ключові метрики (гейджі/лічильники):
- presence{symbol} — частка «китів» (0..1)
- htf_strength{symbol} — сила HTF (0..1)
- ai_one_phase{symbol,phase} — one‑hot скори фаз
- ai_one_scenario{symbol,scenario} — конфіденція сценарію (канд. або стабільний)
- ai_one_bias_state{symbol} — −1/0/+1 за cd/slope
- ai_one_liq_sweep{symbol,side} — TTL‑подія ліківідного свіпа
- ai_one_liq_sweep_total{symbol,side} — лічильник свіпів
- ai_one_sweep_then_breakout_total{symbol} — свіп→брейкаут
- ai_one_sweep_reject_total{symbol} — свіп→відмова
- ai_one_retest_ok{symbol} — TTL‑флаг ретесту після брейкауту
- ai_one_scn_reject_total{symbol,reason} — причини деактивації сценарію
- ai_one_explain_lines_total{symbol} — кількість explain‑рядків
- ai_one_btc_regime{state} — one‑hot BTC режим
- ai_one_context_near_edge_persist{symbol} — персистентність «краю» за 12 барів
- ai_one_context_presence_sustain{symbol} — стійкість присутності за 12 барів

Порада: для швидкого зрізу запустіть наш «віконний» прогін і знімайте `/metrics` паралельно.

## SCEN_EXPLAIN та SCENARIO_TRACE

- `[SCEN_EXPLAIN]` — агреговане пояснення, рейт‑ліміт ≤ 1/10с/символ + кожен N‑й батч (`SCEN_EXPLAIN_VERBOSE_EVERY_N`).
- `[SCENARIO_TRACE]` — регулярний трейс кандидата: pred, decision, reason.

Увімкнення: `SCEN_EXPLAIN_ENABLED=True`, `SCENARIO_TRACE_ENABLED=True`.

## JSONL‑слід рішень (Decision Trace)

- Прапор: `DEBUG_DECISION_DUMP_ENABLED` (типово False).
- Директорія: `DECISION_DUMP_DIR` (типово `./telemetry/decision_traces`).
- Формат: по одному рядку JSON на батч/символ, мінімальний стабільний набір полів:
  - ts, symbol, phase, phase_score, phase_reasons,
  - scenario, scenario_conf, bias_state,
  - btc_regime, btc_htf_strength,
  - band_pct, near_edge, atr_ratio, vol_z, rsi, dvr, cd, slope_atr.

Приклад рядка:
```
{"ts": 1730544000000, "symbol": "BTCUSDT", "phase": "EXPANSION_START", "phase_score": 0.82, "phase_reasons": "band_narrow+slope_strong", "scenario": "breakout_confirmation", "scenario_conf": 0.41, "bias_state": 1, "btc_regime": "trend_up", "btc_htf_strength": 0.33, "band_pct": 0.0075, "near_edge": "upper", "atr_ratio": 1.2, "vol_z": 2.9, "rsi": 63.2, "dvr": 0.75, "cd": 0.42, "slope_atr": 0.9}
```

## market_context.meta

Ключові публікації для UI та інспекції:
- scenario_detected, scenario_confidence — стабільний сценарій або канарейковий кандидат
- decision_last_ts, decision_phase, decision_scenario, decision_confidence — компактний «вказівник» на останнє рішення
- btc_regime, btc_htf_strength, bias_state — контекст і упередженість
- evidence, scenario_explain — уривки з EvidenceBus/EXPLAIN
- liquidity_sweep_hint, sweep_then_breakout, sweep_reject, retest_ok — подієві маркери

## Як увімкнути «максимальну прозорість» швидко

1) Переконайтесь, що пром‑метрики увімкнено:
```
PROM_GAUGES_ENABLED=True
PROM_HTTP_PORT=9108
```
2) Увімкніть пояснювальні логи і JSONL‑дамп:
```
SCEN_EXPLAIN_ENABLED=True
DEBUG_DECISION_DUMP_ENABLED=True
```
3) Запустіть тестовий прогін вікном та знімайте /metrics.

