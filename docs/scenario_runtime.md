## Runtime scenario publishing (stable)

- Published fields: `scenario_detected`, `scenario_confidence` (stabilized via hysteresis).
- Pullback requires stable presence: last 2 bars presence>=0.60 & HTF>=0.20.
- Prometheus: `ai_one_scenario{symbol,scenario}`, `ai_one_phase{symbol,phase}`, `ai_one_htf_strength{symbol}`.
- Alerts: ScenarioTooFlappy, ScenarioBlindSpot (see `monitoring/scenario_rules.yml`).
