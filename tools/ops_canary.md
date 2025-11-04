# Canary run (BTC, TON, SNX)

## Flags
- STRICT_SCENARIO_HYSTERESIS_ENABLED=True
- PROM_GAUGES_ENABLED=True
- SCENARIO_TRACE_ENABLED=False  # enable for 5 min only when needed
- SCENARIO_CANARY_PUBLISH_CANDIDATE=True  # publish candidate to Redis/metrics without hysteresis
 - POLICY_V2_ENABLED=True  # enable Policy v2 core
 - POLICY_V2_TO_STAGE3_ENABLED=True  # route Policy v2 to Stage3 paper canary (JSONL + metrics)
 - STAGE3_PAPER_ENABLED=True  # allow paper trades emission

## Env overrides (optional)
- TEST_SCENARIO_SELECTOR_RELAXED=true
- SCEN_HTF_MIN=0.05
- SCEN_PULLBACK_PRESENCE_MIN=0.10
- SCEN_BREAKOUT_DVR_MIN=0.10
- SCEN_REQUIRE_BIAS=false
- SCEN_PULLBACK_ALLOW_NA=true

## Run (PowerShell)
# 1) start app as usual (run-app task)
# 2) verify state
redis-cli GET ai_one:state:TONUSDT | rg '"scenario_detected"|"scenario_confidence"'
curl -s http://localhost:9108/metrics | rg 'ai_one_scenario|ai_one_phase|ai_one_htf_strength'

# Paper canary (Policy v2 → Stage3):
#   enable POLICY_V2_TO_STAGE3_ENABLED and STAGE3_PAPER_ENABLED, run window for ≥60 min
#   then inspect JSONL and metrics
rg -n "\[STAGE3_PAPER\]" logs/*.log | tail -n 20
type reports\paper_trades.jsonl | more

# Paper PnL proxy (offline): generates Markdown and updates Prometheus gauges under flags
python -m tools.paper_pnl_proxy --trades reports\paper_trades.jsonl --ds-dir datastore --out reports\paper_pnl_proxy.md
type reports\paper_pnl_proxy.md | more
curl -s http://localhost:9108/metrics | rg 'ai_one_paper_winrate|ai_one_paper_avg_r'

# Trace (optional, 5 min)
# enable SCENARIO_TRACE_ENABLED=True, collect logs:
rg -n "\[SCENARIO_TRACE\]" logs/stage2/*.log | head -n 50

# Alerts (de/activate)
rg -n "\[SCENARIO_ALERT\]" logs/stage2/*.log | tail -n 50
