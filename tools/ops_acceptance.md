# Episodes acceptance (no new replays)

```powershell
python -m tools.analyze_replay --in replay_bench_two/run `
  --episodes-manifest replay_bench_two/episodes_manifest.jsonl `
  --set-scenario-from-manifest --merge-phases-from-logs

python -m tools.report_episodes --in replay_bench_two/run/replay_summary.csv
python -m tools.ci_accept_episodes --in replay_bench_two/run/replay_summary.csv
```
