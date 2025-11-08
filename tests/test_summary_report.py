from __future__ import annotations

"""Тест генерації summary для RunnerOrchestrator.

Створюємо мінімальні снапшоти metrics.txt і quality.csv та перевіряємо,
що summary містить очікувані KPI.
"""

from pathlib import Path

from tools.unified_runner import RunnerConfig, RunnerOrchestrator


def _write_metrics_snapshots(path: Path) -> None:
    # Два снапшоти з 15-хв різницею для підрахунку switch_rate
    snap1 = (
        "### SNAPSHOT ts=2025-11-06T12:00:00Z\n"
        # histogram: count=100, sum=10000, p95≈200
        "ai_one_stage1_latency_ms_sum 10000\n"
        "ai_one_stage1_latency_ms_count 100\n"
        'ai_one_stage1_latency_ms_bucket{le="100"} 50\n'
        'ai_one_stage1_latency_ms_bucket{le="200"} 95\n'
        'ai_one_stage1_latency_ms_bucket{le="500"} 100\n'
        # switches total (сумарно по серіях)
        'ai_one_profile_switch_total{from="A",to="B"} 5\n'
        # false breakout total (сумується)
        'ai_one_false_breakout_total{symbol="BTCUSDT"} 3\n'
    )
    snap2 = (
        "### SNAPSHOT ts=2025-11-06T12:15:00Z\n"
        "ai_one_stage1_latency_ms_sum 12000\n"
        "ai_one_stage1_latency_ms_count 120\n"
        'ai_one_stage1_latency_ms_bucket{le="100"} 60\n'
        'ai_one_stage1_latency_ms_bucket{le="200"} 114\n'
        'ai_one_stage1_latency_ms_bucket{le="500"} 120\n'
        'ai_one_profile_switch_total{from="A",to="B"} 17\n'
        'ai_one_false_breakout_total{symbol="ETHUSDT"} 2\n'
    )
    path.write_text(snap1 + snap2, encoding="utf-8")


def _write_quality_csv(path: Path) -> None:
    path.write_text(
        (
            "symbol,scenario,activations,mean_conf,p75_conf,mean_time_to_stabilize_s,btc_gate_effect_rate,"
            "explain_coverage_rate,explain_latency_ms\n"
            "BTCUSDT,pre_breakout,10,0.5,0.6,30,0.0,0.8,120\n"
            "ETHUSDT,momentum,5,0.4,0.5,20,0.0,0.6,110\n"
        ),
        encoding="utf-8",
    )


def test_generate_summary_from_artifacts(tmp_path: Path) -> None:
    out_dir = tmp_path / "run"
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics_txt = out_dir / "metrics.txt"
    _write_metrics_snapshots(metrics_txt)

    quality_csv = out_dir / "quality.csv"
    _write_quality_csv(quality_csv)

    cfg = RunnerConfig(
        mode="live", duration_s=10, namespace="ai_one_test", out_dir=out_dir
    )
    orch = RunnerOrchestrator(cfg)
    # Підмінюємо шляхи (вони вже вказують на out_dir)
    summary_path = orch.generate_summary()

    text = summary_path.read_text(encoding="utf-8")
    # Перевіряємо ключові KPI
    assert "Stage1 p95 latency: 200" in text  # пікети налаштовано на 200
    assert "switch_rate: 0.80/хв" in text  # (17-5)/15 = 0.8
    assert "false_breakout_total: 5" in text  # 3+2
    # ExpCov зважений: (0.8*10 + 0.6*5)/15 = 0.7333 → 0.73
    assert "ExpCov: 0.73" in text
    assert "BTCUSDT:0.80" in text and "ETHUSDT:0.60" in text
