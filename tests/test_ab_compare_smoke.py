from pathlib import Path

from tools.ab_compare import main as ab_compare_main

SAMPLE = """
# HELP ai_one_scn_reject_total ...
ai_one_scn_reject_total{symbol="BTCUSDT",reason="no_candidate"} 3
htf_strength{symbol="BTCUSDT"} 0.12
presence{symbol="BTCUSDT"} 0.34
ai_one_bias_state{symbol="BTCUSDT"} 1
ai_one_liq_sweep_total{symbol="BTCUSDT",side="upper"} 7
ai_one_liq_sweep_total{symbol="BTCUSDT",side="lower"} 4
ai_one_scenario{symbol="BTCUSDT",scenario="breakout_confirmation"} 0.62
ai_one_phase{symbol="BTCUSDT",phase="momentum"} 0.51
""".strip()


def _write_metrics(dirp: Path, idx: int, text: str = SAMPLE) -> None:
    dirp.mkdir(parents=True, exist_ok=True)
    (dirp / f"metrics_20250101_01010{idx}.txt").write_text(text, encoding="utf-8")


def test_ab_compare_short(tmp_path: Path):
    a = tmp_path / "A"
    b = tmp_path / "B"
    _write_metrics(a, 1)
    _write_metrics(
        a, 2, SAMPLE.replace(" 3\n", " 5\n")
    )  # змінюємо лічильник reject_total
    _write_metrics(b, 1)
    _write_metrics(b, 2, SAMPLE.replace("0.34", "0.44"))  # трохи інші значення

    out_md = tmp_path / "report_BTCUSDT_short.md"
    rc = ab_compare_main(
        [
            "--a-dir",
            str(a),
            "--b-dir",
            str(b),
            "--symbol",
            "BTCUSDT",
            "--out",
            str(out_md),
        ]
    )
    assert rc == 0
    txt = out_md.read_text(encoding="utf-8")
    assert "A/B короткий зріз BTCUSDT" in txt
    assert "no_candidate rate" in txt
    assert "mean htf" in txt
