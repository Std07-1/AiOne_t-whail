import json
from pathlib import Path

from tools.run_stage3_canary import handle_scenario_alert


def test_paper_trade_is_recorded(tmp_path: Path, monkeypatch):
    # Перенаправимо вихідний файл у тимчасову теку
    reports_dir = tmp_path / "reports"
    out_file = reports_dir / "paper_trades.jsonl"

    # Мавпопач: підмінимо модульний шлях збереження
    import tools.run_stage3_canary as canary

    monkeypatch.setattr(canary, "_PAPER_OUT", out_file, raising=True)

    # Гарантуємо чистий стан
    if out_file.exists():
        out_file.unlink()

    ok = handle_scenario_alert("BTCUSDT", "breakout_confirmation", 0.71, "BUY")
    assert ok is True
    assert out_file.exists()
    lines = out_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["symbol"] == "BTCUSDT"
    assert rec["scenario"] == "breakout_confirmation"
    assert rec["direction"] == "BUY"
    assert isinstance(rec["confidence"], float)
