from __future__ import annotations

"""Тести CLI парсингу для tools.unified_runner.

Перевіряємо, що _parse_args коректно будує RunnerConfig для
режимів live та replay, і що поля заповнюються очікувано.
"""

from pathlib import Path

from tools.unified_runner import RunnerConfig, _parse_args


def test_parse_args_live(tmp_path: Path) -> None:
    out = tmp_path / "live_run"
    cfg: RunnerConfig = _parse_args(
        [
            "live",
            "--duration",
            "5",
            "--namespace",
            "ai_one_test",
            "--out-dir",
            str(out),
        ]
    )
    assert cfg.mode == "live"
    assert cfg.duration_s == 5
    assert cfg.namespace == "ai_one_test"
    assert cfg.out_dir == out


def test_parse_args_replay(tmp_path: Path) -> None:
    out = tmp_path / "replay_run"
    cfg: RunnerConfig = _parse_args(
        [
            "replay",
            "--limit",
            "10",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--interval",
            "1m",
            "--source",
            "snapshot",
            "--out-dir",
            str(out),
        ]
    )
    assert cfg.mode == "replay"
    assert cfg.limit == 10
    assert cfg.symbols == ["BTCUSDT", "ETHUSDT"]
    assert cfg.interval == "1m"
    assert cfg.source == "snapshot"
    assert cfg.out_dir == out
