from __future__ import annotations

"""Тести шімів: run_window, bench_pseudostream, night_bundle.

Мета: переконатися, що вони друкують попередження і делегують у
tools.unified_runner з коректною побудовою аргументів.
"""

import sys
from types import SimpleNamespace

import tools.bench_pseudostream as bp
import tools.night_bundle as nb
import tools.run_window as rw


class _FakeCompleted:
    def __init__(self, returncode: int = 0) -> None:
        self.returncode = returncode


def test_run_window_shim_delegates(monkeypatch, capsys) -> None:
    called = SimpleNamespace(args=None)

    def fake_run(args, *a, **kw):  # type: ignore[no-redef]
        called.args = list(args)
        return _FakeCompleted(0)

    monkeypatch.setattr("subprocess.run", fake_run)
    # Симулюємо argv для шіма
    old_argv = sys.argv
    sys.argv = [
        "python",
        "-m",
        "tools.run_window",
        "--duration",
        "5",
        "--set",
        "STATE_NAMESPACE=ai_one_X",
        "--set",
        "PROM_HTTP_PORT=9108",
        "--log",
        "dummy.log",
    ]
    try:
        rc = rw.main()
    finally:
        sys.argv = old_argv

    out = capsys.readouterr().out
    assert rc == 0
    assert "unified_runner live" in out
    assert called.args is not None
    # Перевіряємо, що делеговано у unified_runner live
    assert called.args[:3] == [sys.executable, "-m", "tools.unified_runner"]
    assert "live" in called.args
    # Перенесені прапори
    assert "--namespace" in called.args and "ai_one_X" in called.args
    assert "--prom-port" in called.args and "9108" in called.args
    assert "--report" in called.args


def test_bench_pseudostream_shim_delegates(monkeypatch, capsys) -> None:
    called = SimpleNamespace(args=None)

    def fake_run(args, *a, **kw):  # type: ignore[no-redef]
        called.args = list(args)
        return _FakeCompleted(0)

    monkeypatch.setattr("subprocess.run", fake_run)
    rc = bp.main(["--limit", "10", "--symbols", "BTCUSDT,ETHUSDT"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "unified_runner replay" in out
    assert called.args[:3] == [sys.executable, "-m", "tools.unified_runner"]
    assert "replay" in called.args and "--limit" in called.args


def test_night_bundle_shim_delegates(monkeypatch, capsys) -> None:
    called = SimpleNamespace(args=None)

    def fake_run(args, *a, **kw):  # type: ignore[no-redef]
        called.args = list(args)
        return _FakeCompleted(0)

    monkeypatch.setattr("subprocess.run", fake_run)
    rc = nb.main()
    out = capsys.readouterr().out
    assert rc == 0
    assert "unified_runner live" in out
    assert called.args[:3] == [sys.executable, "-m", "tools.unified_runner"]
    assert "--report" in called.args
