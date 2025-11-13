from __future__ import annotations

from pathlib import Path

from tools.unified_runner import main

# Smoke-тест для standalone постпроцесу tools.unified_runner.
# Перевірка: main(["postprocess"]) повертає rc=0 за наявності лише run.log;
# та створюється summary.md у вказаному каталозі.


def test_standalone_postprocess_rc_zero(tmp_path: Path) -> None:
    out = tmp_path / "run_empty"
    out.mkdir(parents=True, exist_ok=True)
    # Мінімально необхідний артефакт — run.log (може бути порожній)
    (out / "run.log").write_text("", encoding="utf-8")
    # Викликаємо CLI main у режимі postprocess
    rc = main(
        [
            "postprocess",
            "--in-dir",
            str(out),
            "--forward-profiles",
            "strong,soft,explain",
        ]
    )
    assert rc == 0
    # Пересвідчитись, що summary.md зʼявився
    assert (out / "summary.md").exists()
