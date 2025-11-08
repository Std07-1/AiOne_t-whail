from __future__ import annotations

import re
from pathlib import Path

from tools.unified_runner import RunnerConfig, RunnerOrchestrator


def _fake_forward(path: Path, profile: str, footer: str) -> None:
    lines = [
        "# Forward filtered (presence>=0.50 & |bias|>=0.40)",
        "# whales_seen=1 alerts_seen=1 alerts_matched=1",
        "",  # blank
        "K=5: N=1 hit≈0.80",
        "K=10: N=1 hit≈0.60",
        "",
        f"# footer: {footer}",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def test_summary_forward_columns_and_params(tmp_path: Path) -> None:
    out_dir = tmp_path / "run"
    out_dir.mkdir(parents=True, exist_ok=True)
    # Створюємо фейкові forward-файли
    _fake_forward(
        out_dir / "forward_strong.md",
        "strong",
        "build=abc | window=[2025-01-01T00:00:00Z,2025-01-01T00:10:00Z] | source=whale | params={presence_min:0.75, bias_abs_min:0.60} | dedup_dropped=2 | N_total=2 | ttf05_median=5 | ttf10_median=9 | whale_max_age_sec=900",
    )
    _fake_forward(
        out_dir / "forward_soft.md",
        "soft",
        "build=abc | window=[2025-01-01T00:00:00Z,2025-01-01T00:10:00Z] | source=whale | params={presence_min:0.55, bias_abs_min:0.40} | dedup_dropped=0 | N_total=2 | ttf05_median=6 | ttf10_median=11 | whale_max_age_sec=1200",
    )
    _fake_forward(
        out_dir / "forward_explain.md",
        "explain",
        "build=abc | window=[2025-01-01T00:00:00Z,2025-01-01T00:10:00Z] | source=explain | params={presence_min:0.45, bias_abs_min:0.30} | dedup_dropped=1 | N_total=3 | ttf05_median=4 | ttf10_median=8 | explain_ttl_sec=600 | ttl_rejected=0 | ttl_reject_ratio=0.00 | skew_dropped=0",
    )

    # Мінімальні необхідні залежності для summary: metrics.txt може бути порожнім
    (out_dir / "metrics.txt").write_text("", encoding="utf-8")

    cfg = RunnerConfig(
        mode="live", duration_s=10, namespace="ai_one", report=False, out_dir=out_dir
    )
    orch = RunnerOrchestrator(cfg)
    # Симулюємо, що профілі задані
    cfg.forward_profiles = ["strong", "soft", "explain"]
    summary = orch.generate_summary()
    text = summary.read_text(encoding="utf-8")

    # Перевірка наявності нових колонок у таблиці
    assert "dedup_dropped_total" in text
    assert "skew_dropped_total" in text

    # Переконаємось, що значення dedup/skew з футера відобразилися (цифри)
    assert re.search(r"strong \| .* \| .* \| .* \| .* \| 2 \| - \|", text)
    assert re.search(r"soft \| .* \| .* \| .* \| .* \| 0 \| - \|", text)
    assert re.search(r"explain \| .* \| .* \| .* \| .* \| 1 \| 0 \|", text)

    # Таблиця параметрів
    assert "Forward profile params" in text
    assert re.search(r"strong \| 0.75 \| 0.60 \| 900 \| -", text)
    assert re.search(r"explain \| 0.45 \| 0.30 \| - \| 600", text)
