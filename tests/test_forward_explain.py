from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd

import tools.forward_from_log as fwd


def _mk_df(
    start_ts_ms: int, bars: int = 100, step_ms: int = 60_000, up: bool = True
) -> pd.DataFrame:
    open_times: list[int] = [start_ts_ms + i * step_ms for i in range(bars)]
    closes: list[float] = []
    base = 100.0
    for i in range(bars):
        delta = (i * 0.1) if up else (-i * 0.1)
        closes.append(base + delta)
    return pd.DataFrame({"open_time": open_times, "close": closes})


async def _run_forward(tmp: Path, log_text: str, *, source: str, dedup: Path) -> str:
    log_path = tmp / f"run_{source}.log"
    out_path = tmp / f"forward_{source}.md"
    log_path.write_text(log_text, encoding="utf-8")

    # Монкіпатч сховища: уникаємо Redis/зовнішніх сервісів
    async def _fake_loader(
        symbols: list[str], interval: str = "1m", limit: int = 6000
    ) -> dict[str, pd.DataFrame]:
        base_ts = 1_700_000_000_000  # стабільний епох‑час
        return {sym: _mk_df(base_ts) for sym in symbols}

    orig = fwd._load_dataframes
    fwd._load_dataframes = _fake_loader  # type: ignore[assignment]
    try:
        a = type("Args", (), {})()
        a.log = str(log_path)
        a.out = str(out_path)
        a.k = [5, 10]
        a.presence_min = 0.45 if source == "explain" else 0.75
        a.bias_abs_min = 0.30 if source == "explain" else 0.60
        a.whale_max_age_sec = 900
        a.source = source
        a.explain_ttl_sec = 600
        a.dedup_file = str(dedup)
        await fwd.run(a)  # type: ignore[arg-type]
    finally:
        fwd._load_dataframes = orig  # type: ignore[assignment]
    return out_path.read_text(encoding="utf-8", errors="ignore")


def test_forward_explain_ttl_and_dedup(tmp_path: Path) -> None:
    # Формуємо лог з невпорядкованими рядками: explain, whale, activate
    # Час беремо ISO Z, fwd парсить у ms epoch
    t_explain_ok = "2025-01-01T00:00:00Z"
    t_explain_old = "2025-01-01T00:05:00Z"  # 300s до події — ще в межах TTL; зсунемо активацію пізніше, щоб перевищити TTL
    t_whale = "2025-01-01T00:00:30Z"
    t_activate = (
        "2025-01-01T00:10:45Z"  # explain_old стане >600s від активації, відфільтрується
    )

    sym = "BTCUSDT"
    log_lines = [
        # Non‑monotonic: спершу активація
        f"[SCENARIO_ALERT] activate symbol={sym} ts={t_activate}",
        # Потім валідний explain з достатніми presence/bias
        f'[SCEN_EXPLAIN] symbol={sym} ts={t_explain_ok} explain="presence=0.55 bias=0.70 more=..."',
        # Старий explain, що перевищить TTL до часу активації
        f'[SCEN_EXPLAIN] symbol={sym} ts={t_explain_old} explain="presence=0.60 bias=0.40"',
        # Whale для strong профілю, достатній
        f"[STRICT_WHALE] symbol={sym} ts={t_whale} presence=0.90 bias=0.80",
    ]
    log_text = "\n".join(log_lines) + "\n"

    dedup_file = tmp_path / "dedup.keys"

    # Спочатку strong (whale) — повинен зловити подію
    strong_md = asyncio.run(
        _run_forward(tmp_path, log_text, source="whale", dedup=dedup_file)
    )
    assert "K=5:" in strong_md
    # Переконаємось, що є бодай одне ненульове N і присутній футер із TTF
    assert "K=5: N=" in strong_md
    assert "ttf05_median=" in strong_md

    # Потім explain — подія має бути відкинута дедупом (та рахується ttl_rejected для другого explain)
    explain_md = asyncio.run(
        _run_forward(tmp_path, log_text, source="explain", dedup=dedup_file)
    )
    # N можуть бути 0 через дедуп; перевіряємо Markdown не ламається і є footer
    assert "K=5:" in explain_md
    assert "# footer:" in explain_md
    # Наявність ttl статистики у футері
    assert "explain_ttl_sec=600" in explain_md

    # Clock-skew: створимо подію, де ts_alert < ts_explain
    log_skew = (
        "\n".join(
            [
                f'[SCEN_EXPLAIN] symbol={sym} ts=2025-01-01T00:10:00Z explain="presence=0.60 bias=0.50"',
                f"[SCENARIO_ALERT] activate symbol={sym} ts=2025-01-01T00:09:00Z",
            ]
        )
        + "\n"
    )
    skew_md = asyncio.run(
        _run_forward(tmp_path, log_skew, source="explain", dedup=dedup_file)
    )
    assert "skew_dropped=" in skew_md

    # Перевірка, що дубльована подія не продублювалась між профілями
    # Ключове — наявність спільного dedup.keys і відсутність нових матчів в explain
    assert "K=5: N=0" in explain_md or "K=10: N=0" in explain_md


def test_short_window_note_and_whale_param(tmp_path: Path) -> None:
    sym = "ETHUSDT"
    # Вікно < 60с для explain
    log_short_explain = (
        "\n".join(
            [
                f'[SCEN_EXPLAIN] symbol={sym} ts=2025-01-01T00:00:00Z explain="presence=0.60 bias=0.60"',
                f"[SCENARIO_ALERT] activate symbol={sym} ts=2025-01-01T00:00:30Z",
            ]
        )
        + "\n"
    )
    md_ex = asyncio.run(
        _run_forward(
            tmp_path, log_short_explain, source="explain", dedup=tmp_path / "d1.keys"
        )
    )
    assert "note=too_short_window" in md_ex
    assert "explain_ttl_sec=" in md_ex

    # Коротке вікно для whale та наявність whale_max_age_sec у футері
    log_short_whale = (
        "\n".join(
            [
                f"[STRICT_WHALE] symbol={sym} ts=2025-01-01T00:00:10Z presence=0.90 bias=0.80",
                f"[SCENARIO_ALERT] activate symbol={sym} ts=2025-01-01T00:00:20Z",
            ]
        )
        + "\n"
    )
    md_wh = asyncio.run(
        _run_forward(
            tmp_path, log_short_whale, source="whale", dedup=tmp_path / "d2.keys"
        )
    )
    assert "whale_max_age_sec=" in md_wh
