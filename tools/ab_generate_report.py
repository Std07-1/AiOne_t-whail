#!/usr/bin/env python3
"""
AB Report Generator: порівнює A/B /metrics знімки і формує Markdown-звіт.

Вхід: дві теки з сирими файлами metrics_YYYYMMDD_HHMMSS.txt (як збережено через Invoke-WebRequest)
Вихід: один .md файл із порівнянням по часових відмітках для символу.

Приклад:
  python -m tools.ab_generate_report \
    --dir-a ab_runs/A --dir-b ab_runs/B \
    --symbol BTCUSDT --out ab_runs/report_BTCUSDT.md \
    --label-a ContextOff --label-b ContextOn
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from statistics import mean

LINE_RE_LABELED = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)\{(?P<labels>[^}]*)\}\s+(?P<value>[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?\d+)?)$"
)
LINE_RE_PLAIN = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)\s+(?P<value>[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?\d+)?)$"
)

WANTED = {
    "htf_strength",
    "presence",
    "ai_one_bias_state",
    "ai_one_liq_sweep",
    "ai_one_liq_sweep_total",
    "ai_one_scenario",
    "ai_one_phase",
}


@dataclass
class Row:
    ts: dt.datetime
    htf: float | None = None
    pres: float | None = None
    bias: float | None = None
    ls_upper: float | None = None
    ls_lower: float | None = None
    lst_upper: float | None = None
    lst_lower: float | None = None
    scn_name: str | None = None
    scn_conf: float | None = None
    phase_name: str | None = None
    phase_score: float | None = None


def _iter_metrics(text: str) -> Iterable[tuple[str, dict[str, str], float]]:
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = LINE_RE_LABELED.match(line)
        if m:
            name = m.group("name")
            if name not in WANTED:
                continue
            labels_raw = m.group("labels")
            value = float(m.group("value"))
            labels: dict[str, str] = {}
            for part in labels_raw.split(","):
                if "=" not in part:
                    continue
                k, v = part.split("=", 1)
                labels[k.strip()] = v.strip().strip('"')
            yield name, labels, value
            continue
        m2 = LINE_RE_PLAIN.match(line)
        if m2:
            name = m2.group("name")
            if name not in WANTED:
                continue
            yield name, {}, float(m2.group("value"))


def _parse_timestamp_from_name(p: Path) -> dt.datetime | None:
    # metrics_YYYYMMDD_HHMMSS.txt
    try:
        s = p.stem.split("_")[1]
        return dt.datetime.strptime(s, "%Y%m%d")  # wrong format? fallback below
    except Exception:
        pass
    try:
        _, ts = p.stem.split("_", 1)
        return dt.datetime.strptime(ts, "%Y%m%d_%H%M%S")
    except Exception:
        return None


def parse_dir(path: Path, symbol: str) -> list[Row]:
    files = sorted([p for p in path.glob("metrics_*.txt") if p.is_file()])
    out: list[Row] = []
    for p in files:
        ts = _parse_timestamp_from_name(p)
        if ts is None:
            continue
        text = p.read_text(encoding="utf-8", errors="replace")
        best_scn_name: str | None = None
        best_scn_val: float = -1.0
        best_phase_name: str | None = None
        best_phase_val: float = -1.0
        row = Row(ts=ts)
        symu = symbol.upper()
        for name, labels, value in _iter_metrics(text):
            if labels.get("symbol", symu).upper() != symu:
                continue
            if name == "htf_strength":
                row.htf = value
            elif name == "presence":
                row.pres = value
            elif name == "ai_one_bias_state":
                row.bias = value
            elif name == "ai_one_liq_sweep":
                side = labels.get("side", "")
                if side == "upper":
                    row.ls_upper = value
                elif side == "lower":
                    row.ls_lower = value
            elif name == "ai_one_liq_sweep_total":
                side = labels.get("side", "")
                if side == "upper":
                    row.lst_upper = value
                elif side == "lower":
                    row.lst_lower = value
            elif name == "ai_one_scenario":
                scn = labels.get("scenario")
                if scn is not None and value >= best_scn_val:
                    best_scn_val = value
                    best_scn_name = scn
            elif name == "ai_one_phase":
                ph = labels.get("phase")
                if ph is not None and value >= best_phase_val:
                    best_phase_val = value
                    best_phase_name = ph
        # finalize best scenario/phase
        if best_scn_name is not None:
            row.scn_name = best_scn_name
            row.scn_conf = best_scn_val if best_scn_val >= 0 else None
        if best_phase_name is not None:
            row.phase_name = best_phase_name
            row.phase_score = best_phase_val if best_phase_val >= 0 else None
        out.append(row)
    return out


def render_markdown(
    symbol: str, label_a: str, label_b: str, rows_a: list[Row], rows_b: list[Row]
) -> str:
    def fmt(v: float | None, nd: int = 4) -> str:
        return f"{v:.{nd}f}" if isinstance(v, float) else ""

    # align by timestamps (use nearest matches: same index order)
    n = min(len(rows_a), len(rows_b))
    rows_a = rows_a[-n:]
    rows_b = rows_b[-n:]

    lines: list[str] = []
    lines.append(f"# A/B моніторинг {symbol}\n")
    lines.append(
        "Це автоматично згенерований порівняльний звіт двох ідентичних інстансів, що слухали різні порти /metrics.\n"
    )
    lines.append("- A: " + label_a)
    lines.append("- B: " + label_b + "\n")

    # quick summary
    def safe_mean(arr: list[float | None]) -> float | None:
        xs = [x for x in arr if isinstance(x, float)]
        return mean(xs) if xs else None

    avg_a_htf = safe_mean([r.htf for r in rows_a])
    avg_b_htf = safe_mean([r.htf for r in rows_b])
    avg_a_pres = safe_mean([r.pres for r in rows_a])
    avg_b_pres = safe_mean([r.pres for r in rows_b])

    last_a = rows_a[-1] if rows_a else Row(ts=dt.datetime.utcfromtimestamp(0))
    last_b = rows_b[-1] if rows_b else Row(ts=dt.datetime.utcfromtimestamp(0))

    lines.append("## Підсумок\n")
    lines.append(
        "- Середній htf_strength: A="
        + (fmt(avg_a_htf, 4) or "–")
        + ", B="
        + (fmt(avg_b_htf, 4) or "–")
    )
    lines.append(
        "- Середній presence: A="
        + (fmt(avg_a_pres, 4) or "–")
        + ", B="
        + (fmt(avg_b_pres, 4) or "–")
    )
    lines.append(
        "- Останній bias_state: A="
        + (fmt(last_a.bias, 0) or "–")
        + ", B="
        + (fmt(last_b.bias, 0) or "–")
    )
    lines.append(
        "- Лічильник sweep_total (upper, lower) — останні значення: A=("
        + (fmt(last_a.lst_upper, 0) or "–")
        + ", "
        + (fmt(last_a.lst_lower, 0) or "–")
        + ") B=("
        + (fmt(last_b.lst_upper, 0) or "–")
        + ", "
        + (fmt(last_b.lst_lower, 0) or "–")
        + ")\n"
    )

    # table header
    lines.append("## Часова лінія (кожні ~15 хв)\n")
    lines.append(
        "| Timestamp | A htf | B htf | A pres | B pres | A bias | B bias | A scn | B scn | A phase | B phase | A sweepU | B sweepU | A sweepL | B sweepL |"
    )
    lines.append(
        "|---|---:|---:|---:|---:|---:|---:|---|---|---|---|---:|---:|---:|---:|"
    )
    for ra, rb in zip(rows_a, rows_b, strict=True):
        ts = ra.ts.strftime("%Y-%m-%d %H:%M:%S")
        lines.append(
            "| "
            + ts
            + " | "
            + (fmt(ra.htf) or "")
            + " | "
            + (fmt(rb.htf) or "")
            + " | "
            + (fmt(ra.pres) or "")
            + " | "
            + (fmt(rb.pres) or "")
            + " | "
            + (fmt(ra.bias, 0) or "")
            + " | "
            + (fmt(rb.bias, 0) or "")
            + " | "
            + (ra.scn_name or "")
            + " ("
            + (fmt(ra.scn_conf, 3) or "")
            + ")"
            + " | "
            + (rb.scn_name or "")
            + " ("
            + (fmt(rb.scn_conf, 3) or "")
            + ")"
            + " | "
            + (ra.phase_name or "")
            + " ("
            + (fmt(ra.phase_score, 3) or "")
            + ")"
            + " | "
            + (rb.phase_name or "")
            + " ("
            + (fmt(rb.phase_score, 3) or "")
            + ")"
            + " | "
            + (fmt(ra.ls_upper, 0) or "")
            + " | "
            + (fmt(rb.ls_upper, 0) or "")
            + " | "
            + (fmt(ra.ls_lower, 0) or "")
            + " | "
            + (fmt(rb.ls_lower, 0) or "")
            + " |"
        )

    lines.append("\n## Пояснення метрик\n")
    lines.append(
        "- htf_strength — сила HTF (0..1). Більше означає сильніший тренд на старшому таймфреймі."
    )
    lines.append("- presence — присутність whale-сигналів (0..1). 0 — відсутні.")
    lines.append("- ai_one_bias_state — узагальнений bias: -1=down, 0=none, +1=up.")
    lines.append(
        "- ai_one_liq_sweep — миттєвий евент-гейдж (0/1) з TTL; показує сам факт події у вікні TTL."
    )
    lines.append(
        "- ai_one_liq_sweep_total — лічильник подій sweep (upper/lower) за весь аптайм процесу."
    )
    lines.append(
        "- ai_one_scenario — впевненість (0..1) по сценарію (показано сценарій із найвищою conf за зріз)."
    )
    lines.append(
        "- ai_one_phase — оцінка фази (показано фазу з найвищим score за зріз).\n"
    )

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="AB Markdown report generator")
    ap.add_argument("--dir-a", required=True)
    ap.add_argument("--dir-b", required=True)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--label-a", default="A")
    ap.add_argument("--label-b", default="B")
    args = ap.parse_args(argv)

    dir_a = Path(args.dir_a)
    dir_b = Path(args.dir_b)
    if not dir_a.exists() or not dir_b.exists():
        raise SystemExit("Input directories do not exist")

    rows_a = parse_dir(dir_a, args.symbol)
    rows_b = parse_dir(dir_b, args.symbol)

    if not rows_a or not rows_b:
        raise SystemExit("No snapshot files found in input directories")

    md = render_markdown(args.symbol, args.label_a, args.label_b, rows_a, rows_b)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"Written report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
