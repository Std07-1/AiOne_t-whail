"""Утиліта для QA PhaseState: підрахунок carry-forward подій за логами.

Запуск:
  python -m tools.analyze_phase_state --log logs/run_phase_state_cf_on.log \
      --out reports/run_phase_state_cf_on/phase_state_qa.md
"""

from __future__ import annotations

import argparse
import math
import re
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median

CARRY_PATTERN = re.compile(
    r"\[PHASE_STATE_CARRY\]\s+symbol=(?P<symbol>\S+)\s+phase_raw=(?P<raw>\S+)\s+"
    r"phase=(?P<phase>\S+)\s+age_s=(?P<age>[0-9.]+)\s+reason=(?P<reason>\S+)"
)


@dataclass(slots=True)
class CarryEvent:
    symbol: str
    raw_phase: str | None
    carried_phase: str | None
    age_s: float
    reason: str | None


def _normalize_token(value: str | None) -> str | None:
    if value is None:
        return None
    val = value.strip()
    if val in {"", "None", "none", "NULL", "null", "-"}:
        return None
    return val


def load_carry_events(log_path: Path) -> tuple[list[CarryEvent], int, int, int]:
    events: list[CarryEvent] = []
    phase_none_lines = 0
    phase_lines = 0
    total_lines = 0
    with log_path.open("r", encoding="utf-8", errors="ignore") as handler:
        for line in handler:
            total_lines += 1
            if "phase=" in line:
                phase_lines += 1
                if "phase=None" in line:
                    phase_none_lines += 1
            match = CARRY_PATTERN.search(line)
            if not match:
                continue
            age_v = float(match.group("age")) if match.group("age") else 0.0
            events.append(
                CarryEvent(
                    symbol=match.group("symbol"),
                    raw_phase=_normalize_token(match.group("raw")),
                    carried_phase=_normalize_token(match.group("phase")),
                    age_s=age_v,
                    reason=_normalize_token(match.group("reason")),
                )
            )
    return events, phase_none_lines, phase_lines, total_lines


def describe_ages(values: Iterable[float]) -> dict[str, float]:
    data = [v for v in values if math.isfinite(v)]
    if not data:
        return {
            "count": 0,
            "mean": math.nan,
            "median": math.nan,
            "p90": math.nan,
            "max": math.nan,
        }
    data.sort()
    idx = max(0, math.ceil(0.9 * len(data)) - 1)
    return {
        "count": len(data),
        "mean": round(float(mean(data)), 2),
        "median": round(float(median(data)), 2),
        "p90": round(float(data[idx]), 2),
        "max": round(float(data[-1]), 2),
    }


def render_markdown(
    *,
    events: list[CarryEvent],
    log_path: Path,
    out_path: Path,
    phase_none_lines: int,
    phase_lines: int,
    total_lines: int,
) -> str:
    total = len(events)
    none_to_phase = sum(
        1 for evt in events if evt.raw_phase is None and evt.carried_phase is not None
    )
    reason_counter = Counter(evt.reason or "unknown" for evt in events)
    per_symbol = defaultdict(int)
    for evt in events:
        per_symbol[evt.symbol] += 1
    age_stats = describe_ages(evt.age_s for evt in events)

    lines: list[str] = []
    lines.append("# PhaseState QA - carry-forward")
    lines.append("")
    lines.append(f"- Лог: `{log_path}`")
    lines.append(f"- Подій `[PHASE_STATE_CARRY]`: **{total}**")
    lines.append(f"- Переходів None→phase: **{none_to_phase}**")
    if phase_lines:
        share_none = phase_none_lines / phase_lines
        lines.append(
            f"- Частка `phase=None` по логу: {phase_none_lines}/{phase_lines} ("
            f"{share_none:.2%})"
        )
    lines.append(f"- Усього рядків у логу: {total_lines}")
    lines.append("")

    lines.append("## Розклад reason-кодів")
    if reason_counter:
        lines.append("| reason | count | share |")
        lines.append("|  ---   |  ---: |  ---: |")
        for reason, cnt in reason_counter.most_common():
            share = cnt / total if total else 0.0
            lines.append(f"| {reason} | {cnt} | {share:.1%} |")
    else:
        lines.append("Немає carry-епізодів у логу.")
    lines.append("")

    lines.append("## Статистика age_s")
    if age_stats["count"]:
        lines.append(
            "- Середнє: {mean:.2f} с; медіана: {median:.2f} с; p90: {p90:.2f} с; максимум: {max:.2f} с".format(
                **age_stats
            )
        )
    else:
        lines.append("- Дані відсутні")
    lines.append("")

    lines.append("## Розклад по символах")
    if per_symbol:
        lines.append("| symbol | carry_events |")
        lines.append("|  ---   |  ---:        |")
        for symbol, cnt in sorted(
            per_symbol.items(), key=lambda item: item[1], reverse=True
        ):
            lines.append(f"| {symbol} | {cnt} |")
    else:
        lines.append("- Порожньо")
    lines.append("")

    lines.append(
        "_Згенеровано `tools.analyze_phase_state` - використовуйте цей звіт у парі з `tools.signals_truth` та Prometheus dump для повної QA-картини._"
    )

    content = "\n".join(lines)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    return content


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Побудова Markdown-звіту за логами PhaseState"
    )
    parser.add_argument(
        "--log", type=Path, required=True, help="Шлях до run_window логів"
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("reports/phase_state_qa.md"),
        help="Куди зберегти Markdown-звіт",
    )
    args = parser.parse_args(argv)
    if not args.log.exists():
        parser.error(f"Лог не знайдено: {args.log}")
    events, phase_none_lines, phase_lines, total_lines = load_carry_events(args.log)
    render_markdown(
        events=events,
        log_path=args.log,
        out_path=args.out,
        phase_none_lines=phase_none_lines,
        phase_lines=phase_lines,
        total_lines=total_lines,
    )
    print(f"✅ Звіт збережено до {args.out} (подій: {len(events)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
