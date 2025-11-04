from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Швидка перевірка покриття [SCEN_EXPLAIN]")
    ap.add_argument(
        "--last-min",
        type=int,
        default=60,
        dest="last_min",
        help="Останні N хв для аналізу",
    )
    ap.add_argument(
        "--logs-dir", default="./logs", help="Каталог із логами (містить app.log)"
    )
    return ap.parse_args()


def iter_recent(log_path: Path, since: datetime):
    ts_re = re.compile(r"\[(\d{2})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})\]")
    if not log_path.exists():
        return
    now = datetime.utcnow()
    cur_y = now.year % 100
    with log_path.open("r", encoding="utf-8", errors="ignore") as fh:
        for ln in fh:
            m = ts_re.search(ln)
            if not m:
                # якщо немає таймстемпу — включаємо як свіже
                yield ln
                continue
            mo, dd, yy, hh, mi, ss = map(int, m.groups())
            year = 2000 + (yy if yy <= cur_y else yy)
            try:
                ts = datetime(year, mo, dd, hh, mi, ss)
            except Exception:
                yield ln
                continue
            if ts >= since:
                yield ln


def main() -> int:
    args = parse_args()
    logs_dir = Path(args.logs_dir)
    log_path = logs_dir / "app.log"
    since = datetime.utcnow() - timedelta(minutes=int(args.last_min))

    sym_re = re.compile(r"symbol=([A-Za-z0-9_]+)")
    explain_tag = "[SCEN_EXPLAIN]"

    total = 0
    by_symbol: dict[str, int] = {}
    recent: list[str] = []
    for ln in iter_recent(log_path, since):
        if explain_tag not in ln:
            continue
        total += 1
        m = sym_re.search(ln)
        sym = (m.group(1) if m else "").upper()
        if sym:
            by_symbol[sym] = by_symbol.get(sym, 0) + 1
        # збираємо останні 3 приклади
        recent.append(ln.rstrip())
        if len(recent) > 3:
            recent.pop(0)

    print(f"explain_lines_total={total}")
    print(f"symbols_seen={len(by_symbol)}")
    # Додатковий QA: ліміти explain — середня кількість рядків на символ за хвилину
    minutes = max(1, int(args.last_min))
    sym_count = max(1, len(by_symbol))
    avg_per_symbol_per_min = round(total / sym_count / minutes, 3)
    print(f"lines_per_symbol_per_min_avg={avg_per_symbol_per_min}")
    # Детальніше: по кожному символу
    rates = {k: round(v / minutes, 3) for k, v in sorted(by_symbol.items())}
    if rates:
        print(
            "lines_rate_by_symbol/min="
            + ", ".join(f"{k}:{v}" for k, v in rates.items())
        )
    print(
        "coverage_by_symbol="
        + ", ".join(f"{k}:{v}" for k, v in sorted(by_symbol.items()))
    )
    if recent:
        print("recent_examples:")
        for r in recent:
            print(r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
