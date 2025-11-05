from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path

RE_WHALE = re.compile(
    r"^.*\[STRICT_WHALE\].*?dir=(?P<dir>\w+).*?reasons=(?P<reasons>.+)$"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser("Summarize STRICT_WHALE reasons from a log file")
    p.add_argument("--log", required=True, help="Path to run_window log file")
    p.add_argument("--out", required=True, help="Output markdown file")
    return p.parse_args()


def main() -> int:
    ns = parse_args()
    log_path = Path(ns.log)
    out_path = Path(ns.out)

    by_key = Counter()
    by_profile = Counter()
    dir_counter = Counter()
    dom_ok = Counter()
    alt_ok = Counter()
    hysteresis = Counter()
    cooldown = Counter()
    stale = Counter()

    if not log_path.exists():
        out_path.write_text(
            "# STRICT_WHALE Summary\n\n(лог не знайдено)\n", encoding="utf-8"
        )
        return 0

    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = RE_WHALE.match(line.strip())
            if not m:
                continue
            dir_counter[m.group("dir")] += 1
            raw = m.group("reasons")
            # відрізати потенційні крапки в кінці рядка
            raw = raw.strip().removesuffix("…").removesuffix("...")
            parts = [p.strip() for p in raw.split(",") if p.strip()]
            for p in parts:
                by_key[p] += 1
                if p.startswith("profile="):
                    by_profile[p.split("=", 1)[1]] += 1
                elif p.startswith("dom_ok="):
                    dom_ok[p.split("=", 1)[1]] += 1
                elif p.startswith("alt_ok="):
                    alt_ok[p.split("=", 1)[1]] += 1
                elif p.startswith("hysteresis="):
                    hysteresis[p.split("=", 1)[1]] += 1
                elif p.startswith("cooldown="):
                    cooldown[p.split("=", 1)[1]] += 1
                elif p.startswith("stale="):
                    stale[p.split("=", 1)[1]] += 1

    def _fmt_counter(title: str, c: Counter) -> str:
        if not c:
            return f"### {title}\n(n/a)\n"
        lines = [f"### {title}"]
        total = sum(c.values())
        for k, v in c.most_common():
            pct = (100.0 * v / total) if total else 0.0
            lines.append(f"- {k}: {v} ({pct:.1f}%)")
        return "\n".join(lines) + "\n"

    md = ["# STRICT_WHALE Reasons Summary", ""]
    md.append(_fmt_counter("Directions", dir_counter))
    md.append(_fmt_counter("Profiles", by_profile))
    md.append(_fmt_counter("dom_ok (0/1)", dom_ok))
    md.append(_fmt_counter("alt_ok (0/1)", alt_ok))
    md.append(_fmt_counter("hysteresis (0/1)", hysteresis))
    md.append(_fmt_counter("cooldown (0/1)", cooldown))
    md.append(_fmt_counter("stale (0/1)", stale))
    md.append(_fmt_counter("Top reason tokens", by_key))

    out_path.write_text("\n".join(md), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
