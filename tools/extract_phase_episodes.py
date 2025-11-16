"""Extract notable PhaseState/scenario episodes from run.log into Markdown.

Usage example:
    python -m tools.extract_phase_episodes \
        --log reports/phase_state_8h_on/run.log \
        --symbol btcusdt \
        --out reports/phase_state_8h_on/episodes_btc.md \
        --phase-state-carried-only \
        --min-conf 0.35 \
        --max-episodes 10
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass
class Episode:
    ts_iso: str
    symbol: str
    phase: str
    scenario: str
    confidence: float
    direction: str
    phase_reason: str


def _parse_timestamp(line: str) -> str | None:
    if not line.startswith("["):
        return None
    end_idx = line.find("]")
    if end_idx == -1:
        return None
    ts_part = line[1:end_idx]
    try:
        dt = datetime.strptime(ts_part, "%m/%d/%y %H:%M:%S")
        dt = dt.replace(tzinfo=UTC)
        return dt.isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def _extract_simple_value(line: str, key: str) -> str | None:
    token = f"{key}="
    idx = line.find(token)
    if idx == -1:
        return None
    start = idx + len(token)
    value_chars: list[str] = []
    for ch in line[start:]:
        if ch.isspace():
            break
        value_chars.append(ch)
    return "".join(value_chars) if value_chars else None


def _maybe_float(text: str | None) -> float:
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _normalize_direction(value: str | None) -> str:
    if not value:
        return "unknown"
    value_low = value.lower()
    if value_low in {"long", "short", "buy", "sell"}:
        return "long" if value_low in {"long", "buy"} else "short"
    return "unknown"


def _is_entry_start(text: str) -> bool:
    return (
        text.startswith("[")
        or text.startswith("INFO ")
        or text.startswith("WARNING ")
        or text.startswith("ERROR ")
        or text.startswith("DEBUG ")
    )


def _collect_entry_lines(
    first_line: str, line_iter: Iterator[str]
) -> tuple[str, str | None]:
    parts = [first_line]
    for raw in line_iter:
        stripped = raw.strip()
        if not stripped:
            continue
        if _is_entry_start(stripped):
            return " ".join(parts), raw
        parts.append(stripped)
    return " ".join(parts), None


def extract_episodes(
    log_path: Path,
    symbol: str,
    min_conf: float,
    carried_only: bool,
    max_episodes: int,
) -> list[Episode]:
    symbol_low = symbol.lower()
    episodes: list[Episode] = []
    last_phase: dict[str, str] = {}
    last_reason: dict[str, str] = {}
    last_scenario: dict[str, tuple[str, float, str]] = {}

    with log_path.open(encoding="utf-8", errors="ignore") as fh:
        lines_iter = iter(fh)
        pending_line: str | None = None
        last_ts_iso: str | None = None

        while True:
            if pending_line is not None:
                raw_line = pending_line
                pending_line = None
            else:
                try:
                    raw_line = next(lines_iter)
                except StopIteration:
                    break

            line = raw_line.strip()
            if not line:
                continue

            ts_candidate = _parse_timestamp(line)
            if ts_candidate:
                last_ts_iso = ts_candidate
            ts_iso = ts_candidate or last_ts_iso

            if "[STRICT_PHASE]" in line:
                if _extract_simple_value(line, "symbol") == symbol_low:
                    phase_val = _extract_simple_value(line, "phase")
                    if phase_val:
                        last_phase[symbol_low] = phase_val
                    reasons = _extract_simple_value(line, "reasons")
                    if reasons:
                        last_reason[symbol_low] = reasons
                continue

            if "[PHASE_STATE_CARRY]" in line:
                if symbol_low not in line.lower() or ts_iso is None:
                    continue
                if _extract_simple_value(line, "symbol") != symbol_low:
                    continue
                phase_val = _extract_simple_value(line, "phase") or last_phase.get(
                    symbol_low, "unknown"
                )
                reason = _extract_simple_value(line, "reason") or last_reason.get(
                    symbol_low, ""
                )
                last_phase[symbol_low] = phase_val
                last_reason[symbol_low] = reason
                scenario, conf, direction = last_scenario.get(
                    symbol_low, ("carry_forward", 0.0, "unknown")
                )
                if conf < min_conf:
                    continue
                episodes.append(
                    Episode(
                        ts_iso=ts_iso,
                        symbol=symbol_low,
                        phase=phase_val or "unknown",
                        scenario=scenario,
                        confidence=conf,
                        direction=direction,
                        phase_reason=reason or "",
                    )
                )
            elif "[SCENARIO_TRACE]" in line:
                combined_line = line
                collected_line, pending_line = _collect_entry_lines(line, lines_iter)
                if collected_line:
                    combined_line = collected_line
                if symbol_low not in combined_line.lower() or ts_iso is None:
                    continue
                scenario_name = (
                    _extract_simple_value(combined_line, "candidate")
                    or _extract_simple_value(combined_line, "scenario")
                    or "unknown"
                )
                direction = _normalize_direction(
                    _extract_simple_value(combined_line, "direction")
                    or _extract_simple_value(combined_line, "side")
                )
                conf = _maybe_float(_extract_simple_value(combined_line, "conf"))
                last_scenario[symbol_low] = (scenario_name, conf, direction)
                if conf < min_conf:
                    continue
                if carried_only:
                    continue
                phase_val = last_phase.get(symbol_low) or "unknown"
                reason = last_reason.get(symbol_low) or (
                    _extract_simple_value(combined_line, "reason") or ""
                )
                episodes.append(
                    Episode(
                        ts_iso=ts_iso,
                        symbol=symbol_low,
                        phase=phase_val,
                        scenario=scenario_name,
                        confidence=conf,
                        direction=direction,
                        phase_reason=reason,
                    )
                )
            elif "[SCENARIO_ALERT]" in line:
                if symbol_low not in line.lower() or ts_iso is None:
                    continue
                scenario_name = (
                    _extract_simple_value(line, "activate")
                    or _extract_simple_value(line, "scenario")
                    or "unknown"
                )
                conf = _maybe_float(_extract_simple_value(line, "conf"))
                direction = _normalize_direction(
                    _extract_simple_value(line, "direction")
                    or _extract_simple_value(line, "side")
                )
                last_scenario[symbol_low] = (scenario_name, conf, direction)
                if conf < min_conf:
                    continue
                if carried_only:
                    continue
                phase_val = last_phase.get(symbol_low) or "unknown"
                reason = last_reason.get(symbol_low) or (
                    _extract_simple_value(line, "reason") or ""
                )
                episodes.append(
                    Episode(
                        ts_iso=ts_iso,
                        symbol=symbol_low,
                        phase=phase_val,
                        scenario=scenario_name,
                        confidence=conf,
                        direction=direction,
                        phase_reason=reason,
                    )
                )
            if len(episodes) >= max_episodes:
                break

    return episodes


def write_markdown(out_path: Path, episodes: Iterable[Episode]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| ts_utc | symbol | phase | scenario | conf | direction | phase_reason |",
        "|  ---   |  ---   | ---   | ---      | ---  | ---       | ---          |",
    ]
    for ep in episodes:
        lines.append(
            f"| {ep.ts_iso} | {ep.symbol} | {ep.phase} | {ep.scenario} | {ep.confidence:.2f} | {ep.direction} | {ep.phase_reason or ''} |"
        )
    if len(lines) == 2:
        lines.append("| - | - | - | - | - | - | - |")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract notable PhaseState/scenario episodes"
    )
    parser.add_argument("--log", required=True, help="Path to run.log")
    parser.add_argument(
        "--symbol", required=True, help="Symbol to filter (e.g. btcusdt)"
    )
    parser.add_argument("--out", required=True, help="Output Markdown path")
    parser.add_argument(
        "--phase-state-carried-only",
        action="store_true",
        help="Only include [PHASE_STATE_CARRY] events",
    )
    parser.add_argument(
        "--min-conf", type=float, default=0.0, help="Minimum scenario confidence"
    )
    parser.add_argument(
        "--max-episodes", type=int, default=10, help="Maximum episodes to emit"
    )
    args = parser.parse_args()

    log_path = Path(args.log)
    out_path = Path(args.out)
    episodes = extract_episodes(
        log_path=log_path,
        symbol=args.symbol,
        min_conf=args.min_conf,
        carried_only=args.phase_state_carried_only,
        max_episodes=max(args.max_episodes, 1),
    )
    write_markdown(out_path, episodes)
    print(f"Saved {len(episodes)} episodes to {out_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
