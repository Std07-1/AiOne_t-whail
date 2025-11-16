"""Утиліта для збору forward-зрізів whale_signal_v1 із stage1_events."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from utils.utils import safe_float

EVENT_NAME = "whale_signal_v1"
DEFAULT_EVENTS_FILE = "stage1_events.jsonl"
SLICE_FIELDS = [
    "ts",
    "symbol",
    "enabled",
    "profile",
    "direction",
    "confidence",
    "presence",
    "bias",
    "vwap_dev",
    "vol_regime",
    "age_s",
    "missing",
    "stale",
    "dominance_buy",
    "dominance_sell",
    "accum_cnt",
    "dist_cnt",
    "reasons",
]


@dataclass
class SymbolStats:
    total: int = 0
    enabled: int = 0
    sum_conf: float = 0.0
    sum_presence: float = 0.0
    sum_bias: float = 0.0

    def update(self, payload: dict[str, Any]) -> None:
        self.total += 1
        if payload.get("enabled"):
            self.enabled += 1
        self.sum_conf += _float(payload.get("confidence"))
        self.sum_presence += _float(payload.get("presence"))
        self.sum_bias += _float(payload.get("bias"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "enabled": self.enabled,
            "enabled_ratio": _ratio(self.enabled, self.total),
            "avg_confidence": _ratio(self.sum_conf, self.total),
            "avg_presence": _ratio(self.sum_presence, self.total),
            "avg_bias": _ratio(self.sum_bias, self.total),
        }


@dataclass
class ForwardSummary:
    total_events: int
    enabled_events: int
    profile_counts: dict[str, int]
    direction_counts: dict[str, int]
    vol_regime_counts: dict[str, int]
    reason_counts: dict[str, int]
    symbols: dict[str, dict[str, Any]]
    avg_confidence: float
    avg_presence: float
    avg_bias: float
    off_samples: int
    on_samples: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Збирає ON/OFF forward-зрізи whale_signal_v1 та формує агрегати за символами."
        )
    )
    parser.add_argument(
        "--events",
        type=str,
        default=str(Path("telemetry") / DEFAULT_EVENTS_FILE),
        help="Шлях до stage1_events.jsonl або директорії, що містить файл",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=str(Path("reports") / "whale_signal_forward"),
        help="Куди зберегти зрізи та підсумки",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="ISO8601 або epoch секунди для фільтрування початку інтервалу",
    )
    parser.add_argument(
        "--until",
        type=str,
        default=None,
        help="ISO8601 або epoch секунди для фільтрування кінця інтервалу",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Кома-розділений перелік символів (наприклад BTCUSDT,ETHUSDT)",
    )
    parser.add_argument(
        "--max-sample-rows",
        type=int,
        default=2000,
        help="Максимальна кількість рядків у кожному зрізі ON/OFF",
    )
    parser.add_argument(
        "--json",
        type=str,
        default=None,
        help="Необов'язковий шлях для збереження JSON-підсумку",
    )
    parser.add_argument(
        "--markdown",
        type=str,
        default=None,
        help="Необов'язковий шлях для збереження Markdown-звіту",
    )
    return parser.parse_args()


def _resolve_events_path(raw: str) -> Path:
    path = Path(raw)
    if path.is_dir():
        candidate = path / DEFAULT_EVENTS_FILE
        if candidate.exists():
            return candidate
    return path


def _parse_time(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        numeric = float(value)
        if numeric > 1e12:
            return numeric / 1000.0
        if numeric > 0:
            return numeric
    except (TypeError, ValueError):
        pass
    try:
        if value.endswith("Z"):
            iso = value.replace("Z", "+00:00")
        else:
            iso = value
        return datetime.fromisoformat(iso).astimezone(UTC).timestamp()
    except Exception:
        return None


def _bool(value: Any) -> bool:
    return bool(value)


def _float(value: Any, default: float = 0.0) -> float:
    resolved = safe_float(value, default)
    if resolved is None:
        return default
    return float(resolved)


def _ratio(num: float, denom: float) -> float:
    if not denom:
        return 0.0
    return float(num) / float(denom)


def _iter_events(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def _should_include(
    payload: dict[str, Any],
    since: float | None,
    until: float | None,
    symbols: set[str] | None,
) -> bool:
    ts = payload.get("ts")
    ts_epoch = None
    if ts:
        ts_epoch = _parse_time(str(ts))
    if since is not None and (ts_epoch is None or ts_epoch < since):
        return False
    if until is not None and (ts_epoch is None or ts_epoch > until):
        return False
    if symbols:
        symbol = str(payload.get("symbol", "")).upper()
        if symbol not in symbols:
            return False
    return True


def _slice_row(payload: dict[str, Any]) -> dict[str, Any]:
    dominance = payload.get("dominance")
    if not isinstance(dominance, dict):
        dominance = {"buy": False, "sell": False}
    zones = payload.get("zones_summary")
    if not isinstance(zones, dict):
        zones = {"accum_cnt": 0, "dist_cnt": 0}
    reasons = payload.get("reasons")
    if isinstance(reasons, list):
        reasons_str = ";".join(str(r) for r in reasons)
    else:
        reasons_str = ""

    return {
        "ts": payload.get("ts"),
        "symbol": str(payload.get("symbol", "")).upper(),
        "enabled": _bool(payload.get("enabled")),
        "profile": str(payload.get("profile") or "none"),
        "direction": str(payload.get("direction") or "unknown"),
        "confidence": _float(payload.get("confidence")),
        "presence": _float(payload.get("presence")),
        "bias": _float(payload.get("bias")),
        "vwap_dev": _float(payload.get("vwap_dev")),
        "vol_regime": str(payload.get("vol_regime") or "unknown"),
        "age_s": _float(payload.get("age_s")),
        "missing": _bool(payload.get("missing")),
        "stale": _bool(payload.get("stale")),
        "dominance_buy": _bool(dominance.get("buy")),
        "dominance_sell": _bool(dominance.get("sell")),
        "accum_cnt": int(zones.get("accum_cnt", 0) or 0),
        "dist_cnt": int(zones.get("dist_cnt", 0) or 0),
        "reasons": reasons_str,
    }


def collect_forward_slices(args: argparse.Namespace) -> ForwardSummary:
    events_path = _resolve_events_path(args.events)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    since = _parse_time(args.since)
    until = _parse_time(args.until)
    symbols = None
    if args.symbols:
        symbols = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}

    profile_counter: Counter[str] = Counter()
    direction_counter: Counter[str] = Counter()
    vol_regime_counter: Counter[str] = Counter()
    reason_counter: Counter[str] = Counter()
    symbols_stats: dict[str, SymbolStats] = defaultdict(SymbolStats)

    total = 0
    enabled = 0
    sum_conf = 0.0
    sum_presence = 0.0
    sum_bias = 0.0

    on_path = out_dir / "whale_forward_on.csv"
    off_path = out_dir / "whale_forward_off.csv"
    on_file = on_writer = off_file = off_writer = None
    try:
        on_file = on_path.open("w", encoding="utf-8", newline="")
        off_file = off_path.open("w", encoding="utf-8", newline="")
        on_writer = csv.DictWriter(on_file, fieldnames=SLICE_FIELDS)
        off_writer = csv.DictWriter(off_file, fieldnames=SLICE_FIELDS)
        on_writer.writeheader()
        off_writer.writeheader()

        on_written = 0
        off_written = 0

        for payload in _iter_events(events_path):
            if payload.get("event") != EVENT_NAME:
                continue
            if not _should_include(payload, since, until, symbols):
                continue

            total += 1
            enabled_flag = _bool(payload.get("enabled"))
            if enabled_flag:
                enabled += 1
            conf_val = _float(payload.get("confidence"))
            sum_conf += conf_val
            sum_presence += _float(payload.get("presence"))
            sum_bias += _float(payload.get("bias"))

            profile = str(payload.get("profile") or "none").lower()
            direction = str(payload.get("direction") or "unknown").lower()
            vol_regime = str(payload.get("vol_regime") or "unknown").lower()
            profile_counter[profile] += 1
            direction_counter[direction] += 1
            vol_regime_counter[vol_regime] += 1

            for reason in payload.get("reasons", []) or []:
                if isinstance(reason, str) and reason:
                    reason_counter[reason] += 1

            symbol = str(payload.get("symbol", "")).upper()
            symbols_stats[symbol].update(payload)

            row = _slice_row(payload)
            if enabled_flag and on_writer and on_written < args.max_sample_rows:
                on_writer.writerow(row)
                on_written += 1
            elif (
                (not enabled_flag) and off_writer and off_written < args.max_sample_rows
            ):
                off_writer.writerow(row)
                off_written += 1
    finally:
        if on_file:
            on_file.close()
        if off_file:
            off_file.close()

    off_estimate = max(total - enabled, 0)
    summary = ForwardSummary(
        total_events=total,
        enabled_events=enabled,
        profile_counts=dict(profile_counter),
        direction_counts=dict(direction_counter),
        vol_regime_counts=dict(vol_regime_counter),
        reason_counts=dict(reason_counter),
        symbols={sym: stats.to_dict() for sym, stats in symbols_stats.items()},
        avg_confidence=_ratio(sum_conf, total),
        avg_presence=_ratio(sum_presence, total),
        avg_bias=_ratio(sum_bias, total),
        off_samples=min(off_estimate, args.max_sample_rows),
        on_samples=min(enabled, args.max_sample_rows),
    )
    return summary


def render_markdown(summary: ForwardSummary) -> str:
    lines = ["# Whale Signal v1 Forward Summary", ""]
    lines.append(f"Total events: {summary.total_events}")
    enabled_ratio = (
        summary.enabled_events / summary.total_events if summary.total_events else 0.0
    )
    lines.append(f"Enabled: {summary.enabled_events} ({enabled_ratio:.2%})")
    lines.append(f"Avg confidence: {summary.avg_confidence:.3f}")
    lines.append(f"Avg presence: {summary.avg_presence:.3f}")
    lines.append(f"Avg bias: {summary.avg_bias:.3f}")
    lines.append("")
    lines.append("## Profiles")
    for key, value in sorted(summary.profile_counts.items()):
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Directions")
    for key, value in sorted(summary.direction_counts.items()):
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Top reasons")
    for key, value in Counter(summary.reason_counts).most_common(10):
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Symbol stats")
    for symbol, stats in sorted(summary.symbols.items()):
        lines.append(
            f"- {symbol}: total={stats['total']} enabled={stats['enabled']} "
            f"avg_conf={stats['avg_confidence']:.3f} avg_presence={stats['avg_presence']:.3f}"
        )
    return "\n".join(lines)


def main() -> None:
    args = _parse_args()
    summary = collect_forward_slices(args)
    enabled_ratio = (
        summary.enabled_events / summary.total_events if summary.total_events else 0.0
    )
    print(
        f"Processed {summary.total_events} events | enabled={summary.enabled_events} | "
        f"avg_confidence={summary.avg_confidence:.3f} | enabled_ratio={enabled_ratio:.2%}"
    )
    if args.json:
        Path(args.json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json).write_text(
            json.dumps(summary.to_dict(), indent=2), encoding="utf-8"
        )
    if args.markdown:
        Path(args.markdown).parent.mkdir(parents=True, exist_ok=True)
        Path(args.markdown).write_text(render_markdown(summary), encoding="utf-8")


if __name__ == "__main__":  # pragma: no cover
    main()
