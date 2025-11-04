#!/usr/bin/env python3
"""
AB Monitor: Паралельний збір Prometheus-метрик від двох інстансів (A/B)

- Порт A і порт B (напр. 9108 і 9118)
- Періодичне опитування /metrics з обох інстансів одночасно
- Збереження сирих знімків і компактного summary.csv для порівняння

Використання (приклад):
  python -m tools.ab_monitor --port-a 9108 --port-b 9118 --duration-sec 300 --interval-sec 10 --symbol BTCUSDT --out-dir ab_runs/run_1
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
import urllib.request
from collections.abc import Iterable
from pathlib import Path


def _fetch_metrics(port: int, timeout: float = 2.0) -> str:
    url = f"http://127.0.0.1:{int(port)}/metrics"
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # nosec B310
        data = resp.read()
    try:
        return data.decode("utf-8", errors="replace")
    except Exception:
        return data.decode("latin-1", errors="replace")


_LABEL_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)\{(?P<labels>[^}]*)\}\s+(?P<value>[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?\d+)?)$"
)
_PLAIN_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)\s+(?P<value>[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?\d+)?)$"
)


def _parse_line(line: str) -> tuple[str, dict[str, str], float] | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    m = _LABEL_RE.match(line)
    if m:
        name = m.group("name")
        labels_raw = m.group("labels")
        value = float(m.group("value"))
        labels: dict[str, str] = {}
        for part in labels_raw.split(","):
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"')
            labels[k] = v
        return (name, labels, value)
    m2 = _PLAIN_RE.match(line)
    if m2:
        return (m2.group("name"), {}, float(m2.group("value")))
    return None


WANTED = {
    "htf_strength",
    "presence",
    "ai_one_bias_state",
    "ai_one_liq_sweep",
    "ai_one_liq_sweep_total",
    "ai_one_scenario",
    "ai_one_phase",
}


def _iter_metrics(text: str) -> Iterable[tuple[str, dict[str, str], float]]:
    for line in text.splitlines():
        parsed = _parse_line(line)
        if not parsed:
            continue
        name, labels, value = parsed
        if name in WANTED:
            yield name, labels, value


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def monitor_ab(
    port_a: int,
    port_b: int,
    duration_sec: int,
    interval_sec: float,
    out_dir: Path,
    symbol: str | None,
) -> None:
    symbol = (symbol or "").upper() or None
    out_a = out_dir / "A"
    out_b = out_dir / "B"
    _ensure_dir(out_a)
    _ensure_dir(out_b)

    summary_path = out_dir / "summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(
            [
                "ts",
                "src",
                "symbol",
                "htf_strength",
                "presence",
                "bias_state",
                "liq_sweep_upper",
                "liq_sweep_lower",
                "liq_sweep_total_upper",
                "liq_sweep_total_lower",
                "scenario",
                "scenario_confidence",
                "phase",
                "phase_score",
            ]
        )
        end_ts = time.time() + float(duration_sec)
        snap_idx = 0
        while time.time() < end_ts:
            snap_idx += 1
            ts = int(time.time())
            for label, port, out_folder in (("A", port_a, out_a), ("B", port_b, out_b)):
                try:
                    text = _fetch_metrics(port)
                except Exception as e:
                    # Записуємо порожній снапшот з помилкою (для діагностики)
                    (out_folder / f"metrics_{ts}.txt").write_text(
                        f"# ERROR: {e}\n", encoding="utf-8"
                    )
                    continue
                # сирий дамп
                (out_folder / f"metrics_{ts}.txt").write_text(text, encoding="utf-8")
                # парсимо ключові метрики
                htf = pres = bias = None
                ls_upper = ls_lower = None
                lst_upper = lst_lower = None
                scn_name = None
                scn_conf = None
                phase_name = None
                phase_score = None
                last_sym: str | None = None
                for name, labels, value in _iter_metrics(text):
                    sym = labels.get("symbol")
                    if sym:
                        last_sym = sym
                    if symbol and (sym or "").upper() != symbol:
                        continue
                    if name == "htf_strength":
                        htf = value
                    elif name == "presence":
                        pres = value
                    elif name == "ai_one_bias_state":
                        bias = value
                    elif name == "ai_one_liq_sweep":
                        side = labels.get("side", "")
                        if side == "upper":
                            ls_upper = value
                        elif side == "lower":
                            ls_lower = value
                    elif name == "ai_one_liq_sweep_total":
                        side = labels.get("side", "")
                        if side == "upper":
                            lst_upper = value
                        elif side == "lower":
                            lst_lower = value
                    elif name == "ai_one_scenario":
                        scn_name = labels.get("scenario", scn_name)
                        scn_conf = value
                    elif name == "ai_one_phase":
                        phase_name = labels.get("phase", phase_name)
                        phase_score = value
                row_symbol = symbol or (last_sym or "").upper()
                writer.writerow(
                    [
                        ts,
                        label,
                        row_symbol,
                        htf if htf is not None else "",
                        pres if pres is not None else "",
                        bias if bias is not None else "",
                        ls_upper if ls_upper is not None else "",
                        ls_lower if ls_lower is not None else "",
                        lst_upper if lst_upper is not None else "",
                        lst_lower if lst_lower is not None else "",
                        scn_name or "",
                        scn_conf if scn_conf is not None else "",
                        phase_name or "",
                        phase_score if phase_score is not None else "",
                    ]
                )
                f_csv.flush()
            time.sleep(max(0.1, float(interval_sec)))


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Parallel A/B /metrics monitor")
    ap.add_argument("--port-a", type=int, required=True)
    ap.add_argument("--port-b", type=int, required=True)
    ap.add_argument("--duration-sec", type=int, default=300)
    ap.add_argument("--interval-sec", type=float, default=10.0)
    ap.add_argument("--out-dir", type=str, default="ab_monitor")
    ap.add_argument("--symbol", type=str, default=None)
    args = ap.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        monitor_ab(
            args.port_a,
            args.port_b,
            args.duration_sec,
            args.interval_sec,
            out_dir,
            args.symbol,
        )
        print(f"Saved A/B metrics to {out_dir}")
        return 0
    except KeyboardInterrupt:
        print("Interrupted")
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
