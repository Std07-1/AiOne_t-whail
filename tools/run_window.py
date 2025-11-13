"""SHIM: tools.run_window → tools.unified_runner live.

Застаріла команда run_window делегує у tools.unified_runner live з мінімальним
перетворенням аргументів.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Sequence


def _extract_sets(
    values: Sequence[str] | None,
) -> tuple[str | None, str | None, list[str]]:
    namespace = None
    prom_port = None
    passthrough: list[str] = []
    for raw in values or []:
        item = raw.strip()
        key_raw, sep, val_raw = item.partition("=")
        key_clean = key_raw.strip()
        val_clean = val_raw.strip()
        if not key_clean:
            continue
        key_upper = key_clean.upper()
        if key_upper == "STATE_NAMESPACE":
            namespace = val_clean or namespace
            continue
        if key_upper == "PROM_HTTP_PORT":
            prom_port = val_clean or prom_port
            continue
        passthrough.append(f"{key_clean}={val_clean}" if sep else item)
    return namespace, prom_port, passthrough


def main(argv: list[str] | None = None) -> int:
    print(
        "[WARNING] tools.run_window застарілий — використовуйте tools.unified_runner live",
        flush=True,
    )
    parser = argparse.ArgumentParser(
        description="Shim for run_window → unified_runner live"
    )
    parser.add_argument("--duration", type=int, required=True)
    parser.add_argument("--log", default="reports/run_A.log")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--set", action="append", default=[])
    args, rest = parser.parse_known_args(argv)

    namespace, prom_port, passthrough_sets = _extract_sets(args.set)

    out_dir = Path(args.out_dir) if args.out_dir else Path(args.log).resolve().parent
    cmd: list[str] = [
        sys.executable,
        "-m",
        "tools.unified_runner",
        "live",
        "--duration",
        str(int(args.duration)),
        "--out-dir",
        str(out_dir),
        "--report",
    ]
    if namespace:
        cmd.extend(["--namespace", namespace])
    if prom_port:
        cmd.extend(["--prom-port", prom_port])
    for set_arg in passthrough_sets:
        cmd.extend(["--set", set_arg])

    try:
        completed = subprocess.run(cmd + rest)
        return int(completed.returncode or 0)
    except Exception as exc:  # pragma: no cover
        print(
            f"[ERROR] Не вдалося делегувати у unified_runner live: {exc}",
            flush=True,
        )
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
