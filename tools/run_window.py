#!/usr/bin/env python3
"""SHIM: tools.run_window → tools.unified_runner live

Цей файл позначено до видалення. Будь ласка, використовуйте:
  python -m tools.unified_runner live ...
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _parse_sets(values: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for raw in values or []:
        if "=" not in raw:
            raise SystemExit(f"--set expects NAME=VALUE, got: {raw}")
        k, v = raw.split("=", 1)
        out.append((k.strip(), v.strip()))
    return out


def main() -> int:
    print(
        "[WARNING] tools.run_window застарілий — використовуйте tools.unified_runner live",
        flush=True,
    )
    ap = argparse.ArgumentParser(
        description="Shim for run_window → unified_runner live"
    )
    ap.add_argument("--duration", type=int, required=True)
    ap.add_argument("--log", default=None)
    ap.add_argument("--set", action="append", default=[])
    args, rest = ap.parse_known_args()

    sets = _parse_sets(args.set)
    ns = next((v for k, v in sets if k == "STATE_NAMESPACE"), "ai_one")
    prom_port = next((v for k, v in sets if k == "PROM_HTTP_PORT"), None)
    out_dir = None
    if args.log:
        lp = Path(args.log)
        out_dir = (
            lp.parent if lp.parent.name else Path("reports/run_window")
        ).resolve()
    else:
        out_dir = Path("reports/run_window").resolve()

    cmd = [
        sys.executable,
        "-m",
        "tools.unified_runner",
        "live",
        "--duration",
        str(int(args.duration)),
        "--namespace",
        ns,
        "--out-dir",
        str(out_dir),
        "--report",
    ]
    if prom_port:
        cmd += ["--prom-port", str(prom_port)]
    for k, v in sets:
        cmd += ["--set", f"{k}={v}"]
    # Попереджуємо про ігнорування --log: unified_runner пише у out_dir/run.log
    if args.log:
        print("[INFO] --log ігнорується шімом; див. out_dir/run.log", flush=True)
    try:
        res = subprocess.run(cmd + rest)
        return int(res.returncode or 0)
    except Exception as e:
        print(f"[ERROR] Не вдалося делегувати у unified_runner: {e}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
