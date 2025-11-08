"""SHIM: tools.night_bundle → tools.unified_runner live

Цей файл позначено до видалення. Будь ласка, використовуйте:
    python -m tools.unified_runner live --duration ... --namespace ... --prom-port ... --out-dir ... --report
"""

from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> int:
    print(
        "[WARNING] tools.night_bundle застарілий — використовуйте tools.unified_runner live --report",
        flush=True,
    )
    ap = argparse.ArgumentParser(
        description="Shim for night_bundle → unified_runner live --report"
    )
    ap.add_argument("--duration", type=int, default=28800)
    ap.add_argument("--prom-port", type=int, default=9125)
    ap.add_argument("--namespace", default="ai_one_night")
    ap.add_argument("--out-dir", default="reports/night_pack")
    args, rest = ap.parse_known_args()

    cmd = [
        sys.executable,
        "-m",
        "tools.unified_runner",
        "live",
        "--duration",
        str(int(args.duration)),
        "--prom-port",
        str(int(args.prom_port)),
        "--namespace",
        str(args.namespace),
        "--out-dir",
        str(args.out_dir),
        "--report",
        "--set",
        "PROM_GAUGES_ENABLED=true",
    ]
    try:
        res = subprocess.run(cmd + rest)
        return int(res.returncode or 0)
    except Exception as e:
        print(f"[ERROR] Не вдалося делегувати у unified_runner: {e}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
