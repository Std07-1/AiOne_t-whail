"""SHIM: tools.bench_pseudostream → tools.unified_runner replay

Цей файл позначено до видалення. Будь ласка, використовуйте:
    python -m tools.unified_runner replay ...
"""

from __future__ import annotations

import argparse
import subprocess
import sys


def main(argv: list[str] | None = None) -> int:
    print(
        "[WARNING] tools.bench_pseudostream застарілий — використовуйте tools.unified_runner replay",
        flush=True,
    )
    p = argparse.ArgumentParser(
        description="Shim for bench_pseudostream → unified_runner replay"
    )
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT,TRXUSDT")
    p.add_argument("--limit", type=int, default=300)
    p.add_argument("--interval", default="1m")
    p.add_argument("--out-dir", default="./replay_bench")
    p.add_argument("--source", default="snapshot", choices=["snapshot", "binance"])
    args, rest = p.parse_known_args(argv)

    cmd = [
        sys.executable,
        "-m",
        "tools.unified_runner",
        "replay",
        "--limit",
        str(int(args.limit)),
        "--symbols",
        str(args.symbols),
        "--interval",
        str(args.interval),
        "--source",
        str(args.source),
        "--out-dir",
        str(args.out_dir),
        "--report",
    ]
    try:
        res = subprocess.run(cmd + rest)
        return int(res.returncode or 0)
    except Exception as e:
        print(f"[ERROR] Не вдалося делегувати у unified_runner: {e}", flush=True)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
