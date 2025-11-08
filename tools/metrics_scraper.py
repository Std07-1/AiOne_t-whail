from __future__ import annotations

import argparse
import threading
import time
import urllib.request
from pathlib import Path


def _append_metrics(
    url: str, out_path: Path, stop_flag: threading.Event, interval_s: int
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    while not stop_flag.is_set():
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = resp.read().decode("utf-8", errors="ignore")
                with out_path.open("a", encoding="utf-8") as fh:
                    fh.write(data)
                    if not data.endswith("\n"):
                        fh.write("\n")
        except Exception:
            pass
        stop_flag.wait(interval_s)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Best-effort scrape /metrics to a file for a duration"
    )
    ap.add_argument("--url", default="http://localhost:9128/metrics")
    ap.add_argument("--out", default="reports/canary_pack/metrics_canary.txt")
    ap.add_argument("--duration", type=int, default=5400)
    ap.add_argument("--interval", type=int, default=15)
    args = ap.parse_args()

    out = Path(args.out)
    stop = threading.Event()
    thr = threading.Thread(
        target=_append_metrics,
        args=(args.url, out, stop, int(args.interval)),
        daemon=True,
    )
    thr.start()
    try:
        time.sleep(max(1, int(args.duration)))
    finally:
        stop.set()
        thr.join(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
