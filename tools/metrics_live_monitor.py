import argparse
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta

P95_RE = re.compile(
    r'ai_one_stage1_latency_ms\{[^}]*quantile="0\.95"[^}]*\}\s+([0-9.]+)'
)
# We aggregate per-symbol labeled counters (e.g. ai_one_explain_lines_total{symbol="BTCUSDT"} 42)


def fetch_metrics_text(url: str, timeout: float = 5.0) -> str | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = resp.read()
            try:
                return data.decode("utf-8", errors="replace")
            except Exception:
                return data.decode("latin-1", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        return None


def parse_value(regex: re.Pattern, text: str) -> float | None:
    m = regex.search(text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def aggregate_labeled_sum(text: str, metric_name: str) -> float | None:
    total = 0.0
    found = False
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        if line.startswith(metric_name):
            # Expect either metric_name{...} value OR metric_name value
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                val = float(parts[-1])
            except ValueError:
                continue
            total += val
            found = True
    return total if found else None


def parse_histogram_p95_from_buckets(
    text: str, metric_prefix: str = "ai_one_stage1_latency_ms"
) -> float | None:
    """Compute approximate p95 from Prometheus histogram buckets.

    Expects lines like:
        {prefix}_bucket{le="25.0"} 10
        ...
        {prefix}_bucket{le="+Inf"} N
        {prefix}_count N
        {prefix}_sum S

    Returns the lowest 'le' value (float) where cumulative_count/total_count >= 0.95.
    If parsing fails or counts are missing, returns None.
    """
    total_count: float | None = None
    buckets: list[tuple[float | float, float]] = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        if line.startswith(f"{metric_prefix}_count"):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    total_count = float(parts[-1])
                except ValueError:
                    pass
        elif line.startswith(f"{metric_prefix}_bucket"):
            # Extract le value and bucket cumulative count
            try:
                # naive parse of le="..."
                le_match = re.search(r'le="([^"]+)"', line)
                parts = line.split()
                if le_match and len(parts) >= 2:
                    le_raw = le_match.group(1)
                    le_val: float
                    if le_raw == "+Inf":
                        le_val = float("inf")
                    else:
                        le_val = float(le_raw)
                    count_val = float(parts[-1])
                    buckets.append((le_val, count_val))
            except Exception:
                continue

    if not buckets or total_count is None or total_count <= 0:
        return None
    # Sort buckets by upper bound just in case
    buckets.sort(key=lambda x: x[0])
    threshold = 0.95 * total_count
    for le_val, cum_count in buckets:
        if cum_count >= threshold:
            # Return finite numeric value; for +Inf, we can't provide numeric p95
            if le_val == float("inf"):
                # Try to return the last finite bucket if available
                finite = [b for b in buckets if b[0] != float("inf")]
                return finite[-1][0] if finite else None
            return le_val
    return None


def ensure_parent(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def write_header_if_missing(path: str, header: str) -> None:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        ensure_parent(path)
        with open(path, "a", encoding="utf-8", newline="") as f:
            f.write(header + "\n")
            f.flush()


def format_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def compute_rate(
    prev: tuple[float, float] | None, cur_val: float | None, now: float
) -> tuple[tuple[float, float] | None, float | None]:
    """
    prev: (value, timestamp_sec)
    cur_val: current absolute counter value (can be None)
    now: current timestamp in seconds

    Returns: (new_prev, rate_per_min)
    """
    if cur_val is None:
        return prev, None
    if prev is None:
        return (cur_val, now), None
    prev_val, prev_ts = prev
    delta_v = cur_val - prev_val
    delta_t_min = max(1e-6, (now - prev_ts) / 60.0)
    # Handle counter reset
    if delta_v < 0:
        # reset baseline
        return (cur_val, now), None
    return (cur_val, now), (delta_v / delta_t_min)


class FiveMinuteWindow:
    def __init__(self, window_seconds: int = 300) -> None:
        self.window_seconds = window_seconds
        self.reset(None)

    def reset(self, start: datetime | None) -> None:
        self.start: datetime | None = start
        self.end: datetime | None = None
        self.samples: list[
            tuple[datetime, float | None, float | None, float | None]
        ] = []

    def add_sample(
        self,
        ts: datetime,
        p95_ms: float | None,
        switch_rate: float | None,
        explain_rate: float | None,
    ) -> None:
        if self.start is None:
            self.start = ts
        self.end = ts
        self.samples.append((ts, p95_ms, switch_rate, explain_rate))

    def should_flush(self, now: datetime) -> bool:
        if self.start is None:
            return False
        return (now - self.start) >= timedelta(seconds=self.window_seconds)

    def flush(
        self,
    ) -> (
        tuple[
            datetime,
            datetime,
            int,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
        ]
        | None
    ):
        if not self.samples or self.start is None or self.end is None:
            self.reset(None)
            return None
        n = len(self.samples)
        p95_vals = [v for (_, v, _, _) in self.samples if v is not None]
        sw_vals = [v for (_, _, v, _) in self.samples if v is not None]
        ex_vals = [v for (_, _, _, v) in self.samples if v is not None]

        def mean(xs: list[float]) -> float | None:
            return sum(xs) / len(xs) if xs else None

        def vmax(xs: list[float]) -> float | None:
            return max(xs) if xs else None

        result = (
            self.start,
            self.end,
            n,
            mean(p95_vals),
            vmax(p95_vals),
            mean(sw_vals),
            vmax(sw_vals),
            mean(ex_vals),
            vmax(ex_vals),
        )
        self.reset(None)
        return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Live monitor for /metrics: per-minute CSV + 5-minute aggregates"
    )
    parser.add_argument("--port", type=int, help="Prometheus HTTP port (e.g., 9124)")
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Full metrics URL (overrides --port), e.g., http://localhost:9124/metrics",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Polling interval in seconds (default: 60)",
    )
    parser.add_argument(
        "--out-csv", type=str, required=True, help="Path to write per-minute CSV"
    )
    parser.add_argument(
        "--summary-csv",
        type=str,
        required=True,
        help="Path to write 5-minute aggregate CSV",
    )
    args = parser.parse_args()

    url = args.url or f"http://localhost:{args.port}/metrics"

    write_header_if_missing(
        args.out_csv, "ts,p95_ms,switch_rate_per_min,explain_rate_per_min"
    )
    write_header_if_missing(
        args.summary_csv,
        "window_start,window_end,n_samples,p95_avg_ms,p95_max_ms,switch_rate_mean,switch_rate_max,explain_rate_mean,explain_rate_max",
    )

    prev_switch: tuple[float, float] | None = None
    prev_explain: tuple[float, float] | None = None

    window = FiveMinuteWindow(window_seconds=300)

    print(
        f"[monitor] polling {url} every {args.interval_seconds}s; writing {args.out_csv} and {args.summary_csv}"
    )
    while True:
        loop_start = time.time()
        ts = datetime.utcnow()

        text = fetch_metrics_text(url, timeout=5.0)
        p95_ms: float | None = None
        cur_switch: float | None = None
        cur_explain: float | None = None

        if text is not None:
            # Try direct quantile first
            p95_ms = parse_value(P95_RE, text)
            # Fallback to histogram buckets if quantiles are not exposed
            if p95_ms is None:
                p95_ms = parse_histogram_p95_from_buckets(text)
            cur_switch = aggregate_labeled_sum(text, "ai_one_profile_switch_total")
            cur_explain = aggregate_labeled_sum(text, "ai_one_explain_lines_total")

        # Treat absent counters as 0 to get explicit 0/min rates
        if cur_switch is None:
            cur_switch = 0.0
        if cur_explain is None:
            cur_explain = 0.0

        now_sec = time.time()
        prev_switch, switch_rate = compute_rate(prev_switch, cur_switch, now_sec)
        prev_explain, explain_rate = compute_rate(prev_explain, cur_explain, now_sec)

        # Write per-minute CSV
        with open(args.out_csv, "a", encoding="utf-8", newline="") as f:
            row = [
                format_ts(ts),
                ("" if p95_ms is None else f"{p95_ms:.3f}"),
                ("" if switch_rate is None else f"{switch_rate:.3f}"),
                ("" if explain_rate is None else f"{explain_rate:.3f}"),
            ]
            f.write(",".join(row) + "\n")
            f.flush()

        # Update window and flush every ~5 minutes
        window.add_sample(ts, p95_ms, switch_rate, explain_rate)
        if window.should_flush(ts):
            agg = window.flush()
            if agg is not None:
                (
                    w_start,
                    w_end,
                    n_s,
                    p95_avg,
                    p95_max,
                    sw_mean,
                    sw_max,
                    ex_mean,
                    ex_max,
                ) = agg
                with open(args.summary_csv, "a", encoding="utf-8", newline="") as sf:
                    row = [
                        format_ts(w_start),
                        format_ts(w_end),
                        str(n_s),
                        ("" if p95_avg is None else f"{p95_avg:.3f}"),
                        ("" if p95_max is None else f"{p95_max:.3f}"),
                        ("" if sw_mean is None else f"{sw_mean:.3f}"),
                        ("" if sw_max is None else f"{sw_max:.3f}"),
                        ("" if ex_mean is None else f"{ex_mean:.3f}"),
                        ("" if ex_max is None else f"{ex_max:.3f}"),
                    ]
                    sf.write(",".join(row) + "\n")
                    sf.flush()

        # Sleep remaining interval
        elapsed = time.time() - loop_start
        to_sleep = max(0.0, args.interval_seconds - elapsed)
        try:
            time.sleep(to_sleep)
        except KeyboardInterrupt:
            print("[monitor] interrupted; exiting.")
            return 0


if __name__ == "__main__":
    sys.exit(main())
