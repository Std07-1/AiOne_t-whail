"""
Canary collector: snapshot Redis state and Prometheus metrics, summarize SCENARIO_TRACE,
and propose threshold tweaks (without changing code).

Outputs (in repo root):
  - state_BTCUSDT.txt, state_TONUSDT.txt, state_SNXUSDT.txt
  - metrics.txt, metrics_scenario.txt, metrics_htf.txt, metrics_phase.txt
  - scenario_trace_summary.txt
  - thresholds_proposal.txt
  - report_canary.txt

Usage (PowerShell):
  # Ensure app is running with PROM_GAUGES_ENABLED=true and canary flags
  # Then execute:
  python -m tools.canary_collect
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

try:
    from redis.asyncio import Redis
except Exception:  # pragma: no cover
    Redis = None  # type: ignore

try:
    from config import config as _cfg  # type: ignore
except Exception:  # pragma: no cover - дефолт при збоях
    _cfg = None


REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT  # write into repo root for easy discovery
LOG_FILE = REPO_ROOT / "logs" / "app.log"


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return float(default)


def _cfg_value(name: str, default: float | bool) -> float | bool:
    try:
        return getattr(_cfg, name)
    except Exception:
        return default


async def _redis_hgetall(r: Any, key: str) -> dict[str, Any]:
    try:
        res = await r.hgetall(key)
        return dict(res) if isinstance(res, dict) else {}
    except Exception:
        return {}


async def _redis_ttl(r: Any, key: str) -> int:
    try:
        ttl = await r.ttl(key)
        return int(ttl) if ttl is not None else -2
    except Exception:
        return -2


async def snapshot_state(symbols: list[str]) -> None:
    # Prefer settings from app
    try:
        from app.settings import settings  # type: ignore

        host, port = settings.redis_host, settings.redis_port
    except Exception:
        host, port = "localhost", 6379

    if Redis is None:
        print("redis-py not available; skipping Redis snapshots", file=sys.stderr)
        return

    r = Redis(host=host, port=port, decode_responses=True, encoding="utf-8")
    try:
        for sym in symbols:
            upper = sym.upper()
            lower = sym.lower()
            # ai_one:state:{symbol} — у коді ключі нижнім регістром; будемо читати обидва
            keys = [f"ai_one:state:{lower}", f"ai_one:state:{upper}"]
            data: dict[str, Any] = {}
            for k in keys:
                got = await _redis_hgetall(r, k)
                if got:
                    data = got
                    break
            out = OUT_DIR / f"state_{upper}.txt"
            with out.open("w", encoding="utf-8") as f:
                for k, v in sorted(data.items()):
                    f.write(f"{k}={v}\n")

        # Liveness TTLs (best-effort; keys may differ per env)
        ttl_out = OUT_DIR / "liveness_ttl.txt"
        with ttl_out.open("w", encoding="utf-8") as f:
            for sym in symbols:
                k = f"ai_one:bars:{sym}:M5"
                ttl = await _redis_ttl(r, k)
                f.write(f"{k} ttl={ttl}\n")
    finally:
        try:
            await r.close()
        except Exception:
            pass


def fetch_metrics(port: int = 9108) -> None:
    url = f"http://127.0.0.1:{port}/metrics"
    raw = b""
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:  # nosec - local call
            raw = resp.read()
    except Exception as exc:  # pragma: no cover
        print(f"WARN: metrics fetch failed: {exc}", file=sys.stderr)
    text = raw.decode("utf-8", errors="ignore")
    (OUT_DIR / "metrics.txt").write_text(text, encoding="utf-8")
    # filters
    lines = text.splitlines()
    (OUT_DIR / "metrics_scenario.txt").write_text(
        "\n".join([ln for ln in lines if "ai_one_scenario" in ln]), encoding="utf-8"
    )
    (OUT_DIR / "metrics_htf.txt").write_text(
        "\n".join([ln for ln in lines if "htf_strength" in ln]), encoding="utf-8"
    )
    (OUT_DIR / "metrics_phase.txt").write_text(
        "\n".join([ln for ln in lines if "ai_one_phase" in ln]), encoding="utf-8"
    )


def _parse_pred_dict(s: str) -> dict[str, Any]:
    # s looks like: pred={'near_edge': 'upper', 'dvr': 0.5, 'vol_z': 1.2, 'htf_strength': 0.18, 'presence': 0.42}
    try:
        # Replace single quotes with double quotes carefully
        js = re.sub(r"([{,]\s*)'([a-zA-Z0-9_]+)'\s*:", r'\1"\2":', s)
        js = (
            js.replace("'upper'", '"upper"')
            .replace("'lower'", '"lower"')
            .replace("'true'", '"true"')
        )
        js = js.replace("'near'", '"near"')
        js = js.replace("'", '"')
        obj = json.loads(js)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def summarize_trace() -> dict[str, float]:
    if not LOG_FILE.exists():
        (OUT_DIR / "scenario_trace_summary.txt").write_text(
            "no logs/app.log present", encoding="utf-8"
        )
        return {}
    # Read last ~1000 lines
    lines = LOG_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()
    tail = lines[-1000:]
    reject = []
    # category counts
    cnt = Counter()

    # thresholds (current from config)
    try:
        htf_min = float(_cfg_value("SCEN_HTF_MIN", 0.20))
    except Exception:
        htf_min = 0.20
    try:
        pres_min = float(_cfg_value("SCEN_PULLBACK_PRESENCE_MIN", 0.60))
    except Exception:
        pres_min = 0.60
    try:
        dvr_min = float(_cfg_value("SCEN_BREAKOUT_DVR_MIN", 0.50))
    except Exception:
        dvr_min = 0.50

    for ln in tail:
        if "[SCENARIO_TRACE]" not in ln:
            continue
        # Cheap parsing
        # example: [SCENARIO_TRACE] symbol=xxx candidate=pullback_continuation pred={...} decision=REJECT reason=phase=momentum conf=0.21
        try:
            decision = "ACCEPT" if " decision=ACCEPT" in ln else "REJECT"
            if decision != "REJECT":
                continue
            # candidate
            m_c = re.search(r"candidate=([a-z_\-]+|-)\b", ln)
            cand = m_c.group(1) if m_c else "-"
            # pred json-ish
            m_p = re.search(r"pred=(\{.*?\})", ln)
            pred = _parse_pred_dict(m_p.group(1)) if m_p else {}
            phase = None
            m_r = re.search(r"reason=([^\s]+)", ln)
            if m_r:
                phase = m_r.group(1)
            # derive categories
            htf = float(pred.get("htf_strength") or 0.0)
            pres = float(pred.get("presence") or 0.0)
            dvr = pred.get("dvr")
            if isinstance(dvr, (int, float)):
                dvrf = float(dvr)
            else:
                try:
                    dvrf = float(dvr)
                except Exception:
                    dvrf = 0.0

            if phase and "accum_monitor" in phase:
                cnt["phase=accum_monitor"] += 1
            elif htf < htf_min:
                cnt["htf_min"] += 1
            elif pres < pres_min:
                cnt["presence_min"] += 1
            elif cand == "breakout_confirmation" and (dvrf or 0.0) < dvr_min:
                cnt["dvr_min"] += 1
            else:
                cnt["misc"] += 1
            reject.append(ln)
        except Exception:
            continue

    total = sum(cnt.values()) or 1
    summary_lines = ["category,count,percent"]
    for k, v in cnt.most_common():
        pct = 100.0 * v / total
        summary_lines.append(f"{k},{v},{pct:.1f}")
    (OUT_DIR / "scenario_trace_summary.txt").write_text(
        "\n".join(summary_lines), encoding="utf-8"
    )
    return {k: (100.0 * v / total) for k, v in cnt.items()}


def propose_thresholds(pcts: dict[str, float]) -> None:
    out = []
    # Rules
    if pcts.get("htf_min", 0.0) > 50.0:
        out.append(
            "Edit config/config.py: set SCEN_HTF_MIN = 0.04    # >50% rejects due to HTF; propose softer gate"
        )
    if pcts.get("presence_min", 0.0) > 50.0:
        out.append(
            "Edit config/config.py: set SCEN_PULLBACK_PRESENCE_MIN = 0.45    # >50% rejects due to presence; try 0.45 (or 0.30 if still too strict)"
        )
    if pcts.get("dvr_min", 0.0) > 50.0:
        out.append(
            "Edit config/config.py: set SCEN_BREAKOUT_DVR_MIN = 0.30    # >50% rejects due to DVR; propose softer breakout DVR gate"
        )
    if not out:
        out.append("# No dominant single cause (>50%). Keep thresholds as-is for now.")
    (OUT_DIR / "thresholds_proposal.txt").write_text("\n".join(out), encoding="utf-8")


def build_report(symbols: list[str]) -> None:
    # metrics_scenario presence
    ms = (OUT_DIR / "metrics_scenario.txt").read_text(encoding="utf-8", errors="ignore")
    has_non_none = any(
        "ai_one_scenario" in ln and 'scenario="none"' not in ln
        for ln in ms.splitlines()
    )
    line_example = next(
        (
            ln
            for ln in ms.splitlines()
            if 'scenario="none"' not in ln and "ai_one_scenario" in ln
        ),
        "",
    )

    # state fields presence
    state_stats = []
    for sym in symbols:
        up = sym.upper()
        text = (OUT_DIR / f"state_{up}.txt").read_text(
            encoding="utf-8", errors="ignore"
        )
        has_detected = "scenario_detected=" in text
        has_conf = "scenario_confidence=" in text
        state_stats.append((up, has_detected and has_conf))

    # top-3 reasons
    summary_text = (OUT_DIR / "scenario_trace_summary.txt").read_text(
        encoding="utf-8", errors="ignore"
    )
    lines = [
        ln for ln in summary_text.splitlines() if ln and not ln.startswith("category,")
    ]
    top3 = lines[:3]

    proposals = (OUT_DIR / "thresholds_proposal.txt").read_text(
        encoding="utf-8", errors="ignore"
    )

    report = []
    report.append("Canary Report (scenario selector)\n")
    report.append(
        f"ai_one_scenario (scenario!=none) present: {'YES' if has_non_none else 'NO'}"
    )
    if line_example:
        report.append(f"example: {line_example}")
    report.append("")
    for sym, ok in state_stats:
        report.append(
            f"state_{sym}.txt has scenario_detected/confidence: {'YES' if ok else 'NO'}"
        )
    report.append("")
    report.append("Top REJECT reasons:")
    report.extend(top3 or ["(no data)"])
    report.append("")
    report.append("Proposed threshold tweaks (suggestions only):")
    report.append(proposals or "(none)")
    (OUT_DIR / "report_canary.txt").write_text("\n".join(report), encoding="utf-8")


async def main() -> None:
    symbols = ["BTCUSDT", "TONUSDT", "SNXUSDT"]

    # 1) Redis state snapshots (+ liveness TTLs)
    await snapshot_state(symbols)

    # 2) /metrics snapshot & filtered files
    port = int(os.environ.get("PROM_HTTP_PORT", "9108"))
    fetch_metrics(port)

    # 3) Summarize SCENARIO_TRACE and build proposals/report
    pcts = summarize_trace()
    propose_thresholds(pcts)
    build_report(symbols)


if __name__ == "__main__":
    asyncio.run(main())
