from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Агрегований Markdown-дашборд по сценаріях із reports/quality.csv"
    )
    ap.add_argument("--csv", default="reports/quality.csv", help="Шлях до quality.csv")
    ap.add_argument(
        "--out", default="reports/quality_dashboard.md", help="Шлях для Markdown-виходу"
    )
    ap.add_argument(
        "--forward-k",
        default="3,5,10",
        help="Опціонально: додати секцію Forward‑K по символах (перелік K через кому)",
    )
    return ap.parse_args()


def _normalize_reports_path(p: Path) -> Path:
    """Якщо вказано лише ім'я файлу без каталогу — зберігаємо у reports/."""
    try:
        if not p.is_absolute() and (p.parent == Path(".")):
            return Path("reports") / p.name
    except Exception:
        pass
    return p


def load_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh)
        return [dict(r) for r in rd]


def _to_int(v: Any) -> int:
    try:
        return int(v) if v not in (None, "") else 0
    except Exception:
        return 0


def _to_float(v: Any) -> float:
    try:
        return float(v) if v not in (None, "") else 0.0
    except Exception:
        return 0.0


def aggregate_by_scenario(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Агрегуємо по назві сценарію з вагами за activations."""
    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "activations": 0,
            "p75_conf_w": 0.0,
            "tts_w": 0.0,
            "cov_w": 0.0,
            "top_reject": {},  # name -> weighted count
        }
    )
    for r in rows:
        scen = str(r.get("scenario", ""))
        acts = _to_int(r.get("activations"))
        b = buckets[scen]
        b["activations"] += acts
        b["p75_conf_w"] += acts * _to_float(r.get("p75_conf"))
        b["tts_w"] += acts * _to_float(r.get("mean_time_to_stabilize_s"))
        b["cov_w"] += acts * _to_float(r.get("explain_coverage_rate"))
        # зберемо найчастішу причину з reject_top1 (обмежимо простотою)
        name = str(r.get("reject_top1", "") or "").strip()
        if name:
            b["top_reject"][name] = b["top_reject"].get(name, 0) + acts
    out: list[dict[str, Any]] = []
    for scen, b in buckets.items():
        acts = max(1, int(b["activations"]))
        tr = "-"
        if b["top_reject"]:
            tr = max(b["top_reject"].items(), key=lambda kv: kv[1])[0]
        out.append(
            {
                "scenario": scen,
                "activations": int(b["activations"]),
                "p75_conf": round(b["p75_conf_w"] / acts, 4),
                "mean_time_to_stabilize_s": round(b["tts_w"] / acts, 2),
                "top_reject": tr,
                "explain_coverage_rate": round(b["cov_w"] / acts, 4),
            }
        )
    # відсортуємо за activations ↓
    out.sort(key=lambda r: (-int(r.get("activations", 0)), r.get("scenario", "")))
    return out


def render_md(agg: list[dict[str, Any]]) -> str:
    header = "| Scenario | Activations | P75 | MeanTTS(s) | TopReject | ExpCov |\n"
    sep = "|---|---:|---:|---:|---|---:|\n"
    lines = [header, sep]
    for r in agg:
        line = "| {scn} | {acts} | {p75:.2f} | {tts:.2f} | {tr} | {cov:.2f} |\n".format(
            scn=str(r.get("scenario", "")),
            acts=int(_to_int(r.get("activations"))),
            p75=float(_to_float(r.get("p75_conf"))),
            tts=float(_to_float(r.get("mean_time_to_stabilize_s"))),
            tr=str(r.get("top_reject", "-")),
            cov=float(_to_float(r.get("explain_coverage_rate"))),
        )
        lines.append(line)
    return "".join(lines)


# ── Forward eval (спрощена версія з quality_snapshot) ─────────────────────

DECISION_TRACE_DIR = Path("telemetry/decision_traces")
DATASTORE_DIR = Path("datastore")
SYMBOLS = ("BTCUSDT", "ETHUSDT", "TONUSDT", "SNXUSDT")


def _load_bars(symbol: str) -> list[tuple[int, float]]:
    fname = f"{symbol.lower()}_bars_1m_snapshot.jsonl"
    p = DATASTORE_DIR / fname
    out: list[tuple[int, float]] = []
    if not p.exists():
        return out
    with p.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            try:
                obj = json.loads(line)
                ot = int(obj.get("open_time"))
                cp = float(obj.get("close"))
                out.append((ot, cp))
            except Exception:
                continue
    return out


def _forward_ret_for_ts(
    bars: list[tuple[int, float]], ts_ms: int, k: int
) -> float | None:
    if not bars or k <= 0:
        return None
    idx = None
    for i in range(len(bars)):
        ot = bars[i][0]
        if ot <= ts_ms < ot + 60_000:
            idx = i
            break
    if idx is None:
        return None
    j = idx + k
    if j >= len(bars):
        return None
    c0 = bars[idx][1]
    ck = bars[j][1]
    if c0 <= 0:
        return None
    return (ck / c0 - 1.0) * 100.0


def _forward_eval(
    symbol: str, ks: list[int], max_events: int = 64
) -> dict[int, dict[str, float]]:
    bars = _load_bars(symbol)
    if not bars:
        return {k: {"n": 0, "hit_cd": 0.0, "med_ret": 0.0, "p75_abs": 0.0} for k in ks}
    trace_path = DECISION_TRACE_DIR / f"{symbol.lower()}.jsonl"
    rows: list[dict[str, Any]] = []
    if trace_path.exists():
        with trace_path.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                try:
                    obj = json.loads(line)
                    rows.append(obj)
                except Exception:
                    continue
    rows = rows[-max_events:] if rows else []
    per_k: dict[int, list[tuple[float, float]]] = {k: [] for k in ks}
    for r in rows:
        ts = int(r.get("ts", 0))
        try:
            cd = float(r.get("cd", 0.0))
        except Exception:
            cd = 0.0
        for k in ks:
            ret = _forward_ret_for_ts(bars, ts, k)
            if ret is None:
                continue
            per_k[k].append((float(ret), cd))
    out: dict[int, dict[str, float]] = {}
    for k in ks:
        vals = per_k.get(k, [])
        n = len(vals)
        if n == 0:
            out[k] = {"n": 0, "hit_cd": 0.0, "med_ret": 0.0, "p75_abs": 0.0}
            continue
        rets = [v for v, _ in vals]
        hits = 0
        denom = 0
        for ret, c in vals:
            if abs(c) < 1e-9:
                continue
            denom += 1
            if (ret > 0 and c > 0) or (ret < 0 and c < 0):
                hits += 1
        hit_rate = (hits / denom) if denom > 0 else 0.0
        rets_sorted = sorted(rets)
        mid = (
            rets_sorted[n // 2]
            if n % 2 == 1
            else (rets_sorted[n // 2 - 1] + rets_sorted[n // 2]) / 2
        )
        abs_sorted = sorted(abs(x) for x in rets)
        p75_idx = max(0, min(n - 1, int(0.75 * (n - 1))))
        p75_abs = abs_sorted[p75_idx]
        out[k] = {
            "n": float(n),
            "hit_cd": float(hit_rate),
            "med_ret": float(mid),
            "p75_abs": float(p75_abs),
        }
    return out


def render_forward_section(ks: list[int]) -> str:
    # Формуємо додаткову секцію з forward‑метриками по символах
    lines: list[str] = []
    lines.append("\n## Forward K‑bars (symbol summary)\n\n")
    header = "Symbol | K | N | hit_rate(cd·ret>0) | med_ret% | p75_abs%\n"
    sep = "---|---:|---:|---:|---:|---:\n"
    lines.append(header)
    lines.append(sep)
    for sym in SYMBOLS:
        stats = _forward_eval(sym, ks)
        for k in ks:
            s = stats.get(k, {"n": 0, "hit_cd": 0.0, "med_ret": 0.0, "p75_abs": 0.0})
            lines.append(
                f"{sym} | {k} | {int(s['n'] or 0)} | {float(s['hit_cd'] or 0.0):.2f} | {float(s['med_ret'] or 0.0):.2f} | {float(s['p75_abs'] or 0.0):.2f}\n"
            )
    return "".join(lines)


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    out_path = _normalize_reports_path(Path(args.out))

    rows = load_rows(csv_path)
    if not rows:
        # Створимо порожній, але валідний файл з заголовком
        md = render_md([])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(md, encoding="utf-8")
        print("(quality.csv не знайдено або порожній)")
        return 0

    agg = aggregate_by_scenario(rows)
    md = render_md(agg)
    # Додатково: якщо задано --forward-k, додамо forward‑секцію для K (напр., 3,5,10,20,30)
    ks: list[int] = []
    try:
        ks = [int(x.strip()) for x in str(args.forward_k).split(",") if x.strip()]
        ks = [k for k in ks if k > 0]
    except Exception:
        ks = []
    if ks:
        md += render_forward_section(ks)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"Markdown дашборд збережено у: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
