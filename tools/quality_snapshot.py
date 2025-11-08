from __future__ import annotations

import argparse
import csv
import glob
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

SYMBOLS = ("BTCUSDT", "ETHUSDT", "TONUSDT", "SNXUSDT")
DECISION_TRACE_DIR = Path("telemetry/decision_traces")
DATASTORE_DIR = Path("datastore")


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Quality Snapshot — короткий MD-зріз")
    ap.add_argument(
        "--csv", default="reports/quality.csv", help="Шлях до reports/quality.csv"
    )
    ap.add_argument("--metrics-dir", default="reports", help="Каталог із metrics_*.txt")
    ap.add_argument(
        "--out", default="reports/quality_snapshot.md", help="Вихідний Markdown файл"
    )
    ap.add_argument(
        "--forward-k",
        default="3,5,10",
        help="K (або перелік через кому) барів для forward‑оцінки hit‑rate/PNL‑proxy",
    )
    ap.add_argument(
        "--forward-enable-long",
        default="false",
        help="Опціональний прапор: якщо true, у CLI дозволено задавати довгі горизонти (напр., 20,30)",
    )
    return ap.parse_args()


def _normalize_reports_path(p: Path) -> Path:
    """Якщо вказано лише ім'я файлу без каталогу — зберігаємо у reports/.

    Абсолютні шляхи або ті, що вже містять каталог, не змінюємо.
    """
    try:
        if not p.is_absolute() and (p.parent == Path(".")):
            return Path("reports") / p.name
    except Exception:
        pass
    return p


def _load_quality_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh)
        return [dict(r) for r in rd]


def _pick_latest_metrics(metrics_dir: Path) -> Path | None:
    files = sorted(
        (Path(p) for p in glob.glob(str(metrics_dir / "metrics_*.txt"))),
        key=lambda p: p.stat().st_mtime if p.exists() else 0.0,
        reverse=True,
    )
    return files[0] if files else None


def _extract_summary_from_metrics(p: Path | None) -> str:
    if not p:
        return "(metrics snapshot: немає файлів)"
    try:
        ts = datetime.utcfromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        return f"metrics: {p.name}@{ts}Z"
    except Exception:
        return f"metrics: {p.name}"


def _parse_explain_lines_from_metrics(text: str, symbol: str) -> int:
    """Повертає значення ai_one_explain_lines_total для символу з тексту метрик.

    Якщо метрика відсутня або не парситься — повертає 0.
    """
    sym = symbol.upper()
    rg = re.compile(
        rf"^ai_one_explain_lines_total\{{[^}}]*symbol=\"{re.escape(sym)}\"[^}}]*\}}\s+([0-9eE+\-.]+)$",
        re.M,
    )
    m = rg.search(text)
    if not m:
        return 0
    try:
        v = float(m.group(1))
        return int(v) if v > 0 else 0
    except Exception:
        return 0


def _has_explain_in_logs(log_path: Path, symbol: str) -> bool:
    """Швидка перевірка наявності [SCEN_EXPLAIN] у ./logs/app.log для символу.

    Best‑effort: читаємо файл повністю (текстовий), шукаємо рядок із тегом
    та іменем символу. У разі помилки повертаємо False.
    """
    try:
        if not log_path.exists():
            return False
        text = log_path.read_text(encoding="utf-8", errors="ignore")
        sym = symbol.lower()
        return ("[SCEN_EXPLAIN]" in text) and (f"symbol={sym}" in text)
    except Exception:
        return False


def _fmt_float(v: Any, nd: int = 2) -> str:
    try:
        return f"{float(v):.{nd}f}"
    except Exception:
        return "0.00"


def _row_for(rows: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    # беремо перший збіг за символом (агрегація вже зроблена у quality_report)
    for r in rows:
        if str(r.get("symbol", "")).upper() == symbol.upper():
            return r
    return None


def _parse_metrics_context(text: str, symbol: str) -> dict[str, Any]:
    """Витягнути htf_strength, presence і найвищу фазу з останнього metrics-файла."""
    sym = symbol.upper()
    out: dict[str, Any] = {
        "htf_strength": None,
        "presence": None,
        "phase": None,
        "phase_score": None,
        "scenario": None,
        "scenario_conf": None,
        "bias_state": None,
        "btc_regime": None,
        "ctx_near_edge_persist": None,
        "ctx_presence_sustain": None,
    }
    # htf_strength{symbol="SYM"} 0.12
    rg_htf = re.compile(
        rf"^htf_strength\{{[^}}]*symbol=\"{re.escape(sym)}\"[^}}]*\}}\s+([0-9eE+\-.]+)$",
        re.M,
    )
    m = rg_htf.search(text)
    if m:
        try:
            out["htf_strength"] = float(m.group(1))
        except Exception:
            out["htf_strength"] = None
    # presence{symbol="SYM"} 0.34
    rg_pres = re.compile(
        rf"^presence\{{[^}}]*symbol=\"{re.escape(sym)}\"[^}}]*\}}\s+([0-9eE+\-.]+)$",
        re.M,
    )
    m = rg_pres.search(text)
    if m:
        try:
            out["presence"] = float(m.group(1))
        except Exception:
            out["presence"] = None
    # ai_one_phase{symbol="SYM",phase="X"} score
    rg_phase = re.compile(
        rf"^ai_one_phase\{{[^}}]*symbol=\"{re.escape(sym)}\"[^}}]*phase=\"([^\"]+)\"[^}}]*\}}\s+([0-9eE+\-.]+)$",
        re.M,
    )
    candidates: list[tuple[str, float]] = []
    for mm in rg_phase.finditer(text):
        try:
            candidates.append((mm.group(1), float(mm.group(2))))
        except Exception:
            continue
    if candidates:
        best = max(candidates, key=lambda kv: kv[1])
        out["phase"], out["phase_score"] = best[0], best[1]
    # ai_one_scenario{symbol="SYM",scenario="X"} conf
    rg_scn = re.compile(
        rf"^ai_one_scenario\{{[^}}]*symbol=\"{re.escape(sym)}\"[^}}]*scenario=\"([^\"]+)\"[^}}]*\}}\s+([0-9eE+\-.]+)$",
        re.M,
    )
    scn_candidates: list[tuple[str, float]] = []
    for mm in rg_scn.finditer(text):
        try:
            scn_candidates.append((mm.group(1), float(mm.group(2))))
        except Exception:
            continue
    if scn_candidates:
        best = max(scn_candidates, key=lambda kv: kv[1])
        out["scenario"], out["scenario_conf"] = best[0], best[1]
    # bias_state
    rg_bias = re.compile(
        rf"^ai_one_bias_state\{{[^}}]*symbol=\"{re.escape(sym)}\"[^}}]*\}}\s+([0-9eE+\-.]+)$",
        re.M,
    )
    m = rg_bias.search(text)
    if m:
        try:
            out["bias_state"] = float(m.group(1))
        except Exception:
            out["bias_state"] = None
    # context helpers
    for metric_name, key in (
        ("ai_one_context_near_edge_persist", "ctx_near_edge_persist"),
        ("ai_one_context_presence_sustain", "ctx_presence_sustain"),
    ):
        rg_ctx = re.compile(
            rf"^{metric_name}\{{[^}}]*symbol=\"{re.escape(sym)}\"[^}}]*\}}\s+([0-9eE+\-.]+)$",
            re.M,
        )
        m = rg_ctx.search(text)
        if m:
            try:
                out[key] = float(m.group(1))
            except Exception:
                out[key] = None
    # BTC regime (one-hot по state)
    rg_btc = re.compile(
        r"^ai_one_btc_regime\{state=\"([^\"]+)\"\}\s+([0-9eE+\-.]+)$", re.M
    )
    btc_best: tuple[str, float] | None = None
    for mm in rg_btc.finditer(text):
        try:
            val = float(mm.group(2))
        except Exception:
            continue
        if btc_best is None or val > btc_best[1]:
            btc_best = (mm.group(1), val)
    if btc_best and btc_best[1] > 0:
        out["btc_regime"] = btc_best[0]
    return out


def _render_md(
    rows: list[dict[str, Any]],
    metrics_note: str,
    contexts: dict[str, dict[str, Any]] | None = None,
) -> str:
    lines: list[str] = []
    lines.append("# Quality Snapshot\n\n")
    if metrics_note:
        lines.append(f"_{metrics_note}_\n\n")
    lines.append(
        "Symbol | Acts | p75_conf | mean_tts(s) | reject_top1..3(+rate) | ExpCov | ExpLat(ms)\n"
    )
    lines.append("---|---:|---:|---:|---|---:|---:\n")
    for sym in SYMBOLS:
        r = _row_for(rows, sym)
        if not r:
            lines.append(f"{sym} | 0 | 0.00 | 0.00 | - | 0.00 | 0\n")
            continue
        # reject причини з rate, якщо наявні
        rej_parts: list[str] = []
        for i in (1, 2, 3):
            name = str(r.get(f"reject_top{i}", "") or "").strip()
            rate = r.get(f"reject_top{i}_rate")
            if name:
                try:
                    rej_parts.append(f"{name}({_fmt_float(rate, 2)})")
                except Exception:
                    rej_parts.append(name)
        rej_str = ", ".join(rej_parts) if rej_parts else "-"
        lines.append(
            f"{sym} | {int(float(r.get('activations') or 0))} | {_fmt_float(r.get('p75_conf'))} | {_fmt_float(r.get('mean_time_to_stabilize_s'))} | {rej_str} | {_fmt_float(r.get('explain_coverage_rate'))} | {int(float(r.get('explain_latency_ms') or 0))}\n"
        )
    # Короткі контекстні рядки по метриках (≤3 рядки)
    if contexts:
        lines.append("\n")
        for sym in SYMBOLS:
            ctx = contexts.get(sym, {}) if isinstance(contexts, dict) else {}
            if not ctx:
                continue
            htf = ctx.get("htf_strength")
            pres = ctx.get("presence")
            ph = ctx.get("phase")
            pscore = ctx.get("phase_score")
            scn = ctx.get("scenario")
            scn_conf = ctx.get("scenario_conf")
            bias = ctx.get("bias_state")
            btc = ctx.get("btc_regime")
            nep = ctx.get("ctx_near_edge_persist")
            cps = ctx.get("ctx_presence_sustain")
            parts: list[str] = []
            if htf is not None:
                parts.append(f"htf_strength={_fmt_float(htf)}")
            if pres is not None:
                parts.append(f"presence={_fmt_float(pres)}")
            if ph:
                try:
                    parts.append(f"ai_one_phase={ph}({_fmt_float(pscore)})")
                except Exception:
                    parts.append(f"ai_one_phase={ph}")
            if scn:
                try:
                    parts.append(f"scenario={scn}({_fmt_float(scn_conf)})")
                except Exception:
                    parts.append(f"scenario={scn}")
            if bias is not None:
                parts.append(f"bias={_fmt_float(bias, 0)}")
            if btc:
                parts.append(f"btc_regime={btc}")
            if nep is not None:
                parts.append(f"near_edge_persist={_fmt_float(nep)}")
            if cps is not None:
                parts.append(f"presence_sustain={_fmt_float(cps)}")
            if parts:
                lines.append(f"- {sym}: " + ", ".join(parts) + "\n")
    # Обмежуємося ~<=30 рядками
    return "".join(lines)


def _load_bars(symbol: str) -> list[tuple[int, float]]:
    """Читає snapshot JSONL з datastore і повертає [(open_time_ms, close_price)]."""
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
    """Повертає відсотковий рет після k барів від бару, в який потрапляє ts_ms.

    Розрахунок: (close[t+k] / close[t] - 1.0) * 100.
    """
    if not bars or k <= 0:
        return None
    # знайдемо індекс бару t, для якого open_time <= ts < open_time+60s
    # припускаємо 1m бари рівномірні
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
    """Обчислює forward-метрики по decision_traces для символу.

    Повертає {K: {n, hit_cd, med_ret, p75_abs}}, де:
      - n: кількість валідних спостережень,
      - hit_cd: частка (0..1), де sign(cd) * ret_k_pct > 0,
      - med_ret: медіанний ret_k_pct,
      - p75_abs: 75-й перцентиль абсолютного |ret_k_pct|.
    """
    bars = _load_bars(symbol)
    if not bars:
        return {k: {"n": 0, "hit_cd": 0.0, "med_ret": 0.0, "p75_abs": 0.0} for k in ks}
    # читаємо останні події з decision_traces
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
    # беремо лише останні max_events рядків
    rows = rows[-max_events:] if rows else []
    per_k: dict[int, list[tuple[float, float]]] = {
        k: [] for k in ks
    }  # k -> [(ret_pct, cd)]
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
        # hit, якщо ret і cd одного знаку (ігноруємо cd≈0)
        hits = 0
        denom = 0
        for ret, c in vals:
            if abs(c) < 1e-9:
                continue
            denom += 1
            if (ret > 0 and c > 0) or (ret < 0 and c < 0):
                hits += 1
        hit_rate = (hits / denom) if denom > 0 else 0.0
        # медіана і p75(|ret|)
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


def main() -> int:
    args = _parse_args()
    csv_path = Path(args.csv)
    metrics_dir = Path(args.metrics_dir)
    out_path = _normalize_reports_path(Path(args.out))
    # forward K list
    try:
        ks = [int(x.strip()) for x in str(args.forward_k).split(",") if x.strip()]
        ks = [k for k in ks if k > 0]
    except Exception:
        ks = [3, 5, 10]
    if not ks:
        ks = [3, 5, 10]

    rows = _load_quality_rows(csv_path)
    latest_metrics = _pick_latest_metrics(metrics_dir)
    metrics_note = _extract_summary_from_metrics(latest_metrics)
    contexts: dict[str, dict[str, Any]] = {}
    try:
        if latest_metrics and latest_metrics.exists():
            text = latest_metrics.read_text(encoding="utf-8", errors="ignore")
            for s in SYMBOLS:
                contexts[s] = _parse_metrics_context(text, s)
            # Примітка: вилучено «best‑effort» підстановку ExpCov — відображаємо чесні значення з CSV без проксі
    except Exception:
        contexts = {}
    md = _render_md(rows, metrics_note, contexts)
    # Додамо forward‑перевірки (по рішенням і цінах)
    try:
        fwd_lines: list[str] = []
        fwd_lines.append("\n## Forward K‑bars (hit‑rate/PNL‑proxy)\n\n")
        fwd_lines.append("Symbol | K | N | hit_rate(cd·ret>0) | med_ret% | p75_abs%\n")
        fwd_lines.append("---|---:|---:|---:|---:|---:\n")
        for sym in SYMBOLS:
            stats = _forward_eval(sym, ks)
            for k in ks:
                s = stats.get(
                    k, {"n": 0, "hit_cd": 0.0, "med_ret": 0.0, "p75_abs": 0.0}
                )
                fwd_lines.append(
                    f"{sym} | {k} | {int(s['n'] or 0)} | {_fmt_float(s['hit_cd'] or 0.0, 2)} | {_fmt_float(s['med_ret'] or 0.0, 2)} | {_fmt_float(s['p75_abs'] or 0.0, 2)}\n"
                )
        md += "".join(fwd_lines)
    except Exception:
        # не зривати генерацію snapshot у разі проблем із forward‑секцією
        pass

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"Quality snapshot збережено у: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
