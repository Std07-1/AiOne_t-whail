from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Аналіз якості сценаріїв за останні години логів (без торгів)"
    )
    ap.add_argument(
        "--last-hours",
        type=int,
        default=4,
        dest="last_hours",
        help="Останні N годин для аналізу",
    )
    ap.add_argument(
        "--logs-dir",
        default="./logs",
        help="Каталог з логами (де є app.log)",
    )
    ap.add_argument(
        "--logs",
        default="",
        help=(
            "Перелік лог-файлів через кому (наприклад: run_A.log,run_B.log). "
            "Якщо вказано, --logs-dir ігнорується."
        ),
    )
    ap.add_argument(
        "--out",
        default="reports/quality.csv",
        help="Шлях до CSV-звіту, який буде згенеровано",
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


def _iter_recent_lines(log_path: Path, since: datetime):
    if not log_path.exists():
        return
    # Пробуємо читати файл построчно і фільтрувати за часом, якщо є мітка [MM/DD/YY HH:MM:SS]
    ts_re = re.compile(r"\[(\d{2})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})\]")
    now = datetime.utcnow()
    current_year = now.year % 100  # дві останні цифри
    last_ts: datetime | None = None
    with log_path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            m = ts_re.search(line)
            if not m:
                # Якщо немає таймстемпу — підставляємо останній відомий або now,
                # щоб події з одного блоку добре вирівнювались у часі для вікон ±30с
                if last_ts is None:
                    last_ts = now
                else:
                    # забезпечимо монотонність часу для рядків без міток
                    last_ts = last_ts + timedelta(seconds=1)
                synth_ts = last_ts
                if synth_ts >= since:
                    prefix = synth_ts.strftime("[%m/%d/%y %H:%M:%S] ")
                    yield prefix + line
                continue
            mo, dd, yy, hh, mi, ss = map(int, m.groups())
            # Проста реконструкція року (XX) у 20XX; достатньо для фільтра останніх годин
            year_full = 2000 + (
                yy if yy <= current_year else yy
            )  # захист від 99→1999 не потрібен тут
            try:
                ts = datetime(year_full, mo, dd, hh, mi, ss)
                last_ts = ts
            except Exception:
                # якщо парсинг часу не вдався — пропускаємо префікс і використовуємо останній відомий
                ts = last_ts or now
            if ts >= since:
                yield line


def _parse_ts(line: str) -> datetime | None:
    """Витягти datetime з префікса [MM/DD/YY HH:MM:SS] у рядку логу."""
    ts_re = re.compile(r"\[(\d{2})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})\]")
    m = ts_re.search(line)
    if not m:
        return None
    mo, dd, yy, hh, mi, ss = map(int, m.groups())
    now = datetime.utcnow()
    year_full = 2000 + (yy if yy <= (now.year % 100) else yy)
    try:
        return datetime(year_full, mo, dd, hh, mi, ss)
    except Exception:
        return None


def _analyze(lines: list[str]) -> list[dict[str, Any]]:
    """Розширений аналіз: групування за (symbol, scenario) з метриками якості.

    Повертає рядки з полями:
      symbol, scenario, activations, mean_conf, p75_conf,
      mean_time_to_stabilize_s, btc_gate_effect_rate
    """
    sym_re = re.compile(r"symbol=([a-z0-9]+)")
    conf_re = re.compile(r"conf(?:idence)?=([0-9]*\.?[0-9]+)")
    cand_re = re.compile(r"\[SCENARIO_TRACE\].*candidate=([a-z_]+)")
    alert_re = re.compile(r"\[SCENARIO_ALERT\].*activate=([a-z_]+)")
    penalty_re = re.compile(r"penalty=0\.2")
    decision_re = re.compile(r"decision=(ACCEPT|REJECT)")
    causes_re = re.compile(r"causes=([a-z_,]+)")
    explain_re = re.compile(
        r"\[SCEN_EXPLAIN\].*symbol=([a-z0-9]+).*scenario=([a-z_]+|none).*explain=\"([^\"]*)\""
    )
    # Фолбек-патерн без вимоги закривальної лапки (деякі лог-форматери підклеюють file:line у кінець рядка)
    explain_fallback_re = re.compile(
        r"\[SCEN_EXPLAIN\].*symbol=([a-z0-9]+).*scenario=([a-z_]+|none)"
    )

    # Зберемо активaції та трейси
    activations: list[dict[str, Any]] = []
    traces: list[dict[str, Any]] = []
    explains: dict[tuple[str, str], dict[str, Any]] = {}
    explain_events: list[dict[str, Any]] = []  # {ts, symbol, scenario}

    for ln in lines:
        ts = _parse_ts(ln) or datetime.utcnow()
        ms = sym_re.search(ln)
        symbol = (ms.group(1) if ms else "").lower()
        if "[SCENARIO_ALERT]" in ln:
            ma = alert_re.search(ln)
            if not (ma and symbol):
                continue
            scen = ma.group(1)
            mc = conf_re.search(ln)
            conf = float(mc.group(1)) if mc else 0.0
            activations.append(
                {"ts": ts, "symbol": symbol, "scenario": scen, "conf": conf}
            )
        elif "[SCENARIO_TRACE]" in ln:
            mcand = cand_re.search(ln)
            if not (mcand and symbol):
                continue
            cand = mcand.group(1)
            penalty = bool(penalty_re.search(ln))
            mdec = decision_re.search(ln)
            decision = mdec.group(1) if mdec else ""
            mca = causes_re.search(ln)
            causes = []
            if mca:
                try:
                    causes = [c.strip() for c in mca.group(1).split(",") if c.strip()]
                except Exception:
                    causes = []
            traces.append(
                {
                    "ts": ts,
                    "symbol": symbol,
                    "candidate": cand,
                    "penalty": penalty,
                    "decision": decision,
                    "causes": causes,
                }
            )
        elif "[SCEN_EXPLAIN]" in ln:
            me = explain_re.search(ln)
            sym_e = ""
            scen_e = "none"
            vals: dict[str, Any] = {}
            if me:
                sym_e = (me.group(1) or "").lower()
                scen_e = (me.group(2) or "none").strip()
                payload = (me.group(3) or "").strip()
                # розбір формату key=value (w=...); key2=value2
                for part in [p.strip() for p in payload.split(";") if p.strip()]:
                    try:
                        kv = part.split(" ", 1)[0]  # до першого пробілу
                        if "=" not in kv:
                            continue
                        k, v = kv.split("=", 1)
                        k = k.strip()
                        v = v.strip()
                        # типізація найважливіших полів
                        if k in {
                            "near_edge_persist",
                            "presence_sustain",
                            "compression.index",
                        }:
                            try:
                                vals[k] = float(v)
                            except Exception:
                                vals[k] = 0.0
                        elif k == "in_zone":
                            vals[k] = 1 if v in {"1", "true", "True"} else 0
                        elif k == "btc_regime_v2":
                            vals[k] = v
                        else:
                            vals[k] = v
                    except Exception:
                        pass
            else:
                # fallback: витягуємо тільки symbol/scenario для обліку покриття
                mfe = explain_fallback_re.search(ln)
                if mfe:
                    sym_e = (mfe.group(1) or "").lower()
                    scen_e = (mfe.group(2) or "none").strip()
            if sym_e:
                if vals:
                    explains[(sym_e, scen_e)] = vals
                explain_events.append({"ts": ts, "symbol": sym_e, "scenario": scen_e})

    # Індекс кандидатів за (symbol, scenario): перший час появи
    first_cand_ts: dict[tuple[str, str], datetime] = {}
    for tr in traces:
        key = (tr["symbol"], tr["candidate"])
        if key not in first_cand_ts or tr["ts"] < first_cand_ts[key]:
            first_cand_ts[key] = tr["ts"]

    # Індекс explain-подій за символом (для швидкого пошуку у вікні ±30с)
    exp_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for ev in explain_events:
        exp_by_symbol.setdefault(ev["symbol"], []).append(ev)
    for v in exp_by_symbol.values():
        v.sort(key=lambda e: e["ts"])  # впорядкуємо за часом

    # Агрегація за (symbol, scenario)
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for act in activations:
        key = (act["symbol"], act["scenario"])  # type: ignore[index]
        b = buckets.setdefault(
            key,
            {
                "confs": [],
                "tts": [],
                "pen_hits": 0,
                "count": 0,
                "reject_causes": {},  # type: ignore[dict-item]
                "exp_hits": 0,
                "exp_latencies_ms": [],
            },
        )
        b["count"] += 1
        b["confs"].append(float(act["conf"]))
        # Час до стабілізації: від першого candidate до activate
        cand_key = (act["symbol"], act["scenario"])  # type: ignore[index]
        t0 = first_cand_ts.get(cand_key)
        if t0 and act["ts"] >= t0:
            b["tts"].append((act["ts"] - t0).total_seconds())
        # Ефект BTC‑gate: чи був penalty=0.2 у TRACE в ±30 с
        t_act = act["ts"]
        lo = t_act - timedelta(seconds=30)
        hi = t_act + timedelta(seconds=30)
        for tr in traces:
            if tr["symbol"] == act["symbol"] and lo <= tr["ts"] <= hi and tr["penalty"]:
                b["pen_hits"] += 1
                break
        # Explain coverage: наявність [SCEN_EXPLAIN] у ±30с для символу (сценарій exact або "none")
        ex_list = exp_by_symbol.get(act["symbol"], [])
        has_explain = False
        nearest_ex_ts = None
        for ev in ex_list:
            if not (lo <= ev["ts"] <= hi):
                continue
            if ev.get("scenario") in (act["scenario"], "none"):
                has_explain = True
                # знайти найближчий до активації
                if (nearest_ex_ts is None) or (
                    abs((ev["ts"] - t_act).total_seconds())
                    < abs((nearest_ex_ts - t_act).total_seconds())
                ):
                    nearest_ex_ts = ev["ts"]
        # Примітка: діагностичне розширення вікна ±300с для ExpCov видалено — тримаємо чесне ±30с
        if has_explain:
            b["exp_hits"] += 1
            # Латентність від першого TRACE кандидата до explain
            if t0 and nearest_ex_ts and nearest_ex_ts >= t0:
                b["exp_latencies_ms"].append(
                    int((nearest_ex_ts - t0).total_seconds() * 1000.0)
                )

    # Додатково: для кожного (symbol, scenario) рахуємо REJECT-причини у TRACE
    for tr in traces:
        key = (tr["symbol"], tr.get("candidate", ""))
        if key not in buckets:
            continue  # рахуємо лише для пар, де були активації
        if str(tr.get("decision", "")) != "REJECT":
            continue
        causes = tr.get("causes") or []
        b = buckets[key]
        rc: dict[str, int] = b.get("reject_causes", {}) or {}
        for c in causes:
            rc[c] = int(rc.get(c, 0)) + 1
        b["reject_causes"] = rc

    def _p75(values: list[float]) -> float:
        if not values:
            return 0.0
        vs = sorted(values)
        # індекс за nearest-rank (без інтерполяції)
        idx = max(0, min(len(vs) - 1, int(0.75 * (len(vs) - 1))))
        return float(vs[idx])

    rows: list[dict[str, Any]] = []
    for (symbol, scen), b in buckets.items():
        cnt = int(b.get("count", 0))
        confs: list[float] = list(b.get("confs", []))
        tts: list[float] = list(b.get("tts", []))
        mean_conf = round(sum(confs) / len(confs), 4) if confs else 0.0
        p75_conf = round(_p75(confs), 4) if confs else 0.0
        mean_tts = round(sum(tts) / len(tts), 2) if tts else 0.0
        pen_rate = round((b.get("pen_hits", 0) or 0) / cnt, 4) if cnt else 0.0
        # Explain coverage/latency
        exp_hits = int(b.get("exp_hits", 0) or 0)
        exp_rate = round((exp_hits / cnt), 4) if cnt else 0.0
        # Примітка: видалено форсоване встановлення exp_rate=1.0 при наявності будь-яких explain — без підміни показника
        lat_list: list[int] = list(b.get("exp_latencies_ms", []))
        exp_latency_ms = int(sum(lat_list) / len(lat_list)) if lat_list else 0
        # Top-3 причин REJECT у TRACE
        rc: dict[str, int] = b.get("reject_causes", {}) or {}
        total_rejects = sum(int(v) for v in rc.values())
        top = sorted(rc.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:3]
        top_names = [t[0] for t in top]
        top_rates = [
            round((int(t[1]) / total_rejects), 4) if total_rejects else 0.0 for t in top
        ]
        # Заповнюємо до 3 елементів
        while len(top_names) < 3:
            top_names.append("")
            top_rates.append(0.0)
        # SCEN_EXPLAIN збагачення (best-effort)
        ex = (
            explains.get((symbol.lower(), scen))
            or explains.get((symbol.lower(), "none"))
            or {}
        )
        try:
            comp_level = float(ex.get("compression.index", 0.0) or 0.0)
        except Exception:
            comp_level = 0.0
        try:
            nep = float(ex.get("near_edge_persist", 0.0) or 0.0)
        except Exception:
            nep = 0.0
        try:
            psu = float(ex.get("presence_sustain", 0.0) or 0.0)
        except Exception:
            psu = 0.0
        persist_score = round(max(nep, psu), 4) if (nep or psu) else 0.0
        in_zone = int(ex.get("in_zone", 0) or 0)
        btc_reg = str(ex.get("btc_regime_v2", ""))
        rows.append(
            {
                "symbol": symbol.upper(),
                "scenario": scen,
                "activations": cnt,
                "mean_conf": mean_conf,
                "p75_conf": p75_conf,
                "mean_time_to_stabilize_s": mean_tts,
                "btc_gate_effect_rate": pen_rate,
                "explain_coverage_rate": exp_rate,
                "explain_latency_ms": exp_latency_ms,
                "reject_top1": top_names[0],
                "reject_top1_rate": top_rates[0],
                "reject_top2": top_names[1],
                "reject_top2_rate": top_rates[1],
                "reject_top3": top_names[2],
                "reject_top3_rate": top_rates[2],
                "in_zone": in_zone,
                "compression_level": round(comp_level, 4),
                "persist_score": persist_score,
                "btc_regime_at_activation": btc_reg,
                "quality_proxy": "-",
            }
        )
    return rows


def main() -> int:
    args = _parse_args()
    out_csv = _normalize_reports_path(Path(args.out))
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    since = datetime.utcnow() - timedelta(hours=int(args.last_hours))
    recent_lines: list[str] = []
    logs_arg: str = str(getattr(args, "logs", "") or "").strip()
    if logs_arg:
        # Підтримка кількох логів через кому
        for part in [p.strip() for p in logs_arg.split(",") if p.strip()]:
            p = Path(part)
            recent_lines.extend(list(_iter_recent_lines(p, since)))
    else:
        logs_dir = Path(args.logs_dir)
        log_path = logs_dir / "app.log"
        recent_lines = list(_iter_recent_lines(log_path, since))
    rows = _analyze(recent_lines)

    fields = [
        "symbol",
        "scenario",
        "activations",
        "mean_conf",
        "p75_conf",
        "mean_time_to_stabilize_s",
        "btc_gate_effect_rate",
        "explain_coverage_rate",
        "explain_latency_ms",
        "reject_top1",
        "reject_top1_rate",
        "reject_top2",
        "reject_top2_rate",
        "reject_top3",
        "reject_top3_rate",
        # Forward‑lite / контекстні поля (best‑effort):
        "in_zone",
        "compression_level",
        "persist_score",
        "btc_regime_at_activation",
        "quality_proxy",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    print(",".join(fields))
    for r in rows:
        print(",".join(str(r[k]) for k in fields))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
