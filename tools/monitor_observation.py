from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

# Best-effort imports (усі залежності вже є в репо)
try:
    from app.settings import settings  # type: ignore
    from config.config import NAMESPACE, PROM_HTTP_PORT  # type: ignore
    from config.keys import build_key  # type: ignore
except Exception:  # pragma: no cover
    NAMESPACE = "ai_one"  # type: ignore[assignment]
    PROM_HTTP_PORT = 9108  # type: ignore[assignment]

    def build_key(ns: str, domain: str, *, symbol: str | None = None, granularity: str | None = None, extra: list[str] | None = None) -> str:  # type: ignore[no-redef]
        parts = [ns, domain]
        if symbol:
            parts.append(symbol.upper())
        if granularity:
            parts.append(granularity)
        if extra:
            parts.extend(extra)
        return ":".join(parts)


try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore[assignment]

logger = logging.getLogger("monitor_observation")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


def _parse_symbols(s: str) -> list[str]:
    return [x.strip().upper() for x in s.split(",") if x.strip()]


def _fetch_metrics(port: int) -> str:
    url = f"http://localhost:{int(port)}/metrics"
    with urllib.request.urlopen(url, timeout=3.0) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _dump_metrics_snapshot(text: str, reports_dir: Path) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
    out = reports_dir / f"metrics_{ts}.txt"
    out.write_text(text, encoding="utf-8")
    return out


def _dump_state_snapshot(r, symbols: list[str], reports_dir: Path) -> list[Path]:
    outs: list[Path] = []
    for sym in symbols:
        try:
            state_key = build_key(NAMESPACE, "state", symbol=sym)
            data = r.hgetall(state_key) if r is not None else {}
            p = (
                reports_dir
                / f"state_{sym}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.txt"
            )
            with p.open("w", encoding="utf-8") as fh:
                for k, v in sorted((data or {}).items()):
                    fh.write(f"{k}={v}\n")
            outs.append(p)
        except Exception:
            continue
    return outs


def _maybe_json(s: Any) -> Any:
    if not isinstance(s, str):
        return None
    s2 = s.strip()
    if not s2:
        return None
    try:
        return json.loads(s2)
    except Exception:
        return None


def _dump_deep_state(r, symbol: str, reports_dir: Path) -> list[Path]:
    """Спроба витягнути market_context.meta.* і зберегти окремими файлами.

    - context_frame → *_context.json
    - structure → *_structure.json
    - evidence → *_evidence.json
    - scenario_explain → *_explain.txt
    """
    outs: list[Path] = []
    if r is None:
        logger.warning("Redis недоступний для глибокого стану символу %s", symbol)
        return outs
    try:
        state_key = build_key(NAMESPACE, "state", symbol=symbol)
        data = r.hgetall(state_key) or {}
        logger.info("Отримано стан для символу %s: %d полів", symbol, len(data))
    except Exception as e:
        logger.error("Помилка отримання стану для символу %s: %s", symbol, str(e))
        data = {}
    # Пошук meta всередині market_context або окремих полів
    meta: dict[str, Any] = {}
    try:
        mc_raw = data.get("market_context") or data.get("market_context.meta")
        mc = _maybe_json(mc_raw)
        if isinstance(mc, dict):
            meta = (mc.get("meta") or mc) if isinstance(mc, dict) else {}
        logger.debug("Витягнуто meta для символу %s: %d елементів", symbol, len(meta))
    except Exception as e:
        logger.warning("Помилка парсингу meta для символу %s: %s", symbol, str(e))
        meta = {}
    # Якщо meta порожня — спробуємо окремі ключі як JSON
    if not meta:
        for key in (
            "context",
            "context_frame",
            "structure",
            "evidence",
            "scenario_explain",
        ):
            val = _maybe_json(data.get(key))
            if val is not None:
                meta[key] = val
            elif key in data:
                # як текстове значення
                meta[key] = data.get(key)
        logger.debug("Доповнено meta окремими ключами для символу %s", symbol)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
    # context_frame
    try:
        ctx = meta.get("context_frame") or meta.get("context")
        if ctx is not None:
            p_ctx = reports_dir / f"state_{symbol}_{ts}_context.json"
            p_ctx.write_text(
                json.dumps(ctx, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            outs.append(p_ctx)
            logger.info("Збережено context для символу %s: %s", symbol, p_ctx.name)
    except Exception as e:
        logger.error("Помилка збереження context для символу %s: %s", symbol, str(e))
    # structure
    try:
        st = meta.get("structure")
        if st is not None:
            p_st = reports_dir / f"state_{symbol}_{ts}_structure.json"
            p_st.write_text(
                json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            outs.append(p_st)
            logger.info("Збережено structure для символу %s: %s", symbol, p_st.name)
    except Exception as e:
        logger.error("Помилка збереження structure для символу %s: %s", symbol, str(e))
    # evidence
    try:
        ev = meta.get("evidence")
        if ev is not None:
            p_ev = reports_dir / f"state_{symbol}_{ts}_evidence.json"
            p_ev.write_text(
                json.dumps(ev, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            outs.append(p_ev)
            logger.info("Збережено evidence для символу %s: %s", symbol, p_ev.name)
    except Exception as e:
        logger.error("Помилка збереження evidence для символу %s: %s", symbol, str(e))
    # explain (рядок або структура)
    try:
        ex = meta.get("scenario_explain")
        if ex is not None:
            p_ex = reports_dir / f"state_{symbol}_{ts}_explain.txt"
            if isinstance(ex, (dict, list)):
                p_ex.write_text(
                    json.dumps(ex, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            else:
                p_ex.write_text(str(ex), encoding="utf-8")
            outs.append(p_ex)
            logger.info("Збережено explain для символу %s: %s", symbol, p_ex.name)
    except Exception as e:
        logger.error("Помилка збереження explain для символу %s: %s", symbol, str(e))
    logger.info("Завершено глибокий стан для символу %s: %d файлів", symbol, len(outs))
    return outs


def _run_quality_report(reports_dir: Path) -> int:
    """Запускає tools.scenario_quality_report як модуль для оновлення CSV."""
    try:
        import subprocess

        out_csv = str(reports_dir / "quality.csv")
        cmd = [
            sys.executable,
            "-m",
            "tools.scenario_quality_report",
            "--last-hours",
            "4",
            "--out",
            out_csv,
        ]
        logger.info("Запуск звіту якості сценаріїв: %s", out_csv)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            logger.warning("Звіт якості завершився з помилкою: %d", proc.returncode)
            print(proc.stdout)
            print(proc.stderr, file=sys.stderr)
        else:
            logger.info("Звіт якості успішно оновлено")
        return int(proc.returncode)
    except Exception as e:
        logger.error("Помилка запуску звіту якості: %s", str(e))
        return 1


def _summarize_quality(reports_dir: Path) -> dict[str, Any]:
    path = reports_dir / "quality.csv"
    out: dict[str, Any] = {
        "by_symbol": {},
        "scenarios": {},
        "btc_gate_effect": {"overall_rate": 0.0, "flat_only": True},
    }
    if not path.exists():
        logger.info("Файл якості не існує: %s", path)
        return out
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        rdr = csv.DictReader(fh)
        for r in rdr:
            try:
                rows.append(
                    {
                        "symbol": r.get("symbol", "").upper(),
                        "scenario": r.get("scenario", ""),
                        "activations": int(r.get("activations", 0) or 0),
                        "mean_conf": float(r.get("mean_conf", 0.0) or 0.0),
                        "p75_conf": float(r.get("p75_conf", 0.0) or 0.0),
                        "mean_time_to_stabilize_s": float(
                            r.get("mean_time_to_stabilize_s", 0.0) or 0.0
                        ),
                        "btc_gate_effect_rate": float(
                            r.get("btc_gate_effect_rate", 0.0) or 0.0
                        ),
                    }
                )
            except Exception:
                continue
    logger.info("Завантажено %d рядків якості з %s", len(rows), path)
    # by_symbol
    by_symbol: dict[str, dict[str, Any]] = {}
    total_acts = 0
    pen_weighted = 0.0
    for r in rows:
        sym = r["symbol"]
        b = by_symbol.setdefault(sym, {"rows": [], "activations": 0})
        b["rows"].append(r)
        b["activations"] += int(r["activations"])  # type: ignore[index]
        total_acts += int(r["activations"])  # type: ignore[index]
        pen_weighted += float(r["btc_gate_effect_rate"]) * float(r["activations"])  # type: ignore[index]
    out["by_symbol"] = by_symbol
    # scenarios (покладемо як останній CSV-рядок на сценарій для простоти)
    scn_map: dict[str, dict[str, Any]] = {}
    for r in rows:
        scn_map[r["scenario"]] = r
    out["scenarios"] = scn_map
    overall_rate = round((pen_weighted / total_acts), 4) if total_acts else 0.0
    out["btc_gate_effect"]["overall_rate"] = overall_rate
    logger.info(
        "Підсумовано якість: загальна ставка BTC gate %.4f, активацій %d",
        overall_rate,
        total_acts,
    )
    return out


def _run_quality_snapshot(reports_dir: Path) -> int:
    """Генерує консолідований snapshot Markdown (quality_snapshot.md)."""
    try:
        import subprocess

        out_md = str(reports_dir / "quality_snapshot.md")
        cmd = [
            sys.executable,
            "-m",
            "tools.quality_snapshot",
            "--metrics-dir",
            str(reports_dir),
            "--csv",
            str(reports_dir / "quality.csv"),
            "--out",
            out_md,
        ]
        logger.info("Генерація snapshot: %s", out_md)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            logger.warning("Snapshot завершився з помилкою: %d", proc.returncode)
            print(proc.stdout)
            print(proc.stderr, file=sys.stderr)
        else:
            logger.info("Snapshot оновлено: %s", out_md)
        return int(proc.returncode)
    except Exception as e:
        logger.error("Помилка генерації snapshot: %s", str(e))
        return 1


def _run_quality_dashboard(reports_dir: Path) -> int:
    """Генерує агрегований дашборд Markdown (quality_dashboard.md)."""
    try:
        import subprocess

        out_md = str(reports_dir / "quality_dashboard.md")
        cmd = [
            sys.executable,
            "-m",
            "tools.quality_dashboard",
            "--csv",
            str(reports_dir / "quality.csv"),
            "--out",
            out_md,
        ]
        logger.info("Генерація дашборду: %s", out_md)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            logger.warning("Дашборд завершився з помилкою: %d", proc.returncode)
            print(proc.stdout)
            print(proc.stderr, file=sys.stderr)
        else:
            logger.info("Дашборд оновлено: %s", out_md)
        return int(proc.returncode)
    except Exception as e:
        logger.error("Помилка генерації дашборду: %s", str(e))
        return 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="48h спостереження: метрики, Redis-стан, якість сценаріїв"
    )
    ap.add_argument(
        "--symbols", default="BTCUSDT,TONUSDT,SNXUSDT", help="Кома‑список символів"
    )
    ap.add_argument(
        "--interval-min", type=int, default=15, help="Інтервал знімків, хвилини"
    )
    ap.add_argument(
        "--duration-min",
        type=int,
        default=2880,
        help="Тривалість спостереження, хвилини (48 год = 2880)",
    )
    ap.add_argument("--out-dir", default="reports", help="Каталог для артефактів")
    ap.add_argument(
        "--deep-state",
        action="store_true",
        help="Знімати окремі context/structure/evidence/explain файли",
    )
    args = ap.parse_args(argv)

    symbols = _parse_symbols(args.symbols)
    reports_dir = Path(args.out_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    r = None
    if redis is not None:
        try:
            r = redis.StrictRedis(
                host=settings.redis_host,
                port=settings.redis_port,
                decode_responses=True,
            )
        except Exception:
            r = None

    deadline = time.time() + (int(args.duration_min) * 60)
    next_tick = time.time()
    # PID-файл для зовнішнього контролю живучості
    try:
        (reports_dir / "monitor.pid").write_text(str(os.getpid()), encoding="utf-8")
    except Exception:
        pass
    logger.info(
        f"[STRICT_SANITY] [monitor] start symbols={symbols} interval_min={args.interval_min} duration_min={args.duration_min} prom_port={PROM_HTTP_PORT}"
    )
    while time.time() < deadline:
        try:
            text = _fetch_metrics(int(PROM_HTTP_PORT))
            p = _dump_metrics_snapshot(text, reports_dir)
            logger.info(f"[STRICT_SANITY] [monitor] metrics-> {p}")
        except Exception as e:
            logger.error(f"[STRICT_SANITY] [monitor] metrics fetch error: {e}")
        try:
            outs = _dump_state_snapshot(r, symbols, reports_dir)
            if outs:
                logger.info(f"[STRICT_SANITY] [monitor] state-> {outs[-1]}")
        except Exception as e:
            logger.error(f"[STRICT_SANITY] [monitor] state dump error: {e}")
        # Опціонально: глибокі снапшоти meta.*
        if args.deep_state and r is not None:
            try:
                for sym in symbols:
                    deep_outs = _dump_deep_state(r, sym, reports_dir)
                    if deep_outs:
                        logger.info(
                            f"[STRICT_SANITY] [monitor] deep-state-> {deep_outs[-1]}"
                        )
            except Exception as e:
                logger.error(f"[STRICT_SANITY] [monitor] deep-state error: {e}")
        try:
            _ = _run_quality_report(reports_dir)
        except Exception:
            pass
        # Snapshot + Dashboard on each tick (best-effort)
        try:
            _ = _run_quality_snapshot(reports_dir)
        except Exception:
            pass
        try:
            _ = _run_quality_dashboard(reports_dir)
        except Exception:
            pass
        # heartbeat (видимий у stdout)
        try:
            logger.info(
                f"MONITOR_TICK ts={datetime.utcnow().isoformat()}Z symbols={','.join(symbols)}",
            )
        except Exception:
            pass
        # планування наступного циклу
        next_tick += int(args.interval_min) * 60
        sleep_s = max(5.0, next_tick - time.time())
        time.sleep(sleep_s)

    # фінальний summary
    try:
        summary = _summarize_quality(reports_dir)
        with (reports_dir / "quality_summary.json").open("w", encoding="utf-8") as fh:
            json.dump(summary, fh, ensure_ascii=False, indent=2)
        logger.info(
            "[STRICT_SANITY] [monitor] summary-> %s",
            reports_dir / "quality_summary.json",
        )
    except Exception as e:
        logger.error(f"[STRICT_SANITY] [monitor] summary error: {e}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
