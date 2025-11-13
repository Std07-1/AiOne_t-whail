"""
Unified Runner (tools.unified_runner)

Опис
- Єдиний CLI-раннер для двох режимів:
    1) live — запуск основного пайплайна моніторингу (app.main.run_pipeline) на заданий час.
    2) replay — псевдострім/реплей історичних барів для набору символів із потрібного джерела.
- Підтримує:
    - центральний out-dir із артефактами (лог, метрики, звіти);
    - tee stdout/stderr у файл run.log;
    - best‑effort віддзеркалення --set у ENV та config.config (з приведенням типів);
    - опційний Prometheus-скрейп у фоні (metrics.txt);
    - постпроцесинг: quality.csv, quality_snapshot.md, forward_<mode>.md, summary_*.md.

Ключові можливості
- Мінімальні вимоги до проєкту: імпорт app.main.run_pipeline для live; tools.replay_stream.run_replay для replay.
- Очистка каталогу в межах свого простору: видаляє лише відомі файли (run.log, metrics.txt, quality.csv, quality_snapshot.md, summary.md, summary_*.md, forward_live.md, forward_replay.md, artifacts/*), решту не чіпає.
- Prometheus: якщо задано --prom-port, автоматично вмикає PROM_GAUGES_ENABLED=true та збирає зрізи у metrics.txt (~кожні 15с).
- Summary: генерує summary_YYYY-MM-DD_HHMMSSZ.md і дублює в summary.md. KPI: Stage1 p95/mean/count, профільні перемикання/хв, false_breakout_total, ExpCov (глобально і по символах), топ-сценарії.
    - Вердикт: GO / WARN / NO-GO за простими порогами (latency>200ms, switch_rate>2/хв, ExpCov<0.60 або 0).

CLI
- Загальний формат:
    python -m tools.unified_runner {live|replay} [опції]

Спільні опції
- --namespace           Значення STATE_NAMESPACE (default: ai_one).
- --prom-port           Локальний порт HTTP для Prometheus метрик. Вмикає скрейпер у metrics.txt.
- --set KEY=VALUE       Повторювана опція для встановлення ENV і віддзеркалення у config.config.
                                                Підтримується приведення типів: true/false → bool, int/float → числа.
- --out-dir PATH        Каталог для артефактів (default: reports/run).
- --report              Увімкнути постпроцесинг і генерацію звітів.

Режим live
- Обовʼязково: --duration S (тривалість у секундах).
- Приклад (Windows PowerShell):
    python -m tools.unified_runner live --duration 900 --namespace ai_one_dev --prom-port 9108 --report --out-dir reports/run
    python -m tools.unified_runner live --duration 600 --set STRICT_HTF_GRAY_GATE_ENABLED=true --set SCEN_BTC_SOFT_GATE=true --report
- Поведінка:
    - Перед стартом робиться очистка від попередніх артефактів (див. вище).
    - Застосовуються --set у ENV і config.config.
    - stdout/stderr дублюються в run.log (tee).
    - Імпортується app.main.run_pipeline() і виконується до закінчення таймера або природного завершення.
    - Якщо заданий --prom-port, фонова нитка знімає /metrics у metrics.txt до кінця сесії.
    - Після завершення (включно з Ctrl+C) виконується post_process(), якщо --report.

Режим replay
- Обовʼязково: --limit N (кількість барів на символ).
- Додаткові опції:
    - --symbols LIST   Кома-сепарований список (default: BTCUSDT,ETHUSDT,TONUSDT,SNXUSDT).
    - --interval STR   Інтервал барів (default: 1m).
    - --source SRC     Джерело даних: snapshot | binance (default: snapshot).
- Приклади:
    python -m tools.unified_runner replay --limit 900 --symbols BTCUSDT,TONUSDT --interval 1m --source snapshot --prom-port 9108 --report --out-dir reports/replay_run
    python -m tools.unified_runner replay --limit 720 --symbols BTCUSDT --source binance --report
- Поведінка:
    - Для кожного символу викликає tools.replay_stream.run_replay з дампом у artifacts/replay_<symbol>_<interval>_<limit>.
    - Після завершення робить фінальний best‑effort знімок метрик (якщо --prom-port) у metrics.txt.
    - Постпроцесинг і звіти — як у live, якщо --report.

Артефакти (у --out-dir)
- run.log                     Повний лог прогону (stdout+stderr).
- metrics.txt                 Зліпки Prometheus /metrics з часовими маркерами.
- quality.csv                 Якість сценаріїв (збирається з run.log, якщо --report).
- quality_snapshot.md         Оглядова аналітика якості/метрик (якщо --report).
- forward_<mode>.md           Витяг forward-сигналів з run.log (якщо --report).
- summary_*.md / summary.md   Підсумковий звіт і остання версія за сталим шляхом.
- artifacts/*                 Проміжні дані, у т.ч. дампи реплею.

Постпроцесинг (--report)
1) tools.scenario_quality_report: будує quality.csv з run.log.
2) tools.quality_snapshot: формує quality_snapshot.md (+forward K таблички).
3) tools.forward_from_log: генерує forward_<mode>.md (фільтрований forward) з дефолтними порогами:
     presence_min=0.75, bias_abs_min=0.6, whale_max_age_sec=600.
4) summary: агрегує KPI з metrics.txt і quality.csv, формує verdict (GO/WARN/NO-GO).
5) README.md: короткий індекс артефактів.

Метрики й KPI (що шукає summary)
- ai_one_stage1_latency_ms_{bucket,sum,count}: обчислює p95, mean, count.
- ai_one_profile_switch_total{...}: різниця між першим і останнім знімками → switch_rate/хв.
- ai_one_false_breakout_total{...}: сумує всі серії → false_breakout_total.
- quality.csv: explain_coverage_rate і activations → зважений ExpCov; також топ-5 сценаріїв за активаціями.
- Пороги вердикту:
    - p95 > 200 → WARN, > 280 → NO-GO;
    - switch_rate > 2/хв → WARN;
    - ExpCov = 0 → WARN; 0 < ExpCov < 0.60 → WARN.

ENV/--set
- --set KEY=VALUE: запис у os.environ та спроба setattr у config.config.
- Приведення типів: true/false → bool, int/float → числа, інакше str.
- Приклад:
    --set STRICT_ACCUM_CAPS_ENABLED=true --set PROM_GAUGES_ENABLED=false --set PROM_HTTP_PORT=9108

Коди завершення
- 0   Успіх або контрольоване завершення (таймер/Cancel).
- 1   Помилка запуску/імпорту/системна помилка.
- 2   Replay відпрацював частково (є помилки на деяких символах).
- 130 Перервано користувачем (KeyboardInterrupt).

Програмне використання
- RunnerConfig: описує параметри запуску.
- RunnerOrchestrator: високорівневий фасад:
    - run_live() / run_replay() — основні шляхи;
    - post_process() — звітність;
    - generate_summary() — побудова summary_*;
    - write_readme() — індекс артефактів.

Примітки
- Очистка каталогу не повинна блокувати запуск (усі помилки — best‑effort, ігноруються).
- Скрейпер метрик — неблокуючий, daemon thread, інтервал 15с.
- Для прометевих метрик необхідно, щоб процес, що надає /metrics, працював локально на --prom-port.
- Всі повідомлення/логи — українською.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import threading
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Профілі forward (основні). hint/quality обробляються окремо без порогів.
FORWARD_PROFILES: dict[str, dict[str, Any]] = {
    "strong": {"presence": 0.75, "bias": 0.60, "whale": 600, "source": "whale"},
    "soft": {"presence": 0.55, "bias": 0.40, "whale": 1200, "source": "whale"},
    "explain": {"presence": 0.45, "bias": 0.30, "ttl": 600, "source": "explain"},
}

_TEE_ORIG_STDOUT = None
_TEE_ORIG_STDERR = None
_TEE_FILE = None


def _now_iso() -> str:
    try:
        return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:  # pragma: no cover
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _tee_stdout_stderr(log_path: Path) -> None:
    """Дублює stdout/stderr у файл log_path. Повторний виклик ігнорується."""
    global _TEE_ORIG_STDOUT, _TEE_ORIG_STDERR, _TEE_FILE
    if _TEE_FILE is not None:
        return
    try:
        _TEE_FILE = open(log_path, "a", encoding="utf-8")
    except Exception:  # pragma: no cover
        return
    _TEE_ORIG_STDOUT = sys.stdout
    _TEE_ORIG_STDERR = sys.stderr

    class _Tee:
        def __init__(self, orig, stream_name: str):
            self._orig = orig
            self._name = stream_name

        def write(self, data: str) -> int:  # type: ignore[override]
            try:
                _TEE_FILE.write(data)
            except Exception:
                pass
            return self._orig.write(data)

        def flush(self) -> None:  # type: ignore[override]
            try:
                _TEE_FILE.flush()
            except Exception:
                pass
            return self._orig.flush()

    sys.stdout = _Tee(sys.stdout, "stdout")  # type: ignore[assignment]
    sys.stderr = _Tee(sys.stderr, "stderr")  # type: ignore[assignment]


def _close_tee() -> None:
    global _TEE_ORIG_STDOUT, _TEE_ORIG_STDERR, _TEE_FILE
    if _TEE_FILE is None:
        return
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    try:
        if _TEE_FILE:
            _TEE_FILE.flush()
            _TEE_FILE.close()
    except Exception:
        pass
    if _TEE_ORIG_STDOUT is not None:
        sys.stdout = _TEE_ORIG_STDOUT  # type: ignore[assignment]
    if _TEE_ORIG_STDERR is not None:
        sys.stderr = _TEE_ORIG_STDERR  # type: ignore[assignment]
    _TEE_FILE = None
    _TEE_ORIG_STDOUT = None
    _TEE_ORIG_STDERR = None


def _coerce_value(val: str) -> Any:
    v = val.strip()
    low = v.lower()
    if low in {"true", "false"}:
        return low == "true"
    try:
        if "." in v:
            return float(v)
        return int(v)
    except Exception:
        return v


def _apply_sets(pairs: list[tuple[str, str]]) -> None:
    for k, v in pairs:
        try:
            os.environ[str(k)] = str(v)
        except Exception:
            pass
        # Віддзеркалення у config.config (best-effort)
        try:
            import config.config as _cfg  # type: ignore

            coerced = _coerce_value(v)
            if hasattr(_cfg, k):
                setattr(_cfg, k, coerced)
        except Exception:
            continue


def _parse_sets(raw: list[str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for item in raw:
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        pairs.append((k, v))
    return pairs


class _MetricsScraper:
    def __init__(self, url: str, out_path: Path, interval_s: int = 15) -> None:
        self._url = url
        self._out = out_path
        self._int = max(1, int(interval_s))
        self._stop = threading.Event()
        self._thr: threading.Thread | None = None

    def start(self) -> None:
        self._out.parent.mkdir(parents=True, exist_ok=True)
        self._thr = threading.Thread(target=self._loop, daemon=True)
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thr:
            self._thr.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                with urllib.request.urlopen(self._url, timeout=5) as resp:
                    data = resp.read().decode("utf-8", errors="ignore")
            except Exception:
                data = ""
            snap = [f"### SNAPSHOT ts={_now_iso()}\n", data]
            try:
                with self._out.open("a", encoding="utf-8") as fh:
                    fh.write("".join(snap))
                    if not data.endswith("\n"):
                        fh.write("\n")
            except Exception:
                pass
            self._stop.wait(self._int)


@dataclass
class RunnerConfig:
    mode: str  # live|replay
    duration_s: int | None = None
    limit: int | None = None
    namespace: str = "ai_one"
    prom_port: int | None = None
    out_dir: Path = Path("reports/run")
    set_flags: list[tuple[str, str]] | None = None
    report: bool = False
    forward_profiles: list[str] | None = None  # e.g. ["strong","soft"]
    # replay extras
    symbols: list[str] | None = None
    interval: str = "1m"
    source: str = "snapshot"
    metrics_grace_sec: int = 0  # секунди утримання метрик після постпроцесу (live)
    tidy_reports: bool = (
        False  # після завершення — розкласти артефакти у reports/summary та reports/replay
    )
    final_only: bool = (
        False  # залишити лише фінальні звіти (видалити проміжні після tidy)
    )


class RunnerOrchestrator:
    def __init__(self, cfg: RunnerConfig) -> None:
        self.cfg = cfg
        self.out_dir = cfg.out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.art_dir = self.out_dir / "artifacts"
        self.art_dir.mkdir(parents=True, exist_ok=True)
        self.run_log = self.out_dir / "run.log"
        self.metrics_txt = self.out_dir / "metrics.txt"
        self._scraper: _MetricsScraper | None = None

    def _cleanup_out_dir(self) -> None:
        """Зачистка попередніх артефактів перед стартом нового прогона.

        Видаляє:
          - run.log / metrics.txt / forward_*.md
          - quality.csv / quality_snapshot.md
          - summary.md / summary_*.md
          - artifacts/*
        Інші файли у каталозі не чіпає (для безпечного співіснування зовнішніх звітів).
        """
        patterns = [
            "run.log",
            "metrics.txt",
            "quality.csv",
            "quality_snapshot.md",
            "summary.md",
        ]
        # Динамічні патерни
        patterns.extend(
            [
                "forward_live.md",
                "forward_replay.md",
                "forward_strong.md",
                "forward_soft.md",
                "forward_explain.md",
            ]
        )
        try:
            for p in list(self.out_dir.glob("summary_*.md")):
                try:
                    p.unlink()
                except Exception:
                    pass
            for name in patterns:
                f = self.out_dir / name
                if f.exists():
                    try:
                        f.unlink()
                    except Exception:
                        pass
            # artifacts/*
            if self.art_dir.exists():
                for child in self.art_dir.iterdir():
                    try:
                        if child.is_file():
                            child.unlink()
                        elif child.is_dir():
                            import shutil

                            shutil.rmtree(child, ignore_errors=True)
                    except Exception:
                        pass
        except Exception:
            # Ніколи не блокуємо старт прогона через помилку зачистки
            pass

    def _prepare_env(self) -> None:
        sets = list(self.cfg.set_flags or [])
        # Always enforce namespace and optional prom port if provided
        if self.cfg.namespace:
            sets.append(("STATE_NAMESPACE", self.cfg.namespace))
        if self.cfg.prom_port is not None:
            sets.append(("PROM_HTTP_PORT", str(int(self.cfg.prom_port))))
            sets.append(("PROM_GAUGES_ENABLED", "true"))
        _apply_sets(sets)

    def _start_scraper(self) -> None:
        if self.cfg.prom_port:
            url = f"http://localhost:{int(self.cfg.prom_port)}/metrics"
            self._scraper = _MetricsScraper(url, self.metrics_txt, interval_s=15)
            self._scraper.start()

    def _stop_scraper(self) -> None:
        try:
            if self._scraper:
                self._scraper.stop()
        finally:
            self._scraper = None

    async def run_live(self) -> int:
        if not self.cfg.duration_s or self.cfg.duration_s <= 0:
            raise SystemExit("--duration must be > 0 for live mode")
        self._prepare_env()
        # Зачистка перед новим прогоном
        self._cleanup_out_dir()
        _tee_stdout_stderr(self.run_log)
        # lazy import after sets
        try:
            import importlib

            app_main = importlib.import_module("app.main")
            run_pipeline = app_main.run_pipeline  # type: ignore[attr-defined]
        except Exception as e:  # pragma: no cover
            print(f"❌ Не вдалося імпортувати app.main: {e}", flush=True)
            return 1
        self._start_scraper()
        task = asyncio.create_task(run_pipeline())
        stop = asyncio.Event()

        def _finish() -> None:
            stop.set()

        # timer
        timer_task = asyncio.create_task(asyncio.sleep(int(self.cfg.duration_s)))
        signal_task = asyncio.create_task(stop.wait())
        try:
            done, pending = await asyncio.wait(
                {task, timer_task, signal_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for p in pending:
                p.cancel()
                try:
                    await p
                except asyncio.CancelledError:
                    pass
            if task in done:
                return 0
            else:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                return 0
        finally:
            # Не зупиняємо scraper тут (live). Дамо можливість post_process() зробити grace-сон
            # і фінальний знімок метрик перед зупинкою.
            pass

    async def run_replay(self) -> int:
        if not self.cfg.limit or self.cfg.limit <= 0:
            raise SystemExit("--limit must be > 0 for replay mode")
        self._prepare_env()
        # Зачистка перед новим прогоном
        self._cleanup_out_dir()
        _tee_stdout_stderr(self.run_log)
        # Symbols default
        symbols = list(self.cfg.symbols or ["BTCUSDT", "ETHUSDT", "TONUSDT", "SNXUSDT"])
        # lazy import
        from tools.replay_stream import ReplayConfig, run_replay  # type: ignore

        rc = 0
        for sym in symbols:
            dump_dir = (
                self.art_dir
                / f"replay_{sym.lower()}_{self.cfg.interval}_{int(self.cfg.limit)}"
            )
            cfg = ReplayConfig(
                symbol=sym,
                interval=str(self.cfg.interval),
                source=str(self.cfg.source),
                limit=int(self.cfg.limit),
                dump_dir=dump_dir,
            )
            try:
                await run_replay(cfg)  # type: ignore[misc]
            except Exception as e:
                print(f"⚠️  Replay для {sym} помилка: {e}", flush=True)
                rc = 2
        # One last scrape (best-effort)
        if self.cfg.prom_port:
            try:
                with urllib.request.urlopen(
                    f"http://localhost:{int(self.cfg.prom_port)}/metrics", timeout=5
                ) as resp:
                    data = resp.read().decode("utf-8", errors="ignore")
                with self.metrics_txt.open("a", encoding="utf-8") as fh:
                    fh.write(f"### SNAPSHOT ts={_now_iso()}\n")
                    fh.write(data)
                    if not data.endswith("\n"):
                        fh.write("\n")
            except Exception:
                pass
        return rc

    def _run_module(self, module: str, args: list[str]) -> int:
        import subprocess

        cmd = [sys.executable, "-m", module, *args]
        try:
            p = subprocess.run(cmd, capture_output=True, text=True)
            # mirror outputs into run.log
            if p.stdout:
                print(p.stdout, end="")
            if p.stderr:
                print(p.stderr, end="", file=sys.stderr)
            return int(p.returncode or 0)
        except Exception as e:  # pragma: no cover
            print(f"⚠️  Не вдалося виконати {module}: {e}")
            return 1

    def _parse_histogram_stats(self, text: str) -> tuple[float, float, int] | None:
        # Expect label-free histogram ai_one_stage1_latency_ms
        import re

        sum_m = re.search(
            r"^ai_one_stage1_latency_ms_sum\s+([0-9eE+\-.]+)$", text, re.M
        )
        cnt_m = re.search(
            r"^ai_one_stage1_latency_ms_count\s+([0-9eE+\-.]+)$", text, re.M
        )
        buckets = []
        for m in re.finditer(
            r'^ai_one_stage1_latency_ms_bucket\{le="([0-9eE+\-.]+)"\}\s+([0-9eE+\-.]+)$',
            text,
            re.M,
        ):
            try:
                le = float(m.group(1))
                val = float(m.group(2))
                buckets.append((le, val))
            except Exception:
                continue
        if not (sum_m and cnt_m and buckets):
            return None
        try:
            total = float(cnt_m.group(1))
            ssum = float(sum_m.group(1))
        except Exception:
            return None
        buckets.sort(key=lambda kv: kv[0])
        threshold = 0.95 * total if total > 0 else 0.0
        p95 = 0.0
        for le, v in buckets:
            if v >= threshold:
                p95 = le
                break
        mean = (ssum / total) if total > 0 else 0.0
        return (p95, mean, int(total))

    def _parse_switch_totals(self, text: str) -> float:
        # Sum all series across from/to
        import re

        tot = 0.0
        for m in re.finditer(
            r"^ai_one_profile_switch_total\{[^}]*\}\s+([0-9eE+\-.]+)$", text, re.M
        ):
            try:
                tot += float(m.group(1))
            except Exception:
                continue
        return tot

    def _parse_metric_values(self, text: str, metric: str) -> dict[str, float]:
        import re

        pattern = re.compile(
            rf"^{re.escape(metric)}\{{([^}}]+)\}}\s+([0-9eE+\-.]+)$",
            re.M,
        )
        values: dict[str, float] = {}
        for match in pattern.finditer(text):
            labels = match.group(1)
            val_raw = match.group(2)
            sym_match = re.search(r'symbol="([^\"]+)"', labels)
            if not sym_match:
                continue
            try:
                values[sym_match.group(1)] = float(val_raw)
            except Exception:
                continue
        return values

    def _parse_gap_bucket_totals(self, text: str) -> dict[str, float]:
        import re

        pattern = re.compile(
            r"^ai_one_whale_publish_gap_bucket_total\{([^}]*)\}\s+([0-9eE+\-.]+)$",
            re.M,
        )
        totals: dict[str, float] = {}
        for match in pattern.finditer(text):
            labels = match.group(1)
            val_raw = match.group(2)
            le_match = re.search(r'le="([^"]+)"', labels)
            if not le_match:
                continue
            try:
                le_key = le_match.group(1)
                totals[le_key] = totals.get(le_key, 0.0) + float(val_raw)
            except Exception:
                continue
        return totals

    def _calc_stale_ratio(self, text: str) -> float | None:
        buckets = self._parse_gap_bucket_totals(text)
        total = float(buckets.get("inf", 0.0))
        if total <= 0.0:
            return None
        non_stale = float(buckets.get("10000", 0.0))
        stale = max(0.0, total - non_stale)
        # Захищаємося від невеликих флуктуацій лічильників
        ratio = max(0.0, min(1.0, stale / total))
        return ratio

    def _calc_stale_ratio_by_symbol(self, text: str) -> dict[str, float]:
        import re

        pattern = re.compile(
            r"^ai_one_whale_publish_gap_bucket_total\{([^}]*)\}\s+([0-9eE+\-.]+)$",
            re.M,
        )
        buckets: dict[str, dict[str, float]] = {}
        for match in pattern.finditer(text):
            labels = match.group(1)
            val_raw = match.group(2)
            sym_match = re.search(r'symbol="([^"]+)"', labels)
            le_match = re.search(r'le="([^"]+)"', labels)
            if not sym_match or not le_match:
                continue
            try:
                sym = sym_match.group(1)
                le_key = le_match.group(1)
                buckets.setdefault(sym, {})[le_key] = float(val_raw)
            except Exception:
                continue

        ratios: dict[str, float] = {}
        for sym, vals in buckets.items():
            total = float(vals.get("inf", 0.0))
            if total <= 0.0:
                continue
            non_stale = float(vals.get("10000", 0.0))
            stale = max(0.0, total - non_stale)
            ratios[sym] = max(0.0, min(1.0, stale / total))
        return ratios

    def _build_metrics_snapshot_lines(
        self, snaps: list[tuple[str, str]], switch_rate: float
    ) -> list[str]:
        if not snaps:
            return []
        text = snaps[-1][1]
        lines: list[str] = []
        stats = self._parse_histogram_stats(text)
        if stats:
            p95, mean, cnt = stats
            lines.append(
                f"- ai_one_stage1_latency_ms_p95={p95:.0f} (mean={mean:.0f}, count={cnt})\n"
            )
        else:
            lines.append("- ai_one_stage1_latency_ms_p95=NA\n")

        stale_ratio_global = self._calc_stale_ratio(text)
        stale_ratio_by_sym = self._calc_stale_ratio_by_symbol(text)
        if stale_ratio_by_sym:
            preview = ", ".join(
                f"{sym}:{ratio:.2f}"
                for sym, ratio in sorted(stale_ratio_by_sym.items())
            )
            lines.append(f"- stale_ratio_by_symbol={preview}\n")
        else:
            lines.append("- stale_ratio_by_symbol=NA\n")
        if stale_ratio_global is not None:
            lines.append(f"- stale_ratio_global={stale_ratio_global:.2f}\n")
        else:
            lines.append("- stale_ratio_global=NA\n")

        lines.append(f"- profile_switch_rate_1m={switch_rate:.2f}/хв\n")

        gap = self._parse_metric_values(text, "ai_one_whale_publish_gap_ms")
        if gap:
            top_gap = sorted(gap.items(), key=lambda kv: -kv[1])[:4]
            preview = ", ".join(f"{sym}:{val:.0f}" for sym, val in top_gap)
            lines.append(f"- whale_publish_gap_ms (топ): {preview}\n")
        gap_warn = self._parse_metric_values(
            text, "ai_one_whale_publish_gap_warn_total"
        )
        if gap_warn:
            total_warn = int(sum(gap_warn.values()))
            top_warn = ", ".join(
                f"{sym}:{int(val)}"
                for sym, val in sorted(gap_warn.items(), key=lambda kv: -kv[1])[:4]
            )
            lines.append(
                f"- whale_publish_gap_warn_total: {total_warn} ({top_warn or '—'})\n"
            )
        presence_warn = self._parse_metric_values(
            text, "ai_one_whale_presence_zero_nonstale_total"
        )
        if presence_warn:
            total_presence = int(sum(presence_warn.values()))
            top_presence = ", ".join(
                f"{sym}:{int(val)}"
                for sym, val in sorted(presence_warn.items(), key=lambda kv: -kv[1])[:4]
            )
            lines.append(
                f"- whale_presence_zero_nonstale_total: {total_presence} ({top_presence or '—'})\n"
            )
        return lines

    def _parse_forward_md(self, path: Path) -> list[tuple[int, int, float]]:
        """Розбір простого формату tools.forward_from_log:
        Лінії виду: "K=5: N=40 hit≈0.62" -> повертає [(k, n, hit), ...]
        """
        if not path.exists():
            return []
        import re

        rows: list[tuple[int, int, float]] = []
        try:
            txt = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return rows
        for ln in txt.splitlines():
            m = re.match(r"^K=(\d+):\s*N=(\d+)\s+hit≈([0-9.]+|nan)", ln.strip())
            if m:
                try:
                    k = int(m.group(1))
                    n = int(m.group(2))
                    hit = float("nan") if m.group(3) == "nan" else float(m.group(3))
                    rows.append((k, n, hit))
                except Exception:
                    continue
        return rows

    def _load_snapshots(self) -> list[tuple[str, str]]:
        # Returns list of (ts_iso, body)
        if not self.metrics_txt.exists():
            return []
        text = self.metrics_txt.read_text(encoding="utf-8", errors="ignore")
        if text.startswith("### SNAPSHOT ts="):
            text = "\n" + text
        parts = text.split("\n### SNAPSHOT ts=")
        snaps: list[tuple[str, str]] = []
        for i, part in enumerate(parts):
            if i == 0:
                # may contain leading content without marker
                continue
            # part begins with ISO + newline + metrics
            try:
                ts, body = part.split("\n", 1)
                snaps.append((ts.strip(), body))
            except Exception:
                continue
        return snaps

    def _calc_switch_rate(self, snaps: list[tuple[str, str]]) -> float:
        if len(snaps) < 2:
            return 0.0
        try:
            t0 = datetime.fromisoformat(snaps[0][0].replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(snaps[-1][0].replace("Z", "+00:00"))
            minutes = max(1.0, (t1 - t0).total_seconds() / 60.0)
        except Exception:
            minutes = max(1.0, (len(snaps) - 1) * 0.25)  # ~15s cadence
        v0 = self._parse_switch_totals(snaps[0][1])
        v1 = self._parse_switch_totals(snaps[-1][1])
        return max(0.0, (v1 - v0) / minutes)

    def generate_summary(self) -> Path:
        mode = self.cfg.mode
        out = (
            self.out_dir
            / f"summary_{datetime.now(tz=UTC).strftime('%Y-%m-%d_%H%M%SZ')}.md"
        )
        snaps = self._load_snapshots()
        # KPI defaults
        p95: float = 0.0
        mean: float = 0.0
        cnt: int = 0
        switch_rate: float = 0.0
        fb_total: int = 0
        expcov_global: float = 0.0
        per_sym: dict[str, float] = {}
        stale_ratio: float | None = None

        # Metrics-derived KPI або офлайн fallback
        if snaps:
            last = snaps[-1][1]
            stats = self._parse_histogram_stats(last)
            if stats:
                p95, mean, cnt = stats
            stale_ratio = self._calc_stale_ratio(last)
            fb_values: dict[str, float] = {}
            for _ts, body in snaps:
                metrics = self._parse_metric_values(body, "ai_one_false_breakout_total")
                if not metrics:
                    continue
                fb_values.update(metrics)
            fb_total = int(sum(fb_values.values()))
            switch_rate = self._calc_switch_rate(snaps)
        else:
            # Offline KPI з run.log (latency_ms=, profile перемикання)
            latencies: list[int] = []
            switches = 0
            t_min: int | None = None
            t_max: int | None = None
            log_path = self.run_log
            if log_path.exists():
                import re as _re_off

                for ln in log_path.read_text(
                    encoding="utf-8", errors="ignore"
                ).splitlines():
                    m_lat = _re_off.search(r"latency_ms=(\d+)", ln)
                    if m_lat:
                        try:
                            v = int(m_lat.group(1))
                            latencies.append(v)
                        except Exception:
                            pass
                    # timestamp epoch ms (ts=...)
                    m_ts = _re_off.search(r"\bts=(\d{10,13})\b", ln)
                    if m_ts:
                        try:
                            ts_val = int(m_ts.group(1))
                            if len(m_ts.group(1)) == 10:
                                ts_val *= 1000
                            if t_min is None or ts_val < t_min:
                                t_min = ts_val
                            if t_max is None or ts_val > t_max:
                                t_max = ts_val
                        except Exception:
                            pass
                    if "profile_switch" in ln.lower():
                        switches += 1
            if latencies:
                latencies.sort()
                cnt = len(latencies)
                p95_idx = int(
                    min(len(latencies) - 1, round(0.95 * (len(latencies) - 1)))
                )
                p95 = float(latencies[p95_idx])
                mean = float(sum(latencies) / len(latencies))
            if (
                switches > 0
                and t_min is not None
                and t_max is not None
                and t_max > t_min
            ):
                minutes_span = max(1.0, (t_max - t_min) / 1000.0 / 60.0)
                switch_rate = switches / minutes_span
            # fb_total залишаємо 0 (немає даних офлайн)
            offline_note = True
        offline_note = locals().get("offline_note", False)

        # ExpCov from quality.csv (weighted by activations if available)
        qcsv = self.out_dir / "quality.csv"
        expcov_na = False
        if qcsv.exists():
            import csv

            total_act = 0.0
            wsum = 0.0
            any_cov_positive = False
            with qcsv.open("r", encoding="utf-8", newline="") as fh:
                rd = csv.DictReader(fh)
                for r in rd:
                    sym = str(r.get("symbol", "")).upper()
                    try:
                        cov = float(r.get("explain_coverage_rate") or 0.0)
                        act = float(r.get("activations") or 0.0)
                    except Exception:
                        cov, act = 0.0, 0.0
                    if sym:
                        per_sym[sym] = max(per_sym.get(sym, 0.0), cov)
                    if cov > 0.0:
                        any_cov_positive = True
                    total_act += act
                    wsum += cov * act
            expcov_global = (
                (wsum / total_act)
                if total_act > 0
                else (sum(per_sym.values()) / max(1, len(per_sym)))
            )
            # Якщо всі значення покриття == 0.0 → вважаємо ExpCov як NA (zero events)
            if not any_cov_positive:
                expcov_na = True
        # Forward md
        fwd_path = self.out_dir / f"forward_{mode}.md"
        fwd_block = ""
        if fwd_path.exists():
            try:
                fwd_text = fwd_path.read_text(encoding="utf-8", errors="ignore")
                # include only table-like lines if present
                lines = [
                    ln
                    for ln in fwd_text.splitlines()
                    if "|" in ln or ln.strip().startswith("K=")
                ]
                fwd_block = "\n".join(lines[:200])
            except Exception:
                fwd_block = ""
        # Scenarios top from quality.csv
        scenarios_summary: dict[str, dict[str, float]] = {}
        if (self.out_dir / "quality.csv").exists():
            import csv

            with (self.out_dir / "quality.csv").open(
                "r", encoding="utf-8", newline=""
            ) as fh:
                rd = csv.DictReader(fh)
                for r in rd:
                    scen = str(r.get("scenario", ""))
                    try:
                        act = float(r.get("activations") or 0.0)
                        p75 = float(r.get("p75_conf") or 0.0)
                        tts = float(r.get("mean_time_to_stabilize_s") or 0.0)
                    except Exception:
                        act, p75, tts = 0.0, 0.0, 0.0
                    s = scenarios_summary.setdefault(
                        scen, {"act": 0.0, "p75": 0.0, "tts": 0.0, "n": 0.0}
                    )
                    s["act"] += act
                    s["p75"] += p75
                    s["tts"] += tts
                    s["n"] += 1.0
        tops = sorted(
            ((k, v) for k, v in scenarios_summary.items()), key=lambda kv: -kv[1]["act"]
        )[:5]
        ts = _now_iso()
        lines: list[str] = []
        lines.append(f"# AiOne_t • Run Summary  (mode={mode})  • {ts}\n\n")
        # Flags
        lines.append("## Flags\n")
        flags_preview = [
            f"STATE_NAMESPACE={self.cfg.namespace}",
            f"PROM_PORT={self.cfg.prom_port}",
        ]
        lines.append("\n".join(flags_preview) + "\n\n")
        # KPI
        lines.append("## KPI\n")
        lines.append(
            f"- Stage1 p95 latency: {p95:.0f} (mean={mean:.0f}, count={cnt})  [ai_one_stage1_latency_ms]\n"
        )
        lines.append(
            f"- switch_rate: {switch_rate:.2f}/хв  [ai_one_profile_switch_total]\n"
        )
        lines.append(
            f"- false_breakout_total: {fb_total}  [ai_one_false_breakout_total]\n"
        )
        if per_sym:
            sym_table = ", ".join(f"{k}:{v:.2f}" for k, v in sorted(per_sym.items()))
        else:
            sym_table = "-"
        if "expcov_na" in locals() and expcov_na:
            lines.append(
                f"- ExpCov: {expcov_global:.2f} (NA)  | по символах: {sym_table}\n"
            )
        else:
            lines.append(f"- ExpCov: {expcov_global:.2f}  | по символах: {sym_table}\n")
        if fwd_block:
            lines.append("- Forward K (K=3/5/10/20/30): див. секцію нижче\n")
        # Scenarios
        if tops:
            lines.append("- Сценарії (топ-5 за активаціями):\n")
            for name, agg in tops:
                n = max(1.0, agg.get("n", 1.0))
                lines.append(
                    f"  - {name}: acts={int(agg.get('act',0))}, p75_conf={agg.get('p75',0.0)/n:.2f}, mean_tts(s)={agg.get('tts',0.0)/n:.2f}\n"
                )
        lines.append("\n")
        # Observability
        lines.append("## Observability\n")
        if snaps:
            lines.append(
                f"- Prometheus snapshot: {self.metrics_txt} (ост. зріз {snaps[-1][0]})\n"
            )
        else:
            lines.append("- Prometheus snapshot: (немає)\n")
        lines.append("\n")
        lines.append("## Metrics snapshot\n")
        if snaps:
            lines.extend(self._build_metrics_snapshot_lines(snaps, switch_rate))
        else:
            lines.extend(
                [
                    "- ai_one_stage1_latency_ms_p95=NA\n",
                    "- stale_ratio_by_symbol=NA\n",
                    "- stale_ratio_global=NA\n",
                    "- profile_switch_rate_1m=NA\n",
                ]
            )
        lines.append("\n")
        if offline_note:
            lines.append("## Offline KPI (no metrics endpoint)\n")
            # Новий формат: офлайн-метрики позначаємо як n/a, а ExpCov беремо з quality.csv (або 0.00)
            expcov_off = expcov_global if isinstance(expcov_global, float) else 0.0
            lines.append(
                f"p95_ms_offline=n/a, mean_ms_offline=n/a, switch_per_min_offline=n/a, ExpCov_from_quality={expcov_off:.2f}\n"
            )
            lines.append(
                "Notes: metrics snapshot n/a; KPIs computed from quality.csv/snapshot\n\n"
            )
        # Artifacts
        lines.append("## Artifacts\n")
        lines.append(
            "- run.log, quality.csv, quality_snapshot.md, forward_"
            + mode
            + ".md, metrics.txt, artifacts/*\n\n"
        )
        # Forward block if any
        if fwd_block:
            lines.append("## Forward (витяг)\n\n")
            lines.append(fwd_block + "\n\n")

        # Forward profiles (опціонально, якщо згенеровані)
        profs = list(self.cfg.forward_profiles or [])
        # Якщо прапор не задано, але файли існують — також підхопимо
        discovered: list[str] = []
        for name in ("strong", "soft", "explain", "hint", "quality"):
            if (self.out_dir / f"forward_{name}.md").exists():
                discovered.append(name)
        for n in discovered:
            if n not in profs:
                profs.append(n)
        # Впорядковуємо за пріоритетом strong > soft > explain
        _pri = {"strong": 0, "soft": 1, "explain": 2}
        profs = [
            p for p in profs if p in ("strong", "soft", "explain", "hint", "quality")
        ]
        # hint має найнижчий пріоритет у виводі
        _pri_hint = {"strong": 0, "soft": 1, "explain": 2, "hint": 3, "quality": 4}
        profs.sort(key=lambda n: _pri_hint.get(n, 99))
        if profs:
            lines.append("## Forward profiles\n\n")
            lines.append(
                "profile | N | win_rate(K=5/10) | median_ttf_0.5% | median_ttf_1.0% | dedup_dropped_total | skew_dropped_total | notes\n"
            )
            lines.append("---|---:|---:|---:|---:|---:|---:|---\n")
            n_totals: dict[str, int] = {}
            from_tags: dict[str, str] = {}
            # Коротка таблиця
            for name in profs:
                rows = self._parse_forward_md(self.out_dir / f"forward_{name}.md")
                by_k = {int(k): (k, n, h) for k, n, h in rows}
                n5 = by_k.get(5, (5, 0, float("nan")))[1]
                h5 = by_k.get(5, (5, 0, float("nan")))[2]
                h10 = by_k.get(10, (10, 0, float("nan")))[2]
                n_total = sum(n for _, n, _ in rows) if rows else 0
                n_totals[name] = int(n_total)
                n_display = n5 if n5 else n_total
                h5s = f"{h5:.2f}" if h5 == h5 else "nan"
                h10s = f"{h10:.2f}" if h10 == h10 else "nan"
                # футер
                ttf05_med = "nan"
                ttf10_med = "nan"
                dedup_total = "-"
                skew_total = "-"
                notes = "-"
                try:
                    import re as _re_fp

                    txt = (self.out_dir / f"forward_{name}.md").read_text(
                        encoding="utf-8", errors="ignore"
                    )
                    m05 = _re_fp.search(r"ttf05_median=([0-9.]+|nan)", txt)
                    if m05:
                        ttf05_med = m05.group(1)
                    m10 = _re_fp.search(r"ttf10_median=([0-9.]+|nan)", txt)
                    if m10:
                        ttf10_med = m10.group(1)
                    m_dd = _re_fp.search(r"dedup_dropped=(\d+)", txt)
                    if m_dd:
                        dedup_total = m_dd.group(1)
                    m_sk = _re_fp.search(r"skew_dropped=(\d+)", txt)
                    if m_sk:
                        skew_total = m_sk.group(1)
                    m_from = _re_fp.search(r"params=\{from:([^}]+)\}", txt)
                    if m_from:
                        from_tags[name] = m_from.group(1)
                    if "note=too_short_window" in txt:
                        notes = (notes + ";" if notes != "-" else "") + "short_window"
                except Exception:
                    pass
                lines.append(
                    f"{name} | {n_display} | {h5s}/{h10s} | {ttf05_med} | {ttf10_med} | {dedup_total} | {skew_total} | {notes}\n"
                )
            # Детальна розкладка
            lines.append("\nprofile | K | N | hit_rate | med_ret% | p75_abs%\n")
            lines.append("---|---:|---:|---:|---:|---:\n")
            for name in profs:
                rows = self._parse_forward_md(self.out_dir / f"forward_{name}.md")
                for k, n, hit in sorted(rows, key=lambda r: int(r[0])):
                    hit_str = f"{hit:.2f}" if hit == hit else "nan"
                    lines.append(f"{name} | {k} | {n} | {hit_str} | n/a | n/a\n")
            # OfflineForward acceptance
            try:
                base_zero = all(
                    int(n_totals.get(p, 0)) == 0
                    for p in ("strong", "soft", "explain", "hint")
                )
                quality_n = int(n_totals.get("quality", 0))
                if base_zero and quality_n > 0:
                    src = from_tags.get("quality", "quality_snapshot.md")
                    src_note = (
                        "from snapshot" if "snapshot" in src else "from quality.csv"
                    )
                    lines.append(
                        f"OfflineForward: quality_profile N>0 ({src_note})\n\n"
                    )
                    offline_forward_ok = True
                else:
                    offline_forward_ok = False
            except Exception:
                offline_forward_ok = False
            lines.append("\n### Forward profile params\n\n")
            lines.append(
                "profile | presence_min | bias_abs_min | whale_max_age_sec | explain_ttl_sec | dedup_dropped_total | skew_dropped_total | score_min\n"
            )
            lines.append("---|---:|---:|---:|---:|---:|---:|---:\n")
            for name in profs:
                try:
                    import re as _re_pp

                    txt = (self.out_dir / f"forward_{name}.md").read_text(
                        encoding="utf-8", errors="ignore"
                    )
                except Exception:
                    txt = ""
                m_presence = _re_pp.search(r"presence_min:([0-9.]+)", txt)
                m_bias = _re_pp.search(r"bias_abs_min:([0-9.]+)", txt)
                m_whale = _re_pp.search(r"whale_max_age_sec=(\d+)", txt)
                m_ttl = _re_pp.search(r"explain_ttl_sec=(\d+)", txt) or _re_pp.search(
                    r"ttl_sec=(\d+)", txt
                )
                m_score = _re_pp.search(r"score_min:([0-9.]+)", txt)
                m_dd = _re_pp.search(r"dedup_dropped=(\d+)", txt)
                m_sk = _re_pp.search(r"skew_dropped=(\d+)", txt)
                p_presence = m_presence.group(1) if m_presence else "-"
                p_bias = m_bias.group(1) if m_bias else "-"
                p_whale = m_whale.group(1) if m_whale else "-"
                p_ttl = m_ttl.group(1) if m_ttl else "-"
                p_score = (
                    m_score.group(1)
                    if m_score
                    else (
                        "0.55"
                        if name == "hint"
                        else ("n/a" if name == "quality" else "-")
                    )
                )
                p_dedup = m_dd.group(1) if m_dd else "-"
                p_skew = m_sk.group(1) if m_sk else "-"
                if name in ("hint", "quality"):
                    lines.append(
                        f"{name} | n/a | n/a | n/a | n/a | {p_dedup} | {p_skew} | {p_score}\n"
                    )
                else:
                    lines.append(
                        f"{name} | {p_presence} | {p_bias} | {p_whale} | {p_ttl} | {p_dedup} | {p_skew} | -\n"
                    )
            lines.append("\n### Acceptance\n\n")
            offline_metrics = bool(offline_note)

            def _pf(cond: bool | None) -> str:
                if cond is None:
                    return "NA"
                return "PASS" if cond else "FAIL"

            rule_p95 = None if offline_metrics else (p95 <= 200.0)
            lines.append(f"- p95(Stage1) ≤ 200 ms: {_pf(rule_p95)}\n")
            rule_sw = None if offline_metrics else (switch_rate <= 2.0)
            lines.append(f"- switch_rate ≤ 2/min: {_pf(rule_sw)}\n")
            rule_stale = None if stale_ratio is None else (stale_ratio < 0.15)
            if stale_ratio is None:
                lines.append(f"- stale_ratio < 15%: {_pf(rule_stale)}\n")
            else:
                lines.append(
                    f"- stale_ratio < 15%: {_pf(rule_stale)} (current={stale_ratio:.2f})\n"
                )
            explain_path = self.out_dir / "forward_explain.md"
            fwd_explain_ok = None
            explain_ttf_ok = None
            if explain_path.exists():
                try:
                    txt = explain_path.read_text(encoding="utf-8", errors="ignore")
                    import re as _re_acc

                    m_nt = _re_acc.search(r"N_total=(\d+)", txt)
                    m_ttf05 = _re_acc.search(r"ttf05_median=([0-9.]+|nan)", txt)
                    m_ttf10 = _re_acc.search(r"ttf10_median=([0-9.]+|nan)", txt)
                    if m_nt:
                        fwd_explain_ok = int(m_nt.group(1)) > 0
                    if m_ttf05 and m_ttf10 and fwd_explain_ok:
                        try:
                            v05 = (
                                float(m_ttf05.group(1))
                                if m_ttf05.group(1) != "nan"
                                else float("nan")
                            )
                            v10 = (
                                float(m_ttf10.group(1))
                                if m_ttf10.group(1) != "nan"
                                else float("nan")
                            )
                            explain_ttf_ok = (v05 == v05) and (v10 == v10)
                        except Exception:
                            explain_ttf_ok = False
                except Exception:
                    pass
            lines.append(
                f"- Forward explain N>0 & median_ttf_0.5/1.0: {_pf(fwd_explain_ok and explain_ttf_ok if fwd_explain_ok is not None and explain_ttf_ok is not None else None)}\n"
            )
            lines.append(
                "- BTC/ETH win_rate K=5/10 ≥ baseline, FP(grab_*) ≤ baseline: NA\n"
            )
            if offline_metrics and locals().get("offline_forward_ok", False):
                lines.append(
                    "Notes: offline forward via quality_profile; metrics unavailable.\n"
                )
            elif offline_metrics:
                lines.append("Notes: metrics unavailable; forward evaluated offline.\n")
            lines.append("\n")
        # Verdict
        lines.append("## Verdict\n")
        verdict = "GO"
        reason: str | None = None
        # Якщо лише офлайн і quality дав N>0 — встановлюємо WARN (offline-only)
        offline_only = (not snaps) and locals().get("offline_forward_ok", False)
        if offline_only:
            verdict = "WARN"
            reason = "offline-only"
        if p95 > 200.0:
            verdict = "WARN" if p95 <= 280.0 else "NO-GO"
            reason = "latency"
        if switch_rate > 2.0:
            verdict = "WARN" if verdict == "GO" else verdict
            reason = (reason + ", ") if reason else ""
            reason = f"{reason}switch_rate"
        # ExpCov acceptance: require >= 0.60 for GO; if no data (0.0) — warn explicitly
        if not ("expcov_na" in locals() and expcov_na):
            if expcov_global <= 0.0:
                verdict = "WARN" if verdict == "GO" else verdict
                reason = (reason + ", ") if reason else ""
                reason = f"{reason}ExpCov=0"
            elif expcov_global < 0.60:
                verdict = "WARN" if verdict == "GO" else verdict
                reason = (reason + ", ") if reason else ""
                reason = f"{reason}ExpCov<0.60"
        if reason:
            lines.append(f"- {verdict} — причина: {reason}\n")
        else:
            lines.append(f"- {verdict}\n")
        # acceptance позначка офлайн
        if locals().get("offline_forward_ok", False):
            lines.append("- acceptance: offline_OK\n")
            if offline_only:
                lines.append("- TODO: зібрати метрики при першій можливості\n")
        out.write_text("".join(lines), encoding="utf-8")
        return out

    def write_readme(self) -> None:
        lines = [
            f"# Run artifacts ({self.cfg.mode})\n\n",
            "- run.log\n",
            "- metrics.txt\n",
            "- quality.csv (якщо --report)\n",
            "- quality_snapshot.md (якщо --report)\n",
            f"- forward_{self.cfg.mode}.md (якщо --report)\n",
            "- forward_strong.md / forward_soft.md / forward_explain.md (якщо --report і увімкнені профілі)\n",
            "- summary_*.md (якщо --report)\n",
            "- artifacts/*\n",
        ]
        (self.out_dir / "README.md").write_text("".join(lines), encoding="utf-8")

    def post_process(self) -> None:
        if not self.cfg.report:
            return
        # Перед будь-яким переміщенням файлів у tidy треба закрити tee, щоб run.log не був заблокований
        try:
            _close_tee()
        except Exception:
            pass
        # 1) quality.csv з нашого run.log
        q_csv = self.out_dir / "quality.csv"
        self._run_module(
            "tools.scenario_quality_report",
            ["--logs", str(self.run_log), "--out", str(q_csv)],
        )
        # 2) quality_snapshot.md із metrics_dir=self.out_dir
        self._run_module(
            "tools.quality_snapshot",
            [
                "--csv",
                str(q_csv),
                "--metrics-dir",
                str(self.out_dir),
                "--out",
                str(self.out_dir / "quality_snapshot.md"),
                "--forward-k",
                "3,5,10,20,30",
                "--forward-enable-long",
                "true",
            ],
        )
        # 3) forward_<mode>.md (фільтрований forward)
        try:
            from types import SimpleNamespace

            from tools import forward_from_log as fwd

            ns = SimpleNamespace(
                log=str(self.run_log),
                out=str(self.out_dir / f"forward_{self.cfg.mode}.md"),
                k=[3, 5, 10, 20, 30],
                presence_min=0.75,
                bias_abs_min=0.6,
                whale_max_age_sec=600,
                source="whale",
                explain_ttl_sec=600,
            )
            asyncio.run(fwd.run(ns))
        except Exception:
            # fallback через -m, якщо прямий виклик не вдався
            self._run_module(
                "tools.forward_from_log",
                [
                    "--log",
                    str(self.run_log),
                    "--out",
                    str(self.out_dir / f"forward_{self.cfg.mode}.md"),
                    "--k",
                    "3",
                    "5",
                    "10",
                    "20",
                    "30",
                    "--presence-min",
                    "0.75",
                    "--bias-abs-min",
                    "0.6",
                    "--source",
                    "whale",
                ],
            )

        # 3b) Додаткові профілі forward (опційно)
        profiles = list(self.cfg.forward_profiles or [])
        # Валідація та впорядкування за пріоритетом strong > soft > explain
        _pri = {"strong": 0, "soft": 1, "explain": 2}
        profiles = [p for p in profiles if p in FORWARD_PROFILES]
        profiles.sort(key=lambda n: _pri.get(n, 99))

        # Спільний dedup-файл між профілями, щоб одна подія не потрапила у кілька звітів
        dedup_path = self.art_dir / "forward_dedup.keys"
        try:
            dedup_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        for name in profiles:
            spec = FORWARD_PROFILES.get(name)
            if not spec:
                continue
            out_path = self.out_dir / f"forward_{name}.md"
            try:
                from types import SimpleNamespace

                from tools import forward_from_log as fwd

                # Вибір параметрів залежно від джерела
                source = str(spec.get("source", "whale"))
                if source == "explain":
                    ns = SimpleNamespace(
                        log=str(self.run_log),
                        out=str(out_path),
                        k=[3, 5, 10, 20, 30],
                        presence_min=float(spec["presence"]),
                        bias_abs_min=float(spec["bias"]),
                        source="explain",
                        explain_ttl_sec=int(spec.get("ttl", 600)),
                        dedup_file=str(dedup_path),
                    )
                else:
                    ns = SimpleNamespace(
                        log=str(self.run_log),
                        out=str(out_path),
                        k=[3, 5, 10, 20, 30],
                        presence_min=float(spec["presence"]),
                        bias_abs_min=float(spec["bias"]),
                        whale_max_age_sec=int(spec["whale"]),
                        source="whale",
                        dedup_file=str(dedup_path),
                    )
                asyncio.run(fwd.run(ns))
            except Exception:
                # fallback через -m
                args = [
                    "--log",
                    str(self.run_log),
                    "--out",
                    str(out_path),
                    "--k",
                    "3",
                    "5",
                    "10",
                    "20",
                    "30",
                    "--presence-min",
                    str(spec["presence"]),
                    "--bias-abs-min",
                    str(spec["bias"]),
                    "--dedup-file",
                    str(dedup_path),
                ]
                source = str(spec.get("source", "whale"))
                if source == "explain":
                    args.extend(
                        [
                            "--source",
                            "explain",
                            "--explain-ttl-sec",
                            str(spec.get("ttl", 600)),
                        ]
                    )
                else:
                    args.extend(
                        ["--source", "whale", "--whale-max-age-sec", str(spec["whale"])]
                    )
                self._run_module("tools.forward_from_log", args)
        # 4) summary (завжди генеруємо, незалежно від шляху до forward)
        summary_path = self.generate_summary()
        # Дублюємо останній summary у сталий шлях summary.md для простішого доступу
        try:
            import shutil

            shutil.copyfile(summary_path, self.out_dir / "summary.md")
        except Exception:
            pass
        # 5) README index
        self.write_readme()

        # 6) (live) Grace для метрик і фінальний знімок, потім зупинка скрейпера
        if self.cfg.prom_port:
            # Якщо задано grace — почекаємо, поки forward/якість встигнуть емітнути метрики
            try:
                import time as _time

                g = max(0, int(self.cfg.metrics_grace_sec or 0))
            except Exception:
                g = 0
            if g > 0:
                try:
                    print(
                        f"[Runner] Grace-період для метрик {g}s перед фінальним знімком...",
                        flush=True,
                    )
                except Exception:
                    pass
                try:
                    _time.sleep(g)
                except Exception:
                    pass
            # One-shot фінальний знімок /metrics
            try:
                with urllib.request.urlopen(
                    f"http://localhost:{int(self.cfg.prom_port)}/metrics", timeout=5
                ) as resp:
                    data = resp.read().decode("utf-8", errors="ignore")
                with self.metrics_txt.open("a", encoding="utf-8") as fh:
                    fh.write(f"### SNAPSHOT ts={_now_iso()}\n")
                    fh.write(data)
                    if not data.endswith("\n"):
                        fh.write("\n")
            except Exception:
                pass
            # Тепер можна зупинити скрейпер, якщо він ще працює
            self._stop_scraper()

        # 7) Опційно впорядкувати звіти: перемістити фінальні у reports/summary, проміжні у reports/replay
        tidy = bool(getattr(self.cfg, "tidy_reports", False))
        if tidy or bool(getattr(self.cfg, "final_only", False)):
            try:
                # active-min=0, щоб поточний ран також було переміщено одразу після завершення
                self._run_module("tools.reports_tidy", ["--apply", "--active-min", "0"])
            except Exception:
                pass
        # 8) Якщо задано final_only — видалити проміжні артефакти для цього ран-а
        if getattr(self.cfg, "final_only", False):
            try:
                base = self.out_dir.parent
                run_name = self.out_dir.name
                replay_dir = base / "replay" / run_name
                # видаляємо папку з проміжними
                if replay_dir.exists():
                    import shutil as _sh

                    _sh.rmtree(replay_dir, ignore_errors=True)
                # спробувати прибрати вихідний out_dir, якщо він ще існує
                try:
                    if self.out_dir.exists():
                        import shutil as _sh2

                        _sh2.rmtree(self.out_dir, ignore_errors=True)
                except Exception:
                    pass
            except Exception:
                pass

    # ── Standalone postprocess (offline) ────────────────────────────────────
    def run_standalone_postprocess(self) -> int:
        """Generate forward_* and summary.md from an existing out-dir.

        Prerequisites in self.out_dir:
          - run.log (required)
          - metrics.txt (optional but recommended)
        """
        # Ensure out_dir exists
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.art_dir.mkdir(parents=True, exist_ok=True)

        if not self.run_log.exists():
            print(f"[postprocess] run.log not found in {self.out_dir}")
            return 1

        # 1) quality.csv
        q_csv = self.out_dir / "quality.csv"
        try:
            self._run_module(
                "tools.scenario_quality_report",
                ["--logs", str(self.run_log), "--out", str(q_csv)],
            )
        except Exception as e:
            print(f"[postprocess] quality report failed: {e}")

        # 2) quality_snapshot.md (metrics-dir=self.out_dir)
        try:
            self._run_module(
                "tools.quality_snapshot",
                [
                    "--csv",
                    str(q_csv),
                    "--metrics-dir",
                    str(self.out_dir),
                    "--out",
                    str(self.out_dir / "quality_snapshot.md"),
                    "--forward-k",
                    "3,5,10,20,30",
                    "--forward-enable-long",
                    "true",
                ],
            )
        except Exception as e:
            print(f"[postprocess] quality snapshot failed: {e}")

        # 3) forward profiles (strong/soft/explain)
        profiles = list(self.cfg.forward_profiles or ["strong", "soft", "explain"])  # type: ignore[attr-defined]
        # Validate subset
        profiles = [p for p in profiles if p in FORWARD_PROFILES]
        # Dedup file shared
        dedup_path = self.art_dir / "forward_dedup.keys"
        try:
            dedup_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        # Build spec with CLI overrides
        presence_override = getattr(self.cfg, "pp_presence_min", None)
        bias_override = getattr(self.cfg, "pp_bias_abs_min", None)
        whale_override = getattr(self.cfg, "pp_whale_max_age_sec", None)
        ttl_override = getattr(self.cfg, "pp_explain_ttl_sec", None)

        for name in profiles:
            spec = dict(FORWARD_PROFILES.get(name, {}))
            if presence_override is not None:
                if "presence" in spec:
                    spec["presence"] = float(presence_override)
            if bias_override is not None:
                if "bias" in spec:
                    spec["bias"] = float(bias_override)
            if name in ("strong", "soft") and whale_override is not None:
                spec["whale"] = int(whale_override)
            if name == "explain" and ttl_override is not None:
                spec["ttl"] = int(ttl_override)

            out_path = self.out_dir / f"forward_{name}.md"
            args = [
                "--log",
                str(self.run_log),
                "--out",
                str(out_path),
                "--k",
                "3",
                "5",
                "10",
                "20",
                "30",
                "--presence-min",
                str(spec.get("presence", 0.0)),
                "--bias-abs-min",
                str(spec.get("bias", 0.0)),
                "--dedup-file",
                str(dedup_path),
            ]
            source = str(spec.get("source", "whale"))
            if source == "explain":
                args.extend(
                    [
                        "--source",
                        "explain",
                        "--explain-ttl-sec",
                        str(int(spec.get("ttl", 600))),
                    ]
                )
            else:
                args.extend(
                    [
                        "--source",
                        "whale",
                        "--whale-max-age-sec",
                        str(int(spec.get("whale", 600))),
                    ]
                )
            try:
                self._run_module("tools.forward_from_log", args)
            except Exception as e:
                print(f"[postprocess] forward {name} failed: {e}")

        # 3b) Explain fallback: if missing or N=0 → rebuild with soft thresholds and TTL=1200
        exp_path = self.out_dir / "forward_explain.md"
        need_fallback = True
        if exp_path.exists():
            try:
                txt = exp_path.read_text(encoding="utf-8", errors="ignore")
                # Heuristic: footer contains 'N=0'
                need_fallback = "N=0" in txt
            except Exception:
                need_fallback = True
        if need_fallback:
            try:
                fb_args = [
                    "--log",
                    str(self.run_log),
                    "--out",
                    str(exp_path),
                    "--k",
                    "3",
                    "5",
                    "10",
                    "20",
                    "30",
                    "--presence-min",
                    str(0.35),
                    "--bias-abs-min",
                    str(0.20),
                    "--source",
                    "explain",
                    "--explain-ttl-sec",
                    str(int(ttl_override or 1200)),
                    "--dedup-file",
                    str(dedup_path),
                ]
                self._run_module("tools.forward_from_log", fb_args)
            except Exception as e:
                print(f"[postprocess] explain fallback failed: {e}")

        # 3c) Hint fallback (smoke): якщо всі strong/soft/explain мають N=0 →
        #  спершу прогін activation-only; якщо N=0 → другий прогін зі score_min=0.45.
        try:

            def _has_events(path: Path) -> bool:
                if not path.exists():
                    return False
                try:
                    txt = path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    return False
                import re as _re_h

                return bool(
                    _re_h.search(r"N_total=(?!0)\d+", txt)
                    or _re_h.search(r"K=\d+: N=(?!0)\d+", txt)
                )

            all_zero = all(
                not _has_events(self.out_dir / f"forward_{name}.md")
                for name in ("strong", "soft", "explain")
            )
            if all_zero:
                print(
                    "[SMOKE_HINT] strong/soft/explain порожні → запускаємо hint (activation-only)"
                )
                hint_path = self.out_dir / "forward_hint.md"
                hint_args1 = [
                    "--log",
                    str(self.run_log),
                    "--out",
                    str(hint_path),
                    "--k",
                    "3",
                    "5",
                    "10",
                    "20",
                    "30",
                    "--source",
                    "hint",
                    "--score-min",
                    "-1",
                    "--dir-field",
                    "dir",
                    "--dedup-file",
                    str(dedup_path),
                ]
                self._run_module("tools.forward_from_log", hint_args1)
                # Перевіряємо чи зʼявились події
                if not _has_events(hint_path):
                    print("[SMOKE_HINT] activation-only N=0 → пробуємо score_min=0.45")
                    hint_args2 = [
                        "--log",
                        str(self.run_log),
                        "--out",
                        str(hint_path),
                        "--k",
                        "3",
                        "5",
                        "10",
                        "20",
                        "30",
                        "--source",
                        "hint",
                        "--score-min",
                        "0.45",
                        "--dir-field",
                        "dir",
                        "--dedup-file",
                        str(dedup_path),
                    ]
                    self._run_module("tools.forward_from_log", hint_args2)
        except Exception as e:
            print(f"[postprocess] hint smoke fallback failed: {e}")

        # 3d) Quality fallback: якщо й hint не дав подій (усі strong/soft/explain/hint N=0)
        try:
            all_zero = True
            for name in ("strong", "soft", "explain", "hint"):
                pth = self.out_dir / f"forward_{name}.md"
                if not pth.exists():
                    continue  # відсутній файл не знімає all_zero
                try:
                    txt = pth.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                import re as _re_q

                if _re_q.search(r"N_total=(?!0)\d+", txt) or _re_q.search(
                    r"K=\d+: N=(?!0)\d+", txt
                ):
                    all_zero = False
                    break
            if all_zero:
                quality_args = [
                    "--log",
                    str(
                        self.run_log
                    ),  # для уніфікованого інтерфейсу, не використовується у source=quality
                    "--out",
                    str(self.out_dir / "forward_quality.md"),
                    "--k",
                    "3",
                    "5",
                    "10",
                    "20",
                    "30",
                    "--source",
                    "quality",
                    "--quality-csv",
                    str(self.out_dir / "quality.csv"),
                    "--dedup-file",
                    str(dedup_path),
                ]
                self._run_module("tools.forward_from_log", quality_args)
        except Exception as e:
            print(f"[postprocess] quality fallback failed: {e}")

        # 4) summary
        try:
            summary_path = self.generate_summary()
            import shutil as _sh

            _sh.copyfile(summary_path, self.out_dir / "summary.md")
        except Exception as e:
            print(f"[postprocess] summary generation failed: {e}")

        # 5) readme index
        try:
            self.write_readme()
        except Exception:
            pass

        # Закриваємо tee/handlers (best-effort), щоб звільнити дескриптори
        try:
            _close_tee()
        except Exception:
            pass
        try:
            import logging as _logging

            _lg = _logging.getLogger()
            for h in list(_lg.handlers):
                try:
                    h.flush()  # type: ignore[attr-defined]
                except Exception:
                    pass
        except Exception:
            pass

        print(f"[postprocess] Done → {self.out_dir}")
        return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Unified live/replay runner with reporting")
    sub = p.add_subparsers(dest="mode", required=True)

    # common
    def add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--namespace", default="ai_one", help="STATE_NAMESPACE value")
        sp.add_argument(
            "--prom-port", type=int, default=None, help="Prometheus HTTP port"
        )
        sp.add_argument(
            "--set",
            action="append",
            default=[],
            dest="sets",
            help="Override flags ENV: KEY=VALUE (repeatable)",
        )
        sp.add_argument(
            "--out-dir", default="reports/run", help="Output directory for artifacts"
        )
        sp.add_argument(
            "--report", action="store_true", help="Enable post-run report generation"
        )
        sp.add_argument(
            "--forward-profiles",
            default="strong,soft,explain",
            help="Comma-separated forward profiles to generate (choices: strong,soft,explain)",
        )
        sp.add_argument(
            "--metrics-grace-sec",
            type=int,
            default=0,
            help="Stay alive N seconds after pipeline to capture final /metrics snapshot",
        )
        sp.add_argument(
            "--tidy-reports",
            action="store_true",
            help="Після завершення автоматично розкласти звіти у reports/summary та reports/replay",
        )
        sp.add_argument(
            "--final-only",
            action="store_true",
            help="Зберегти лише фінальні звіти (summary/*) і видалити проміжні після завершення",
        )

    live = sub.add_parser("live", help="Живий моніторинг")
    live.add_argument("--duration", type=int, required=True, help="Duration in seconds")
    add_common(live)

    rep = sub.add_parser("replay", help="Псевдострім/реплей")
    rep.add_argument("--limit", type=int, required=True, help="Bars limit")
    rep.add_argument(
        "--symbols",
        default="BTCUSDT,ETHUSDT,TONUSDT,SNXUSDT",
        help="Comma-separated symbols for replay",
    )
    rep.add_argument("--interval", default="1m", help="Bar interval")
    rep.add_argument(
        "--source",
        default="snapshot",
        choices=["snapshot", "binance"],
        help="Data source",
    )
    add_common(rep)

    # postprocess (standalone)
    post = sub.add_parser(
        "postprocess",
        help="Generate forward_* and summary.md from existing out-dir without rerunning pipeline",
    )
    post.add_argument(
        "--in-dir",
        required=True,
        help="Directory containing run.log & metrics.txt (e.g. reports/prof_canary6)",
    )
    post.add_argument(
        "--forward-profiles",
        default="strong,soft,explain",
        help="Comma-separated profiles to generate (subset of strong,soft,explain)",
    )
    post.add_argument(
        "--presence-min",
        type=float,
        default=None,
        help="Override presence_min (all profiles)",
    )
    post.add_argument(
        "--bias-abs-min",
        type=float,
        default=None,
        help="Override bias_abs_min (all profiles)",
    )
    post.add_argument(
        "--whale-max-age-sec",
        type=int,
        default=None,
        help="Override whale_max_age_sec (whale profiles)",
    )
    post.add_argument(
        "--explain-ttl-sec",
        type=int,
        default=None,
        help="Override explain_ttl_sec (explain profile)",
    )
    post.add_argument(
        "--max-lines",
        type=int,
        default=None,
        help="Read at most N lines from run.log (speed-up on huge logs)",
    )

    return p


def _parse_args(argv: Iterable[str] | None = None) -> RunnerConfig:
    pa = _build_parser()
    a = pa.parse_args(argv)
    mode = str(a.mode)
    if mode == "postprocess":
        cfg = RunnerConfig(mode=mode, out_dir=Path(a.in_dir))
        raw_fp = getattr(a, "forward_profiles", None)
        if isinstance(raw_fp, str) and raw_fp.strip():
            vals = [s.strip().lower() for s in raw_fp.split(",") if s.strip()]
            cfg.forward_profiles = [v for v in vals if v in FORWARD_PROFILES]
        # Direct attribute assignment (avoid setattr for static attr names)
        cfg.pp_presence_min = getattr(a, "presence_min", None)  # type: ignore[attr-defined]
        cfg.pp_bias_abs_min = getattr(a, "bias_abs_min", None)  # type: ignore[attr-defined]
        cfg.pp_whale_max_age_sec = getattr(a, "whale_max_age_sec", None)  # type: ignore[attr-defined]
        cfg.pp_explain_ttl_sec = getattr(a, "explain_ttl_sec", None)  # type: ignore[attr-defined]
        cfg.pp_max_lines = getattr(a, "max_lines", None)  # type: ignore[attr-defined]
        return cfg
    out_dir = Path(a.out_dir)
    cfg = RunnerConfig(
        mode=mode,
        duration_s=int(getattr(a, "duration", 0) or 0) if mode == "live" else None,
        limit=int(getattr(a, "limit", 0) or 0) if mode == "replay" else None,
        namespace=str(a.namespace),
        prom_port=(int(a.prom_port) if a.prom_port is not None else None),
        out_dir=out_dir,
        set_flags=_parse_sets(list(a.sets or [])),
        report=bool(a.report),
        metrics_grace_sec=int(getattr(a, "metrics_grace_sec", 0) or 0),
        tidy_reports=bool(getattr(a, "tidy_reports", False)),
        final_only=bool(getattr(a, "final_only", False)),
    )
    # forward profiles parsing (спільно для live/replay)
    raw_fp = getattr(a, "forward_profiles", None)
    if isinstance(raw_fp, str) and raw_fp.strip():
        vals = [s.strip().lower() for s in raw_fp.split(",") if s.strip()]
        cfg.forward_profiles = [v for v in vals if v in FORWARD_PROFILES]
    if mode == "replay":
        syms = [s.strip().upper() for s in str(a.symbols).split(",") if s.strip()]
        cfg.symbols = syms
        cfg.interval = str(a.interval)
        cfg.source = str(a.source)
    return cfg


def main(argv: Iterable[str] | None = None) -> int:
    cfg = _parse_args(argv)
    orch = RunnerOrchestrator(cfg)
    # rc ініціалізуємо як успішний (0) — далі тільки підвищуємо при помилках
    rc: int = 0
    if cfg.mode == "postprocess":
        # Захист: переконаємось, що не передані параметри live/replay
        try:
            assert (
                cfg.duration_s is None and cfg.limit is None
            ), "postprocess received live/replay arguments"
        except AssertionError as ae:
            print(f"[Main] postprocess assertion failed: {ae}")
            return 1
        try:
            rc = orch.run_standalone_postprocess()
        except Exception:
            rc = 1
            try:
                import logging as _logging

                _logging.getLogger(__name__).exception("postprocess failed")
            except Exception:
                print("[Main] postprocess failed (no logger)")
        return rc
    # live / replay шляхи
    try:
        if cfg.mode == "live":
            rc = asyncio.run(orch.run_live())
        elif cfg.mode == "replay":
            rc = asyncio.run(orch.run_replay())
        else:
            rc = 1
    except asyncio.CancelledError:
        print("[Main] Завершення за скасуванням (CancelledError)", flush=True)
        rc = 0
    except KeyboardInterrupt:
        print("[Main] Завершення за скасуванням (KeyboardInterrupt)", flush=True)
        rc = 130
    finally:
        if cfg.mode in {"live", "replay"}:
            try:
                orch.post_process()
            except Exception as e:  # pragma: no cover
                print(f"⚠️  Post-process error: {e}")
    return rc


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
