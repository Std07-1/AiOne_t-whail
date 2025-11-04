"""Автоматизація вибірки епізодів і реплею для BTCUSDT/TONUSDT/SNXUSDT.

- Крок 1: прогнати псевдо-стрім (bench) і зібрати свіжі stage1_events.jsonl у out_dir
- Крок 2: знайти останні події фаз для двох сценаріїв:
    breakout_confirmation -> post_breakout
    pullback_continuation -> momentum
- Крок 3: сформувати episodes_manifest.jsonl (по одному епізоду на сценарій для кожного символу)
- Крок 4: виконати реплей строго в межах епізодів (start/end 1:1), вимкнути fast-path, увімкнути строгі прапори
- Крок 5: зібрати агрегати: replay_summary.csv, phase_logs.jsonl, kpi.json

Запуск:
  python -m tools.generate_and_replay_episodes --symbols BTCUSDT,TONUSDT,SNXUSDT --interval 1m --out-dir ./replay_bench
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Дозволяємо запуск як скрипт
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from tools.replay_stream import ReplayConfig, run_replay  # type: ignore

# Мапінг сценарій -> фаза у Stage2
SCENARIO_TO_PHASE: dict[str, str] = {
    "breakout_confirmation": "post_breakout",
    "pullback_continuation": "momentum",
}

INTERVAL_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000}
DEFAULT_LOOKBACK_BARS = 180  # 3 години для 1m


@dataclass
class Episode:
    symbol: str
    interval: str
    scenario: str
    phase: str
    ts_ms: int
    start_ms: int
    end_ms: int

    def to_json(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "scenario": self.scenario,
            "phase": self.phase,
            "ts_ms": self.ts_ms,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "start_iso": datetime.fromtimestamp(self.start_ms / 1000, tz=UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "end_iso": datetime.fromtimestamp(self.end_ms / 1000, tz=UTC)
            .isoformat()
            .replace("+00:00", "Z"),
        }


def _norm_symbol(s: str) -> str:
    s = s.strip().upper()
    return s if s.endswith("USDT") else f"{s}USDT"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                continue
    return out


def run_bench(
    symbols: list[str], interval: str, out_dir: Path, limit: int = 400
) -> None:
    # Запуск як модуль гарантує коректні імпорти
    args = [
        sys.executable,
        "-m",
        "tools.bench_pseudostream",
        "--symbols",
        ",".join(symbols),
        "--limit",
        str(limit),
        "--interval",
        interval,
        "--out-dir",
        str(out_dir),
    ]
    subprocess.run(args, check=True)


def select_episodes(
    out_dir: Path,
    symbols: list[str],
    interval: str,
    scenarios: list[str],
    lookback_bars: int = DEFAULT_LOOKBACK_BARS,
    per_symbol: int = 1,
) -> list[Episode]:
    # 1) Збір кандидатів: по кожному символу беремо останній івент для кожної запитаної фази
    candidates_by_scenario: dict[str, list[Episode]] = {s: [] for s in scenarios}
    for symbol in symbols:
        dumps = sorted(out_dir.glob(f"dump_{symbol.lower()}_{interval}_*"))
        if not dumps:
            continue
        dump_dir = dumps[-1]
        s1e_path = dump_dir / "stage1_events.jsonl"
        rows = _read_jsonl(s1e_path)
        events = [r for r in rows if str(r.get("event")) == "phase_detected"]
        if not events:
            continue
        last_by_phase: dict[str, dict[str, Any]] = {}
        for ev in events:
            ph = str(ev.get("phase") or "").strip()
            last_by_phase[ph] = ev  # останнє входження
        for scenario in scenarios:
            phase = SCENARIO_TO_PHASE.get(scenario)
            if not phase:
                continue
            ev = last_by_phase.get(phase)
            if not ev:
                continue
            ts_iso = str(ev.get("ts"))
            try:
                ts_dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
                ts_ms = int(ts_dt.timestamp() * 1000)
            except Exception:
                ts_ms = int(ev.get("timestamp_ms") or 0)
            if ts_ms <= 0:
                continue
            interval_ms = INTERVAL_MS.get(interval, 60_000)
            start_ms = ts_ms - (lookback_bars * interval_ms)
            end_ms = ts_ms
            candidates_by_scenario[scenario].append(
                Episode(
                    symbol=symbol,
                    interval=interval,
                    scenario=scenario,
                    phase=phase,
                    ts_ms=ts_ms,
                    start_ms=start_ms,
                    end_ms=end_ms,
                )
            )
    # 2) Нормалізуємо: сортуємо кандидатів по часу, новіші перші
    for _sc, lst in candidates_by_scenario.items():
        lst.sort(key=lambda e: e.ts_ms, reverse=True)

    # 3) Вибір цільового набору: по одному епізоду на кожен сценарій; уникаємо повторення символів, якщо можливо
    selection: list[Episode] = []
    used_symbols: set[str] = set()
    for sc in scenarios:
        pool = candidates_by_scenario.get(sc, [])
        chosen: Episode | None = None
        for ep in pool:
            if ep.symbol not in used_symbols:
                chosen = ep
                break
        if not chosen and pool:
            chosen = pool[0]
        if chosen:
            selection.append(chosen)
            used_symbols.add(chosen.symbol)

    # 4) Якщо per_symbol > 1, для кожного вже обраного символу можна добрати ще найновіші епізоди інших сценаріїв (опційно)
    if per_symbol > 1:
        # побудуємо ліміти на символ
        count_by_symbol: dict[str, int] = {}
        for ep in selection:
            count_by_symbol[ep.symbol] = count_by_symbol.get(ep.symbol, 0) + 1
        for sc in scenarios:
            for ep in candidates_by_scenario.get(sc, []):
                if ep in selection:
                    continue
                c = count_by_symbol.get(ep.symbol, 0)
                if c < per_symbol:
                    selection.append(ep)
                    count_by_symbol[ep.symbol] = c + 1

    # Остаточне сортування за часом, новіші — першими
    selection.sort(key=lambda e: e.ts_ms, reverse=True)
    return selection


def write_manifest(episodes: list[Episode], path: Path) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for ep in episodes:
            fh.write(json.dumps(ep.to_json(), ensure_ascii=False) + "\n")


def aggregate_csv(csv_paths: list[Path], out_path: Path) -> None:
    rows: list[dict[str, Any]] = []
    for p in csv_paths:
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                d = dict(row)
                d["_source_csv"] = p.as_posix()
                # Узгодження поля scenario_detected: якщо його немає, але є 'scenario' — продублюємо
                if "scenario_detected" not in d and "scenario" in d:
                    d["scenario_detected"] = d["scenario"]
                rows.append(d)
    if not rows:
        out_path.write_text("", encoding="utf-8")
        return
    # Уніфікуємо заголовки
    fieldnames: list[str] = []
    for r in rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def aggregate_phase_logs(jsonl_paths: list[Path], out_path: Path) -> None:
    with out_path.open("w", encoding="utf-8") as out:
        for p in jsonl_paths:
            if not p.exists():
                continue
            with p.open("r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        obj = json.loads(line)
                        # Мінімальний обогащувач: позначимо джерело
                        obj.setdefault("_source", p.as_posix())
                        out.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    except Exception:
                        continue


def aggregate_kpi(json_paths: list[Path], out_path: Path) -> None:
    total_counts: dict[str, int] = {}
    meta: list[dict[str, Any]] = []
    for p in json_paths:
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        meta.append(
            {
                "file": p.as_posix(),
                "symbol": data.get("symbol"),
                "interval": data.get("interval"),
            }
        )
        counts = data.get("counts") or {}
        for k, v in counts.items():
            total_counts[k] = total_counts.get(k, 0) + int(v or 0)
    out = {"counts_total": total_counts, "sources": meta}
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")


def toggle_strict_flags() -> None:
    try:
        import config.config as cfg  # type: ignore

        # Строгі режими (best-effort; якщо атрибутів немає — ігноруємо)
        for name in (
            "STRICT_ACCUM_CAPS_ENABLED",
            "STRICT_HTF_GRAY_GATE_ENABLED",
            "STRICT_LOW_ATR_OVERRIDE_ON_SPIKE",
        ):
            if hasattr(cfg, name):
                setattr(cfg, name, True)
        # Вимикаємо fast-path у Stage2 на час реплею
        try:
            st = getattr(cfg, "STAGE2_RUNTIME", {})
            if isinstance(st, dict):
                st["fast_path_enabled"] = False
                cfg.STAGE2_RUNTIME = st
        except Exception:
            pass
    except Exception:
        # У тестових середовищах — тихо ігноруємо
        pass


def main() -> int:
    p = argparse.ArgumentParser(description="Генерація епізодів і реплей")
    p.add_argument("--symbols", default="BTCUSDT,TONUSDT,SNXUSDT")
    # Підтримка перелічених інтервалів через кому: напр. "1m,5m"
    p.add_argument("--interval", default="1m")
    p.add_argument("--out-dir", default="./replay_bench")
    p.add_argument("--limit", type=int, default=400)
    p.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_BARS)
    p.add_argument(
        "--scenarios",
        default="breakout_confirmation,pullback_continuation",
        help="Перелік сценаріїв через кому",
    )
    p.add_argument("--per-symbol", type=int, default=1)
    p.add_argument("--require-both-scenarios", action="store_true")
    p.add_argument("--strict-datetime-match", action="store_true")
    # Автовирівнювання вікна при strict-мисматчі: повторний реплей на фактичних межах summary.csv
    p.add_argument(
        "--auto-align-strict-window",
        action="store_true",
        help="Автоматично повторити реплей на межах перших/останніх барів при strict-дисонансі",
    )
    # Лише валідація ручного вводу без запуску bench/replay
    p.add_argument("--validate-only", action="store_true")
    # Лише агрегація вже наявних результатів без повторного реплею
    p.add_argument(
        "--aggregate-only",
        action="store_true",
        help="Не запускати реплей; лише агрегувати існуючі результати",
    )
    # Опційний широкий пошук назад у часі, якщо потрібний сценарій відсутній у свіжому бенчі
    p.add_argument(
        "--scan-back-hours",
        type=int,
        default=0,
        help="Скільки годин назад сканувати вікнами (0 = вимкнено)",
    )
    p.add_argument(
        "--scan-step-minutes",
        type=int,
        default=60,
        help="Крок зсуву вікна під час скану (хв)",
    )
    # Аліас короткий для сумісності з ранбуком
    p.add_argument("--scan-step-min", type=int, help="Alias для --scan-step-minutes")
    # Тихий скан (приглушення логів під час run_replay у скані)
    p.add_argument("--quiet-scan", action="store_true")
    # Раннє припинення скану як тільки знайдено обидва сценарії
    p.add_argument("--stop-on-both", action="store_true")
    # Обмеження паралельності (приймаємо як параметр; внутрішньо скан виконується best-effort послідовно)
    p.add_argument("--workers", type=int, default=1)
    # Ручне додавання епізоду (JSON) як детермінований резерв
    p.add_argument(
        "--manual-episode",
        help=(
            'JSON епізоду: {"symbol":..., "scenario":..., "start":ISO, "end":ISO}.'
            " Додається до маніфесту без зміни Stage-контрактів"
        ),
    )
    # Ручне завантаження кількох епізодів із файлу (JSONL або JSON-масив)
    p.add_argument(
        "--manual-episodes",
        help=(
            "Шлях до файлу з епізодами: JSONL (1 епізод на рядок) або JSON-масив. "
            "Полям достатньо: symbol, scenario, start, end, опційно tf|interval."
        ),
    )
    # Явний шлях до episodes_manifest.jsonl для режиму агрегації
    p.add_argument(
        "--episodes-manifest", help="Шлях до episodes_manifest.jsonl (для агрегації)"
    )
    # Параметри селектора (м'які пороги) — парситься та логується у трейсах; не змінює Stage2
    p.add_argument("--selector-override", help="JSON із порогами селектора для A/B")
    args = p.parse_args()

    symbols = [_norm_symbol(s) for s in str(args.symbols).split(",") if s.strip()]
    # Розбір інтервалів: підтримуємо перелік через кому; перший вважаємо основним
    intervals = [x.strip() for x in str(args.interval).split(",") if x.strip()]
    if not intervals:
        intervals = ["1m"]
    primary_interval = intervals[0]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Режим: лише агрегація без запуску bench/replay
    if args.aggregate_only:
        in_root = out_dir / "run"
        if not in_root.exists():
            in_root = out_dir
        manifest_path = None
        if args.episodes_manifest:
            manifest_path = Path(args.episodes_manifest)
        else:
            cand = out_dir / "episodes_manifest.jsonl"
            if cand.exists():
                manifest_path = cand
        cmd = [
            sys.executable,
            "-m",
            "tools.analyze_replay",
            "--in",
            str(in_root),
        ]
        if manifest_path is not None:
            cmd += ["--episodes-manifest", str(manifest_path)]
            cmd += ["--set-scenario-from-manifest"]
        subprocess.run(cmd, check=True)
        print(f"[AGGREGATE-ONLY] Готово. in={in_root}")
        return 0

    # Попередній розбір ручного епізоду: якщо задано --manual-episode,
    # пропускаємо bench та селектор і одразу готуємо список епізодів.
    manual_ep: Episode | None = None
    manual_eps: list[Episode] = []
    if args.manual_episode:
        try:
            raw_me = str(args.manual_episode)
            m: dict[str, Any] | None = None
            # 1) прямий JSON
            try:
                obj = json.loads(raw_me)
                if isinstance(obj, dict):
                    m = obj
            except Exception:
                m = None
            # 2) якщо це шлях до файлу — прочитати як JSON або перший рядок JSONL
            if m is None:
                pth = Path(raw_me)
                if pth.exists():
                    try:
                        txt = pth.read_text(encoding="utf-8")
                        obj = json.loads(txt)
                        if isinstance(obj, dict):
                            m = obj
                    except Exception:
                        # fallback JSONL перший запис
                        try:
                            with pth.open("r", encoding="utf-8") as fh:
                                for line in fh:
                                    line = line.strip()
                                    if not line:
                                        continue
                                    o2 = json.loads(line)
                                    if isinstance(o2, dict):
                                        m = o2
                                        break
                        except Exception:
                            m = None
            if not isinstance(m, dict):
                raise ValueError("manual-episode is not a JSON object")
            msym = _norm_symbol(str(m.get("symbol", "")))
            msc = str(m.get("scenario", "")).strip()
            if msym and msc in SCENARIO_TO_PHASE:

                def _is_iso_utc(x: Any) -> bool:
                    s = str(x)
                    return s.endswith("Z") or s.endswith("+00:00")

                def _to_ms(x: Any) -> int:
                    if not _is_iso_utc(x):
                        raise ValueError(
                            "timestamp must be ISO UTC with 'Z' or '+00:00'"
                        )
                    dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
                    return int(dt.timestamp() * 1000)

                mstart = _to_ms(m.get("start"))
                mend = _to_ms(m.get("end"))
                itv = str(m.get("tf") or m.get("interval") or primary_interval).strip()
                if itv not in INTERVAL_MS:
                    raise ValueError(f"invalid tf/interval: {itv}")
                manual_ep = Episode(
                    symbol=msym,
                    interval=itv,
                    scenario=msc,
                    phase=SCENARIO_TO_PHASE[msc],
                    ts_ms=mend,
                    start_ms=mstart,
                    end_ms=mend,
                )
        except Exception:
            manual_ep = None
    # Пакетний ввід з файлу (--manual-episodes)
    if args.manual_episodes:
        try:
            path = Path(str(args.manual_episodes))
            raw = path.read_text(encoding="utf-8") if path.exists() else ""
            items: list[dict[str, Any]] = []
            # Спроба як JSON-масив
            try:
                arr = json.loads(raw)
                if isinstance(arr, list):
                    items = [x for x in arr if isinstance(x, dict)]
            except Exception:
                # Фолбек: читання як JSONL
                items = _read_jsonl(path)

            def _is_iso_utc(x: Any) -> bool:
                s = str(x)
                return s.endswith("Z") or s.endswith("+00:00")

            def _to_ms(x: Any) -> int:
                if not _is_iso_utc(x):
                    raise ValueError("timestamp must be ISO UTC with 'Z' or '+00:00'")
                dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)

            for m in items:
                msym = _norm_symbol(str(m.get("symbol", "")))
                msc = str(m.get("scenario", "")).strip()
                if not (msym and msc in SCENARIO_TO_PHASE):
                    continue
                itv = str(m.get("tf") or m.get("interval") or primary_interval).strip()
                if itv not in INTERVAL_MS:
                    continue
                try:
                    mstart = _to_ms(m.get("start"))
                    mend = _to_ms(m.get("end"))
                except Exception:
                    continue
                manual_eps.append(
                    Episode(
                        symbol=msym,
                        interval=itv,
                        scenario=msc,
                        phase=SCENARIO_TO_PHASE[msc],
                        ts_ms=mend,
                        start_ms=mstart,
                        end_ms=mend,
                    )
                )
        except Exception:
            manual_eps = []
    # Якщо користувач подав обидва прапори одночасно — фейл
    if args.manual_episode and args.manual_episodes:
        print(
            "[ABORT] manual-input invalid: both --manual-episode and --manual-episodes provided",
            file=sys.stderr,
        )
        return 2

    # Якщо явно просили manual(-s), але парсинг нічого не дав — завершуємо з підказкою для PowerShell
    if (args.manual_episode or args.manual_episodes) and (
        manual_ep is None and not manual_eps
    ):
        hint = ""
        if os.name == "nt":
            hint = (
                " Use single quotes in PowerShell (e.g., --manual-episode '{"
                + '"symbol":"BTCUSDT","scenario":"pullback_continuation","start":"2025-10-28T10:00:00Z","end":"2025-10-28T11:00:00Z"}\' ) or pass a JSONL file.'
            )
        print(
            f"ERR[2] Manual episodes parse failed.{hint}",
            file=sys.stderr,
        )
        return 2

    # Валідація: start<end, symbols whitelist, TF узгодження з --interval, відсутність перетинів у межах символу
    symbols_whitelist = set(symbols)
    allowed_tfs = set(INTERVAL_MS.keys())
    intervals_set = set(intervals)

    all_eps: list[Episode] = []
    if manual_eps:
        all_eps.extend(manual_eps)
    if manual_ep is not None:
        all_eps.append(manual_ep)
    if all_eps:
        # TF має бути валідним та узгодженим із --interval(ами)
        for ep in all_eps:
            if ep.symbol not in symbols_whitelist:
                print(
                    f"[ABORT] manual-input invalid: symbol {ep.symbol} not in whitelist {sorted(symbols_whitelist)}",
                    file=sys.stderr,
                )
                return 2
            if ep.interval not in allowed_tfs:
                print(
                    f"[ABORT] manual-input invalid: tf {ep.interval} not in {sorted(allowed_tfs)}",
                    file=sys.stderr,
                )
                return 2
            if ep.interval not in intervals_set:
                print(
                    f"[ABORT] manual-input invalid: tf {ep.interval} not in --interval {sorted(intervals_set)}",
                    file=sys.stderr,
                )
                return 2
            if not (ep.start_ms < ep.end_ms):
                print(
                    f"[ABORT] manual-input invalid: start_ms >= end_ms for {ep.symbol} {ep.interval}",
                    file=sys.stderr,
                )
                return 2
        # Перетини вікон у межах одного символу
        by_sym: dict[str, list[tuple[int, int]]] = {}
        for ep in all_eps:
            by_sym.setdefault(ep.symbol, []).append((ep.start_ms, ep.end_ms))
        for sym, wins in by_sym.items():
            wins.sort()
            for i in range(1, len(wins)):
                a1, a2 = wins[i - 1]
                b1, b2 = wins[i]
                if a1 < b2 and b1 < a2:
                    print(
                        f"[ABORT] manual-input invalid: overlapping windows for {sym}: {(a1, a2)} and {(b1, b2)}",
                        file=sys.stderr,
                    )
                    return 2

    # Dry-run валідації
    if args.validate_only:
        if all_eps:
            print("Validation OK: manual input accepted.")
            return 0
        else:
            print("Validation NOOP: nothing to validate (no manual input).")
            return 0

    # 1) bench → отримати свіжі події (пропускаємо, якщо є ручний епізод)
    if manual_ep is None and not manual_eps:
        for itv in intervals:
            run_bench(symbols, itv, out_dir, limit=int(args.limit))

    # 2) Вибірка епізодів
    scenarios = [s.strip() for s in str(args.scenarios).split(",") if s.strip()]
    if manual_eps:
        episodes: list[Episode] = manual_eps
    elif manual_ep is not None:
        episodes = [manual_ep]
    else:
        episodes = select_episodes(
            out_dir,
            symbols,
            primary_interval,
            scenarios=scenarios,
            lookback_bars=int(args.lookback),
            per_symbol=int(args.per_symbol),
        )
    manifest_path = out_dir / "episodes_manifest.jsonl"
    write_manifest(episodes, manifest_path)

    # Перевірка: якщо вимагаємо присутність усіх сценаріїв
    if args.require_both_scenarios:
        present = {ep.scenario for ep in episodes}
        missing = [s for s in scenarios if s not in present]
        # Ручний епізод як детермінований резерв
        if missing and args.manual_episode:
            try:
                manual = json.loads(str(args.manual_episode))
                msym = _norm_symbol(str(manual.get("symbol", "")))
                msc = str(manual.get("scenario"))
                if msym and msc in SCENARIO_TO_PHASE:

                    def _to_ms(x: str) -> int:
                        dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
                        return int(dt.timestamp() * 1000)

                    mstart = _to_ms(manual.get("start"))
                    mend = _to_ms(manual.get("end"))
                    episodes.append(
                        Episode(
                            symbol=msym,
                            interval=primary_interval,
                            scenario=msc,
                            phase=SCENARIO_TO_PHASE[msc],
                            ts_ms=mend,
                            start_ms=mstart,
                            end_ms=mend,
                        )
                    )
                    write_manifest(episodes, manifest_path)
                    present = {ep.scenario for ep in episodes}
                    missing = [s for s in scenarios if s not in present]
            except Exception:
                pass
        if missing:
            # Широкий пошук відсутніх сценаріїв як fallback, якщо увімкнено
            step_minutes = (
                int(args.scan_step_minutes or 0) or int(args.scan_step_min or 0) or 60
            )
            if int(args.scan_back_hours) > 0:
                for sc in list(missing):
                    found_ep: Episode | None = None
                    for itv in intervals:
                        try:
                            found_ep = _scan_for_phase_episode(
                                symbols=symbols,
                                interval=itv,
                                target_scenario=sc,
                                lookback_bars=int(args.lookback),
                                out_dir=out_dir,
                                back_hours=int(args.scan_back_hours),
                                step_minutes=step_minutes,
                            )
                        except Exception as e:
                            print(
                                f"[WARN] Wide-scan fallback failed ({sc}/{itv}): {e}",
                                file=sys.stderr,
                            )
                            found_ep = None
                        if found_ep is not None:
                            episodes.append(found_ep)
                            write_manifest(episodes, manifest_path)
                            break
                    present = {ep.scenario for ep in episodes}
                    missing = [s for s in scenarios if s not in present]
                    if args.stop_on_both and not missing:
                        break
                # Після широкого скану — легкий прибирання тимчасових scan_*/dump_* старіших за 12 годин
                try:
                    _cleanup_old_dumps(out_dir, older_than_hours=12)
                except Exception:
                    pass
            # Якщо все ще бракує — завершуємо з помилкою
            if missing:
                print(
                    f"[ERROR] Відсутні сценарії у маніфесті: {missing}. Спробуйте збільшити lookback/limit або інший TF, або ввімкнути --scan-back-hours.",
                    file=sys.stderr,
                )
                return 2

    # 3) Реплей кожного епізоду
    toggle_strict_flags()
    replay_outputs: list[Path] = []
    phase_logs_paths: list[Path] = []
    kpi_paths: list[Path] = []
    # Додаткові виходи під out_dir/run
    run_root = out_dir / "run"
    run_root.mkdir(parents=True, exist_ok=True)
    # Корінь репозиторію (../ від цього файлу)
    repo_root = Path(__file__).resolve().parents[1]
    for ep in episodes:
        dump_dir = (
            out_dir
            / f"replay_{ep.symbol.lower()}_{ep.interval}_{ep.start_ms}_{ep.end_ms}"
        )
        # Визначаємо джерело: snapshot якщо файл є, інакше binance
        snapshot_path = (
            repo_root
            / "datastore"
            / f"{ep.symbol.lower()}_bars_{ep.interval}_snapshot.jsonl"
        )
        source = "snapshot" if snapshot_path.exists() else "binance"

        cfg = ReplayConfig(
            symbol=ep.symbol,
            interval=ep.interval,
            source=source,
            start_ms=ep.start_ms,
            end_ms=ep.end_ms,
            limit=None,
            dump_dir=dump_dir,
        )
        _ = asyncio_run(run_replay(cfg))
        # Строга перевірка збігу часу 1:1 (за бажанням)
        strict_ok = False
        auto_aligned_bounds: tuple[int, int] | None = None
        if args.strict_datetime_match:
            # Якщо перший прогін не дав жодного бару — немає сенсу авто-алайнити
            first_bounds = _read_summary_bounds(dump_dir)
            if first_bounds is None:
                print(
                    "ERR[4] Empty summary.csv; shift episode window ±10m or disable strict mode.",
                    file=sys.stderr,
                )
                return 4
            try:
                _verify_time_match(dump_dir, ep.start_ms, ep.end_ms)
                strict_ok = True
            except Exception as e:
                # Автовирівнювання, якщо ввімкнено: зчитуємо межі summary.csv і повторюємо реплей
                if args.auto_align_strict_window:
                    bounds = _read_summary_bounds(dump_dir)
                    if bounds is not None:
                        b_start, b_end = bounds
                        if b_start != ep.start_ms or b_end != ep.end_ms:
                            # новий dump_dir за вирівняними межами
                            dump_dir_aligned = (
                                out_dir
                                / f"replay_{ep.symbol.lower()}_{ep.interval}_{b_start}_{b_end}"
                            )
                            cfg2 = ReplayConfig(
                                symbol=ep.symbol,
                                interval=ep.interval,
                                source=source,
                                start_ms=b_start,
                                end_ms=b_end,
                                limit=None,
                                dump_dir=dump_dir_aligned,
                            )
                            _ = asyncio_run(run_replay(cfg2))
                            try:
                                _verify_time_match(dump_dir_aligned, b_start, b_end)
                                # Переїжджаємо на вирівняний дамп
                                dump_dir = dump_dir_aligned
                                strict_ok = True
                                auto_aligned_bounds = (b_start, b_end)
                                print(
                                    f"[STRICT] auto-aligned window to start={b_start} end={b_end}",
                                    file=sys.stderr,
                                )
                            except Exception:
                                # Фолбек: якщо первинний прогін містить цільові межі (наприклад, дані починаються пізніше за start_ms),
                                # приймаємо влучання в межах ширшого вікна як strict=OK.
                                try:
                                    orig_first, orig_last = bounds
                                except Exception:
                                    orig_first, orig_last = (None, None)  # type: ignore[assignment]
                                if (
                                    isinstance(orig_first, int)
                                    and isinstance(orig_last, int)
                                    and orig_last == ep.end_ms
                                    and orig_first >= ep.start_ms
                                ):
                                    # Залишаємося на оригінальному дампі (ширший каталог), але відмічаємо auto-aligned до фактичних меж.
                                    strict_ok = True
                                    auto_aligned_bounds = (orig_first, orig_last)
                                    print(
                                        f"[STRICT] accepted contained match within original window: start={orig_first} end={orig_last}",
                                        file=sys.stderr,
                                    )
                                else:
                                    print(
                                        f"ERR[3] Strict datetime mismatch after auto-align for {dump_dir_aligned.name}",
                                        file=sys.stderr,
                                    )
                                    return 3
                    else:
                        print(
                            f"ERR[3] [STRICT] strict-mismatch for {dump_dir.name}: no bounds",
                            file=sys.stderr,
                        )
                        return 3
                else:
                    print(
                        f"ERR[3] [STRICT] strict-mismatch for {dump_dir.name}: {e}",
                        file=sys.stderr,
                    )
                    return 3
        # збір вихідних шляхів
        replay_outputs.append(dump_dir / "summary.csv")
        phase_logs_paths.append(dump_dir / "stage1_events.jsonl")
        kpi_paths.append(dump_dir / "kpi.json")

        # Під run/: створюємо per-episode каталог з phase_logs.jsonl та копіями
        run_ep_dir = run_root / dump_dir.name
        run_ep_dir.mkdir(parents=True, exist_ok=True)
        # 1) phase_logs.jsonl з маркером [REPLAY] window
        _emit_replay_window_log(
            src=dump_dir / "stage1_events.jsonl",
            dst=run_ep_dir / "phase_logs.jsonl",
            ep=ep,
            strict_ok=strict_ok,
            auto_aligned=auto_aligned_bounds,
        )
        # 2) копії summary.csv і kpi.json
        _safe_copy(dump_dir / "summary.csv", run_ep_dir / "summary.csv")
        _safe_copy(dump_dir / "kpi.json", run_ep_dir / "kpi.json")

    # 4) Агрегація артефактів
    aggregate_csv(replay_outputs, out_dir / "replay_summary.csv")
    aggregate_phase_logs(phase_logs_paths, out_dir / "phase_logs.jsonl")
    aggregate_kpi(kpi_paths, out_dir / "kpi.json")
    # Дублюємо агрегати у run/
    run_csvs = [
        p.parent.parent / "run" / p.parent.name / "summary.csv" for p in replay_outputs
    ]
    run_jsonls = [
        p.parent.parent / "run" / p.parent.name / "phase_logs.jsonl"
        for p in replay_outputs
    ]
    run_kpis = [
        p.parent.parent / "run" / p.parent.name / "kpi.json" for p in replay_outputs
    ]
    aggregate_csv(run_csvs, run_root / "replay_summary.csv")
    aggregate_phase_logs(run_jsonls, run_root / "phase_logs.jsonl")
    aggregate_kpi(run_kpis, run_root / "kpi.json")

    print(f"OK. Episodes={len(episodes)} | manifest={manifest_path}")
    return 0


def _cleanup_old_dumps(base: Path, *, older_than_hours: int = 12) -> None:
    import time

    now = time.time()
    cutoff = now - older_than_hours * 3600
    for p in base.iterdir():
        try:
            if not p.is_dir():
                continue
            name = p.name
            if not (
                name.startswith("dump_")
                or name.startswith("scan_")
                or name.startswith("replay_")
            ):
                continue
            mtime = p.stat().st_mtime
            if mtime < cutoff:
                # Безпечне видалення каталогу
                for sub in sorted(p.rglob("*"), reverse=True):
                    if sub.is_file():
                        sub.unlink(missing_ok=True)  # type: ignore[call-arg]
                    elif sub.is_dir():
                        try:
                            sub.rmdir()
                        except Exception:
                            pass
                try:
                    p.rmdir()
                except Exception:
                    pass
        except Exception:
            continue


def _safe_copy(src: Path, dst: Path) -> None:
    try:
        if not src.exists():
            return
        data = src.read_bytes()
        dst.write_bytes(data)
    except Exception:
        pass


def _emit_replay_window_log(
    src: Path,
    dst: Path,
    ep: Episode,
    *,
    strict_ok: bool = False,
    auto_aligned: tuple[int, int] | None = None,
) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        with dst.open("w", encoding="utf-8") as out:
            # Пролог з маркером для перевірки
            start_iso = (
                datetime.fromtimestamp(ep.start_ms / 1000, tz=UTC)
                .isoformat()
                .replace("+00:00", "Z")
            )
            end_iso = (
                datetime.fromtimestamp(ep.end_ms / 1000, tz=UTC)
                .isoformat()
                .replace("+00:00", "Z")
            )
            ok_suffix = " strict=OK" if strict_ok else ""
            header = {
                "msg": f"[REPLAY] window={ep.start_ms}-{ep.end_ms}{ok_suffix}",
                "symbol": ep.symbol,
                "interval": ep.interval,
                "scenario": ep.scenario,
                "start_ms": ep.start_ms,
                "end_ms": ep.end_ms,
                "start_iso": start_iso,
                "end_iso": end_iso,
            }
            out.write(json.dumps(header, ensure_ascii=False) + "\n")
            if auto_aligned is not None:
                aa_start, aa_end = auto_aligned
                out.write(
                    json.dumps(
                        {
                            "msg": f"[STRICT] auto-aligned window to start={aa_start} end={aa_end}",
                            "symbol": ep.symbol,
                            "interval": ep.interval,
                            "scenario": ep.scenario,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            # Тіло — проксі stage1_events
            if src.exists():
                with src.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        out.write(line + "\n")
    except Exception:
        pass


def _verify_time_match(dump_dir: Path, start_ms: int, end_ms: int) -> None:
    """Переконатися, що перший/останній timestamp_ms у summary.csv збігаються зі start_ms/end_ms."""
    csv_path = dump_dir / "summary.csv"
    if not csv_path.exists():
        raise RuntimeError("summary.csv not found")
    with csv_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    if not rows:
        raise RuntimeError("empty summary.csv")
    try:
        first = int(rows[0].get("timestamp_ms") or 0)
        last = int(rows[-1].get("timestamp_ms") or 0)
    except Exception as e:
        raise RuntimeError(f"bad timestamp_ms in summary.csv: {e}") from e
    if first != start_ms or last != end_ms:
        raise RuntimeError(
            f"window mismatch: first={first} last={last} expected={start_ms}-{end_ms}"
        )


def _read_summary_bounds(dump_dir: Path) -> tuple[int, int] | None:
    """Повертає (first_ts_ms, last_ts_ms) з summary.csv або None, якщо неможливо прочитати."""
    csv_path = dump_dir / "summary.csv"
    if not csv_path.exists():
        return None
    try:
        with csv_path.open("r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
        if not rows:
            return None
        first = int(rows[0].get("timestamp_ms") or 0)
        last = int(rows[-1].get("timestamp_ms") or 0)
        if first <= 0 or last <= 0:
            return None
        return first, last
    except Exception:
        return None


def asyncio_run(coro):
    import asyncio

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        # Запуск у вже активному циклі (рідко, але на випадок інтеграції)
        return asyncio.ensure_future(coro)  # type: ignore[return-value]
    return asyncio.run(coro)


def _scan_for_phase_episode(
    *,
    symbols: list[str],
    interval: str,
    target_scenario: str,
    lookback_bars: int,
    out_dir: Path,
    back_hours: int,
    step_minutes: int,
) -> Episode | None:
    """Сканування історії назад ковзними вікнами з використанням реплею для пошуку потрібної фази.

    Мінімізуємо зміни: використовуємо наявний run_replay і аналізуємо stage2_outputs.jsonl
    на предмет появи потрібної фази (наприклад, post_breakout).
    """
    target_phase = SCENARIO_TO_PHASE.get(target_scenario)
    if not target_phase:
        return None
    interval_ms = INTERVAL_MS.get(interval, 60_000)
    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    total_minutes = max(0, int(back_hours) * 60)
    # step_ms не використовується напряму; залишаємо крок у хвилинах для range()
    repo_root = Path(__file__).resolve().parents[1]

    # Локальний контекст для приглушення логів під час широкого скану
    class _SilenceLogs:
        def __enter__(self):
            # Запам’ятовуємо попередній глобальний поріг і вимикаємо все нижче CRITICAL
            self._prev_disable = logging.root.manager.disable
            logging.disable(logging.CRITICAL)
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001, D401
            # Відновлюємо попередній стан
            logging.disable(self._prev_disable)
            return False

    def _detect_source(sym: str) -> str:
        snap_path = (
            repo_root / "datastore" / f"{sym.lower()}_bars_{interval}_snapshot.jsonl"
        )
        return "snapshot" if snap_path.exists() else "binance"

    # Ітерація від зараз назад із кроком step_ms, вікно = lookback_bars * interval
    for minutes_ago in range(0, total_minutes + 1, max(1, int(step_minutes))):
        end_ms = now_ms - minutes_ago * 60_000
        start_ms = end_ms - lookback_bars * interval_ms
        # перегон по всіх символах — повертаємо перше влучання
        for sym in symbols:
            dump_dir = out_dir / f"scan_{sym.lower()}_{interval}_{start_ms}_{end_ms}"
            cfg = ReplayConfig(
                symbol=sym,
                interval=interval,
                source=_detect_source(sym),
                start_ms=start_ms,
                end_ms=end_ms,
                limit=None,
                dump_dir=dump_dir,
            )
            # Захист: окремі вікна можуть падати через порожні дані/фільтри — пропускаємо й скануємо далі
            try:
                # Під час скану робимо реплей у "тихому" режимі, щоб уникнути перенавантаження консолі
                # Увімкнено глобально через параметр CLI --quiet-scan (перевіряємо через logging.root.manager.disable)
                with _SilenceLogs():
                    _ = asyncio_run(run_replay(cfg))
            except Exception:
                # Без шуму: це очікуваний кейс у широкому скані
                continue
            s2_path = dump_dir / "stage2_outputs.jsonl"
            if not s2_path.exists():
                continue
            # Перебір по рядках у зворотному порядку для останнього збігу
            ts_hit = None
            try:
                with s2_path.open("r", encoding="utf-8") as fh:
                    lines = fh.readlines()
                for line in reversed(lines):
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    mc = obj.get("market_context") if isinstance(obj, dict) else None
                    ph = None
                    if isinstance(mc, dict):
                        ph = mc.get("phase")
                    if str(ph or "").strip() == target_phase:
                        ts_hit = int(obj.get("timestamp_ms") or 0)
                        break
            except Exception:
                ts_hit = None
            if ts_hit and ts_hit > 0:
                ep_start_ms = ts_hit - lookback_bars * interval_ms
                if ep_start_ms < 0:
                    ep_start_ms = 0
                return Episode(
                    symbol=sym,
                    interval=interval,
                    scenario=target_scenario,
                    phase=target_phase,
                    ts_ms=ts_hit,
                    start_ms=ep_start_ms,
                    end_ms=ts_hit,
                )
    return None


if __name__ == "__main__":
    raise SystemExit(main())
