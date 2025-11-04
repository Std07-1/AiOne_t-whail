from __future__ import annotations

import argparse
import csv
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Формат папок: replay_{symbollower}_{tf}_{start_ms}_{end_ms}
RE_EP_DIR = re.compile(
    r"^replay_(?P<symbol>[a-z0-9]+)_(?P<tf>[0-9a-z]+)_(?P<start>\d+)_(?P<end>\d+)$"
)


@dataclass
class Episode:
    symbol: str
    start_ms: int
    end_ms: int
    scenario: str | None
    tf: str | None

    @property
    def episode_id(self) -> str:
        return f"{self.symbol}:{self.start_ms}-{self.end_ms}"


def _load_manifest(path: Path) -> list[Episode]:
    episodes: list[Episode] = []
    if not path.exists():
        return episodes
    txt = path.read_text(encoding="utf-8").strip()
    if not txt:
        return episodes
    rows: list[dict[str, Any]] = []
    # JSON lines або JSON-масив
    if txt.startswith("["):
        try:
            arr = json.loads(txt)
            if isinstance(arr, list):
                rows = [x for x in arr if isinstance(x, dict)]
        except Exception:
            rows = []
    else:
        for line in txt.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
            except Exception:
                continue
    for r in rows:
        sym = str(r.get("symbol") or r.get("Symbol") or "").upper()
        # підтримка start/end як ISO або *_ms як int
        start_ms = _coerce_ms(r.get("start_ms"), r.get("start"))
        end_ms = _coerce_ms(r.get("end_ms"), r.get("end"))
        if not sym or start_ms is None or end_ms is None:
            continue
        scenario = r.get("scenario") or r.get("Scenario")
        tf = r.get("tf") or r.get("interval") or r.get("granularity")
        episodes.append(
            Episode(
                symbol=sym, start_ms=start_ms, end_ms=end_ms, scenario=scenario, tf=tf
            )
        )
    return episodes


def _coerce_ms(ms_val: Any, iso_val: Any) -> int | None:
    try:
        if ms_val is not None:
            v = int(ms_val)
            return v if v >= 0 else None
    except Exception:
        pass
    # Парсити ISO як YYYY-MM-DDTHH:MM:SSZ або без секунд
    if isinstance(iso_val, str) and iso_val:
        s = iso_val.strip().replace("Z", "")
        # найпростіший спосіб: уникати важких залежностей — очікуємо, що маніфест вже мав *_ms у більшості випадків
        from datetime import UTC, datetime

        fmts = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"]
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt).replace(tzinfo=UTC)
                return int(dt.timestamp() * 1000)
            except Exception:
                continue
    return None


def _iter_episode_dirs(root: Path) -> Iterable[tuple[Path, dict[str, Any]]]:
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        m = RE_EP_DIR.match(child.name)
        if not m:
            continue
        info = {
            "symbol": m.group("symbol").upper(),
            "tf": m.group("tf"),
            "start_ms": int(m.group("start")),
            "end_ms": int(m.group("end")),
        }
        yield child, info


def _read_phase_logs_for_strict_ok(p: Path) -> int:
    jpath = p / "phase_logs.jsonl"
    if not jpath.exists():
        return 0
    try:
        with jpath.open("r", encoding="utf-8") as fh:
            for line in fh:
                if "[REPLAY] window=" in line and "strict=OK" in line:
                    return 1
    except Exception:
        return 0
    return 0


def _read_summary_rows(p: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cpath = p / "summary.csv"
    if not cpath.exists():
        return rows
    with cpath.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append(dict(r))
    return rows


def _read_phase_logs_rows(p: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    jpath = p / "phase_logs.jsonl"
    if not jpath.exists():
        return rows
    from datetime import datetime

    def _iso_to_ms(s: str) -> int | None:
        try:
            s = s.strip().replace("Z", "+00:00")
            return int(datetime.fromisoformat(s).timestamp() * 1000)
        except Exception:
            return None

    with jpath.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("event") != "phase_detected":
                continue
            ts_ms = None
            if isinstance(obj.get("payload"), dict):
                t0 = obj["payload"].get("timestamp")
                try:
                    if t0 is not None:
                        ts_ms = int(t0)
                except Exception:
                    ts_ms = None
            if ts_ms is None and isinstance(obj.get("ts"), str):
                ts_ms = _iso_to_ms(obj["ts"]) or None
            if ts_ms is None and obj.get("timestamp_ms") is not None:
                try:
                    ts_ms = int(obj.get("timestamp_ms"))
                except Exception:
                    ts_ms = None
            if ts_ms is None:
                continue
            phase = obj.get("phase")
            score = obj.get("score")
            reasons = obj.get("reasons")
            if isinstance(reasons, list):
                reasons_str = json.dumps(reasons, ensure_ascii=False)
            elif isinstance(reasons, str):
                reasons_str = reasons
            else:
                reasons_str = ""
            presence = obj.get("whale_presence")
            rows.append(
                {
                    "timestamp_ms": ts_ms,
                    "timestamp_iso": obj.get("ts") or "",
                    "phase": phase,
                    "confidence": score,
                    "reasons": reasons_str,
                    "presence": presence,
                    "phase_source": "logs",
                }
            )
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Агрегація пер-епізодних summary у replay_summary.csv"
    )
    ap.add_argument(
        "--in",
        dest="in_dir",
        required=True,
        help="Каталог з пер-епізодними папками (наприклад, <out>/run)",
    )
    ap.add_argument(
        "--episodes-manifest",
        dest="manifest",
        help="Шлях до episodes_manifest.jsonl/JSON",
    )
    ap.add_argument(
        "--set-scenario-from-manifest",
        action="store_true",
        help="Проставляти scenario_detected із маніфесту",
    )
    ap.add_argument(
        "--merge-phases-from-logs",
        action="store_true",
        help="Збагачувати summary бар-рівневими фазами з phase_logs.jsonl",
    )
    args = ap.parse_args()

    root = Path(args.in_dir)
    root.mkdir(parents=True, exist_ok=True)

    episodes_by_key: dict[tuple[str, int, int], Episode] = {}
    if args.manifest:
        for ep in _load_manifest(Path(args.manifest)):
            episodes_by_key[(ep.symbol, ep.start_ms, ep.end_ms)] = ep

    all_rows: list[dict[str, Any]] = []

    for ep_dir, meta in _iter_episode_dirs(root):
        base_rows = _read_summary_rows(ep_dir)
        rows: list[dict[str, Any]] = []
        if args.merge_phases_from_logs:
            logs_rows = _read_phase_logs_rows(ep_dir)
            # Побудуємо індекс по timestamp_ms для summary
            by_ts: dict[int, dict[str, Any]] = {}
            for r in base_rows:
                try:
                    ts_key = int(r.get("timestamp_ms"))
                except Exception:
                    continue
                by_ts[ts_key] = dict(r)
            # Об'єднаний набір часових міток
            all_ts: set[int] = set(by_ts.keys()) | {
                int(x["timestamp_ms"]) for x in logs_rows
            }
            # Індекс логів по ts
            logs_by_ts: dict[int, dict[str, Any]] = {
                int(x["timestamp_ms"]): x for x in logs_rows
            }
            merged: list[dict[str, Any]] = []
            for ts in sorted(all_ts):
                srow = dict(by_ts.get(ts) or {})
                lrow = logs_by_ts.get(ts)
                if lrow:
                    # Пріоритет логів для фаз/увереності/причин/таймів/присутності
                    for k in (
                        "timestamp_ms",
                        "timestamp_iso",
                        "phase",
                        "confidence",
                        "reasons",
                        "presence",
                        "phase_source",
                    ):
                        v = lrow.get(k)
                        if v not in (None, ""):
                            srow[k] = v
                # Мінімально забезпечуємо timestamp_ms
                srow.setdefault("timestamp_ms", ts)
                merged.append(srow)
            rows = merged
            # Перезапишемо оновлений summary.csv у пер-епізодній папці
            if rows:
                hdrs: list[str] = []
                for r in rows:
                    for k in r.keys():
                        if k not in hdrs:
                            hdrs.append(k)
                with (ep_dir / "summary.csv").open(
                    "w", encoding="utf-8", newline=""
                ) as fh:
                    w = csv.DictWriter(fh, fieldnames=hdrs)
                    w.writeheader()
                    w.writerows(rows)
        else:
            rows = base_rows
        if not rows:
            continue
        strict_ok = _read_phase_logs_for_strict_ok(ep_dir)
        symbol = meta["symbol"]
        start_ms = meta["start_ms"]
        end_ms = meta["end_ms"]
        tf = str(meta.get("tf") or "")
        ep = episodes_by_key.get((symbol, start_ms, end_ms))
        scenario_detected: str | None = None
        episode_tf: str | None = tf
        if args.set_scenario_from_manifest and ep is not None:
            scenario_detected = ep.scenario or None
            episode_tf = ep.tf or tf
        eid = f"{symbol}:{start_ms}-{end_ms}"
        for r in rows:
            r = dict(r)
            r.setdefault("episode_id", eid)
            r.setdefault("episode_tf", episode_tf)
            r.setdefault("strict_ok", strict_ok)
            if scenario_detected is not None:
                r["scenario_detected"] = scenario_detected
            all_rows.append(r)

    # Якщо нічого не знайшли — створимо порожній файл
    out_csv = root / "replay_summary.csv"
    if not all_rows:
        out_csv.write_text("", encoding="utf-8")
        return 0

    # Збір уніфікованого хедеру
    fieldnames: list[str] = []
    for r in all_rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    with out_csv.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
