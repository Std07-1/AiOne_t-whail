"""Аналайзер телеметрії Stage1/Stage3 (JSONL → зведений звіт).

Призначення
- Зчитує JSONL‑логи з директорії телеметрії (див. config.TELEMETRY_BASE_DIR).
- Підтримує файли:
    - stage1_events.jsonl (події Stage1 через telemetry_sink)
    - stage1_signals.jsonl (додаткові записи з Stage1 монітора, напр. volume_spike_reject)
    - stage3_events.jsonl (події Stage3 через telemetry_sink)
- Формує стабільний, розширюваний звіт (Markdown/JSON), без потреби щоразу змінювати код.

Детальний звіт по символу:
- Виклик з параметром --symbol BTCUSDT додає секцію “Symbol detail: BTCUSDT”.
- Показує останні ALERT-и з часом, reasons, фазою та whale-полями (presence, bias), якщо вони були в payload.

Перевірка “правдивості” (follow-through):
- Параметр --verify N обчислює для кожного ALERT у цього символу, чи ціна протягом наступних N барів досягла +1 ATR раніше, ніж -1 ATR (успіх/провал), а також середні runup/drawdown в ATR.
- Дані беруться з snapshotів у datastore/*_bars_1m_snapshot.jsonl. Якщо snapshot відсутній або бракує ATR/ціни в події — пропускаємо кейс, але не ламаємо звіт.

Як користуватись

Спочатку:
python.exe -m tools.analyze_telemetry --out telemetry_report.md --json telemetry_report.json

Загальний звіт (весь портфель):
        python -m tools.analyze_telemetry

Детальний по символу (без верифікації):
        python -m tools.analyze_telemetry --symbol BTCUSDT

Детальний по символу + перевірка follow-through на 10 барів:
        python -m tools.analyze_telemetry --symbol BTCUSDT --verify 10

Обмежити кількість показаних ALERT-ів у секції символу (наприклад, 10):
        python -m tools.analyze_telemetry --symbol BTCUSDT --verify 10 --max-alerts 10

Зберегти у файли:
        python -m tools.analyze_telemetry --out telemetry_report.md --json telemetry_report.json
        по символу: додайте --symbol/--verify, за потреби.

Як читати результат

Загальні секції:
- Stage1: скільки подій, розподіл сигналів, топ-тригери.
- Phase (розширено): розподіл фаз, середній score по фазах, топ reasons, HTF-статистика, середні directional/whale метрики.
- Stage2 hints: розподіл напрямків і середній score.
- Stage1 signals (монітор): статистика відхилень volume_spike (причини).

По символу (Symbol detail):
- Список останніх ALERT-ів (час, причини, фаза, whale).
- Follow-through: evaluated — скільки ALERT-ів змогли оцінити; success_rate — частка кейсів, де +1 ATR був до -1 ATR; avg_runup_atr/avg_drawdown_atr — середні рухи в ATR.

Архітектурні нотатки
- Жодних змін контрактів Stage1/Stage2/Stage3.
- Шляхи і імена файлів беруться з config де можливо; є безпечні дефолти.
- Best‑effort до форматів: пропускаємо зламані рядки, не кидаємо винятків назовні.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _parse_iso_or_epoch(obj: Any) -> float | None:
    """Повертає epoch‑секунди з різних форматів часу (ISO, ms, s).

    Args:
        obj: Рядок ISO8601 або число в сек/мс.

    Returns:
        float | None: Epoch seconds або None, якщо невідомий формат.
    """

    if obj is None:
        return None
    # Числові значення: мс/сек
    try:
        if isinstance(obj, (int, float)):
            v = float(obj)
            if v > 1e12:  # мілісекунди
                return v / 1000.0
            if v > 1e9:  # секунди
                return v
    except Exception:
        pass

    # ISO‑рядки з суфіксом Z або без
    if isinstance(obj, str) and obj:
        s = obj.strip()
        # Нормалізація суфікса Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s).timestamp()
        except Exception:
            # fallback: спробувати без таймзони, уявляючи як UTC
            try:
                return (
                    datetime.fromisoformat(s.replace("+00:00", ""))
                    .replace(tzinfo=UTC)
                    .timestamp()
                )
            except Exception:
                return None
    return None


def _read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Зчитує JSONL без винятків: ламаються рядки — пропускаємо.

    Args:
        path: Шлях до JSONL‑файла.

    Yields:
        dict: Розпарсені об’єкти, якщо можливо.
    """

    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        yield obj
                except Exception:
                    continue
    except FileNotFoundError:
        return


@dataclass
class TelemetryInput:
    base_dir: Path
    stage1_events_name: str = "stage1_events.jsonl"
    stage1_signals_name: str = "stage1_signals.jsonl"
    stage3_events_name: str = "stage3_events.jsonl"

    @classmethod
    def from_config(cls) -> TelemetryInput:
        """Створює конфігурацію шляхів із config, із дефолтами при відсутності.

        Returns:
            TelemetryInput: Налаштований вхід.
        """

        try:
            import config.config as _cfg  # type: ignore  # noqa: I001

            telemetry_base_dir = str(_cfg.TELEMETRY_BASE_DIR)
            stage1_events_name = str(_cfg.STAGE1_EVENTS_LOG)
            stage3_events_name = str(_cfg.STAGE3_EVENTS_LOG)
        except Exception:
            telemetry_base_dir = "./telemetry"
            stage1_events_name = "stage1_events.jsonl"
            stage3_events_name = "stage3_events.jsonl"

        return cls(
            base_dir=Path(telemetry_base_dir),
            stage1_events_name=stage1_events_name,
            stage1_signals_name="stage1_signals.jsonl",
            stage3_events_name=stage3_events_name,
        )


@dataclass
class AnalyzeParams:
    since: float | None = None  # epoch seconds
    until: float | None = None  # epoch seconds
    symbol: str | None = None  # target symbol filter (e.g., BTCUSDT)
    verify_nbars: int = 0  # follow-through verification window (bars); 0 = disabled
    max_alerts_list: int = 20  # max recent alerts to list in Markdown


def _within_range(ts: float | None, p: AnalyzeParams) -> bool:
    if ts is None:
        return True  # не фільтруємо, якщо немає часу
    if p.since is not None and ts < p.since:
        return False
    if p.until is not None and ts > p.until:
        return False
    return True


def _flatten_reasons(val: Any) -> Iterable[str]:
    if isinstance(val, list):
        for x in val:
            if isinstance(x, str) and x:
                yield x
    elif isinstance(val, str) and val:
        yield val


def analyze_directory(
    inp: TelemetryInput, params: AnalyzeParams | None = None
) -> tuple[dict[str, Any], str]:
    """Основна функція аналайзера: читає файли, рахує метрики, віддає JSON+Markdown.

    Args:
        inp: Вхідні шляхи/імена файлів (із config або CLI).
        params: Обмеження часу (since/until у epoch секундах).

    Returns:
        (summary_json, markdown_report)
    """

    p = params or AnalyzeParams()
    base = inp.base_dir
    s1e_path = base / inp.stage1_events_name
    s1s_path = base / inp.stage1_signals_name
    s3e_path = base / inp.stage3_events_name

    present = {
        "stage1_events": s1e_path.exists(),
        "stage1_signals": s1s_path.exists(),
        "stage3_events": s3e_path.exists(),
    }

    # Агрегатори Stage1
    s1_total = 0
    s1_by_signal: dict[str, int] = {}
    s1_by_event: dict[str, int] = {}
    s1_by_symbol: dict[str, int] = {}
    s1_trigger_reasons: dict[str, int] = {}
    s1_trap_fired = 0
    s1_trap_count = 0
    s1_trap_score_sum = 0.0

    # Розширені агрегації для phase_detected і повного дашборду
    phase_total = 0
    phase_by_name: dict[str, int] = {}
    phase_score_sum_by_name: dict[str, float] = {}
    phase_reasons: dict[str, int] = {}
    volz_source_dist: dict[str, int] = {}
    # Context-only reasons + guard/tags aggregations
    ctx_only_events = 0
    ctx_only_reason_counts: dict[str, int] = {}
    tags_counts: dict[str, int] = {}
    # Coverage counters per key
    coverage_keys = [
        "band_pct",
        "near_edge",
        "atr_ratio",
        "vol_z",
        "rsi",
        "dvr",
        "cd",
        "slope_atr",
        "volz_source",
        "whale_presence",
        "whale_bias",
        "whale_vwap_dev",
        "whale_vol_regime",
        "whale_age_ms",
        "whale_stale",
    ]
    coverage_present: dict[str, int] = {k: 0 for k in coverage_keys}
    coverage_total: dict[str, int] = {k: 0 for k in coverage_keys}
    # Missing fields tallies (top-N diagnostics)
    missing_fields: dict[str, int] = {}
    htf_ok_count = 0
    htf_seen = 0
    htf_score_sum = 0.0
    htf_strength_sum = 0.0
    # Directional/базові метрики: суми та лічильники присутності
    agg_sum: dict[str, float] = {}
    agg_cnt: dict[str, int] = {}
    # Whale
    whale_stale_count = 0
    whale_seen = 0
    whale_vol_regime: dict[str, int] = {}
    zones_accum_sum = 0.0
    zones_dist_sum = 0.0

    # Hints (stage2_hint)
    hint_total = 0
    hint_by_dir: dict[str, int] = {}
    hint_score_sum = 0.0

    # Агрегатори Stage1_signals (rejects тощо)
    s1sig_total = 0
    s1sig_reject_by_reason: dict[str, int] = {}

    # Агрегатори Stage3
    s3_total = 0
    s3_by_event: dict[str, int] = {}
    s3_by_symbol: dict[str, int] = {}
    s3_rr_values: list[float] = []

    # Helper: optional per-symbol verification using datastore snapshots
    def _load_snapshot(sym: str) -> list[dict[str, Any]]:
        path = Path("./datastore") / f"{sym.lower()}_bars_1m_snapshot.jsonl"
        rows: list[dict[str, Any]] = []
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict) and "open_time" in obj:
                            rows.append(obj)
                    except Exception:
                        continue
        except FileNotFoundError:
            return []
        rows.sort(key=lambda r: r.get("open_time", 0))
        return rows

    def _verify_follow_through(
        sym: str,
        alerts: list[dict[str, Any]],
        nbars: int,
    ) -> dict[str, Any]:
        bars = _load_snapshot(sym)
        if not bars:
            return {"available": False}
        success = 0
        total = 0
        runup_sum = 0.0
        drawdown_sum = 0.0
        for ev in alerts:
            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}
            stats = (
                payload.get("stats") if isinstance(payload.get("stats"), dict) else {}
            )
            try:
                entry = float(
                    stats.get("current_price") or payload.get("current_price") or 0.0
                )
                atr = float(stats.get("atr") or payload.get("atr") or 0.0)
            except Exception:
                entry = 0.0
                atr = 0.0
            if entry <= 0 or atr <= 0:
                continue
            # event time in ms
            ts_ms = None
            try:
                v = stats.get("timestamp") or payload.get("timestamp")
                if isinstance(v, (int, float)):
                    ts_ms = int(v)
            except Exception:
                ts_ms = None
            if ts_ms is None:
                tsec = _parse_iso_or_epoch(ev.get("ts"))
                ts_ms = int(tsec * 1000) if tsec else None
            if ts_ms is None:
                continue
            # find first bar index with open_time >= ts_ms
            lo, hi = 0, len(bars) - 1
            idx = None
            while lo <= hi:
                mid = (lo + hi) // 2
                ot = int(bars[mid].get("open_time", 0))
                if ot < ts_ms:
                    lo = mid + 1
                else:
                    idx = mid
                    hi = mid - 1
            if idx is None:
                continue
            window = bars[idx : idx + nbars]
            if not window:
                continue
            max_high = max(float(b.get("high", 0.0)) for b in window)
            min_low = min(float(b.get("low", 0.0)) for b in window)
            runup = max(0.0, max_high - entry)
            drawdown = max(0.0, entry - min_low)
            # order of hits (first touch of +/-1 ATR)
            outcome = None
            up_level = entry + atr
            dn_level = entry - atr
            for b in window:
                try:
                    high = float(b.get("high", 0.0))
                    low = float(b.get("low", 0.0))
                except Exception:
                    continue
                if high >= up_level:
                    outcome = "up"
                    break
                if low <= dn_level:
                    outcome = "down"
                    break
            total += 1
            runup_sum += (runup / atr) if atr else 0.0
            drawdown_sum += (drawdown / atr) if atr else 0.0
            if outcome == "up":
                success += 1
        return {
            "available": True,
            "evaluated": total,
            "success_rate": (success / total) if total else None,
            "avg_runup_atr": (runup_sum / total) if total else None,
            "avg_drawdown_atr": (drawdown_sum / total) if total else None,
        }

    # Stage1 events
    symbol_last_phases: list[dict[str, Any]] = []
    for row in _read_jsonl(s1e_path):
        # ts полем telemetry_sink є ISO (рядок)
        ts = _parse_iso_or_epoch(row.get("ts"))
        if not _within_range(ts, p):
            continue
        sym = str(row.get("symbol") or "").upper() or "UNKNOWN"
        # optional symbol filter
        if p.symbol and sym != p.symbol.upper():
            continue
        s1_total += 1
        ev = str(row.get("event") or "").strip() or "unknown"
        s1_by_event[ev] = s1_by_event.get(ev, 0) + 1
        s1_by_symbol[sym] = s1_by_symbol.get(sym, 0) + 1
        if ev == "stage1_signal":
            sig = (
                str(
                    row.get("signal") or row.get("payload", {}).get("signal") or ""
                ).strip()
                or "NONE"
            )
            s1_by_signal[sig] = s1_by_signal.get(sig, 0) + 1
            reasons = row.get("trigger_reasons") or row.get("payload", {}).get(
                "trigger_reasons"
            )
            for r in _flatten_reasons(reasons):
                s1_trigger_reasons[r] = s1_trigger_reasons.get(r, 0) + 1
        elif ev == "trap_eval":
            s1_trap_count += 1
            try:
                if row.get("fired") is True or (
                    row.get("payload", {}).get("fired") is True
                ):
                    s1_trap_fired += 1
            except Exception:
                pass
            try:
                score = row.get("score")
                if score is None:
                    score = (row.get("payload", {}) or {}).get("score")
                if isinstance(score, (int, float)):
                    s1_trap_score_sum += float(score)
            except Exception:
                pass
        elif ev == "phase_detected":
            phase_total += 1
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            # Дістаємо безпечним способом поля з верхнього рівня або payload
            _payload = payload

            def gp(
                k: str, _row: dict[str, Any] = row, _payload: dict[str, Any] = _payload
            ) -> Any:
                return _row[k] if k in _row else _payload.get(k)

            name = str(gp("phase") or "").strip() or "unknown"
            try:
                score = float(gp("score")) if gp("score") is not None else None
            except Exception:
                score = None
            phase_by_name[name] = phase_by_name.get(name, 0) + 1
            if isinstance(score, (int, float)):
                phase_score_sum_by_name[name] = phase_score_sum_by_name.get(
                    name, 0.0
                ) + float(score)

            # reasons
            for r in _flatten_reasons(gp("reasons")):
                phase_reasons[r] = phase_reasons.get(r, 0) + 1

            # HTF telemetry
            htf_seen += 1
            try:
                if gp("htf_ok") is True:
                    htf_ok_count += 1
            except Exception:
                pass
            try:
                v = gp("htf_score")
                if isinstance(v, (int, float)):
                    htf_score_sum += float(v)
            except Exception:
                pass
            try:
                v = gp("htf_strength")
                if isinstance(v, (int, float)):
                    htf_strength_sum += float(v)
            except Exception:
                pass

            # Для режиму символа: накопичити останні події phase_detected з ключовими метриками
            if p.symbol:
                try:
                    # ISO ts як є; надійний парсер нижче при сортуванні
                    ts_iso = row.get("ts")
                    # stage2_hint може бути в payload як dict
                    hint_dir = None
                    hint_score = None
                    try:
                        hint_blk = (
                            payload.get("stage2_hint")
                            if isinstance(payload.get("stage2_hint"), dict)
                            else None
                        )
                        if isinstance(hint_blk, dict):
                            hd = hint_blk.get("dir")
                            hs = hint_blk.get("score")
                            hint_dir = str(hd).upper() if isinstance(hd, str) else None
                            hint_score = (
                                float(hs) if isinstance(hs, (int, float)) else None
                            )
                    except Exception:
                        hint_dir = None
                        hint_score = None

                    symbol_last_phases.append(
                        {
                            "ts": ts_iso,
                            "phase": name,
                            "score": score,
                            "band_pct": gp("band_pct"),
                            "atr_ratio": gp("atr_ratio"),
                            "vol_z": (
                                gp("vol_z")
                                if gp("vol_z") is not None
                                else gp("volume_z")
                            ),
                            "dvr": gp("dvr") or gp("directional_volume_ratio"),
                            "cd": gp("cd") or gp("cumulative_delta"),
                            "htf_ok": gp("htf_ok"),
                            "htf_strength": gp("htf_strength"),
                            "stage2_hint": {
                                "dir": hint_dir,
                                "score": hint_score,
                            },
                        }
                    )
                except Exception:
                    pass

            # Базові/Directional ключі: band_pct, near_edge, atr_ratio, vol_z, rsi, atr, current_price, dvr, cd, slope_atr
            for k in [
                "band_pct",
                "near_edge",
                "atr_ratio",
                "vol_z",
                "rsi",
                "atr",
                "current_price",
                "dvr",
                "cd",
                "slope_atr",
            ]:
                try:
                    v = gp(k)
                    if isinstance(v, (int, float)):
                        agg_sum[k] = agg_sum.get(k, 0.0) + float(v)
                        agg_cnt[k] = agg_cnt.get(k, 0) + 1
                    elif k == "near_edge":
                        # для булевого рахуємо як 1/0 для середнього
                        if isinstance(v, bool):
                            agg_sum[k] = agg_sum.get(k, 0.0) + (1.0 if v else 0.0)
                            agg_cnt[k] = agg_cnt.get(k, 0) + 1
                except Exception:
                    continue

            # Coverage accounting for those keys
            for ck in coverage_keys:
                try:
                    vv = gp(ck)
                    coverage_total[ck] += 1
                    if vv is not None and vv != "":
                        coverage_present[ck] += 1
                except Exception:
                    coverage_total[ck] += 1

            # Missing-field diagnostics (which of important fields are absent)
            must_keys = [
                "band_pct",
                "atr_ratio",
                "vol_z",
                "dvr",
                "cd",
                "slope_atr",
                "whale_presence",
                "whale_bias",
                "whale_vwap_dev",
                "whale_vol_regime",
            ]
            for mk in must_keys:
                try:
                    if gp(mk) in (None, ""):
                        missing_fields[mk] = missing_fields.get(mk, 0) + 1
                except Exception:
                    missing_fields[mk] = missing_fields.get(mk, 0) + 1

            # Whale fields
            whale_seen += 1
            try:
                if gp("whale_stale") is True:
                    whale_stale_count += 1
            except Exception:
                pass
            for wk in [
                "whale_presence",
                "whale_bias",
                "whale_vwap_dev",
                "whale_age_ms",
            ]:
                try:
                    v = gp(wk)
                    if isinstance(v, (int, float)):
                        agg_sum[wk] = agg_sum.get(wk, 0.0) + float(v)
                        agg_cnt[wk] = agg_cnt.get(wk, 0) + 1
                except Exception:
                    pass
            try:
                vr = str(gp("whale_vol_regime") or "").strip() or "unknown"
                whale_vol_regime[vr] = whale_vol_regime.get(vr, 0) + 1
            except Exception:
                pass
            try:
                vzs = str(gp("volz_source") or "").strip() or "unknown"
                volz_source_dist[vzs] = volz_source_dist.get(vzs, 0) + 1
            except Exception:
                pass
            # Context-only reasons (if any)
            try:
                cor = gp("context_only_reasons")
                if isinstance(cor, list) and cor:
                    ctx_only_events += 1
                    for r in cor:
                        if isinstance(r, str) and r:
                            ctx_only_reason_counts[r] = (
                                ctx_only_reason_counts.get(r, 0) + 1
                            )
            except Exception:
                pass
            # Tags (guards like heavy_skip, whale_stale, etc.)
            try:
                tags = gp("tags")
                if isinstance(tags, list):
                    for t in tags:
                        if isinstance(t, str) and t:
                            tags_counts[t] = tags_counts.get(t, 0) + 1
            except Exception:
                pass
            try:
                acc = gp("whale_accum_cnt")
                if isinstance(acc, (int, float)):
                    zones_accum_sum += float(acc)
            except Exception:
                pass
            try:
                dst = gp("whale_dist_cnt")
                if isinstance(dst, (int, float)):
                    zones_dist_sum += float(dst)
            except Exception:
                pass
        elif ev == "stage2_hint":
            hint_total += 1
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            dir_hint = str(payload.get("dir") or "").strip() or "unknown"
            hint_by_dir[dir_hint] = hint_by_dir.get(dir_hint, 0) + 1
            try:
                sc = payload.get("score")
                if isinstance(sc, (int, float)):
                    hint_score_sum += float(sc)
            except Exception:
                pass

    # Перелік останніх ALERT подій (для symbol-режиму)
    recent_alerts: list[dict[str, Any]] = []
    if p.symbol:
        # перечитати stage1_events.jsonl для вибірки ALERTів цього символу
        # Перелік останніх phase_detected (для symbol-режиму)
        recent_phases: list[dict[str, Any]] = []

        for row in _read_jsonl(s1e_path):
            sym = str(row.get("symbol") or "").upper() or "UNKNOWN"
            if sym != p.symbol.upper():
                continue
            evn = str(row.get("event") or "").strip()
            if evn == "stage1_signal":
                sig = (
                    str(
                        row.get("signal") or row.get("payload", {}).get("signal") or ""
                    ).strip()
                    or "NONE"
                )
                if sig.upper() == "ALERT":
                    recent_alerts.append(row)
            elif evn == "phase_detected":
                recent_phases.append(row)
        # відсортувати за ts і обрізати до ліміту
        recent_alerts.sort(
            key=lambda r: _parse_iso_or_epoch(r.get("ts")) or 0.0, reverse=True
        )
        if len(recent_alerts) > p.max_alerts_list:
            recent_alerts = recent_alerts[: p.max_alerts_list]
        recent_phases.sort(
            key=lambda r: _parse_iso_or_epoch(r.get("ts")) or 0.0, reverse=True
        )
        if len(recent_phases) > 5:
            recent_phases = recent_phases[:5]

    # Stage1 signals (моніторинговий файл, напр. volume_spike_reject)
    for row in _read_jsonl(s1s_path):
        # Поля часу: timestamp_iso | timestamp_ms
        ts = _parse_iso_or_epoch(row.get("timestamp_iso") or row.get("timestamp_ms"))
        if not _within_range(ts, p):
            continue
        s1sig_total += 1
        if str(row.get("event") or "").strip() == "volume_spike_reject":
            raw_reason = row.get("reject_reason")
            reason_norm = str(raw_reason).strip()
            # Нормалізуємо неоднозначні значення у зручну мітку
            reason_key = reason_norm.lower()
            if reason_key in {"", "none", "null", "n/a", "na", "unknown"}:
                reason_norm = "unspecified"
            s1sig_reject_by_reason[reason_norm] = (
                s1sig_reject_by_reason.get(reason_norm, 0) + 1
            )

    # Stage3 events
    for row in _read_jsonl(s3e_path):
        ts = _parse_iso_or_epoch(row.get("ts"))
        if not _within_range(ts, p):
            continue
        s3_total += 1
        ev = str(row.get("event") or "").strip() or "unknown"
        sym = str(row.get("symbol") or "").upper() or "UNKNOWN"
        s3_by_event[ev] = s3_by_event.get(ev, 0) + 1
        s3_by_symbol[sym] = s3_by_symbol.get(sym, 0) + 1
        # RR ratio, якщо присутній у payload (різні продюсери можуть додавати)
        rr = row.get("rr_ratio")
        if rr is None and isinstance(row.get("payload"), dict):
            rr = row["payload"].get("rr_ratio")
        if isinstance(rr, (int, float)):
            try:
                s3_rr_values.append(float(rr))
            except Exception:
                pass

    def _topn(d: dict[str, int], n: int = 10) -> list[tuple[str, int]]:
        return sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]

    # Середні значення по агрегаторах
    def _avg(k: str) -> float | None:
        c = agg_cnt.get(k, 0)
        return (agg_sum.get(k, 0.0) / c) if c else None

    # Підсумковий JSON
    summary: dict[str, Any] = {
        "inputs": {
            "base_dir": str(base.resolve()),
            "present": present,
        },
        "stage1": {
            "events_total": s1_total,
            "by_event": s1_by_event,
            "by_signal": s1_by_signal,
            "top_symbols": _topn(s1_by_symbol),
            "trigger_reason_counts": s1_trigger_reasons,
            "trap": {
                "count": s1_trap_count,
                "volz_source": volz_source_dist,
                "fired": s1_trap_fired,
                "avg_score": (
                    (s1_trap_score_sum / s1_trap_count) if s1_trap_count else None
                ),
            },
            "phase": {
                "total": phase_total,
                "by_name": phase_by_name,
                "avg_score_by_name": {
                    k: (phase_score_sum_by_name.get(k, 0.0) / v) if v else None
                    for k, v in phase_by_name.items()
                },
                "reasons_top": sorted(
                    phase_reasons.items(), key=lambda kv: kv[1], reverse=True
                )[:12],
                "htf": {
                    "seen": htf_seen,
                    "ok_rate": (htf_ok_count / htf_seen) if htf_seen else None,
                    "avg_score": (htf_score_sum / htf_seen) if htf_seen else None,
                    "avg_strength": (htf_strength_sum / htf_seen) if htf_seen else None,
                },
                "directional": {
                    "avg_band_pct": _avg("band_pct"),
                    "near_edge_rate": _avg("near_edge"),
                    "avg_atr_ratio": _avg("atr_ratio"),
                    "avg_vol_z": _avg("vol_z"),
                    "avg_rsi": _avg("rsi"),
                    "avg_dvr": _avg("dvr"),
                    "avg_cd": _avg("cd"),
                    "avg_slope_atr": _avg("slope_atr"),
                },
                "whale": {
                    "seen": whale_seen,
                    "stale_rate": (
                        (whale_stale_count / whale_seen) if whale_seen else None
                    ),
                    "avg_presence": _avg("whale_presence"),
                    "avg_bias": _avg("whale_bias"),
                    "avg_vwap_dev": _avg("whale_vwap_dev"),
                    "avg_age_ms": _avg("whale_age_ms"),
                    "vol_regime": whale_vol_regime,
                    "zones_avg": {
                        "accum_cnt": (
                            (zones_accum_sum / whale_seen) if whale_seen else None
                        ),
                        "dist_cnt": (
                            (zones_dist_sum / whale_seen) if whale_seen else None
                        ),
                    },
                },
                "context_only": {
                    "events_with_context_only": ctx_only_events,
                    "top_reasons": sorted(
                        ctx_only_reason_counts.items(),
                        key=lambda kv: kv[1],
                        reverse=True,
                    )[:12],
                },
                "guards": {
                    "tags_top": sorted(
                        tags_counts.items(), key=lambda kv: kv[1], reverse=True
                    )[:12],
                    "heavy_skip_count": tags_counts.get("heavy_skip", 0),
                },
            },
            "hints": {
                "total": hint_total,
                "by_dir": hint_by_dir,
                "avg_score": (hint_score_sum / hint_total) if hint_total else None,
            },
            "coverage": {
                k: {
                    "present_pct": (
                        (coverage_present.get(k, 0) / coverage_total.get(k, 1))
                        if coverage_total.get(k, 0)
                        else None
                    )
                }
                for k in coverage_keys
            },
            "missing_fields_top": sorted(
                missing_fields.items(), key=lambda kv: kv[1], reverse=True
            )[:12],
        },
        "stage1_signals": {
            "rows_total": s1sig_total,
            "rejects_by_reason": s1sig_reject_by_reason,
        },
        "stage3": {
            "events_total": s3_total,
            "by_event": s3_by_event,
            "top_symbols": _topn(s3_by_symbol),
            "rr_ratio": {
                "count": len(s3_rr_values),
                "avg": (
                    (sum(s3_rr_values) / len(s3_rr_values)) if s3_rr_values else None
                ),
                "min": min(s3_rr_values) if s3_rr_values else None,
                "max": max(s3_rr_values) if s3_rr_values else None,
            },
        },
    }

    def _fmt_dt(ts: float | None) -> str:
        if ts is None:
            return "—"
        return datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")

    def _fmt_opt(v: Any, *, digits: int = 3, as_int: bool = False) -> str:
        """Форматує опційні значення: число → з точністю, None/інше → '—'."""
        try:
            if v is None:
                return "—"
            if as_int and isinstance(v, (int, float)):
                return str(int(v))
            if isinstance(v, (int, float)):
                return f"{float(v):.{digits}f}"
        except Exception:
            return "—"
        return "—"

    # Markdown‑звіт
    md_lines: list[str] = []
    md_lines.append("# Звіт телеметрії\n")
    md_lines.append(f"Директорія: `{summary['inputs']['base_dir']}`  ")
    md_lines.append(f"Вікно: since={_fmt_dt(p.since)} until={_fmt_dt(p.until)}  \n")
    md_lines.append("")

    # Stage1
    md_lines.append("## Stage1")
    md_lines.append(f"Подій: {s1_total}")
    if s1_by_signal:
        md_lines.append(
            "- Сигнали за типом: "
            + ", ".join(f"{k}: {v}" for k, v in sorted(s1_by_signal.items()))
        )
    if s1_by_event:
        md_lines.append(
            "- Події: " + ", ".join(f"{k}: {v}" for k, v in sorted(s1_by_event.items()))
        )
    if s1_trigger_reasons:
        top_reasons = sorted(
            s1_trigger_reasons.items(), key=lambda kv: kv[1], reverse=True
        )[:12]
        md_lines.append(
            "- Топ trigger_reasons: " + ", ".join(f"{k}: {v}" for k, v in top_reasons)
        )
    if s1_trap_count:
        md_lines.append(
            f"- TRAP: count={s1_trap_count} fired={s1_trap_fired} avg_score={summary['stage1']['trap']['avg_score']:.3f}"
        )

    # Phase (розширено)
    md_lines.append("\n### Phase (розширено)")
    md_lines.append(f"Визначень фази: {phase_total}")
    if phase_by_name:
        md_lines.append(
            "- Розподіл фаз: "
            + ", ".join(f"{k}: {v}" for k, v in sorted(phase_by_name.items()))
        )
    avg_by_name = summary["stage1"]["phase"]["avg_score_by_name"]
    if avg_by_name:
        md_lines.append(
            "- Середній score по фазах: "
            + ", ".join(
                f"{k}: {v:.3f}" for k, v in sorted(avg_by_name.items()) if v is not None
            )
        )
    if phase_reasons:
        top_reasons = summary["stage1"]["phase"]["reasons_top"]
        md_lines.append(
            "- Топ reasons: " + ", ".join(f"{k}: {v}" for k, v in top_reasons)
        )
    vzs = summary["stage1"]["phase"].get("volz_source") or {}
    if vzs:
        md_lines.append(
            "- volz_source: " + ", ".join(f"{k}: {v}" for k, v in sorted(vzs.items()))
        )
    htf = summary["stage1"]["phase"]["htf"]
    md_lines.append(
        "- HTF: "
        f"seen={htf['seen']} "
        f"ok_rate={_fmt_opt(htf['ok_rate'])} "
        f"avg_score={_fmt_opt(htf['avg_score'])} "
        f"avg_strength={_fmt_opt(htf['avg_strength'])}"
    )
    dirsec = summary["stage1"]["phase"]["directional"]
    md_lines.append(
        "- Directional avg: "
        f"band_pct={_fmt_opt(dirsec['avg_band_pct'])} "
        f"near_edge_rate={_fmt_opt(dirsec['near_edge_rate'])} "
        f"atr_ratio={_fmt_opt(dirsec['avg_atr_ratio'])} "
        f"vol_z={_fmt_opt(dirsec['avg_vol_z'])} "
        f"rsi={_fmt_opt(dirsec['avg_rsi'])} "
        f"dvr={_fmt_opt(dirsec['avg_dvr'])} "
        f"cd={_fmt_opt(dirsec['avg_cd'])} "
        f"slope_atr={_fmt_opt(dirsec['avg_slope_atr'])}"
    )
    wsec = summary["stage1"]["phase"]["whale"]
    md_lines.append(
        "- Whale: "
        f"seen={wsec['seen']} "
        f"stale_rate={_fmt_opt(wsec['stale_rate'])} "
        f"presence={_fmt_opt(wsec['avg_presence'])} "
        f"bias={_fmt_opt(wsec['avg_bias'])} "
        f"vwap_dev={_fmt_opt(wsec['avg_vwap_dev'])} "
        f"age_ms={_fmt_opt(wsec['avg_age_ms'], as_int=True)} "
        "vol_regime: "
        + ", ".join(f"{k}: {v}" for k, v in sorted(wsec["vol_regime"].items()))
    )

    # Context-only coverage
    ctx = summary["stage1"]["phase"].get("context_only") or {}
    md_lines.append("\n### Context-only coverage")
    md_lines.append(
        "- Подій із context_only_reasons: "
        + str(ctx.get("events_with_context_only", 0))
    )
    top_ctx = ctx.get("top_reasons") or []
    if top_ctx:
        md_lines.append(
            "- Топ context_only reasons: " + ", ".join(f"{k}: {v}" for k, v in top_ctx)
        )

    # Guards / Tags summary
    gsec = summary["stage1"]["phase"].get("guards") or {}
    md_lines.append("\n### Guards summary")
    md_lines.append("- heavy_skip_count: " + str(gsec.get("heavy_skip_count", 0)))
    ttop = gsec.get("tags_top") or []
    if ttop:
        md_lines.append("- Топ tags: " + ", ".join(f"{k}: {v}" for k, v in ttop))

    # Hints
    md_lines.append("\n### Stage2 hints")
    md_lines.append(f"Подій: {hint_total}")
    if hint_by_dir:
        md_lines.append(
            "- Напрямки: "
            + ", ".join(f"{k}: {v}" for k, v in sorted(hint_by_dir.items()))
        )
    if hint_total:
        md_lines.append(
            f"- Середній score: {summary['stage1']['hints']['avg_score']:.3f}"
        )

    # Coverage (ключові поля)
    cov = summary["stage1"].get("coverage") or {}
    if cov:
        md_lines.append("\n### Coverage по ключових полях")
        lines = []
        for k in sorted(cov.keys()):
            pct = cov[k].get("present_pct")
            lines.append(f"{k}={_fmt_opt(pct)}")
        md_lines.append("- " + ", ".join(lines))
    miss = summary["stage1"].get("missing_fields_top") or []
    if miss:
        md_lines.append(
            "- Відсутні ключі (топ): " + ", ".join(f"{k}: {v}" for k, v in miss)
        )

    # Stage1_signals (rejects)
    md_lines.append("\n## Stage1 signals (монітор)")
    md_lines.append(f"Рядків: {s1sig_total}")
    if s1sig_reject_by_reason:
        md_lines.append(
            "- Відхилення volume_spike за причиною: "
            + ", ".join(f"{k}: {v}" for k, v in sorted(s1sig_reject_by_reason.items()))
        )

    # Пер-символьний розділ (детально)
    if p.symbol:
        md_lines.append(f"\n## Symbol detail: {p.symbol.upper()}")
        md_lines.append(f"ALERTів (останні {p.max_alerts_list}): {len(recent_alerts)}")
        for ev in recent_alerts:
            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}

            def gp(
                k: str, _ev: dict[str, Any] = ev, _payload: dict[str, Any] = payload
            ) -> Any:
                return _ev[k] if k in _ev else _payload.get(k)

            # reasons: спочатку з кореня, потім з payload/raw_trigger_reasons як fallback
            reasons = gp("trigger_reasons") or gp("raw_trigger_reasons")
            # phase: з кореня/payload або з stats.phase
            phase = gp("phase") or (
                (gp("stats") or {}) if isinstance(gp("stats"), dict) else {}
            ).get("phase", {})
            # whale presence/bias: з кореня/payload або stats.whale
            stats_obj = gp("stats") if isinstance(gp("stats"), dict) else {}
            whale_presence = (
                gp("whale_presence")
                if gp("whale_presence") is not None
                else (stats_obj.get("whale") or {}).get("presence")
            )
            whale_bias = (
                gp("whale_bias")
                if gp("whale_bias") is not None
                else (stats_obj.get("whale") or {}).get("bias")
            )
            # Normalize for display (avoid None in report)
            if isinstance(reasons, list):
                reasons_list = [r for r in reasons if isinstance(r, str) and r]
            elif isinstance(reasons, str) and reasons:
                reasons_list = [reasons]
            else:
                reasons_list = []
            reasons_disp = reasons_list if reasons_list else "-"

            phase_name = phase.get("name") if isinstance(phase, dict) else phase
            if not isinstance(phase_name, str) or not phase_name:
                phase_name = "unknown"

            md_lines.append(
                "- "
                f"ts={ev.get('ts')} "
                f"reasons={reasons_disp} "
                f"phase={phase_name} "
                f"presence={_fmt_opt(whale_presence)} bias={_fmt_opt(whale_bias)}"
            )
        # Follow-through verification (опційно)
        if p.verify_nbars > 0:
            vt = _verify_follow_through(p.symbol, recent_alerts, p.verify_nbars)
            if vt.get("available"):
                md_lines.append(
                    "- Follow-through ("
                    f"{p.verify_nbars} bars) "
                    f"evaluated={vt['evaluated']} "
                    f"success_rate={_fmt_opt(vt['success_rate'])} "
                    f"avg_runup_atr={_fmt_opt(vt['avg_runup_atr'])} "
                    f"avg_drawdown_atr={_fmt_opt(vt['avg_drawdown_atr'])}"
                )
            else:
                md_lines.append("- Follow-through: немає snapshot для цього символу")

    # Stage3
    md_lines.append("\n## Stage3")
    md_lines.append(f"Подій: {s3_total}")
    if s3_by_event:
        md_lines.append(
            "- Події за типом: "
            + ", ".join(f"{k}: {v}" for k, v in sorted(s3_by_event.items()))
        )
    rr = summary["stage3"]["rr_ratio"]
    if rr["count"]:
        md_lines.append(
            f"- RR ratio: n={rr['count']} avg={rr['avg']:.3f} min={rr['min']:.3f} max={rr['max']:.3f}"
        )

    # Деталізація для символу (останній короткий зріз фаз)
    if p.symbol and recent_phases:
        md_lines.append(f"\n## Symbol last phases: {p.symbol}")
        for ev in recent_phases:
            ts_iso = ev.get("ts")
            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}

            def gp(
                k: str, _ev: dict[str, Any] = ev, _payload: dict[str, Any] = payload
            ) -> Any:
                return _ev[k] if k in _ev else _payload.get(k)

            phase = str(gp("phase") or "").strip() or "unknown"
            score = gp("score")
            # directional/basic
            band_pct = gp("band_pct")
            atr_ratio = gp("atr_ratio")
            vol_z = gp("vol_z")
            dvr_v = gp("dvr")
            cd_v = gp("cd")
            # HTF
            htf_ok = gp("htf_ok")
            htf_strength = gp("htf_strength")
            # stage2_hint (якщо є в payload)
            hint = payload.get("stage2_hint") if isinstance(payload, dict) else None
            hint_dir = hint.get("dir") if isinstance(hint, dict) else None
            hint_score = hint.get("score") if isinstance(hint, dict) else None
            hint_dir_disp = hint_dir if isinstance(hint_dir, str) and hint_dir else "-"
            hint_score_disp = _fmt_opt(hint_score)
            md_lines.append(
                "- ts={} phase={} score={} band_pct={} atr_ratio={} vol_z={} dvr={} cd={} htf_ok={} htf_strength={} hint.dir={} hint.score={}".format(
                    ts_iso,
                    phase,
                    (
                        f"{float(score):.3f}"
                        if isinstance(score, (int, float))
                        else score
                    ),
                    (
                        f"{float(band_pct):.4f}"
                        if isinstance(band_pct, (int, float))
                        else band_pct
                    ),
                    (
                        f"{float(atr_ratio):.4f}"
                        if isinstance(atr_ratio, (int, float))
                        else atr_ratio
                    ),
                    (
                        f"{float(vol_z):.3f}"
                        if isinstance(vol_z, (int, float))
                        else vol_z
                    ),
                    (
                        f"{float(dvr_v):.3f}"
                        if isinstance(dvr_v, (int, float))
                        else dvr_v
                    ),
                    (f"{float(cd_v):.3f}" if isinstance(cd_v, (int, float)) else cd_v),
                    htf_ok,
                    (
                        f"{float(htf_strength):.3f}"
                        if isinstance(htf_strength, (int, float))
                        else htf_strength
                    ),
                    hint_dir_disp,
                    hint_score_disp,
                )
            )

    # Якщо обрано конкретний символ — додати компактний розділ останніх фаз
    if p.symbol and symbol_last_phases:
        # відсортувати за часом і взяти останні 5
        def _ts_to_epoch(row: dict[str, Any]) -> float:
            return float(_parse_iso_or_epoch(row.get("ts")) or 0.0)

        last = sorted(symbol_last_phases, key=_ts_to_epoch, reverse=True)[:5]
        md_lines.append(
            f"\n## Symbol last phases: {p.symbol.upper()} (last {len(last)})"
        )
        for it in last:
            ts_show = str(it.get("ts") or "?")
            phase_show = str(it.get("phase") or "?")
            score_show = it.get("score")
            hint = it.get("stage2_hint") or {}
            hint_dir = hint.get("dir")
            hint_score = hint.get("score")
            # Рядок 1: ts/phase/score
            if isinstance(score_show, (int, float)):
                md_lines.append(
                    f"- ts={ts_show} phase={phase_show} score={score_show:.3f}"
                )
            else:
                md_lines.append(f"- ts={ts_show} phase={phase_show} score=—")
            # Рядок 2: метрики + hint
            if isinstance(hint_score, (int, float)):
                md_lines.append(
                    f"  band_pct={it.get('band_pct')} atr_ratio={it.get('atr_ratio')} vol_z={it.get('vol_z')} "
                    f"dvr={it.get('dvr')} cd={it.get('cd')} htf_ok={it.get('htf_ok')} htf_strength={it.get('htf_strength')} "
                    f"| hint={(hint_dir or '-')} / {hint_score:.3f}"
                )
            else:
                md_lines.append(
                    f"  band_pct={it.get('band_pct')} atr_ratio={it.get('atr_ratio')} vol_z={it.get('vol_z')} "
                    f"dvr={it.get('dvr')} cd={it.get('cd')} htf_ok={it.get('htf_ok')} htf_strength={it.get('htf_strength')} "
                    f"| hint={(hint_dir or '-')} / -"
                )

    md = "\n".join(md_lines) + "\n"
    return summary, md


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Аналіз телеметрії Stage1/Stage3 (JSONL)")
    p.add_argument(
        "--dir",
        dest="dir",
        default=None,
        help="Директорія з JSONL логами (за замовчуванням із config)",
    )
    p.add_argument(
        "--symbol",
        dest="symbol",
        default=None,
        help="Фільтр за символом (наприклад, BTCUSDT); додає детальний розділ",
    )
    p.add_argument(
        "--since",
        dest="since",
        default=None,
        help="Початкова межа часу (ISO або epoch сек/мс)",
    )
    p.add_argument(
        "--until",
        dest="until",
        default=None,
        help="Кінцева межа часу (ISO або epoch сек/мс)",
    )
    p.add_argument(
        "--verify",
        dest="verify_nbars",
        type=int,
        default=0,
        help="Перевірка follow-through: кількість наступних барів для оцінки (0 = вимкнено)",
    )
    p.add_argument(
        "--max-alerts",
        dest="max_alerts_list",
        type=int,
        default=20,
        help="Максимум останніх ALERT у детальному розділі символа",
    )
    p.add_argument(
        "--out",
        dest="out_md",
        default=None,
        help="Куди зберегти Markdown звіт (за замовчуванням stdout)",
    )
    p.add_argument(
        "--json",
        dest="out_json",
        default=None,
        help="Куди зберегти JSON підсумок (опційно)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    cli = _build_cli()
    args = cli.parse_args(argv)

    inp = TelemetryInput.from_config()
    if args.dir:
        inp.base_dir = Path(args.dir)

    params = AnalyzeParams(
        since=_parse_iso_or_epoch(args.since),
        until=_parse_iso_or_epoch(args.until),
        symbol=(args.symbol.upper() if args.symbol else None),
        verify_nbars=int(args.verify_nbars or 0),
        max_alerts_list=int(args.max_alerts_list or 20),
    )

    summary, md = analyze_directory(inp, params)

    # Вивід
    if args.out_md:
        Path(args.out_md).write_text(md, encoding="utf-8")
    else:
        sys.stdout.write(md)

    if args.out_json:
        Path(args.out_json).write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
