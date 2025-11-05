"""
Збір ранкового підсумку: p95 латентності Stage1 із Prometheus‑знімків та опційно
агрегація якості/forward. Виводить короткий Markdown‑звіт із авто‑GO/NO‑GO.

Використання (приклад):
    python -m tools.night_post --metrics reports/metrics_night.txt --out reports/night_summary.md --quality reports/quality_night.csv

Поля/логіка:
    - p95 та mean із гістограми ai_one_stage1_latency_ms (label‑free)
    - switch_rate (профільні перемикання/хв) із ai_one_profile_switch_total (приблизно за всією тривалістю файлу)
    - false_breakout_total — сума останнього знімка ai_one_false_breakout_total
    - ExpCov, chop_confirm_rate — якщо наявні у quality CSV
    - Вердикт: GO, WARN, NO‑GO

Умови вердикту (T0):
    • GO якщо p95 ≤ 200, switch_rate ≤ 2/хв, ExpCov ≥ 0.60
    • Інакше WARN, якщо відхилення помірне; суттєві відхилення → NO‑GO

Безпечний: працює з частково пошкодженими файлами, пропускає помилки.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
from dataclasses import dataclass

HIST_NAME = "ai_one_stage1_latency_ms"


@dataclass
class HistStats:
    count: int
    total: float
    buckets: dict[float, int]  # cumulative per le

    @property
    def mean(self) -> float:
        try:
            return (self.total / self.count) if self.count > 0 else 0.0
        except Exception:
            return 0.0

    def p_quantile(self, q: float) -> float | None:
        if not self.buckets or self.count <= 0:
            return None
        try:
            threshold = float(self.count) * float(q)
        except Exception:
            threshold = float(self.count) * 0.95
        best = None
        for le in sorted([k for k in self.buckets.keys() if math.isfinite(k)]):
            v = self.buckets.get(le, 0)
            if v >= threshold:
                best = le
                break
        return best


def parse_histogram_from_text(text: str, metric: str = HIST_NAME) -> HistStats | None:
    # Збираємо останні значення: у форматі Prometheus текст експозиції останні рядки
    # вже містять найсвіжіші cumulative значення
    bucket_re = re.compile(
        rf'^{re.escape(metric)}_bucket\{{le="([^"]+)"\}}\s+(\d+)', re.M
    )
    sum_re = re.compile(rf"^{re.escape(metric)}_sum\s+([-+]?[0-9]*\.?[0-9]+)", re.M)
    cnt_re = re.compile(rf"^{re.escape(metric)}_count\s+(\d+)", re.M)
    buckets: dict[float, int] = {}
    total = None
    count = None
    for line in text.splitlines():
        m = bucket_re.match(line.strip())
        if m:
            le_s, val_s = m.group(1), m.group(2)
            try:
                if le_s == "+Inf":
                    le = float("inf")
                else:
                    le = float(le_s)
                buckets[le] = int(val_s)
            except Exception:
                continue
            continue
        m = sum_re.match(line.strip())
        if m:
            try:
                total = float(m.group(1))
            except Exception:
                total = None
            continue
        m = cnt_re.match(line.strip())
        if m:
            try:
                count = int(m.group(1))
            except Exception:
                count = None
            continue
    if count is None or total is None or not buckets:
        return None
    return HistStats(count=count, total=total, buckets=buckets)


def _split_snapshots(text: str) -> list[str]:
    """Грубе розбиття concat‑файла метрик на знімки за маркером HIST_NAME.

    Повертає список блоків (можуть перетинатися за HELP/TYPE), але цього достатньо
    для відбору першого/останнього.
    """
    marker = f"# TYPE {HIST_NAME} histogram"
    parts = text.split(marker)
    # parts[0] — шапка перед першим; знімки — це наступні шматки з доданим маркером
    snaps: list[str] = []
    for i in range(1, len(parts)):
        snaps.append(marker + parts[i])
    return snaps


def _sum_counter(text: str, metric: str) -> int:
    """Сумує значення counter‑метрики (всі лейбл‑серії) у тексті одного знімка."""
    pat = re.compile(rf"^{re.escape(metric)}\{{[^}}]*\}}\s+([0-9eE+\-.]+)$", re.M)
    s = 0
    for m in pat.finditer(text):
        try:
            v = float(m.group(1))
            s += int(v)
        except Exception:
            continue
    return s


def _compute_switch_rate_per_min(all_text: str, scrape_sec: int = 15) -> float:
    snaps = _split_snapshots(all_text)
    if len(snaps) < 2:
        return 0.0
    first = snaps[0]
    last = snaps[-1]
    c0 = _sum_counter(first, "ai_one_profile_switch_total")
    c1 = _sum_counter(last, "ai_one_profile_switch_total")
    if c1 < c0:
        return 0.0
    # Тривалість ~ (N-1) * scrape_sec
    mins = max(1.0, float((len(snaps) - 1) * scrape_sec) / 60.0)
    return float(c1 - c0) / mins


def read_text(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except Exception:
        try:
            with open(path, encoding="cp1251") as f:
                return f.read()
        except Exception:
            return ""


def parse_quality_csv(path: str | None) -> dict[str, float] | None:
    if not path or not os.path.exists(path):
        return None
    out: dict[str, float] = {}
    try:
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            # беремо перший рядок агрегованого підсумку, якщо є
            for row in r:
                for k, v in row.items():
                    try:
                        out[str(k)] = float(v)  # type: ignore[arg-type]
                    except Exception:
                        continue
                break
    except Exception:
        return None
    return out if out else None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Нічний підсумок (p95 Stage1 latency, авто‑GO/NO‑GO)"
    )
    ap.add_argument("--metrics", required=True, help="Шлях до знімка Prometheus")
    ap.add_argument("--out", required=True, help="Markdown звіт для запису")
    ap.add_argument("--quality", help="Опційно: CSV якості для вітринних метрик")
    ap.add_argument(
        "--scrape-interval-sec",
        type=int,
        default=15,
        help="Оцінка інтервалу скрейпу /metrics (сек), для switch_rate",
    )
    args = ap.parse_args()

    text = read_text(args.metrics)
    hs = parse_histogram_from_text(text, HIST_NAME)
    p95 = hs.p_quantile(0.95) if hs else None
    mean = hs.mean if hs else None
    switch_rate = _compute_switch_rate_per_min(
        text, scrape_sec=int(args.scrape_interval_sec or 15)
    )
    # false_breakout_total — беремо з останнього знімка
    snaps = _split_snapshots(text)
    fb_total = _sum_counter(snaps[-1], "ai_one_false_breakout_total") if snaps else 0

    verdict = "UNKNOWN"
    recs: list[str] = []
    quality = parse_quality_csv(args.quality) if args.quality else None
    exp_cov = None
    chop_rate = None
    if quality:
        # Очікувані ключі: explain_coverage_rate, chop_confirm_rate
        exp_cov = quality.get("explain_coverage_rate") or quality.get("ExpCov")
        chop_rate = quality.get("chop_confirm_rate")
    # Вердикт за умовами T0
    cond_p95 = (p95 is not None) and (p95 <= 200.0)
    cond_sw = (switch_rate is not None) and (switch_rate <= 2.0)
    cond_cov = (exp_cov is not None) and (float(exp_cov) >= 0.60)
    if cond_p95 and cond_sw and cond_cov:
        verdict = "GO"
    else:
        verdict = "WARN"
        if not cond_p95:
            verdict = "NO-GO" if (p95 or 1e9) > 260.0 else verdict
            recs.append(
                "Latency↑: профілювати вузькі місця Stage1; знизити explain cadence"
            )
        if not cond_sw:
            recs.append(
                "Switch‑rate↑: посилити гістерезис профілів або пороги select_profile"
            )
        if not cond_cov:
            recs.append(
                "ExpCov<0.60: підвищити покриття explain або розслабити gray‑gate для контексту"
            )

    lines: list[str] = []
    lines.append("# Нічний підсумок")
    lines.append("")
    lines.append("## Stage1 latency")
    if hs:
        lines.append(f"- p95: {p95:.0f} ms")
        lines.append(f"- mean: {mean:.1f} ms")
        lines.append(f"- count: {hs.count}")
    else:
        lines.append("- Дані відсутні (не знайдено гістограму)")
    lines.append("")
    lines.append("## Операційні метрики")
    lines.append(f"- switch_rate: {switch_rate:.2f} /хв")
    lines.append(f"- false_breakout_total: {fb_total}")
    if exp_cov is not None:
        lines.append(f"- ExpCov: {float(exp_cov):.2f}")
    if chop_rate is not None:
        try:
            lines.append(f"- chop_confirm_rate: {float(chop_rate):.2f}")
        except Exception:
            pass
    lines.append("")
    lines.append(f"## Вердикт: {verdict}")
    if recs:
        lines.append("")
        lines.append("### Рекомендації")
        for r in recs:
            lines.append(f"- {r}")
    lines.append("")
    if quality:
        lines.append("## Quality (з CSV)")
        for k in sorted(quality.keys()):
            try:
                v = quality[k]
                lines.append(f"- {k}: {v}")
            except Exception:
                continue
        lines.append("")

    try:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
    except Exception:
        pass
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
