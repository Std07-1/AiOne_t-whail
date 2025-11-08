"""
Tools: reports_tidy

Purpose:
- Розкладає вміст каталогу `reports/` за категоріями:
  * проміжні результати → reports/replay/<run_name>/
  * фінальні звіти → reports/summary/<run_name>/
- Обережно обходить активні ран-и (не рухає директорії, де файли оновлювались нещодавно).

Usage:
  python -m tools.reports_tidy --dry-run
  python -m tools.reports_tidy --apply

Політика класифікації:
- "Фінальні" файли: *.md, що містять summary або починаються на forward_*, також README.md, quality_snapshot.md.
- "Проміжні": run.log, metrics.txt, *.csv (наприклад live_metrics*.csv, quality.csv), підкаталог artifacts/.
- Структура зберігається по підкаталогах ран-ів (наприклад prof_canary6).

Захист активних ран-ів:
- Якщо у директорії є будь-який файл зі свіжою модифікацією (молодше N хвилин), директорія пропускається.
  N за замовчуванням 10 хв (налаштовується через --active-min).
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
REPLAY_DIR = REPORTS_DIR / "replay"
SUMMARY_DIR = REPORTS_DIR / "summary"


FINAL_MD_PREFIXES = ("forward_", "summary")
FINAL_MD_EXACT = {"README.md", "quality_snapshot.md"}


@dataclass
class MovePlan:
    src: Path
    dst: Path


def is_run_dir(p: Path) -> bool:
    """Heuristic: директорія ран-а містить run.log або summary.md."""
    if not p.is_dir():
        return False
    names = {c.name for c in p.iterdir()}
    return ("run.log" in names) or ("summary.md" in names)


def is_active_dir(p: Path, active_seconds: int) -> bool:
    """Вважаємо директорію активною, якщо будь-який файл змінено менше ніж active_seconds тому."""
    now = time.time()
    for root, _, files in os.walk(p):
        for f in files:
            try:
                mtime = os.path.getmtime(os.path.join(root, f))
            except OSError:
                continue
            if (now - mtime) < active_seconds:
                return True
    return False


def classify_file(path: Path) -> str:
    """Повертає 'final' або 'intermediate' або 'skip'."""
    name = path.name
    if path.is_dir():
        if name == "artifacts":
            return "intermediate"
        # інші директорії всередині ран-а ігноруємо (будемо обробляти файлами)
        return "skip"

    lower = name.lower()
    if lower.endswith(".md"):
        if lower in {n.lower() for n in FINAL_MD_EXACT}:
            return "final"
        if any(lower.startswith(pref) for pref in FINAL_MD_PREFIXES):
            return "final"
        # інші .md за замовчуванням теж вважаємо фінальними звітами
        return "final"

    if lower.endswith(".csv"):
        return "intermediate"
    if lower in {"run.log", "metrics.txt"}:
        return "intermediate"

    return "skip"


def plan_moves(
    run_dir: Path, active_seconds: int
) -> tuple[list[MovePlan], list[MovePlan]]:
    finals: list[MovePlan] = []
    inters: list[MovePlan] = []

    if is_active_dir(run_dir, active_seconds):
        return finals, inters  # нічого не рухаємо для активних

    for child in sorted(run_dir.iterdir()):
        category = classify_file(child)
        if category == "final":
            dst = SUMMARY_DIR / run_dir.name / child.name
            finals.append(MovePlan(child, dst))
        elif category == "intermediate":
            dst = (
                REPLAY_DIR
                / run_dir.name
                / (child.name if child.is_file() else child.name)
            )
            inters.append(MovePlan(child, dst))
        # skip — нічого не робимо

    return finals, inters


def ensure_parents(plans: Iterable[MovePlan]) -> None:
    for plan in plans:
        plan.dst.parent.mkdir(parents=True, exist_ok=True)


def execute_moves(plans: Iterable[MovePlan]) -> None:
    for plan in plans:
        if plan.src.is_dir():
            # переміщуємо всю директорію (наприклад artifacts)
            if plan.dst.exists():
                shutil.rmtree(plan.dst)
            try:
                shutil.move(str(plan.src), str(plan.dst))
            except PermissionError as e:
                # На Windows файл/каталог може бути ще відкритий; пропускаємо з попередженням
                print(
                    f"[reports_tidy] ⚠️  Пропуск директорії через блокування: {plan.src} -> {plan.dst} ({e})"
                )
                continue
            except OSError as e:
                print(
                    f"[reports_tidy] ⚠️  Не вдалося перемістити директорію: {plan.src} -> {plan.dst} ({e})"
                )
                continue
        else:
            try:
                shutil.move(str(plan.src), str(plan.dst))
            except PermissionError as e:
                # Швидкий одноразовий повтор через 0.2с, далі — пропуск
                try:
                    time.sleep(0.2)
                    shutil.move(str(plan.src), str(plan.dst))
                except Exception:
                    print(
                        f"[reports_tidy] ⚠️  Пропуск файлу через блокування: {plan.src} -> {plan.dst} ({e})"
                    )
                    continue
            except OSError as e:
                print(
                    f"[reports_tidy] ⚠️  Не вдалося перемістити файл: {plan.src} -> {plan.dst} ({e})"
                )
                continue


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Виконати переміщення (інакше лише dry-run)",
    )
    ap.add_argument(
        "--active-min",
        type=int,
        default=10,
        help="Вік файлів (хв), молодші вважаємо активними і не чіпаємо",
    )
    args = ap.parse_args(argv)

    REPORTS_DIR.mkdir(exist_ok=True)
    REPLAY_DIR.mkdir(exist_ok=True)
    SUMMARY_DIR.mkdir(exist_ok=True)

    active_seconds = args.active_min * 60
    runs = [p for p in REPORTS_DIR.iterdir() if is_run_dir(p)]

    total_final: list[MovePlan] = []
    total_inter: list[MovePlan] = []

    for run in sorted(runs):
        finals, inters = plan_moves(run, active_seconds)
        total_final.extend(finals)
        total_inter.extend(inters)

    if not total_final and not total_inter:
        print(
            "[reports_tidy] Немає файлів для переміщення (можливо всі ран-и активні або вже чисто)."
        )
        return 0

    print(
        f"[reports_tidy] План фінальних: {len(total_final)}, проміжних: {len(total_inter)}"
    )
    for plan in total_final:
        print(f"  FINAL: {plan.src} -> {plan.dst}")
    for plan in total_inter:
        print(f"  INTER: {plan.src} -> {plan.dst}")

    if args.apply:
        ensure_parents([*total_final, *total_inter])
        execute_moves([*total_final, *total_inter])
        print("[reports_tidy] Переміщення виконано.")
    else:
        print("[reports_tidy] Dry-run. Додайте --apply для застосування.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
