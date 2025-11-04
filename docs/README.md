# AiOne_t — Документація

Центральний огляд для розробників: як улаштована система, як її запускати, тестувати і діагностувати.

## Швидкий старт

- Python 3.11+ і віртуальне середовище у `venv/`.
- Базові команди (Windows PowerShell):

```
# Запуск застосунку (локальний режим)
& "./venv/Scripts/python.exe" -m app.main

# Лінт + типи + тести
& "./venv/Scripts/python.exe" -m ruff check .
& "./venv/Scripts/python.exe" -m mypy .
& "./venv/Scripts/python.exe" -m pytest -q
```

## Архітектура
- Пайплайн Stage1 → Stage2 → Stage3 та телеметрія описані у `docs/ARCHITECTURE.md`.
- Контракти даних стабільні; нові поля — тільки у `market_context.meta.*`.

## Телеметрія й реплей
- Аналізатор телеметрії: `docs/TELEMETRY.md`.
- Історичний реплей і KPI: `docs/README_historical_replay.md`.

## Швидкий чек‑лист explain

- Прапори (config/config.py): `SCEN_EXPLAIN_ENABLED=True`, `SCEN_EXPLAIN_VERBOSE_EVERY_N=20`.
- Запустіть пайплайн через `tools.run_window` (див. `docs/OBSERVATION_MODE.md`).
- Перевірте explain у логах за останню годину:
	```powershell
	.\.venv\Scripts\python.exe -m tools.verify_explain --last-min 60 --logs-dir .\logs
	```
- Згенеруйте якісний CSV за 4 години та подивіться покриття/латентність explain:
	```powershell
	.\.venv\Scripts\python.exe -m tools.scenario_quality_report --last-hours 4 --out reports\quality.csv
	```
- (Опційно) Зробіть компактний дашборд‑табличку з CSV у Markdown:
	```powershell
	.\.venv\Scripts\python.exe -m tools.quality_dashboard --csv reports\quality.csv --out reports\quality_dashboard.md
	```
	Вміст: таблиця з колонками `Symbol | Scenario | Acts | MeanConf | P75 | ExpCov | ExpLat(ms) | Persist | InZone | BTCRegime`.

## UI та пейлоад
- Схема UI‑пейлоаду і інсайти: `docs/ui_payload_schema_v2.md`.

## Redis‑ключі
- Канонічні правила і приклади: `docs/REDIS_KEYS.md`.

## Фіче‑флаги
- Призначення і правила rollback: `docs/FEATURE_FLAGS.md`.
- Власне оголошення флагів: `config/flags.py`.

## Стиль/типи/внесок
- Правила стилю: `docs/STYLE_GUIDE.md`.
- Типізація і дорожня карта: `docs/TYPING_ROADMAP.md`.
- Напрями тюнінгу/осцилятори: `docs/tuning.md`.
- Як робити зміни: `docs/CONTRIBUTING.md` (див. Minimal‑Diff, тести, логи).

## Статус‑документи
- Directional gating: `docs/directional_gating_plan.md`.
- Whale v2: `docs/whale_detection_v2_status.md`.
- Crisis Mode: `docs/crisis_mode_status_2025-10-22.md`.