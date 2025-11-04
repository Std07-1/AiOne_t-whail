# Історичний реплей Stage1→Stage2

Офлайн інструмент для відтворення пайплайна на історичних snapshot‑ах і побудови KPI/інсайтів.

## Навіщо
- Валідація фаз/індикаторів без підключення до біржі/Redis.
- Порівняння параметрів/фіче‑флагів перед продакшен‑ввімкненням.
- Генерація артефактів для аналізу: JSONL/CSV/KPI/інсайти.

## Вхідні дані
- Snapshotи барів у `datastore/*_bars_1m_snapshot.jsonl` (є для топових символів). Кожен рядок: `{open_time, open, high, low, close, volume, ...}`.

## Вихідні артефакти
- `telemetry/stage1_signals.jsonl` — події Stage1.
- `telemetry/stage2_outputs.jsonl` — агрегований вихід Stage2‑lite.
- `telemetry/replay_insights_*.jsonl` — компактні інсайти для швидкого перегляду.
- `telemetry/summary.csv` — табличний підсумок по барах.
- `telemetry/kpi.json` — підсумкові лічильники фаз, середні оцінки, частки.

## Запуск (Windows PowerShell)

Приклади команд у кореневій директорії репозиторію:

```
# 600 барів BTCUSDT (1m), джерело snapshot, збереження артефактів у telemetry/
& "./venv/Scripts/python.exe" -m tools.replay_stream BTCUSDT --interval 1m --source snapshot --limit 600

# Таргетоване вікно з прогрівом (часи в UTC ISO8601)
& "./venv/Scripts/python.exe" -m tools.replay_stream BTCUSDT --interval 1m --source snapshot --start 2025-10-26T02:30:00Z --end 2025-10-26T05:30:00Z --limit 220
```

Параметри (основні):
- `--interval 1m` — інтервал барів.
- `--source snapshot` — джерело. Для офлайн режиму використовує snapshotи з `datastore/`.
- `--limit N` — кількість барів для обробки (з урахуванням прогріву).
- `--start/--end` — межі часу у ISO8601Z або epoch (опц.).
- `--dump-dir PATH` — альтернативна директорія виводу замість `telemetry/` (опц.).

## KPI і перевірки
- Лічильники фаз: `momentum`, `false_breakout`, `exhaustion`, `drift_trend` (за флагом), `none`.
- Середні `phase_score` по кожній фазі.
- Частка HTF‑підтверджень (ok_rate), середні `htf_score/strength`.
- Whale‑метрики: `presence`, `bias`, `vwap_dev`, `age_ms`, частка `stale`.

KPI зберігаються у `telemetry/kpi.json` та відображаються у логах запуску.

## Інсайти реплею
Compact JSONL (`telemetry/replay_insights_*.jsonl`) для швидкого огляду останніх подій:
- Час/символ/інтервал (`ts`, `symbol`, `interval`).
- Directional поля: `band_pct`, `near_edge`, `atr_ratio`, `vol_z`, `dvr`, `cd`, `slope_atr`.
- HTF: `htf_ok`, `htf_strength`.
- Whale: `presence`, `bias`, `vwap_dev`, `age_ms`, `vol_regime`, `stale`.
- Теги (`tags`) і причини (`reasons`) з фазного детектора.

## Примітки
- Фазний детектор додає «тихий тренд» (`drift_trend`) лише під фіче‑флагом `FEATURE_PARTICIPATION_LIGHT` (телеметрія/UI, контракти незмінні).
- Прогрів: для коректних метрик ATR/HTF необхідний «теплий старт» (декілька десятків барів).
- Запуск на кількох символах робиться окремими інстансами команд.

## Далі читати
- Архітектура: `docs/ARCHITECTURE.md`
- Телеметрія/аналіз: `docs/TELEMETRY.md`
- Фіче‑флаги: `docs/FEATURE_FLAGS.md`
