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

## Forward profiles (офлайн)

Інструмент `tools.forward_from_log` будує «forward» оцінки за три профілі сигналів: `strong`, `soft`, `explain`.

- `source=whale` — бере останній `[STRICT_WHALE]` для символу поблизу часу активації сценарію і фільтрує за порогами `presence_min` та `|bias|>=bias_abs_min`. Додатково перевіряється «свіжість» китового запису за `whale_max_age_sec`.
- `source=explain` — бере останній `[SCEN_EXPLAIN]` (explain‑payload) для символу та фільтрує за тими ж порогами, але з перевіркою `explain_ttl_sec` (макс. вік explain‑рядка).

Дедуплікація: ключ `SYMBOL|ts_ms|(+|-)` (знак за `bias`). Ідентичні події в одному й тому ж барі вважаються дублями; «близнюки» з `ts±1` не зливаються.

Порогові значення:
- Жорсткі дефолти: `presence_min=0.75`, `bias_abs_min=0.60`, `whale_max_age_sec=600`, `explain_ttl_sec=600`.
- Фіче‑флаг м'яких порогів (офлайн‑only): `FORWARD_SOFT_THRESH_ENABLED=True` разом із профілем `FORWARD_SOFT_THRESH` застосує м'які значення для `whale`/`explain`, якщо CLI не передав явних параметрів. У футері додається `note=soft_thresholds`.

Великі логи: опція `--max-lines N` дозволяє ранньо зупинити парсинг надвеликих логів; у футері буде `note=early_stop`.

У підсумку `forward_*.md` містить:
- Шапку з параметрами фільтра, кількістю побачених/зіставлених подій.
- Строки `K=<bars>` із часткою «згоди знаку» на горизонтах.
- Футер із вікном часу, `N_total`, медіанами TTF (`ttf05_median`/`ttf10_median`) і явними `ttf_thresholds`.
