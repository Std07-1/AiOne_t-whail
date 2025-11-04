# Зміни та оновлення (спостереження / explain) — 2025-11-02

Цей файл підсумовує останні зміни, пов’язані з прозорістю селектора, explain‑логами та якісними звітами. Контракти Stage1/Stage2/Stage3 не змінювалися; усі нові дані — у `market_context.meta.*`, логи та Prometheus.

## Нове

- SCEN_EXPLAIN логування з рейт‑лімітом і "тонким" слідом:
  - Прапори в `config/config.py`: `SCEN_EXPLAIN_ENABLED=True`, `SCEN_EXPLAIN_VERBOSE_EVERY_N=20`.
  - Хелпер `_explain_should_log` і пер-символьний лічильник батчів у `app/process_asset_batch.py`.
  - Формат лишився сталим: `[SCEN_EXPLAIN] symbol=... scenario=... explain="..."`.
- EvidenceBus: збір ключових "доказів" рішення селектора з підсумком у `market_context.meta.scenario_explain`.
- Якісний звіт (tools/scenario_quality_report.py):
  - Додано `explain_coverage_rate` (частка активацій із explain у ±30 с).
  - Додано `explain_latency_ms` (середній час від першого TRACE кандидата до найближчого explain).
  - Збережені раніше колонки: `in_zone`, `compression_level`, `persist_score`, `btc_regime_at_activation`, `quality_proxy`, `reject_top{1..3}`, `reject_top{1..3}_rate`.
- Перевірка explain (tools/verify_explain.py):
  - Параметри: `--last-min 60`, `--logs-dir ./logs`.
  - Вивід: `explain_lines_total`, `symbols_seen`, `coverage_by_symbol`, `recent_examples` (останні 3 explain‑рядки).

## Документація

- `docs/OBSERVATION_MODE.md` — додано прапори SCEN_EXPLAIN, колонки coverage/latency у `quality.csv`, приклад запуску `tools.verify_explain`.
- `docs/TELEMETRY.md` — описано керування explain‑логами (рейт‑ліміт + кожен N‑й батч), нові метрики у `quality.csv` і утиліту верифікації.

## Примітки

- Єдиний /metrics експортер — у пайплайні; паблішери не стартують HTTP.
- Продуктивність — без регресії; логи explain рейт‑обмежені та безпечні.
- Всі зміни підлягають вимкненню прапорами (rollback‑friendly).
