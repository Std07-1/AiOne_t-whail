# Внесок у AiOne_t (CONTRIBUTING)

## Політика PR
- Minimal‑Diff: бажано ≤ ~30 LOC, 1–2 файли. Великі зміни — ділити на кроки.
- Не змінюйте контракти Stage1/Stage2/Stage3. Нові поля — лише у `market_context.meta.*` або `confidence_metrics`.
- Redis‑ключі — тільки через `config/keys.py`; формат `ai_one:{domain}:{symbol}:{granularity}`; TTL з `config/config.py`.
- Логи інформативні, без секретів; маркери `[STRICT_*]` для ключових шляхів.

## Якість (локально)

```
& "./venv/Scripts/python.exe" -m ruff check .
& "./venv/Scripts/python.exe" -m mypy .
& "./venv/Scripts/python.exe" -m pytest -q
```

- Після зміни runnable‑коду проганяйте хоча б smoke‑тести.
- Для Stage1/Stage2 — псевдо‑стрім/реплей із `tools/replay_stream.py` (див. `docs/README_historical_replay.md`).

## Типізація та стиль
- Повна PEP‑484 типізація публічних API; докстрінги українською (Args/Returns/Raises).
- Стиль — `docs/STYLE_GUIDE.md`.

## Фіче‑флаги
- Оголошуйте у `config/flags.py` з коротким описом; вплив має вимикатися повністю при `False` (rollback за 1 клік).
- Див. `docs/FEATURE_FLAGS.md`.

## Тести
- Мінімум: happy‑path + 1 edge. Для потокових сценаріїв — псевдо‑стрім.
- При зміні суперважливих шляхів — додайте крихітні регресійні тести.

## Документація
- Якщо публічна поведінка змінилася — оновіть відповідні `docs/*.md` і/або README.
- Додавайте приклади запусків (Windows PowerShell) у релевантні документи.
