# Структура репортів та політика дедуплікації

## Мета
Спрощення навігації та зменшення шуму у каталозі `reports/` шляхом автоматичного розкладання артефактів на фінальні та проміжні. Уникнення дублювання forward‑подій між профілями.

## Категорії
- `reports/summary/<run_name>/` — фінальні звіти для архівування та читання:
  - `summary_*.md`, `summary.md`
  - `forward_strong.md`, `forward_soft.md`, `forward_explain.md`, `forward_<mode>.md`
  - `quality_snapshot.md`, `README.md`
- `reports/replay/<run_name>/` — проміжні та сировина для реконструкції:
  - `run.log`, `metrics.txt`, `quality.csv`, `live_metrics*.csv`
  - `artifacts/` (у т.ч. dedup ключі, дампи реплею)

## Дедуплікація forward подій між профілями
Під час постпроцесингу `unified_runner` передає у `tools.forward_from_log` параметр `--dedup-file artifacts/forward_dedup.keys`. Це файл з ключами вже опублікованих подій. Схема пріоритетів: `strong` → `soft` → `explain`. Подія, що потрапила у старший профіль, не зʼявиться у молодшому.

## Автоматичне впорядкування
Опція `--tidy-reports` для `tools.unified_runner` вмикає виклик `tools.reports_tidy --apply --active-min 0` наприкінці прогону. Поточний ран також переміщується одразу (active-min=0). Якщо прапор не задано — структура залишається як є.

Опція `--final-only` залишає тільки фінальні звіти: після `tidy` видаляється відповідний каталог у `reports/replay/<run_name>/` та початковий `out_dir` прогону. У підсумку файли доступні лише в `reports/summary/<run_name>/`.

### Ручний запуск
```
python -m tools.reports_tidy --apply
```
(Або без `--apply` для dry-run.)

## Обмеження та контракт
- Не змінює файл `run.log` під час виконання; лише переміщення після завершення.
- Не зачіпає Stage1/2/3 контракти; лише файлову організацію.
- Видалення каталогу `reports/` без збереження `reports/summary/` призведе до втрати історії — використовуйте tidy замість ручного видалення.

## KPI перевірки
- Відсутні дублікати forward‑подій між профілями (перевірка за кількістю `dedup_dropped` у footer).
- Кількість фінальних файлів у корені `reports/` → 0 після tidy (все переміщено).
- Новий прогін з `--tidy-reports` створює лише дві нові гілки під `summary/` та `replay/`.

## Приклад виклику
```
python -m tools.unified_runner live --duration 1800 \
  --namespace ai_one_demo \
  --prom-port 9108 \
  --report \
  --forward-profiles strong,soft,explain \
  --tidy-reports --final-only
```

## Rollback
Просто не використовувати `--tidy-reports`; існуючі переміщені каталоги лишаються валідними.
