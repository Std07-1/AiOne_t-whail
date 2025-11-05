# Redis‑ключі та канали (канонічні правила)

Єдине джерело формування ключів/каналів — `config/keys.py`. Заборонено хардкодити рядки ключів у коді.

## Формат
- Базовий шаблон ключа: `ai_one:{domain}:{symbol}:{granularity}`
  - `domain`: `stats` | `levels` | `signal` | `ui` | `admin` | ін.
  - `symbol`: у нижньому регістрі (наприклад, `btcusdt`).
  - `granularity`: `1m`, `1d`, ін. (опційно, залежить від домену).

- Канали публікації: `ai_one:channel:{topic}:{subtopic}`
  - приклад: `ai_one:channel:ui:asset_state`.

## Приклади
- `build_key(NAMESPACE, "stats", symbol="btcusdt", granularity="1m")`
  → `ai_one:stats:btcusdt:1m`
- `build_key(NAMESPACE, "levels", symbol="ethusdt", granularity="1m")`
  → `ai_one:levels:ethusdt:1m`
- `build_channel(NAMESPACE, "ui", "asset_state")`
  → `ai_one:channel:ui:asset_state`

## TTL і політика кешу
- TTL беріть з `config/config.py`: `INTERVAL_TTL_MAP`, `REDIS_CACHE_TTL`.
- Не створюйте «вічних» ключів без причини. Для проміжних розрахунків ставте обґрунтований TTL.

## Правила сумісності
- Нові ключі/домени — лише через `config/keys.py`.
- При міграції існуючих ключів забезпечуйте backward‑сумісність на період розкатки (або чіткий план міграції).
- Поля HSET — канонічні і в нижньому snake_case (читачі можуть мати fallback, але сховище — канонічне).

## Live‑перевірка
- Валідуйте публікації через Redis CLI/GUI, формуючи ключі через `config/keys.py`.
- Перевіряйте TTL та наявність основних полів (`phase`, `vol_z`, `dvr`, `cd`, `htf_ok`).

## Додатково
- Див. також: `docs/ARCHITECTURE.md` (розділ про Redis), `config/README_Config_v2.md`.

## Runtime ctx‑ключі (best‑effort)

Деякі короткоживучі стани зберігаються у контекстних ключах із невеликим TTL, щоб пережити рестарти процесу без зміни контрактів:

- `ai_one:ctx:{symbol}:cooldown` — epoch seconds, TTL≈120 с
  - Призначення: персист кулдауна профільного хінта (Stage2‑lite) для символу.
  - Поведінка: write‑through і read‑back — best‑effort, помилки ігноруються (фолбек — ін‑меморі стан).
  - Сумісність: формат стабільний; `symbol` у нижньому регістрі.

Нагадування: Формувати ключі тільки через `config/keys.py`, а не хардкодом у коді. Цей ключ має вузький скоп, тож допускається локальне формування рядка у гарячому шляху, але бажано централізувати при першій нагоді.
