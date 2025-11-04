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
