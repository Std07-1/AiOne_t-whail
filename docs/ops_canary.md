# Канарейка: швидкий запуск HTF/Whale потоків (Windows)

Мета: дати Stage2 реальний HTF‑контекст і whale‑метрики, щоб UI перестав повертати `accum_monitor | SCN=none`.

## Кроки запуску

1) Запустити два процеси у двох окремих консолях PowerShell:

```bat
venv\Scripts\python.exe -m tools.htf_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --htf M5,M15 --publish-redis
venv\Scripts\python.exe -m tools.whale_publisher --symbols BTCUSDT,TONUSDT,SNXUSDT --cadence-ms 3000
```

2) Запустити основний пайплайн як зараз (UI/publisher/processor).

3) Перевірки (Windows, без rg):

```bat
rem HTF ключі
redis-cli KEYS ai_one:bars:*:M5

rem HTF TTL (> 0)
redis-cli TTL ai_one:bars:BTCUSDT:M5

rem Whale cadence
redis-cli GET ai_one:metrics:whale:last_publish_ms

rem State (hash): витягнути у файл і перевірити ключові поля
redis-cli HGETALL ai_one:state:BTCUSDT > state.json
findstr "htf:strength htf_strength htf_stale_ms whale scenario_detected scenario_confidence" state.json

rem Метрики (якщо ввімкнено Prometheus у config)
curl -s http://localhost:9108/metrics > metrics.txt
findstr ai_one_scenario metrics.txt
findstr htf_strength metrics.txt
```

## Очікуваний результат

- `TTL ai_one:bars:BTCUSDT:M5` > 0 (ключ оновлюється).
- `ai_one:metrics:whale:last_publish_ms` оновлюється кожні ≤ 5 с.
- У UI зʼявляються `SCN=<...>` коли ринок дає умови, фаза виходить з `accum_monitor`.
- На `/metrics` видно числові ряди `ai_one_scenario{...}` і `htf_strength{...}` (якщо увімкнено в `config`).
