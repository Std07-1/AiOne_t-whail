"""Stage3 пайплайн AiOne_t (жовтень 2025).

Потік цін (price shadow): `price_stream_service` формує Redis Stream з актуальних
котирувань Stage1/Stage2, а `price_stream_shadow_consumer` через XREADGROUP
ре-клеймить застряглі повідомлення, підтримує буфер і надає pending-метрики.
Ці показники використовує `Stage3HealthOrchestrator`, аби виявляти затори та
формувати загальний рівень завантаженості.

Оновлення угод: `trade_update_service` читає shadow-дані й викликає
`TradeLifecycleManager` для trailing-логіки, ручних виходів та таймаутів. Сам
орchestrator зберігає останній health snapshot прямо в
`trade_manager.health_snapshot`, тому оновлення не вимагають додаткових звернень
до Redis.

Менеджмент відкриття: `open_trades` отримує Stage2-сигнали, застосовує elite
метадані, debounce та ATR/HTF guard. Health-контроль базується на
`STAGE3_HEALTH_LOCK_CFG` (жорстке блокування відкриттів при критичному pending) і
`STAGE3_HEALTH_CONFIDENCE_DAMPING` (демпфування впевненості та зменшення
`max_parallel_trades`). Схвалені угоди додатково публікуються у shadow stream із
телеметрією (`log_stage3_event`).

Управління життєвим циклом: `TradeLifecycleManager` інкапсулює процес відкриття,
trailing-stop, ранні виходи, таймаути й збір статистики. Політика береться з
`config.config`: глобальні параметри й overrides на символ. Збережений health
snapshot доступний як сервісам Stage3, так і UI.

Health і телеметрія: `health/health_orchestrator` обраховує busy_level за
метриками потоку та угод, кешує snapshot у Redis (`core` документ) і шле події
`stage3_health`. UI (`publish_full_state`) використовує цей snapshot для блоку
Stage3, моніторинг – для алертів.

Конвенції: усі пороги/TTL/ключі беруться з `config.config`; докстрінги і логи –
українською та без чутливих даних; модульні тести `test/test_stage3_*.py` покривають
happy-path, edge-кейси й health-гейти, для потокових сценаріїв створюємо
псевдо-стрім тести.
"""
