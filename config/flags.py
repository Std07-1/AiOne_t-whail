"""Фіче-флаги системи (стабільні перемикачі)

Мета:
- Централізувати флаги, щоб спростити керування й rollback.
- Без бізнес-логіки; лише булеві/прості значення з докстрінгами.

Примітка:
- На етапі 1 значення синхронізовані з наявним config/config.py; config.py робитиме re-export.
"""

from __future__ import annotations

# ── Stage1 trigger control (QDE shutdown / structural prioritize-only) ─────
# Якщо True — вимикає додавання причин для «QDE»-тригерів у Stage1
# (RSI/дивергенції, VWAP-відхилення, сплеск волатильності).
# Тригери залишаються у телеметрії (context-only), але не впливають на ALERT.
STAGE1_QDE_TRIGGERS_DISABLED: bool = True

# Якщо True — структурні «near_*» тригери (near_high/near_low/near_daily_*)
# додаються лише як контекст (пріоритезація), але не рахуються до ALERT.
STAGE1_STRUCTURAL_PRIORITIZE_ONLY: bool = True

# Додаткові гейти важких детекторів у Stage1 (телеметрія‑first, контракти незмінні)
# Якщо True — пропускаємо обчислення дивергенцій RSI (використовуємо готове stats.rsi)
STAGE1_SKIP_DIVERGENCE_DETECTION: bool = True
# Якщо True — пропускаємо детектор сплеску волатильності (volatility_spike)
STAGE1_SKIP_VOLATILITY_SPIKE_DETECTION: bool = True

# Strict/Insight/Whale
STRICT_PROFILE_ENABLED: bool = True
# Увімкнути оцінювання китів у Stage1 (best-effort)
WHALE_SCORING_V2_ENABLED: bool = True
# Увімкнути провідку whale у процесорі сигналів (best-effort)
INSIGHT_LAYER_ENABLED: bool = True
# Увімкнути провідку insight у процесорі сигналів (best-effort)
PROCESSOR_INSIGHT_WIREUP: bool = True
# Дозволити використання vol_z_min proxy для етапу виснаження
VOLZ_PROXY_ALLOWED_FOR_EXHAUSTION: bool = True

# Якщо True — позначаємо stats.vol_z_source="proxy" (коли ввімкнено проксі‑агрегацію).
# За замовчуванням False, Stage1 виставляє "real".
VOLZ_SOURCE_PROXY_MODE: bool = True

# If True — skip the "full" (heavy) filtering stage in Stage1 and return
# results immediately after the base prefilter (quoteVolume/priceChange).
# Useful for running monitoring directly on post-basic-filter symbols.
STAGE1_SKIP_FULL_FILTER: bool = False

# Stage2-lite керування (телеметрія/алерти/гейти)
# STAGE2_ENABLED — головний фіче‑флаг для Stage2‑lite. При False: повністю вимикає
# будь‑який вплив і додаткову телеметрію з елементами Stage2 (rollback за 1 клік).
STAGE2_ENABLED: bool = True
# STAGE2_PROFILE — назва профілю порогів/поведінки Stage2‑lite (наприклад, "strict").
# Використовується для подальшої маршрутизації до YAML/налаштувань без зміни контрактів.
STAGE2_PROFILE: str = (
    "strict"  # можливі варіанти: "strict", "legacy", "legacy_aggressive", "legacy_conservative"
)

# STAGE2_WHALE_EMBED_ENABLED — окремий флаг для вбудовування китової телеметрії
# у Stage1.stats.whale (best‑effort). Вимкнення не впливає на роботу WhaleWorker/Redis.
STAGE2_WHALE_EMBED_ENABLED: bool = True

# STAGE2_HINT_ENABLED — флаг для формування "stage2_hint" у stats (телеметрія‑only):
# підказки long/short без впливу на Stage1 сигнал. За замовчуванням вимкнено.
STAGE2_HINT_ENABLED: bool = True

# STAGE2_SIGNAL_V2_ENABLED — флаг для формування "signal_v2" у stats (телеметрія‑only):
# невтручальний роутер на базі whale+phase/Directional метрик. Не змінює K_SIGNAL.
STAGE2_SIGNAL_V2_ENABLED: bool = True

# Кризовий хінт стратегії для short-exhaustion (телеметрія → Stage3 профіль)
EXH_STRATEGY_HINT_ENABLED: bool = True

# UI/Insight publish controls and telemetry mode
# Якщо True — публікуємо лише телеметрію/карти, без впливу на рекомендації
# Якщо False — публікуємо повний UI payload із рекомендаціями на базі Stage2.
INSIGHT_TELEMETRY_ONLY: bool = True
# Увімкнути додавання секцій whale/insight у UI payload (best‑effort)
UI_WHALE_PUBLISH_ENABLED: bool = True
UI_INSIGHT_PUBLISH_ENABLED: bool = True
# Частота публікацій whale/insight воркером (секунди)
WHALE_THROTTLE_SEC: int = 3
# Вмикає строгі лог‑маркери типу [STRICT_WHALE]
STRICT_LOG_MARKERS: bool = True

# "SOFT_" рекомендації з hint‑ів (лише UI, без зміни контрактів Stage1/Stage2)
# Якщо True — у UI на базі stage2_hint.{dir,score} може з'являтись "SOFT_BUY/SELL".
# Гейт: htf_ok=True та волатильність не "hyper"; поріг score керується нижче.
SOFT_RECOMMENDATIONS_ENABLED: bool = True
# Мінімальний score для формування SOFT_ рекомендації (0..1)
SOFT_RECO_SCORE_THR: float = 0.57

# UI: показувати бейджі на базі stats.signal_v2 (SOFT/ALERT) у колонках (без зміни K_SIGNAL)
UI_SIGNAL_V2_BADGES_ENABLED: bool = True

# Роутер: вимагати whale.dominance.* для ALERT_* (SOFT без змін)
ROUTER_ALERT_REQUIRE_DOMINANCE: bool = True

# Directional gating/reweighting
# Увімкнути додавання провідки directional у процесорі сигналів (best-effort)
FEATURE_DIRECTIONAL_GATING: bool = True
# Увімкнути провідку directional у процесорі сигналів (best-effort)
FEATURE_DIRECTIONAL_REWEIGHT: bool = (
    False  # менш агресивне перенесення ваги, ризик оверфіту
)
# Увімкнути гистерезис rapid_move у процесорі сигналів (best-effort)
# Більше чутливості до поточних імпульсів, менше шуму
# True для додаткової стабілізації при різких рухах.
FEATURE_RAPID_MOVE_HYSTERESIS: bool = True

# Narrative layer
STAGE2_NARRATIVE_ENABLED: bool = False

# Stage2 thresholds source (migration flag)
# Якщо True — STAGE2_PHASE_THRESHOLDS можуть бути завантажені з YAML
# (config/data/stage2_thresholds.yaml). За замовчуванням вимкнено для
# мінімального ризику; використовується лише у legacy-профілі.
FEATURE_STAGE2_THRESHOLDS_YAML: bool = True

# Обраний профіль YAML-порогів для legacy‑шляху.
# Доступні: "legacy", "legacy_aggressive", "legacy_conservative" (див. YAML).
STAGE2_THRESHOLDS_YAML_PROFILE: str = "legacy"

# UI publish оптимізації
# Публікувати в канал тільки при суттєвій зміні (ключові агрегати/дельти)
UI_SMART_PUBLISH_ENABLED: bool = True
# Мінімальний інтервал між публікаціями у канал (мс)
UI_PUBLISH_MIN_INTERVAL_MS: int = 250

# Динамічне тюнінгування семафорів I/O (Stage1 fetchers) — scaffolding флаг
DYNAMIC_SEMAPHORE_TUNING: bool = True

# Евристичний гейтинг важких обчислень у Stage1 (TRAP/phase) — телеметрія‑спочатку
# Коли True, у «спокійному» режимі (низька волатильність, далеко від краю, без сильних тригерів)
# важкі розрахунки можуть бути пропущені без впливу на базові тригери. Контракт Stage1Signal незмінний.
HEAVY_COMPUTE_GATING_ENABLED: bool = True

# Participation-light ("quiet mode") — класифікація у фазовому детекторі Stage2
# Якщо True, детектор може видавати м'яку фазу "drift_trend", коли присутні сигнали імпульсу,
# але бракує сильної участі у breakout. Лише для телеметрії; контракти Stage2Output не змінюються,
# побічних ефектів для gating немає.
FEATURE_PARTICIPATION_LIGHT: bool = True

# ── Stage1 per-trigger enable flags (centralised, overrideable in runtime) ──
# Дозволяють системно керувати окремими тригерами Stage1.
# Пріоритет: feature_switches["triggers"] у рантаймі > ці глобальні прапори.
STAGE1_TRIGGER_VOLUME_SPIKE_ENABLED: bool = False
STAGE1_TRIGGER_BREAKOUT_ENABLED: bool = False
STAGE1_TRIGGER_VOLATILITY_SPIKE_ENABLED: bool = False
STAGE1_TRIGGER_RSI_ENABLED: bool = False
STAGE1_TRIGGER_VWAP_DEVIATION_ENABLED: bool = False
STAGE1_EXCLUDE_LOW_VOL_ATR_TRIGGERS: bool = True

# Радикальний режим пре-фільтра: якщо True, пре-фільтр формується виключно на основі стану Redis
# (наприклад, UnifiedDataStore.get_fast_symbols / ручний seed / whitelist) та TRAP-перевірок.
# Це вимикає пороги пре-фільтрації на базі 24h ticker.
# УВАГА: у production Redis мають бути попередньо додані потрібні кандидати.
STAGE1_PREFILTER_REDIS_TRAP_ONLY: bool = False

# Перемикач строгого модуля пре-фільтра.
# Якщо True, Stage1 виконає строгий пре-фільтр після збору статистики по батчу
# та опублікує топ-k список у Redis (ключ: ai_one:prefilter:strict:list).
# За замовчуванням False, щоб уникнути впливу на поведінку без явного вмикання.
STAGE1_PREFILTER_STRICT_ENABLED: bool = False

# Профіль строгого пре-фільтра: "soft" знижує пороги участі/HTF і дає бонус за широкий діапазон.
# Rollback = False (повернення до жорстких порогів без змін коду).
STAGE1_PREFILTER_STRICT_SOFT_PROFILE: bool = True

# Strict prefilter: relaxed HTF gate control
# Якщо True — relaxed-умова може спрацьовувати і без htf_ok, якщо htf_strength ≥ порога
STRICT_PREFILTER_RELAXED_ALLOW_HTF_STRENGTH: bool = True
# Мінімальний поріг сили HTF для relaxed-шляху (коли htf_ok=False)
STRICT_PREFILTER_RELAXED_MIN_HTF_STRENGTH: float = 0.02

# Якщо True, основний моніторинг Stage1 обмежує обробку символами зі строгого пре‑фільтра
# (ключ у Redis: ai_one:prefilter:strict:list). Rollback = False.
STAGE1_MONITOR_LIMIT_TO_STRICT_PREFILTER: bool = True

# Якщо True — навіть порожній список зі строгого пре‑фільтра вважається авторитетним
# (моніторинг не повертається до «всіх активів»). Якщо False — порожній список ігнорується.
STAGE1_PREFILTER_STRICT_AUTHORITATIVE_IF_PRESENT: bool = True

# Фонове оновлення строгого пре‑фільтра: періодично запускає prefilter_symbols
# поверх наявних snapshotів Stage1, щоби підтримувати свіжий whitelist у Redis.
STAGE1_PREFILTER_STRICT_BG_REFRESH_ENABLED: bool = True
# Інтервал фонового оновлення (секунди); має бути меншим або близьким до TTL ключа (30с за замовч.).
STAGE1_PREFILTER_STRICT_BG_INTERVAL_SEC: int = 12

# Stage3 high-risk StrategyPack контролюється через окремий флаг (rollback-friendly)
PACK_EXHAUSTION_REVERSAL_ENABLED: bool = True
# Дозволити Stage3 обходити cooldown при сильному TRAP override (фолбек увімкнено)
TRAP_COOLDOWN_OVERRIDE_ENABLED: bool = True

# Вмикає детекцію великих ордерів (whale trades).
ENABLE_LARGE_ORDER_DETECTION = True  # Рекомендується вимкнути для тестування latency.

# Stage1 детектори (активні у продюсері)/ модуль whale аналізу

# Активує детектор пасток (trap detector) у стакані.
ENABLE_TRAP_DETECTOR = True  # Вимикайте, якщо багато false positive.

# Дозволяє запуск orderbook-детекторів (аналіз глибини ринку).
ENABLE_ORDERBOOK_DETECTORS = True  # Для оптимізації — вимкнути на low-liquidity.

# Вмикає детектор каскадних подій (cascade detector).
ENABLE_CASCADE_DETECTOR = True  # Корисно для виявлення різких рухів.

# Фільтр якості рівнів (level quality filter) для orderbook.
ENABLE_LEVEL_QUALITY_FILTER = True  # Вимикайте, якщо потрібна максимальна чутливість.

# Вмикає матрицю сценаріїв для прогнозування (predictive scenario matrix).
ENABLE_PREDICTIVE_SCENARIO_MATRIX = True  # Вимкнення — fallback до простих сценаріїв.

# Активує систему "шахового майстра" (chess master system) для складних стратегій.
ENABLE_CHESS_MASTER_SYSTEM = True  # Вимикайте для спрощеного режиму.

# Вмикає прогнозування поведінки китів (whale behavior predictor).
ENABLE_WHALE_BEHAVIOR_PREDICTOR = True  # Вимкнення — лише базова логіка.

# Менеджер реакцій на дії китів (whale response manager).
ENABLE_WHALE_RESPONSE_MANAGER = True  # Вимикайте для ручного контролю реакцій.

# Scenario trace логування (rate-limited) у проді
SCENARIO_TRACE_ENABLED: bool = True

# Гістерезис сценарію (мінімізувати миготіння)
STRICT_SCENARIO_HYSTERESIS_ENABLED: bool = True

# Канарейкове публікування сценарію‑кандидата (нестабільного):
# Якщо True — при наявності кандидата (resolve_scenario повертає scn_name, scn_conf),
# але гістерезис ще не підтвердив стабільність, ми все одно публікуємо
# scenario_detected/scenario_confidence у market_context.meta та Prometheus ai_one_scenario.
# Контракти Stage1/Stage2 не змінюються; ЗА ЗАМОВЧУВАННЯМ ВИМКНЕНО (канарейка завершена).
SCENARIO_CANARY_PUBLISH_CANDIDATE: bool = False

__all__ = [
    "STAGE1_QDE_TRIGGERS_DISABLED",
    "STAGE1_STRUCTURAL_PRIORITIZE_ONLY",
    "STAGE1_SKIP_DIVERGENCE_DETECTION",
    "STAGE1_SKIP_VOLATILITY_SPIKE_DETECTION",
    "STAGE1_EXCLUDE_LOW_VOL_ATR_TRIGGERS",
    "STRICT_PROFILE_ENABLED",
    "WHALE_SCORING_V2_ENABLED",
    "INSIGHT_LAYER_ENABLED",
    "PROCESSOR_INSIGHT_WIREUP",
    "VOLZ_PROXY_ALLOWED_FOR_EXHAUSTION",
    "VOLZ_SOURCE_PROXY_MODE",
    "STAGE2_ENABLED",
    "STAGE2_PROFILE",
    "STAGE2_WHALE_EMBED_ENABLED",
    "STAGE2_HINT_ENABLED",
    "STAGE2_SIGNAL_V2_ENABLED",
    "STAGE2_WHALE_EMBED_ENABLED",
    "STAGE2_HINT_ENABLED",
    "EXH_STRATEGY_HINT_ENABLED",
    "INSIGHT_TELEMETRY_ONLY",
    "UI_WHALE_PUBLISH_ENABLED",
    "UI_INSIGHT_PUBLISH_ENABLED",
    "WHALE_THROTTLE_SEC",
    "STRICT_LOG_MARKERS",
    "SOFT_RECOMMENDATIONS_ENABLED",
    "SOFT_RECO_SCORE_THR",
    "UI_SMART_PUBLISH_ENABLED",
    "UI_PUBLISH_MIN_INTERVAL_MS",
    "UI_SIGNAL_V2_BADGES_ENABLED",
    "DYNAMIC_SEMAPHORE_TUNING",
    "HEAVY_COMPUTE_GATING_ENABLED",
    "FEATURE_DIRECTIONAL_GATING",
    "FEATURE_DIRECTIONAL_REWEIGHT",
    "FEATURE_RAPID_MOVE_HYSTERESIS",
    "STAGE2_NARRATIVE_ENABLED",
    "FEATURE_STAGE2_THRESHOLDS_YAML",
    "STAGE2_THRESHOLDS_YAML_PROFILE",
    "FEATURE_PARTICIPATION_LIGHT",
    "ROUTER_ALERT_REQUIRE_DOMINANCE",
    "STAGE1_TRIGGER_VOLUME_SPIKE_ENABLED",
    "STAGE1_TRIGGER_BREAKOUT_ENABLED",
    "STAGE1_TRIGGER_VOLATILITY_SPIKE_ENABLED",
    "STAGE1_TRIGGER_RSI_ENABLED",
    "STAGE1_TRIGGER_VWAP_DEVIATION_ENABLED",
    "STAGE1_PREFILTER_STRICT_ENABLED",
    "STAGE1_PREFILTER_STRICT_SOFT_PROFILE",
    "STRICT_PREFILTER_RELAXED_ALLOW_HTF_STRENGTH",
    "STRICT_PREFILTER_RELAXED_MIN_HTF_STRENGTH",
    "STAGE1_MONITOR_LIMIT_TO_STRICT_PREFILTER",
    "STAGE1_PREFILTER_STRICT_AUTHORITATIVE_IF_PRESENT",
    "STAGE1_PREFILTER_STRICT_BG_REFRESH_ENABLED",
    "STAGE1_PREFILTER_STRICT_BG_INTERVAL_SEC",
    "PACK_EXHAUSTION_REVERSAL_ENABLED",
    "TRAP_COOLDOWN_OVERRIDE_ENABLED",
    "ENABLE_LARGE_ORDER_DETECTION",
    "ENABLE_TRAP_DETECTOR",
    "ENABLE_ORDERBOOK_DETECTORS",
    "ENABLE_CASCADE_DETECTOR",
    "ENABLE_LEVEL_QUALITY_FILTER",
    "ENABLE_PREDICTIVE_SCENARIO_MATRIX",
    "ENABLE_CHESS_MASTER_SYSTEM",
    "ENABLE_WHALE_BEHAVIOR_PREDICTOR",
    "ENABLE_WHALE_RESPONSE_MANAGER",
    "SCENARIO_TRACE_ENABLED",
    "STRICT_SCENARIO_HYSTERESIS_ENABLED",
    "SCENARIO_CANARY_PUBLISH_CANDIDATE",
]
