"""BTC regime helper (soft gate/hint) — best-effort, без змін контрактів.

Функції:
    - get_btc_regime(now_ts_ms, read_from_redis=True) -> str  # one of: trend_up, trend_down, flat, shock
  - get_btc_htf_strength(read_from_redis=True) -> float
  - apply_btc_gate(symbol, conf, regime, btc_htf_strength, scen_htf_min, enabled=True)
      -> tuple[float, bool]  # (new_conf, penalty_applied)

Примітки:
  - Читання з Redis — best-effort і безпечно падає у no-op.
  - Ключі будуються через config.keys.build_key.
  - Жодних нових залежностей.
"""

from __future__ import annotations

import logging

from rich.console import Console
from rich.logging import RichHandler

# Уникаємо додаткових імпортів типів для сумісності лінтера

try:
    from app.settings import settings  # type: ignore
    from config.config import NAMESPACE  # type: ignore
    from config.keys import build_key  # type: ignore
except Exception:  # pragma: no cover - імпорт-заглушки для статичного аналізу
    settings = None  # type: ignore
    NAMESPACE = "ai_one"  # type: ignore

    def build_key(*_a, **_k):  # type: ignore
        return "ai_one:state:BTCUSDT"


try:
    import redis  # type: ignore
except Exception:  # pragma: no cover - дозволяє працювати без redis
    redis = None  # type: ignore


# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("btc_regime")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.INFO)
    # show_path=True для відображення файлу/рядка у WARN/ERROR
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False  # Не пропагувати далі, щоб уникнути дублювання логів


def _read_btc_state_hash() -> dict:
    """Прочитати хеш ai_one:state:BTCUSDT з Redis у best-effort режимі та повернути словник.

    Ця функція намагається підключитися до Redis та отримати всі поля хеша для BTCUSDT.
    Якщо Redis недоступний, імпорти відсутні або виникає помилка — повертає порожній словник.
    Це забезпечує безпеку та відсутність блокувань у критичному шляху.

    Повертає:
        dict: Словник з полями хеша, наприклад 'htf_strength', 'htf:strength' тощо.
              Порожній dict у разі помилок.
    """
    # Перевірка наявності Redis та налаштувань — якщо ні, повертаємо порожній dict
    if redis is None or settings is None:
        return {}

    # Спроба створити клієнт Redis з налаштувань
    try:
        r = redis.StrictRedis(  # type: ignore[attr-defined]
            host=settings.redis_host, port=settings.redis_port, decode_responses=True
        )
    except Exception:
        # Якщо не вдалося створити клієнт — повертаємо порожній dict
        return {}

    # Спроба прочитати хеш з Redis
    try:
        # Будуємо ключ через config.keys.build_key для консистентності
        key = build_key(NAMESPACE, "state", symbol="BTCUSDT")
        # Отримуємо всі поля хеша; якщо хеш порожній — повертаємо {}
        h = r.hgetall(key) or {}
        # Поля можуть включати 'htf:strength', 'htf_strength', 'whale' (як JSON) та інші
        return h
    except Exception:
        # Будь-яка помилка при читанні — повертаємо порожній dict для best-effort
        return {}


def get_btc_htf_strength(read_from_redis: bool = True) -> float:
    """
    Оцінює поточну силу HTF (Higher Time Frame) для BTC у діапазоні від 0.0 до 1.0.
    Якщо оцінка неможлива або виникають помилки, повертає 0.0.
    Функція використовує хеш стану BTC, зчитаний з Redis, і намагається отримати значення
    з поля 'htf_strength' або, якщо його немає, з поля 'htf:strength'. Якщо значення
    відсутнє або не може бути перетворено на float, повертається 0.0.
    Параметри:
        read_from_redis (bool): Чи зчитувати дані з Redis. Якщо False, завжди повертає 0.0.
                               За замовчуванням True.
    Повертає:
        float: Значення сили HTF у діапазоні [0.0, 1.0], або 0.0 у разі помилки чи відсутності даних.
    Примітки:
        - Функція обробляє винятки внутрішньо, повертаючи 0.0 при будь-яких помилках.
        - Залежить від функції _read_btc_state_hash() для отримання хешу стану.
    """
    if not read_from_redis:
        return 0.0
    h = _read_btc_state_hash()
    try:
        val = h.get("htf_strength")
        if val is None:
            val = h.get("htf:strength")
        return float(val) if val is not None else 0.0
    except Exception:
        return 0.0


def get_btc_regime_v2(stats: dict | None = None, read_from_redis: bool = True) -> str:
    """Визначити BTC‑режим (v2): flat/trend_up/trend_down.

    Правила:
      - Якщо htf_strength < SCEN_HTF_MIN → 'flat'
      - Якщо phase ∈ {"momentum","drift_trend"} і (cd ≥ 0 або slope_atr ≥ 0) → 'trend_up'
      - Якщо phase ∈ {"momentum","drift_trend"} і (cd < 0 або slope_atr < 0) → 'trend_down'
      - Інакше → 'flat'

    Джерела:
      - htf_strength із stats або з Redis state (best‑effort)
      - phase із stats["phase"]["name"] або stats["phase"]/stats["phase_name"]
      - cd із stats["cd"] або stats["cumulative_delta"]
      - slope_atr із stats["slope_atr"]
    """
    try:
        import config.config as _cfg  # type: ignore

        scen_htf_min = float(getattr(_cfg, "SCEN_HTF_MIN", 0.02))
    except Exception:
        scen_htf_min = 0.02

    st = stats or {}
    # htf
    try:
        htf = float(st.get("htf_strength"))  # type: ignore[arg-type]
        if not (htf == htf):  # NaN guard
            raise ValueError
    except Exception:
        htf = get_btc_htf_strength(read_from_redis=read_from_redis)
    if htf < scen_htf_min:
        logger.info(
            f"[BTC_REGIME] BTCUSDT htf_strength={htf:.3f} < {scen_htf_min:.3f} → flat"
        )
        return "flat"

    # phase name
    phase_name = None
    try:
        p = st.get("phase")
        if isinstance(p, dict):
            phase_name = p.get("name")
    except Exception:
        phase_name = None
    if not phase_name:
        phase_name = st.get("phase_name") or (
            st.get("phase") if isinstance(st.get("phase"), str) else None
        )
    phase_name = str(phase_name or "").lower()

    # directional proxies
    def _f(x: object) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    cd = _f(st.get("cd") if st is not None else 0.0)
    if cd == 0.0:
        cd = _f(st.get("cumulative_delta") if st is not None else 0.0)
    slope = _f(st.get("slope_atr") if st is not None else 0.0)

    if phase_name in {"momentum", "drift_trend"}:
        if cd >= 0.0 or slope >= 0.0:
            logger.info(
                f"[BTC_REGIME] BTCUSDT phase={phase_name} cd={cd:.3f} slope_atr={slope:.3f} → trend_up"
            )
            return "trend_up"
        if cd < 0.0 or slope < 0.0:
            logger.info(
                f"[BTC_REGIME] BTCUSDT phase={phase_name} cd={cd:.3f} slope_atr={slope:.3f} → trend_down"
            )
            return "trend_down"
    logger.info(
        f"[BTC_REGIME] BTCUSDT phase={phase_name} cd={cd:.3f} slope_atr={slope:.3f} → flat"
    )
    return "flat"


def apply_btc_gate(
    symbol: str,
    conf: float,
    regime: str,
    btc_htf_strength: float,
    scen_htf_min: float,
    enabled: bool = True,
) -> tuple[float, bool]:
    """
    Застосувати м'який штраф (penalty) до значення confidence (conf) для альткоїнів у випадку, коли BTC знаходиться в режимі "flat" і його сила на вищому таймфреймі нижча за мінімальний поріг сценарію.
    Ця функція призначена для регулювання впевненості в сигналах для криптовалютних пар, які не є BTCUSDT, коли ринок BTC перебуває в невизначеному стані (flat), що може вказувати на підвищений ризик. Штраф зменшує confidence на 20% (множення на 0.8), щоб зменшити агресивність торгівлі в таких умовах.
    Параметри:
        symbol (str): Символ торгової пари, наприклад 'ETHUSDT'. Функція перевіряє, чи це не 'BTCUSDT', оскільки штраф застосовується тільки до альткоїнів.
        conf (float): Початкове значення confidence (впевненості), яке може бути зменшене.
        regime (str): Поточний режим ринку BTC, наприклад 'flat' (плоский), 'bull' (бичачий) або 'bear' (ведмежий). Штраф застосовується тільки при 'flat'.
        btc_htf_strength (float): Сила BTC на вищому таймфреймі (HTF), яка вимірює імпульс або тренд BTC.
        scen_htf_min (float): Мінімальний поріг для сценарію HTF. Якщо btc_htf_strength нижчий за цей поріг, штраф активується.
        enabled (bool, за замовчуванням True): Чи увімкнено цю логіку штрафу. Якщо False, функція повертає оригінальні значення без змін.
    Повертає:
        tuple[float, bool]: Кортеж, що містить:
            - new_conf (float): Нове значення confidence після можливого штрафу (або оригінальне, якщо штраф не застосовується).
            - applied_flag (bool): Прапор, що вказує, чи був штраф застосований (True) чи ні (False).
    Умови застосування штрафу:
        - enabled має бути True.
        - symbol має бути відмінним від 'BTCUSDT' (тобто для альткоїнів).
        - regime має дорівнювати 'flat'.
        - btc_htf_strength має бути меншим за scen_htf_min.
    Якщо умови не виконуються або виникає помилка (наприклад, некоректні типи даних), функція повертає оригінальне conf без штрафу та applied_flag=False.
    Приклад використання:
        new_conf, applied = apply_btc_gate('ETHUSDT', 0.9, 'flat', 0.5, 0.7)
        # Якщо умови виконуються, new_conf = 0.72, applied = True
    """
    try:
        sym = str(symbol or "").upper()
    except Exception:
        sym = ""
    if not enabled:
        return float(conf or 0.0), False
    if sym == "BTCUSDT":
        return float(conf or 0.0), False
    try:
        if str(regime) == "flat" and float(btc_htf_strength) < float(scen_htf_min):
            return float(conf or 0.0) * 0.8, True
    except Exception:
        return float(conf or 0.0), False
    return float(conf or 0.0), False
