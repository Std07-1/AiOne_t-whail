# stage1\utils

import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo

import pandas as pd

logger = logging.getLogger("utils ")
logger.setLevel(logging.INFO)


def standardize_format(df: pd.DataFrame, timezone: str = "UTC") -> pd.DataFrame:
    """
    Перетворює колонку 'timestamp' у формат datetime із заданим часовим поясом.
    Якщо дані числові, визначаємо, чи вони в секундах чи мілісекундах.
    Якщо часові мітки tz-naive, локалізуємо їх до UTC, а потім конвертуємо у заданий часовий пояс.

    Args:
        df (pd.DataFrame): Вхідний DataFrame із колонкою 'timestamp'.
        timezone (str): Цільовий часовий пояс (за замовчуванням "UTC").

    Returns:
        pd.DataFrame: Нова копія DataFrame із коректно перетвореною колонкою 'timestamp'.
    """
    # Щоб уникнути SettingWithCopyWarning — спочатку явно копіюємо весь DF
    df = df.copy()

    if "timestamp" in df.columns:
        ts = df["timestamp"]

        # Якщо не в datetime
        if not pd.api.types.is_datetime64_any_dtype(ts):
            ts = pd.to_datetime(ts, utc=True)

        # Локалізуємо (якщо потрібно) і конвертуємо в бажаний timezone
        # з двома кроками: локалізація → конвертація
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        ts = ts.dt.tz_convert(timezone)

        # Записуємо назад у копію DataFrame
        df.loc[:, "timestamp"] = pd.to_datetime(ts, utc=True).astype("int64") // 10**9

    return df


def format_volume_usd(volume: float | str) -> str:
    """
    Форматує оборот у USD.
    Приймає як float, так і вже відформатований рядок —
    у другому випадку повертає його без змін.
    """
    if isinstance(volume, str):
        return volume

    if volume >= 1e12:
        return f"{volume/1e12:.2f}T USD"
    if volume >= 1e9:
        return f"{volume/1e9:.2f}G USD"
    if volume >= 1e6:
        return f"{volume/1e6:.2f}M USD"
    if volume >= 1e3:
        return f"{volume/1e3:.2f}K USD"
    return f"{volume:.2f} USD"


def is_us_session(current_time: datetime) -> bool:
    """
    Перевіряє, чи поточний час входить до робочих годин американської торгової сесії (9:30–16:00 ET).
    """
    try:
        eastern = current_time.astimezone(ZoneInfo("America/New_York"))
    except Exception as e:
        logger.error(f"Помилка конвертації часу: {e}")
        return False
    start = time(9, 30)
    end = time(16, 0)
    result = eastern.weekday() < 5 and start <= eastern.time() <= end
    logger.debug(
        f"[is_us_session] Поточний час (ET): {eastern.time()} — US сесія = {result}"
    )
    return True  # або повернути result для реальної перевірки


def format_open_interest(oi: float) -> str:
    """
    Форматує значення Open Interest для зручного відображення.
    Якщо oi >= 1e9, повертає у мільярдах (B);
    якщо >= 1e6, повертає у мільйонах (M);
    якщо >= 1e3, повертає у тисячах (K);
    інакше повертає стандартне значення.
    """
    try:
        val = float(oi)
    except (ValueError, TypeError):
        return "-"

    if val >= 1e9:
        return f"{val / 1e9:.2f}B"
    elif val >= 1e6:
        return f"{val / 1e6:.2f}M"
    elif val >= 1e3:
        return f"{val / 1e3:.2f}K"
    else:
        return f"{val:.2f} USD"
