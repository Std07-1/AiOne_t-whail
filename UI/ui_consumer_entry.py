import asyncio
import logging
import os
import sys

from rich.console import Console
from rich.logging import RichHandler

from config.config import REDIS_CHANNEL_ASSET_STATE  # legacy fallback
from config.config import REDIS_CHANNEL_UI_ASSET_STATE, UI_USE_V2_NAMESPACE
from UI.ui_consumer import UIConsumer

# ── Налаштування логування ─────────────────────────────────────────────────
logger = logging.getLogger("ui_consumer_entry")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


async def main():
    # Додаємо low_atr_threshold як у конструкторі UI_Consumer
    # Отримуємо SIMPLE_UI_MODE динамічно (fallback False для сумісності зі старими версіями)
    ui = UIConsumer(vol_z_threshold=2.5, low_atr_threshold=0.005)
    logger.info("🚀 Запуск UI Consumer...")

    logger.info(
        "Коротке пояснення: \n"
        "Blocks: lowvol|htf|lowconf|OK = A|B|C|D \n"
        "A = blocked_alerts_lowvol (накопичено)\n"
        "B = blocked_alerts_htf\n"
        "C = blocked_alerts_lowconf\n"
        "D = passed_alerts (ALERT, що дійшли без даунгрейду)\n"
        "Downgraded: загальна кількість випадків, коли первинна рекомендація була змінена.\n"
        "Gen: кумулятивно скільки разів Stage2 реально отримав пакет alert_signals (скільки сигналів оброблено)."
        "Skip: скільки циклів без жодного Stage1 ALERT."
    )
    # Автовибір каналу: якщо увімкнено v2‑namespace, слухаємо ui_asset_state,
    # інакше — старий asset_state. Дозволяє уникнути "Очікування даних" при
    # невідповідності namespace між паблішером та консюмером.
    channel = (
        REDIS_CHANNEL_UI_ASSET_STATE
        if UI_USE_V2_NAMESPACE
        else REDIS_CHANNEL_ASSET_STATE
    )

    await ui.redis_consumer(
        redis_url=(
            os.getenv("REDIS_URL")
            or f"redis://{os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT','6379')}/0"
        ),
        channel=channel,
        refresh_rate=0.8,
        loading_delay=1.5,
        smooth_delay=0.05,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Грейсфул завершення при Ctrl+C
        logger.info("Завершення UI Consumer по Ctrl+C…")
        sys.exit(0)
    except asyncio.CancelledError:
        logger.info("UI Consumer скасовано…")
        sys.exit(0)
