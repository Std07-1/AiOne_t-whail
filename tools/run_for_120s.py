# !/usr/bin/env python3
""" Запускає пайплайн на 120 секунд.
Особливості:
    • Використовує RichHandler для гарного логування в консоль.
    • Додає корінь проєкту в sys.path для коректних імпортів.
    • Акуратно зупиняє пайплайн через Cancel, щоб усі ресурси закрилися.
    • Призначений для швидкого тестування або профілювання.
    • Не призначений для продакшену.
    • Логування на рівні INFO.
Спосіб використання:
    $ python tools/run_for_120s.py
    """

import asyncio
import logging
import os
import sys

from rich.logging import RichHandler

# Додаємо корінь проєкту в sys.path, щоб імпорти на кшталт `from app.main` працювали
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.main import run_pipeline


async def main():
    # акуратний запуск/зупинка пайплайна
    task = asyncio.create_task(run_pipeline())
    try:
        await asyncio.sleep(120)
    finally:
        # Зупинка через Cancel — усі ресурси закриваються у finally run_pipeline
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, handlers=[RichHandler()])
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
