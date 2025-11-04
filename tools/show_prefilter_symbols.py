"""Запуск prefilter і вивід списку символів після базової фільтрації.

Використання:
    python -m tools.show_prefilter_symbols

Цей скрипт виконує prefilter (stage1.optimized_asset_filter.get_filtered_assets)
і друкує список `BinanceFutureAssetFilter.last_prefiltered`.
"""

import asyncio
import json

import aiohttp

from stage1.binance_future_asset_filter import BinanceFutureAssetFilter
from stage1.optimized_asset_filter import get_filtered_assets


async def main():
    async with aiohttp.ClientSession() as session:
        # cache_handler аргумент може бути None — реалізація використовує UnifiedDataStore у проді,
        # для швидкого локального запуску передаємо None (fetchers роблять HTTP запити напряму).
        try:
            await get_filtered_assets(
                session=session, cache_handler=None, dynamic=True, max_symbols=200
            )
        except Exception as e:
            print("Prefilter run raised:", e)

        pref = BinanceFutureAssetFilter.last_prefiltered
        filt = BinanceFutureAssetFilter.last_filtered_list
        print("\n--- Після базової фільтрації (prefiltered) ---")
        if not pref:
            print("(порожній або недоступний)")
        else:
            print(json.dumps(pref, ensure_ascii=False, indent=2))

        print("\n--- Після повної фільтрації (filtered) ---")
        if not filt:
            print("(порожній або недоступний)")
        else:
            print(json.dumps(filt, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
