"""Запустити Stage1 моніторинг (process_asset_batch) для символів після базової фільтрації.

Використання:
    python -m tools.run_monitor_on_prefilter

Примітка: за замовчуванням обмежую виконання першими 10 символами для швидкого запуску.
"""

import asyncio
import logging

import aiohttp

from app.asset_state_manager import AssetStateManager
from app.screening_producer import process_asset_batch
from data.unified_store import StoreConfig, StoreProfile, UnifiedDataStore
from stage1.asset_monitoring import AssetMonitorStage1
from stage1.binance_future_asset_filter import BinanceFutureAssetFilter
from stage1.optimized_asset_filter import get_filtered_assets


class DummyRedis:
    async def get(self, *args, **kwargs):
        return None

    async def set(self, *args, **kwargs):
        return None


async def main(limit: int = 10, lookback: int = 120):
    async with aiohttp.ClientSession() as session:
        # Запускаємо prefilter, щоб наповнити last_prefiltered
        await get_filtered_assets(
            session=session, cache_handler=None, dynamic=True, max_symbols=None
        )
        pre = BinanceFutureAssetFilter.last_prefiltered or []
        if not pre:
            print("Prefilter повернув порожній список — завершення")
            return
        symbols = [s for s in pre]
        if len(symbols) > limit:
            symbols = symbols[:limit]
        print(f"Запущено моніторинг для {len(symbols)} символів (із prefilter)")

        # Ініціалізуємо UnifiedDataStore у RAM-only режимі (io_retry_attempts=0)
        cfg = StoreConfig()
        cfg.io_retry_attempts = 0
        cfg.profile = StoreProfile(name="small")
        # DummyRedis передається як аргумент redis — RedisAdapter не здійснюватиме викликів при io_retry_attempts=0
        ds = UnifiedDataStore(redis=DummyRedis(), cfg=cfg)

        # Ініціалізація state manager
        init_assets = [s.lower() for s in symbols]
        state_mgr = AssetStateManager(initial_assets=init_assets)

        # Монітор Stage1
        monitor = AssetMonitorStage1(cache_handler=ds, state_manager=state_mgr)

        # Викликаємо обробку батчу (process_asset_batch)
        await process_asset_batch(
            symbols,
            monitor,
            ds,
            timeframe="1m",
            lookback=lookback,
            state_manager=state_mgr,
        )

        # Показуємо результати коротко
        alerts = state_mgr.get_alert_signals()
        print(f"Генерованих ALERT сигналів: {len(alerts)}")
        for a in alerts:
            print(a.get("symbol"), a.get("signal"), a.get("confidence"))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
