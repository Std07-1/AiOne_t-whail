"""Запустити strict prefilter незалежно від основного пайплайну.

Використання:
    python -m tools.run_prefilter_strict

Поведінка:
    - Запускає базовий prefilter (get_filtered_assets) щоб отримати candidate list;
    - Для першого N символів будує stats через AssetMonitorStage1.update_statistics;
    - Виконує stage1.prefilter_strict.prefilter_symbols і друкує/логирує результат.
"""

import asyncio
import logging

import aiohttp

from config.config import PREFILTER_STRICT_PROFILES
from config.flags import STAGE1_PREFILTER_STRICT_SOFT_PROFILE
from data.unified_store import StoreConfig, StoreProfile, UnifiedDataStore
from stage1.asset_monitoring import AssetMonitorStage1
from stage1.binance_future_asset_filter import BinanceFutureAssetFilter
from stage1.optimized_asset_filter import get_filtered_assets
from stage1.prefilter_strict import StrictThresholds, prefilter_symbols


class DummyRedis:
    async def get(self, *args, **kwargs):
        return None

    async def set(self, *args, **kwargs):
        return None


async def main(limit: int = 24, lookback: int = 120):
    async with aiohttp.ClientSession() as session:
        await get_filtered_assets(
            session=session, cache_handler=None, dynamic=True, max_symbols=None
        )
        pre = BinanceFutureAssetFilter.last_prefiltered or []
        if not pre:
            print("Prefilter returned empty list — exiting")
            return
        symbols = [s for s in pre][:limit]
        print(f"Building stats for {len(symbols)} symbols")

        cfg = StoreConfig()
        cfg.io_retry_attempts = 0
        cfg.profile = StoreProfile(name="small")
        ds = UnifiedDataStore(redis=DummyRedis(), cfg=cfg)

        monitor = AssetMonitorStage1(cache_handler=ds, state_manager=None)

        snapshots = []
        for s in symbols:
            df = await ds.get_df(s, "1m", limit=lookback)
            if df is None or df.empty:
                continue
            try:
                stats = await monitor.update_statistics(s, df)
                snapshots.append(stats)
            except Exception as e:
                logging.exception("Failed to build stats for %s: %s", s, e)

        # Diagnostic: print per-symbol score / reasons to see why many are rejected
        from stage1.prefilter_strict import score_symbol

        print("Per-symbol scoring (symbol, score, reasons, key_fields):")
        for s in snapshots:
            try:
                sc, rs = score_symbol(s, StrictThresholds())
            except Exception as e:
                sc, rs = 0.0, [f"score_error:{e}"]
            # show a few useful fields to debug (turnover_usd, vol_z, band_pct, htf_ok, trap_score)
            key_fields = {
                "turnover_usd": s.get("turnover_usd"),
                "vol_z": s.get("volume_z") or s.get("vol_z"),
                "band_pct": s.get("band_pct"),
                "htf_ok": s.get("htf_ok"),
                "trap_score": s.get("trap_score") or (s.get("trap") or {}).get("score"),
            }
            print(s.get("symbol"), sc, rs, key_fields)

        profile_name = "soft" if STAGE1_PREFILTER_STRICT_SOFT_PROFILE else "default"
        profile = PREFILTER_STRICT_PROFILES.get(profile_name, {})
        thresholds = StrictThresholds(**profile) if profile else StrictThresholds()
        res = prefilter_symbols(snapshots, thresholds, redis_client=None)
        print(f"Kept {len(res)} symbols:")
        for r in res:
            print(r.get("symbol"), r.get("score"), r.get("reasons"))

        # --- Quick experiment: relaxed thresholds to see how many symbols would pass
        print("\nQuick experiment: relaxed thresholds (turnover=0, htf_required=False)")
        relaxed = StrictThresholds(min_turnover_usd=0.0, htf_required=False)
        try:
            res_relaxed = prefilter_symbols(snapshots, relaxed, redis_client=None)
            print(f"Relaxed kept {len(res_relaxed)} symbols:")
            for r in res_relaxed:
                print(r.get("symbol"), r.get("score"), r.get("reasons"))
        except Exception:
            import traceback

            print("Relaxed experiment failed")
            traceback.print_exc()
        # Even more relaxed: ignore participation and cd gates to see ranking
        print(
            "\nExperiment 2: ignore participation & cd gates (min_participation_volz=0, min_cd=-1)"
        )
        more_relaxed = StrictThresholds(
            min_turnover_usd=0.0,
            htf_required=False,
            min_participation_volz=0.0,
            min_cd=-1.0,
        )
        try:
            res_more = prefilter_symbols(snapshots, more_relaxed, redis_client=None)
            print(f"More relaxed kept {len(res_more)} symbols:")
            for r in res_more:
                print(r.get("symbol"), r.get("score"), r.get("reasons"))
        except Exception:
            import traceback

            print("More relaxed experiment failed")
            traceback.print_exc()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
