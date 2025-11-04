import json

from stage2.manipulation_detector import (
    Features,
    attach_to_market_context,
    detect_manipulation,
)


def main() -> None:
    # Craft features to trigger a clear liquidity_grab_up
    f = Features(
        symbol="TESTUSDT",
        o=100.0,
        h=110.0,
        low=99.0,
        c=101.0,
        atr=1.0,
        volume_z=0.3,
        range_z=(110.0 - 99.0) / 1.0,  # 11.0
        trade_count_z=0.5,
        near_edge=None,
        band_pct=None,
        htf_strength=0.2,
        cumulative_delta=-0.1,
        price_slope_atr=0.2,
        whale_presence=0.2,
        whale_bias=0.1,
        whale_stale=False,
    )
    ins = detect_manipulation(f)
    market_context: dict = {}
    attach_to_market_context(market_context, ins)
    print(json.dumps(market_context.get("meta", {}), ensure_ascii=False))


if __name__ == "__main__":
    main()
