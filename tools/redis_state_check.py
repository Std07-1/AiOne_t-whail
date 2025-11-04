"""Redis live-state checker for AiOne_t.

Validates current state of a symbol against breakout expectations.

Usage (PowerShell):
    C:/Users/vikto/Desktop/AiOne_t-main/venv/Scripts/python.exe tools/redis_state_check.py --symbol BTCUSDT

Notes:
- Keys are built via config.keys.build_key in the canonical format ai_one:{domain}:{symbol}.
- Symbol is normalized to lower-case when building keys.
- This script is read-only and exits with code 0 even when checks fail (prints PASS/FAIL).
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

from config.config import NAMESPACE
from config.keys import build_key
from data.redis_connection import acquire_redis, release_redis


def _to_float(v: Any) -> float | None:
    try:
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str) and v.strip() != "":
            return float(v)
    except Exception:
        return None
    return None


ASYNC_OK = {"1", "true", "True", "TRUE", "yes", "on"}


async def check_symbol_state(
    symbol: str,
    *,
    band_pct_min: float = 0.005,
    vol_z_min: float = 1.8,
    dvr_min: float = 1.0,
    cd_min: float = 0.0,
    htf_strength_min: float = 0.02,
) -> int:
    sym = symbol.upper()
    key = build_key(NAMESPACE, "state", symbol=sym)
    client = await acquire_redis()
    try:

        async def hget(field: str) -> Any:
            try:
                return await client.hget(key, field)
            except Exception:
                return None

        phase = await hget("phase")
        band_pct = _to_float(await hget("band_pct"))
        # vol_z fallback: prefer canonical 'vol_z', fallback to legacy 'vol:z'
        vol_z = _to_float(await hget("vol_z"))
        if vol_z is None:
            vol_z = _to_float(await hget("vol:z"))
        dvr = _to_float(await hget("dvr"))
        cd = _to_float(await hget("cd"))
        # htf_ok fallback: support both 'htf:ok' and 'htf_ok'
        htf_ok_raw = await hget("htf:ok")
        if htf_ok_raw is None:
            htf_ok_raw = await hget("htf_ok")
        htf_strength = _to_float(await hget("htf:strength"))
        if htf_strength is None:
            htf_strength = _to_float(await hget("htf_strength"))

        # Evaluate checks
        pass_phase = str(phase or "").strip().lower() in {"momentum", "post_breakout"}
        pass_band = (band_pct is not None) and (band_pct >= band_pct_min)
        # Breakout participation logic: vol_z >= thr OR (abs(vol_z) >= thr and cd >= cd_min)
        abs_volz_ok = (vol_z is not None) and (abs(float(vol_z)) >= vol_z_min)
        pass_volz = (vol_z is not None) and (
            (float(vol_z) >= vol_z_min)
            or (abs_volz_ok and (cd is not None) and (cd >= cd_min))
        )
        pass_dvr = (dvr is not None) and (dvr >= dvr_min)
        pass_cd = (cd is not None) and (cd >= cd_min)
        pass_htf_ok = str(htf_ok_raw) in ASYNC_OK or str(htf_ok_raw) == "1"
        pass_htf_strength = (htf_strength is not None) and (
            htf_strength >= htf_strength_min
        )

        print("# Redis state check")
        print(f"Key: `{key}` (symbol={sym})\n")
        print(f"- phase={phase} -> PASS={pass_phase}")
        print(f"- band_pct={band_pct} (>= {band_pct_min}) -> PASS={pass_band}")
        print(
            f"- vol_z={vol_z} (>= {vol_z_min} or abs>= with cd>= {cd_min}) -> PASS={pass_volz}"
        )
        print(f"- dvr={dvr} (>= {dvr_min}) -> PASS={pass_dvr}")
        print(f"- cd={cd} (>= {cd_min}) -> PASS={pass_cd}")
        print(f"- htf:ok={htf_ok_raw} (== 1/true) -> PASS={pass_htf_ok}")
        print(
            f"- htf:strength={htf_strength} (>= {htf_strength_min}) -> PASS={pass_htf_strength}"
        )

        return 0
    finally:
        await release_redis(client)


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Redis live-state checker for AiOne_t")
    p.add_argument("--symbol", required=True, help="Symbol, e.g. BTCUSDT")
    p.add_argument("--band-pct-min", type=float, default=0.005)
    p.add_argument("--volz-min", type=float, default=1.8)
    p.add_argument("--dvr-min", type=float, default=1.0)
    p.add_argument("--cd-min", type=float, default=0.0)
    p.add_argument("--htf-strength-min", type=float, default=0.02)
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_cli().parse_args(argv)
    return asyncio.run(
        check_symbol_state(
            args.symbol,
            band_pct_min=args.band_pct_min,
            vol_z_min=args.volz_min,
            dvr_min=args.dvr_min,
            cd_min=args.cd_min,
            htf_strength_min=args.htf_strength_min,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
