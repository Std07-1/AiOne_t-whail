"""UI Snapshot Dumper

Призначення:
- Підписатись на UI-канал та зберігати кожен снапшот у JSONL-файл для подальшого аналізу.
- Альтернатива: періодично опитувати snapshot-ключ (poll) і писати у файл.

Використання:
    # Підписка на канал (рекомендовано)
    python.exe -m tools.ui_snapshot_dumper --out ./replay_dump/ui_snapshots_light.jsonl --duration 3600 --light

    # Poll-режим (fallback)
    C:/Users/vikto/Desktop/AiOne_t-main/venv/Scripts/python.exe -m tools.ui_snapshot_dumper --mode poll --interval 10 --out ./replay_dump/ui_snapshots_light.jsonl --light

Формат:
- Кожний рядок — це JSON payload, який публікує UI.publisher (містить meta.ts/seq).
- Можна потім відфільтрувати окремі символи, агреги тощо.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from config.config import (
    REDIS_CHANNEL_ASSET_STATE,
    REDIS_CHANNEL_UI_ASSET_STATE,
    REDIS_SNAPSHOT_KEY,
    REDIS_SNAPSHOT_UI_KEY,
    UI_USE_V2_NAMESPACE,
)
from data.redis_connection import acquire_redis, release_redis


def _default_out_path() -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"./replay_dump/ui_snapshot_{ts}.jsonl"


def _ensure_dir(path: str) -> None:
    p = Path(path).expanduser().resolve()
    if not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)


def _light_project(payload: dict[str, Any]) -> dict[str, Any]:
    """Зменшена проекція снапшоту: counters + спрощені поля по активах.

    Це допомагає тримати файл невеликим і полегшує подальший аналіз у таблицях/BI.
    """
    out: dict[str, Any] = {
        "meta": payload.get("meta", {}),
        "counters": payload.get("counters", {}),
        "assets": [],
    }
    for a in payload.get("assets", []) or []:
        if not isinstance(a, dict):
            continue
        an = a.get("analytics") if isinstance(a.get("analytics"), dict) else {}
        whale = a.get("whale") if isinstance(a.get("whale"), dict) else {}
        mc = a.get("market_context") or {}
        meta = mc.get("meta") if isinstance(mc, dict) else {}
        out["assets"].append(
            {
                "symbol": a.get("symbol"),
                "price": a.get("price"),
                "band_pct": a.get("band_pct"),
                "confidence": a.get("confidence"),
                "signal": a.get("signal"),
                "recommendation": a.get("recommendation"),
                "htf_ok": a.get("htf_ok"),
                "corridor": {
                    "band_pct": an.get("corridor_band_pct"),
                    "dist_to_edge_pct": an.get("corridor_dist_to_edge_pct"),
                    "near_edge": an.get("corridor_near_edge"),
                },
                "whale": {
                    "ts": whale.get("ts"),
                    "presence": whale.get("presence"),
                    "bias": whale.get("bias"),
                    "vwap_dev": whale.get("vwap_dev"),
                },
                "meta": {
                    "atr_pct": meta.get("atr_pct"),
                    "low_gate": meta.get("low_gate"),
                },
            }
        )
    return out


async def _subscribe_and_dump(
    channel: str, out_path: str, duration: float | None, light: bool
) -> None:
    client = await acquire_redis()
    try:
        pubsub = client.pubsub()
        await pubsub.subscribe(channel)
        print(
            f"[ui_snapshot_dumper] Subscribed to channel={channel}, writing to {out_path}"
        )
        start = asyncio.get_event_loop().time()
        with open(out_path, "a", encoding="utf-8") as f:
            while True:
                if (
                    duration is not None
                    and (asyncio.get_event_loop().time() - start) >= duration
                ):
                    print("[ui_snapshot_dumper] Duration reached — stopping.")
                    break
                msg: dict[str, Any] | None = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.3
                )
                if not msg:
                    await asyncio.sleep(0.05)
                    continue
                if msg.get("type") == "message":
                    data = msg.get("data")
                    try:
                        if isinstance(data, (bytes, bytearray)):
                            line = data.decode("utf-8", errors="ignore")
                        else:
                            line = str(data)
                        # Перевірка/проекція: якщо JSON — можемо спростити (light)
                        try:
                            payload = json.loads(line)
                            if light and isinstance(payload, dict):
                                payload = _light_project(payload)
                                line = json.dumps(payload, ensure_ascii=False)
                        except Exception:
                            line = json.dumps(
                                {"raw": line, "ts": datetime.utcnow().isoformat() + "Z"}
                            )
                        f.write(line.rstrip("\n") + "\n")
                    except Exception:
                        # best-effort: тримаємо процес живим
                        pass
    finally:
        await release_redis(client)


async def _poll_and_dump(
    key: str, out_path: str, interval: float, duration: float | None, light: bool
) -> None:
    client = await acquire_redis()
    try:
        print(
            f"[ui_snapshot_dumper] Polling key={key} every {interval}s, writing to {out_path}"
        )
        start = asyncio.get_event_loop().time()
        last_seq: int | None = None
        with open(out_path, "a", encoding="utf-8") as f:
            while True:
                if (
                    duration is not None
                    and (asyncio.get_event_loop().time() - start) >= duration
                ):
                    print("[ui_snapshot_dumper] Duration reached — stopping.")
                    break
                raw = await client.get(key)
                if raw:
                    try:
                        payload = json.loads(raw)
                        if light and isinstance(payload, dict):
                            payload = _light_project(payload)
                            raw = json.dumps(payload, ensure_ascii=False)
                        meta = (
                            payload.get("meta", {}) if isinstance(payload, dict) else {}
                        )
                        seq = meta.get("seq") if isinstance(meta, dict) else None
                        # Щоб уникати дублікатів — записуємо лише нові seq
                        if not isinstance(seq, int) or last_seq != seq:
                            f.write(raw.rstrip("\n") + "\n")
                            last_seq = seq if isinstance(seq, int) else last_seq
                    except Exception:
                        # якщо не JSON — запишемо як raw
                        f.write(str(raw).rstrip("\n") + "\n")
                await asyncio.sleep(max(0.1, float(interval)))
    finally:
        await release_redis(client)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dump UI snapshots to JSONL")
    parser.add_argument("--mode", choices=["subscribe", "poll"], default="subscribe")
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Poll interval in seconds (for poll mode)",
    )
    parser.add_argument(
        "--out", type=str, default=_default_out_path(), help="Output JSONL path"
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Optional duration limit in seconds",
    )
    parser.add_argument(
        "--channel", type=str, default=None, help="Override channel name"
    )
    parser.add_argument("--key", type=str, default=None, help="Override snapshot key")
    parser.add_argument(
        "--light",
        action="store_true",
        help="Write reduced projection to minimize file size",
    )
    return parser.parse_args()


async def amain() -> None:
    args = parse_args()
    use_v2 = bool(UI_USE_V2_NAMESPACE)
    channel = args.channel or (
        REDIS_CHANNEL_UI_ASSET_STATE if use_v2 else REDIS_CHANNEL_ASSET_STATE
    )
    key = args.key or (REDIS_SNAPSHOT_UI_KEY if use_v2 else REDIS_SNAPSHOT_KEY)
    out_path = args.out
    _ensure_dir(out_path)
    if args.mode == "subscribe":
        await _subscribe_and_dump(channel, out_path, args.duration, args.light)
    else:
        await _poll_and_dump(key, out_path, args.interval, args.duration, args.light)


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        print("[ui_snapshot_dumper] Interrupted by user")


if __name__ == "__main__":
    main()
