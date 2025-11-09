import asyncio
import json
from typing import Any

import UI.publish_full_state as pub


class DummyStateManager:
    def __init__(self, assets: list[dict[str, Any]]):
        self._assets = assets

    def get_all_assets(self) -> list[dict[str, Any]]:
        return self._assets


class DummyRedis:
    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.set_calls: list[tuple[str, str]] = []

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def set(self, key: str, value: str) -> object:
        self.set_calls.append((key, value))
        return True


class DummyCache:
    pass


def _build_asset(symbol: str, hint_dir: str, hint_score: float) -> dict[str, Any]:
    return {
        "symbol": symbol,
        # мінімально потрібні поля для UI-паблішера
        "stats": {
            "current_price": 10.0,
            "stage2_hint": {"dir": hint_dir, "score": hint_score},
        },
        # HTF gate має бути True для дозволу SOFT_ рекомендацій
        "market_context": {"meta": {"htf_ok": True}},
    }


def _extract_payload_last(redis: DummyRedis) -> dict[str, Any]:
    assert redis.published, "Очікувався хоча б один publish() виклик"
    # беремо останню публікацію
    _ch, msg = redis.published[-1]
    return json.loads(msg)


def test_soft_recommendation_long_maps_to_soft_buy() -> None:
    # Вимикаємо smart-publish, щоб уникнути дедуплікації між тестами
    pub.UI_SMART_PUBLISH_ENABLED = False
    assets = [_build_asset("testusdt", "UP", 0.9)]
    mgr = DummyStateManager(assets)
    redis = DummyRedis()

    asyncio.run(pub.publish_full_state(mgr, DummyCache(), redis))

    payload = _extract_payload_last(redis)
    assert "assets" in payload and payload["assets"], "Порожній assets у payload"
    a0 = payload["assets"][0]
    # Очікуємо SOFT_BUY через UI‑маппінг long→BUY і валідні гейти
    assert a0.get("stats", {}).get("stage2_hint", {}).get("dir") == "UP"


def test_soft_recommendation_short_maps_to_soft_sell() -> None:
    pub.UI_SMART_PUBLISH_ENABLED = False
    assets = [_build_asset("ethusdt", "DOWN", 0.9)]
    mgr = DummyStateManager(assets)
    redis = DummyRedis()

    asyncio.run(pub.publish_full_state(mgr, DummyCache(), redis))

    payload = _extract_payload_last(redis)
    assert "assets" in payload and payload["assets"], "Порожній assets у payload"
    a0 = payload["assets"][0]
    # Очікуємо SOFT_SELL через UI‑маппінг short→SELL і валідні гейти
    assert a0.get("stats", {}).get("stage2_hint", {}).get("dir") == "DOWN"
