"""Публікація агрегованого стану активів у Redis (UI snapshot).

Шлях: ``UI/publish_full_state.py``

Винос з `app.screening_producer` для розділення відповідальностей:
    • збір та нормалізація стану (producer)
    • публікація / форматування для UI (цей модуль)

Формат payload (type = REDIS_CHANNEL_ASSET_STATE):
    {
        "type": REDIS_CHANNEL_ASSET_STATE,
        "meta": {"ts": ISO8601UTC},
        "counters": {"assets": N, "alerts": A},
        "assets": [ { ... нормалізовані поля ... } ]
    }

Примітка: Форматовані рядкові значення (`price_str`, `volume_str`, `tp_sl`) додаються
щоб UI не перевизначав бізнес-логіку форматування.

Приклади запуску:
    python.exe -m UI.ui_consumer_entry
    або
    C:/Users/vikto/Desktop/AiOne_t_v2-main/venv/Scripts/python.exe -m UI.ui_consumer_entry
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import Counter
from datetime import datetime
from typing import Any, Protocol

from rich.console import Console
from rich.logging import RichHandler

from config.config import (
    NAMESPACE,
    REDIS_CHANNEL_ASSET_STATE,
    REDIS_CHANNEL_UI_ASSET_STATE,
    REDIS_SNAPSHOT_KEY,
    REDIS_SNAPSHOT_UI_KEY,
    SOFT_RECO_SCORE_THR,
    SOFT_RECOMMENDATIONS_ENABLED,
    STATE_PUBLISH_META_ENABLED,
    UI_DUAL_PUBLISH,
    UI_INSIGHT_PUBLISH_ENABLED,
    UI_PAYLOAD_SCHEMA_VERSION,
    UI_PUBLISH_MIN_INTERVAL_MS,
    UI_SMART_PUBLISH_ENABLED,
    UI_SNAPSHOT_TTL_SEC,
    UI_TP_SL_FROM_STAGE3_ENABLED,
    UI_USE_V2_NAMESPACE,
    UI_WHALE_PUBLISH_ENABLED,
)
from config.config_stage2 import STAGE2_VOLATILITY_REGIME
from config.flags import UI_SIGNAL_V2_BADGES_ENABLED
from config.keys import build_key
from utils.utils import (
    format_price as fmt_price_stage1,
)
from utils.utils import (
    format_volume_usd,
    safe_float,
)
from utils.utils import (
    map_reco_to_signal as _map_reco_to_signal,
)

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("ui.publish_full_state")
if not logger.handlers:  # guard від повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False

# Монотонний sequence для meta (у межах процесу)
_SEQ: int = 0
_LAST_PUB_TS: float | None = None
_LAST_SIG: str | None = None


def _build_signature(counters: dict[str, Any], assets: list[dict[str, Any]]) -> str:
    """Формує компактну сигнатуру для smart-publish.

    Включає стабільні агрегати та мінімальний піднабір полів по активах,
    округлених до розумної точності, відсортовано за символом.
    """
    assets_sig: list[tuple] = []
    for a in assets:
        if not isinstance(a, dict):
            continue
        sym = str(a.get("symbol", "")).upper()
        signal = str(a.get("signal", ""))
        status = str(a.get("status", ""))
        price_str = str(a.get("price_str", a.get("price", "")))
        bp = a.get("band_pct")
        cf = a.get("confidence")
        # Округлення для стабільності
        bp_r = round(float(bp), 4) if isinstance(bp, (int, float)) else None
        cf_r = round(float(cf), 3) if isinstance(cf, (int, float)) else None
        assets_sig.append((sym, signal, status, price_str, bp_r, cf_r))
    assets_sig.sort(key=lambda x: x[0])
    base = [int(counters.get("assets", 0)), int(counters.get("alerts", 0)), assets_sig]
    raw = json.dumps(base, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


class RedisLike(Protocol):
    async def publish(
        self, channel: str, message: str
    ) -> int:  # pragma: no cover - типізація
        ...

    async def set(self, key: str, value: str) -> object:  # pragma: no cover - типізація
        ...


class AssetStateManagerProto(Protocol):
    def get_all_assets(self) -> list[dict[str, Any]]:  # pragma: no cover - типізація
        ...


async def publish_full_state(
    state_manager: AssetStateManagerProto, cache_handler: object, redis_conn: RedisLike
) -> None:
    """Публікує агрегований стан активів у Redis одним повідомленням.

    Формат payload (type = REDIS_CHANNEL_ASSET_STATE):
        {
            "type": REDIS_CHANNEL_ASSET_STATE,
            "meta": {"ts": ISO8601UTC},
            "counters": {"assets": N, "alerts": A},
            "assets": [ ... нормалізовані поля ... ]
        }

    UI може брати заголовок зі ``counters``, а таблицю — з ``assets``.

    Args:
        state_manager: Постачальник станів активів (має метод ``get_all_assets()``).
        cache_handler: Резервний параметр для майбутнього кешу (не використовується).
        redis_conn: Підключення до Redis із методами ``publish`` та ``set``.

    Returns:
        None: Побічно публікує повідомлення у канал і зберігає снапшот у Redis.

    Raises:
        Винятки драйвера Redis або серіалізації зазвичай перехоплюються та логуються,
        оскільки виконання обгорнуто у блок ``try`` (best‑effort).
    """
    global _LAST_PUB_TS, _LAST_SIG
    try:
        all_assets = state_manager.get_all_assets()  # список dict

        def _normalize_ts(value: Any) -> float:
            if value is None:
                return 0.0
            if isinstance(value, (int, float)):
                try:
                    return float(value)
                except Exception:
                    return 0.0
            if isinstance(value, str) and value.strip():
                try:
                    return datetime.fromisoformat(
                        value.replace("Z", "+00:00")
                    ).timestamp()
                except Exception:
                    try:
                        return float(value)
                    except Exception:
                        return 0.0
            return 0.0

        dedup_assets: dict[str, dict[str, Any]] = {}
        if isinstance(all_assets, list):
            for asset in all_assets:
                if not isinstance(asset, dict):
                    continue
                sym_raw = asset.get("symbol")
                sym_key = str(sym_raw).upper() if sym_raw is not None else ""
                if not sym_key:
                    sym_key = f"__UNNAMED__{len(dedup_assets)}"
                stats_obj = (
                    asset.get("stats") if isinstance(asset.get("stats"), dict) else {}
                )
                ts_candidate = None
                if isinstance(stats_obj, dict):
                    for key in ("ts", "timestamp", "price_ts"):
                        if stats_obj.get(key) is not None:
                            ts_candidate = stats_obj.get(key)
                            break
                if ts_candidate is None:
                    ts_candidate = asset.get("last_update_ts") or asset.get("ts")
                ts_value = _normalize_ts(ts_candidate)
                price_candidate = (
                    stats_obj.get("current_price")
                    if isinstance(stats_obj, dict)
                    else None
                )
                if price_candidate is None:
                    price_candidate = asset.get("price")
                price_val = safe_float(price_candidate)
                has_price = bool(price_val is not None and price_val > 0)

                prev_entry = dedup_assets.get(sym_key)
                if prev_entry is None:
                    dedup_assets[sym_key] = {
                        "asset": asset,
                        "ts": ts_value,
                        "has_price": has_price,
                    }
                    continue

                prev_has_price = bool(prev_entry.get("has_price"))
                prev_ts = float(prev_entry.get("ts", 0.0) or 0.0)
                keep_new = False
                if has_price and not prev_has_price:
                    keep_new = True
                elif has_price == prev_has_price and ts_value > prev_ts:
                    keep_new = True

                if keep_new:
                    prev_entry.update(
                        {"asset": asset, "ts": ts_value, "has_price": has_price}
                    )

            all_assets = [entry["asset"] for entry in dedup_assets.values()]

        serialized_assets: list[dict[str, Any]] = []
        band_samples: list[float] = []
        dist_edge_samples: list[float] = []
        edge_ratio_samples: list[float] = []
        low_gate_samples: list[float] = []
        atr_meta_samples: list[float] = []
        atr_vs_low_gate_samples: list[float] = []
        near_edge_counter: Counter[str] = Counter()
        near_edge_alerts = 0
        near_edge_total = 0
        within_true = 0
        within_false = 0
        low_vol_assets = 0
        low_vol_alerts = 0

        # Попередньо завантажимо core:trades для TP/SL таргетів (best-effort)
        core_stats: dict[str, Any] | None = None
        core_trades: dict[str, Any] | None = None
        try:
            # cache_handler може бути UnifiedStore із redis.jget; якщо ні — пропускаємо
            redis_attr = getattr(cache_handler, "redis", None)
            jget = getattr(redis_attr, "jget", None) if redis_attr is not None else None
            if callable(jget):
                # Новий шлях (INFRA v2): плоский ключ ai_one:core:stats
                doc_direct = await jget("core", "stats", default=None)
                if isinstance(doc_direct, dict):
                    core_stats = doc_direct
                else:
                    # Fallback: старий формат — єдиний документ ai_one:core зі вкладеним полем stats
                    core_doc = await jget("core", default=None)
                    if isinstance(core_doc, dict):
                        nested_stats = core_doc.get("stats")
                        if isinstance(nested_stats, dict):
                            core_stats = nested_stats
                        nested_trades = core_doc.get("trades")
                        if isinstance(nested_trades, dict):
                            core_trades = nested_trades
                # Спробуємо також прямий ключ для trades (новий шлях)
                if core_trades is None:
                    doc_trades = await jget("core", "trades", default=None)
                    if isinstance(doc_trades, dict):
                        core_trades = doc_trades
            else:
                core_stats = None
                core_trades = None
        except Exception:
            core_stats = None
            core_trades = None

        # Витягнемо мапу targets із core_trades (символ → {tp,sl})
        targets_map: dict[str, dict[str, float]] = {}
        try:
            if isinstance(core_trades, dict) and isinstance(
                core_trades.get("targets"), dict
            ):
                # Нормалізуємо ключі символів до upper
                for k, v in core_trades["targets"].items():
                    if isinstance(k, str) and isinstance(v, dict):
                        sym = k.upper()
                        tpv = v.get("tp")
                        slv = v.get("sl")
                        if isinstance(tpv, (int, float)) and isinstance(
                            slv, (int, float)
                        ):
                            targets_map[sym] = {"tp": float(tpv), "sl": float(slv)}
        except Exception:
            targets_map = {}

        telemetry_map: dict[str, dict[str, Any]] = {}
        try:
            if isinstance(core_trades, dict) and isinstance(
                core_trades.get("telemetry"), dict
            ):
                for k, v in core_trades["telemetry"].items():
                    if isinstance(k, str) and isinstance(v, dict):
                        telemetry_map[k.upper()] = v
        except Exception:
            telemetry_map = {}

        stage3_rr_samples: list[float] = []
        stage3_unrealized_samples: list[float] = []
        stage3_mfe_samples: list[float] = []
        stage3_mae_samples: list[float] = []
        stage3_trail_armed = 0
        stage3_trail_total = 0

        for asset in all_assets:
            # Захист: stats має бути dict
            if not isinstance(asset.get("stats"), dict):
                asset["stats"] = {}
            # числові поля для рядка таблиці
            for key in ["tp", "sl", "rsi", "volume", "atr", "confidence"]:
                if key in asset:
                    try:
                        asset[key] = (
                            float(asset[key])
                            if asset[key] not in [None, "", "NaN"]
                            else 0.0
                        )
                    except (TypeError, ValueError):
                        asset[key] = 0.0

            # ціна для UI: форматування виконується нижче через fmt_price_stage1

            # нормалізуємо базові статс (лише якщо ключ існує; не вводимо штучні 0.0)
            if "stats" in asset:
                for stat_key in [
                    "current_price",
                    "atr",
                    "volume_mean",
                    "open_interest",
                    "rsi",
                    "rel_strength",
                    "btc_dependency_score",
                ]:
                    if stat_key in asset["stats"]:
                        try:
                            val = asset["stats"][stat_key]
                            asset["stats"][stat_key] = (
                                float(val) if val not in [None, "", "NaN"] else None
                            )
                        except (TypeError, ValueError):  # narrow: очікувана валідація
                            asset["stats"][stat_key] = None

            # ── UI flattening layer ────────────────────────────────────────
            stats = asset.get("stats") or {}
            # Уніфіковані кореневі ключі, щоб UI не мав додаткових мапперів
            # Ціну завжди беремо зі stats.current_price (джерело правди).
            cp = stats.get("current_price")
            try:
                cp_f = float(cp) if cp is not None else None
            except Exception:
                cp_f = None
            if cp_f is not None and cp_f > 0:
                asset["price"] = cp_f
                try:
                    asset["price_str"] = fmt_price_stage1(
                        float(asset["price"]), str(asset.get("symbol", "")).lower()
                    )
                    # Уніфікація форматування: без роздільників тисячних (ком)
                    try:
                        if isinstance(asset.get("price_str"), str):
                            asset["price_str"] = asset["price_str"].replace(",", "")
                    except Exception:
                        pass
                except Exception:
                    asset.pop("price_str", None)
            else:
                # Поточна ціна невалідна → прибираємо застаріле форматування
                asset.pop("price", None)
                asset.pop("price_str", None)
            # Raw volume_mean (кількість контрактів/штук) — оновлюємо КОЖЕН цикл
            vm = stats.get("volume_mean")
            try:
                if isinstance(vm, (int, float)):
                    asset["raw_volume"] = float(vm)
                else:
                    asset.pop("raw_volume", None)
            except Exception:
                asset.pop("raw_volume", None)
            # Обчислюємо оборот у USD (notional) = raw_volume * current_price (переобчислюємо кожен раз)
            cp_val = stats.get("current_price")
            try:
                cp_f2 = float(cp_val) if cp_val is not None else None
            except Exception:
                cp_f2 = None
            if (
                isinstance(asset.get("raw_volume"), (int, float))
                and cp_f2 is not None
                and cp_f2 > 0
            ):
                asset["volume"] = float(asset["raw_volume"]) * float(cp_f2)
                try:
                    asset["volume_str"] = format_volume_usd(float(asset["volume"]))
                except Exception:
                    asset.pop("volume_str", None)
            else:
                asset.pop("volume", None)
                asset.pop("volume_str", None)
            # ATR% (для UI) — перераховуємо завжди (може змінюватися ATR або ціна)
            atr_v = stats.get("atr")
            cp_for_atr = stats.get("current_price")
            try:
                atr_f = float(atr_v) if atr_v is not None else None
            except Exception:
                atr_f = None
            try:
                cp_f_atr = float(cp_for_atr) if cp_for_atr is not None else None
            except Exception:
                cp_f_atr = None
            if atr_f is not None and cp_f_atr is not None and cp_f_atr > 0:
                asset["atr_pct"] = float(atr_f) / float(cp_f_atr) * 100.0
            else:
                # Якщо більше невалідно — прибираємо, щоб не залишався застарілий відсоток
                asset.pop("atr_pct", None)
            # RSI — перезаписуємо якщо присутній у stats; не тримаємо старе значення
            rsi_v = stats.get("rsi")
            try:
                rsi_f = float(rsi_v) if rsi_v is not None else None
            except Exception:
                rsi_f = None
            if rsi_f is not None:
                asset["rsi"] = rsi_f
            else:
                asset.pop("rsi", None)
            # status: перераховуємо щоразу, щоб не застрягав у 'init'
            status_val = asset.get("state")
            if isinstance(status_val, dict):  # захист
                status_val = status_val.get("status") or status_val.get("state")
            if not isinstance(status_val, str) or not status_val:
                status_val = (
                    asset.get("scenario") or asset.get("stage2_status") or "normal"
                )
            # Більше НЕ замінюємо 'init' на 'initializing' – коротка форма
            asset["status"] = status_val

            # Узгодження сигналу зі Stage2 recommendation: якщо rec → ALERT*,
            # форсуємо signal і статус 'alert', аби уникнути розсинхрону зі стейтом
            try:
                rec_val = asset.get("recommendation")
                sig_from_rec = _map_reco_to_signal(rec_val)
                # Сигнал у колонці "Сигнал" = Stage2 мапований;
                # Статус (state) не форсуємо — якщо Stage1 виставив ALERT і Stage2 понизив, залишаємо ALERT.
                if sig_from_rec in ("ALERT_BUY", "ALERT_SELL"):
                    asset["signal"] = sig_from_rec
                # Якщо сигнали нейтральні, не чіпаємо asset['state'] / status
            except Exception:
                pass

            sym_up = str(asset.get("symbol", "")).upper()

            # tp_sl: береться виключно зі Stage3 (core:trades.targets), без локальних розрахунків
            # Можна вимкнути повністю через feature‑flag UI_TP_SL_FROM_STAGE3_ENABLED
            if not UI_TP_SL_FROM_STAGE3_ENABLED:
                asset["tp_sl"] = "-"
            else:
                try:
                    tgt = targets_map.get(sym_up)
                    if (
                        tgt
                        and isinstance(tgt.get("tp"), (int, float))
                        and isinstance(tgt.get("sl"), (int, float))
                    ):
                        fmt_tp = fmt_price_stage1(float(tgt["tp"]), sym_up.lower())
                        fmt_sl = fmt_price_stage1(float(tgt["sl"]), sym_up.lower())
                        asset["tp_sl"] = f"TP: {fmt_tp} | SL: {fmt_sl}"
                    else:
                        asset["tp_sl"] = "-"
                except Exception:
                    asset["tp_sl"] = "-"

            stage3_payload = telemetry_map.get(sym_up)
            if isinstance(stage3_payload, dict):
                stage3_block: dict[str, Any] = {}
                rr_val = safe_float(stage3_payload.get("rr"))
                if rr_val is not None:
                    rr_round = round(rr_val, 2)
                    stage3_block["rr"] = rr_round
                    stage3_rr_samples.append(rr_round)
                unrealized_val = safe_float(stage3_payload.get("unrealized_pct"))
                if unrealized_val is not None:
                    unrealized_round = round(unrealized_val, 2)
                    stage3_block["unrealized_pct"] = unrealized_round
                    stage3_unrealized_samples.append(unrealized_round)
                mfe_val = safe_float(stage3_payload.get("mfe_pct"))
                if mfe_val is not None:
                    mfe_round = round(mfe_val, 2)
                    stage3_block["mfe_pct"] = mfe_round
                    stage3_mfe_samples.append(mfe_round)
                mae_val = safe_float(stage3_payload.get("mae_pct"))
                if mae_val is not None:
                    mae_round = round(mae_val, 2)
                    stage3_block["mae_pct"] = mae_round
                    stage3_mae_samples.append(mae_round)
                trail_payload = stage3_payload.get("trail")
                if isinstance(trail_payload, dict):
                    trail_block: dict[str, Any] = {}
                    if "armed" in trail_payload:
                        armed_val = bool(trail_payload.get("armed"))
                        trail_block["armed"] = armed_val
                        stage3_trail_total += 1
                        if armed_val:
                            stage3_trail_armed += 1
                    buffer_pct_val = safe_float(trail_payload.get("buffer_pct"))
                    if buffer_pct_val is not None:
                        trail_block["buffer_pct"] = round(buffer_pct_val, 2)
                    move_atr_val = safe_float(trail_payload.get("move_atr"))
                    if move_atr_val is not None:
                        trail_block["move_atr"] = round(move_atr_val, 2)
                    distance_pct_val = safe_float(trail_payload.get("distance_pct"))
                    if distance_pct_val is not None:
                        trail_block["distance_pct"] = round(distance_pct_val, 2)
                    allow_be = trail_payload.get("allow_break_even")
                    if allow_be is not None:
                        trail_block["allow_break_even"] = bool(allow_be)
                    armed_ts_val = trail_payload.get("armed_ts")
                    if isinstance(armed_ts_val, str) and armed_ts_val:
                        trail_block["armed_ts"] = armed_ts_val
                    first_sl_val = safe_float(trail_payload.get("first_sl"))
                    if first_sl_val is not None:
                        trail_block["first_sl"] = first_sl_val
                    if trail_block:
                        stage3_block["trail"] = trail_block
                if stage3_block:
                    asset["stage3"] = stage3_block
                else:
                    asset.pop("stage3", None)
            else:
                asset.pop("stage3", None)
            # гарантуємо signal (для UI фільтра)
            if not asset.get("signal"):
                asset["signal"] = "NONE"
            # видимість (fallback True якщо не задано)
            if "visible" in asset and asset["visible"] is False:
                pass  # залишаємо як є
            else:
                asset.setdefault("visible", True)

            # Проксі метаданих HTF та коридорної аналітики для UI
            mc_raw = asset.get("market_context")
            mc = mc_raw if isinstance(mc_raw, dict) else {}
            meta_candidate = mc.get("meta") if isinstance(mc, dict) else {}
            meta = meta_candidate if isinstance(meta_candidate, dict) else {}
            analytics_bucket = asset.get("analytics")
            if not isinstance(analytics_bucket, dict):
                analytics_bucket = {}

            try:
                if "htf_alignment" in meta and "htf_alignment" not in asset:
                    val = meta.get("htf_alignment")
                    if isinstance(val, (int, float)):
                        asset["htf_alignment"] = float(val)
                if "htf_ok" in meta and "htf_ok" not in asset:
                    hov = meta.get("htf_ok")
                    if isinstance(hov, bool):
                        asset["htf_ok"] = hov
                if "htf_ok" in meta:
                    hov = meta.get("htf_ok")
                    if isinstance(hov, bool):
                        analytics_bucket.setdefault("htf_ok", hov)
            except Exception:
                pass

            corridor_meta: dict[str, Any] = {}
            corridor_candidate = (
                meta.get("corridor") if isinstance(meta, dict) else None
            )
            if isinstance(corridor_candidate, dict):
                corridor_meta = corridor_candidate
            else:
                km = mc.get("key_levels_meta") if isinstance(mc, dict) else {}
                if isinstance(km, dict):
                    corridor_meta = km

            signal_upper = str(asset.get("signal", "")).upper()
            was_near_edge_asset = False

            low_gate_val = safe_float(meta.get("low_gate"))
            if low_gate_val is not None:
                analytics_bucket["low_gate"] = low_gate_val
                low_gate_samples.append(low_gate_val)

            atr_meta_val = safe_float(meta.get("atr_pct"))
            if atr_meta_val is not None:
                analytics_bucket["atr_pct_stage2"] = atr_meta_val
                atr_meta_samples.append(atr_meta_val)

            atr_vs_low_gate = None
            if (
                atr_meta_val is not None
                and low_gate_val is not None
                and low_gate_val > 0
            ):
                atr_vs_low_gate = atr_meta_val / low_gate_val
                analytics_bucket["atr_vs_low_gate_ratio"] = atr_vs_low_gate
                atr_vs_low_gate_samples.append(atr_vs_low_gate)

            low_vol_flag: bool | None = None
            if atr_meta_val is not None and low_gate_val is not None:
                low_vol_flag = atr_meta_val < low_gate_val
                if low_vol_flag:
                    low_vol_assets += 1
                    if signal_upper.startswith("ALERT"):
                        low_vol_alerts += 1
                analytics_bucket["low_volatility_flag"] = low_vol_flag

            band_val = safe_float(corridor_meta.get("band_pct"))
            if band_val is not None:
                analytics_bucket["corridor_band_pct"] = band_val
                band_samples.append(band_val)

            dist_edge_pct = safe_float(corridor_meta.get("dist_to_edge_pct"))
            if dist_edge_pct is not None:
                analytics_bucket["corridor_dist_to_edge_pct"] = dist_edge_pct
                dist_edge_samples.append(dist_edge_pct)

            dist_edge_ratio = safe_float(corridor_meta.get("dist_to_edge_ratio"))
            if dist_edge_ratio is not None:
                analytics_bucket["corridor_dist_to_edge_ratio"] = dist_edge_ratio
                edge_ratio_samples.append(dist_edge_ratio)

            nearest_edge = corridor_meta.get("nearest_edge")
            if isinstance(nearest_edge, str):
                analytics_bucket["corridor_nearest_edge"] = nearest_edge

            near_edge_val = corridor_meta.get("near_edge")
            if isinstance(near_edge_val, str):
                analytics_bucket["corridor_near_edge"] = near_edge_val
                near_edge_counter[near_edge_val] += 1
                was_near_edge_asset = True

            is_near_edge = corridor_meta.get("is_near_edge")
            if isinstance(is_near_edge, bool):
                analytics_bucket["corridor_is_near_edge"] = is_near_edge
                if is_near_edge:
                    was_near_edge_asset = True

            within_corridor = corridor_meta.get("within_corridor")
            if isinstance(within_corridor, bool):
                analytics_bucket["corridor_within"] = within_corridor
                if within_corridor:
                    within_true += 1
                else:
                    within_false += 1

            if was_near_edge_asset:
                near_edge_total += 1
                if signal_upper.startswith("ALERT"):
                    near_edge_alerts += 1

            # ── Легка проєкція ключових метрик у корінь для UI колонок ──
            try:
                stats_obj_for_cols = (
                    asset.get("stats") if isinstance(asset.get("stats"), dict) else {}
                )
                # Додамо версію схеми на рівні активу (для майбутньої сумісності)
                asset["schema_version"] = UI_PAYLOAD_SCHEMA_VERSION
                # Band% у корінь (UI також читає з analytics, але це спростить рендер)
                band_root_val = safe_float(stats_obj_for_cols.get("band_pct"))
                if band_root_val is not None:
                    asset["band_pct"] = band_root_val

                # Conf% як проксі: беремо phase.score, інакше stage2_hint.score (0..1)
                phase_blk = (
                    stats_obj_for_cols.get("phase")
                    if isinstance(stats_obj_for_cols.get("phase"), dict)
                    else {}
                )
                hint_blk = (
                    stats_obj_for_cols.get("stage2_hint")
                    if isinstance(stats_obj_for_cols.get("stage2_hint"), dict)
                    else {}
                )
                conf_phase = safe_float(
                    getattr(phase_blk, "get", dict.get)(phase_blk, "score")
                )
                conf_hint = safe_float(
                    getattr(hint_blk, "get", dict.get)(hint_blk, "score")
                )
                if conf_phase is not None:
                    asset["confidence"] = max(0.0, min(1.0, float(conf_phase)))
                elif conf_hint is not None:
                    asset["confidence"] = max(0.0, min(1.0, float(conf_hint)))
                else:
                    # Фолбек: використовуємо впевненість роутера signal_v2_conf, якщо доступна
                    try:
                        sv2_conf_fb = safe_float(
                            getattr(stats_obj_for_cols, "get", dict.get)(
                                stats_obj_for_cols, "signal_v2_conf"
                            )
                        )
                        if sv2_conf_fb is not None:
                            asset["confidence"] = max(0.0, min(1.0, float(sv2_conf_fb)))
                    except Exception:
                        pass

                # Мінімальна логіка "SOFT_" рекомендацій на базі hint‑ів (без зміни контрактів)
                # Умови: фіче‑флаг, наявні hint.dir/score, htf_ok=True, і не hyper‑волатильність.
                if SOFT_RECOMMENDATIONS_ENABLED:
                    try:
                        # Витягуємо поточний recommendation, щоб не перезаписати "жорсткий"
                        existing_rec_raw = asset.get("recommendation")
                        existing_rec = (
                            str(existing_rec_raw).upper() if existing_rec_raw else ""
                        )
                        # Працюємо лише якщо рекомендація відсутня/порожня/"SOFT_*"
                        can_override_soft = (
                            not existing_rec
                            or existing_rec == "-"
                            or existing_rec.startswith("SOFT_")
                        )
                        if can_override_soft:
                            hint_dir = (
                                getattr(hint_blk, "get", dict.get)(hint_blk, "dir")
                                if isinstance(hint_blk, dict)
                                else None
                            )
                            hint_score = safe_float(
                                getattr(hint_blk, "get", dict.get)(hint_blk, "score")
                            )
                            # HTF gate: з кореня (якщо вже проксовано), інакше з meta
                            htf_ok_val = asset.get("htf_ok")
                            if not isinstance(htf_ok_val, bool):
                                mc = asset.get("market_context") or {}
                                meta = mc.get("meta") if isinstance(mc, dict) else {}
                                hov = (
                                    meta.get("htf_ok")
                                    if isinstance(meta, dict)
                                    else None
                                )
                                htf_ok_val = (
                                    bool(hov) if isinstance(hov, bool) else None
                                )

                            # Vol regime gate через atr_vs_low_gate_ratio
                            # Якщо ratio ≥ hyper_threshold → забороняємо SOFT_ рекомендації
                            mc2 = asset.get("market_context") or {}
                            meta2 = mc2.get("meta") if isinstance(mc2, dict) else {}
                            atr_pct_meta = safe_float(meta2.get("atr_pct"))
                            low_gate_meta = safe_float(meta2.get("low_gate"))
                            ratio = None
                            if (
                                atr_pct_meta is not None
                                and low_gate_meta is not None
                                and low_gate_meta > 0
                            ):
                                ratio = atr_pct_meta / low_gate_meta
                            hyper_thr = float(
                                STAGE2_VOLATILITY_REGIME.get("hyper_threshold", 2.5)
                            )
                            not_hyper = True if ratio is None else (ratio < hyper_thr)

                            if (
                                isinstance(hint_dir, str)
                                and hint_dir.upper() in {"BUY", "SELL"}
                                and (hint_score is not None)
                                and float(hint_score) >= float(SOFT_RECO_SCORE_THR)
                                and (htf_ok_val is True)
                                and not_hyper
                            ):
                                soft_rec = (
                                    "SOFT_BUY"
                                    if hint_dir.upper() == "BUY"
                                    else "SOFT_SELL"
                                )
                                asset["recommendation"] = soft_rec
                            elif existing_rec.startswith("SOFT_"):
                                # Якщо умови більше не виконуються — приберемо застарілу SOFT_ мітку
                                asset["recommendation"] = "-"
                    except Exception:
                        # Ніколи не ламаємо публікацію через допоміжну SOFT‑логіку
                        pass
            except Exception:
                # Не ламаємо публікацію у випадку будь-яких аномалій
                pass

            # UI-бейдж для signal_v2 (опціонально, без зміни базових контрактів)
            try:
                if UI_SIGNAL_V2_BADGES_ENABLED:
                    stats_blk2 = (
                        asset.get("stats")
                        if isinstance(asset.get("stats"), dict)
                        else {}
                    )
                    sv2_raw = stats_blk2.get("signal_v2")
                    if isinstance(sv2_raw, str) and sv2_raw:
                        sv2 = sv2_raw.upper()
                        conf_v = safe_float(stats_blk2.get("signal_v2_conf"))
                        # Мапа важливості для простого відображення в UI
                        if sv2 in ("ALERT_BUY", "ALERT_SELL"):
                            severity = "alert"
                        elif sv2 in ("SOFT_BUY", "SOFT_SELL"):
                            severity = "soft"
                        elif sv2 in ("OBSERVE", "AVOID"):
                            severity = "neutral"
                        else:
                            severity = "neutral"
                        label = sv2.replace("_", " ")
                        badge = {"label": label, "severity": severity}
                        if conf_v is not None:
                            badge["conf"] = round(float(conf_v), 2)
                        badges_obj = (
                            asset.get("badges")
                            if isinstance(asset.get("badges"), dict)
                            else {}
                        )
                        badges_obj["signal_v2"] = badge
                        asset["badges"] = badges_obj
            except Exception:
                # badges — допоміжна функціональність; не ламаємо публікацію
                pass

            # Короткий бейдж для bias_state (↓, 0, ↑) біля символу — лише UI, контрактів не чіпаємо
            try:
                mc_meta = (asset.get("market_context") or {}).get("meta", {})
                if isinstance(mc_meta, dict):
                    bs_val = mc_meta.get("bias_state")
                    if isinstance(bs_val, (int, float)):
                        arrow = "0"
                        if int(bs_val) > 0:
                            arrow = "↑"
                        elif int(bs_val) < 0:
                            arrow = "↓"
                        badges_obj = (
                            asset.get("badges")
                            if isinstance(asset.get("badges"), dict)
                            else {}
                        )
                        badges_obj["bias"] = {"label": arrow, "severity": "neutral"}
                        asset["badges"] = badges_obj
            except Exception:
                pass

            if analytics_bucket:
                asset["analytics"] = analytics_bucket
            else:
                asset.pop("analytics", None)

            # ── Whale/Insight merge (best‑effort, не ламає контрактів) ──
            try:
                sym_for_keys = str(asset.get("symbol", "")).upper()
                jget = None
                redis_obj = getattr(cache_handler, "redis", None)
                if redis_obj is not None:
                    jget = getattr(redis_obj, "jget", None)
                if callable(jget):
                    if UI_WHALE_PUBLISH_ENABLED:
                        w = await jget("whale", sym_for_keys, "1m", default=None)
                        w = w if isinstance(w, dict) else {}
                        # Нормалізація legacy-полів у канонічні назви
                        try:
                            if "presence_score" in w and "presence" not in w:
                                w["presence"] = w.get("presence_score")
                                w.pop("presence_score", None)
                            if "vwap_deviation" in w and "vwap_dev" not in w:
                                w["vwap_dev"] = w.get("vwap_deviation")
                                w.pop("vwap_deviation", None)
                            if "zones" in w and "zones_summary" not in w:
                                w["zones_summary"] = w.get("zones")
                                w.pop("zones", None)
                        except Exception:
                            pass
                        asset["whale"] = w
                    if UI_INSIGHT_PUBLISH_ENABLED:
                        ins = await jget("insight", sym_for_keys, "1m", default=None)
                        asset["insight"] = ins if isinstance(ins, dict) else {}
                else:
                    # якщо немає JSON‑інтерфейсу — повертати порожні блоки
                    if UI_WHALE_PUBLISH_ENABLED:
                        asset.setdefault("whale", {})
                    if UI_INSIGHT_PUBLISH_ENABLED:
                        asset.setdefault("insight", {})
            except Exception:
                # захист від побічних ефектів у критичному UI‑шляху
                if UI_WHALE_PUBLISH_ENABLED:
                    asset.setdefault("whale", {})
                if UI_INSIGHT_PUBLISH_ENABLED:
                    asset.setdefault("insight", {})

            # Опціональна де-редундація: якщо raw_trigger_reasons == trigger_reasons — приберемо raw*
            try:
                tr = asset.get("trigger_reasons")
                rtr = asset.get("raw_trigger_reasons")
                if isinstance(tr, list) and isinstance(rtr, list) and tr == rtr:
                    asset.pop("raw_trigger_reasons", None)
            except Exception:
                pass

            # Флет сценарію з market_context.meta (для зручності споживачів)
            try:
                mc_meta = (asset.get("market_context") or {}).get("meta", {})
                if isinstance(mc_meta, dict) and mc_meta:
                    if "scenario_detected" in mc_meta:
                        asset["scenario_detected"] = mc_meta.get("scenario_detected")
                    if "scenario_confidence" in mc_meta:
                        asset["scenario_confidence"] = mc_meta.get(
                            "scenario_confidence"
                        )
            except Exception:
                pass

            serialized_assets.append(asset)

        analytics_summary: dict[str, Any] = {}
        total_assets = len(serialized_assets)
        if band_samples:
            analytics_summary["corridor_band_pct"] = {
                "avg": round(sum(band_samples) / len(band_samples), 5),
                "min": round(min(band_samples), 5),
                "max": round(max(band_samples), 5),
                "count": len(band_samples),
            }
        if dist_edge_samples:
            analytics_summary["corridor_dist_to_edge_pct"] = {
                "avg": round(sum(dist_edge_samples) / len(dist_edge_samples), 5),
                "min": round(min(dist_edge_samples), 5),
                "max": round(max(dist_edge_samples), 5),
                "count": len(dist_edge_samples),
            }
        if edge_ratio_samples:
            analytics_summary["corridor_dist_to_edge_ratio"] = {
                "avg": round(sum(edge_ratio_samples) / len(edge_ratio_samples), 5),
                "min": round(min(edge_ratio_samples), 5),
                "max": round(max(edge_ratio_samples), 5),
                "count": len(edge_ratio_samples),
            }
        if low_gate_samples:
            analytics_summary["low_gate"] = {
                "avg": round(sum(low_gate_samples) / len(low_gate_samples), 5),
                "min": round(min(low_gate_samples), 5),
                "max": round(max(low_gate_samples), 5),
                "count": len(low_gate_samples),
            }
        if atr_meta_samples:
            analytics_summary["atr_pct_stage2"] = {
                "avg": round(sum(atr_meta_samples) / len(atr_meta_samples), 5),
                "min": round(min(atr_meta_samples), 5),
                "max": round(max(atr_meta_samples), 5),
                "count": len(atr_meta_samples),
            }
        if atr_vs_low_gate_samples:
            analytics_summary["atr_vs_low_gate_ratio"] = {
                "avg": round(
                    sum(atr_vs_low_gate_samples) / len(atr_vs_low_gate_samples), 5
                ),
                "min": round(min(atr_vs_low_gate_samples), 5),
                "max": round(max(atr_vs_low_gate_samples), 5),
                "count": len(atr_vs_low_gate_samples),
            }
        if near_edge_counter:
            analytics_summary["near_edge_counts"] = dict(near_edge_counter)
        if near_edge_total:
            analytics_summary["near_edge_assets"] = int(near_edge_total)
            if total_assets:
                analytics_summary["near_edge_assets_share"] = round(
                    near_edge_total / total_assets, 3
                )
        if near_edge_alerts:
            analytics_summary["near_edge_alerts"] = int(near_edge_alerts)
        if within_true or within_false:
            analytics_summary["within_corridor"] = {
                "true": int(within_true),
                "false": int(within_false),
            }
        if low_vol_assets or low_vol_alerts:
            summary_block: dict[str, float | int] = {
                "assets": int(low_vol_assets),
            }
            if total_assets:
                summary_block["assets_share"] = round(low_vol_assets / total_assets, 3)
            if low_vol_alerts:
                summary_block["alerts"] = int(low_vol_alerts)
            analytics_summary["low_volatility"] = summary_block

        if telemetry_map:
            stage3_summary: dict[str, Any] = {"active": int(len(telemetry_map))}
            if stage3_rr_samples:
                try:
                    stage3_summary["rr_avg"] = round(
                        sum(stage3_rr_samples) / len(stage3_rr_samples), 3
                    )
                    stage3_summary["rr_max"] = round(max(stage3_rr_samples), 3)
                    stage3_summary["rr_min"] = round(min(stage3_rr_samples), 3)
                except Exception:
                    pass
            if stage3_unrealized_samples:
                try:
                    stage3_summary["unrealized_avg"] = round(
                        sum(stage3_unrealized_samples) / len(stage3_unrealized_samples),
                        3,
                    )
                    stage3_summary["unrealized_max"] = round(
                        max(stage3_unrealized_samples), 3
                    )
                    stage3_summary["unrealized_min"] = round(
                        min(stage3_unrealized_samples), 3
                    )
                except Exception:
                    pass
            if stage3_mfe_samples:
                try:
                    stage3_summary["mfe_avg"] = round(
                        sum(stage3_mfe_samples) / len(stage3_mfe_samples), 3
                    )
                except Exception:
                    pass
            if stage3_mae_samples:
                try:
                    stage3_summary["mae_avg"] = round(
                        sum(stage3_mae_samples) / len(stage3_mae_samples), 3
                    )
                except Exception:
                    pass
            if stage3_trail_total:
                stage3_summary["trails_total"] = int(stage3_trail_total)
                stage3_summary["trails_armed"] = int(stage3_trail_armed)
            if stage3_summary:
                analytics_summary["stage3"] = stage3_summary

        # counters для хедера (+ базові агрегати Stage3‑гейтів)
        alerts_list = [
            a
            for a in serialized_assets
            if str(a.get("signal", "")).upper().startswith("ALERT")
        ]
        htf_blocks = 0
        lowatr_blocks = 0
        alerts_buy = 0
        alerts_sell = 0
        for a in alerts_list:
            sig = str(a.get("signal", "")).upper()
            if sig == "ALERT_BUY":
                alerts_buy += 1
            elif sig == "ALERT_SELL":
                alerts_sell += 1
            # Оцінка потенційних блоків Stage3: якщо meta доступна
            try:
                meta = (a.get("market_context") or {}).get("meta", {})
                if isinstance(meta, dict):
                    if meta.get("htf_ok") is False:
                        htf_blocks += 1
                    atr_pct = meta.get("atr_pct")
                    low_gate = meta.get("low_gate")
                    if (
                        isinstance(atr_pct, (int, float))
                        and isinstance(low_gate, (int, float))
                        and float(atr_pct) < float(low_gate)
                    ):
                        lowatr_blocks += 1
            except Exception:
                pass
        # Додаткові лічильники (best-effort): скільки згенеровано/пропущено за цикл
        # Якщо state_manager надає ці значення, використаємо їх; інакше не включаємо
        generated_signals = None
        skipped_signals = None
        try:
            generated_signals = getattr(state_manager, "generated_signals", None)
            skipped_signals = getattr(state_manager, "skipped_signals", None)
        except Exception:
            pass

        # counters: агрегати для хедера UI; деякі поля можуть мати float чи dict
        counters: dict[str, Any] = {}
        counters["assets"] = int(len(serialized_assets))
        counters["alerts"] = int(len(alerts_list))
        counters["alerts_buy"] = int(alerts_buy)
        counters["alerts_sell"] = int(alerts_sell)
        counters["htf_blocked"] = int(htf_blocks)
        counters["lowatr_blocked"] = int(lowatr_blocks)
        if isinstance(generated_signals, int):
            counters["generated_signals"] = generated_signals
        if isinstance(skipped_signals, int):
            counters["skipped_signals"] = skipped_signals
        # Додаємо накопичувальні лічильники блокувань / проходжень ALERT (якщо є у state_manager)
        try:
            blocked_lv = getattr(state_manager, "blocked_alerts_lowvol", None)
            blocked_htf = getattr(state_manager, "blocked_alerts_htf", None)
            blocked_lc = getattr(state_manager, "blocked_alerts_lowconf", None)
            blocked_lv_lc = getattr(
                state_manager, "blocked_alerts_lowvol_lowconf", None
            )
            passed_total = getattr(state_manager, "passed_alerts", None)
            downgraded_total = getattr(state_manager, "downgraded_alerts", None)
            if isinstance(blocked_lv, int):
                counters["blocked_alerts_lowvol"] = blocked_lv
            if isinstance(blocked_htf, int):
                counters["blocked_alerts_htf"] = blocked_htf
            if isinstance(blocked_lc, int):
                counters["blocked_alerts_lowconf"] = blocked_lc
            if isinstance(blocked_lv_lc, int):
                counters["blocked_alerts_lowvol_lowconf"] = blocked_lv_lc
            if isinstance(passed_total, int):
                counters["passed_alerts"] = passed_total
            if isinstance(downgraded_total, int):
                counters["downgraded_alerts"] = downgraded_total
        except Exception:
            pass

        if isinstance(core_trades, dict):
            active_trades_ct = core_trades.get("active")
            if isinstance(active_trades_ct, (int, float)):
                counters["active_trades"] = int(active_trades_ct)
            closed_trades_ct = core_trades.get("closed")
            if isinstance(closed_trades_ct, (int, float)):
                counters["closed_trades"] = int(closed_trades_ct)
            interval_label = core_trades.get("interval")
            if isinstance(interval_label, str):
                counters["trade_interval"] = interval_label
            telemetry_block = core_trades.get("telemetry")
            if isinstance(telemetry_block, dict):
                counters["telemetry_trades"] = int(len(telemetry_block))
            exit_counts = core_trades.get("exit_reason_counts")
            if isinstance(exit_counts, dict) and exit_counts:
                counters["exit_reason_counts"] = exit_counts

        if isinstance(core_stats, dict):
            for key in (
                "skipped",
                "skipped_ewma",
                "dynamic_interval",
                "cycle_interval",
                "drift_ratio",
                "pressure",
                "pressure_norm",
                "alpha",
            ):
                value = core_stats.get(key)
                if isinstance(value, (int, float)):
                    counters[key] = value
            last_update_ts = core_stats.get("last_update_ts")
            if isinstance(last_update_ts, (int, float)):
                counters["core_last_update_ts"] = float(last_update_ts)
            # Нові блоки: perf/system від Stage2 агрегації
            try:
                perf_block = core_stats.get("perf")
                if isinstance(perf_block, dict):
                    avg_wall_ms = perf_block.get("avg_wall_ms")
                    samples = perf_block.get("samples")
                    if isinstance(avg_wall_ms, (int, float)):
                        counters["avg_wall_ms"] = round(float(avg_wall_ms), 1)
                    if isinstance(samples, (int, float)):
                        counters["wall_samples"] = int(samples)
                system_block = core_stats.get("system")
                if isinstance(system_block, dict):
                    sys_cpu = system_block.get("system_cpu_percent")
                    proc_cpu = system_block.get("process_cpu_percent")
                    mem_pct = system_block.get("mem_percent")
                    if isinstance(sys_cpu, (int, float)):
                        counters["sys_cpu_pct"] = round(float(sys_cpu), 1)
                    if isinstance(proc_cpu, (int, float)):
                        counters["proc_cpu_pct"] = round(float(proc_cpu), 1)
                    if isinstance(mem_pct, (int, float)):
                        counters["mem_pct"] = round(float(mem_pct), 1)
            except Exception:
                pass
            thresholds_block = core_stats.get("thresholds")
            if isinstance(thresholds_block, dict):
                drift_high = thresholds_block.get("drift_high")
                drift_low = thresholds_block.get("drift_low")
                pressure_thr = thresholds_block.get("pressure")
                if isinstance(drift_high, (int, float)):
                    counters["th_drift_high"] = float(drift_high)
                if isinstance(drift_low, (int, float)):
                    counters["th_drift_low"] = float(drift_low)
                if isinstance(pressure_thr, (int, float)):
                    counters["th_pressure"] = float(pressure_thr)
            consecutive_block = core_stats.get("consecutive")
            if isinstance(consecutive_block, dict):
                drift_seq = consecutive_block.get("drift_high")
                pressure_seq = consecutive_block.get("pressure_high")
                if isinstance(drift_seq, int):
                    counters["consec_drift_high"] = drift_seq
                if isinstance(pressure_seq, int):
                    counters["consec_pressure_high"] = pressure_seq
            skip_reasons_block = core_stats.get("skip_reasons")
            if isinstance(skip_reasons_block, dict) and skip_reasons_block:
                counters["skip_reasons"] = skip_reasons_block

        # Fallback: якщо core:stats відсутній або не містить system-блоку,
        # добираємо миттєві метрики навантаження тут (best-effort),
        # щоб заголовок UI завжди показував CPU/MEM навіть у періоди без ALERT.
        try:
            need_sys = (
                "sys_cpu_pct" not in counters
                or "proc_cpu_pct" not in counters
                or "mem_pct" not in counters
            )
            if need_sys:
                import psutil  # type: ignore

                proc = psutil.Process()
                if "sys_cpu_pct" not in counters:
                    counters["sys_cpu_pct"] = round(
                        float(psutil.cpu_percent(interval=None)), 1
                    )
                if "proc_cpu_pct" not in counters:
                    counters["proc_cpu_pct"] = round(
                        float(proc.cpu_percent(interval=None)), 1
                    )
                if "mem_pct" not in counters:
                    counters["mem_pct"] = round(
                        float(psutil.virtual_memory().percent), 1
                    )
        except Exception:
            pass

        if stage3_rr_samples:
            try:
                counters["avg_rr"] = round(
                    sum(stage3_rr_samples) / len(stage3_rr_samples), 3
                )
            except Exception:
                pass
        if stage3_unrealized_samples:
            try:
                counters["avg_unrealized"] = round(
                    sum(stage3_unrealized_samples) / len(stage3_unrealized_samples),
                    3,
                )
            except Exception:
                pass
        if stage3_trail_total:
            counters["trails_total"] = int(stage3_trail_total)
            counters["trails_armed"] = int(stage3_trail_armed)

        # Confidence перцентилі (best-effort) — окремо від counters (щоб counters залишались int-only для сумісності)
        confidence_stats: dict[str, float] | None = None
        try:
            samples = getattr(state_manager, "conf_samples", [])
            if isinstance(samples, list) and len(samples) >= 5:
                import math

                sorted_vals = [v for v in samples if isinstance(v, (int, float))]
                sorted_vals.sort()
                if sorted_vals:

                    def _pct(p: float) -> float:
                        k = (len(sorted_vals) - 1) * p
                        f = math.floor(k)
                        c = math.ceil(k)
                        if f == c:
                            return float(sorted_vals[int(k)])
                        d0 = sorted_vals[f] * (c - k)
                        d1 = sorted_vals[c] * (k - f)
                        return float(d0 + d1)

                    confidence_stats = {
                        "p50": round(_pct(0.50), 3),
                        "p75": round(_pct(0.75), 3),
                        "p90": round(_pct(0.90), 3),
                        "count": float(len(sorted_vals)),  # для дебагу/контексту
                    }
        except Exception:
            confidence_stats = None

        # Нормалізуємо символи для UI (єдиний формат UPPER)
        for a in serialized_assets:
            if isinstance(a, dict) and "symbol" in a:
                try:
                    a["symbol"] = str(a["symbol"]).upper()
                except Exception:  # broad except: upper-case sanitation
                    pass

        # Оновлюємо sequence (проста монотонність у межах процесу)
        global _SEQ
        _SEQ = (_SEQ + 1) if _SEQ < 2**31 - 1 else 1

        payload = {
            "type": REDIS_CHANNEL_ASSET_STATE,
            "meta": {
                "ts": datetime.utcnow().isoformat() + "Z",
                "seq": _SEQ,
                "schema_version": UI_PAYLOAD_SCHEMA_VERSION,
            },
            "counters": counters,
            "assets": serialized_assets,
        }
        if analytics_summary:
            payload["analytics"] = analytics_summary
        if confidence_stats:
            payload["confidence_stats"] = confidence_stats

        try:
            if serialized_assets:
                first_keys = list(serialized_assets[0].keys())
            else:
                first_keys = []
            logger.debug(
                "Publish payload counters=%s assets_len=%d first_asset_keys=%s",
                counters,
                len(serialized_assets),
                first_keys,
            )
        except Exception:
            pass

        # Компактна серіалізація (менше пробілів) для зменшення трафіку
        payload_json = json.dumps(payload, default=str, separators=(",", ":"))
        # Вибір namespace для публікації (PR6): v1 або v2, та опційний dual‑publish
        # Зчитуємо фіче-флаги один раз (можливий override через ENV поза тестами)
        use_v2 = bool(UI_USE_V2_NAMESPACE)
        dual_publish = bool(UI_DUAL_PUBLISH)

        primary_snapshot = REDIS_SNAPSHOT_UI_KEY if use_v2 else REDIS_SNAPSHOT_KEY
        primary_channel = (
            REDIS_CHANNEL_UI_ASSET_STATE if use_v2 else REDIS_CHANNEL_ASSET_STATE
        )
        secondary_snapshot = REDIS_SNAPSHOT_KEY if use_v2 else REDIS_SNAPSHOT_UI_KEY
        secondary_channel = (
            REDIS_CHANNEL_ASSET_STATE if use_v2 else REDIS_CHANNEL_UI_ASSET_STATE
        )

        # ── Lightweight per‑symbol state publish (best‑effort, не ламає контракти UI) ──
        # Пишемо компактні поля для швидких live‑перевірок у ai_one:state:{symbol}
        try:
            hset = getattr(redis_conn, "hset", None)
            expire = getattr(redis_conn, "expire", None)
            for a in serialized_assets:
                if not isinstance(a, dict):
                    continue
                sym = str(a.get("symbol") or "").upper()
                if not sym:
                    continue
                stats_blk = a.get("stats") if isinstance(a.get("stats"), dict) else {}
                mc_blk = (
                    a.get("market_context")
                    if isinstance(a.get("market_context"), dict)
                    else {}
                )
                meta_blk = (
                    mc_blk.get("meta") if isinstance(mc_blk.get("meta"), dict) else {}
                )
                phase_blk = (
                    stats_blk.get("phase")
                    if isinstance(stats_blk.get("phase"), dict)
                    else {}
                )

                phase_name = (
                    str(phase_blk.get("name") or phase_blk.get("phase") or "").strip()
                    or None
                )
                band_pct = (
                    a.get("band_pct")
                    if isinstance(a.get("band_pct"), (int, float))
                    else stats_blk.get("band_pct")
                )
                vol_z = (
                    stats_blk.get("vol_z")
                    if isinstance(stats_blk.get("vol_z"), (int, float))
                    else stats_blk.get("volume_z")
                )
                dvr = stats_blk.get("directional_volume_ratio")
                cd = stats_blk.get("cumulative_delta")
                atr_ratio = stats_blk.get("atr_ratio")
                near_edge = stats_blk.get("near_edge")
                htf_ok = a.get("htf_ok")
                if not isinstance(htf_ok, bool):
                    hov = meta_blk.get("htf_ok") if isinstance(meta_blk, dict) else None
                    htf_ok = bool(hov) if isinstance(hov, bool) else None
                htf_strength = (
                    meta_blk.get("htf_strength") if isinstance(meta_blk, dict) else None
                )

                state_key = build_key(NAMESPACE, "state", symbol=sym)
                mapping = {
                    "phase": phase_name,
                    "band_pct": band_pct,
                    "vol_z": vol_z,
                    "vol:z": vol_z,  # сумісність зі старими читачами
                    "dvr": dvr,
                    "cd": cd,
                    "htf:ok": (
                        1 if htf_ok is True else (0 if htf_ok is False else None)
                    ),
                    "htf:strength": htf_strength,
                    "atr_ratio": atr_ratio,
                    "near_edge": near_edge,
                }
                # Опційно додаємо compact market_context.meta як JSON у поле "market_context"
                if (
                    STATE_PUBLISH_META_ENABLED
                    and isinstance(meta_blk, dict)
                    and meta_blk
                ):
                    try:
                        # Відібрані ключі meta для спостереження (уникаємо зайвого шуму)
                        allowed_keys = {
                            "liquidity_sweep_hint",
                            "sweep_then_breakout",
                            "sweep_reject",
                            "retest_ok",
                            "bias",
                            "btc_regime",
                            "btc_htf_strength",
                            "htf_strength",
                        }
                        meta_filtered = {
                            k: v for k, v in meta_blk.items() if k in allowed_keys
                        }
                        if meta_filtered:
                            mapping["market_context"] = json.dumps(
                                {"meta": meta_filtered}, ensure_ascii=False
                            )
                    except Exception:
                        pass
                # Додаємо сценарій (якщо доступний у зібраному asset або meta)
                try:
                    scn_name = a.get("scenario_detected")
                    scn_conf = a.get("scenario_confidence")
                    if scn_name is None and isinstance(meta_blk, dict):
                        scn_name = meta_blk.get("scenario_detected")
                    if scn_conf is None and isinstance(meta_blk, dict):
                        scn_conf = meta_blk.get("scenario_confidence")
                    if scn_name is not None:
                        mapping["scenario_detected"] = scn_name
                    if scn_conf is not None:
                        mapping["scenario_confidence"] = scn_conf
                except Exception:
                    pass
                # Приберемо None, щоб не засмічувати хеш
                mapping = {k: v for k, v in mapping.items() if v is not None}
                try:
                    if callable(hset) and mapping:
                        # redis-py: hset(name, mapping={...})
                        await hset(state_key, mapping=mapping)
                        if callable(expire):
                            await expire(state_key, int(UI_SNAPSHOT_TTL_SEC))
                except Exception:
                    # best‑effort: не ламаємо основну публікацію
                    pass
        except Exception:
            # не критично; пропускаємо, якщо клієнт не підтримує hset/expire
            pass

        # Спочатку snapshot → потім publish (щоб listener мав консистентний снапшот)
        async def _set_with_ttl(key: str) -> None:
            """Записати снапшот із TTL одним атомарним викликом, якщо можливо.

            Порядок спроб:
            1) setex(key, ttl, value) якщо доступно у клієнта
            2) fallback: set(key, value) → expire(key, ttl)
            """
            try:
                setex = getattr(redis_conn, "setex", None)
                if callable(setex):
                    # Атомарний запис із TTL (мінус один RTT)
                    await setex(key, UI_SNAPSHOT_TTL_SEC, payload_json)
                    return
                # Fallback: окремі виклики (сумісність зі старими клієнтами)
                await redis_conn.set(key, payload_json)
                try:
                    await redis_conn.expire(key, UI_SNAPSHOT_TTL_SEC)  # type: ignore[attr-defined]
                except Exception:
                    pass
            except Exception:
                logger.debug("Не вдалося записати snapshot key=%s", key, exc_info=True)

        await _set_with_ttl(primary_snapshot)
        if dual_publish:
            await _set_with_ttl(secondary_snapshot)

        # Smart-publish: публікуємо в канал лише при зміні суттєвих полів
        # або по мінімальному інтервалу як heartbeat.
        sig = _build_signature(counters, serialized_assets)
        now = time.monotonic()
        min_interval_sec = max(0.0, float(UI_PUBLISH_MIN_INTERVAL_MS) / 1000.0)
        allowed_by_time = (
            _LAST_PUB_TS is None or (now - _LAST_PUB_TS) >= min_interval_sec
        )
        allowed_by_change = (_LAST_SIG is None) or (_LAST_SIG != sig)

        should_publish = True
        if UI_SMART_PUBLISH_ENABLED:
            should_publish = allowed_by_change or allowed_by_time

        if not should_publish:
            logger.debug(
                "Smart-publish: пропущено публікацію (без змін, у межах інтервалу)"
            )
            return

        # Публікуємо у основний канал та, за потреби, в обидва
        await redis_conn.publish(primary_channel, payload_json)
        if dual_publish:
            try:
                await redis_conn.publish(secondary_channel, payload_json)
            except Exception:
                logger.debug(
                    "Dual publish у %s не вдався", secondary_channel, exc_info=True
                )

        # Оновлюємо маркери smart‑publish
        _LAST_PUB_TS = now
        _LAST_SIG = sig

        logger.debug(f"✅ Опубліковано стан {len(serialized_assets)} активів")

    except Exception as e:  # broad except: публікація best-effort
        logger.error(f"Помилка публікації стану: {str(e)}")


__all__ = ["publish_full_state"]
# -*- coding: utf-8 -*-
