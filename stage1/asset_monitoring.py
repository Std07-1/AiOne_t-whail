"""Stage1 моніторинг потокових барів (1m/5m) та генерація сирих сигналів.

Шлях: ``stage1/asset_monitoring.py``

Призначення:
    • підтримка інкрементальної статистики (RSI, VWAP, ATR, VolumeZ);
    • агрегація тригерів (volume / breakout / volatility / RSI / VWAP deviation);
    • нормалізація причин (`normalize_trigger_reasons`) і формування сигналу ALERT/NORMAL.

Особливості:
    • lazy ініціалізація порогів (Redis / дефолти);
    • динамічні RSI пороги (over/under) із історії;
    • можливість каліброваних параметрів через state_manager.
"""

import asyncio
import datetime as dt
import json
import logging
import math
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from app.thresholds import Thresholds, load_thresholds
from config.config import (  # додано USE_RSI_DIV, USE_VWAP_DEVIATION
    DIRECTIONAL_PARAMS,
    HEAVY_COMPUTE_GATING_ENABLED,
    INTERVAL_TTL_MAP,
    K_CUMULATIVE_DELTA,
    K_DIRECTIONAL_VOLUME_RATIO,
    K_PRICE_SLOPE_ATR,
    K_SIGNAL,
    K_STATS,
    K_SYMBOL,
    K_TRIGGER_REASONS,
    LOW_ATR_SPIKE_OVERRIDE,
    STAGE1_BEARISH_REASON_BONUS,
    STAGE1_BEARISH_TRIGGER_TAGS,
    STAGE1_MONITOR_PARAMS,
    STAGE1_TRAP,
    STAGE1_TRAP_ENABLED,
    STAGE1_TRAP_INFLUENCE_ENABLED,
    STAGE1_TRAP_MARK_STRONG,
    STAGE1_TRAP_STRONG_VOLZ_THR,
    STAGE2_HTF_OFF_THRESH,
    STAGE2_HTF_ON_THRESH,
    STAGE2_HTF_STRENGTH_ALPHA,
    STAGE2_VOLATILITY_REGIME,
    STRICT_LOW_ATR_OVERRIDE_ON_SPIKE,
    USE_VOL_ATR,
)
from config.flags import (
    ENABLE_TRAP_DETECTOR,
    STAGE1_EXCLUDE_LOW_VOL_ATR_TRIGGERS,
    STAGE1_TRIGGER_BREAKOUT_ENABLED,
    STAGE1_TRIGGER_RSI_ENABLED,
    STAGE1_TRIGGER_VOLATILITY_SPIKE_ENABLED,
    STAGE1_TRIGGER_VOLUME_SPIKE_ENABLED,
    STAGE1_TRIGGER_VWAP_DEVIATION_ENABLED,
    TRAP_COOLDOWN_OVERRIDE_ENABLED,
)
from stage1.asset_triggers import (
    breakout_level_trigger,
    rsi_divergence_trigger,
    volatility_spike_trigger,
    volume_spike_trigger,
)
from stage1.indicators import (
    ATRManager,
    RSIManager,
    VolumeZManager,
    VWAPManager,
    format_rsi,
    vwap_deviation_trigger,
)
from stage1.trap_detector import detect_trap_signals
from utils.phase_adapter import detect_phase_from_stats
from utils.range_edges import compute_range_edges
from utils.utils import normalize_trigger_reasons
from utils.volatility_adapter import compute_vol_regime_from_df

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.stage1.asset_monitoring")
if not logger.handlers:  # guard від подвійного підключення
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


class AssetMonitorStage1:
    """
    Stage1: Моніторинг крипто-активів у реальному часі на основі WS-барів.
    Основні тригери:
      • Сплеск обсягу (volume_z)
      • Динамічний RSI (overbought/oversold)
      • Локальні рівні підтримки/опору
      • VWAP
      • ATR-коридор (волатильність)
    """

    def __init__(
        self,
        cache_handler: Any,
        state_manager: Any = None,
        *,
        vol_z_threshold: float | None = None,
        rsi_overbought: float | None = None,
        rsi_oversold: float | None = None,
        dynamic_rsi_multiplier: float | None = None,
        min_reasons_for_alert: int | None = None,
        enable_stats: bool = True,
        feature_switches: dict | None = None,
        on_alert: Any | None = None,
    ):
        self.cache_handler = cache_handler
        # Use config defaults when explicit args are not provided
        cfg = STAGE1_MONITOR_PARAMS or {}
        self.vol_z_threshold = (
            float(vol_z_threshold)
            if vol_z_threshold is not None
            else float(cfg.get("vol_z_threshold", 2.0))
        )
        self.rsi_manager = RSIManager(period=14)
        self.atr_manager = ATRManager(period=14)
        self.vwap_manager = VWAPManager(window=30)
        self.volumez_manager = VolumeZManager(window=20)
        self.global_levels: dict[str, list[float]] = {}
        self.rsi_overbought = (
            float(rsi_overbought)
            if rsi_overbought is not None
            else cfg.get("rsi_overbought")
        )
        self.rsi_oversold = (
            float(rsi_oversold) if rsi_oversold is not None else cfg.get("rsi_oversold")
        )
        self.dynamic_rsi_multiplier = (
            float(dynamic_rsi_multiplier)
            if dynamic_rsi_multiplier is not None
            else float(cfg.get("dynamic_rsi_multiplier", 1.1))
        )
        self.min_reasons_for_alert = (
            int(min_reasons_for_alert)
            if min_reasons_for_alert is not None
            else int(cfg.get("min_reasons_for_alert", 2))
        )
        self.enable_stats = enable_stats
        self.asset_stats: dict[str, dict[str, Any]] = {}
        self._symbol_cfg: dict[str, Thresholds] = {}
        self.state_manager = state_manager
        # Статистики для anti-spam/визначення частоти тригерів можна додати тут, якщо потрібно
        self.feature_switches = feature_switches or {}
        self._sw_triggers = self.feature_switches.get("triggers") or {}
        # Stage2 trigger callback (async function expected). Signature: (signal: dict) -> Awaitable[None]
        self._on_alert_cb = on_alert
        # Службові маркери для дедуплікації обробки барів
        self._last_processed_last_ts: dict[str, float] = {}
        # Пер-символьні замки для реактивної обробки
        # Per-symbol reactive lock to avoid overlapping processing
        self._locks: dict[str, asyncio.Lock] = {}
        # Тогл для OR-гілки Vol/ATR у volume_spike
        self.use_vol_atr: bool = USE_VOL_ATR
        self._bearish_bonus_enabled: bool = bool(STAGE1_BEARISH_REASON_BONUS)
        self._bearish_tags = frozenset(STAGE1_BEARISH_TRIGGER_TAGS)
        # TRAP кулдаун по символу (мс)
        self.last_trap_ts = {}
        # TTL-кеш для override low_atr gate після різкого сплеску
        self._low_atr_override_state: dict[str, dict[str, Any]] = {}

        # Можливий оверрайд через feature_switches
        sw = (feature_switches or {}).get("volume_spike", {})
        if isinstance(sw, dict) and "use_vol_atr" in sw:
            self.use_vol_atr = bool(sw["use_vol_atr"])

        logger.debug("[Stage1] use_vol_atr=%s", self.use_vol_atr)

    def _is_trigger_enabled(self, name: str) -> bool:
        """Визначає, чи дозволений тригер з урахуванням глобальних прапорів і локальних оверрайдів.

        Пріоритет: feature_switches["triggers"][name] (якщо bool) > глобальні STAGE1_TRIGGER_*.
        Невідомі назви тригерів вважаються дозволеними (defensive default).
        """
        override = self._sw_triggers.get(name)
        if isinstance(override, bool):
            return override
        mapping: dict[str, bool] = {
            "volume_spike": bool(STAGE1_TRIGGER_VOLUME_SPIKE_ENABLED),
            "breakout": bool(STAGE1_TRIGGER_BREAKOUT_ENABLED),
            "volatility_spike": bool(STAGE1_TRIGGER_VOLATILITY_SPIKE_ENABLED),
            "rsi": bool(STAGE1_TRIGGER_RSI_ENABLED),
            "vwap_deviation": bool(STAGE1_TRIGGER_VWAP_DEVIATION_ENABLED),
            "exclude_low_vol_atr": bool(STAGE1_EXCLUDE_LOW_VOL_ATR_TRIGGERS),
        }
        return mapping.get(name, True)

    async def _update_low_atr_override(
        self, symbol: str, stats: dict[str, Any]
    ) -> tuple[bool, dict[str, Any] | None]:
        """Оновлює TTL-стан override low_atr gate, якщо зафіксовано агресивний сплеск.

        Реалізовано як async щоб за можливості записати TTL у Redis (write-through).
        Якщо Redis не доступний — працюємо лише в пам'яті.
        """

        if not STRICT_LOW_ATR_OVERRIDE_ON_SPIKE:
            self._low_atr_override_state.pop(symbol, None)
            return False, None

        cfg = dict(LOW_ATR_SPIKE_OVERRIDE or {})

        def _finite(val: Any) -> float | None:
            try:
                f = float(val)
            except (TypeError, ValueError):
                return None
            return f if math.isfinite(f) else None

        band_expand_min = _finite(cfg.get("band_expand_min")) or 0.0
        spike_ratio_min = _finite(cfg.get("spike_ratio_min")) or 0.0
        abs_volz_min = _finite(cfg.get("abs_volz_min")) or 0.0
        dvr_min = _finite(cfg.get("dvr_min")) or 0.0
        bars_ttl = int(cfg.get("bars_ttl", 0) or 0)

        band_expand_val = _finite(stats.get("band_expand"))
        atr_spike_val = _finite(stats.get("atr_spike_ratio"))
        volz_val = _finite(stats.get("vol_z"))
        dvr_val = _finite(stats.get("dvr"))
        abs_volz_val = abs(volz_val) if volz_val is not None else None
        dvr_magnitude = None
        if dvr_val is not None and dvr_val > 0:
            inv = 1.0 / dvr_val if dvr_val != 0 else 0.0
            dvr_magnitude = max(dvr_val, inv)

        metrics = {
            "band_expand": band_expand_val,
            "band_expand_ratio": _finite(stats.get("band_expand_ratio")),
            "atr_spike_ratio": atr_spike_val,
            "spike_ratio": atr_spike_val,
            "abs_vol_z": abs_volz_val,
            "dvr": dvr_val,
            "dvr_magnitude": dvr_magnitude,
        }

        triggered = (
            band_expand_val is not None
            and band_expand_val >= band_expand_min
            and atr_spike_val is not None
            and atr_spike_val >= spike_ratio_min
            and abs_volz_val is not None
            and abs_volz_val >= abs_volz_min
            and dvr_magnitude is not None
            and dvr_magnitude >= max(1.0, dvr_min)
        )

        entry = self._low_atr_override_state.get(symbol)
        if triggered:
            direction = None
            if dvr_val is not None:
                direction = "SELL" if dvr_val >= 1.0 else "BUY"
            if volz_val is not None:
                direction = "SELL" if volz_val < 0 else "BUY"
            now_ms = int(time.time() * 1000)
            entry = {
                "ttl": max(0, bars_ttl),
                "metrics": metrics,
                "direction": direction,
                "activated_ts": now_ms,
                "last_ts": now_ms,
                "reason": "spike_detected",
            }
            self._low_atr_override_state[symbol] = entry
            try:
                logger.info(
                    "[STRICT_GUARD] %s low_atr_override armed metrics=%s",
                    symbol,
                    metrics,
                )
            except Exception:
                pass
            # Best-effort: persist TTL into Redis if cache is available
            try:
                redis_obj = getattr(self.cache_handler, "redis", None)
                jset = (
                    getattr(redis_obj, "jset", None) if redis_obj is not None else None
                )
                if (
                    callable(jset)
                    and bars_ttl
                    and isinstance(bars_ttl, int)
                    and bars_ttl > 0
                ):
                    interval_sec = int((INTERVAL_TTL_MAP or {}).get("1m", 60))
                    ttl_sec = max(1, interval_sec * int(bars_ttl))
                    # store under logical path: phase.low_atr_override.<symbol>
                    try:
                        await jset(
                            "phase",
                            "low_atr_override",
                            symbol.lower(),
                            value=entry,
                            ttl=ttl_sec,
                        )
                    except Exception:
                        # swallow errors — best-effort only
                        pass
            except Exception:
                # best-effort only: do not break Stage1 if cache access fails
                pass
        elif entry is None:
            return False, None

        entry = self._low_atr_override_state.get(symbol)
        if not entry:
            return False, None
        ttl = int(entry.get("ttl", 0))
        if ttl <= 0:
            self._low_atr_override_state.pop(symbol, None)
            return False, None
        entry["ttl"] = ttl - 1
        entry["last_ts"] = int(time.time() * 1000)
        payload = {
            "active": True,
            "ttl": ttl,
            "metrics": entry.get("metrics", {}),
            "direction": entry.get("direction"),
            "activated_ts": entry.get("activated_ts"),
            "last_ts": entry.get("last_ts"),
            "reason": entry.get("reason"),
            "triggered_now": triggered,
        }
        return True, payload

    async def _append_stage1_jsonl(self, record: dict[str, Any]) -> None:
        """Best‑effort запис рядка у stage1_signals.jsonl (без винятків назовні).

        Пише у TELEMETRY_BASE_DIR / TELEMETRY_DOMAIN_FILES["stage1_signal"].
        Якщо конфіг недоступний — fallback на ./telemetry/stage1_signals.jsonl.

        Args:
            record: Словник з подією/сигналом Stage1 для офлайн‑аналізу.
        """
        try:  # конфіг для шляхів
            from config.config import (  # type: ignore
                TELEMETRY_BASE_DIR,
                TELEMETRY_DOMAIN_FILES,
            )

            base_dir = TELEMETRY_BASE_DIR
            stage1_file = TELEMETRY_DOMAIN_FILES.get(
                "stage1_signal", "stage1_signals.jsonl"
            )
        except Exception:
            base_dir = "./telemetry"
            stage1_file = "stage1_signals.jsonl"

        out_path = Path(base_dir) / stage1_file
        out_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            line = json.dumps(record, ensure_ascii=False) + "\n"
        except Exception:
            # Не провалюємо пайплайн через серіалізацію — тихо пропускаємо
            return

        def _write() -> None:
            with out_path.open("a", encoding="utf-8") as f:
                f.write(line)

        try:
            await asyncio.to_thread(_write)
        except Exception:
            # В разі збою запису — не блокуємо Stage1
            return

    def _detect_market_state(self, symbol: str, stats: dict[str, Any]) -> str | None:
        """Грубе евристичне визначення стану ринку.

        Повертає один з: "range_bound" | "trend_strong" | "high_volatility" | None

        Heuristics (мінімально інвазивно):
          - high_volatility: ATR% > high_gate
          - range_bound: ATR% < low_gate і |price_change| < 1%
          - trend_strong: |price_change| >= 2% або RSI далеко від 50 (>|60| або <|40|)
        """
        try:
            price = float(stats.get("current_price") or 0.0)
            atr = float(stats.get("atr") or 0.0)
            price_change = float(stats.get("price_change") or 0.0)
            rsi = float(stats.get("rsi") or 50.0)
            thr = self._symbol_cfg.get(symbol)
            low_gate = getattr(thr, "low_gate", 0.0035) if thr else 0.0035
            high_gate = getattr(thr, "high_gate", 0.015) if thr else 0.015
            atr_pct = (atr / price) if price else 0.0
            if atr_pct > high_gate:
                return "high_volatility"
            if atr_pct < low_gate and abs(price_change) < 0.01:
                return "range_bound"
            if abs(price_change) >= 0.02 or rsi >= 60 or rsi <= 40:
                return "trend_strong"
        except (
            TypeError,
            ValueError,
            ZeroDivisionError,
        ) as exc:  # broad except: stats можуть бути неповними
            logger.debug(
                f"[{symbol}] Не вдалося визначити ринковий стан: {exc}", exc_info=True
            )
            return None
        return None

    def update_params(
        self,
        vol_z_threshold: float | None = None,
        rsi_overbought: float | None = None,
        rsi_oversold: float | None = None,
    ) -> None:
        """
        Оновлює параметри монітора під час бектесту
        """
        if vol_z_threshold is not None:
            self.vol_z_threshold = vol_z_threshold
        if rsi_overbought is not None:
            self.rsi_overbought = rsi_overbought
        if rsi_oversold is not None:
            self.rsi_oversold = rsi_oversold

        logger.debug(
            f"Оновлено параметри Stage1: vol_z={vol_z_threshold}, "
            f"rsi_ob={rsi_overbought}, rsi_os={rsi_oversold}"
        )

    async def ensure_symbol_cfg(self, symbol: str) -> Thresholds:
        """
        Завантажує індивідуальні пороги (з Redis або дефолтні).
        Додає захист від ситуації, коли замість Thresholds приходить рядок (наприклад, symbol).
        """
        import traceback

        if symbol not in self._symbol_cfg:
            thr = await load_thresholds(symbol, self.cache_handler)
            # Захист: якщо thr — це рядок, а не Thresholds
            if isinstance(thr, str):
                logger.error(
                    f"[{symbol}] load_thresholds повернув рядок замість Thresholds: {thr}"
                )
                logger.error(traceback.format_stack())
                raise TypeError(
                    f"[{symbol}] load_thresholds повернув рядок замість Thresholds: {thr}"
                )
            if thr is None:
                logger.warning(
                    f"[{symbol}] Не знайдено порогів у Redis, використовую стандартні"
                )
                thr = Thresholds(symbol=symbol, config={})
            self._symbol_cfg[symbol] = thr
            logger.debug(
                f"[{symbol}] Завантажено пороги: {getattr(thr, 'to_dict', lambda: thr)()}"
            )
        return self._symbol_cfg[symbol]

    async def update_statistics(
        self,
        symbol: str,
        df: pd.DataFrame,
    ) -> dict[str, Any]:
        """
        Оновлення базових метрик для швидкого моніторингу (1m/5m, максимум 1-3 години).
        Забезпечує стандартизацію формату, коректний розрахунок RSI (інкрементально),
        крос-метрики для UI та тригерів.
        """
        # Не виконуємо конвертацію часу: працюємо з наданим df як є
        if df.empty:
            raise ValueError(f"[{symbol}] Передано порожній DataFrame для статистики!")

        # 2. Основні ціни/зміни
        price = df["close"].iloc[-1]
        first = df["close"].iloc[0]
        price_change = (price / first - 1) if first else 0.0

        # 3. Денні high/low/range з цього ж df
        daily_high = df["high"].max()
        daily_low = df["low"].min()
        daily_range = daily_high - daily_low

        # 4. Volume statistics (з урахуванням NaN / коротких вікон)
        vol_series = pd.to_numeric(df["volume"], errors="coerce")
        latest_vol = vol_series.iloc[-1] if len(vol_series) else 0.0
        clean_vol = vol_series.dropna()
        if len(clean_vol) < 2:
            vol_mean = float(clean_vol.mean()) if len(clean_vol) else 0.0
            vol_std = 1.0
            volume_z = 0.0
        else:
            vol_mean = float(clean_vol.mean())
            vol_std = float(clean_vol.std(ddof=0)) or 1.0
            volume_z = 0.0 if pd.isna(latest_vol) else (latest_vol - vol_mean) / vol_std

        # 5. RSI (інкрементально) O(1) (RAM-fast)
        self.rsi_manager.ensure_state(symbol, df["close"])  # на всяк випадок при старті

        # RSI (RAM-fast, seed-based)
        rsi = self.rsi_manager.update(symbol, price)
        rsi_bar = format_rsi(rsi, symbol=symbol)
        # Уникаємо повного перерахунку RSI кожен раз; беремо історію з менеджера
        rsi_hist = list(self.rsi_manager.history_map.get(symbol, []))
        rsi_s = (
            pd.Series(rsi_hist[-min(len(rsi_hist), 120) :])
            if rsi_hist
            else pd.Series([rsi])
        )

        # 6. VWAP (інкрементально) (FIFO)
        # seed-буфер із всіх, крім останнього бару
        # ініціалізація буфера відбувається лише якщо він відсутній (без перезаливки кожен крок)
        self.vwap_manager.ensure_buffer(symbol, df.iloc[:-1])
        # додаємо новий бар у буфер
        volume = df["volume"].iloc[-1]
        self.vwap_manager.update(symbol, price, volume)
        # 3) розраховуємо VWAP вже по оновленому буферу
        vwap = self.vwap_manager.compute_vwap(symbol)

        # 7. ATR (інкрементально) (O(1)!) з захистом ініціалізації стану
        self.atr_manager.ensure_state(symbol, df)
        high = df["high"].iloc[-1]
        low = df["low"].iloc[-1]
        close = df["close"].iloc[-1]
        try:
            atr = self.atr_manager.update(symbol, high, low, close)
        except KeyError:
            # Fallback: одноразово ініціалізуємо стан за допомогою векторного ATR
            try:
                from stage1.indicators.atr_indicator import compute_atr  # local import

                seed_window = max(self.atr_manager.period + 1, 20)
                atr_val = float(
                    compute_atr(df.tail(seed_window), self.atr_manager.period, symbol)
                )
                if atr_val != atr_val:  # NaN check
                    atr_val = 0.0
                # встановлюємо стан і повертаємо атр без виклику update ще раз
                self.atr_manager.state_map[symbol] = {  # type: ignore[attr-defined]
                    "atr": float(atr_val),
                    "last_close": float(close),
                }
                atr = atr_val
            except Exception:
                atr = 0.0

        # 8. Volume Z-score (інкрементально) (RAM-fast)
        # ініціалізація буфера лише за потреби (без перезаливки)
        self.volumez_manager.ensure_buffer(symbol, df)
        volume = df["volume"].iloc[-1]
        volume_z = self.volumez_manager.update(symbol, volume)

        # 8.1. Напрямкові метрики (observe mode): DVR/CD/slope_atr
        # Вікно коротке W (за замовчуванням 3 бари)
        try:
            w_short = int(max(1, float(DIRECTIONAL_PARAMS.get("w_short", 3))))
        except Exception:
            w_short = 3
        try:
            closes = pd.to_numeric(df["close"], errors="coerce").dropna()
            opens = pd.to_numeric(df["open"], errors="coerce").dropna()
            vols = pd.to_numeric(df["volume"], errors="coerce").dropna()
            # обмежуємо останнє вікно
            c_win = closes.tail(w_short)
            o_win = opens.tail(w_short)
            v_win = vols.tail(w_short)
            up_mask = c_win > o_win
            down_mask = c_win < o_win
            up_vol = float(v_win[up_mask].sum()) if len(v_win) else 0.0
            down_vol = float(v_win[down_mask].sum()) if len(v_win) else 0.0
            total_vol = up_vol + down_vol
            min_total = float(DIRECTIONAL_PARAMS.get("min_total_volume", 1e-6))
            # DVR: обережно для малих обсягів і up_vol≈0
            if total_vol < min_total:
                dvr = 1.0
            else:
                dvr = (down_vol / up_vol) if up_vol > 0 else 2.0
            # Cumulative delta (приблизно, якщо немає тиків): signed volume
            signed = (v_win.where(c_win > o_win, -v_win)).sum() if len(v_win) else 0.0
            denom = float(v_win.sum()) if len(v_win) else 0.0
            cd = float(signed / denom) if denom > 0 else 0.0
            # Price slope в ATR-одиницях
            if len(closes) >= w_short + 1 and atr and atr > 0:
                ref = float(closes.iloc[-w_short - 1])
                slope_atr = float((price - ref) / max(1e-9, atr))
            else:
                slope_atr = 0.0
        except Exception:
            dvr, cd, slope_atr = 1.0, 0.0, 0.0

        # 10. Динамічні пороги RSI
        avg_rsi = rsi_s.mean()

        # Якщо не задані константи, використовуй динаміку
        over = getattr(self, "rsi_overbought", None) or min(
            avg_rsi * getattr(self, "dynamic_rsi_multiplier", 1.25), 90
        )
        under = getattr(self, "rsi_oversold", None) or max(
            avg_rsi / getattr(self, "dynamic_rsi_multiplier", 1.25), 10
        )

        # 11. Зберемо попередній схил для acceleration_detected порівняння
        prev_stats: dict[str, Any] | None = None
        try:
            prev_stats_candidate = (
                self.asset_stats.get(symbol) if hasattr(self, "asset_stats") else None
            )
            if isinstance(prev_stats_candidate, dict):
                prev_stats = prev_stats_candidate
            slope_prev_val = (
                float(prev_stats.get(K_PRICE_SLOPE_ATR))
                if isinstance(prev_stats, dict)
                else 0.0
            )
        except Exception:
            slope_prev_val = 0.0
            prev_stats = None

        # 12. Краї діапазону (best-effort) та близькість до краю
        edges = compute_range_edges(df)
        near_edge = edges.get("near_edge")
        dist_to_edge_pct = edges.get("dist_to_edge_pct")
        band_pct = edges.get("band_pct")
        try:
            logger.info(
                "[EDGES] %s near=%s dist=%.4f band_pct=%.4f",
                symbol,
                str(near_edge),
                (
                    float(dist_to_edge_pct)
                    if isinstance(dist_to_edge_pct, (int, float))
                    else -1.0
                ),
                float(band_pct) if isinstance(band_pct, (int, float)) else -1.0,
            )
        except Exception:
            pass

        band_expand = None
        band_expand_ratio = None
        try:
            prev_band_candidate = (
                float(prev_stats.get("band_pct"))
                if isinstance(prev_stats, dict)
                and isinstance(prev_stats.get("band_pct"), (int, float))
                else None
            )
        except Exception:
            prev_band_candidate = None
        if isinstance(band_pct, (int, float)) and isinstance(
            prev_band_candidate, (int, float)
        ):
            band_expand = float(band_pct) - float(prev_band_candidate)
            if abs(float(prev_band_candidate)) > 1e-9:
                try:
                    band_expand_ratio = (
                        float(band_pct) / float(prev_band_candidate)
                    ) - 1.0
                except Exception:
                    band_expand_ratio = None

        # 13. Визначимо режим волатильності (strict) на основі проксі ATR%
        try:
            vol_meta = compute_vol_regime_from_df(df.tail(150))
            vol_regime_strict = str(vol_meta.get("regime", "normal"))
            atr_ratio = float(vol_meta.get("atr_ratio", 0.0) or 0.0)
            crisis_score = float(vol_meta.get("crisis_vol_score", 0.0) or 0.0)
            atr_spike_ratio = float(vol_meta.get("atr_spike_ratio", 0.0) or 0.0)
            crisis_reason = vol_meta.get("crisis_reason")
            if vol_regime_strict == "crisis":
                try:
                    logger.info(
                        "[STRICT_PHASE] %s volatility crisis regime detected spike=%.2f crisis=%.2f reason=%s",
                        symbol,
                        atr_spike_ratio,
                        crisis_score,
                        crisis_reason,
                    )
                except Exception:
                    pass
        except Exception:
            vol_regime_strict = "normal"
            atr_ratio = 0.0
            crisis_score = 0.0
            atr_spike_ratio = 0.0
            crisis_reason = None

        # 13.1. HTF підтвердження тренду (1h/4h EMA slope) з гістерезисом
        # Обережний, без зовнішнього I/O: використовуємо 1m дані як проксі для 1h/4h
        htf_ok_val: bool | None = None
        htf_score_val: float | None = None
        htf_strength_val: float | None = None
        try:
            closes_1m = pd.to_numeric(df["close"], errors="coerce").dropna()
            # Потрібно хоча б 2 точки для диференціалу; бажано >= span+1
            if len(closes_1m) >= 5:
                # EMA як проксі для HTF (60 і 240 барів)
                ema1h = closes_1m.ewm(span=60, adjust=False).mean()
                ema4h = closes_1m.ewm(span=240, adjust=False).mean()

                # Похідна (наближено) як різниця останніх двох значень
                def _last_diff(s: pd.Series) -> float:
                    if len(s) < 2:
                        return 0.0
                    return float(s.iloc[-1] - s.iloc[-2])

                # Оцінка напрямку HTF: 1.0 (вгору), 0.0 (вниз), 0.5 (плоско)
                d1 = _last_diff(ema1h)
                d4 = _last_diff(ema4h)
                up1 = 1.0 if d1 > 0 else 0.0 if d1 < 0 else 0.5
                up4 = 1.0 if d4 > 0 else 0.0 if d4 < 0 else 0.5
                # 1.0 — обидві вгору; 0.0 — обидві вниз; 0.5 — конфлікт/плоско
                htf_score_val = round((up1 + up4) / 2.0, 4)

                # Сила HTF: відносні схили EMA (нормалізовані), насичення за _HTF_ALPHA
                try:
                    last1 = float(ema1h.iloc[-2]) if len(ema1h) >= 2 else 0.0
                    last4 = float(ema4h.iloc[-2]) if len(ema4h) >= 2 else 0.0
                    rel1 = (d1 / abs(last1)) if last1 else 0.0
                    rel4 = (d4 / abs(last4)) if last4 else 0.0
                    alpha = float(STAGE2_HTF_STRENGTH_ALPHA or 1e-6)
                    comp1 = min(1.0, abs(rel1) / alpha)
                    comp4 = min(1.0, abs(rel4) / alpha)
                    htf_strength_val = round((comp1 + comp4) / 2.0, 4)
                except Exception:
                    htf_strength_val = None

                # Визначення htf_ok з гістерезисом
                prev_htf_ok = None
                try:
                    prev_htf_ok = bool(self.asset_stats.get(symbol, {}).get("htf_ok"))
                except Exception:
                    prev_htf_ok = None

                # Гістерезисні пороги
                on_thr = float(STAGE2_HTF_ON_THRESH)
                off_thr = float(STAGE2_HTF_OFF_THRESH)
                if htf_score_val >= on_thr:
                    htf_ok_val = True
                elif htf_score_val <= off_thr:
                    htf_ok_val = False
                else:
                    # У зоні гістерезису тримаємо попередній стан, якщо був
                    if isinstance(prev_htf_ok, bool):
                        htf_ok_val = prev_htf_ok
                    else:
                        htf_ok_val = None
        except Exception:
            htf_ok_val = None
            htf_score_val = None
            htf_strength_val = None

        # 14. Збираємо всі метрики в один словник для UI і тригерів
        # константи K_* імпортовані на рівні модуля

        stats = {
            "symbol": str(symbol),
            "current_price": float(price),
            "price_change": float(price_change),
            "daily_high": float(daily_high),
            "daily_low": float(daily_low),
            "daily_range": float(daily_range),
            "volume_mean": float(vol_mean),
            "volume_std": float(vol_std),
            "rsi": float(rsi) if rsi is not None else np.nan,
            "rsi_bar": str(rsi_bar),
            "dynamic_overbought": float(over) if over is not None else np.nan,
            "dynamic_oversold": float(under) if under is not None else np.nan,
            "vwap": float(vwap) if vwap is not None else np.nan,
            "atr": float(atr) if atr is not None else np.nan,
            "volume_z": float(volume_z) if volume_z is not None else np.nan,
            # Directional (observe-mode): без впливу на тригери у цьому PR
            K_DIRECTIONAL_VOLUME_RATIO: float(dvr),
            K_CUMULATIVE_DELTA: float(cd),
            K_PRICE_SLOPE_ATR: float(slope_atr),
            "price_slope_atr_prev": float(slope_prev_val),
            "near_edge": near_edge,
            "dist_to_edge_pct": (
                float(dist_to_edge_pct)
                if isinstance(dist_to_edge_pct, (int, float))
                else None
            ),
            "band_pct": float(band_pct) if isinstance(band_pct, (int, float)) else None,
            "band_expand": (
                float(band_expand) if isinstance(band_expand, (int, float)) else None
            ),
            "band_expand_ratio": (
                float(band_expand_ratio)
                if isinstance(band_expand_ratio, (int, float))
                else None
            ),
            "vol_regime_strict": vol_regime_strict,
            "volatility_regime": {
                "regime": vol_regime_strict,
                "atr_ratio": float(atr_ratio),
                "crisis_vol_score": float(crisis_score),
                "atr_spike_ratio": float(atr_spike_ratio),
                "crisis_reason": crisis_reason,
            },
            "atr_ratio": float(atr_ratio),
            "crisis_vol_score": float(crisis_score),
            "atr_spike_ratio": float(atr_spike_ratio),
            # HTF підтвердження тренду (телеметрія‑only)
            "htf_ok": htf_ok_val,
            "htf_score": float(htf_score_val) if htf_score_val is not None else None,
            "htf_strength": (
                float(htf_strength_val) if htf_strength_val is not None else None
            ),
            "last_updated": dt.datetime.now(dt.UTC).isoformat(),
            # Опціонально: можна додати median, quantile, trend, etc.
        }

        # ── Додаткові поля для сумісності зі strict-prefilter ─────────────
        # 1) Оборот у USD за вікном df: сумарно sum(close * volume)
        try:
            _vol = pd.to_numeric(df["volume"], errors="coerce")
            _px = pd.to_numeric(df["close"], errors="coerce")
            turnover_usd = float((_px * _vol).dropna().sum())
        except Exception:
            turnover_usd = 0.0
        stats["turnover_usd"] = turnover_usd

        # 2) Аліаси ключів, які очікує strict-prefilter
        #    (зберігаємо оригінали й додаємо короткі назви)
        try:
            stats.setdefault("vol_z", float(stats.get("volume_z")))
        except Exception:
            stats.setdefault("vol_z", 0.0)
        try:
            stats.setdefault("dvr", float(stats.get(K_DIRECTIONAL_VOLUME_RATIO)))
        except Exception:
            stats.setdefault("dvr", 1.0)
        try:
            stats.setdefault("cd", float(stats.get(K_CUMULATIVE_DELTA)))
        except Exception:
            stats.setdefault("cd", 0.0)
        try:
            stats.setdefault("slope_atr", float(stats.get(K_PRICE_SLOPE_ATR)))
        except Exception:
            stats.setdefault("slope_atr", 0.0)

        # 15. Зберігаємо в кеші монітора та лог
        self.asset_stats[symbol] = stats
        if getattr(self, "enable_stats", False):
            logger.debug(f"[{symbol}] Оновлено статистику: {stats}")
        return stats

    async def check_anomalies(
        self,
        symbol: str,
        df: pd.DataFrame,
        stats: dict[str, Any] | None = None,
        trigger_reasons: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Аналізує основні тригери та формує raw signal.
        Додає захист від ситуації, коли пороги некоректні (наприклад, рядок).
        """
        import traceback

        # Нормалізація mutable default
        if trigger_reasons is None:
            trigger_reasons = []

        # Boundary log: отримано DataFrame для аналізу (лише raw numeric значення)
        try:
            n = len(df)
            if "timestamp" in df.columns:
                t_head = (
                    pd.to_numeric(df["timestamp"], errors="coerce")
                    .astype("Int64")
                    .head(3)
                    .dropna()
                    .astype("int64")
                    .tolist()
                )
                t_tail = (
                    pd.to_numeric(df["timestamp"], errors="coerce")
                    .astype("Int64")
                    .tail(3)
                    .dropna()
                    .astype("int64")
                    .tolist()
                )
                logger.debug(
                    "[Stage1 RECEIVE] %s | rows=%d timestamp head=%s tail=%s",
                    symbol,
                    n,
                    t_head,
                    t_tail,
                )
        except (
            Exception
        ) as exc:  # broad except: діагностичний лог не має зривати аналіз
            logger.debug(
                f"[{symbol}] Не вдалося зібрати timestamp-лог: {exc}", exc_info=True
            )

        # Додатково: лог сирих open_time/close_time як приходять (інт/рядки)
        try:
            if "open_time" in df.columns:
                ot = pd.to_numeric(df["open_time"], errors="coerce").astype("Int64")
                logger.debug(
                    "[check_anomalies] %s | RAW open_time head=%s tail=%s",
                    symbol,
                    ot.head(3).dropna().astype("int64").tolist(),
                    ot.tail(3).dropna().astype("int64").tolist(),
                )
            if "close_time" in df.columns:
                ct = pd.to_numeric(df["close_time"], errors="coerce").astype("Int64")
                logger.debug(
                    "[check_anomalies] %s | RAW close_time head=%s tail=%s",
                    symbol,
                    ct.head(3).dropna().astype("int64").tolist(),
                    ct.tail(3).dropna().astype("int64").tolist(),
                )
        except (
            Exception
        ) as exc:  # broad except: конверсія timestamp може впасти на зіпсованих даних
            logger.debug(
                f"[{symbol}] Неможливо зібрати open/close-time лог: {exc}",
                exc_info=True,
            )

        # Не конвертуємо час — лишаємо raw numeric логіку вище

        # Завжди оновлюємо метрики по новому df
        stats = await self.update_statistics(symbol, df)
        price = stats["current_price"]

        anomalies: list[str] = []
        reasons: list[str] = []
        # Контекстні причини (не впливають на ALERT)
        context_only_reasons: list[str] = []

        # Фіче-флаги керування тригерами (без циклічних імпортів)
        try:
            from config import flags as _flags  # type: ignore

            _qde_disabled = bool(getattr(_flags, "STAGE1_QDE_TRIGGERS_DISABLED", False))
            _struct_prior_only = bool(
                getattr(_flags, "STAGE1_STRUCTURAL_PRIORITIZE_ONLY", False)
            )
            _skip_divergence = bool(
                getattr(_flags, "STAGE1_SKIP_DIVERGENCE_DETECTION", False)
            )
            _skip_vol_spike = bool(
                getattr(_flags, "STAGE1_SKIP_VOLATILITY_SPIKE_DETECTION", False)
            )
            _exclude_low_vol_atr = bool(
                getattr(_flags, "STAGE1_EXCLUDE_LOW_VOL_ATR_TRIGGERS", False)
            )
        except Exception:
            _qde_disabled = False
            _struct_prior_only = False
            _skip_divergence = False
            _skip_vol_spike = False
            _exclude_low_vol_atr = False

        thr = await self.ensure_symbol_cfg(symbol)
        # Захист: якщо thr — це рядок, а не Thresholds
        if isinstance(thr, str):
            logger.error(
                f"[{symbol}] ensure_symbol_cfg повернув рядок замість Thresholds: {thr}"
            )
            logger.error(traceback.format_stack())
            raise TypeError(
                f"[{symbol}] ensure_symbol_cfg повернув рядок замість Thresholds: {thr}"
            )
        logger.debug(
            f"[{symbol}] Пороги: low={thr.low_gate*100:.2f}%, high={thr.high_gate*100:.2f}%"
        )

        # Калібровані параметри видалені — використовуються лише завантажені/дефолтні thresholds

        # Визначаємо стан ринку і ефективні пороги (мінімальні зміни)
        market_state = self._detect_market_state(symbol, stats)
        try:
            effective = thr.effective_thresholds(market_state=market_state)
        except (
            Exception
        ) as exc:  # broad except: fallback на сирі пороги, щоб не втратити сигнал
            logger.debug(
                f"[{symbol}] effective_thresholds fallback: {exc}", exc_info=True
            )
            effective = thr.to_dict()
        logger.debug(
            f"[check_anomalies] {symbol} | Застосовано пороги: "
            f"lg={effective.get('low_gate'):.4f}, hg={effective.get('high_gate'):.4f}, "
            f"volz={effective.get('vol_z_threshold'):.2f}, "
            f"rsi_os={effective.get('rsi_oversold')}, rsi_ob={effective.get('rsi_overbought')}, "
            f"state={market_state}"
        )
        # Інформативний лог на INFO-рівні (нечасто): показати зміну стану
        try:
            # Лог лише коли стан змінюється (зберігаємо попередній у self.asset_stats)
            prev_state = self.asset_stats.get(symbol, {}).get("_market_state")
            if prev_state != market_state:
                logger.debug(
                    "%s Ринковий стан: %s → ефективні пороги: \n"
                    " volZ=%.2f \n"
                    " vwap=%.3f \n"
                    " gates=[%.3f..%.3f] \n",
                    symbol,
                    market_state,
                    float(effective.get("vol_z_threshold", float("nan"))),
                    float(effective.get("vwap_deviation", float("nan"))),
                    float(effective.get("low_gate", float("nan"))),
                    float(effective.get("high_gate", float("nan"))),
                )
            # збережемо стан для наступного порівняння
            self.asset_stats.setdefault(symbol, {})["_market_state"] = market_state
        except (
            Exception
        ) as exc:  # broad except: діагностичний лог не повинен ламати пайплайн
            logger.debug(
                f"[{symbol}] Неможливо оновити кеш ринкового стану: {exc}",
                exc_info=True,
            )

        def _add(reason: str, text: str) -> None:
            anomalies.append(text)
            reasons.append(reason)

        # ————— Перевірка ATR —————
        atr_pct = stats["atr"] / price

        # Ініціалізація змінних
        low_atr_flag = False  # Флаг для визначення, чи ринок спокійний

        over = stats.get("dynamic_overbought", 70)
        under = stats.get("dynamic_oversold", 30)

        # ————— Якщо ATR занадто низький — позначаємо low_atr і готуємо gate
        if atr_pct < thr.low_gate:
            logger.debug(
                f"[{symbol}] ATR={atr_pct:.4f} < поріг low_gate — ринок спокійний, але продовжуємо аналіз.."
            )
            low_atr_flag = True
            if _exclude_low_vol_atr:
                try:
                    logger.info(
                        "[STRICT_GUARD] symbol=%s skip=low_volatility_reason", symbol
                    )
                except Exception:
                    pass
            else:
                _add("low_volatility", "📉 Низька волатильність")

        logger.debug(
            f"[{symbol}] Перевірка тригерів:"
            f" price={price:.4f}"
            f" - ATR={atr_pct:.4f} (поріг low={effective.get('low_gate'):.4f}, high={effective.get('high_gate'):.4f})"
            f" - VolumeZ: {stats['volume_z']:.2f} (поріг {effective.get('vol_z_threshold'):.2f})"
            f" - RSI: {stats['rsi']:.2f} (OB {over:.2f}, OS {under:.2f})"
        )

        # ————— ІНТЕГРАЦІЯ ВСІХ СУЧАСНИХ ТРИГЕРІВ —————
        # 1. Сплеск обсягу (використовуємо виключно Z‑score, vol/atr шлях опційний)
        if self._is_trigger_enabled("volume_spike"):
            volz = float(
                effective.get("vol_z_threshold", getattr(thr, "vol_z_threshold", 2.0))
            )
            # За замовчуванням використовуємо лише Z-score (use_vol_atr=False)
            fired, meta_vs = volume_spike_trigger(
                df,
                z_thresh=volz,
                symbol=symbol,
                use_vol_atr=self.use_vol_atr,
            )
            if fired:
                # Використовуємо метадані тригера (анти-лукап, точні значення)
                z_val = float(meta_vs.get("z", 0.0))
                upward = bool(meta_vs.get("upbar", True))
                # (VOL/ATR гілка вимкнена за замовчуванням)
                if upward:
                    reason_txt = (
                        f"📈 Бичий сплеск обсягу (Z≥{volz:.2f})"
                        if z_val >= volz
                        else "📈 Бичий сплеск обсягу (VOL/ATR)"
                    )
                    _add("bull_volume_spike", reason_txt)
                    logger.debug(
                        f"[{symbol}] Bull volume spike | Z={z_val:.2f} thr={volz:.2f} use_vol_atr={self.use_vol_atr}"
                    )
                else:
                    reason_txt = (
                        f"📉 Ведмежий сплеск обсягу (Z≥{volz:.2f})"
                        if z_val >= volz
                        else "📉 Ведмежий сплеск обсягу (VOL/ATR)"
                    )
                    _add("bear_volume_spike", reason_txt)
                    logger.debug(
                        f"[{symbol}] Bear volume spike | Z={z_val:.2f} thr={volz:.2f} use_vol_atr={self.use_vol_atr}"
                    )
            else:
                # Логуємо відхилення тригера у JSONL для подальшої аналітики
                try:
                    reason = (
                        str(meta_vs.get("reason"))
                        if isinstance(meta_vs, dict)
                        else None
                    )
                    if reason:
                        # Використовуємо часову мітку бара
                        ts_ms = None
                        if "open_time" in df.columns:
                            try:
                                ts_ms = int(
                                    pd.to_numeric(
                                        df["open_time"].iloc[-1], errors="coerce"
                                    )
                                )
                            except Exception:
                                ts_ms = None
                        # ISO‑час (UTC) для зручності
                        if ts_ms and ts_ms > 1e12:  # мілісекунди
                            ts_iso = (
                                dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.UTC)
                                .isoformat()
                                .replace("+00:00", "Z")
                            )
                        elif ts_ms and ts_ms > 1e9:  # секунди
                            ts_iso = (
                                dt.datetime.fromtimestamp(ts_ms, tz=dt.UTC)
                                .isoformat()
                                .replace("+00:00", "Z")
                            )
                        else:
                            ts_iso = dt.datetime.utcnow().isoformat() + "Z"

                        record = {
                            "timestamp_ms": ts_ms,
                            "timestamp_iso": ts_iso,
                            "symbol": symbol,
                            "event": "volume_spike_reject",
                            "reject_reason": reason,
                            "reject_meta": meta_vs,
                            # Контракт Stage1Signal поля — для сумісності з тулінгом
                            "signal": "NORMAL",
                            "trigger_reasons": [],
                        }
                        await self._append_stage1_jsonl(record)
                except Exception:
                    # Тихий пропуск телеметрії, не впливає на основний пайплайн
                    pass

        # 2. Пробій рівнів (локальний breakout, підхід до рівня)
        if self._is_trigger_enabled("breakout"):
            # Налаштування breakout із конфігурації (state-aware)
            br_cfg: dict[str, Any] = {}
            st = (
                effective.get("signal_thresholds", {})
                if isinstance(effective, dict)
                else {}
            )
            if isinstance(st, dict):
                br_cfg = st.get("breakout", {}) or {}

            band_pct_atr = br_cfg.get("band_pct_atr", br_cfg.get("band_pct"))
            confirm_bars = int(br_cfg.get("confirm_bars", 1) or 1)
            min_retests = int(br_cfg.get("min_retests", 0) or 0)

            # Обчислимо поріг близькості як частку від ціни: band_pct_atr * (ATR/price)
            try:
                atr_pct_local = float(stats.get("atr", 0.0)) / float(price)
            except (TypeError, ValueError, ZeroDivisionError):
                atr_pct_local = 0.0
            if isinstance(band_pct_atr, (int, float)) and atr_pct_local > 0:
                near_thr = float(band_pct_atr) * atr_pct_local
                # Клапани безпеки: мінімум 0.20% щоб уникнути "липких" near_high/near_low на мікро‑ATR
                min_near_pct = 0.002  # 0.20%
                near_thr = float(min(0.03, max(min_near_pct, near_thr)))
            else:
                # Дефолт 0.5%, але не нижче мінімуму
                near_thr = 0.005

            logger.debug(
                "[%s] Breakout cfg: band_pct_atr=%s → near_thr=%.5f, confirm_bars=%d, min_retests=%d",
                symbol,
                band_pct_atr,
                near_thr,
                confirm_bars,
                min_retests,
            )

            # Виконуємо перевірку breakout
            breakout = breakout_level_trigger(
                df,
                stats,
                window=20,  # локальне вікно для high/low
                near_threshold=float(near_thr),
                near_daily_threshold=0.5,  # у % (0.5% за замовчуванням)
                symbol=symbol,
                confirm_bars=confirm_bars,
                min_retests=min_retests,
            )
            if breakout["breakout_up"]:
                _add("breakout_up", "🔺 Пробій вгору локального максимуму")
            if breakout["breakout_down"]:
                _add("breakout_down", "🔻 Пробій вниз локального мінімуму")
            # Структурні тригери можуть бути лише для пріоритезації
            if breakout["near_high"]:
                if _struct_prior_only:
                    context_only_reasons.append("near_high")
                else:
                    _add("near_high", "📈 Підхід до локального максимуму")
            if breakout["near_low"]:
                if _struct_prior_only:
                    context_only_reasons.append("near_low")
                else:
                    _add("near_low", "📉 Підхід до локального мінімуму")
            if breakout["near_daily_support"]:
                if _struct_prior_only:
                    context_only_reasons.append("near_daily_support")
                else:
                    _add("near_daily_support", "🟢 Підхід до денного рівня підтримки")
            if breakout["near_daily_resistance"]:
                if _struct_prior_only:
                    context_only_reasons.append("near_daily_resistance")
                else:
                    _add(
                        "near_daily_resistance",
                        "🔴 Підхід до денного рівня опору",
                    )

        # 3. Сплеск волатильності
        if self._is_trigger_enabled("volatility_spike"):
            if _skip_vol_spike:
                try:
                    logger.info(
                        "[STRICT_GUARD] symbol=%s skip=volatility_spike", symbol
                    )
                except Exception:
                    pass
            else:
                if volatility_spike_trigger(df, window=14, threshold=2.0):
                    if _qde_disabled:
                        context_only_reasons.append("volatility_spike")
                    else:
                        _add("volatility_spike", "⚡️ Сплеск волатильності (ATR/TR)")

        # 4. RSI + дивергенції
        if self._is_trigger_enabled("rsi"):
            if _skip_divergence:
                # Використовуємо вже порахований stats.rsi без детекції дивергенцій
                try:
                    rsi_val = (
                        float(stats.get("rsi"))
                        if stats.get("rsi") is not None
                        else None
                    )
                except Exception:
                    rsi_val = None
                rsi_res = {"rsi": rsi_val}
                try:
                    logger.info("[STRICT_GUARD] symbol=%s skip=rsi_divergence", symbol)
                except Exception:
                    pass
            else:
                rsi_res = rsi_divergence_trigger(df, rsi_period=14)
            if rsi_res.get("rsi") is not None:
                # Замість фіксованих 70/30 — динамічні з stats, із clamp від конфігу (за наявності)
                over = stats["dynamic_overbought"]
                under = stats["dynamic_oversold"]
                # Застосуємо обмеження (стеля/підлога) з signal_thresholds.rsi_trigger
                st = (
                    effective.get("signal_thresholds", {})
                    if isinstance(effective, dict)
                    else {}
                )
                rsi_cfg = st.get("rsi_trigger", {}) if isinstance(st, dict) else {}
                clamp_over = rsi_cfg.get("overbought")
                clamp_under = rsi_cfg.get("oversold")
                over_eff = (
                    float(min(float(over), float(clamp_over)))
                    if isinstance(clamp_over, (int, float))
                    else float(over)
                )
                under_eff = (
                    float(max(float(under), float(clamp_under)))
                    if isinstance(clamp_under, (int, float))
                    else float(under)
                )
                if over_eff != over or under_eff != under:
                    logger.debug(
                        "[%s] RSI clamp застосовано",
                        symbol,
                        extra={
                            "base": {"over": float(over), "under": float(under)},
                            "clamp": {"over": clamp_over, "under": clamp_under},
                            "effective": {"over": over_eff, "under": under_eff},
                        },
                    )
                over = over_eff
                under = under_eff
                if _qde_disabled:
                    if rsi_res["rsi"] > over:
                        context_only_reasons.append("rsi_overbought")
                    elif rsi_res["rsi"] < under:
                        context_only_reasons.append("rsi_oversold")
                    if rsi_res.get("bearish_divergence"):
                        context_only_reasons.append("bearish_div")
                    if rsi_res.get("bullish_divergence"):
                        context_only_reasons.append("bullish_div")
                else:
                    if rsi_res["rsi"] > over:
                        _add(
                            "rsi_overbought",
                            f"🔺 RSI перекупленість ({rsi_res['rsi']:.1f} > {over:.1f})",
                        )
                    elif rsi_res["rsi"] < under:
                        _add(
                            "rsi_oversold",
                            f"🔻 RSI перепроданість ({rsi_res['rsi']:.1f} < {under:.1f})",
                        )
                    if rsi_res.get("bearish_divergence"):
                        _add("bearish_div", "🦀 Ведмежа дивергенція RSI/ціна")
                    if rsi_res.get("bullish_divergence"):
                        _add("bullish_div", "🦅 Бичача дивергенція RSI/ціна")

        # 5. Відхилення від VWAP (порог з thresholds)
        if self._is_trigger_enabled("vwap_deviation"):
            vwap_thr = float(
                effective.get("vwap_deviation", getattr(thr, "vwap_deviation", 0.02))
                or 0.02
            )
            vwap_trig = vwap_deviation_trigger(
                self.vwap_manager, symbol, price, threshold=float(vwap_thr)
            )
            if vwap_trig["trigger"]:
                if _qde_disabled:
                    context_only_reasons.append("vwap_deviation")
                else:
                    _add(
                        "vwap_deviation",
                        f"⚖️ Відхилення від VWAP на {vwap_trig['deviation']*100:.2f}% (поріг {float(vwap_thr)*100:.2f}%)",
                    )

        # 6. Сплеск відкритого інтересу (OI)
        # if open_interest_spike_trigger(df, z_thresh=3.0):
        #    _add("oi_spike", "🆙 Сплеск відкритого інтересу (OI)")

        # 7. Додатково: ATR-коридор (волатильність) з урахуванням мінімального ATR
        min_atr_pct = float(getattr(thr, "min_atr_percent", 0.0) or 0.0)
        if atr_pct > thr.high_gate:
            _add("high_atr", f"📊 ATR > {thr.high_gate:.2%}")
        elif low_atr_flag or (min_atr_pct and atr_pct < min_atr_pct):
            if _exclude_low_vol_atr:
                try:
                    logger.info("[STRICT_GUARD] symbol=%s skip=low_atr_reason", symbol)
                except Exception:
                    pass
            else:
                _add("low_atr", f"📉 ATR < {thr.low_gate:.2%}")

        # Зберігаємо причини тригерів для подальшої обробки
        raw_reasons = list(reasons)  # зберігаємо «як є» для діагностики

        # Додаємо контекстні причини у stats (не впливають на сигнал)
        try:
            if context_only_reasons:
                stats["context_only_reasons"] = list(
                    dict.fromkeys(context_only_reasons)
                )
                logger.info(
                    "[STRICT_GUARD] symbol=%s context_only=%s",
                    symbol,
                    "+".join(stats.get("context_only_reasons", [])),
                )
        except Exception:
            pass

        # Нормалізуємо причини тригерів
        trigger_reasons = normalize_trigger_reasons(raw_reasons)

        override_active = False
        override_payload: dict[str, Any] | None = None
        if isinstance(stats, dict):
            # _update_low_atr_override may persist TTL to Redis; await it
            try:
                override_active, override_payload = await self._update_low_atr_override(
                    symbol, stats
                )
            except Exception:
                # best-effort: if override update fails, continue without it
                override_active, override_payload = False, None
            if override_payload:
                override_copy = dict(override_payload)
                override_copy["active"] = True
                stats["low_atr_override"] = override_copy
                overrides_map = stats.setdefault("overrides", {})
                overrides_map["low_atr_spike"] = override_copy
                tags = stats.get("tags")
                if isinstance(tags, list):
                    if "override_low_atr_on_spike" not in tags:
                        tags.append("override_low_atr_on_spike")
                else:
                    stats["tags"] = ["override_low_atr_on_spike"]
            else:
                stats.pop("low_atr_override", None)
                overrides_map = stats.get("overrides")
                if isinstance(overrides_map, dict):
                    overrides_map.pop("low_atr_spike", None)
                tags = stats.get("tags")
                if isinstance(tags, list):
                    stats["tags"] = [
                        tag for tag in tags if tag != "override_low_atr_on_spike"
                    ]

        # Gate: якщо ринок спокійний (low ATR) і немає сильних тригерів — не ескалюємо до ALERT
        strong_trigs = {"breakout_up", "breakout_down", "vwap_deviation"}
        has_strong = any(t in strong_trigs for t in trigger_reasons)
        low_atr_gate_applied = False
        if low_atr_flag and not has_strong:
            if override_active:
                try:
                    logger.info(
                        "[STRICT_GUARD] %s low_atr_override allow metrics=%s ttl=%s",
                        symbol,
                        (override_payload or {}).get("metrics"),
                        (override_payload or {}).get("ttl"),
                    )
                except Exception:
                    pass
            else:
                signal = "NORMAL"
                low_atr_gate_applied = True
        else:
            effective_min_reasons = self.min_reasons_for_alert
            if self._bearish_bonus_enabled and trigger_reasons:
                bearish_hits = sum(
                    reason in self._bearish_tags for reason in trigger_reasons
                )
                if bearish_hits:
                    effective_min_reasons = max(1, effective_min_reasons - 1)
                    logger.debug(
                        "[%s] застосовано bearish-бонус: hits=%d -> min_reasons=%d (база=%d)",
                        symbol,
                        bearish_hits,
                        effective_min_reasons,
                        self.min_reasons_for_alert,
                    )

            signal = (
                "ALERT" if len(trigger_reasons) >= effective_min_reasons else "NORMAL"
            )

        # ─────────────── TRAP detector (dry‑run інтеграція) ───────────────
        # Евристичний гейтинг важких обчислень у спокійному/далекому режимі
        skip_heavy = False
        try:
            if bool(HEAVY_COMPUTE_GATING_ENABLED):
                # У «спокійному» режимі, далеко від краю, без сильних тригерів і без HTF‑підтвердження
                # можемо пропустити TRAP/phase для зниження навантаження (контракти не змінюємо)
                vol_regime_val = str(locals().get("vol_regime_strict", "normal"))
                near_edge_flag = bool(locals().get("near_edge", False))
                htf_ok_bool = bool(locals().get("htf_ok_val", False))
                if low_atr_flag and (vol_regime_val == "normal"):
                    if not near_edge_flag and not bool(has_strong) and not htf_ok_bool:
                        skip_heavy = True
        except Exception:
            skip_heavy = False

        trap_block = None
        try:
            if STAGE1_TRAP_ENABLED and ENABLE_TRAP_DETECTOR and not skip_heavy:
                # Нормалізуємо базу для volatility_spike у TRAP: якщо є історія — беремо rolling p50 ATR%.
                # Інакше — використовуємо low_gate як безпечний проксі, щоб уникнути ratio→∞ на нулях.
                try:
                    from config import (
                        config as _cfg,  # локальний імпорт для уникнення циклів
                    )
                except Exception:
                    _cfg = None  # type: ignore[assignment]

                try:
                    _runtime = (
                        (getattr(_cfg, "STAGE2_RUNTIME", {}) or {}) if _cfg else {}
                    )
                except Exception:
                    _runtime = {}
                p50_window = int(_runtime.get("atr_history_len", 120) or 120)
                atr_p50_val: float | None = None
                try:
                    if "atr" in df.columns and (
                        "close" in df.columns or "price" in df.columns
                    ):
                        close_col = "close" if "close" in df.columns else "price"
                        # Обчислюємо ATR% для останнього вікна барів і беремо медіану
                        tail_df = df[["atr", close_col]].tail(max(5, p50_window))
                        ratio_series = []
                        for a, c in zip(
                            tail_df["atr"].tolist(),
                            tail_df[close_col].tolist(),
                            strict=False,
                        ):
                            try:
                                af = float(a)
                                cf = float(c)
                                if cf > 0:
                                    ratio_series.append(af / cf)
                            except Exception:
                                continue
                        if ratio_series:
                            ratio_series_sorted = sorted(ratio_series)
                            m = len(ratio_series_sorted)
                            if m % 2 == 1:
                                atr_p50_val = float(ratio_series_sorted[m // 2])
                            else:
                                atr_p50_val = float(
                                    (
                                        ratio_series_sorted[m // 2 - 1]
                                        + ratio_series_sorted[m // 2]
                                    )
                                    / 2.0
                                )
                except Exception:
                    atr_p50_val = None

                if not isinstance(atr_p50_val, (int, float)):
                    try:
                        atr_p50_val = float(getattr(thr, "low_gate", 0.0035) or 0.0035)
                    except Exception:
                        atr_p50_val = 0.0035

                try:
                    stats["atr_pct_p50"] = float(atr_p50_val)
                    logger.info(
                        "[TRAP_BASE] %s p50=%.6f window=%d",
                        symbol,
                        float(atr_p50_val),
                        int(p50_window),
                    )
                except Exception:
                    pass
                # Вхідні дані — вже готові stats; volume_data дозволено передати ті ж stats
                trap_res = detect_trap_signals(stats, stats)
                # Гейти: score_gate + cooldown + low_volatility guard
                score_gate = float(STAGE1_TRAP.get("score_gate", 0.67) or 0.67)
                cooldown_sec = int(STAGE1_TRAP.get("cooldown_sec", 120) or 120)
                log_prefix = str(STAGE1_TRAP.get("log_prefix", "[TRAP]"))

                trap_score = float(trap_res.get("trap_score", 0.0) or 0.0)
                trap_detected = bool(trap_res.get("trap_detected", False))
                trap_reasons = list(trap_res.get("reasons", []) or [])
                # timestamp мс з df (пріоритетний), fallback — now
                try:
                    ts_ms = None
                    if "close_time" in df.columns:
                        ts_ms = int(
                            pd.to_numeric(df["close_time"].iloc[-1], errors="coerce")
                        )
                    if not ts_ms and "open_time" in df.columns:
                        ts_ms = int(
                            pd.to_numeric(df["open_time"].iloc[-1], errors="coerce")
                        )
                except Exception:
                    ts_ms = None
                if not ts_ms:
                    ts_ms = int(dt.datetime.now(dt.UTC).timestamp() * 1000)

                # Guards
                suppressed: str | None = None
                # Low volatility guard (ATR% нижче low_gate)
                try:
                    atr_pct_local = float(stats.get("atr", 0.0)) / float(price)
                    if atr_pct_local < float(thr.low_gate):
                        suppressed = "low_volatility"
                except Exception:
                    pass

                # If strong volume spike — дозволяємо TRAP навіть за low_volatility
                try:
                    volz_now = float(stats.get("volume_z") or 0.0)
                except Exception:
                    volz_now = 0.0
                strong_volz_thr = float(STAGE1_TRAP_STRONG_VOLZ_THR or 2.5)
                if suppressed == "low_volatility" and volz_now >= strong_volz_thr:
                    suppressed = None

                # Cooldown guard + extreme override
                last_ts = int(self.last_trap_ts.get(symbol, 0))
                ratios = trap_res.get("ratios", {}) or {}
                try:
                    spike_ratio = float(ratios.get("atr_spike_ratio") or 0.0)
                except Exception:
                    spike_ratio = 0.0
                try:
                    crisis_spike_thr = float(
                        STAGE2_VOLATILITY_REGIME.get("crisis_spike_ratio", 3.0)
                    )
                except Exception:
                    crisis_spike_thr = 3.0
                cooldown_hit = (ts_ms - last_ts) < int(cooldown_sec * 1000)
                strong_override = (
                    TRAP_COOLDOWN_OVERRIDE_ENABLED
                    and trap_score >= 0.95
                    and spike_ratio >= crisis_spike_thr
                )
                if cooldown_hit and not strong_override:
                    suppressed = suppressed or "cooldown"
                override_applied = cooldown_hit and strong_override
                if override_applied:
                    suppressed = None
                    try:
                        logger.info(
                            "[STRICT_TRAP] override_cooldown symbol=%s score=%.3f spike_ratio=%.2f",
                            symbol,
                            trap_score,
                            spike_ratio,
                        )
                    except Exception:
                        pass

                # Додаткові гейти для зниження шуму:
                #  - вимагати ≥2 причин та trap_score ≥ score_gate
                #  - якщо присутній acceleration_detected, вимагати vol_z ≥ 1.0 або atr_pct ≥ atr_pct_p50
                fired_base = (
                    trap_detected
                    and (trap_score >= score_gate)
                    and (len(trap_reasons) >= 2)
                    and not suppressed
                )
                if fired_base:
                    accel_present = "acceleration_detected" in trap_reasons
                    if accel_present:
                        try:
                            volz_now = float(ratios.get("volume_z") or 0.0)
                            atr_pct_now = ratios.get("atr_pct")
                            atr_p50_now = ratios.get("atr_pct_p50")
                            atr_gate_ok = (
                                isinstance(atr_pct_now, (int, float))
                                and isinstance(atr_p50_now, (int, float))
                                and float(atr_pct_now) >= float(atr_p50_now)
                            )
                        except Exception:
                            volz_now = 0.0
                            atr_gate_ok = False
                        fired = bool(volz_now >= 1.0 or atr_gate_ok)
                    else:
                        fired = True
                else:
                    fired = False
                if fired:
                    self.last_trap_ts[symbol] = ts_ms

                # Лог для телеметрії
                try:
                    logger.info(
                        "%s symbol=%s score=%.3f fired=%s suppressed=%s reasons=%s",
                        log_prefix,
                        symbol,
                        trap_score,
                        fired,
                        suppressed,
                        trap_res.get("reasons"),
                    )
                except Exception:
                    pass

                trap_block = {
                    "score": trap_score,
                    "reasons": trap_reasons,
                    "ratios": ratios,
                    "fired": bool(fired),
                    "suppressed": suppressed,
                    "cooldown_override": bool(override_applied),
                }
                # Якщо TRAP не пройшов нові гейти, додамо тег‑нагляд у stats.tags
                try:
                    if not fired and "acceleration_detected" in trap_reasons:
                        tags = (
                            stats.get("tags")
                            if isinstance(stats.get("tags"), list)
                            else []
                        )
                        if "accel_watch" not in tags:
                            tags.append("accel_watch")
                        stats["tags"] = tags
                except Exception:
                    pass
                # Керований вплив на сигнал/причини
                if STAGE1_TRAP_INFLUENCE_ENABLED and ENABLE_TRAP_DETECTOR and fired:
                    raw_reasons.append("trap")
                    # Якщо low_atr gate застосовано, і TRAP має високе vol_z — дозволяємо ескалацію
                    if (
                        low_atr_gate_applied
                        and STAGE1_TRAP_MARK_STRONG
                        and volz_now >= strong_volz_thr
                    ):
                        # Перерахунок trigger_reasons та сигналу з урахуванням нової причини
                        trigger_reasons = normalize_trigger_reasons(raw_reasons)
                        effective_min_reasons = self.min_reasons_for_alert
                        if self._bearish_bonus_enabled and trigger_reasons:
                            bearish_hits = sum(
                                reason in self._bearish_tags
                                for reason in trigger_reasons
                            )
                            if bearish_hits:
                                effective_min_reasons = max(
                                    1, effective_min_reasons - 1
                                )
                        if len(trigger_reasons) >= effective_min_reasons:
                            signal = "ALERT"
        except Exception:
            # Уникаємо впливу на основний пайплайн у разі будь-яких помилок TRAP
            trap_block = {
                "score": 0.0,
                "reasons": [],
                "ratios": {},
                "fired": False,
                "suppressed": "error",
            }

        # Додаємо блок у stats
        try:
            if isinstance(trap_block, dict):
                stats["trap"] = trap_block
        except Exception:
            pass

        # Позначка джерела vol_z: за замовчуванням "real"; якщо ввімкнено проксі‑режим — "proxy"
        try:
            # Локальний імпорт у тілі функції, щоб уникнути циклічних залежностей
            from config import flags as _flags  # type: ignore

            stats["vol_z_source"] = (
                "proxy" if getattr(_flags, "VOLZ_SOURCE_PROXY_MODE", False) else "real"
            )
        except Exception:
            try:
                stats["vol_z_source"] = "real"
            except Exception:
                pass

        # Strict Phase (телеметрія‑тільки): визначаємо фазу без впливу на сигнал
        try:
            if skip_heavy:
                # Позначимо пропуск для телеметрії
                try:
                    tags = (
                        stats.get("tags") if isinstance(stats.get("tags"), list) else []
                    )
                    if "heavy_skip" not in tags:
                        tags.append("heavy_skip")
                    stats["tags"] = tags
                    logger.info(
                        "[STRICT_GUARD] symbol=%s skip=heavy_compute reasons=low_vol_far_edge_no_strong_htf_off",
                        symbol,
                    )
                except Exception:
                    pass
                raise RuntimeError("heavy_compute_skipped")
            low_gate_eff = None
            try:
                low_gate_eff = float(effective.get("low_gate")) if effective else None
            except Exception:
                low_gate_eff = None
            phase_info = detect_phase_from_stats(
                stats, symbol=symbol, low_gate_effective=low_gate_eff
            )
            if isinstance(phase_info, dict):
                stats["phase"] = phase_info
                name = phase_info.get("name")
                # Додамо тег для нагляду у stats.tags
                if name:
                    tags = (
                        stats.get("tags") if isinstance(stats.get("tags"), list) else []
                    )
                    if name not in tags:
                        tags.append(name)
                    stats["tags"] = tags
                # Телеметрійний лог
                try:
                    logger.info(
                        "[STRICT_PHASE] symbol=%s phase=%s score=%.2f reasons=%s",
                        symbol,
                        name,
                        float(phase_info.get("score", 0.0) or 0.0),
                        "+".join(phase_info.get("reasons", []) or []),
                    )
                    # Статистичний евент для подальшого аналізу якості фаз
                    try:
                        from monitoring.telemetry_sink import (  # локальний імпорт, щоб уникнути циклів
                            log_stage1_event,
                        )

                        price_now = None
                        try:
                            # price визначається вище в пайплайні; дублюємо захоплення значення, якщо воно доступне
                            price_now = float(price)  # type: ignore[name-defined]
                        except Exception:
                            try:
                                price_now = float(stats.get("current_price"))
                            except Exception:
                                price_now = None

                        await log_stage1_event(
                            event="phase_detected",
                            symbol=str(symbol),
                            payload={
                                "name": name,
                                "score": float(phase_info.get("score", 0.0) or 0.0),
                                "reasons": phase_info.get("reasons"),
                                "price": price_now,
                            },
                        )
                    except Exception:
                        # Telemetry — best‑effort, не впливає на основний флоу
                        pass
                except Exception:
                    pass
        except Exception:
            # Безпечно пропускаємо будь-які помилки фази, не впливаючи на пайплайн
            pass

        logger.debug(
            f"[{symbol}] SIGNAL={signal}, тригери={trigger_reasons}, ціна={price:.4f}"
        )

        return {
            K_SYMBOL: symbol,
            "current_price": price,
            "anomalies": anomalies,
            K_SIGNAL: signal,
            K_TRIGGER_REASONS: trigger_reasons,  # повертаємо канонічні імена
            "raw_trigger_reasons": raw_reasons,  # опційно: залишимо для дебагу
            K_STATS: stats,
            "calibrated_params": thr.to_dict(),
            "thresholds": thr.to_dict(),
        }

    # Сумісний обгортковий метод для реактивного хуку WSWorker
    def process_new_bar(self, symbol: str):
        """Сумісний обгортковий метод для WSWorker: запускає обробку нового бару для заданого символу.

        Повертає корутину для асинхронного виконання.

        Args:
            symbol: Тікер інструменту (рядок).

        Returns:
            Корутину для асинхронної перевірки аномалій або None у разі помилки.
        """
        try:
            getter = getattr(self.cache_handler, "get_df", None)
            if callable(getter):
                maybe = getter(symbol, "1m", limit=50)
                if asyncio.iscoroutine(maybe):

                    async def _do():
                        df = await maybe
                        if df is None:
                            return {}
                        return await self.check_anomalies(symbol, df)

                    return _do()
                else:
                    df = maybe
                    # Повертаємо корутину для уніфікованості
                    return self.check_anomalies(symbol, df)
        except Exception:
            return None

    def update_and_check(self, symbol: str, payload: Any):
        """Сумісний обгортковий метод, який приймає payload (за наявності) та викликає check_anomalies.

        Якщо у payload є DataFrame під ключем 'df', він буде використаний; інакше — fallback на process_new_bar.

        Args:
            symbol: Тікер інструменту (рядок).
            payload: Дані, які можуть містити DataFrame під ключем 'df'.

        Returns:
            Корутину для асинхронної перевірки аномалій або None у разі помилки.

        Приклад:
            >>> monitor.update_and_check("btcusdt", {"df": df})
        """
        try:
            df = None
            if isinstance(payload, dict):
                df = payload.get("df")
            if df is not None:
                return self.check_anomalies(symbol, df)
            return self.process_new_bar(symbol)
        except Exception:
            return None
