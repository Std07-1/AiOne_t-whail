# Level quality filtering (noise reduction for zones/levels)

from __future__ import annotations

import logging
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("level_quality_filter")
if not logger.handlers:  # guard
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
    logger.propagate = False


class LevelQualityFilter:
    """Фільтрація та пріоритизація рівнів/зон для зменшення шуму.

    Призначення:
    - Надати централізований набір евристик для відбору найбільш релевантних
      accumulation/distribution/stop-hunt рівнів перед передачею у Stage2/Stage3.
    - Мінімізувати "шум" (дуже дрібні або далекі зони) та повернути метрики
      якості для телеметрії.

    Параметри методів:
    - levels_data: dict з ключами accumulation_zones, distribution_zones, stop_hunt_levels.
      Кожен елемент може бути:
        - tuple/ліст (low, high)
        - dict з keys {"low","high","volume"} або {"price": float} для stop_hunt
        - просте число (float/int) — інтерпретується як price (для stop_hunt_levels)
    - current_price: поточна ринкова ціна (float) — використовуємо для оцінки proximity.
    - thresholds: словник порогів/параметрів (див. DEFAULTS нижче). Передача threshold
      дає можливість тестувати/налаштовувати фільтр без зміни коду.

    Повертає:
    - dict з тими ж ключами, але вже відфільтрованими/збагаченими:
        {
          "accumulation_zones": list[dict{"low","high","volume","proximity"}],
          "distribution_zones": list[dict{"low","high","volume","proximity"}],
          "stop_hunt_levels": list[float]
        }

    TODOs (високого рівня):
    - TODO(config): Зчитувати DEFAULTS з config/config.py (константи/feature-flags).
      Чому: централізована конфігурація дозволить контролювати агресивність фільтрів
      без релізу коду, а також використовувати feature-flags для A/B тестів.
    - TODO(metrics): Експортувати метрики (counts_before/after, rejection_reasons)
      в систему телеметрії (Prometheus/JSONL) для моніторингу впливу фільтра.
      Чому: потрібно виміряти, як фільтр впливає на downstream (PnL / alert-rate).
    - TODO(tests): Додати unit-тести (happy-path, edge-cases, псевдо-стріми) для валідації
      поведінки фільтра під різними threshold-ами.
    """

    # Локальні дефолти — тимчасово, поки не винесено в config
    DEFAULTS: dict[str, float] = {
        "MIN_ACCUMULATION_VOLUME": 100_000.0,  # мінімум обсягу для accumulation zone
        "MIN_DISTRIBUTION_VOLUME": 200_000.0,  # мінімум обсягу для distribution zone
        "MAX_DISTANCE_FROM_PRICE": 0.03,  # 3% від поточної ціни
        "STOP_HUNT_MAX_DISTANCE": 0.02,  # 2% для stop-hunt
        "MAX_LEVELS_PER_TYPE": 3,
        "STOP_HUNT_TOP_N": 5,
    }

    @staticmethod
    def _get_price_from_level(level: Any) -> float | None:
        """Отримати price із рівня (для stop_hunt_levels).

        Підтримує числові значення та dict з ключем 'price'.
        Повертає None якщо не вдалось інтерпретувати.
        """
        if isinstance(level, (int, float)):
            try:
                return float(level)
            except Exception:
                return None
        if isinstance(level, dict):
            p = level.get("price")
            if isinstance(p, (int, float)):
                try:
                    return float(p)
                except Exception:
                    return None
            # Також допускаємо структури з 'low'/'high' — повертаємо центр
            if "low" in level and "high" in level:
                try:
                    low = float(level.get("low", 0.0))
                    high = float(level.get("high", low))
                    return (low + high) / 2.0
                except Exception:
                    return None
        return None

    @staticmethod
    def _get_low_high(level: Any) -> tuple[float, float] | None:
        """Нормалізація входящого zone у пару (low, high).

        Підтримує:
        - tuple/list з двома елементами,
        - dict з 'low' і 'high',
        - dict з 'price' (в такому випадку повертається (price, price)).

        Повертає None при некоректному форматі.
        """
        if isinstance(level, (list, tuple)) and len(level) == 2:
            try:
                low = float(level[0])
                high = float(level[1])
                return (min(low, high), max(low, high))
            except Exception:
                return None
        if isinstance(level, dict):
            if "low" in level and "high" in level:
                try:
                    low = float(level.get("low"))
                    high = float(level.get("high"))
                    return (min(low, high), max(low, high))
                except Exception:
                    return None
            if "price" in level:
                try:
                    p = float(level.get("price"))
                    return (p, p)
                except Exception:
                    return None
        # Якщо передано просто число (інколи використовують для stop_hunt) — виловити в іншому хендлері
        return None

    @staticmethod
    def filter_quality_levels(
        levels_data: dict[str, Any],
        current_price: float,
        thresholds: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """Фільтрація рівнів за об'ємом, близькістю та лімітом кількості.

        Args:
            levels_data: вхідні рівні (див. docstring класу).
            current_price: поточна ринкова ціна для обчислення proximity.
            thresholds: (опційно) словник порогів; якщо None — використовуються DEFAULTS.

        Returns:
            dict з відфільтрованими зонами (див. docstring класу).
        """
        logger.info(
            "LevelQualityFilter.filter_quality_levels: вхідні розміри	accum=%d dist=%d stop=%d current_price=%.6f",
            len(levels_data.get("accumulation_zones") or []),
            len(levels_data.get("distribution_zones") or []),
            len(levels_data.get("stop_hunt_levels") or []),
            float(current_price),
        )

        thr = dict(LevelQualityFilter.DEFAULTS)
        if isinstance(thresholds, dict):
            thr.update({k: thresholds.get(k, thr[k]) for k in thr.keys()})

        out: dict[str, Any] = {
            "accumulation_zones": [],
            "distribution_zones": [],
            "stop_hunt_levels": [],
        }

        try:
            min_acc_vol = float(thr.get("MIN_ACCUMULATION_VOLUME", 100_000.0))
            min_dist_vol = float(thr.get("MIN_DISTRIBUTION_VOLUME", 200_000.0))
            max_dist = float(thr.get("MAX_DISTANCE_FROM_PRICE", 0.03))
            stop_max_dist = float(thr.get("STOP_HUNT_MAX_DISTANCE", 0.02))
            max_levels = int(thr.get("MAX_LEVELS_PER_TYPE", 3))
            stop_top_n = int(thr.get("STOP_HUNT_TOP_N", 5))
        except Exception:
            logger.exception(
                "LevelQualityFilter.filter_quality_levels: помилка при інтерпретації thresholds — використовуємо DEFAULTS"
            )
            min_acc_vol = float(LevelQualityFilter.DEFAULTS["MIN_ACCUMULATION_VOLUME"])
            min_dist_vol = float(LevelQualityFilter.DEFAULTS["MIN_DISTRIBUTION_VOLUME"])
            max_dist = float(LevelQualityFilter.DEFAULTS["MAX_DISTANCE_FROM_PRICE"])
            stop_max_dist = float(LevelQualityFilter.DEFAULTS["STOP_HUNT_MAX_DISTANCE"])
            max_levels = int(LevelQualityFilter.DEFAULTS["MAX_LEVELS_PER_TYPE"])
            stop_top_n = int(LevelQualityFilter.DEFAULTS["STOP_HUNT_TOP_N"])

        # Accumulation: вибрати найбільш релевантні за volume+proximity
        acc = levels_data.get("accumulation_zones") or []
        acc_enriched: list[dict[str, Any]] = []
        for i, z in enumerate(acc):
            lh = LevelQualityFilter._get_low_high(z)
            if not lh:
                logger.debug(
                    "accumulation_zones[%d] ігнорується (невалидний формат): %r", i, z
                )
                continue
            low, high = lh
            center = (low + high) / 2.0
            # proximity: 1.0 - нормалізована відстань (чим ближче — тим більше)
            prox = max(
                0.0, 1.0 - abs(center - current_price) / max(current_price, 1e-9)
            )
            vol = 0.0
            if isinstance(z, dict):
                try:
                    vol = float(z.get("volume", 0.0))
                except Exception:
                    vol = 0.0
            # Фільтр за об'ємом та відстанню
            distance_pct = abs(center - current_price) / max(current_price, 1e-9)
            if vol >= min_acc_vol and distance_pct <= max_dist:
                acc_enriched.append(
                    {"low": low, "high": high, "volume": vol, "proximity": prox}
                )
            else:
                logger.debug(
                    "accumulation_zones[%d] відкинуто (vol=%.2f prox=%.4f dist=%.4f thresholds vol>=%.2f dist<=%.4f)",
                    i,
                    vol,
                    prox,
                    distance_pct,
                    min_acc_vol,
                    max_dist,
                )
        if acc_enriched:
            acc_sorted = sorted(
                acc_enriched,
                key=lambda x: (x.get("volume", 0.0), x.get("proximity", 0.0)),
                reverse=True,
            )
            # Вибираємо мінімум 1, максимум min(2, max_levels)
            pick_n = max(1, min(2, max_levels))
            out["accumulation_zones"] = acc_sorted[:pick_n]
            logger.info(
                "LevelQualityFilter: selected %d accumulation_zones (pick_n=%d)",
                len(out["accumulation_zones"]),
                pick_n,
            )

        # Distribution: топ-ні за об'ємом у межах відстані
        dist = levels_data.get("distribution_zones") or []
        dist_enriched: list[dict[str, Any]] = []
        for i, z in enumerate(dist):
            lh = LevelQualityFilter._get_low_high(z)
            if not lh:
                logger.debug(
                    "distribution_zones[%d] ігнорується (невалидний формат): %r", i, z
                )
                continue
            low, high = lh
            center = (low + high) / 2.0
            prox = max(
                0.0, 1.0 - abs(center - current_price) / max(current_price, 1e-9)
            )
            vol = 0.0
            if isinstance(z, dict):
                try:
                    vol = float(z.get("volume", 0.0))
                except Exception:
                    vol = 0.0
            distance_pct = abs(center - current_price) / max(current_price, 1e-9)
            if vol >= min_dist_vol and distance_pct <= max_dist:
                dist_enriched.append(
                    {"low": low, "high": high, "volume": vol, "proximity": prox}
                )
            else:
                logger.debug(
                    "distribution_zones[%d] відкинуто (vol=%.2f dist=%.4f thresholds vol>=%.2f dist<=%.4f)",
                    i,
                    vol,
                    distance_pct,
                    min_dist_vol,
                    max_dist,
                )
        if dist_enriched:
            dist_sorted = sorted(
                dist_enriched, key=lambda x: float(x.get("volume", 0.0)), reverse=True
            )
            pick_n = max(1, min(3, max_levels))
            out["distribution_zones"] = dist_sorted[:pick_n]
            logger.info(
                "LevelQualityFilter: selected %d distribution_zones (pick_n=%d)",
                len(out["distribution_zones"]),
                pick_n,
            )

        # Stop-hunt: лише найближчі рівні у межах stop_max_dist
        sh = levels_data.get("stop_hunt_levels") or []
        sh_vals: list[float] = []
        for i, lvl in enumerate(sh):
            p = LevelQualityFilter._get_price_from_level(lvl)
            if p is None:
                logger.debug(
                    "stop_hunt_levels[%d] ігнорується (невалидний формат): %r", i, lvl
                )
                continue
            distance_pct = abs(p - current_price) / max(current_price, 1e-9)
            if distance_pct <= stop_max_dist:
                sh_vals.append(float(p))
            else:
                logger.debug(
                    "stop_hunt_levels[%d] відкинуто (distance=%.4f > stop_max_dist=%.4f) price=%.6f",
                    i,
                    distance_pct,
                    stop_max_dist,
                    p,
                )
        if sh_vals:
            sh_vals.sort(key=lambda x: abs(x - current_price))
            out["stop_hunt_levels"] = sh_vals[:stop_top_n]
            logger.info(
                "LevelQualityFilter: selected %d stop_hunt_levels (top_n=%d)",
                len(out["stop_hunt_levels"]),
                stop_top_n,
            )

        # Логування результатів фільтрації — INFO для summary, DEBUG для деталей
        logger.debug(
            "LevelQualityFilter.filter_quality_levels: result_counts acc=%d dist=%d stop=%d",
            len(out["accumulation_zones"]),
            len(out["distribution_zones"]),
            len(out["stop_hunt_levels"]),
        )
        # TODO(metrics): тут додати emission метрик: counts_before/after та rejection_reasons,
        # щоб можна було відслідковувати вплив фільтра на сигнали та PnL.

        return out

    @staticmethod
    def calculate_overall_quality(filtered_levels: dict[str, Any]) -> float:
        """Грубий інтегральний скор якості 0..1 на основі присутності топових рівнів.

        Пояснення:
        - Дає швидку (cheap) proxy-метрику, чи є достатньо якісних рівнів для побудови сценаріїв.
        - Вага розподілена так, щоб distribution та stop-hunt мали більший вплив,
          оскільки вони частіше визначають ризики для входу.
        """
        try:
            acc_n = len(filtered_levels.get("accumulation_zones") or [])
            dist_n = len(filtered_levels.get("distribution_zones") or [])
            sh_n = len(filtered_levels.get("stop_hunt_levels") or [])
            # Нормалізація: очікуємо acc<=2, dist<=3, sh<=5
            acc_score = min(1.0, acc_n / 2.0)
            dist_score = min(1.0, dist_n / 3.0)
            sh_score = min(1.0, sh_n / 5.0)
            overall = float(
                round((0.4 * dist_score + 0.4 * sh_score + 0.2 * acc_score), 3)
            )
            logger.debug(
                "LevelQualityFilter.calculate_overall_quality: acc_n=%d dist_n=%d sh_n=%d overall=%.3f",
                acc_n,
                dist_n,
                sh_n,
                overall,
            )
            return overall
        except Exception:
            logger.exception(
                "LevelQualityFilter.calculate_overall_quality: помилка при обчисленні quality"
            )
            return 0.0

    # -------------------------
    # Місця для розширення логіки / мислення (TODO з поясненням)
    # -------------------------
    # TODO: Реалізувати адаптивні пороги (auto-tune) на основі історичних даних:
    #   Чому: статичні пороги можуть бути непридатні для різних інструментів/волатильності.
    #   Навіщо: зменшити manual tuning та підвищити стабільність сигналів.
    #
    # TODO: Додати опціональне логування rejection_reasons з деталізацією (для audit/QA):
    #   Чому: важливо розуміти, які зони відкидаються (недостатній обсяг vs. занадто далеко).
    #   Навіщо: швидка діагностика та аналіз false-negative випадків.
    #
    # TODO: Інтегрувати з config/config.py для отримання шаблонів Redis-ключів і TTL,
    #       а також feature-flags для тимчасового відключення фільтрів у продакшені.
    #   Чому: централізоване управління дозволяє швидко змінювати поведінку без релізу коду.
    #   Навіщо: оперативне реагування на регресії під час live-ринків.
    #
    # TODO(tests): Написати псевдо-стрім тести — feed historical windows and assert stable outputs.
    #   Чому: фільтри мають поводитись коректно при інкрементальному надходженні даних.
    #   Навіщо: виявлення багів, які проявляються тільки при послідовному обробленні.
