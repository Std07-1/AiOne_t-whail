Ось оновлена **Copilot Quick Reference — AiOne_t • v2025-10-29r**. Замініть файл повністю.

## TL;DR

* Контракти Stage1/2/3 **не чіпати**. Нове → `market_context.meta.*` або `confidence_metrics.*`.
* Усі зміни під фіче-флагами з `config/config.py`. Rollback = вимкнути флаг.
* Redis-ключі лише через `config`, формат `ai_one:{domain}:{symbol}:{granularity}`.
* Перф-бюджет: ≤ +20% `avg_wall_ms` у піках. Важке → `cpu_pool`. I/O → best-effort.
* PR: minimal-diff (≈≤30 LOC, 1–2 файли) + тести (unit + pseudo-stream) + STRICT-логи.

## Фіче-флаги (мають бути в конфігу)

```
STRICT_ACCUM_CAPS_ENABLED
STRICT_ZONE_RISK_CLAMP_ENABLED
STRICT_HTF_GRAY_GATE_ENABLED
STRICT_LOW_ATR_OVERRIDE_ON_SPIKE
PROM_GAUGES_ENABLED
PROM_HTTP_PORT

PRESENCE_CAPS = {"accum_only_cap": 0.30}
HTF_GATES = {"gray_low": 0.10, "ok": 0.20, "gray_penalty": 0.20}
STAGE2_PRESENCE_PROMOTION_GUARDS = {"cap_without_confirm": 0.20}
LOW_ATR_SPIKE_OVERRIDE = {
  "band_expand_min": 0.013, "spike_ratio_min": 0.60,
  "abs_volz_min": 2.50, "dvr_min": 1.20, "bars_ttl": 3
}
```

## Guard-правила, які вже діють

* **Low-ATR override**: Stage1 озброює `low_atr_override` за умовою з `LOW_ATR_SPIKE_OVERRIDE`; Stage2 читає **Redis → stats → none**. Override лише знімає `low_atr_guard`.
* **ACCUM_MONITOR постпроцесор**: якщо `phase=None ∧ atr_ratio<1.0 ∧ !override` → `phase="ACCUM_MONITOR"`, `score=0`, reason `low_atr_guard`.
* **HTF “сіра” зона**:
  `htf_strength<0.10` або `htf_stale_ms>stage1_htf_stale_ms` → `htf_ok=False`;
  `0.10≤htf_strength<0.20` → `score -= gray_penalty` (не нижче 0).
* **Flat-policy**: `dist_zones=0 ∧ liquidity_pools=0` →
  `presence ≤ PRESENCE_CAPS.accum_only_cap` **і** `risk_from_zones ≤ cap_without_confirm`.
  Відсутній orderbook → **не штрафувати** (просто не додавати компонент).

## Логи

Використовувати маркери:
`[STRICT_PHASE] [STRICT_SANITY] [STRICT_WHALE] [STRICT_GUARD] [STRICT_RR] [STRICT_VOLZ] [LOW_ATR_OVERRIDE] [HTF]`
Порядок полів: `symbol ts phase scenario reasons presence bias rr gates`.

## Тести, які мають бути в пакеті

* `presence_cap_flat`, `risk_clamp_flat`, `accum_monitor_fallback`, `htf_gray_stale`, `low_atr_override_ttl`.
* Псевдо-стрім на 2–3 активи з `--strict-datetime-match`.

## CLI для A/B (якщо додано)

```
--enable/--disable-accum-caps
--enable/--disable-htf-gray
--enable/--disable-low-atr-spike-override
--prometheus
```

## Метрики Prometheus (опційно, під прапором)

* `low_atr_override_active{symbol}`, `low_atr_override_ttl{symbol}`, `htf_strength{symbol}`, `presence{symbol}`.
* Ендпоінт: `http://localhost:{PROM_HTTP_PORT}/metrics`.

## Acceptance для вмикання впливових флагів

* `Breakout/Retest` precision ≥ 0.7 (реальний `vol_z`).
* Directional-guard: WAIT/OBSERVE у ≥95% конфліктів.
* Whale bias узгоджується з 1–3×ATR зсувом ≥ 60%.
* TRX у флєті: `presence≤0.30`, `risk≤0.20`, `phase=ACCUM_MONITOR`, без рекомендацій.
* Без `RuntimeWarning` у CSV/Parquet.

## Заборони

* Хардкод ключів/порогів/таймінгів.
* Зміни API/сигнатур Stage1/2/3.
* Вимикати фільтри/валідації без фіче-флагів.
* Ігнорувати `REDIS_CACHE_TTL` та `INTERVAL_TTL_MAP`.
* Дублювати утиліти — використовуй `utils`/`stage1.indicators`.

## Швидкі посилання

* Повні правила: `.github/copilot-instructions.md`
* Конфіг/константи: `config/config.py`, `config/config_whale.py`, `config/config_stahges2.py`, `config/flags.py`
* Док-плани: `docs/crisis_mode_status_2025-10-22.md`, `docs/directional_gating_plan.md`, `docs/whale_detection_v2_status.md`
* Пам'ять Copilot: `.github/copilot-memory.md`