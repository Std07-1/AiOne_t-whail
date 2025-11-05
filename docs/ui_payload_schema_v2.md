# UI Payload Schema v2 — коротка довідка

Мета цього документу — зафіксувати структуру вкладених полів у `UI/publish_full_state.py` та суміжних
шарів (Stage1/Stage2-lite телеметрія), щоб уникнути плутанини й спростити дебаг/аналіз.

Актуальна версія схеми в `payload.meta.schema_version`: `v2`.
Кожен актив також отримує `asset.schema_version = "v2"` (починаючи з цього коміту).

## Корінь payload
- type: рядок (назва каналу)
- meta: { ts: ISO8601Z, seq: int, schema_version: "v2" }
- counters: агрегати (alerts, active_trades, системні метрики тощо)
- assets: список активів (див. нижче)

## Актив (assets[])
Мінімальні ключі для UI‑таблиці (флет):
- symbol: UPPER
- price: float, price_str: рядок (без роздільників тисячних)
- volume_str: рядок (best‑effort)
- signal: "NONE" | "ALERT_BUY" | "ALERT_SELL" (мапінг від Stage2 recommendation)
- status: рядок ("normal", "alert", ...)
- band_pct: float (% коридору) — можливе джерело `stats.band_pct`
- confidence: float (0..1) — proxy з `stats.phase.score` або `stats.stage2_hint.score`
- htf_ok: bool — з `market_context.meta.htf_ok`
- schema_version: "v2"
- analytics: { corridor_band_pct, corridor_dist_to_edge_pct|ratio, corridor_near_edge|within, low_volatility_flag, ... }
- market_context.meta: { atr_pct, low_gate, corridor{...}, htf_alignment, htf_ok, ... }

Опційні блоки:
- whale (канонічний):
  - version: рядок (опц.)
  - ts: ISO8601Z
  - age_ms: float (опц.)
  - stale: bool (опц.)
  - presence: float (0..1)
  - bias: float (-1..1) або (0..1) — див. конкретну імплементацію
  - vwap_dev: float (абс. відхилення)
  - dev_level: "low"|"mid"|"strong" (опц.)
  - zones_summary: { accum: int, dist: int, ... } (опц.)
  - vol_regime: "normal"|"high"|"hyper" (опц.)

  Примітка: у publisher виконується нормалізація legacy‑полів:
  - presence_score → presence
  - vwap_deviation → vwap_dev
  - zones → zones_summary

- insight: { ... } — телеметрія стадії інсайтів (best‑effort)
- stage3: { rr, unrealized_pct, mfe_pct, mae_pct, trail{...} } — якщо доступно
- tp_sl: рядок псевдо‑формату "TP: x | SL: y" (лише зі Stage3 targets)

## trap (телеметрія Stage1)
- У поточній схемі відображається через `stats`/`market_context.meta`/логи; окремого блоку немає.
- Ключові атрибути TRAP (dry‑run) — див. `config.config: STAGE1_TRAP` і строгі логи `[TRAP]`.

## phase (Stage2-lite, телеметрія)
- Розміщення: `stats.phase`: { name, score, reasons?, ttl? } (best‑effort)
- Окремо в root: `confidence` бере score із цього блоку (або `stats.stage2_hint`).

### insights.quiet_mode (опційно)
- Розміщення: `market_context.meta.insights.quiet_mode: bool`, `market_context.meta.insights.quiet_score: float`
- Призначення: сигналізує «тихий тренд» (participation‑light) із помірним нахилом ціни за умов обмеженої участі (низький DVR) — телеметрія лише для UI/аналітики, не змінює контракти та рішення.
- Походження: Stage2‑lite фазний детектор (за фіче‑флагом `FEATURE_PARTICIPATION_LIGHT`).
- Сумісність: відсутність поля означає, що інсайт неактивний або не застосовний для поточного бару.

## stage2_hint (телеметрія → SOFT_ рекомендації в UI)
- `stats.stage2_hint`: { dir: "BUY"|"SELL", score: 0..1, reasons?: [...], ttl?: sec }
- Легка логіка SOFT_ рекомендацій у UI‑publisher (за фіче‑флагом `SOFT_RECOMMENDATIONS_ENABLED`):
  - умови: `htf_ok=True`, не hyper‑волатильність, `score ≥ SOFT_RECO_SCORE_THR`
  - результат: `recommendation = SOFT_BUY|SOFT_SELL` (не перезаписуємо "жорсткі" рекомендації)

## trigger_reasons vs raw_trigger_reasons
- Stage1Signal контракт: `trigger_reasons` — канонічні теги; `raw_trigger_reasons` — сирі назви детекторів.
- Допустимо, що при `signal=NORMAL` масиви порожні.
- У UI‑publisher: якщо `raw_trigger_reasons == trigger_reasons` — raw прибирається для зменшення шуму.

## tags (приклади)
- `whale_stale` — whale‑телеметрія застаріла (age_ms/ts)
- `false_breakout` — сценарій хибного пробою (за фазним детектором)
- `near_high/near_low`, `low_volatility`, `vol_spike`, `pre_breakout` тощо

Просимо розширювати цей розділ у PR, якщо додаються нові теги/поля.

## Зміни у версіях
- v2: додано flatten‑поля, analytics.*, stage3 з trail‑метриками, SOFT_ рекомендації (за флагом).
- Сумісність: UI спирається на `schema_version` і має fallback на ключові поля (price, signal, band_pct, confidence).

```text
Контакти схеми та правила:
- Контракти Stage1/Stage2/Stage3 незмінні; нові поля — лише в market_context.meta.* або telemetry.
- Redis‑ключі формуються через config.keys.build_key, формат ai_one:{domain}:{symbol}:{granularity}
- Логи — [STRICT_*], без чутливих даних.
```
