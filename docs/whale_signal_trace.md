# Whale Signal Trace (replay 2025-11-14)

Джерело: `reports/whale_replay_2h/run.log` (запуск `tools.unified_runner replay --limit 120 --interval 1m --symbols BTCUSDT,ETHUSDT,TONUSDT,SNXUSDT --source snapshot --namespace ai_one_whale_replay --set WHALE_MINSIGNAL_V1_ENABLED=true --report`). Нижче — технічний трейс для BTCUSDT і TONUSDT.

## BTCUSDT

### Ланцюжок подій
1. **19:35:36 (епізод A, рядки 15–82)**
   WhaleTelemetry дала `presence=0.013` (без orderbook, тільки одна accum-зона) і `bias=0.000`. Гварди одразу спрацювали (`low_volatility`, `low_atr`, `heavy_compute`), а `[HTF]` повернув `ok=False strength=0.0191 (gray_low)`. Фазу не визначено (`phase=None`, `missing_fields=0`). Жодних згадок `_enforce_whale_signal_v1` чи Stage3 — сигнал глохне до мін-сигналу.
2. **19:35:37 (епізод B, рядки 304–402)**
   Через VWAP-флаг Presence зростає до `0.263`, `bias=-0.600` (net sell). Фазовий блок знаходить `false_breakout (score=0.80)`, але той самий бар фіксує `[HTF] ... ok=False reason=gray_low strength≈0.016`, тому сценарії не стартують і Stage3 не бачить кандидата.
3. **19:35:40–19:35:41 (епізод C, рядки 1830–1890)**
   Зустрічається короткий сплеск `HTF ok=True`, проте після фазового перерахунку одразу прилітає `ok=False gray_low` й `phase=None`. WhaleTelemetry у цих проходах дає лише `presence≈0.025` (iceberg=True, але orderbook пустий), тож навіть за валідного HTF мін-сигнал не формується й `_enforce` не логувався.

### Агрегований підсумок
- WhaleTelemetry викликів: **200** (усі `presence≤0.263`, профіль strong не з'являвся).
- `_enforce_whale_signal_v1` / `whale_signal_v1`: **0** логів → мін-сигнал взагалі не публікується.
- Напрями: усі зафіксовані упередження `bias∈{0.000,-0.600}` (long не бачитись).
- HTF Gate: **63** проходження з `ok=True` vs **123** з `ok=False` (більшість «gray_low» навіть тоді, коли сила <0.1). Після кожного `ok=True` швидко йде зворотний запис `ok=False` → гейт фактично закритий.
- Фази: `phase=None` — 86 разів, `false_breakout` — 3, `drift_trend` — 7, `exhaustion` — 3, `momentum` — 1 (усі з `htf_conflict`).
- Сценарії та Stage3: жодних `ScenarioTrace`, `scenario_gate`, `Act` або `no_act` записів → Stage3 не активувався.

## TONUSDT

### Ланцюжок подій
1. **19:36:05 (епізод A, рядки 17120–17170)**
   WhaleTelemetry відразу дає `presence=0.487` завдяки VWAP+iceberg та одному dist-рівню; `bias=-0.620`. Попри сильний сирий сигнал, `[HTF] ok=False (gray_penalty strength=0.1488)` і `phase=None`. Ланцюг обривається ще до мін-сигналу.
2. **19:36:05 (епізод B, рядки 17182–17210)**
   Повторний прогін дає ті самі гварди (`low_volatility`, `low_atr`, `heavy_compute`). Phase-блок знову не знаходить фазу, `HTF` тримається в `gray_low`, `_enforce` не викликається.
3. **19:36:06 (епізод C, рядки 17600–17670)**
   Фаза нарешті визначена як `false_breakout (score=0.80)` із причинами `weak_directional_volume`, `cd_against`, `htf_conflict`, але `htf_ok=False` з `strength≈0.26`. WhaleTelemetry водночас показує `presence=0.487`, `bias=-0.620`. Через `htf_conflict` сценарії й Stage3 не стартують, `whale_signal_v1` не фіксується.

### Агрегований підсумок
- WhaleTelemetry викликів: **200** (більшість `presence≈0.487`, тобто сирий сигнал потенційно «soft/strong»).
- `_enforce_whale_signal_v1` / `whale_signal_v1`: **0** записів → мін-сигнал жодного разу не вийшов назовні.
- Напрями: усі зафіксовані `bias=-0.620` (кореляція з перманентним sell-присутністю).
- HTF Gate: лише **19** проходів з `ok=True` проти **136** відмов (`gray_low`/`gray_penalty`). Навіть при `ok=True` наступні записи повертають `ok=False` до завершення фазового блоку.
- Фази: `phase=None` — 87 разів, `false_breakout` — 10 (усі з `htf_conflict`). Інакших фаз не спостерігалось.
- Сценарії та Stage3: відсутні `ScenarioTrace`/`Act`/`no_act` логі — рекомендаційний шар не отримав жодного кандидата.

## Висновок (чернетка)
- Обидва символи мають сирі whale-сигнали (presence до 0.26 у BTC і ≈0.49 у TON), але ланцюг глохне ще до `_enforce_whale_signal_v1`: у логах взагалі немає записів про мін-сигнал чи Stage3.
- Найчастіший обрив — HTF/phase гейт: 60–80% циклів закінчуються `phase=None` + `htf_conflict` (`gray_low`). Навіть коли HTF коротко дозволяє `ok=True`, наступний запис його скидає до `ok=False`, тому сценраії не стартують.
- Жодного кейсу, де strong/soft профіль пройшов би в Act: Stage3-логів (`Act`, `no_act`, `risk_reject`) не існує в `run.log` для цих символів.

## Phase reject reasons (live 2025-11-14)

Джерело: `reports/run_phase_diag/run.log` (живий прогон `tools.run_window --duration 900 --out-dir reports/run_phase_diag --log reports/run_phase_diag/run.log --set STATE_NAMESPACE=ai_one_phase_diag --set PROM_HTTP_PORT=9108 --set PROM_GAUGES_ENABLED=true --report`). За 15 хвилин зібрано всі `phase=None` проходи для BTCUSDT і TONUSDT.

| Символ | Всього phase=None | Топ-3 причин | Пояснення |
| --- | --- | --- | --- |
| BTCUSDT | 46 | `presence_cap_no_bias_htf` — 45, без тега — 1 | Raw presence ~0.55, але guard обрізає до 0.20, бо `htf_strength≈0.18` → відхилення clamp-ом до ковзного мінімуму. |
| TONUSDT | 69 | `presence_cap_no_bias_htf` — 57, `reasons=` порожній — 12 | Сирий presence ~0.35 теж обрізається до 0.20 під тим самим гвардом; ще 12 записів не дали тегів (phase адаптер повернув `None`, лише `threshold_tag`). |

Ключові спостереження:
- `presence_cap_no_bias_htf` домінує (98% для BTC, 83% для TON), тобто відмови спричиняє подвійний clamp: HTF у зоні gray_low та відсутність покупчого/продажного ухилу.
- Порожні причини (`reasons=` без значення) — окремі проходи phase-адаптера, де гварди не записали тег; ці 12 випадків також мають `htf_ok=False`, а отже підпадають під ті ж обмеження.
- Жодного випадку, де `presence_cap_no_bias_htf` не спрацював би повторно, навіть коли raw presence виростає (BTC 0.55, TON 0.35): гейт тримається закритим поки HTF не перейде у `ok ≥ 0.20`.
