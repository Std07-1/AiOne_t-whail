## Опис змін

- Коротко: що зроблено і навіщо
- Посилання: issue/документація/артефакти

## Вплив на систему

- Етапи: Stage1 / Stage2 / Stage3 / UI / config / інше
- Контракти даних: зламані? — Ні (допускається лише meta.* / confidence_metrics)
- Redis: нові/змінені ключі? — лише через config; формат `ai_one:{domain}:{symbol}:{granularity}`, TTL із конфігу
- Фіче‑флаги: які додано/використано? стани за замовчуванням? rollback = off
- Перф: зміни у latency/wall_ms? бюджет ≤ +20% у піках; тяжке — в cpu_pool

## Логи і діагностика

- Маркери: [STRICT_PHASE] / [STRICT_SANITY] / [STRICT_WHALE] / [STRICT_GUARD] / [STRICT_RR] / [STRICT_VOLZ]
- Формат: `symbol ts phase scenario reasons presence bias rr gates`

## Тести

- [ ] Unit (happy + edge)
- [ ] Псевдо‑стрім (stream/stateful)
- [ ] Інтеграційний smoke на JSONL (без мережі)
- [ ] Smoke `python -m app.main` (таймаут 20с; код 124 — очікуваний timeout)
- [ ] Mini‑replay (за потреби) і KPI‑чек

## KPI / Acceptance (для фіче‑флагів)

- Breakout/Retest precision ≥ 0.7 (реальний vol_z)
- Directional‑guard → WAIT/OBSERVE у ≥95% конфліктів (без HOLD)
- Whale bias узгодженість із 1–3×ATR зсувом ≥ 60%

## Rollback план

- Які фіче‑флаги вимкнути або як revertнути PR

## Посилання на доки

- `docs/crisis_mode_status_2025-10-22.md`
- `docs/directional_gating_plan.md`
- `docs/whale_detection_v2_status.md`

---

# Чек‑лист перед merge

- [ ] Контракти Stage1/Stage2/Stage3 незмінні; нові поля лише у market_context.meta.* / confidence_metrics
- [ ] Redis‑ключі тільки через config; TTL з конфігу
- [ ] Minimal‑diff (≈≤30 LOC, 1–2 файли) або розбито на кроки
- [ ] Логи зі STRICT‑маркерами, без чутливих даних
- [ ] ruff + mypy + pytest — PASS
- [ ] Документація/докстрінги оновлені
