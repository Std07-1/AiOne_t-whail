#!/usr/bin/env python3
"""
Stage3 Canary (paper): мінімальний раннер для фіксації «паперових» угод на подію сценарію.

- Без змін контрактів Stage1/2/3; працює лише під прапором STAGE3_PAPER_ENABLED.
- Ліміти ризику зчитуються з config.config (RISK_PER_TRADE_R, MAX_TRADES_PER_SYMBOL, MAX_TRADES_PER_DAY).
- Подія обробки: handle_scenario_alert(symbol, scenario, confidence, dir="BUY"|"SELL").
- Запис у JSONL: reports/paper_trades.jsonl
- Прометей-лічильник: ai_one_paper_trades_total{symbol,scenario}

Призначення: інфраструктурний гак для канарейки. Реальне підключення до пайплайна буде виконано окремо.
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    from prometheus_client import Counter  # type: ignore
except Exception:  # pragma: no cover - пром залежність опційна
    Counter = None  # type: ignore

from config.config import (
    MAX_TRADES_PER_DAY,
    MAX_TRADES_PER_SYMBOL,
    RISK_PER_TRADE_R,
    STAGE3_PAPER_ENABLED,
    STAGE3_PAPER_SYMBOLS,
)

_PAPER_OUT = Path("reports") / "paper_trades.jsonl"
_lock = threading.Lock()
_day_counters: dict[str, int] = {}
_total_counter: int = 0

if Counter is not None:
    PAPER_TRADE_TOTAL = Counter(
        "ai_one_paper_trades_total",
        "Paper trades created by Stage3 canary",
        labelnames=("symbol", "scenario"),
    )
else:  # pragma: no cover
    PAPER_TRADE_TOTAL = None  # type: ignore


@dataclass
class PaperTrade:
    ts: str
    symbol: str
    scenario: str
    confidence: float
    direction: str
    risk_r: float
    reason: str
    meta: dict[str, Any]


def _today_key() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _allowed(symbol: str) -> bool:
    sym = symbol.upper()
    if STAGE3_PAPER_SYMBOLS and sym not in {s.upper() for s in STAGE3_PAPER_SYMBOLS}:
        return False
    # добовий ліміт сумарно
    global _total_counter
    # пер-символьний
    per_sym = _day_counters.get(sym, 0)
    if MAX_TRADES_PER_SYMBOL and per_sym >= int(MAX_TRADES_PER_SYMBOL):
        return False
    if MAX_TRADES_PER_DAY and _total_counter >= int(MAX_TRADES_PER_DAY):
        return False
    return True


def handle_scenario_alert(
    symbol: str,
    scenario: str,
    confidence: float,
    direction: str,
    reason: str = "scenario_alert",
    meta: dict[str, Any] | None = None,
) -> bool:
    """Обробляє подію сценарію і створює запис «паперової» угоди під лімітами.

    Повертає True, якщо запис створено; False — якщо відхилено лімітами або вимкнено.
    """
    if not STAGE3_PAPER_ENABLED:
        return False
    if not _allowed(symbol):
        return False
    payload = PaperTrade(
        ts=datetime.now(UTC).isoformat(),
        symbol=symbol.upper(),
        scenario=str(scenario),
        confidence=float(confidence or 0.0),
        direction=str(direction).upper(),
        risk_r=float(RISK_PER_TRADE_R),
        reason=reason,
        meta=dict(meta or {}),
    )
    # Атомарний запис та інкременти лічильників у межах процесу
    with _lock:
        os.makedirs(_PAPER_OUT.parent, exist_ok=True)
        _PAPER_OUT.open("a", encoding="utf-8").write(json.dumps(asdict(payload)) + "\n")
        # лічильники доби
        _day_counters[payload.symbol] = _day_counters.get(payload.symbol, 0) + 1
        global _total_counter
        _total_counter += 1
    if PAPER_TRADE_TOTAL is not None:
        try:
            PAPER_TRADE_TOTAL.labels(
                symbol=payload.symbol, scenario=payload.scenario
            ).inc()
        except Exception:
            pass
    # Легке логування для прозорості пайплайна
    try:
        ts_ms = int((meta or {}).get("ts_ms") or 0)
    except Exception:
        ts_ms = 0
    print(
        f"[STAGE3_PAPER] symbol={payload.symbol} scenario={payload.scenario} dir={payload.direction} p={payload.confidence:.2f} ts_ms={ts_ms}"
    )
    return True


def main() -> int:
    # Мінімальний ехо-режим: показати, що раннер увімкнено і читає конфіг
    enabled = STAGE3_PAPER_ENABLED
    syms = ",".join(STAGE3_PAPER_SYMBOLS)
    print(
        f"Stage3 paper runner: enabled={enabled} symbols=[{syms}] R={RISK_PER_TRADE_R} limits: per_sym={MAX_TRADES_PER_SYMBOL} per_day={MAX_TRADES_PER_DAY}"
    )
    print(
        "Цей раннер не слухає шину подій; інтеґрація з пайплайном буде додана окремо."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
