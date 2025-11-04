import json
from datetime import UTC, datetime
from pathlib import Path

base = Path("datastore")
if not base.exists():
    raise SystemExit("datastore directory missing")

records_by_symbol: dict[str, list[dict[str, object]]] = {}
candidates: list[dict[str, object]] = []
for path in sorted(base.glob("*_bars_1m_snapshot.jsonl")):
    symbol = path.stem.replace("_bars_1m_snapshot", "").upper()
    entries: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            entries.append(rec)
            op = float(rec["open"])
            cl = float(rec["close"])
            hi = float(rec["high"])
            lo = float(rec["low"])
            vol = float(rec["volume"])
            ts = int(rec["open_time"])
            pct_change = (cl - op) / op if op else 0.0
            range_pct = (hi - lo) / op if op else 0.0
            candidates.append(
                {
                    "symbol": symbol,
                    "ts": ts,
                    "index": len(entries) - 1,
                    "ts_iso": datetime.fromtimestamp(ts / 1000, tz=UTC)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "pct_change": pct_change,
                    "range_pct": range_pct,
                    "volume": vol,
                    "open": op,
                    "close": cl,
                    "high": hi,
                    "low": lo,
                }
            )
    records_by_symbol[symbol] = entries

candidates.sort(
    key=lambda r: (abs(r["pct_change"]), r["range_pct"], r["volume"]), reverse=True
)

for rec in candidates[:10]:
    print(
        f"pct={rec['pct_change']*100:.2f}% range={rec['range_pct']*100:.2f}% "
        f"vol={rec['volume']:.2f} sym={rec['symbol']} ts={rec['ts']} ({rec['ts_iso']}) open={rec['open']} "
        f"close={rec['close']} high={rec['high']} low={rec['low']}"
    )

TOP_N = 2
WINDOW_PRE = 30
WINDOW_POST = 30
replay_dir = base / "replay"
replay_dir.mkdir(exist_ok=True)

for rec in candidates[:TOP_N]:
    symbol = str(rec["symbol"])
    entries = records_by_symbol.get(symbol)
    if not entries:
        continue
    idx = int(rec["index"])
    start = max(0, idx - WINDOW_PRE)
    end = min(len(entries), idx + WINDOW_POST + 1)
    subset = entries[start:end]
    if not subset:
        continue
    iso_for_file = str(rec["ts_iso"]).replace(":", "-").replace("Z", "Z")
    file_name = f"{symbol.lower()}_{iso_for_file}_replay_1m.jsonl"
    out_path = replay_dir / file_name
    with out_path.open("w", encoding="utf-8") as fh:
        for item in subset:
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(
        f"Replay chunk written: {out_path} | bars={len(subset)} window=[{start},{end - 1}]"
    )
