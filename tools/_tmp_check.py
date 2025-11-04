import json
import os
import urllib.request

import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

sym = os.environ.get("SYM", "snxusdt").lower()
state_key = f"ai_one:state:{sym}"
print("-- HGETALL state (lower)")
print(r.hgetall(state_key))
print("-- HGETALL state (upper)")
print(r.hgetall(f"ai_one:state:{sym.upper()}"))

snapshot_key = "ai_one:ui_snapshot"
snap = r.get(snapshot_key)
if not snap:
    print("no snapshot present")
else:
    try:
        data = json.loads(snap)
        symbol_entry = next(
            (
                a
                for a in data.get("assets", [])
                if str(a.get("symbol", "")).lower() == sym
            ),
            None,
        )
        print("symbol found:", bool(symbol_entry))
        if symbol_entry:
            fields = {
                k: symbol_entry.get(k)
                for k in ("scenario_detected", "scenario_confidence")
            }
            meta = symbol_entry.get("market_context", {}).get("meta", {})
            fields_meta = {
                k: meta.get(k) for k in ("scenario_detected", "scenario_confidence")
            }
            print("asset_root:", fields)
            print("market_context.meta:", fields_meta)
    except Exception as e:
        print("snapshot parse error:", e)

port = int(os.getenv("PROM_HTTP_PORT", "9108"))
url = f"http://127.0.0.1:{port}/metrics"
try:
    with urllib.request.urlopen(url, timeout=2) as resp:  # nosec B310
        txt = resp.read().decode("utf-8", errors="ignore")
    lines = [
        ln
        for ln in txt.splitlines()
        if ln.startswith("ai_one_scenario") and sym.upper() in ln
    ]
    print("metrics lines:")
    for ln in lines[:5]:
        print(ln)
except Exception as e:
    print("metrics error:", e)
