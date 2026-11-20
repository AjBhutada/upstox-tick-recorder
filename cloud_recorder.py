"""Cloud tick recorder (GitHub Actions). Records Upstox full-mode ticks for every stock in
watchlist.txt plus all F&O of FNO_UNDERLYINGS, until STOP (IST). Token from env UPSTOX_ACCESS_TOKEN.
Usage: python cloud_recorder.py <out_file> <HH:MM stop IST>
"""
import gzip, io, json, os, sys, time, urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo
import upstox_client

IST = ZoneInfo("Asia/Kolkata")
OUT, STOP = sys.argv[1], sys.argv[2]
EQUITIES = {l.strip() for l in open("watchlist.txt") if l.strip()}
FNO_UNDERLYINGS = {"IDEA"}
now = lambda: datetime.now(IST)

raw = urllib.request.urlopen("https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz", timeout=120).read()
keys, names = [], {}
for x in json.load(gzip.open(io.BytesIO(raw))):
    if x.get("segment") == "NSE_EQ" and x.get("instrument_type") == "EQ" and x.get("trading_symbol") in EQUITIES:
        keys.append(x["instrument_key"]); names[x["instrument_key"]] = x["trading_symbol"]
    if x.get("segment") == "NSE_FO" and x.get("underlying_symbol") in FNO_UNDERLYINGS:
        keys.append(x["instrument_key"]); names[x["instrument_key"]] = x["trading_symbol"]
json.dump(names, open(OUT.replace(".jsonl", "_instruments.json"), "w"))
print(f"{now():%H:%M:%S} subscribing {len(keys)} instruments, stop {STOP} IST", flush=True)

out = open(OUT, "a", buffering=1)
n = {"n": 0, "last": time.time()}
def on_message(msg):
    n["n"] += 1; n["last"] = time.time()
    out.write(json.dumps({"rx": time.time(), "msg": msg}, default=str) + "\n")
    if n["n"] % 5000 == 0:
        print(f"{now():%H:%M:%S} {n['n']} messages", flush=True)

cfg = upstox_client.Configuration(); cfg.access_token = os.environ["UPSTOX_ACCESS_TOKEN"]
s = upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(cfg), keys, "full")
s.on("message", on_message)
s.on("open", lambda: print(f"{now():%H:%M:%S} connected", flush=True))
s.on("error", lambda e: print("error:", e, flush=True))
s.on("close", lambda *a: print("closed", a, flush=True))
s.auto_reconnect(True, 5, 100)
s.connect()
while now().strftime("%H:%M") < STOP:
    time.sleep(10)
print(f"{now():%H:%M:%S} stopping, {n['n']} messages", flush=True)
s.disconnect(); out.close(); os._exit(0)
