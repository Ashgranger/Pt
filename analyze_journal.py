"""Offline conditional-markout report. Usage: python analyze_journal.py journal_nvda_v2.jsonl [horizon_s=5]
Joins fills with their markouts and prints mean markout (bps) + count by level, ET hour, obi/tfi alignment, spread."""
import json, sys, collections

path = sys.argv[1]
H = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0
fills, mo = {}, {}
for line in open(path):
    try:
        r = json.loads(line)
    except Exception:
        continue
    if r.get("type") == "markout":
        if abs(r["h"] - H) < 0.05:
            mo[r["fill_ts"]] = r["markout_bps"]
    elif "side" in r:
        fills[r["ts"]] = r

rows = []
for ts, f in fills.items():
    if ts not in mo:
        continue
    sgn = 1 if f["side"] == "BUY" else -1
    obi = float(f.get("obi", 0)) * sgn      # >0 => book leaning our way (buyers stacked on a BUY fill)
    tfi = float(f.get("tfi", 0)) * sgn      # >0 => recent flow in our direction (BUY fill with buying flow = adverse for us? see note)
    rows.append(dict(level=f.get("level", -1), et=(f.get("et") or "??:??")[:2], obi=obi, tfi=tfi,
                     spr=float(f.get("spr_bps", 0)), m=mo[ts]))

def report(name, keyf):
    g = collections.defaultdict(list)
    for r in rows:
        g[keyf(r)].append(r["m"])
    print(f"\n== {name} ==")
    for k in sorted(g, key=str):
        v = g[k]
        print(f"{str(k):>10}  n={len(v):4d}  mean={sum(v)/len(v):+.3f}bps  win={sum(x>0 for x in v)/len(v):.0%}")

print(f"{len(rows)} fills with {H}s markout | overall mean {sum(r['m'] for r in rows)/max(1,len(rows)):+.3f}bps")
report("level", lambda r: r["level"])
report("ET hour", lambda r: r["et"])
report("book lean (obi*side)", lambda r: "against" if r["obi"] < -0.3 else ("with" if r["obi"] > 0.3 else "flat"))
report("flow (tfi*side)", lambda r: "against" if r["tfi"] < -0.3 else ("with" if r["tfi"] > 0.3 else "flat"))
report("spread", lambda r: "<=0.5bps" if r["spr"] <= 0.5 else ">0.5bps")
print("\nQuote only buckets with mean > ~0.2bps (half-spread) and n >= 30.")
