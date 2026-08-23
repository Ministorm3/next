import json, sys, math, glob
from datetime import datetime

bench = sys.argv[1]; arm = sys.argv[2]
def parse_iso(s): return datetime.fromisoformat(s)

# merge all snapshots: segment path -> (pdt, item_id, duration)
segs = {}
for line in open(f"{bench}/{arm}.segments.jsonl"):
    line = line.strip()
    if not line: continue
    try: d = json.loads(line)
    except json.JSONDecodeError: continue
    for s in d.get("segments", []):
        segs[s["path"]] = s

# first segment PDT per item occurrence: items repeat (shuffled playlist),
# so key on item_id which the playout generator makes unique per slot
first = {}
for s in sorted(segs.values(), key=lambda s: s["path"]):
    iid = s["item_id"]
    if iid not in first:
        first[iid] = parse_iso(s["program_date_time"])

# playout schedule
pf = sorted(glob.glob(f"{bench}/channels/1/playout/*.json"))[0]
sched = {}
for it in json.load(open(pf))["items"]:
    src = it.get("source") or {}
    ph = src.get("probe_hint") or {}
    vid = ph.get("video") or []
    fr = vid[0].get("frame_rate") if vid else None
    sched[it["id"]] = (parse_iso(it["start"]), parse_iso(it["finish"]), fr,
                       src.get("path","?").split("/")[-1])

rows = []
for iid, pdt in first.items():
    if iid not in sched: continue
    st, fi, fr, path = sched[iid]
    rows.append((st, iid, path, fr, (fi-st).total_seconds(), (pdt-st).total_seconds()*1000))
rows.sort()
if not rows:
    print("no rows"); sys.exit(0)

base = rows[0][5]  # boot offset: drift of the first item
print(f"{arm}: {len(rows)} items; first-item baseline {base:.1f}ms (subtracted)")
print(f"{'item':>9} {'file':>7} {'fps':>11} {'slot_s':>9} {'drift_ms':>9} {'step':>7} {'pred_step':>9}")
prev = base
cum_pred = 0.0
for st, iid, path, fr, slot, drift in rows:
    step = drift - prev
    pred = ""
    if fr:
        num, den = (fr.split("/") + ["1"])[:2]
        fps = float(num)/float(den)
        p = (math.ceil(slot*fps - 1e-9)/fps - slot)*1000
        cum_pred += p
        pred = f"{p:9.1f}"
    print(f"{iid:>9} {path:>7} {fr:>11} {slot:9.3f} {drift-base:9.1f} {step:7.1f} {pred}")
    prev = drift
print(f"\ncumulative: measured {rows[-1][5]-base:+.1f}ms over {len(rows)} items; ceil-law predicts {cum_pred:+.1f}ms (padded arms only)")
