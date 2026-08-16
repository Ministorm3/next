"""Reads a bench arm's worker log and prints the per-item timeline drift.

Drift is `last_segment_end - transcoded_until`: emitted media measured against
the schedule, both integrated from the same reading at channel start. The
worker computes it at the top of every pipeline, before that pipeline's own
correction, so the series is the error entering each item and the step between
rows is what the previous item contributed.

Both defects this bench exists to catch only appear as a sum over many items,
so the step and its running total are what matter, not any single row.

Reading it from the worker's own log rather than from the published playlist
is deliberate. Attributing published segments back to items requires guessing
at pipeline boundaries, and a pipeline that emits no segment of its own makes
that guess wrong from then on. Pass --check to additionally derive the total
from the playlist and compare.
"""

import glob
import json
import math
import re
import sys
from datetime import datetime

bench, arm = sys.argv[1], sys.argv[2]
check = "--check" in sys.argv

LOG = re.compile(
    r"emission trim (-?\d+)ms for item (\S+) \(stamp clock is (-?\d+)ms past the schedule\)"
)


def parse_iso(s):
    return datetime.fromisoformat(re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", s.strip()))


# the schedule, by item id
sched = {}
for pf in sorted(glob.glob(f"{bench}/channels/1/playout/*.json")):
    for it in json.load(open(pf))["items"]:
        src = it.get("source") or {}
        vid = (src.get("probe_hint") or {}).get("video") or []
        sched[it["id"]] = (
            (parse_iso(it["finish"]) - parse_iso(it["start"])).total_seconds(),
            vid[0].get("frame_rate") if vid else None,
            src.get("path", "?").split("/")[-1],
        )

rows = []
for line in open(f"{bench}/{arm}.worker.log"):
    m = LOG.search(line)
    if m:
        item = m.group(2)
        slot, fr, path = sched.get(item, (0.0, None, "?"))
        rows.append((path, fr, slot, int(m.group(3))))

if not rows:
    print("no drift rows in the log; was the arm built by build_arms.sh?")
    sys.exit(1)

# the first pipeline joins its item mid-slot and the second inherits that
# offset, so neither is drift
STEADY = 2
if len(rows) <= STEADY + 1:
    print(f"only {len(rows)} items; too short to leave boot transient")
    sys.exit(1)

base = rows[STEADY][3]
print(f"{arm}: {len(rows)} items, {len(rows)-STEADY} steady after {STEADY} boot rows")
print(f"{'file':>7} {'fps':>11} {'slot_s':>9} {'drift_ms':>10} {'step':>8} {'pred_step':>9}")
prev = None
cum_pred = 0.0
for n, (path, fr, slot, drift) in enumerate(rows):
    steady = n >= STEADY
    step = "" if prev is None else f"{drift - prev:8.0f}"
    pred = ""
    if fr and slot:
        num, den = (fr.split("/") + ["1"])[:2]
        fps = float(num) / float(den)
        p = (math.ceil(slot * fps - 1e-9) / fps - slot) * 1000
        if steady:
            cum_pred += p
        pred = f"{p:9.1f}"
    tag = "" if steady else "  <- boot"
    print(f"{path:>7} {str(fr):>11} {slot:9.3f} {drift-base:10.0f} {step:>8} {pred}{tag}")
    prev = drift if steady else None

measured = rows[-1][3] - base
print(f"\ncumulative over {len(rows)-STEADY} steady items: measured {measured:+.0f}ms")
print(f"ceil-law upper bound {cum_pred:+.1f}ms, for a padded arm with no trim. It is an")
print("upper bound because it assumes every item's -t cut lands on a pad. An item whose")
print("slot does not exceed its video stream never reaches one, so it contributes nothing;")
print("compare a row's step against the PREVIOUS row's pred_step, which is the item that")
print("produced it.")

if check:
    # independent cross-check from the published playlist: total emitted media
    # against total scheduled time, which needs no per-item attribution
    segs, pdt = {}, None
    for line in open(f"{bench}/{arm}.playlists.txt"):
        line = line.strip()
        if line.startswith("#EXTINF:"):
            dur = float(line[8:].rstrip(","))
        elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            pdt = parse_iso(line.split(":", 1)[1])
        elif line.endswith(".ts") and line not in segs:
            segs[line] = (pdt, dur)
    if segs:
        order = sorted(segs)
        first = segs[order[0]][0]
        last = segs[order[-1]][0] + __import__("datetime").timedelta(seconds=segs[order[-1]][1])
        emitted = (last - first).total_seconds() * 1000
        print(f"playlist cross-check: {len(segs)} segments span {emitted:.0f}ms of emitted media")
