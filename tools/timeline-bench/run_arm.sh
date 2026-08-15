#!/bin/bash
# run_arm.sh <bench-dir> <arm-name> [duration-seconds] [worker-binary]
#
# Runs the channel worker against the bench lineup for a while, polling the
# playlist metadata into <arm>.segments.jsonl for analyze_arm.py. Touches the
# worker's heartbeat file on every poll, standing in for the server, so the
# 90s idle reaper does not end the run early.
set -e
BENCH=$(cd "$1" && pwd); ARM=$2; DUR=${3:-270}
WORKER=${4:-"$(cd "$(dirname "$0")/../.." && pwd)/target/debug/ersatztv-channel"}
OUT="$BENCH/out-$ARM"
rm -rf "$OUT"; mkdir -p "$OUT"
"$WORKER" run "$BENCH/channels/1/channel.json" --output-folder "$OUT" --number 1 \
  > "$BENCH/$ARM.worker.log" 2>&1 &
WPID=$!
echo "worker pid $WPID, running ${DUR}s"
MERGED="$BENCH/$ARM.segments.jsonl"
: > "$MERGED"
END=$((SECONDS + DUR))
while [ $SECONDS -lt $END ]; do
  if ! kill -0 $WPID 2>/dev/null; then echo "worker exited early"; break; fi
  touch "$OUT/.heartbeat"
  if [ -f "$OUT/live.m3u8.meta.json" ]; then
    cat "$OUT/live.m3u8.meta.json" >> "$MERGED" 2>/dev/null || true
    echo >> "$MERGED"
  fi
  sleep 2
done
kill $WPID 2>/dev/null || true
wait $WPID 2>/dev/null || true
echo "done; snapshots: $(grep -c . "$MERGED")"
