#!/bin/bash
# run_arm.sh <bench-dir> <arm-name> [duration-seconds] [worker-binary]
#
# Runs the channel worker against the bench lineup for a while, polling the
# published playlist into <arm>.playlists.txt for analyze_arm.py. Touches the
# worker's heartbeat file on every poll, standing in for the server, so the
# idle reaper does not end the run early.
#
# The published playlist is a rolling ten segment window, so the poll has to
# be faster than the window advances. Segments are four seconds; two is safe.
set -e
BENCH=$(cd "$1" && pwd); ARM=$2; DUR=${3:-270}
WORKER=${4:-"$(cd "$(dirname "$0")/../.." && pwd)/target/debug/ersatztv-channel"}
OUT="$BENCH/out-$ARM"
rm -rf "$OUT"; mkdir -p "$OUT"
"$WORKER" run "$BENCH/channels/1/channel.json" --output-folder "$OUT" --number 1 \
  > "$BENCH/$ARM.worker.log" 2>&1 &
WPID=$!
echo "worker pid $WPID, running ${DUR}s"
MERGED="$BENCH/$ARM.playlists.txt"
: > "$MERGED"
END=$((SECONDS + DUR))
while [ $SECONDS -lt $END ]; do
  if ! kill -0 $WPID 2>/dev/null; then echo "worker exited early"; break; fi
  touch "$OUT/.heartbeat"
  if [ -f "$OUT/live.m3u8" ]; then
    echo "===SNAPSHOT===" >> "$MERGED"
    cat "$OUT/live.m3u8" >> "$MERGED" 2>/dev/null || true
  fi
  sleep 2
done
kill $WPID 2>/dev/null || true
wait $WPID 2>/dev/null || true
echo "done; snapshots: $(grep -c '===SNAPSHOT===' "$MERGED")"
