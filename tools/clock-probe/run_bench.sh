#!/bin/bash
# run_bench.sh <bench-dir> <run-name> [duration-seconds] [worker-binary]
#
# Runs the channel worker against the bench lineup with the clock trace on,
# then leaves <bench>/trace/clock-1.jsonl for ersatztv-clock-probe to read.
#
# Touches the heartbeat file on every poll, standing in for the server, so the
# idle reaper does not end the run early.
set -e
BENCH=$(cd "$1" && pwd); RUN=$2; DUR=${3:-270}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
WORKER=${4:-"$ROOT/target/debug/ersatztv-channel"}

OUT="$BENCH/out-$RUN"
TRACE="$BENCH/trace-$RUN"
rm -rf "$OUT" "$TRACE"
mkdir -p "$OUT" "$TRACE"

ETV_CLOCK_TRACE="$TRACE" \
ETV_CLOCK_TRACE_LEVEL="${ETV_CLOCK_TRACE_LEVEL:-all}" \
  "$WORKER" run "$BENCH/channels/1/channel.json" \
  --output-folder "$OUT" --number 1 \
  > "$BENCH/$RUN.worker.log" 2>&1 &
WPID=$!
echo "worker pid $WPID, running ${DUR}s, trace at $TRACE"

END=$((SECONDS + DUR))
while [ $SECONDS -lt $END ]; do
  if ! kill -0 $WPID 2>/dev/null; then echo "worker exited early"; break; fi
  touch "$OUT/.heartbeat"
  sleep 2
done

kill $WPID 2>/dev/null || true
wait $WPID 2>/dev/null || true
echo "done; $(wc -l < "$TRACE/clock-1.jsonl") records"
