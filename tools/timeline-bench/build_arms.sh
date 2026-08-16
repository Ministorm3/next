#!/bin/bash
# build_arms.sh <out-dir>
#
# Builds the three comparison arms into <out-dir>/worker-{nopad,padonly,pair}.
#
#   nopad    no padding, trim measured but not applied: main's behaviour
#   padonly  padding, trim measured but not applied: the ceil climb alone
#   pair     the branch as it stands
#
# Every arm still MEASURES the stamp error on every item and logs it. The
# failure arms differ only in whether the correction is applied, so all three
# report the same quantity and can be read side by side. The failure arms
# matter as much as the passing one: a green result from an instrument that
# has never reproduced the defect proves nothing.
set -e
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUT=$(cd "$1" && pwd)
CS="$ROOT/crates/ersatztv-channel/src/channel_session.rs"

cp "$CS" "$CS.bench-backup"
restore() { mv -f "$CS.bench-backup" "$CS" 2>/dev/null || true; }
trap restore EXIT

need() { grep -q "$2" "$1" || { echo "patch anchor missing: $2"; exit 1; }; }

build() {
  (cd "$ROOT" && cargo build -q -p ersatztv-channel --bin ersatztv-channel)
  cp "$ROOT/target/debug/ersatztv-channel" "$OUT/worker-$1"
  echo "built arm: $1"
}

# every arm reports on every item, so a zero-trim item still produces a row
report_every_item() {
  perl -0pi -e 's/if trim_ms != 0 \{/if true \{ \/\/ bench: report every item/' "$CS"
  need "$CS" "bench: report every item"
}

# pair
cp -f "$CS.bench-backup" "$CS"; report_every_item; build pair

# padonly: measured, logged, not applied
cp -f "$CS.bench-backup" "$CS"; report_every_item
perl -0pi -e 's/let audio = Self::apply_emission_trim\(audio, trim_ms\);/let audio = Self::apply_emission_trim(audio, 0); \/\/ bench: trim not applied/' "$CS"
perl -0pi -e 's/let video = Self::apply_emission_trim\(video, trim_ms\);/let video = Self::apply_emission_trim(video, 0);/' "$CS"
need "$CS" "bench: trim not applied"
build padonly

# nopad: also drop the padding, which is main's behaviour
perl -0pi -e 's/pad_to_duration: true,/pad_to_duration: false, \/\/ bench: padding disabled/' "$CS"
need "$CS" "bench: padding disabled"
build nopad

echo "arms in $OUT"
