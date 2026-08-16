# timeline-bench

A cumulative multi-item timeline bench for the channel worker's stamp clock.

Drift here is `first_segment_pdt - scheduled_start` per item. A single-file
bench cannot see the defects this measures: the shortfall staircase (files
whose video ends before their container) and the padding quantization climb
(the `-t` cut emits the straddling frame whole, so a padded item runs up to
one frame long) both only exist as sums over many items.

This branch exists to keep the bench out of the PR it measures. It is
`fix/timeline-drift-pad-and-trim` plus this directory, nothing else.

## Setup, once

```bash
cargo build --workspace
mkdir -p /tmp/tb && cp tools/timeline-bench/gen_content.sh /tmp/tb/
mkdir -p /tmp/tb/content && bash /tmp/tb/gen_content.sh
target/debug/ersatztv add-lineup /tmp/tb/lineup.json --channels 1
# then edit /tmp/tb/channels/1/channel.json: set ffmpeg/ffprobe paths and a
# small output size (640x360 keeps a laptop realtime)
target/debug/ersatztv-playout-generator \
  --content-folder /tmp/tb/content --lineup /tmp/tb/lineup.json --channel 1
```

The content mix matters. Real library files nearly always have an audio tail
a few ms past the video stream, which is what puts the `-t` cut onto a tpad
clone; frame-perfect lavfi files (video exactly frame-aligned, audio no
longer than video) sit outside the defect regime and emit exactly their
video. gen_content.sh therefore includes files with audio outliving video
(b1, b2: the staircase fixtures, which also exercise padding) and a
frame-aligned control (a4).

## Run and read

```bash
tools/timeline-bench/build_arms.sh /tmp/tb          # worker-{nopad,padonly,pair}
tools/timeline-bench/run_arm.sh /tmp/tb pair 240 /tmp/tb/worker-pair
python3 tools/timeline-bench/analyze_arm.py /tmp/tb pair
```

`build_arms.sh` produces the three arms by toggling at most two lines each,
so the only thing that varies is the mechanism under test. It restores the
source on exit.

| arm | padding | emission trim | what it shows |
|---|---|---|---|
| `nopad` | no | measured, not applied | main's behaviour: the shortfall staircase |
| `padonly` | yes | measured, not applied | the ceil climb, in isolation |
| `pair` | yes | applied | the bounded walk |

Every arm measures and logs the error on every item. The failure arms differ
only in whether the correction is applied, so all three report the same
quantity and can be read side by side.

The analyzer prints per-item drift, the per-item step (the emission error of
the previous row's item), and the ceil-law prediction
`ceil(slot * fps)/fps - slot` that applies to a padded arm with no trim. The
first two rows are boot transient (the worker joins mid-slot) and are marked
and excluded from the totals.

## Reference results

Three arms, 240s each, same lineup, run back to back on one machine
(2026-08-16, macOS, software x264 at 640x360):

    arm       items   cumulative   per-item steps
    nopad        42      -3937ms   0, except -408 after b1 and -380 after b2
    padonly      42       +231ms   0, except  +26 after b1 and  +20 after b2
    pair         43         -2ms   bounded, never outside one frame

The staircase validates itself against the fixtures: `b1` is a 5.572s video
in a 5.980s container, and it loses exactly the 408ms difference on every
airing. `b2` is 6.320s in 6.700s and loses exactly 380ms. Those are the only
two files in the mix whose slot exceeds their video stream, and they are the
only two that move the clock in either failure arm.

The padded arm's steps match `ceil(slot * fps)/fps - slot` for the same two
files, +26ms and +20ms. The other files sit outside the defect regime, so
the printed ceil-law total is an upper bound rather than a prediction.

## How drift is measured

Drift is `last_segment_end - transcoded_until`, read at the top of every
pipeline before that pipeline's own correction. `last_segment_end` advances
only by the EXTINF durations ffmpeg reports, so it is what was really emitted;
`transcoded_until` advances by scheduled finishes. The analyzer reads the
series out of the worker log, which every arm emits per item.

The obvious alternative, deriving it from the published playlist, was tried
first and abandoned. The playlist carries no per-segment item id, so segments
have to be attributed to items by treating each discontinuity-delimited run as
one pipeline. That assumption breaks: on the unpadded arm a 240s run produced
44 pipelines but only 41 discontinuity-delimited runs, because a pipeline that
emits no segment of its own hands its discontinuity to the next one. The
pairing then slips by one and stays wrong, which showed up as per-item steps
of one or two seconds with random signs. The log-based series over the same
run gives clean zeros with exact -408ms and -380ms steps on the two fixtures
that should produce them.

`run_arm.sh` still polls the published playlist every two seconds, because it
is a rolling ten segment window and a slower poll would drop segments. Pass
`--check` to `analyze_arm.py` to print the emitted span derived from it as an
independent cross-check on the log series.

## Caveat when comparing arms

The generated playout is shuffled and each arm starts at its own wall clock
moment, so the arms do not see the same slice of the schedule. Compare the
shape and the per-item step against `pred_step`, which is computed from the
items that arm actually played. Do not read across arms on the absolute
total alone.

A change to padding, `-t` computation, or the emission trim should rerun all
relevant arms. The failure arms matter as much as the passing one: a green
result from an instrument that has not reproduced the defect is worth
nothing.
