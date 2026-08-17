# clock-probe

Measures every clock a channel keeps, across a whole playout.

The worker keeps six of them. Every timing defect in this project's history
has been a confusion between two, not an arithmetic mistake, and none of the
free text logging says which clock a number came from. This turns that from a
re-derivation by hand into a table.

## The six domains

| | domain | carried by |
|---|---|---|
| `W` | wall clock | `now_utc`, `now_local`, and a monotonic reading beside it |
| `S` | schedule cursor | `transcoded_until`, item start and finish |
| `E` | emitted media clock | `last_segment_end`, the program date times clients see |
| `P` | media presentation timestamps | `output_ts_offset`, the scanner, the WebVTT timestamp map |
| `Q` | sequence counters | media sequence, segment names, the monotonic clamp |
| `C` | source content positions | in points, out points, seeks, zero at the start of a file |

Every field in the trace carries the letter of its domain as a prefix, so a raw
line is legible without the map at hand.

## Two halves

The worker writes **readings**. The probe does the **arithmetic**.

Nothing in the worker ever computes a difference between two clocks. That rule
is not tidiness. A wrong formula in a worker is baked into every trace it ever
writes and costs a rebuild and a redeploy to correct, and PR #212 shipped one:
it subtracted the emitted clock from the schedule cursor without adding the
virtual start offset back, so an entire hour read as drift and the fix then
trimmed real content to chase it. Keeping the formulas in the probe makes that
class of error a rerun.

## Recording

Off unless the `clock-trace` feature is compiled in, and dormant even then
until the environment asks for it. An image can ship with the seam present and
switch it on per channel with no rebuild.

```bash
cargo build --workspace --features ersatztv-channel/clock-trace
```

| variable | meaning |
|---|---|
| `ETV_CLOCK_TRACE` | a folder. Unset or empty means off. |
| `ETV_CLOCK_TRACE_LEVEL` | `items`, `segments` or `all`. Default `segments`. |
| `ETV_CLOCK_TRACE_MAX_MB` | roll threshold per file. Default 64. |

Each worker writes `clock-<channel>.jsonl`. At the threshold the file moves to
`.jsonl.prev` and a fresh one opens, so a channel costs at most twice the
threshold on disk.

Pick the level by how long you intend to leave it on:

- `items` is a handful of records per item. Leave it on for days.
- `segments` adds one record roughly every two seconds.
- `all` adds the publish loop, another record every two seconds. Use it when
  the question is about the served window, the monotonic clamp, or retention.

A trace that cannot be written turns itself off. A channel never dies because
its instrument did.

## Reading

```bash
ersatztv-clock-probe summary   <trace-folder>
ersatztv-clock-probe items     <trace-folder>
ersatztv-clock-probe segments  <trace-folder>
ersatztv-clock-probe crossings <trace-folder>
ersatztv-clock-probe check     <trace-folder>   # exits non zero on a failure
```

A folder contributes every `clock-*.jsonl` inside it, so pointing at the trace
folder picks up whichever channels were recording. Add `--channel N` to narrow.

`items` is the main view. One row per pipeline, every domain on the same line:

```
   # C source            state             W lead      S cursor       E stamp    drift     step   C slot   E emit     err    pred    P off
   4 b1.mp4              ZeroAndWorkAhead   11.7s  +0:00:15.163  +0:00:14.767     -395       +0     5980     5572    -407     +26    15247
   5 a1.mp4              ZeroAndWorkAhead   16.8s  +0:00:21.143  +0:00:20.339     -803     -408     6439     6439      +0      +1    21241
```

- `drift` is `E - S` with the virtual start offset added back. This is the
  quantity that must not trend.
- `step` is what the previous item contributed. A defect repeats a value here.
  Noise does not.
- `err` is emitted media against schedule time consumed, per item.
- `pred` is the frame quantization law, `ceil(slot * fps) / fps - slot`, which
  is how much a padded pipeline is expected to overshoot.

## What check tests

| check | the invariant |
|---|---|
| `wall-clock` | the wall clock tracked the monotonic clock. Nothing else can see a system clock step. |
| `split-origin` | reports the virtual start offset, and what an uncorrected difference would have read |
| `stamp-drift` | the emitted clock does not walk away from the schedule, by total, by rate per hour, and by how many steps moved the same way |
| `seek-purity` | no measured value reaches an input seek. Take the schedule progress back out of a seek and the remainder cannot move while an item plays. |
| `trim-safety` | nothing was deleted from inside the published window |
| `trim-domain` | which trim the build runs, read from the trace: a wall clock cutoff against an emitted stamp is the unsound crossing, a served position cutoff keeps both sides emitted |
| `retention` | held history against the lag of the live edge behind real time |
| `publish-horizon` | no window carried a stamp from past the horizon |
| `sequence` | the media sequence and the name order stayed in step |

Thresholds are flags on `check`. `--max-drift-rate-ms-per-hour` matters most:
a rate catches a ratchet that any tolerable absolute limit lets through. The
2026-08-14 padding regression was 113ms over seventeen items.

## Running a bench

```bash
cargo build --workspace --features ersatztv-channel/clock-trace
mkdir -p /tmp/cp/content && cp tools/clock-probe/gen_content.sh /tmp/cp/
bash /tmp/cp/gen_content.sh
target/debug/ersatztv add-lineup /tmp/cp/lineup.json --channels 1
# then edit /tmp/cp/channels/1/channel.json: set the ffmpeg and ffprobe paths
# and a small output size, which keeps a laptop realtime
target/debug/ersatztv-playout-generator \
  --content-folder /tmp/cp/content --lineup /tmp/cp/lineup.json --channel 1

bash tools/clock-probe/run_bench.sh /tmp/cp v1 210
target/debug/ersatztv-clock-probe items /tmp/cp/trace-v1
```

The content mix matters. Real library files nearly always have an audio tail a
few milliseconds past the video stream, which is what puts the duration cut
onto a padded clone and produces the overshoot. Frame perfect files sit outside
that regime and emit exactly their video, so a bench built only from them
cannot see the defect at all. `gen_content.sh` includes both, `b1` and `b2`
with an audio tail and `a4` as the frame aligned control.

To exercise the split origin, set `virtual_start` in `channel.json` to an hour
ahead and rerun. `check` then prints the corrected and uncorrected readings
side by side.

## Validating a change to the instrument

Every check has a test that feeds it a trace carrying the defect it exists to
catch, and a second trace that differs only in the fault. A green result from
an instrument that has never seen the defect is what shipped the 2026-08-14
regression.

```bash
cargo test -p ersatztv-clock-probe
```

Two of those tests exist because the first version of this tool was wrong in
ways only a real run revealed:

- The emitted clock was read before the pipeline boundary absorbed the
  outgoing pipeline's last segments, so drift swung by a whole item and
  emission was credited to the wrong row.
- Retention was read from the trimmed segments. A trimmed segment is older
  than the cutoff by definition, so its age is always the whole budget or
  more, and the check called every healthy channel starved.

## Reference readings

Taken on `origin/main` at 4eec042, 640x360, roughly 200 seconds per run.

| run | result |
|---|---|
| default | `stamp-drift` fails, about -54000ms per hour. Only `b1` and `b2` rows carry a nonzero `err`, at -407 and -380ms, which is the shortfall staircase. `trim-domain` warns, because the cutoff is a wall clock reading. |
| `virtual_start` one hour ahead | corrected -3169ms, uncorrected -3598491ms. The difference is the whole offset, and acting on the uncorrected number is what PR #212 does. |
| a build carrying the padding and trim fixes | -1ms over 33 steps, 0ms per item, and `trim-domain` reports the served position instead. |

The third row is the point. The same bench and the same content separate a
build that has the fixes from one that does not, in one line each, which is
what makes this usable as a gate rather than only as a microscope.

A change to padding, to the duration cut, or to the trim should rerun all
three. The failing arm matters as much as the passing one: a green result from
an instrument that has not reproduced the defect is what shipped the
regression.
