# timeline-bench

A cumulative multi-item timeline bench for the channel worker's stamp clock.

Drift here is `first_segment_pdt - scheduled_start` per item, the same
quantity the production drift meter records. A single-file bench cannot see
the defects this measures: the shortfall staircase (files whose video ends
before their container) and the padding quantization climb (the `-t` cut
emits the straddling frame whole, so a padded item runs up to one frame
long) both only exist as sums over many items. The 2026-08-14 padding
regression shipped because the bench of the day measured one file.

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
tools/timeline-bench/run_arm.sh /tmp/tb <arm-name> 270
python3 tools/timeline-bench/analyze_arm.py /tmp/tb <arm-name>
```

The analyzer prints per-item drift, the per-item step (the emission error of
the previous row's item), and the ceil-law prediction
`ceil(slot * fps)/fps - slot` that applies to padded pipelines. The first
two rows are boot transient (the worker joins mid-slot); read from row 3.

Reference results, 2026-08-15, 17 steady items each:

    pad templated only        -2822ms   staircase, -380/-408ms steps
    pad unconditional          +113ms   every padded item +20/+26ms (ceil law)
    pad + emission trim         -14ms   bounded walk, never past one frame

A change to padding, `-t` computation, or the emission trim should rerun all
relevant arms. The failure arm matters as much as the passing one: a green
result from an instrument that has not reproduced the defect is what shipped
the regression.
