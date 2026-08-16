#!/bin/bash
# Builds a content mix that exercises the source content clock.
#
# The mix matters. Real library files nearly always have an audio tail a few
# milliseconds past the video stream, which is what puts the duration cut onto
# a padded clone and produces the frame quantization overshoot. Frame perfect
# files sit outside that regime and emit exactly their video, so a bench built
# only from them cannot see the defect at all.
#
# b1 and b2 carry an audio tail. a4 is the frame aligned control.
set -e
cd "$(dirname "$0")/content"

gen() {
  name=$1; fps=$2; vdur=$3; apad=$4
  adur=$(python3 -c "print($vdur + $apad)")
  ffmpeg -y -loglevel error \
    -f lavfi -i "testsrc2=rate=$fps:size=640x360:duration=$vdur" \
    -f lavfi -t "$adur" -i "sine=frequency=440:sample_rate=48000" \
    -c:v libx264 -preset veryfast -g 30 -pix_fmt yuv420p -c:a aac -b:a 96k \
    "$name.mp4"
}

gen a1 30000/1001 6.437 0
gen a2 25         7.213 0
gen a3 24000/1001 5.891 0
gen a4 30         6.100 0
gen b1 30000/1001 5.560 0.42
gen b2 25         6.320 0.38
gen a5 24000/1001 7.489 0
gen a6 30000/1001 4.777 0

for f in *.mp4; do
  ffprobe -v error -show_entries format=duration \
    -show_entries stream=codec_type,duration,r_frame_rate -of csv "$f" | tr '\n' ' '
  echo " <- $f"
done
