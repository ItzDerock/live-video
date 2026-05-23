#!/usr/bin/env bash

# INPUT Configuration
DEVICE="/dev/video0"
WIDTH=1280
HEIGHT=720
FPS=30

# ENCODER configuration
ENCODER="${1:?Usage: $0 <mpp|vaapi|software>}"
BPS=4000000
GOP=60
MUX_BITRATE=4900000

# OUTPUT configuration
RAPTORQ_MUXRATE=6911765
ZMQ_SOCK="ipc:///run/user/1000/raptorq.sock"

### GST PIPELINE

GST_SRC="v4l2src device=$DEVICE io-mode=mmap \
  ! image/jpeg,width=$WIDTH,height=$HEIGHT,framerate=$FPS/1 \
  ! jpegdec \
  ! videoconvert \
  ! textoverlay text=\"DUKE AERO\" valignment=top halignment=left font-desc=\"Sans Bold 24\" shaded-background=true"

case "$ENCODER" in
  mpp)
    GST_ENC="mpph265enc rc-mode=cbr bps=$BPS gop=$GOP \
      ! h265parse"
    ;;
  vaapi)
    GST_ENC="vah265enc rate-control=cbr bitrate=$((BPS / 1000)) key-int-max=$GOP \
      ! h265parse"
    ;;
  software)
    GST_ENC="x265enc bitrate=$((BPS / 1000)) key-int-max=$GOP speed-preset=ultrafast tune=zerolatency \
      ! h265parse"
    ;;
  *)
    echo "Unknown encoder: $ENCODER (expected mpp, vaapi, or software)" >&2
    exit 1
    ;;
esac

GST_SINK="queue max-size-buffers=3 \
  ! mpegtsmux bitrate=$MUX_BITRATE \
  ! filesink location=/dev/stdout"

### LAUNCH PIPELINE

GST_PIPELINE="$GST_SRC ! $GST_ENC ! $GST_SINK"

echo "[*] Pipeline starting:"
echo $GST_PIPELINE

eval "gst-launch-1.0 $GST_PIPELINE" \
    | mbuffer -m 16M 2> >(stdbuf -o0 tr '\r' '\n' >&2) \
    | ./raptorq/target/release/raptorq-enc --rate $RAPTORQ_MUXRATE --output-sock $ZMQ_SOCK zmq
