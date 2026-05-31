#!/usr/bin/env bash

# pipeline that utilizes hardware accel as much as possible
#
#   v4l2 (MJPEG) -> mjpeg_rkmpp (HW decode, DRM frames)
#                -> overlay_rkrga (HW blend of a pre-baked DUKE AERO layer)
#                -> [nv12 via RGA] -> hevc_rkmpp (HW encode)
#                -> mpegts (VBR) -> mbuffer -> raptorq -> ZMQ
#
# Goal: keep frames in DRM memory end-to-end so the CPU is freed for the
# software DVB-S modulator.
#
set -euo pipefail

### INPUT configuration
DEVICE="/dev/video0"
WIDTH=1280
HEIGHT=720
FPS=30

### ENCODER configuration
BPS=2300000
GOP=60          # keyframes every 2 s (GOP / FPS)
MUX_BITRATE=2900000   # only used by the CBR-TS fallback (see bottom comment)

### OUTPUT configuration
# Symbol rate * 2 (QPSK) * 3/4 (FEC rate) * 188/204 (RS overhead)
RAPTORQ_MUXRATE=3594117
ZMQ_SOCK="${ZMQ_SOCK-ipc:///run/user/1000/raptorq.sock}"

### Tooling / assets (override via env if paths differ)
FFMPEG="${FFMPEG:-/usr/lib/jellyfin-ffmpeg/ffmpeg}"
FONT="${FONT:-/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf}"
RAPTORQ_ENC="${RAPTORQ_ENC:-./raptorq/target/release/raptorq-enc}"
OVERLAY_PNG="${OVERLAY_PNG:-/run/live-video/duke-aero-overlay.png}"

# Small jitter buffer only; NOT a 16M latency sink. ~1 MiB smooths I-frame
# bursts + 64 KiB raptorq blocks without hiding a sustained rate deficit.
MBUF_SIZE="${MBUF_SIZE:-1M}"

### ----------------------------------------------------------------------
### Preflight
### ----------------------------------------------------------------------
[[ -x "$FFMPEG" ]]      || { echo "[!] ffmpeg not executable: $FFMPEG" >&2; exit 1; }
[[ -e "$DEVICE" ]]      || { echo "[!] capture device missing: $DEVICE" >&2; exit 1; }
[[ -f "$FONT" ]]        || { echo "[!] overlay font missing: $FONT (apt install fonts-dejavu-core)" >&2; exit 1; }
[[ -x "$RAPTORQ_ENC" ]] || { echo "[!] raptorq-enc not executable: $RAPTORQ_ENC" >&2; exit 1; }

# Capture-then-match (pipefail + grep -q would false-fail on SIGPIPE).
FILTERS="$("$FFMPEG"  -hide_banner -filters  2>/dev/null || true)"
DECODERS="$("$FFMPEG" -hide_banner -decoders 2>/dev/null || true)"
ENCODERS="$("$FFMPEG" -hide_banner -encoders 2>/dev/null || true)"
grep -q overlay_rkrga <<<"$FILTERS"  || { echo "[!] no overlay_rkrga filter — need an ffmpeg-rockchip RGA build" >&2; exit 1; }
grep -q scale_rkrga   <<<"$FILTERS"  || { echo "[!] no scale_rkrga filter" >&2; exit 1; }
grep -q mjpeg_rkmpp   <<<"$DECODERS" || { echo "[!] no mjpeg_rkmpp decoder" >&2; exit 1; }
grep -q hevc_rkmpp    <<<"$ENCODERS" || { echo "[!] no hevc_rkmpp encoder" >&2; exit 1; }

### ----------------------------------------------------------------------
### Bake the static overlay ONCE (software, only at startup).
### IMPORTANT: a SMALL, FULLY-OPAQUE label box — not a full-frame transparent
### layer. RGA alpha blending is unreliable on RK356x (known channel/alpha
### swizzle bug), so a transparent overlay composites as opaque and paints over
### the whole frame. An opaque box blitted at the corner needs no alpha at all;
### the rest of the frame stays live video.
mkdir -p "$(dirname "$OVERLAY_PNG")"
if [[ ! -f "$OVERLAY_PNG" ]]; then
    echo "[*] Baking overlay -> $OVERLAY_PNG"
    "$FFMPEG" -hide_banner -loglevel error \
        -f lavfi -i "color=c=black:s=260x48:d=1" \
        -frames:v 1 \
        -vf "drawtext=fontfile=${FONT}:text='DUKE AERO':x=12:y=10:fontsize=24:fontcolor=white,format=rgba" \
        -y "$OVERLAY_PNG"
fi

### ----------------------------------------------------------------------
### The pipeline
### ----------------------------------------------------------------------
# [VERIFY #1] mjpeg_rkmpp must decode THIS camera's MJPEG cleanly. Some webcams
#   emit MJPEG that HW decoders reject. If you see decode errors, fall back to
#   software decode + hwupload (see the SOFT-DECODE block in the notes) — you
#   keep overlay+scale+encode on hardware, only the JPEG decode goes to CPU.
#
# [VERIFY #2] RGA color/range. scale_rkrga/overlay_rkrga handle range
#   differently from swscale and can shift levels. If the picture looks washed
#   out/crushed, that's the place to tune (and the -color_range tag below).
#
# overlay_rkrga blends, then an explicit scale_rkrga does the format conversion
# to NV12 (its format option is reliable; overlay_rkrga's is not). Ending the
# graph on scale_rkrga gives the encoder a clean drm_prime NV12 with no
# software auto_scale inserted. If overlay_rkrga ever needs a format itself,
# add it there too — but keep the terminating scale_rkrga.
FILTERGRAPH="[1:v]format=bgra,hwupload[ovl];[0:v][ovl]overlay_rkrga=x=10:y=10:eof_action=repeat[ov];[ov]scale_rkrga=format=nv12[venc]"

FFMPEG_CMD=(
  "$FFMPEG" -hide_banner -loglevel warning
  # Shared rkmpp hw device so input-1's hwupload lands in the SAME context
  # as the decoded main stream (mismatched contexts -> auto_scale failure).
  -init_hw_device rkmpp=rk -filter_hw_device rk
  # input 0: live camera, hardware MJPEG decode straight to DRM frames
  -fflags nobuffer -flags low_delay
  -hwaccel rkmpp -hwaccel_output_format drm_prime -hwaccel_device rk -c:v mjpeg_rkmpp
  -f v4l2 -input_format mjpeg -video_size "${WIDTH}x${HEIGHT}" -framerate "$FPS" -i "$DEVICE"
  # input 1: pre-baked overlay; image2 reads ONE frame, hwupload runs ONCE,
  # overlay's eof_action=repeat reuses that single DRM layer for every frame
  -i "$OVERLAY_PNG"
  # all-hardware RGA blend -> NV12
  -filter_complex "$FILTERGRAPH"
  -map "[venc]"
  # hardware HEVC, CBR (bounds bursts), no B-frames.
  # NOTE: no -color_range here — on the HW path it forces a software range
  # conversion (swscale) that can't touch drm_prime and trips auto_scale.
  # Handle range inside RGA if needed; otherwise tag/convert on the RX side.
  -c:v hevc_rkmpp -rc_mode CBR -b:v "$BPS" -g "$GOP" -bf 0
  # VBR transport stream — raptorq paces the channel, so no -muxrate here
  -f mpegts -flush_packets 1
  pipe:1
)

echo "[*] HW TX pipeline starting:"
printf '    %q ' "${FFMPEG_CMD[@]}"; echo

"${FFMPEG_CMD[@]}" \
  | mbuffer -m "$MBUF_SIZE" 2> >(stdbuf -o0 tr '\r' '\n' >&2) \
  | "$RAPTORQ_ENC" --rate "$RAPTORQ_MUXRATE" --output-sock "$ZMQ_SOCK" zmq
