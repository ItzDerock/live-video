#!/usr/bin/env bash

socat -b 8388608 UDP-RECV:8000,bind=127.0.0.1,rcvbuf=8388608 - | ./raptorq/target/release/raptorq-dec | tee -a "./out/video-$(date +%s).ts" | ffplay -probesize 32 -analyzeduration 0 -fflags nobuffer -flags low_delay -
