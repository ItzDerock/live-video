# raptorq

Application-layer FEC for MPEG-TS over DVB-S2 or DVB-S. Wraps a TS stream in RaptorQ erasure codes so the decoder can reconstruct missing packets from repair symbols.

## Architecture

```
stdin (MPEG-TS)
    │
    ▼
raptorq-enc ──── stdout (TS-shaped wire) ──── dvbs2-tx
                 or ZMQ PUSH socket

dvbs2-rx ──── stdin (TS-shaped wire) ──── raptorq-dec ──── stdout (MPEG-TS)
```

Three threads inside the encoder:

- **ts-reader**: reads stdin into fixed-size source blocks
- **block-encoder**: encodes each block with RaptorQ, interleaves repair symbols
- **paced-writer**: emits 188-byte TS packets at the configured muxrate, stuffing with noops when the encoder is ahead

The decoder ingests TS packets from stdin, reassembles source blocks in order using RaptorQ, and emits recovered MPEG-TS on stdout. Blocks that exceed the timeout are emitted best-effort (partial source symbols) or as zeros if nothing arrived.

## Wire format

Each 188-byte TS packet carries one RaptorQ symbol in its payload:

```
[TS header 4B][frame type 1B][OTI 12B][payload ID 4B][symbol N bytes]
```

Frame types: `0x01` data, `0x02` noop/stuffing.

## Build

Requires a Nix devshell (provides `zeromq`, `pkg-config`, `gcc`, `cargo`):

```sh
nix develop
cargo build --release
```

Binaries: `target/release/raptorq-enc`, `target/release/raptorq-dec`

## Usage

### Encoder

```
raptorq-enc [OPTIONS] [stdout|zmq] [--output-sock <ENDPOINT>]
```

| Flag                    | Default  | Description                                                 |
| ----------------------- | -------- | ----------------------------------------------------------- |
| `--rate <bps>`          | required | Muxrate in bits/sec. Must match `dvbs2-tx`.                 |
| `--block-size <bytes>`  | 66248    | Source block size. Must be multiple of symbol size (171 B). |
| `--repair-overhead <f>` | 0.25     | Fraction of repair symbols per block (e.g. 0.25 = 25%).     |
| `--pid <hex>`           | 0x100    | TS PID for framed packets.                                  |
| `--queue-blocks <n>`    | 4        | In-flight block buffer depth.                               |
| `--output-sock <addr>`  | none     | ZMQ endpoint to bind (required for `zmq` output).           |

**Stdout mode** (pipe directly to `dvbs2-tx`):

```sh
ffmpeg -i input.ts -f mpegts - | \
  raptorq-enc --rate 50000000 stdout | \
  dvbs2-tx ...
```

**ZMQ mode** (PUSH socket, receiver connects with PULL):

```sh
raptorq-enc --rate 50000000 zmq --output-sock tcp://*:5555
# or
raptorq-enc --rate 50000000 zmq --output-sock ipc:///tmp/raptorq.sock
```

### Decoder

```
raptorq-dec [OPTIONS]
```

| Flag                         | Default | Description                       |
| ---------------------------- | ------- | --------------------------------- |
| `--pid <hex>`                | 0x100   | TS PID to parse.                  |
| `--max-blocks-in-flight <n>` | 8       | Block reorder window size.        |
| `--block-timeout-ms <ms>`    | 700     | Time before giving up on a block. |

```sh
dvbs2-rx ... | raptorq-dec | ffplay -i -
```

### Logging

Both tools log to stderr. Control verbosity with `RUST_LOG`:

```sh
RUST_LOG=debug raptorq-enc --rate 50000000 stdout
RUST_LOG=warn  raptorq-dec
```

## Parameters

**Block size** and **repair overhead** must match between encoder and decoder. The decoder reads OTI from each packet header, so they don't need to be configured explicitly on the decoder side.

**Rate** must match the `dvbs2-tx` muxrate exactly. The encoder's paced writer sleeps between packets to hit the target rate; if the rate is wrong the downstream FD will stall or drop packets.

**Repair overhead** of 0.25 (25%) recovers up to ~20% packet loss at the block level, with some margin. Higher overhead costs bandwidth; lower overhead reduces protection.
