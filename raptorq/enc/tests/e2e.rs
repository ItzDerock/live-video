//! End-to-end subprocess tests: spawn raptorq-enc, optionally drop packets,
//! pipe into raptorq-dec, and verify the byte stream matches.
//!
//! Cargo only exposes CARGO_BIN_EXE_ for the package being tested. We locate
//! raptorq-dec as a sibling in the same target directory and run `cargo build`
//! at test start to ensure it exists.

use std::io::{Read, Write, ErrorKind};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use raptorq_ts_common::{
    DEFAULT_BLOCK_SIZE, FRAME_TYPE_DATA, FRAME_TYPE_NOOP, FRAME_TYPE_OFFSET, SYMBOL_SIZE, TS_HEADER_SIZE,
    TS_PACKET_SIZE,
};

fn bins() -> (PathBuf, PathBuf) {
    let enc = PathBuf::from(env!("CARGO_BIN_EXE_raptorq-enc"));
    let dir = enc.parent().unwrap().to_path_buf();
    let dec = dir.join("raptorq-dec");
    if !dec.exists() {
        let status = Command::new(env!("CARGO"))
            .args(["build", "--bin", "raptorq-dec"])
            .status()
            .expect("cargo build");
        assert!(status.success(), "failed to build raptorq-dec");
    }
    (enc, dec)
}

fn deterministic_payload(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for _ in 0..len {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        out.push((x >> 33) as u8);
    }
    out
}

/// Make a fake MPEG-TS stream: 0x47 every 188 bytes, plausible header bytes,
/// pseudo-random payload. Lets the byte-stream comparison stay meaningful.
fn fake_ts_stream(num_packets: usize, seed: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(num_packets * TS_PACKET_SIZE);
    let body = deterministic_payload(seed, num_packets * (TS_PACKET_SIZE - 4));
    for i in 0..num_packets {
        buf.push(0x47);
        buf.push(0x40);
        buf.push(0x21);
        buf.push((0x10 | (i & 0x0F)) as u8);
        let start = i * (TS_PACKET_SIZE - 4);
        buf.extend_from_slice(&body[start..start + TS_PACKET_SIZE - 4]);
    }
    buf
}

/// Forward 188-byte packets from `reader` to `writer` until the reader hits EOF
/// (the encoder closed its stdout) or the writer goes away. Only packets for
/// which `keep(index)` returns true are written, modelling link loss. `writer`
/// is moved in and dropped on return, closing the decoder's stdin.
fn forward<R: Read, W: Write>(mut reader: R, mut writer: W, mut keep: impl FnMut(usize) -> bool) {
    let mut buf = [0u8; TS_PACKET_SIZE];
    let mut idx = 0usize;
    loop {
        match reader.read_exact(&mut buf) {
            Ok(()) => {
                if keep(idx) && writer.write_all(&buf).is_err() {
                    break;
                }
                idx += 1;
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(_) => break,
        }
    }
}

#[test]
fn enc_dec_loopback_clean() {
    let (enc, dec) = bins();
    let blocks = 3;
    let stream_len = blocks * DEFAULT_BLOCK_SIZE;
    let ts_packets = stream_len / TS_PACKET_SIZE;
    let input = fake_ts_stream(ts_packets, 42);

    // Send-rate large enough that pacing isn't the bottleneck for the test.
    let mut enc_proc = Command::new(&enc)
        .args(["--rate", "100000000"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn enc");

    let mut dec_proc = Command::new(&dec)
        .args(["--block-timeout-ms", "200"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn dec");

    let mut enc_stdin = enc_proc.stdin.take().unwrap();
    let enc_stdout = enc_proc.stdout.take().unwrap();
    let dec_stdin = dec_proc.stdin.take().unwrap();
    let mut dec_stdout = dec_proc.stdout.take().unwrap();

    let writer_input = input.clone();
    let in_thread = thread::spawn(move || {
        enc_stdin.write_all(&writer_input).unwrap();
        drop(enc_stdin);
    });

    // Pipe enc -> dec, dropping nothing. Forward the encoder's entire output
    // (until it closes stdout) rather than a fixed packet count: raptorq's
    // ~300ms first-block table build means real data can trail a long run of
    // startup noops, which a small cap would miss.
    let pipe_thread = thread::spawn(move || {
        forward(enc_stdout, dec_stdin, |_| true);
    });

    let mut out = Vec::new();
    let reader_thread = thread::spawn(move || {
        dec_stdout.read_to_end(&mut out).unwrap();
        out
    });

    // Both children exit on their own: enc drains and exits when its stdin
    // closes, dec flushes and exits when the pipe closes its stdin. Wait for
    // that instead of racing a fixed sleep against the warm-up.
    in_thread.join().unwrap();
    pipe_thread.join().unwrap();
    let out = reader_thread.join().unwrap();
    let _ = enc_proc.wait();
    let _ = dec_proc.wait();

    assert!(
        out.len() >= input.len(),
        "decoder emitted {} bytes, expected at least {}",
        out.len(),
        input.len()
    );
    assert_eq!(
        &out[..input.len()],
        &input[..],
        "decoded byte stream does not match input"
    );
}

#[test]
fn enc_emits_noops_under_underrun() {
    // Real underrun = pipe open but no data flowing. Use Stdio::piped() and never
    // write anything; the encoder's stdin read should block, but the paced writer
    // should still emit noops at the target rate.
    let (enc, _dec) = bins();
    let rate_bps: u64 = 1_460_000;
    let mut proc = Command::new(&enc)
        .args(["--rate", &rate_bps.to_string()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn enc");

    let mut stdout = proc.stdout.take().unwrap();
    // Keep the stdin write-end alive in the parent so the child sees an open empty pipe.
    let _stdin_keep_open = proc.stdin.take().unwrap();

    // Sample 200 ms of output. At 1.46 Mbps that's ~190 packets. raptorq's
    // ~300ms first-block table build emits nothing, so start the clock on the
    // first byte and measure throughput over the window after it.
    let measurement = Duration::from_millis(200);
    let (tx, rx) = std::sync::mpsc::channel::<(Vec<u8>, f64)>();
    let reader = thread::spawn(move || {
        let mut all = Vec::with_capacity(64 * 1024);
        let mut buf = [0u8; 4096];
        let first = loop {
            match stdout.read(&mut buf) {
                Ok(0) => {
                    tx.send((all, 0.0)).unwrap();
                    return;
                }
                Ok(n) => {
                    all.extend_from_slice(&buf[..n]);
                    break std::time::Instant::now();
                }
                Err(_) => {
                    tx.send((all, 0.0)).unwrap();
                    return;
                }
            }
        };
        while first.elapsed() < measurement {
            match stdout.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => all.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
        }
        tx.send((all, first.elapsed().as_secs_f64())).unwrap();
    });

    let (got, secs) = rx.recv().unwrap();
    let _ = proc.kill();
    let _ = proc.wait();
    reader.join().unwrap();

    let bytes_per_sec = (got.len() as f64) / secs;
    let expected_bps = rate_bps as f64;
    let ratio = bytes_per_sec * 8.0 / expected_bps;
    assert!(
        ratio > 0.5 && ratio < 1.3,
        "underrun output rate {bytes_per_sec:.0} B/s vs expected {} B/s (ratio {ratio:.2})",
        expected_bps / 8.0
    );

    // Confirm output bytes are TS-aligned.
    assert!(got.len() >= TS_PACKET_SIZE);
    assert_eq!(got[0], 0x47, "first byte should be TS sync");
    assert_eq!(got[TS_PACKET_SIZE], 0x47, "second packet should be aligned");
}

#[test]
fn enc_dec_loopback_with_random_drops() {
    let (enc, dec) = bins();
    let blocks = 4;
    let stream_len = blocks * DEFAULT_BLOCK_SIZE;
    let ts_packets = stream_len / TS_PACKET_SIZE;
    let input = fake_ts_stream(ts_packets, 99);

    let mut enc_proc = Command::new(&enc)
        .args(["--rate", "100000000"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn enc");

    let mut dec_proc = Command::new(&dec)
        .args(["--block-timeout-ms", "200"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn dec");

    let mut enc_stdin = enc_proc.stdin.take().unwrap();
    let enc_stdout = enc_proc.stdout.take().unwrap();
    let dec_stdin = dec_proc.stdin.take().unwrap();
    let mut dec_stdout = dec_proc.stdout.take().unwrap();

    let writer_input = input.clone();
    let in_thread = thread::spawn(move || {
        enc_stdin.write_all(&writer_input).unwrap();
        drop(enc_stdin);
    });

    // Drop ~10% of TS packets randomly; within the 25% repair budget. Forward to
    // EOF (see enc_dec_loopback_clean) so the post-warm-up data is not missed.
    let pipe_thread = thread::spawn(move || {
        let mut rng = 0xdeadbeefu64;
        forward(enc_stdout, dec_stdin, move |_| {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            (rng >> 40) % 100 >= 10
        });
    });

    let reader_thread = thread::spawn(move || {
        let mut out = Vec::new();
        dec_stdout.read_to_end(&mut out).unwrap();
        out
    });

    in_thread.join().unwrap();
    pipe_thread.join().unwrap();
    let out = reader_thread.join().unwrap();
    let _ = enc_proc.wait();
    let _ = dec_proc.wait();

    assert!(
        out.len() >= input.len(),
        "decoder emitted {} bytes, expected at least {}",
        out.len(),
        input.len()
    );
    assert_eq!(
        &out[..input.len()],
        &input[..],
        "decoded byte stream does not match input under 10% loss"
    );
}

/// Spawn the encoder with one block of input but stdin held open (so it sits
/// idle with airtime to spare), sample ~`window` of output, and count DATA vs
/// NOOP frames.
fn count_frames(enc: &Path, adaptive: bool, input: &[u8]) -> (usize, usize) {
    let mut proc = Command::new(enc)
        .args([
            "--rate",
            "8000000",
            "--adaptive-fec",
            if adaptive { "true" } else { "false" },
            "--max-repair-overhead",
            "5.0",
            "--repair-horizon-ms",
            "5000",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn enc");

    let mut stdin = proc.stdin.take().unwrap();
    let mut stdout = proc.stdout.take().unwrap();

    let owned = input.to_vec();
    let writer = thread::spawn(move || {
        stdin.write_all(&owned).ok();
        // Keep stdin open past the raptorq warm-up so there is genuine idle
        // airtime to sample after output starts.
        thread::sleep(Duration::from_millis(900));
        drop(stdin);
    });

    // raptorq's ~300ms first-block table build emits nothing; start the sampling
    // window on the first byte so it measures the steady state, not the warm-up.
    let window = Duration::from_millis(250);
    let reader = thread::spawn(move || {
        let mut all = Vec::new();
        let mut buf = [0u8; 8192];
        let mut started: Option<std::time::Instant> = None;
        loop {
            if started.map_or(false, |t| t.elapsed() >= window) {
                break;
            }
            match stdout.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    all.extend_from_slice(&buf[..n]);
                    started.get_or_insert_with(std::time::Instant::now);
                }
                Err(_) => break,
            }
        }
        all
    });

    let out = reader.join().unwrap();
    let _ = writer.join();
    let _ = proc.kill();
    let _ = proc.wait();

    let mut data = 0usize;
    let mut noop = 0usize;
    for pkt in out.chunks_exact(TS_PACKET_SIZE) {
        match pkt[TS_HEADER_SIZE + FRAME_TYPE_OFFSET] {
            FRAME_TYPE_DATA => data += 1,
            FRAME_TYPE_NOOP => noop += 1,
            _ => {}
        }
    }
    (data, noop)
}

#[test]
fn adaptive_fec_fills_idle_with_repair_not_noops() {
    let (enc, _dec) = bins();
    // Slightly more than one block so exactly one full block is encoded; the
    // rest of stdin stays open and idle.
    let ts_packets = DEFAULT_BLOCK_SIZE / TS_PACKET_SIZE + 1;
    let input = fake_ts_stream(ts_packets, 7);
    // One block = K source + 25% baseline repair.
    let baseline_data = DEFAULT_BLOCK_SIZE / SYMBOL_SIZE + (DEFAULT_BLOCK_SIZE / SYMBOL_SIZE) / 4;

    let (adaptive_data, adaptive_noop) = count_frames(&enc, true, &input);
    let (plain_data, plain_noop) = count_frames(&enc, false, &input);

    // Adaptive turns idle airtime into extra repair: more DATA frames than the
    // single block's baseline, and far more than the non-adaptive run.
    assert!(
        adaptive_data > baseline_data,
        "adaptive should mint repair beyond baseline {baseline_data}: got {adaptive_data}"
    );
    assert!(
        adaptive_data > plain_data,
        "adaptive should emit more DATA than plain: adaptive={adaptive_data}, plain={plain_data}"
    );
    // Non-adaptive fills the same idle airtime with noop stuffing instead.
    assert!(
        plain_noop > 100,
        "plain run should emit noop stuffing: got {plain_noop}"
    );
    assert!(
        adaptive_noop * 4 < plain_noop,
        "adaptive should nearly eliminate noops: adaptive={adaptive_noop}, plain={plain_noop}"
    );
}
