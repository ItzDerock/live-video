//! End-to-end subprocess tests: spawn raptorq-enc, optionally drop packets,
//! pipe into raptorq-dec, and verify the byte stream matches.
//!
//! Cargo only exposes CARGO_BIN_EXE_ for the package being tested. We locate
//! raptorq-dec as a sibling in the same target directory and run `cargo build`
//! at test start to ensure it exists.

use std::io::{Read, Write, ErrorKind};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use raptorq_ts_common::{DEFAULT_BLOCK_SIZE, TS_PACKET_SIZE};

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
    let mut dec_stdin = dec_proc.stdin.take().unwrap();
    let mut dec_stdout = dec_proc.stdout.take().unwrap();

    let writer_input = input.clone();
    let in_thread = thread::spawn(move || {
        enc_stdin.write_all(&writer_input).unwrap();
        drop(enc_stdin);
    });

    // Pipe enc -> dec, dropping nothing.
    let pipe_thread = thread::spawn(move || {
        let mut reader = enc_stdout;
        let mut buf = [0u8; TS_PACKET_SIZE];
        let mut forwarded = 0usize;
        let limit = blocks * (DEFAULT_BLOCK_SIZE / 167) * 2 + 100;
        loop {
            match reader.read_exact(&mut buf) {
                Ok(()) => {
                    dec_stdin.write_all(&buf).unwrap();
                    forwarded += 1;
                    if forwarded > limit {
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => panic!("read error: {e}"),
            }
        }
        drop(dec_stdin);
    });

    let mut out = Vec::new();
    let reader_thread = thread::spawn(move || {
        dec_stdout.read_to_end(&mut out).unwrap();
        out
    });

    in_thread.join().unwrap();
    pipe_thread.join().unwrap();
    let _ = enc_proc.kill();
    let _ = enc_proc.wait();
    // Give dec a moment to flush, then close.
    thread::sleep(Duration::from_millis(300));
    let _ = dec_proc.kill();
    let _ = dec_proc.wait();
    let out = reader_thread.join().unwrap();

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

    // Sample 200 ms of output. At 1.46 Mbps that's ~190 packets.
    let measurement = Duration::from_millis(200);
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    let reader = thread::spawn(move || {
        let mut all = Vec::with_capacity(64 * 1024);
        let mut buf = [0u8; 4096];
        let start = std::time::Instant::now();
        while start.elapsed() < measurement + Duration::from_millis(50) {
            match stdout.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => all.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
        }
        tx.send(all).unwrap();
    });

    thread::sleep(measurement + Duration::from_millis(200));
    let _ = proc.kill();
    let _ = proc.wait();
    reader.join().unwrap();
    let got = rx.recv().unwrap();

    let bytes_per_sec = (got.len() as f64) / (measurement.as_secs_f64() + 0.05);
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
    let mut dec_stdin = dec_proc.stdin.take().unwrap();
    let mut dec_stdout = dec_proc.stdout.take().unwrap();

    let writer_input = input.clone();
    let in_thread = thread::spawn(move || {
        enc_stdin.write_all(&writer_input).unwrap();
        drop(enc_stdin);
    });

    // Drop ~10% of TS packets randomly; within 25% repair budget.
    let pipe_thread = thread::spawn(move || {
        let mut reader = enc_stdout;
        let mut buf = [0u8; TS_PACKET_SIZE];
        let mut idx = 0usize;
        let mut rng = 0xdeadbeefu64;
        let limit = blocks * (DEFAULT_BLOCK_SIZE / 167) * 2 + 200;
        loop {
            match reader.read_exact(&mut buf) {
                Ok(()) => {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let drop = (rng >> 40) % 100 < 10;
                    if !drop {
                        dec_stdin.write_all(&buf).unwrap();
                    }
                    idx += 1;
                    if idx > limit {
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(_) => break,
            }
        }
        drop(dec_stdin);
    });

    let reader_thread = thread::spawn(move || {
        let mut out = Vec::new();
        dec_stdout.read_to_end(&mut out).unwrap();
        out
    });

    in_thread.join().unwrap();
    pipe_thread.join().unwrap();
    let _ = enc_proc.kill();
    let _ = enc_proc.wait();
    thread::sleep(Duration::from_millis(500));
    let _ = dec_proc.kill();
    let _ = dec_proc.wait();
    let out = reader_thread.join().unwrap();

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
