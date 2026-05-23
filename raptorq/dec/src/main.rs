use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder};
use raptorq_ts_common::{
    parse_packet, ParsedFrame, DEFAULT_PID, SYMBOL_SIZE, TS_PACKET_SIZE, TS_SYNC_BYTE,
};
use tracing::{debug, info, trace, warn};

#[derive(Parser, Debug)]
#[command(
    name = "raptorq-dec",
    about = "Reads TS-shaped wire packets from dvbs2-rx, reconstructs the original \
             MPEG-TS stream using RaptorQ, and emits it in order on stdout."
)]
struct Args {
    #[arg(long, value_parser = parse_pid, default_value_t = DEFAULT_PID)]
    pid: u16,

    /// Number of in-flight blocks to keep state for.
    #[arg(long, default_value_t = 8)]
    max_blocks_in_flight: usize,

    /// How long to wait before giving up on a block and emitting best-effort.
    #[arg(long, default_value_t = 700)]
    block_timeout_ms: u64,
}

fn parse_pid(s: &str) -> std::result::Result<u16, String> {
    let s = s.trim_start_matches("0x").trim_start_matches("0X");
    u16::from_str_radix(s, 16).map_err(|e| format!("invalid pid {s:?}: {e}"))
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!(
        pid = format!("{:#x}", args.pid),
        max_in_flight = args.max_blocks_in_flight,
        timeout_ms = args.block_timeout_ms,
        "decoder configured"
    );

    let (tx, rx) = sync_channel::<Vec<u8>>(1024);
    let reader = thread::Builder::new()
        .name("ts-reader".into())
        .spawn(move || reader_thread(tx))?;

    let result = run_decoder(rx, args);
    reader.join().expect("reader panicked").ok();
    result
}

fn reader_thread(out: SyncSender<Vec<u8>>) -> Result<()> {
    let mut stdin = std::io::stdin().lock();
    let mut scratch = [0u8; TS_PACKET_SIZE];
    let mut filled = 0usize;
    let mut resync_skipped = 0u64;
    loop {
        match stdin.read(&mut scratch[filled..]) {
            Ok(0) => {
                info!("stdin closed; reader exiting");
                return Ok(());
            }
            Ok(n) => {
                filled += n;
                while filled == TS_PACKET_SIZE {
                    if scratch[0] != TS_SYNC_BYTE {
                        scratch.copy_within(1.., 0);
                        filled -= 1;
                        resync_skipped += 1;
                        if resync_skipped.is_power_of_two() {
                            warn!(resync_skipped, "skipping bytes to find 0x47 sync");
                        }
                        continue;
                    }
                    let pkt = scratch.to_vec();
                    if out.send(pkt).is_err() {
                        return Ok(());
                    }
                    filled = 0;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e).context("reading stdin"),
        }
    }
}

struct BlockState {
    k: u32,
    block_size: usize,
    decoder: SourceBlockDecoder,
    source_symbols: HashMap<u32, Vec<u8>>,
    repair_count: u32,
    completed: Option<Vec<u8>>,
    first_seen: Instant,
}

impl BlockState {
    fn new(sbn_u8: u8, oti: &ObjectTransmissionInformation) -> Self {
        let block_size = oti.transfer_length() as usize;
        let k = (block_size / oti.symbol_size() as usize) as u32;
        Self {
            k,
            block_size,
            decoder: SourceBlockDecoder::new(sbn_u8, oti, block_size as u64),
            source_symbols: HashMap::new(),
            repair_count: 0,
            completed: None,
            first_seen: Instant::now(),
        }
    }

    fn ingest(&mut self, packet: EncodingPacket) {
        if self.completed.is_some() {
            return;
        }
        let esi = packet.payload_id().encoding_symbol_id();
        if esi < self.k {
            self.source_symbols
                .entry(esi)
                .or_insert_with(|| packet.data().to_vec());
        } else {
            self.repair_count += 1;
        }
        self.completed = self.decoder.decode(std::iter::once(packet));
    }

    fn best_effort(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.block_size];
        for (esi, sym) in &self.source_symbols {
            let off = *esi as usize * SYMBOL_SIZE;
            if off + SYMBOL_SIZE <= self.block_size {
                out[off..off + SYMBOL_SIZE].copy_from_slice(sym);
            }
        }
        out
    }
}

fn unwrap_sbn(cursor: u64, sbn_u8: u8) -> u64 {
    let cursor_low = (cursor & 0xFF) as u8;
    let diff = sbn_u8.wrapping_sub(cursor_low) as i8 as i64;
    (cursor as i64 + diff).max(0) as u64
}

fn run_decoder(rx: Receiver<Vec<u8>>, args: Args) -> Result<()> {
    let timeout = Duration::from_millis(args.block_timeout_ms);
    let max_in_flight = args.max_blocks_in_flight as u64;
    let pid = args.pid;
    let poll_tick = Duration::from_millis(20);

    let mut pool: BTreeMap<u64, BlockState> = BTreeMap::new();
    let mut cursor: u64 = 0;
    let mut highest_seen: u64 = 0;
    let mut started = false;

    let mut stdout = std::io::stdout().lock();
    let mut decoded_blocks = 0u64;
    let mut besteffort_blocks = 0u64;
    let mut zero_blocks = 0u64;
    let mut dropped_packets = 0u64;

    loop {
        match rx.recv_timeout(poll_tick) {
            Ok(bytes) => match parse_packet(&bytes, pid) {
                Ok(ParsedFrame::Data {
                    oti,
                    payload_id,
                    symbol,
                }) => {
                    let sbn_u8 = payload_id.source_block_number();
                    let logical = if started {
                        unwrap_sbn(cursor, sbn_u8)
                    } else {
                        cursor = sbn_u8 as u64;
                        highest_seen = cursor;
                        started = true;
                        sbn_u8 as u64
                    };
                    if logical < cursor {
                        dropped_packets += 1;
                        trace!(logical, cursor, "stale packet dropped");
                        continue;
                    }
                    if logical > highest_seen {
                        highest_seen = logical;
                    }
                    let entry = pool
                        .entry(logical)
                        .or_insert_with(|| BlockState::new(sbn_u8, &oti));
                    let pkt = EncodingPacket::new(payload_id, symbol);
                    entry.ingest(pkt);
                }
                Ok(ParsedFrame::Noop) => {}
                Err(e) => {
                    dropped_packets += 1;
                    trace!(error = %e, "dropping unparseable packet");
                }
            },
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                info!(
                    decoded_blocks,
                    besteffort_blocks, zero_blocks, dropped_packets, "input closed; flushing"
                );
                flush_remaining(&mut pool, &mut cursor, &mut stdout)?;
                return Ok(());
            }
        }

        loop {
            let should_force = pool
                .keys()
                .next_back()
                .copied()
                .map(|hi| hi >= cursor + max_in_flight)
                .unwrap_or(false);

            match pool.get(&cursor) {
                Some(b) if b.completed.is_some() => {
                    let data = pool.remove(&cursor).unwrap().completed.unwrap();
                    stdout.write_all(&data).context("writing stdout")?;
                    decoded_blocks += 1;
                    debug!(sbn = cursor, "emitted decoded block");
                    cursor += 1;
                }
                Some(b) if b.first_seen.elapsed() >= timeout || should_force => {
                    let recovered = b.source_symbols.len();
                    let repair = b.repair_count;
                    let data = b.best_effort();
                    pool.remove(&cursor);
                    stdout.write_all(&data).context("writing stdout")?;
                    besteffort_blocks += 1;
                    warn!(
                        sbn = cursor,
                        recovered, repair, "best-effort emit (timeout)"
                    );
                    cursor += 1;
                }
                None if started && (should_force || highest_seen >= cursor + max_in_flight) => {
                    let data = vec![0u8; raptorq_ts_common::DEFAULT_BLOCK_SIZE];
                    stdout.write_all(&data).context("writing stdout")?;
                    zero_blocks += 1;
                    warn!(sbn = cursor, "emitting zero block (no symbols)");
                    cursor += 1;
                }
                _ => break,
            }
        }
    }
}

fn flush_remaining(
    pool: &mut BTreeMap<u64, BlockState>,
    cursor: &mut u64,
    stdout: &mut impl Write,
) -> Result<()> {
    let keys: Vec<u64> = pool.keys().copied().collect();
    for k in keys {
        while *cursor < k {
            *cursor += 1;
        }
        if let Some(b) = pool.remove(&k) {
            let data = match b.completed {
                Some(d) => d,
                None => b.best_effort(),
            };
            stdout.write_all(&data).context("writing stdout")?;
            *cursor = k + 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwrap_sbn_forward() {
        assert_eq!(unwrap_sbn(300, 50), 306);
        assert_eq!(unwrap_sbn(300, 45), 301);
        assert_eq!(unwrap_sbn(300, 44), 300);
    }

    #[test]
    fn unwrap_sbn_stale() {
        assert_eq!(unwrap_sbn(300, 200), 200);
    }
}
