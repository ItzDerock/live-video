use std::collections::{BTreeMap, HashMap};
use std::io::{BufWriter, Read, Write};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder};
use raptorq_ts_common::{
    parse_packet, ParsedFrame, SYMBOL_SIZE, TS_PACKET_SIZE, TS_SYNC_BYTE,
};
use tracing::{debug, info, trace, warn};

#[derive(Parser, Debug)]
#[command(
    name = "raptorq-dec",
    about = "Reads TS-shaped wire packets from dvbs2-rx, reconstructs the original \
             MPEG-TS stream using RaptorQ, and emits it in order on stdout."
)]
struct Args {
    #[arg(long, value_parser = parse_pid, default_value = "0x100")]
    pid: u16,

    /// Number of in-flight blocks to keep state for.
    #[arg(long, default_value_t = 8)]
    max_blocks_in_flight: usize,

    /// How long to wait before giving up on a block and emitting best-effort.
    #[arg(long, default_value_t = 700)]
    block_timeout_ms: u64,

    /// Print link stats to stderr every N seconds (0 = off).
    #[arg(long, default_value_t = 10)]
    stats_interval_secs: u64,
}

fn parse_pid(s: &str) -> std::result::Result<u16, String> {
    let s = s.trim_start_matches("0x").trim_start_matches("0X");
    u16::from_str_radix(s, 16).map_err(|e| format!("invalid pid {s:?}: {e}"))
}

/// Largest K (source symbols per block) the raptorq decoder accepts; building a
/// `SourceBlockDecoder` with more trips an assertion deep inside the crate
/// (`systematic_constants::extended_source_block_symbols`). Mirrors raptorq's
/// private `MAX_SOURCE_SYMBOLS_PER_BLOCK`.
const MAX_SOURCE_SYMBOLS_PER_BLOCK: u64 = 56403;

/// Reject OTIs that a corrupt-but-still-parseable DATA frame can carry. The link
/// is lossy, so a packet can pass TS framing (sync, PID, frame type) yet hold
/// garbage in its OTI field. Our encoder always uses the fixed wire `SYMBOL_SIZE`
/// and a block whose K is in range, so anything else is corruption. Feeding it on
/// would divide-by-zero (`symbol_size == 0`) or trip raptorq's K assertion.
fn oti_is_sane(oti: &ObjectTransmissionInformation) -> bool {
    if oti.symbol_size() as usize != SYMBOL_SIZE {
        return false;
    }
    let sym = SYMBOL_SIZE as u64;
    let transfer = oti.transfer_length();
    transfer > 0 && transfer % sym == 0 && (transfer / sym) <= MAX_SOURCE_SYMBOLS_PER_BLOCK
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
    // Don't let a reader-thread panic re-panic the main thread on shutdown.
    match reader.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(error = %e, "reader thread exited with error"),
        Err(_) => warn!("reader thread panicked"),
    }
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

#[derive(Default)]
struct IntervalStats {
    decoded: u64,
    besteffort: u64,
    be_timed_out: u64,
    be_forced: u64,
    /// sum of (src_rcvd + repair_rcvd) for best-effort blocks
    be_syms_rcvd: u64,
    /// sum of K for best-effort blocks
    be_syms_needed: u64,
    zero: u64,
    /// data symbols accepted into a still-open block
    pkts_ok: u64,
    /// noop/stuffing packets received
    noops: u64,
    /// data packets for a block already emitted: unneeded repair, NOT loss
    pkts_redundant: u64,
    /// packets that failed to parse (corruption); NOT counted as link loss
    pkts_unparseable: u64,
    /// packets that never arrived, inferred from continuity-counter gaps
    pkts_lost: u64,
}

fn log_interval_stats(s: &IntervalStats, elapsed: f64) {
    let be_fill_pct = if s.be_syms_needed > 0 {
        s.be_syms_rcvd * 100 / s.be_syms_needed
    } else {
        100
    };
    // Every parsed frame (data + noop) carries a continuity counter, so the
    // count of packets that actually arrived on the wire is their sum. Add the
    // gap-inferred losses to recover how many were sent.
    let recv = s.pkts_ok + s.pkts_redundant + s.noops;
    let sent = recv + s.pkts_lost;
    let loss_pct = if sent > 0 {
        s.pkts_lost as f64 * 100.0 / sent as f64
    } else {
        0.0
    };
    // Fraction of received DATA packets that were redundant repair. On a clean
    // link this equals the encoder's repair overhead (e.g. 0.25/1.25 = 20%)
    // and is expected, not loss.
    let data_recv = s.pkts_ok + s.pkts_redundant;
    let redundant_pct = if data_recv > 0 {
        s.pkts_redundant as f64 * 100.0 / data_recv as f64
    } else {
        0.0
    };
    info!(
        interval_s = format!("{elapsed:.1}"),
        decoded = s.decoded,
        best_effort = s.besteffort,
        be_fill_pct,
        be_timed_out = s.be_timed_out,
        be_forced = s.be_forced,
        zero_blocks = s.zero,
        pkts_ok = s.pkts_ok,
        noops = s.noops,
        redundant = s.pkts_redundant,
        redundant_pct = format!("{redundant_pct:.2}"),
        unparseable = s.pkts_unparseable,
        lost = s.pkts_lost,
        loss_pct = format!("{loss_pct:.2}"),
        "link stats",
    );
}

fn unwrap_sbn(cursor: u64, sbn_u8: u8) -> u64 {
    let cursor_low = (cursor & 0xFF) as u8;
    let diff = sbn_u8.wrapping_sub(cursor_low) as i8 as i64;
    (cursor as i64 + diff).max(0) as u64
}

/// The TS continuity counter increments once per packet on the PID (mod 16),
/// across both data and noop frames. A gap therefore means packets vanished in
/// transit — genuine link loss. Returns the number missed since the previous
/// packet and updates `last_cc`. Bursts of >= 16 consecutive losses alias the
/// 4-bit field and are undercounted.
fn cc_gap(last_cc: &mut Option<u8>, cc: u8) -> u64 {
    let gap = match *last_cc {
        Some(prev) => {
            let expected = (prev + 1) & 0x0F;
            (cc.wrapping_sub(expected) as u64) & 0x0F
        }
        None => 0,
    };
    *last_cc = Some(cc);
    gap
}

fn run_decoder(rx: Receiver<Vec<u8>>, args: Args) -> Result<()> {
    let timeout = Duration::from_millis(args.block_timeout_ms);
    let max_in_flight = args.max_blocks_in_flight as u64;
    let pid = args.pid;
    let poll_tick = Duration::from_millis(20);
    let stats_interval = (args.stats_interval_secs > 0)
        .then(|| Duration::from_secs(args.stats_interval_secs));

    let mut pool: BTreeMap<u64, BlockState> = BTreeMap::new();
    let mut cursor: u64 = 0;
    let mut highest_seen: u64 = 0;
    let mut started = false;

    let mut stdout = BufWriter::new(std::io::stdout().lock());
    let mut decoded_blocks = 0u64;
    let mut besteffort_blocks = 0u64;
    let mut zero_blocks = 0u64;
    let mut redundant_packets = 0u64;
    let mut lost_packets = 0u64;
    let mut last_cc: Option<u8> = None;

    let mut istats = IntervalStats::default();
    let mut last_stats = Instant::now();

    loop {
        match rx.recv_timeout(poll_tick) {
            Ok(bytes) => match parse_packet(&bytes, pid) {
                Ok(ParsedFrame::Data {
                    cc,
                    oti,
                    payload_id,
                    symbol,
                }) => {
                    let gap = cc_gap(&mut last_cc, cc);
                    istats.pkts_lost += gap;
                    lost_packets += gap;
                    if !oti_is_sane(&oti) {
                        // Parsed as DATA but the OTI is garbage (corruption that
                        // slipped past TS framing). Building a decoder from it
                        // would panic, so drop it like any unusable frame.
                        istats.pkts_unparseable += 1;
                        trace!("dropping data frame with insane OTI");
                        continue;
                    }
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
                        // Block already emitted; this is unneeded repair, not loss.
                        redundant_packets += 1;
                        istats.pkts_redundant += 1;
                        trace!(logical, cursor, "redundant packet (block already emitted)");
                        continue;
                    }
                    if logical > highest_seen {
                        highest_seen = logical;
                    }
                    istats.pkts_ok += 1;
                    let entry = pool
                        .entry(logical)
                        .or_insert_with(|| BlockState::new(sbn_u8, &oti));
                    let pkt = EncodingPacket::new(payload_id, symbol);
                    entry.ingest(pkt);
                }
                Ok(ParsedFrame::Noop { cc }) => {
                    let gap = cc_gap(&mut last_cc, cc);
                    istats.pkts_lost += gap;
                    lost_packets += gap;
                    istats.noops += 1;
                }
                Err(e) => {
                    // Corrupt frame: cannot read its cc, so it is bucketed
                    // separately rather than counted as a continuity gap.
                    istats.pkts_unparseable += 1;
                    trace!(error = %e, "dropping unparseable packet");
                }
            },
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                info!(
                    decoded_blocks,
                    besteffort_blocks,
                    zero_blocks,
                    redundant_packets,
                    lost_packets,
                    "input closed; flushing"
                );
                flush_remaining(&mut pool, &mut cursor, &mut stdout)?;
                stdout.flush().context("flushing stdout")?;
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
                    // Guard guarantees both are Some; degrade to break rather
                    // than unwrap-panic if that ever stops holding.
                    let data = match pool.remove(&cursor).and_then(|b| b.completed) {
                        Some(d) => d,
                        None => break,
                    };
                    stdout.write_all(&data).context("writing stdout")?;
                    decoded_blocks += 1;
                    istats.decoded += 1;
                    debug!(sbn = cursor, "emitted decoded block");
                    cursor += 1;
                }
                Some(b) if b.first_seen.elapsed() >= timeout || should_force => {
                    let timed_out = b.first_seen.elapsed() >= timeout;
                    let recovered = b.source_symbols.len();
                    let repair = b.repair_count;
                    let k = b.k;
                    let data = b.best_effort();
                    pool.remove(&cursor);
                    stdout.write_all(&data).context("writing stdout")?;
                    besteffort_blocks += 1;
                    istats.besteffort += 1;
                    istats.be_syms_rcvd += recovered as u64 + repair as u64;
                    istats.be_syms_needed += k as u64;
                    if timed_out {
                        istats.be_timed_out += 1;
                    } else {
                        istats.be_forced += 1;
                    }
                    warn!(
                        sbn = cursor,
                        recovered,
                        repair,
                        k,
                        timed_out,
                        "best-effort emit"
                    );
                    cursor += 1;
                }
                None if started && (should_force || highest_seen >= cursor + max_in_flight) => {
                    let data = vec![0u8; raptorq_ts_common::DEFAULT_BLOCK_SIZE];
                    stdout.write_all(&data).context("writing stdout")?;
                    zero_blocks += 1;
                    istats.zero += 1;
                    warn!(sbn = cursor, "emitting zero block (no symbols)");
                    cursor += 1;
                }
                _ => break,
            }
        }

        // Push every emitted block downstream immediately; otherwise blocks sit
        // in the buffer and the player stalls (and a killed process loses them).
        stdout.flush().context("flushing stdout")?;

        if let Some(interval) = stats_interval {
            let elapsed = last_stats.elapsed();
            if elapsed >= interval {
                log_interval_stats(&istats, elapsed.as_secs_f64());
                istats = IntervalStats::default();
                last_stats = Instant::now();
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

    fn default_oti() -> ObjectTransmissionInformation {
        raptorq_ts_common::make_block_oti(
            raptorq_ts_common::DEFAULT_BLOCK_SIZE as u64,
            SYMBOL_SIZE as u16,
        )
    }

    #[test]
    fn oti_sane_accepts_encoder_default() {
        // Round-trip through the wire form the decoder actually deserializes.
        let oti = ObjectTransmissionInformation::deserialize(&default_oti().serialize());
        assert!(oti_is_sane(&oti));
    }

    #[test]
    fn oti_sane_rejects_corrupt_oti() {
        // Huge transfer_length, valid symbol size -> K far over the raptorq cap.
        // This is the exact corruption that used to panic the decoder; note
        // deserialize() does no validation, so only oti_is_sane stops it.
        let mut bytes = default_oti().serialize();
        bytes[0] = 0xFF;
        bytes[1] = 0xFF;
        assert!(!oti_is_sane(&ObjectTransmissionInformation::deserialize(&bytes)));

        // All-0xFF garbage: wrong symbol size.
        assert!(!oti_is_sane(&ObjectTransmissionInformation::deserialize(&[0xFF; 12])));

        // In-range transfer length that is not a whole number of symbols.
        let mut bytes = default_oti().serialize();
        bytes[4] = bytes[4].wrapping_add(1);
        assert!(!oti_is_sane(&ObjectTransmissionInformation::deserialize(&bytes)));
    }
}
