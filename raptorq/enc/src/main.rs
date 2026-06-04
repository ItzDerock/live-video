use std::collections::VecDeque;
use std::io::{Read, Write};
use std::option::Option;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockEncoder};
use raptorq_ts_common::{
    build_data_packet, build_noop_packet, make_block_oti, ContinuityCounter, DEFAULT_BLOCK_SIZE,
    DEFAULT_REPAIR_OVERHEAD, SYMBOL_SIZE, TS_PACKET_SIZE,
};
use tracing::{debug, info, warn};

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputMode {
    /// Output raw to stdout
    Stdout,

    /// Bind a ZeroMQ PUSH socket; receiver connects with PULL
    Zmq,
}

enum Sink {
    Stdout,
    Zmq(zmq::Socket),
}

#[derive(Parser, Debug)]
#[command(
    name = "raptorq-enc",
    about = "Adds RaptorQ application-layer FEC on top of an MPEG-TS stream and outputs \
             a paced TS-shaped wire stream suitable for piping into dvbs2-tx."
)]
struct Args {
    /// Output muxrate in bits per second. Must match dvbs2-tx muxrate so the FD never stalls.
    #[arg(long)]
    rate: u64,

    /// Source-block size in bytes. Must be a multiple of the symbol size.
    #[arg(long, default_value_t = DEFAULT_BLOCK_SIZE)]
    block_size: usize,

    /// Baseline (floor) fraction of repair symbols added per block, always sent
    /// in-band. With adaptive FEC this is the minimum; idle airtime adds more.
    #[arg(long, default_value_t = DEFAULT_REPAIR_OVERHEAD)]
    repair_overhead: f32,

    /// Use otherwise-idle airtime to send extra repair symbols for recent
    /// in-flight blocks instead of noop stuffing. Floors at --repair-overhead.
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    adaptive_fec: bool,

    /// Ceiling on per-block repair fraction when adaptive FEC fills idle
    /// airtime. total >= K/(1-p) survives loss fraction p; 1.0 => survive ~50%.
    #[arg(long, default_value_t = 1.0)]
    max_repair_overhead: f32,

    /// How long extra repair for a block stays useful, i.e. how long the
    /// decoder is expected to hold it. Should track the decoder's
    /// --block-timeout-ms. Blocks older than this get no extra repair.
    #[arg(long, default_value_t = 700)]
    repair_horizon_ms: u64,

    /// TS PID for our framed packets.
    #[arg(long, value_parser = parse_pid, default_value = "0x100")]
    pid: u16,

    /// Channel capacity in number of fully-encoded blocks waiting for the writer.
    #[arg(long, default_value_t = 4)]
    queue_blocks: usize,

    /// Output mode.
    #[arg(value_enum, default_value_t = OutputMode::Stdout)]
    output: OutputMode,

    /// ZeroMQ endpoint to bind (required when output=zmq).
    /// Examples: tcp://*:5555   ipc:///tmp/raptorq.sock
    #[arg(long, required_if_eq("output", "zmq"))]
    output_sock: Option<String>,
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

    if args.block_size == 0 || args.block_size % SYMBOL_SIZE != 0 {
        anyhow::bail!(
            "block_size {} must be a positive multiple of symbol size {}",
            args.block_size,
            SYMBOL_SIZE
        );
    }

    if !args.repair_overhead.is_finite() || args.repair_overhead < 0.0 {
        anyhow::bail!("repair_overhead must be >= 0");
    }

    if !args.max_repair_overhead.is_finite() || args.max_repair_overhead < args.repair_overhead {
        anyhow::bail!("max_repair_overhead must be finite and >= repair_overhead");
    }

    if args.rate == 0 {
        anyhow::bail!("rate must be > 0");
    }

    let k = (args.block_size / SYMBOL_SIZE) as u32;
    let repair = (k as f32 * args.repair_overhead).ceil() as u32;
    let max_repair = (k as f32 * args.max_repair_overhead).ceil() as u32;
    info!(
        block_size = args.block_size,
        k,
        repair,
        adaptive_fec = args.adaptive_fec,
        max_repair,
        repair_horizon_ms = args.repair_horizon_ms,
        rate_bps = args.rate,
        pid = format!("{:#x}", args.pid),
        "encoder configured"
    );

    let oti = make_block_oti(args.block_size as u64, SYMBOL_SIZE as u16);

    // Pay raptorq's ~300ms first-block table-build cost now, before the paced
    // writer starts, so the live stream doesn't stall on its first block.
    let warm = Instant::now();
    raptorq_ts_common::warm_raptorq(args.block_size as u64, SYMBOL_SIZE as u16);
    debug!(elapsed_ms = warm.elapsed().as_millis() as u64, "raptorq tables warmed");

    let sink = match args.output {
        OutputMode::Stdout => Sink::Stdout,
        OutputMode::Zmq => {
            let ctx = zmq::Context::new();
            let socket = ctx.socket(zmq::PUSH).context("create zmq push socket")?;
            let addr = args.output_sock.as_deref().unwrap();
            socket
                .bind(addr)
                .with_context(|| format!("zmq bind to {addr}"))?;
            info!(addr, "zmq push socket bound");
            Sink::Zmq(socket)
        }
    };

    let (block_tx, block_rx) = sync_channel::<Vec<u8>>(args.queue_blocks);
    let (pkt_tx, pkt_rx) =
        sync_channel::<EncodingPacket>(args.queue_blocks * (k + repair) as usize);

    let reader = thread::Builder::new().name("ts-reader".into()).spawn({
        let block_size = args.block_size;
        move || reader_thread(block_size, block_tx)
    })?;

    let encoder = thread::Builder::new()
        .name("block-encoder".into())
        .spawn(move || {
            encoder_thread(
                oti,
                repair,
                max_repair,
                args.adaptive_fec,
                Duration::from_millis(args.repair_horizon_ms),
                block_rx,
                pkt_tx,
            )
        })?;

    let writer = thread::Builder::new()
        .name("paced-writer".into())
        .spawn(move || writer_thread(args.pid, args.rate, oti, pkt_rx, sink))?;

    let reader_res = reader.join().expect("reader panicked");
    let encoder_res = encoder.join().expect("encoder panicked");
    let writer_res = writer.join().expect("writer panicked");

    reader_res?;
    encoder_res?;
    writer_res?;
    Ok(())
}

fn reader_thread(block_size: usize, out: SyncSender<Vec<u8>>) -> Result<()> {
    let mut stdin = std::io::stdin().lock();
    let mut block = vec![0u8; block_size];
    let mut filled = 0usize;
    loop {
        match stdin.read(&mut block[filled..]) {
            Ok(0) => {
                if filled > 0 {
                    warn!(
                        filled,
                        "stdin closed mid-block; padding with zero and flushing"
                    );
                    for b in &mut block[filled..] {
                        *b = 0;
                    }
                    let _ = out.send(std::mem::take(&mut block));
                }
                info!("stdin closed; reader exiting");
                return Ok(());
            }
            Ok(n) => {
                filled += n;
                if filled == block_size {
                    out.send(std::mem::replace(&mut block, vec![0u8; block_size]))
                        .context("encoder receiver dropped")?;
                    filled = 0;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e).context("reading stdin"),
        }
    }
}

/// A recently-encoded block kept alive so idle airtime can mint more repair
/// symbols for it on demand. RaptorQ is a fountain code, so repair is unbounded.
struct BlockGen {
    encoder: SourceBlockEncoder,
    /// Next repair symbol id to generate; starts past the in-band baseline.
    next_repair_id: u32,
    /// Ceiling on total repair symbols for this block (--max-repair-overhead).
    max_repair: u32,
    /// When the block's source went out; used to drop it once the decoder would
    /// no longer be holding it.
    created: Instant,
}

impl BlockGen {
    fn exhausted(&self) -> bool {
        self.next_repair_id >= self.max_repair
    }
}

/// Cheap deterministic PRNG (xorshift64) for recency-weighted repair targeting.
struct Xorshift64(u64);

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// ~50/50 coin flip.
    fn coin(&mut self) -> bool {
        (self.next_u64() >> 33) & 1 == 1
    }
}

/// Hard cap on retained blocks, guarding against a very large horizon.
const MAX_RING: usize = 64;

/// Drop blocks the decoder would no longer hold (older than the horizon) or that
/// hit their repair ceiling, and enforce the ring cap. Created times are
/// monotonic, so only the front needs horizon checking.
fn prune_ring(ring: &mut VecDeque<BlockGen>, horizon: Duration) {
    while ring.len() > MAX_RING {
        ring.pop_front();
    }
    while let Some(front) = ring.front() {
        if front.created.elapsed() > horizon || front.exhausted() {
            ring.pop_front();
        } else {
            break;
        }
    }
}

/// Pick which live block gets the next idle repair symbol, weighted toward the
/// freshest (geometric: freshest 1/2, next 1/4, ...). The freshest block is the
/// one most likely still in the decoder's pool; spreading to older live blocks
/// hedges against a loss burst the (feedback-less) encoder cannot see. Returns a
/// deque index, or None if every retained block is exhausted.
fn pick_block(ring: &VecDeque<BlockGen>, rng: &mut Xorshift64) -> Option<usize> {
    let mut oldest_live = None;
    // Freshest (back) -> oldest (front). Horizon already enforced by prune_ring.
    for i in (0..ring.len()).rev() {
        if ring[i].exhausted() {
            continue;
        }
        oldest_live = Some(i);
        if rng.coin() {
            return Some(i);
        }
    }
    oldest_live
}

/// Encode one input block: emit its source symbols plus the baseline repair,
/// interleaved. Returns the live encoder for the repair ring, or `Err(())` if
/// the writer has gone away (signalling the thread to exit).
fn emit_block(
    block: &[u8],
    sbn: u8,
    oti: &ObjectTransmissionInformation,
    baseline_repair: u32,
    rep_interval: u32,
    out: &SyncSender<EncodingPacket>,
) -> std::result::Result<SourceBlockEncoder, ()> {
    let encoder = SourceBlockEncoder::new(sbn, oti, block);
    let source = encoder.source_packets();
    let repair_pkts = encoder.repair_packets(0, baseline_repair);
    debug!(
        sbn,
        src = source.len(),
        repair = repair_pkts.len(),
        "block encoded"
    );
    for p in interleave(source, repair_pkts, rep_interval) {
        if out.send(p).is_err() {
            warn!("writer dropped; encoder exiting");
            return Err(());
        }
    }
    Ok(encoder)
}

fn encoder_thread(
    oti: ObjectTransmissionInformation,
    baseline_repair: u32,
    max_repair: u32,
    adaptive: bool,
    horizon: Duration,
    blocks: Receiver<Vec<u8>>,
    out: SyncSender<EncodingPacket>,
) -> Result<()> {
    let block_size = oti.transfer_length() as usize;
    let k_block = (block_size / SYMBOL_SIZE) as u32;
    let rep_interval = if baseline_repair > 0 {
        (k_block / baseline_repair).max(1)
    } else {
        u32::MAX
    };
    let mut sbn: u8 = 0;

    // Non-adaptive (or no headroom above the floor): original behaviour —
    // baseline repair only, block on input.
    if !adaptive || max_repair <= baseline_repair {
        while let Ok(block) = blocks.recv() {
            debug_assert_eq!(block.len(), block_size);
            if emit_block(&block, sbn, &oti, baseline_repair, rep_interval, &out).is_err() {
                return Ok(());
            }
            sbn = sbn.wrapping_add(1);
        }
        return Ok(());
    }

    // Adaptive: fill otherwise-idle airtime with extra repair for recent blocks
    // instead of letting the writer emit noop stuffing. The bounded packet
    // channel backpressures `out.send`, so this self-paces to the writer's rate
    // and only fires when there is genuine slack.
    let mut ring: VecDeque<BlockGen> = VecDeque::new();
    let mut rng = Xorshift64::new(0x9E37_79B9_7F4A_7C15);
    let idle_nap = Duration::from_millis(2);

    loop {
        match blocks.try_recv() {
            Ok(block) => {
                debug_assert_eq!(block.len(), block_size);
                let encoder =
                    match emit_block(&block, sbn, &oti, baseline_repair, rep_interval, &out) {
                        Ok(e) => e,
                        Err(()) => return Ok(()),
                    };
                ring.push_back(BlockGen {
                    encoder,
                    next_repair_id: baseline_repair,
                    max_repair,
                    created: Instant::now(),
                });
                prune_ring(&mut ring, horizon);
                sbn = sbn.wrapping_add(1);
                continue;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                info!("input closed; encoder exiting");
                return Ok(());
            }
        }

        // No fresh input: spend the idle slot on extra repair, if any block is
        // still worth protecting.
        prune_ring(&mut ring, horizon);
        if let Some(idx) = pick_block(&ring, &mut rng) {
            let bg = &mut ring[idx];
            let extra = bg.encoder.repair_packets(bg.next_repair_id, 1).pop();
            bg.next_repair_id += 1;
            if let Some(p) = extra {
                if out.send(p).is_err() {
                    warn!("writer dropped; encoder exiting");
                    return Ok(());
                }
            }
            continue;
        }

        // Genuinely nothing useful to send (no input, nothing left to protect).
        // Nap briefly; the writer covers this gap with noop stuffing.
        thread::sleep(idle_nap);
    }
}

fn writer_thread(
    pid: u16,
    rate_bps: u64,
    oti: ObjectTransmissionInformation,
    packets: Receiver<EncodingPacket>,
    mut sink: Sink,
) -> Result<()> {
    let bits_per_packet = (TS_PACKET_SIZE * 8) as u128;
    let nanos_per_packet = 1_000_000_000u128 * bits_per_packet / rate_bps as u128;

    let mut stdout = std::io::stdout().lock();
    let mut cc = ContinuityCounter::default();
    let mut buf = [0u8; TS_PACKET_SIZE];

    let start = Instant::now();
    let mut packets_sent: u64 = 0;
    let mut noops_sent: u64 = 0;
    let mut last_log = start;
    let mut input_closed = false;

    loop {
        let pkt = if input_closed {
            None
        } else {
            match packets.try_recv() {
                Ok(p) => Some(p),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => {
                    input_closed = true;
                    None
                }
            }
        };

        match pkt {
            Some(packet) => {
                build_data_packet(pid, cc.advance(), &oti, &packet, &mut buf);
                packets_sent += 1;
            }
            None => {
                if input_closed {
                    info!(packets_sent, noops_sent, "writer drained; exiting");
                    return Ok(());
                }
                build_noop_packet(pid, cc.advance(), &mut buf);
                noops_sent += 1;
            }
        }

        match &mut sink {
            Sink::Stdout => {
                stdout.write_all(&buf).context("stdout write")?;
                stdout.flush().context("stdout flush")?;
            }
            Sink::Zmq(s) => {
                s.send(buf.as_ref(), 0).context("zmq send")?;
            }
        }

        let total = packets_sent + noops_sent;
        let next_deadline_ns = nanos_per_packet * total as u128;
        let elapsed_ns = start.elapsed().as_nanos();
        if next_deadline_ns > elapsed_ns {
            thread::sleep(Duration::from_nanos((next_deadline_ns - elapsed_ns) as u64));
        }

        if Instant::now().duration_since(last_log) >= Duration::from_secs(5) {
            debug!(packets_sent, noops_sent, "writer heartbeat");
            last_log = Instant::now();
        }
    }
}

/// Interleave source and repair packets so that one repair packet follows every
/// `rep_interval` source packets.  Any leftover repairs are appended at the end.
fn interleave(
    source: Vec<EncodingPacket>,
    repair: Vec<EncodingPacket>,
    rep_interval: u32,
) -> impl Iterator<Item = EncodingPacket> {
    let mut src_it = source.into_iter();
    let mut rep_it = repair.into_iter();
    let mut src_count = 0u32;
    let mut next_rep_after = rep_interval;
    std::iter::from_fn(move || {
        if src_count >= next_rep_after {
            if let Some(r) = rep_it.next() {
                next_rep_after += rep_interval;
                return Some(r);
            }
        }
        match src_it.next() {
            Some(s) => {
                src_count += 1;
                Some(s)
            }
            None => rep_it.next(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq::SourceBlockEncoder;
    use raptorq_ts_common::{make_block_oti, DEFAULT_BLOCK_SIZE, DEFAULT_REPAIR_OVERHEAD, SYMBOL_SIZE};
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};

    fn make_packets(k: u32, repair: u32) -> (Vec<EncodingPacket>, Vec<EncodingPacket>) {
        let block_size = k as usize * SYMBOL_SIZE;
        let oti = make_block_oti(block_size as u64, SYMBOL_SIZE as u16);
        let data: Vec<u8> = (0..block_size).map(|i| (i & 0xFF) as u8).collect();
        let enc = SourceBlockEncoder::new(0, &oti, &data);
        (enc.source_packets(), enc.repair_packets(0, repair))
    }

    #[test]
    fn repair_packets_are_evenly_spaced() {
        let k: u32 = 392;
        let repair: u32 = 98;
        let rep_interval = (k / repair).max(1);
        let (source, repair_pkts) = make_packets(k, repair);

        let out: Vec<EncodingPacket> = interleave(source, repair_pkts, rep_interval).collect();
        assert_eq!(out.len(), (k + repair) as usize);

        // Between any two consecutive repair packets there must be at least
        // rep_interval - 1 source packets (i.e., no repair bursts).
        let repair_positions: Vec<usize> = out
            .iter()
            .enumerate()
            .filter(|(_, p)| p.payload_id().encoding_symbol_id() >= k)
            .map(|(i, _)| i)
            .collect();

        assert_eq!(repair_positions.len(), repair as usize, "wrong repair count");
        for window in repair_positions.windows(2) {
            let gap = window[1] - window[0];
            assert!(
                gap >= rep_interval as usize,
                "repair packets too close together: positions {} and {} (gap {}, need >= {})",
                window[0], window[1], gap, rep_interval
            );
        }
    }

    #[test]
    fn no_repair_passes_source_through_unchanged() {
        let k: u32 = 8;
        let (source, repair_pkts) = make_packets(k, 0);
        assert!(repair_pkts.is_empty());
        let out: Vec<EncodingPacket> = interleave(source.clone(), repair_pkts, u32::MAX).collect();
        assert_eq!(out.len(), source.len());
        for (a, b) in out.iter().zip(source.iter()) {
            assert_eq!(a.payload_id().encoding_symbol_id(), b.payload_id().encoding_symbol_id());
        }
    }

    #[test]
    fn default_block_interleave_survives_burst_loss_of_repair_cluster() {
        // With the old broken code all repairs were bunched after the 4th source
        // packet; losing positions 4..102 wiped the entire repair budget.
        // With the fix, a burst of that length only hits ~25 repairs, leaving
        // enough to decode.
        let k = (DEFAULT_BLOCK_SIZE / SYMBOL_SIZE) as u32;
        let repair = (k as f32 * DEFAULT_REPAIR_OVERHEAD).ceil() as u32;
        let rep_interval = (k / repair).max(1);
        let (source, repair_pkts) = make_packets(k, repair);

        let out: Vec<EncodingPacket> = interleave(source, repair_pkts, rep_interval).collect();

        // Drop a burst of rep_interval * 2 packets starting at position 4
        // (where the old code placed all repairs).
        let burst_start = 4usize;
        let burst_len = rep_interval as usize * 2;
        let surviving: Vec<&EncodingPacket> = out
            .iter()
            .enumerate()
            .filter(|(i, _)| *i < burst_start || *i >= burst_start + burst_len)
            .map(|(_, p)| p)
            .collect();

        let repairs_surviving = surviving
            .iter()
            .filter(|p| p.payload_id().encoding_symbol_id() >= k)
            .count();

        // After a burst of 2*rep_interval, we should lose at most 2 repair
        // packets (one per rep_interval), leaving the vast majority intact.
        assert!(
            repairs_surviving >= (repair - 2) as usize,
            "too many repairs lost in burst: {} surviving out of {repair}",
            repairs_surviving
        );
    }

    fn dummy_blockgen(next_repair_id: u32, max_repair: u32, created: Instant) -> BlockGen {
        let k = 8u32;
        let block_size = k as usize * SYMBOL_SIZE;
        let oti = make_block_oti(block_size as u64, SYMBOL_SIZE as u16);
        let encoder = SourceBlockEncoder::new(0, &oti, &vec![0u8; block_size]);
        BlockGen {
            encoder,
            next_repair_id,
            max_repair,
            created,
        }
    }

    #[test]
    fn pick_block_prefers_freshest() {
        let now = Instant::now();
        let mut ring: VecDeque<BlockGen> = VecDeque::new();
        // 3 live blocks; freshest is the back (index 2).
        for _ in 0..3 {
            ring.push_back(dummy_blockgen(0, 100, now));
        }
        let mut rng = Xorshift64::new(0x1234_5678);
        let n = 20_000;
        let mut counts = [0usize; 3];
        for _ in 0..n {
            counts[pick_block(&ring, &mut rng).unwrap()] += 1;
        }
        // Freshest dominates and lands near the geometric 1/2.
        assert!(
            counts[2] > counts[1] && counts[2] > counts[0],
            "freshest should dominate: {counts:?}"
        );
        let frac = counts[2] as f64 / n as f64;
        assert!((0.45..0.55).contains(&frac), "freshest ~1/2: {counts:?}");
    }

    #[test]
    fn pick_block_skips_exhausted() {
        let now = Instant::now();
        let mut ring: VecDeque<BlockGen> = VecDeque::new();
        ring.push_back(dummy_blockgen(0, 100, now)); // live
        ring.push_back(dummy_blockgen(50, 50, now)); // exhausted (freshest)
        let mut rng = Xorshift64::new(7);
        for _ in 0..1000 {
            assert_eq!(pick_block(&ring, &mut rng), Some(0));
        }
    }

    #[test]
    fn pick_block_none_when_all_exhausted() {
        let now = Instant::now();
        let mut ring: VecDeque<BlockGen> = VecDeque::new();
        ring.push_back(dummy_blockgen(50, 50, now));
        let mut rng = Xorshift64::new(1);
        assert_eq!(pick_block(&ring, &mut rng), None);
    }

    #[test]
    fn prune_ring_drops_exhausted_and_expired_front() {
        let now = Instant::now();
        let horizon = Duration::from_millis(500);

        // Exhausted front is dropped, live block kept.
        let mut ring: VecDeque<BlockGen> = VecDeque::new();
        ring.push_back(dummy_blockgen(50, 50, now));
        ring.push_back(dummy_blockgen(0, 100, now));
        prune_ring(&mut ring, horizon);
        assert_eq!(ring.len(), 1);
        assert!(!ring[0].exhausted());

        // Expired front (older than horizon) is dropped.
        if let Some(old) = now.checked_sub(Duration::from_secs(5)) {
            let mut ring: VecDeque<BlockGen> = VecDeque::new();
            ring.push_back(dummy_blockgen(0, 100, old));
            ring.push_back(dummy_blockgen(0, 100, now));
            prune_ring(&mut ring, horizon);
            assert_eq!(ring.len(), 1);
        }
    }

    #[test]
    fn coin_is_roughly_fair() {
        let mut rng = Xorshift64::new(0xDEAD_BEEF);
        let n = 100_000;
        let heads = (0..n).filter(|_| rng.coin()).count();
        let frac = heads as f64 / n as f64;
        assert!((0.47..0.53).contains(&frac), "coin biased: {frac}");
    }
}
