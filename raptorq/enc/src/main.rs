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
    DEFAULT_PID, DEFAULT_REPAIR_OVERHEAD, SYMBOL_SIZE, TS_PACKET_SIZE,
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

    /// Fraction of repair symbols added per block.
    #[arg(long, default_value_t = DEFAULT_REPAIR_OVERHEAD)]
    repair_overhead: f32,

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

    if args.rate == 0 {
        anyhow::bail!("rate must be > 0");
    }

    let k = (args.block_size / SYMBOL_SIZE) as u32;
    let repair = (k as f32 * args.repair_overhead).ceil() as u32;
    info!(
        block_size = args.block_size,
        k,
        repair,
        rate_bps = args.rate,
        pid = format!("{:#x}", args.pid),
        "encoder configured"
    );

    let oti = make_block_oti(args.block_size as u64, SYMBOL_SIZE as u16);

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
        .spawn(move || encoder_thread(oti, repair, block_rx, pkt_tx))?;

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

fn encoder_thread(
    oti: ObjectTransmissionInformation,
    repair: u32,
    blocks: Receiver<Vec<u8>>,
    out: SyncSender<EncodingPacket>,
) -> Result<()> {
    let block_size = oti.transfer_length() as usize;
    let mut sbn: u8 = 0;
    while let Ok(block) = blocks.recv() {
        debug_assert_eq!(block.len(), block_size);
        let encoder = SourceBlockEncoder::new(sbn, &oti, &block);
        let source = encoder.source_packets();
        let repair_pkts = encoder.repair_packets(0, repair);
        debug!(
            sbn,
            src = source.len(),
            repair = repair_pkts.len(),
            "block encoded"
        );
        let mut src_it = source.into_iter();
        let mut rep_it = repair_pkts.into_iter();
        let k_block = (block_size / SYMBOL_SIZE) as u32;
        let rep_interval = if repair > 0 {
            (k_block / repair).max(1)
        } else {
            u32::MAX
        };
        let mut src_count = 0u32;
        let interleaved = std::iter::from_fn(move || {
            if repair > 0 && src_count > 0 && src_count % rep_interval == 0 {
                if let Some(r) = rep_it.next() {
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
        });
        for p in interleaved {
            if out.send(p).is_err() {
                warn!("writer dropped; encoder exiting");
                return Ok(());
            }
        }
        sbn = sbn.wrapping_add(1);
    }
    Ok(())
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
