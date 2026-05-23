//! Library-level loopback tests for the framed RaptorQ pipeline.
//!
//! Builds a test stream, encodes each block with `SourceBlockEncoder`, frames the
//! packets through `build_data_packet`, optionally drops some, then decodes via
//! `parse_packet` + `SourceBlockDecoder` and checks the result.

use std::collections::HashSet;

use raptorq::{SourceBlockDecoder, SourceBlockEncoder};
use raptorq_ts_common::{
    build_data_packet, make_block_oti, parse_packet, ParsedFrame, DEFAULT_BLOCK_SIZE, SYMBOL_SIZE,
    TS_PACKET_SIZE,
};

fn deterministic_block(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for _ in 0..len {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        out.push((x >> 33) as u8);
    }
    out
}

fn frame_block(sbn: u8, block: &[u8], repair: u32) -> Vec<[u8; TS_PACKET_SIZE]> {
    let oti = make_block_oti(block.len() as u64, SYMBOL_SIZE as u16);
    let enc = SourceBlockEncoder::new(sbn, &oti, block);
    let mut wire = Vec::new();
    for (i, pkt) in enc
        .source_packets()
        .into_iter()
        .chain(enc.repair_packets(0, repair))
        .enumerate()
    {
        let mut buf = [0u8; TS_PACKET_SIZE];
        build_data_packet(0x100, (i & 0x0F) as u8, &oti, &pkt, &mut buf);
        wire.push(buf);
    }
    wire
}

fn decode_block(wire: &[[u8; TS_PACKET_SIZE]]) -> Option<Vec<u8>> {
    let mut dec: Option<SourceBlockDecoder> = None;
    let mut result: Option<Vec<u8>> = None;
    for pkt in wire {
        match parse_packet(pkt, 0x100).expect("frame parse") {
            ParsedFrame::Data {
                oti,
                payload_id,
                symbol,
            } => {
                let d = dec.get_or_insert_with(|| {
                    SourceBlockDecoder::new(
                        payload_id.source_block_number(),
                        &oti,
                        oti.transfer_length(),
                    )
                });
                if result.is_some() {
                    continue;
                }
                let p = raptorq::EncodingPacket::new(payload_id, symbol);
                result = d.decode(std::iter::once(p));
            }
            ParsedFrame::Noop => {}
        }
    }
    result
}

#[test]
fn clean_block_roundtrip() {
    let block = deterministic_block(1, DEFAULT_BLOCK_SIZE);
    let wire = frame_block(3, &block, 0);
    assert_eq!(wire.len() as usize, DEFAULT_BLOCK_SIZE / SYMBOL_SIZE);
    let out = decode_block(&wire).expect("decode");
    assert_eq!(out, block);
}

#[test]
fn drop_within_repair_budget_recovers() {
    // With 25% repair overhead, theoretical max loss = 0.25/1.25 = 20% of total packets.
    // Drop 18% to leave comfortable margin for raptorq's small per-block overhead.
    let block = deterministic_block(2, DEFAULT_BLOCK_SIZE);
    let k = (DEFAULT_BLOCK_SIZE / SYMBOL_SIZE) as u32;
    let repair = (k as f32 * 0.25).ceil() as u32;
    let wire = frame_block(5, &block, repair);

    let total = wire.len();
    let mut rng = 0x1234u64;
    let drop_count = total * 18 / 100;
    let mut drop_idxs = HashSet::new();
    while drop_idxs.len() < drop_count {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        drop_idxs.insert((rng as usize) % total);
    }
    let surviving: Vec<_> = wire
        .iter()
        .enumerate()
        .filter(|(i, _)| !drop_idxs.contains(i))
        .map(|(_, p)| *p)
        .collect();

    let out = decode_block(&surviving).expect("decode after 18% drop");
    assert_eq!(out, block);
}

#[test]
fn drop_beyond_repair_budget_fails() {
    let block = deterministic_block(7, DEFAULT_BLOCK_SIZE);
    let k = (DEFAULT_BLOCK_SIZE / SYMBOL_SIZE) as u32;
    let repair = (k as f32 * 0.25).ceil() as u32;
    let wire = frame_block(11, &block, repair);

    let total = wire.len();
    let truncated: Vec<_> = wire.into_iter().take(total - repair as usize - 5).collect();
    assert!(decode_block(&truncated).is_none());
}

#[test]
fn too_many_drops_fails_to_decode() {
    let block = deterministic_block(3, DEFAULT_BLOCK_SIZE);
    let k = (DEFAULT_BLOCK_SIZE / SYMBOL_SIZE) as u32;
    let repair = (k as f32 * 0.25).ceil() as u32;
    let wire = frame_block(9, &block, repair);
    let keep = (k - 5) as usize;
    let truncated: Vec<_> = wire.into_iter().take(keep).collect();
    assert!(decode_block(&truncated).is_none());
}

#[test]
fn many_blocks_in_sequence() {
    for sbn in 0u8..16 {
        let block = deterministic_block(100 + sbn as u64, DEFAULT_BLOCK_SIZE);
        let wire = frame_block(sbn, &block, 0);
        let out = decode_block(&wire).expect("decode");
        assert_eq!(out, block, "block {sbn} mismatch");
    }
}
