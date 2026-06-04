//! Wire format and TS-packet helpers shared by raptorq-enc and raptorq-dec.

use raptorq::{
    EncodingPacket, ObjectTransmissionInformation, PayloadId, SourceBlockDecoder,
    SourceBlockEncoder,
};
use thiserror::Error;

pub const TS_PACKET_SIZE: usize = 188;
pub const TS_SYNC_BYTE: u8 = 0x47;
pub const TS_HEADER_SIZE: usize = 4;
pub const TS_PAYLOAD_SIZE: usize = TS_PACKET_SIZE - TS_HEADER_SIZE;

pub const FRAME_TYPE_OFFSET: usize = 0;
pub const FRAME_OTI_OFFSET: usize = 1;
pub const FRAME_PAYLOAD_ID_OFFSET: usize = 13;
pub const FRAME_SYMBOL_OFFSET: usize = 17;
pub const FRAME_OVERHEAD: usize = FRAME_SYMBOL_OFFSET;

/// Size of the CRC-32 trailer appended to every frame. RaptorQ is an erasure
/// code: it recovers *missing* symbols but trusts every symbol it receives
/// bit-exact. The DVB-S RS layer can miscorrect a burst and hand up a corrupt
/// packet with TEI clear; without this check that corrupt symbol would silently
/// poison a whole decoded block. A failed CRC turns the frame into an erasure,
/// which RaptorQ is designed to recover.
pub const CRC_SIZE: usize = 4;
pub const SYMBOL_SIZE: usize = TS_PAYLOAD_SIZE - FRAME_OVERHEAD - CRC_SIZE;

/// Offset within the TS payload of the 4-byte CRC trailer. Everything before it
/// (frame type, OTI, payload id, symbol) is covered by the CRC.
pub const FRAME_CRC_OFFSET: usize = FRAME_SYMBOL_OFFSET + SYMBOL_SIZE;

pub const FRAME_TYPE_DATA: u8 = 0x01;
pub const FRAME_TYPE_NOOP: u8 = 0x02;

pub const DEFAULT_PID: u16 = 0x100;
pub const DEFAULT_BLOCK_SYMBOLS: u32 = 392;
pub const DEFAULT_BLOCK_SIZE: usize = DEFAULT_BLOCK_SYMBOLS as usize * SYMBOL_SIZE;
pub const DEFAULT_REPAIR_OVERHEAD: f32 = 0.25;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("packet wrong length: expected {} got {0}", TS_PACKET_SIZE)]
    WrongLength(usize),
    #[error("missing 0x47 sync byte")]
    BadSync,
    #[error("transport error indicator set")]
    TransportError,
    #[error("PID {got:#x} does not match expected {want:#x}")]
    WrongPid { got: u16, want: u16 },
    #[error("frame CRC mismatch (corrupt frame)")]
    BadCrc,
    #[error("unknown frame type {0:#x}")]
    UnknownFrameType(u8),
}

const fn crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC32_TABLE: [u32; 256] = crc32_table();

/// Standard CRC-32 (IEEE 802.3, reflected, poly 0xEDB88320) — the same value as
/// zlib/PNG. Used as the per-frame integrity trailer.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ b as u32) & 0xFF) as usize];
    }
    crc ^ 0xFFFF_FFFF
}

#[derive(Debug, Clone, Copy)]
pub struct TsHeader {
    pub tei: bool,
    pub payload_unit_start: bool,
    pub priority: bool,
    pub pid: u16,
    pub scrambling: u8,
    pub adaptation: u8,
    pub continuity_counter: u8,
}

impl TsHeader {
    pub fn parse(bytes: &[u8; TS_HEADER_SIZE]) -> Result<Self, ParseError> {
        if bytes[0] != TS_SYNC_BYTE {
            return Err(ParseError::BadSync);
        }
        Ok(Self {
            tei: bytes[1] & 0x80 != 0,
            payload_unit_start: bytes[1] & 0x40 != 0,
            priority: bytes[1] & 0x20 != 0,
            pid: (((bytes[1] & 0x1F) as u16) << 8) | bytes[2] as u16,
            scrambling: (bytes[3] >> 6) & 0x03,
            adaptation: (bytes[3] >> 4) & 0x03,
            continuity_counter: bytes[3] & 0x0F,
        })
    }

    pub fn write(&self, out: &mut [u8; TS_HEADER_SIZE]) {
        out[0] = TS_SYNC_BYTE;
        out[1] = ((self.tei as u8) << 7)
            | ((self.payload_unit_start as u8) << 6)
            | ((self.priority as u8) << 5)
            | ((self.pid >> 8) as u8 & 0x1F);
        out[2] = (self.pid & 0xFF) as u8;
        out[3] = ((self.scrambling & 0x03) << 6)
            | ((self.adaptation & 0x03) << 4)
            | (self.continuity_counter & 0x0F);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ContinuityCounter(u8);

impl ContinuityCounter {
    pub fn advance(&mut self) -> u8 {
        let v = self.0;
        self.0 = self.0.wrapping_add(1) & 0x0F;
        v
    }
}

/// Build a 188-byte data packet carrying one raptorq symbol.
pub fn build_data_packet(
    pid: u16,
    cc: u8,
    oti: &ObjectTransmissionInformation,
    pkt: &EncodingPacket,
    out: &mut [u8; TS_PACKET_SIZE],
) {
    let mut hdr = [0u8; TS_HEADER_SIZE];
    TsHeader {
        tei: false,
        payload_unit_start: false,
        priority: false,
        pid,
        scrambling: 0,
        adaptation: 1,
        continuity_counter: cc,
    }
    .write(&mut hdr);
    out[..TS_HEADER_SIZE].copy_from_slice(&hdr);

    out[TS_HEADER_SIZE + FRAME_TYPE_OFFSET] = FRAME_TYPE_DATA;
    out[TS_HEADER_SIZE + FRAME_OTI_OFFSET..TS_HEADER_SIZE + FRAME_OTI_OFFSET + 12]
        .copy_from_slice(&oti.serialize());
    out[TS_HEADER_SIZE + FRAME_PAYLOAD_ID_OFFSET..TS_HEADER_SIZE + FRAME_PAYLOAD_ID_OFFSET + 4]
        .copy_from_slice(&pkt.payload_id().serialize());
    let sym = pkt.data();
    debug_assert_eq!(sym.len(), SYMBOL_SIZE);
    out[TS_HEADER_SIZE + FRAME_SYMBOL_OFFSET..TS_HEADER_SIZE + FRAME_SYMBOL_OFFSET + SYMBOL_SIZE]
        .copy_from_slice(sym);

    write_frame_crc(out);
}

/// Compute the CRC-32 over the frame body and write it into the trailer. Covers
/// everything in the TS payload up to (but not including) the CRC field.
fn write_frame_crc(out: &mut [u8; TS_PACKET_SIZE]) {
    let crc = crc32(&out[TS_HEADER_SIZE..TS_HEADER_SIZE + FRAME_CRC_OFFSET]);
    out[TS_HEADER_SIZE + FRAME_CRC_OFFSET..].copy_from_slice(&crc.to_be_bytes());
}

/// Build a 188-byte noop stuffing packet.
pub fn build_noop_packet(pid: u16, cc: u8, out: &mut [u8; TS_PACKET_SIZE]) {
    let mut hdr = [0u8; TS_HEADER_SIZE];
    TsHeader {
        tei: false,
        payload_unit_start: false,
        priority: false,
        pid,
        scrambling: 0,
        adaptation: 1,
        continuity_counter: cc,
    }
    .write(&mut hdr);
    out[..TS_HEADER_SIZE].copy_from_slice(&hdr);
    out[TS_HEADER_SIZE] = FRAME_TYPE_NOOP;
    for b in &mut out[TS_HEADER_SIZE + 1..TS_HEADER_SIZE + FRAME_CRC_OFFSET] {
        *b = 0xFF;
    }
    write_frame_crc(out);
}

#[derive(Debug)]
pub enum ParsedFrame {
    Data {
        /// 4-bit TS continuity counter; gaps across consecutive frames reveal
        /// packets lost in transit.
        cc: u8,
        oti: ObjectTransmissionInformation,
        payload_id: PayloadId,
        symbol: Vec<u8>,
    },
    Noop {
        /// 4-bit TS continuity counter; see [`ParsedFrame::Data`].
        cc: u8,
    },
}

pub fn parse_packet(bytes: &[u8], expected_pid: u16) -> Result<ParsedFrame, ParseError> {
    if bytes.len() != TS_PACKET_SIZE {
        return Err(ParseError::WrongLength(bytes.len()));
    }
    let hdr_bytes: [u8; TS_HEADER_SIZE] = bytes[..TS_HEADER_SIZE].try_into().unwrap();
    let hdr = TsHeader::parse(&hdr_bytes)?;
    if hdr.tei {
        return Err(ParseError::TransportError);
    }
    if hdr.pid != expected_pid {
        return Err(ParseError::WrongPid {
            got: hdr.pid,
            want: expected_pid,
        });
    }
    let payload = &bytes[TS_HEADER_SIZE..];
    // Integrity gate before trusting any frame field. A corrupt-but-parseable
    // frame (DVB-S RS miscorrection, byte-stream resync aliasing) is rejected
    // here so RaptorQ never ingests a poisoned symbol or a garbage OTI.
    let stored_crc = u32::from_be_bytes(
        payload[FRAME_CRC_OFFSET..FRAME_CRC_OFFSET + CRC_SIZE]
            .try_into()
            .unwrap(),
    );
    if crc32(&payload[..FRAME_CRC_OFFSET]) != stored_crc {
        return Err(ParseError::BadCrc);
    }
    let frame_type = payload[FRAME_TYPE_OFFSET];
    match frame_type {
        FRAME_TYPE_NOOP => Ok(ParsedFrame::Noop {
            cc: hdr.continuity_counter,
        }),
        FRAME_TYPE_DATA => {
            let oti_bytes: [u8; 12] = payload[FRAME_OTI_OFFSET..FRAME_OTI_OFFSET + 12]
                .try_into()
                .unwrap();
            let oti = ObjectTransmissionInformation::deserialize(&oti_bytes);
            let pid_bytes: [u8; 4] = payload[FRAME_PAYLOAD_ID_OFFSET..FRAME_PAYLOAD_ID_OFFSET + 4]
                .try_into()
                .unwrap();
            let payload_id = PayloadId::deserialize(&pid_bytes);
            let symbol = payload[FRAME_SYMBOL_OFFSET..FRAME_SYMBOL_OFFSET + SYMBOL_SIZE].to_vec();
            Ok(ParsedFrame::Data {
                cc: hdr.continuity_counter,
                oti,
                payload_id,
                symbol,
            })
        }
        other => Err(ParseError::UnknownFrameType(other)),
    }
}

pub fn make_block_oti(block_size: u64, symbol_size: u16) -> ObjectTransmissionInformation {
    ObjectTransmissionInformation::new(block_size, symbol_size, 1, 1, 1)
}

/// Force raptorq to build its lazy RFC-6330 systematic-constant tables for the
/// given block geometry by running a throwaway encode→decode roundtrip. The
/// first `SourceBlockEncoder::new`/`SourceBlockDecoder` for a value of K costs
/// ~300ms while these (process-global) tables are computed; calling this at
/// startup moves that one-time stall off the live stream instead of glitching
/// the first block. Cheap once the tables for this K already exist.
pub fn warm_raptorq(block_size: u64, symbol_size: u16) {
    let oti = make_block_oti(block_size, symbol_size);
    let block = vec![0u8; block_size as usize];
    let enc = SourceBlockEncoder::new(0, &oti, &block);
    let mut packets = enc.source_packets();
    packets.extend(enc.repair_packets(0, 1));
    let mut dec = SourceBlockDecoder::new(0, &oti, block_size);
    let _ = dec.decode(packets);
}

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq::SourceBlockEncoder;

    #[test]
    fn frame_sizes_fit_ts_packet() {
        assert_eq!(FRAME_OVERHEAD + SYMBOL_SIZE + CRC_SIZE, TS_PAYLOAD_SIZE);
        assert_eq!(FRAME_CRC_OFFSET + CRC_SIZE, TS_PAYLOAD_SIZE);
        assert_eq!(TS_HEADER_SIZE + TS_PAYLOAD_SIZE, TS_PACKET_SIZE);
    }

    #[test]
    fn crc32_matches_known_vector() {
        // "123456789" -> 0xCBF43926 is the standard CRC-32 check value.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn warm_raptorq_runs() {
        // Just exercises the roundtrip path; must not panic for the default geometry.
        warm_raptorq(DEFAULT_BLOCK_SIZE as u64, SYMBOL_SIZE as u16);
    }

    #[test]
    fn crc_rejects_symbol_bit_flip() {
        let block: Vec<u8> = (0..DEFAULT_BLOCK_SIZE).map(|i| (i & 0xFF) as u8).collect();
        let oti = make_block_oti(block.len() as u64, SYMBOL_SIZE as u16);
        let enc = SourceBlockEncoder::new(7, &oti, &block);
        let src = enc.source_packets();
        let mut buf = [0u8; TS_PACKET_SIZE];
        build_data_packet(0x100, 5, &oti, &src[0], &mut buf);

        // Flip one bit in the symbol region; CRC must catch it.
        buf[TS_HEADER_SIZE + FRAME_SYMBOL_OFFSET] ^= 0x01;
        assert!(matches!(parse_packet(&buf, 0x100), Err(ParseError::BadCrc)));
    }

    #[test]
    fn crc_rejects_oti_corruption() {
        // A corrupt OTI is exactly what panicked the decoder; the CRC rejects it
        // at the frame boundary, before any SourceBlockDecoder is constructed.
        let block: Vec<u8> = (0..DEFAULT_BLOCK_SIZE).map(|i| (i & 0xFF) as u8).collect();
        let oti = make_block_oti(block.len() as u64, SYMBOL_SIZE as u16);
        let enc = SourceBlockEncoder::new(1, &oti, &block);
        let mut buf = [0u8; TS_PACKET_SIZE];
        build_data_packet(0x100, 0, &oti, &enc.source_packets()[0], &mut buf);

        buf[TS_HEADER_SIZE + FRAME_OTI_OFFSET] ^= 0xFF;
        assert!(matches!(parse_packet(&buf, 0x100), Err(ParseError::BadCrc)));
    }

    #[test]
    fn crc_passes_for_clean_noop() {
        let mut buf = [0u8; TS_PACKET_SIZE];
        build_noop_packet(0x100, 1, &mut buf);
        assert!(matches!(parse_packet(&buf, 0x100), Ok(ParsedFrame::Noop { .. })));
        // Corrupt the noop stuffing -> rejected.
        buf[TS_HEADER_SIZE + 1] ^= 0xFF;
        assert!(matches!(parse_packet(&buf, 0x100), Err(ParseError::BadCrc)));
    }

    #[test]
    fn header_roundtrip() {
        let h = TsHeader {
            tei: false,
            payload_unit_start: true,
            priority: false,
            pid: 0x1ABC,
            scrambling: 0,
            adaptation: 1,
            continuity_counter: 7,
        };
        let mut bytes = [0u8; TS_HEADER_SIZE];
        h.write(&mut bytes);
        let parsed = TsHeader::parse(&bytes).unwrap();
        assert_eq!(parsed.pid, 0x1ABC);
        assert!(parsed.payload_unit_start);
        assert_eq!(parsed.continuity_counter, 7);
    }

    #[test]
    fn noop_roundtrip() {
        let mut buf = [0u8; TS_PACKET_SIZE];
        build_noop_packet(0x100, 3, &mut buf);
        match parse_packet(&buf, 0x100).unwrap() {
            ParsedFrame::Noop { cc } => assert_eq!(cc, 3),
            other => panic!("expected noop, got {:?}", other),
        }
    }

    #[test]
    fn data_roundtrip() {
        let block: Vec<u8> = (0..DEFAULT_BLOCK_SIZE).map(|i| (i & 0xFF) as u8).collect();
        let oti = make_block_oti(block.len() as u64, SYMBOL_SIZE as u16);
        let enc = SourceBlockEncoder::new(7, &oti, &block);
        let src = enc.source_packets();
        let mut buf = [0u8; TS_PACKET_SIZE];
        build_data_packet(0x100, 5, &oti, &src[0], &mut buf);
        match parse_packet(&buf, 0x100).unwrap() {
            ParsedFrame::Data {
                cc,
                oti: got_oti,
                payload_id,
                symbol,
            } => {
                assert_eq!(cc, 5);
                assert_eq!(got_oti.transfer_length(), oti.transfer_length());
                assert_eq!(payload_id.source_block_number(), 7);
                assert_eq!(payload_id.encoding_symbol_id(), 0);
                assert_eq!(symbol, src[0].data());
            }
            other => panic!("expected data, got {:?}", other),
        }
    }

    #[test]
    fn wrong_pid_rejected() {
        let mut buf = [0u8; TS_PACKET_SIZE];
        build_noop_packet(0x100, 0, &mut buf);
        match parse_packet(&buf, 0x101) {
            Err(ParseError::WrongPid {
                got: 0x100,
                want: 0x101,
            }) => {}
            other => panic!("expected WrongPid, got {:?}", other),
        }
    }

    #[test]
    fn tei_flag_rejected() {
        let mut buf = [0u8; TS_PACKET_SIZE];
        build_noop_packet(0x100, 0, &mut buf);
        buf[1] |= 0x80;
        match parse_packet(&buf, 0x100) {
            Err(ParseError::TransportError) => {}
            other => panic!("expected TransportError, got {:?}", other),
        }
    }
}
