//! Wire format and TS-packet helpers shared by raptorq-enc and raptorq-dec.

use raptorq::{EncodingPacket, ObjectTransmissionInformation, PayloadId};
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
pub const SYMBOL_SIZE: usize = TS_PAYLOAD_SIZE - FRAME_OVERHEAD;

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
    #[error("unknown frame type {0:#x}")]
    UnknownFrameType(u8),
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
    for b in &mut out[TS_HEADER_SIZE + 1..] {
        *b = 0xFF;
    }
}

#[derive(Debug)]
pub enum ParsedFrame {
    Data {
        oti: ObjectTransmissionInformation,
        payload_id: PayloadId,
        symbol: Vec<u8>,
    },
    Noop,
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
    let frame_type = payload[FRAME_TYPE_OFFSET];
    match frame_type {
        FRAME_TYPE_NOOP => Ok(ParsedFrame::Noop),
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

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq::SourceBlockEncoder;

    #[test]
    fn frame_sizes_fit_ts_packet() {
        assert_eq!(FRAME_OVERHEAD + SYMBOL_SIZE, TS_PAYLOAD_SIZE);
        assert_eq!(TS_HEADER_SIZE + TS_PAYLOAD_SIZE, TS_PACKET_SIZE);
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
            ParsedFrame::Noop => {}
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
                oti: got_oti,
                payload_id,
                symbol,
            } => {
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
