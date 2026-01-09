//! Connection-related TNS messages.

use crate::error::Result;
use crate::protocol::constants::*;
use crate::protocol::message::{DataMessage, Message, WriteExt};

// ============================================================================
// ConnectMessage - TNS CONNECT packet payload
// ============================================================================

/// TNS CONNECT packet message.
///
/// This is sent as the first packet to establish a connection.
/// It's a raw packet (not a DATA packet), so it doesn't have data_flags.
pub struct ConnectMessage<'a> {
    /// Connect descriptor string (e.g., "(DESCRIPTION=...)")
    pub connect_string: &'a [u8],
    /// SDU size to request
    pub sdu: u32,
}

impl ConnectMessage<'_> {
    /// Maximum connect data that can fit in the CONNECT packet itself.
    /// If longer, data must be sent in a separate DATA packet.
    pub fn connect_data_in_packet(&self) -> bool {
        self.connect_string.len() <= TNS_MAX_CONNECT_DATA as usize
    }
}

impl Message for ConnectMessage<'_> {
    fn wire_size(&self) -> usize {
        let mut size = 0;

        // Fixed header fields
        size += 2; // version_desired
        size += 2; // version_minimum
        size += 2; // service_options
        size += 2; // sdu (16-bit)
        size += 2; // tdu (16-bit)
        size += 2; // protocol_characteristics
        size += 2; // line_turnaround
        size += 2; // value_of_1
        size += 2; // connect_data_length
        size += 2; // connect_data_offset
        size += 4; // max_receivable_data
        size += 1; // nsi_flags_1
        size += 1; // nsi_flags_2
        size += 24; // padding (3 x u64)
        size += 4; // large_sdu
        size += 4; // large_tdu
        size += 4; // connect_flags_1
        size += 4; // connect_flags_2

        // Connect data (if fits in packet)
        if self.connect_data_in_packet() {
            size += self.connect_string.len();
        }

        size
    }

    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        let connect_data_len = self.connect_string.len() as u16;

        // TNS connect header
        buf.write_u16_be(TNS_VERSION_DESIRED);
        buf.write_u16_be(TNS_VERSION_MINIMUM);

        // Service options - don't include OOB for async
        buf.write_u16_be(TNS_GSO_DONT_CARE);

        buf.write_u16_be(self.sdu as u16); // SDU
        buf.write_u16_be(self.sdu as u16); // TDU
        buf.write_u16_be(TNS_PROTOCOL_CHARACTERISTICS);
        buf.write_u16_be(0); // Line turnaround
        buf.write_u16_be(1); // Value of 1

        buf.write_u16_be(connect_data_len);
        buf.write_u16_be(74); // Offset to connect data (fixed)
        buf.write_u32_be(0); // Max receivable data

        let nsi_flags = TNS_NSI_SUPPORT_SECURITY_RENEG | TNS_NSI_DISABLE_NA;
        buf.write_u8(nsi_flags);
        buf.write_u8(nsi_flags);

        // Padding (obsolete fields)
        buf.write_zeros(24);

        // Large SDU/TDU
        buf.write_u32_be(self.sdu);
        buf.write_u32_be(self.sdu);

        // Connect flags (no OOB for async)
        buf.write_u32_be(0); // connect_flags_1
        buf.write_u32_be(0); // connect_flags_2

        // Connect data (if fits)
        if self.connect_data_in_packet() {
            buf.write_bytes(self.connect_string);
        }

        Ok(())
    }
}

// ============================================================================
// ProtocolMessage - Protocol negotiation (TNS_MSG_TYPE_PROTOCOL)
// ============================================================================

/// Protocol negotiation message.
///
/// Sent after CONNECT/ACCEPT to negotiate protocol capabilities.
pub struct ProtocolMessage<'a> {
    /// Driver name to send to server
    pub driver_name: &'a [u8],
}

impl Default for ProtocolMessage<'_> {
    fn default() -> Self {
        Self {
            driver_name: b"oracle-thin-rs",
        }
    }
}

impl Message for ProtocolMessage<'_> {
    fn wire_size(&self) -> usize {
        let mut size = 0;
        size += 1; // message type
        size += 1; // protocol version
        size += 1; // array terminator
        size += self.driver_name.len();
        size += 1; // null terminator
        size
    }

    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.write_u8(TNS_MSG_TYPE_PROTOCOL);
        buf.write_u8(6); // protocol version
        buf.write_u8(0); // array terminator
        buf.write_bytes(self.driver_name);
        buf.write_u8(0); // null terminator
        Ok(())
    }
}

impl DataMessage for ProtocolMessage<'_> {}

// ============================================================================
// MarkerMessage - Reset/Break marker
// ============================================================================

/// Marker message (RESET or BREAK).
pub struct MarkerMessage {
    /// Marker type (TNS_MARKER_TYPE_RESET or TNS_MARKER_TYPE_BREAK)
    pub marker_type: u8,
}

impl MarkerMessage {
    pub fn reset() -> Self {
        Self {
            marker_type: TNS_MARKER_TYPE_RESET,
        }
    }
}

impl Message for MarkerMessage {
    fn wire_size(&self) -> usize {
        3 // constant + constant + marker_type
    }

    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.write_u8(1); // constant
        buf.write_u8(0); // constant
        buf.write_u8(self.marker_type);
        Ok(())
    }
}

/// Marker type constants
pub const TNS_MARKER_TYPE_BREAK: u8 = 1;
pub const TNS_MARKER_TYPE_RESET: u8 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_message_wire_size() {
        let connect_str = b"(DESCRIPTION=(ADDRESS=(HOST=localhost)(PORT=1521)))";
        let msg = ConnectMessage {
            connect_string: connect_str,
            sdu: 8192,
        };

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
    }

    #[test]
    fn test_protocol_message_wire_size() {
        let msg = ProtocolMessage::default();

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
    }

    #[test]
    fn test_marker_message_wire_size() {
        let msg = MarkerMessage::reset();

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
        assert_eq!(buf, vec![1, 0, TNS_MARKER_TYPE_RESET]);
    }
}
