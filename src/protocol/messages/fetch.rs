//! Fetch message for retrieving more rows from a cursor.

use crate::error::Result;
use crate::protocol::constants::*;
use crate::protocol::message::{ub4_wire_size, DataMessage, Message, WriteExt};

/// Fetch message for retrieving additional rows from an open cursor.
///
/// This is used after the initial execute when more rows need to be fetched.
/// The message is very simple: just cursor_id and the number of rows to fetch.
pub struct FetchMessage {
    /// Cursor ID (assigned by server during execute).
    pub cursor_id: u32,
    /// Number of rows to fetch.
    pub fetch_size: u32,
}

impl FetchMessage {
    /// Create a new fetch message.
    pub fn new(cursor_id: u32, fetch_size: u32) -> Self {
        Self {
            cursor_id,
            fetch_size,
        }
    }
}

impl Message for FetchMessage {
    fn wire_size(&self) -> usize {
        let mut size = 0;

        // Function header
        size += 1; // message type (TNS_MSG_TYPE_FUNCTION)
        size += 1; // function code (TNS_FUNC_FETCH)
        size += 1; // sequence number

        // Cursor ID and fetch size
        size += ub4_wire_size(self.cursor_id);
        size += ub4_wire_size(self.fetch_size);

        size
    }

    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        // Function header
        buf.write_u8(TNS_MSG_TYPE_FUNCTION);
        buf.write_u8(TNS_FUNC_FETCH);
        buf.write_u8(1); // sequence number

        // Cursor ID and fetch size
        buf.write_ub4(self.cursor_id);
        buf.write_ub4(self.fetch_size);

        Ok(())
    }
}

impl DataMessage for FetchMessage {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_message_wire_size() {
        let msg = FetchMessage::new(42, 100);

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
    }

    #[test]
    fn test_fetch_message_content() {
        let msg = FetchMessage::new(42, 100);

        let mut buf = Vec::new();
        msg.write_to(&mut buf).unwrap();

        // Check header
        assert_eq!(buf[0], TNS_MSG_TYPE_FUNCTION);
        assert_eq!(buf[1], TNS_FUNC_FETCH);
        assert_eq!(buf[2], 1); // sequence
    }
}
