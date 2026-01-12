//! Message traits and builders for zero-copy TNS protocol serialization.
//!
//! This module provides infrastructure for building TNS messages with minimal allocations.
//! Messages implement the `Message` trait which allows calculating wire size before allocation,
//! enabling single-allocation serialization.

use crate::error::Result;
use crate::protocol::constants::*;

// ============================================================================
// Core Traits
// ============================================================================

/// A message that can calculate its wire size and serialize to bytes.
///
/// Implementing this trait allows messages to be serialized with a single allocation:
/// 1. Call `wire_size()` to determine buffer capacity needed
/// 2. Allocate buffer with exact capacity
/// 3. Call `write_to()` to serialize directly into buffer
pub trait Message {
    /// Calculate the serialized size in bytes (excluding packet header).
    fn wire_size(&self) -> usize;

    /// Write message content to buffer.
    ///
    /// The caller guarantees the buffer has sufficient capacity (from `wire_size()`).
    /// Implementations should use `buf.push()` and `buf.extend_from_slice()`.
    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()>;
}

/// A DATA packet message that includes data_flags prefix.
///
/// DATA packets have a 2-byte flags field before the message content.
pub trait DataMessage: Message {
    /// Get the data flags for this message.
    fn data_flags(&self) -> u16 {
        0
    }

    /// Total wire size including the 2-byte data_flags prefix.
    fn data_wire_size(&self) -> usize {
        2 + self.wire_size()
    }
}

// ============================================================================
// Size Calculation Helpers
// ============================================================================

/// Calculate wire size for a UB1 value (always 1 byte).
#[inline]
pub const fn ub1_wire_size(_val: u8) -> usize {
    1
}

/// Calculate wire size for a UB2 value in TNS variable-length format.
///
/// Format: length byte + value bytes (little-endian)
/// - 0: 1 byte (0x00)
/// - 1-255: 2 bytes (0x01, val)
/// - 256-65535: 3 bytes (0x02, lo, hi)
#[inline]
pub const fn ub2_wire_size(val: u16) -> usize {
    match val {
        0 => 1,
        1..=0xFF => 2,
        _ => 3,
    }
}

/// Calculate wire size for a UB4 value in TNS variable-length format.
///
/// Format: length byte + value bytes (little-endian)
/// - 0: 1 byte
/// - 1-255: 2 bytes
/// - 256-65535: 3 bytes
/// - 65536-16777215: 4 bytes
/// - 16777216+: 5 bytes
#[inline]
pub const fn ub4_wire_size(val: u32) -> usize {
    match val {
        0 => 1,
        1..=0xFF => 2,
        0x100..=0xFFFF => 3,
        0x10000..=0xFFFFFF => 4,
        _ => 5,
    }
}

/// Calculate wire size for a UB8 value in TNS variable-length format.
#[inline]
pub const fn ub8_wire_size(val: u64) -> usize {
    match val {
        0 => 1,
        1..=0xFF => 2,
        0x100..=0xFFFF => 3,
        0x10000..=0xFFFFFF => 4,
        0x1000000..=0xFFFFFFFF => 5,
        0x100000000..=0xFFFFFFFFFF => 6,
        0x10000000000..=0xFFFFFFFFFFFF => 7,
        0x1000000000000..=0xFFFFFFFFFFFFFF => 8,
        _ => 9,
    }
}

/// Calculate wire size for bytes with length prefix.
///
/// Format depends on length:
/// - 0: 1 byte (0x00)
/// - 1-252: 1 + len bytes
/// - 253: 1 byte (0x00) - treated as empty
/// - 254+: Long format with chunking (0xFE + chunks)
#[inline]
pub const fn bytes_with_length_wire_size(len: usize) -> usize {
    if len == 0 {
        1
    } else if len < TNS_LONG_LENGTH_INDICATOR as usize {
        1 + len
    } else {
        // Long format: 0xFE marker + chunked data
        // Each chunk: ub4(chunk_len) + data
        // Final: ub4(0) terminator
        let num_chunks = len.div_ceil(65536);
        1 + (num_chunks * 5) + len + 1 // FE + chunk headers + data + terminator
    }
}

/// Calculate wire size for a string with length prefix.
#[inline]
pub const fn str_with_length_wire_size(s: &str) -> usize {
    bytes_with_length_wire_size(s.len())
}

/// Calculate wire size for a key-value pair in auth messages.
///
/// Format: ub4(key_len) + bytes_with_length(key) + ub4(value_len) + bytes_with_length(value) + ub4(flags)
pub fn key_value_wire_size(key: &str, value: &str, flags: u32) -> usize {
    let key_len = key.len();
    let value_len = value.len();

    ub4_wire_size(key_len as u32)
        + bytes_with_length_wire_size(key_len)
        + ub4_wire_size(value_len as u32)
        + if value_len == 0 {
            0
        } else {
            bytes_with_length_wire_size(value_len)
        }
        + ub4_wire_size(flags)
}

// ============================================================================
// Write Helpers
// ============================================================================

/// Extension trait for writing TNS protocol data to Vec<u8>.
pub trait WriteExt {
    /// Write a single byte.
    fn write_u8(&mut self, val: u8);

    /// Write a big-endian u16.
    fn write_u16_be(&mut self, val: u16);

    /// Write a little-endian u16.
    fn write_u16_le(&mut self, val: u16);

    /// Write a big-endian u32.
    fn write_u32_be(&mut self, val: u32);

    /// Write a big-endian u64.
    fn write_u64_be(&mut self, val: u64);

    /// Write raw bytes.
    fn write_bytes(&mut self, bytes: &[u8]);

    /// Write zeros.
    fn write_zeros(&mut self, count: usize);

    /// Write a UB1 value.
    fn write_ub1(&mut self, val: u8);

    /// Write a UB2 value in TNS variable-length format.
    fn write_ub2(&mut self, val: u16);

    /// Write a UB4 value in TNS variable-length format.
    fn write_ub4(&mut self, val: u32);

    /// Write a UB8 value in TNS variable-length format.
    fn write_ub8(&mut self, val: u64);

    /// Write bytes with length prefix.
    fn write_bytes_with_length(&mut self, bytes: &[u8]);

    /// Write string with length prefix.
    fn write_str_with_length(&mut self, s: &str);

    /// Write a key-value pair for auth messages.
    fn write_key_value(&mut self, key: &str, value: &str, flags: u32);
}

impl WriteExt for Vec<u8> {
    #[inline]
    fn write_u8(&mut self, val: u8) {
        self.push(val);
    }

    #[inline]
    fn write_u16_be(&mut self, val: u16) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    #[inline]
    fn write_u16_le(&mut self, val: u16) {
        self.extend_from_slice(&val.to_le_bytes());
    }

    #[inline]
    fn write_u32_be(&mut self, val: u32) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    #[inline]
    fn write_u64_be(&mut self, val: u64) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    #[inline]
    fn write_bytes(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    #[inline]
    fn write_zeros(&mut self, count: usize) {
        self.resize(self.len() + count, 0);
    }

    #[inline]
    fn write_ub1(&mut self, val: u8) {
        self.push(val);
    }

    fn write_ub2(&mut self, val: u16) {
        if val == 0 {
            self.push(0);
        } else if val <= 0xFF {
            self.push(1);
            self.push(val as u8);
        } else {
            // Big-endian: high byte first
            self.push(2);
            self.push((val >> 8) as u8);
            self.push(val as u8);
        }
    }

    fn write_ub4(&mut self, val: u32) {
        if val == 0 {
            self.push(0);
        } else {
            // UB4 uses big-endian byte order with length prefix
            let bytes = val.to_be_bytes();
            let leading = (val.leading_zeros() / 8) as usize;
            let len = 4 - leading;
            self.push(len as u8);
            self.extend_from_slice(&bytes[leading..]);
        }
    }

    fn write_ub8(&mut self, val: u64) {
        if val == 0 {
            self.push(0);
        } else {
            // UB8 uses big-endian byte order with length prefix
            let bytes = val.to_be_bytes();
            let leading = (val.leading_zeros() / 8) as usize;
            let len = 8 - leading;
            self.push(len as u8);
            self.extend_from_slice(&bytes[leading..]);
        }
    }

    fn write_bytes_with_length(&mut self, bytes: &[u8]) {
        let len = bytes.len();
        if len == 0 {
            self.push(0);
        } else if len < TNS_LONG_LENGTH_INDICATOR as usize {
            self.push(len as u8);
            self.extend_from_slice(bytes);
        } else {
            // Long format: chunked
            self.push(TNS_LONG_LENGTH_INDICATOR);
            let mut offset = 0;
            while offset < len {
                let chunk_len = std::cmp::min(len - offset, 65536);
                self.write_ub4(chunk_len as u32);
                self.extend_from_slice(&bytes[offset..offset + chunk_len]);
                offset += chunk_len;
            }
            self.write_ub4(0); // End marker
        }
    }

    #[inline]
    fn write_str_with_length(&mut self, s: &str) {
        self.write_bytes_with_length(s.as_bytes());
    }

    fn write_key_value(&mut self, key: &str, value: &str, flags: u32) {
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();

        self.write_ub4(key_bytes.len() as u32);
        self.write_bytes_with_length(key_bytes);
        self.write_ub4(value_bytes.len() as u32);
        if !value_bytes.is_empty() {
            self.write_bytes_with_length(value_bytes);
        }
        self.write_ub4(flags);
    }
}

// ============================================================================
// Packet Header Writing
// ============================================================================

/// TNS packet header size (standard).
pub const PACKET_HEADER_SIZE: usize = 8;

/// Write a TNS packet header.
///
/// # Arguments
/// * `buf` - Buffer to write to (must have capacity for header + payload)
/// * `packet_type` - TNS packet type (CONNECT, DATA, etc.)
/// * `packet_flags` - Packet flags (usually 0)
/// * `total_size` - Total packet size including header
/// * `use_large_sdu` - Whether to use 4-byte length (true) or 2-byte (false)
pub fn write_packet_header(
    buf: &mut Vec<u8>,
    packet_type: u8,
    packet_flags: u8,
    total_size: usize,
    use_large_sdu: bool,
) {
    if use_large_sdu {
        buf.write_u32_be(total_size as u32);
    } else {
        buf.write_u16_be(total_size as u16);
        buf.write_u16_be(0); // Checksum (unused)
    }
    buf.write_u8(packet_type);
    buf.write_u8(packet_flags);
    buf.write_u16_be(0); // Header checksum (unused)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ub2_wire_size() {
        assert_eq!(ub2_wire_size(0), 1);
        assert_eq!(ub2_wire_size(1), 2);
        assert_eq!(ub2_wire_size(255), 2);
        assert_eq!(ub2_wire_size(256), 3);
        assert_eq!(ub2_wire_size(65535), 3);
    }

    #[test]
    fn test_ub4_wire_size() {
        assert_eq!(ub4_wire_size(0), 1);
        assert_eq!(ub4_wire_size(1), 2);
        assert_eq!(ub4_wire_size(255), 2);
        assert_eq!(ub4_wire_size(256), 3);
        assert_eq!(ub4_wire_size(65535), 3);
        assert_eq!(ub4_wire_size(65536), 4);
        assert_eq!(ub4_wire_size(16777215), 4);
        assert_eq!(ub4_wire_size(16777216), 5);
        assert_eq!(ub4_wire_size(u32::MAX), 5);
    }

    #[test]
    fn test_bytes_with_length_wire_size() {
        assert_eq!(bytes_with_length_wire_size(0), 1);
        assert_eq!(bytes_with_length_wire_size(1), 2);
        assert_eq!(bytes_with_length_wire_size(252), 253);
        assert_eq!(bytes_with_length_wire_size(253), 254);
        // Long format kicks in at 254+
        assert!(bytes_with_length_wire_size(254) > 255);
    }

    #[test]
    fn test_write_ub2() {
        let mut buf = Vec::new();

        buf.write_ub2(0);
        assert_eq!(buf, vec![0]);

        buf.clear();
        buf.write_ub2(1);
        assert_eq!(buf, vec![1, 1]);

        buf.clear();
        buf.write_ub2(255);
        assert_eq!(buf, vec![1, 255]);

        buf.clear();
        buf.write_ub2(256);
        assert_eq!(buf, vec![2, 1, 0]); // Big-endian: 0x0100

        buf.clear();
        buf.write_ub2(0x1234);
        assert_eq!(buf, vec![2, 0x12, 0x34]); // Big-endian
    }

    #[test]
    fn test_write_ub4() {
        let mut buf = Vec::new();

        buf.write_ub4(0);
        assert_eq!(buf, vec![0]);

        buf.clear();
        buf.write_ub4(1);
        assert_eq!(buf, vec![1, 1]);

        buf.clear();
        buf.write_ub4(0x12345678);
        assert_eq!(buf, vec![4, 0x12, 0x34, 0x56, 0x78]); // Big-endian

        // Test options value 0x8061 (PARSE|EXECUTE|FETCH|NOT_PLSQL)
        buf.clear();
        buf.write_ub4(0x8061);
        assert_eq!(buf, vec![2, 0x80, 0x61]); // Big-endian: 2 bytes
    }

    #[test]
    fn test_write_bytes_with_length() {
        let mut buf = Vec::new();

        buf.write_bytes_with_length(&[]);
        assert_eq!(buf, vec![0]);

        buf.clear();
        buf.write_bytes_with_length(&[1, 2, 3]);
        assert_eq!(buf, vec![3, 1, 2, 3]);

        buf.clear();
        buf.write_bytes_with_length(&[0xAB; 100]);
        assert_eq!(buf.len(), 101); // 1 byte length + 100 bytes data
        assert_eq!(buf[0], 100);
    }

    #[test]
    fn test_wire_size_matches_written() {
        // Test that wire_size calculations match actual written bytes
        let mut buf = Vec::new();

        // Test UB4 values
        for val in [
            0u32,
            1,
            255,
            256,
            65535,
            65536,
            0xFFFFFF,
            0x1000000,
            u32::MAX,
        ] {
            buf.clear();
            buf.write_ub4(val);
            assert_eq!(
                buf.len(),
                ub4_wire_size(val),
                "UB4 wire size mismatch for {}",
                val
            );
        }

        // Test bytes_with_length
        for len in [0, 1, 100, 252, 253] {
            buf.clear();
            let data = vec![0u8; len];
            buf.write_bytes_with_length(&data);
            assert_eq!(
                buf.len(),
                bytes_with_length_wire_size(len),
                "bytes_with_length wire size mismatch for len={}",
                len
            );
        }
    }

    #[test]
    fn test_write_key_value() {
        let mut buf = Vec::new();
        buf.write_key_value("KEY", "VALUE", 0);

        // Verify structure: ub4(3) + "KEY" with len + ub4(5) + "VALUE" with len + ub4(0)
        let expected_size = key_value_wire_size("KEY", "VALUE", 0);
        assert_eq!(buf.len(), expected_size);
    }

    #[test]
    fn test_packet_header() {
        let mut buf = Vec::new();

        // Standard header (2-byte length)
        write_packet_header(&mut buf, TNS_PACKET_TYPE_DATA, 0, 100, false);
        assert_eq!(buf.len(), 8);
        assert_eq!(buf[0..2], [0, 100]); // Length BE
        assert_eq!(buf[4], TNS_PACKET_TYPE_DATA);

        buf.clear();

        // Large SDU header (4-byte length)
        write_packet_header(&mut buf, TNS_PACKET_TYPE_DATA, 0, 100, true);
        assert_eq!(buf.len(), 8);
        assert_eq!(buf[0..4], [0, 0, 0, 100]); // Length BE 4-byte
        assert_eq!(buf[4], TNS_PACKET_TYPE_DATA);
    }
}
