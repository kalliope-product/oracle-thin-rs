//! Buffer utilities for reading and writing TNS protocol data.

use crate::error::{Error, Result};
use crate::protocol::constants::*;
use bytes::{BufMut, Bytes, BytesMut};

/// A buffer for reading TNS protocol data.
pub struct ReadBuffer {
    data: Bytes,
    pos: usize,
}

impl ReadBuffer {
    /// Create a new read buffer from bytes.
    pub fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }

    /// Get the current position in the buffer.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Get the remaining bytes in the buffer.
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// Check if the buffer has at least `n` bytes remaining.
    pub fn has_remaining(&self, n: usize) -> bool {
        self.remaining() >= n
    }

    /// Get a slice of the remaining data.
    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.pos..]
    }

    /// Skip `n` bytes.
    pub fn skip(&mut self, n: usize) -> Result<()> {
        if !self.has_remaining(n) {
            return Err(Error::BufferTooSmall {
                needed: n,
                available: self.remaining(),
            });
        }
        self.pos += n;
        Ok(())
    }

    /// Read a single byte.
    pub fn read_u8(&mut self) -> Result<u8> {
        if !self.has_remaining(1) {
            return Err(Error::BufferTooSmall {
                needed: 1,
                available: self.remaining(),
            });
        }
        let val = self.data[self.pos];
        self.pos += 1;
        Ok(val)
    }

    /// Read a big-endian u16.
    pub fn read_u16_be(&mut self) -> Result<u16> {
        if !self.has_remaining(2) {
            return Err(Error::BufferTooSmall {
                needed: 2,
                available: self.remaining(),
            });
        }
        let val = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(val)
    }

    /// Read a little-endian u16.
    pub fn read_u16_le(&mut self) -> Result<u16> {
        if !self.has_remaining(2) {
            return Err(Error::BufferTooSmall {
                needed: 2,
                available: self.remaining(),
            });
        }
        let val = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(val)
    }

    /// Read a big-endian u32.
    pub fn read_u32_be(&mut self) -> Result<u32> {
        if !self.has_remaining(4) {
            return Err(Error::BufferTooSmall {
                needed: 4,
                available: self.remaining(),
            });
        }
        let val = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(val)
    }

    /// Read a big-endian u64.
    pub fn read_u64_be(&mut self) -> Result<u64> {
        if !self.has_remaining(8) {
            return Err(Error::BufferTooSmall {
                needed: 8,
                available: self.remaining(),
            });
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_be_bytes(bytes))
    }

    /// Read raw bytes.
    pub fn read_bytes(&mut self, n: usize) -> Result<Bytes> {
        if !self.has_remaining(n) {
            return Err(Error::BufferTooSmall {
                needed: n,
                available: self.remaining(),
            });
        }
        let bytes = self.data.slice(self.pos..self.pos + n);
        self.pos += n;
        Ok(bytes)
    }

    /// Read a variable-length unsigned integer (Oracle's UB format).
    /// The first byte indicates length; if high bit is set, value is negative.
    pub fn read_ub1(&mut self) -> Result<u8> {
        self.read_u8()
    }

    /// Read a variable-length u16 (Oracle's UB2 format).
    /// Uses big-endian byte order: first byte is most significant.
    pub fn read_ub2(&mut self) -> Result<u16> {
        let length = self.read_u8()?;
        if length == 0 {
            return Ok(0);
        }
        if length == 1 {
            return Ok(self.read_u8()? as u16);
        }
        if length == 2 {
            // Big-endian: first byte is most significant
            let b1 = self.read_u8()? as u16;
            let b2 = self.read_u8()? as u16;
            return Ok((b1 << 8) | b2);
        }
        Err(Error::protocol(format!("Invalid UB2 length: {}", length)))
    }

    /// Read a variable-length u32 (Oracle's UB4 format).
    /// Uses big-endian byte order: first byte is most significant.
    pub fn read_ub4(&mut self) -> Result<u32> {
        let length = self.read_u8()?;
        if length == 0 {
            return Ok(0);
        }
        // Cap length at 4 bytes for u32
        let actual_len = length.min(4) as usize;
        let mut val: u32 = 0;
        // Big-endian: first byte is most significant
        for _ in 0..actual_len {
            val = (val << 8) | (self.read_u8()? as u32);
        }
        // Skip any extra bytes if length > 4
        for _ in actual_len..(length as usize) {
            self.read_u8()?;
        }
        Ok(val)
    }

    /// Read a variable-length u64 (Oracle's UB8 format).
    /// Uses big-endian byte order: first byte is most significant.
    pub fn read_ub8(&mut self) -> Result<u64> {
        let length = self.read_u8()?;
        if length == 0 {
            return Ok(0);
        }
        let mut val: u64 = 0;
        // Big-endian: first byte is most significant
        for _ in 0..length {
            val = (val << 8) | (self.read_u8()? as u64);
        }
        Ok(val)
    }

    /// Skip a variable-length u32 (Oracle's UB4 format).
    /// Reads the length byte and skips that many bytes.
    pub fn skip_ub4(&mut self) -> Result<()> {
        let length = self.read_u8()?;
        if length > 0 {
            self.skip(length as usize)?;
        }
        Ok(())
    }

    /// Read bytes with a length prefix.
    pub fn read_bytes_with_length(&mut self) -> Result<Option<Bytes>> {
        let length = self.read_u8()?;
        if length == TNS_NULL_LENGTH_INDICATOR {
            return Ok(None);
        }
        if length == TNS_LONG_LENGTH_INDICATOR {
            // Chunked read for long values
            let mut result = BytesMut::new();
            loop {
                let chunk_len = self.read_ub4()?;
                if chunk_len == 0 {
                    break;
                }
                let chunk = self.read_bytes(chunk_len as usize)?;
                result.extend_from_slice(&chunk);
            }
            return Ok(Some(result.freeze()));
        }
        let data = self.read_bytes(length as usize)?;
        Ok(Some(data))
    }

    /// Read a string with a length prefix.
    /// Uses lossy UTF-8 conversion to handle binary data gracefully.
    pub fn read_str_with_length(&mut self) -> Result<Option<String>> {
        match self.read_bytes_with_length()? {
            Some(bytes) => {
                // Use lossy conversion to handle binary data
                let s = String::from_utf8_lossy(&bytes).to_string();
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    /// Skip n bytes in the buffer.
    pub fn skip_bytes(&mut self, n: usize) -> Result<()> {
        if !self.has_remaining(n) {
            return Err(Error::BufferTooSmall {
                needed: n,
                available: self.remaining(),
            });
        }
        self.pos += n;
        Ok(())
    }

    /// Skip raw bytes that may or may not be chunked.
    /// The first byte gives the length. If length is TNS_LONG_LENGTH_INDICATOR (0xFE),
    /// chunks are read and discarded.
    pub fn skip_raw_bytes_chunked(&mut self) -> Result<()> {
        let length = self.read_u8()?;
        if length != 0xFE {
            // TNS_LONG_LENGTH_INDICATOR
            self.skip_bytes(length as usize)?;
        } else {
            // Chunked format
            loop {
                let chunk_len = self.read_ub4()?;
                if chunk_len == 0 {
                    break;
                }
                self.skip_bytes(chunk_len as usize)?;
            }
        }
        Ok(())
    }
}

/// A buffer for writing TNS protocol data.
pub struct WriteBuffer {
    data: BytesMut,
}

impl WriteBuffer {
    /// Create a new write buffer with default capacity.
    pub fn new() -> Self {
        Self::with_capacity(8192)
    }

    /// Create a new write buffer with specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: BytesMut::with_capacity(capacity),
        }
    }

    /// Get the current length of the buffer.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the buffer contents as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Freeze the buffer into immutable bytes.
    pub fn freeze(self) -> Bytes {
        self.data.freeze()
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Write a single byte.
    pub fn write_u8(&mut self, val: u8) {
        self.data.put_u8(val);
    }

    /// Write a big-endian u16.
    pub fn write_u16_be(&mut self, val: u16) {
        self.data.put_u16(val);
    }

    /// Write a little-endian u16.
    pub fn write_u16_le(&mut self, val: u16) {
        self.data.put_u16_le(val);
    }

    /// Write a big-endian u32.
    pub fn write_u32_be(&mut self, val: u32) {
        self.data.put_u32(val);
    }

    /// Write a big-endian u64.
    pub fn write_u64_be(&mut self, val: u64) {
        self.data.put_u64(val);
    }

    /// Write raw bytes.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
    }

    /// Write a variable-length unsigned integer (Oracle's UB format).
    pub fn write_ub1(&mut self, val: u8) {
        self.write_u8(val);
    }

    /// Write a variable-length u16 (Oracle's UB2 format).
    pub fn write_ub2(&mut self, val: u16) {
        if val == 0 {
            self.write_u8(0);
        } else if val <= 0xff {
            self.write_u8(1);
            self.write_u8(val as u8);
        } else {
            self.write_u8(2);
            self.write_u8(val as u8);
            self.write_u8((val >> 8) as u8);
        }
    }

    /// Write a variable-length u32 (Oracle's UB4 format).
    pub fn write_ub4(&mut self, val: u32) {
        if val == 0 {
            self.write_u8(0);
        } else {
            let bytes = val.to_le_bytes();
            let len = 4 - (val.leading_zeros() / 8) as usize;
            self.write_u8(len as u8);
            self.data.extend_from_slice(&bytes[..len]);
        }
    }

    /// Write a variable-length u64 (Oracle's UB8 format).
    pub fn write_ub8(&mut self, val: u64) {
        if val == 0 {
            self.write_u8(0);
        } else {
            let bytes = val.to_le_bytes();
            let len = 8 - (val.leading_zeros() / 8) as usize;
            self.write_u8(len as u8);
            self.data.extend_from_slice(&bytes[..len]);
        }
    }

    /// Write bytes with a length prefix.
    pub fn write_bytes_with_length(&mut self, bytes: &[u8]) {
        let len = bytes.len();
        if len == 0 {
            self.write_u8(0);
        } else if len < TNS_LONG_LENGTH_INDICATOR as usize {
            self.write_u8(len as u8);
            self.write_bytes(bytes);
        } else {
            // Chunked write for long values
            self.write_u8(TNS_LONG_LENGTH_INDICATOR);
            let mut offset = 0;
            while offset < len {
                let chunk_len = std::cmp::min(len - offset, 65536);
                self.write_ub4(chunk_len as u32);
                self.write_bytes(&bytes[offset..offset + chunk_len]);
                offset += chunk_len;
            }
            self.write_ub4(0); // End of chunks
        }
    }

    /// Write a string with a length prefix.
    pub fn write_str_with_length(&mut self, s: &str) {
        self.write_bytes_with_length(s.as_bytes());
    }

    /// Write padding zeros.
    pub fn write_zeros(&mut self, count: usize) {
        for _ in 0..count {
            self.write_u8(0);
        }
    }

    /// Set a u16 value at a specific position (big-endian).
    pub fn set_u16_be(&mut self, pos: usize, val: u16) {
        let bytes = val.to_be_bytes();
        self.data[pos] = bytes[0];
        self.data[pos + 1] = bytes[1];
    }

    /// Set a u32 value at a specific position (big-endian).
    pub fn set_u32_be(&mut self, pos: usize, val: u32) {
        let bytes = val.to_be_bytes();
        self.data[pos] = bytes[0];
        self.data[pos + 1] = bytes[1];
        self.data[pos + 2] = bytes[2];
        self.data[pos + 3] = bytes[3];
    }
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new()
    }
}
