//! LOB (Large Object) types for CLOB, BLOB, and BFILE columns.
//!
//! Oracle LOBs can be returned in two ways:
//! 1. **Prefetched**: The LOB data is returned inline with the row data
//!    (when LOB size <= prefetch_length)
//! 2. **Locator**: A LOB locator handle is returned, and the data must be
//!    fetched separately via LOB_OP messages
//!
//! This module provides types to represent both cases.

/// LOB locator handle returned by the server.
///
/// The locator is an opaque byte sequence that identifies a specific LOB
/// in the database. It can be used to read/write the LOB data via LOB_OP
/// messages.
#[derive(Debug, Clone, PartialEq)]
pub struct LobLocator {
    /// Raw locator bytes from the server.
    pub locator: Vec<u8>,
    /// Total size of the LOB in characters (CLOB) or bytes (BLOB).
    pub size: u64,
    /// Recommended chunk size for read/write operations.
    pub chunk_size: u32,
    /// Whether metadata (size, chunk_size) is available.
    /// BFILE types don't have metadata from prefetch.
    pub has_metadata: bool,
}

impl LobLocator {
    /// Create a new LOB locator.
    pub fn new(locator: Vec<u8>, size: u64, chunk_size: u32, has_metadata: bool) -> Self {
        Self {
            locator,
            size,
            chunk_size,
            has_metadata,
        }
    }

    /// Create an empty LOB locator for temporary LOBs.
    pub fn empty() -> Self {
        Self {
            locator: vec![0; 40],
            size: 0,
            chunk_size: 0,
            has_metadata: false,
        }
    }

    /// Check if this LOB is a BLOB based on locator flags.
    pub fn is_blob(&self) -> bool {
        if self.locator.len() > TNS_LOB_LOC_OFFSET_FLAG_1 {
            self.locator[TNS_LOB_LOC_OFFSET_FLAG_1] & TNS_LOB_LOC_FLAGS_BLOB != 0
        } else {
            false
        }
    }

    /// Check if this is a temporary LOB.
    pub fn is_temp(&self) -> bool {
        if self.locator.len() > TNS_LOB_LOC_OFFSET_FLAG_4 {
            self.locator[TNS_LOB_LOC_OFFSET_FLAG_4] & TNS_LOB_LOC_FLAGS_TEMP != 0
        } else {
            false
        }
    }

    /// Check if this is an abstract LOB.
    pub fn is_abstract(&self) -> bool {
        if self.locator.len() > TNS_LOB_LOC_OFFSET_FLAG_1 {
            self.locator[TNS_LOB_LOC_OFFSET_FLAG_1] & TNS_LOB_LOC_FLAGS_ABSTRACT != 0
        } else {
            false
        }
    }
}

/// LOB value that may contain prefetched data or just a locator.
///
/// When the LOB size is <= prefetch_length, the data is returned inline.
/// Otherwise, only the locator is returned and data must be fetched separately.
#[derive(Debug, Clone, PartialEq)]
pub struct LobValue {
    /// The LOB locator (always present).
    pub locator: LobLocator,
    /// Prefetched data (present if LOB was small enough to prefetch).
    /// For CLOB: UTF-8 encoded string data
    /// For BLOB: raw binary data
    pub data: Option<Vec<u8>>,
}

impl LobValue {
    /// Create a LOB value with prefetched data.
    pub fn with_data(locator: LobLocator, data: Vec<u8>) -> Self {
        Self {
            locator,
            data: Some(data),
        }
    }

    /// Create a LOB value with only a locator (data must be fetched separately).
    pub fn locator_only(locator: LobLocator) -> Self {
        Self {
            locator,
            data: None,
        }
    }

    /// Check if the LOB data was prefetched.
    pub fn has_data(&self) -> bool {
        self.data.is_some()
    }

    /// Get the prefetched data as a string (for CLOB).
    /// Returns None if no data was prefetched.
    pub fn as_string(&self) -> Option<String> {
        // Convert to UTF-16 string
        self.data.as_ref().map(|d| {
            String::from_utf16_lossy(
                &d.chunks(2)
                    .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                    .collect::<Vec<u16>>(),
            )
        })
    }

    /// Get the prefetched data as bytes (for BLOB).
    /// Returns None if no data was prefetched.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    /// Get the LOB size in characters (CLOB) or bytes (BLOB).
    pub fn size(&self) -> u64 {
        self.locator.size
    }

    /// Check if the LOB is NULL (no locator data).
    pub fn is_null(&self) -> bool {
        self.locator.locator.is_empty()
    }
}

// LOB locator offset constants (from constants.pxi)
// These constants are used for interpreting the LOB locator byte structure.
// Some are used now, others will be used for LOB operations in Phase 3.

/// Offset of first flag byte in locator
pub const TNS_LOB_LOC_OFFSET_FLAG_1: usize = 4;
/// Offset of third flag byte in locator (used for charset detection)
#[allow(dead_code)]
pub const TNS_LOB_LOC_OFFSET_FLAG_3: usize = 6;
/// Offset of fourth flag byte in locator
pub const TNS_LOB_LOC_OFFSET_FLAG_4: usize = 7;
/// Fixed offset for BFILE directory/filename (used in get_file_name)
#[allow(dead_code)]
pub const TNS_LOB_LOC_FIXED_OFFSET: usize = 16;

// LOB locator flag constants (from constants.pxi)
// These flags are used to interpret the meaning of bytes in the LOB locator.

/// Flag indicating BLOB type
pub const TNS_LOB_LOC_FLAGS_BLOB: u8 = 0x01;
/// Flag indicating value-based LOB (used for LOB operations)
#[allow(dead_code)]
pub const TNS_LOB_LOC_FLAGS_VALUE_BASED: u8 = 0x20;
/// Flag indicating abstract LOB
pub const TNS_LOB_LOC_FLAGS_ABSTRACT: u8 = 0x40;
/// Flag indicating LOB is initialized (used for LOB operations)
#[allow(dead_code)]
pub const TNS_LOB_LOC_FLAGS_INIT: u8 = 0x08;
/// Flag indicating temporary LOB
pub const TNS_LOB_LOC_FLAGS_TEMP: u8 = 0x01;
/// Flag indicating variable length charset (used for encoding detection)
#[allow(dead_code)]
pub const TNS_LOB_LOC_FLAGS_VAR_LENGTH_CHARSET: u8 = 0x80;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lob_locator_new() {
        let locator = LobLocator::new(vec![1, 2, 3, 4], 100, 8192, true);
        assert_eq!(locator.size, 100);
        assert_eq!(locator.chunk_size, 8192);
        assert!(locator.has_metadata);
    }

    #[test]
    fn test_lob_locator_empty() {
        let locator = LobLocator::empty();
        assert_eq!(locator.locator.len(), 40);
        assert_eq!(locator.size, 0);
        assert!(!locator.has_metadata);
    }

    #[test]
    fn test_lob_value_with_data() {
        let locator = LobLocator::new(vec![1, 2, 3, 4], 5, 8192, true);
        let value = LobValue::with_data(locator, b"hello".to_vec());

        assert!(value.has_data());
        assert_eq!(value.as_string(), Some("hello".to_string()));
        assert_eq!(value.as_bytes(), Some(b"hello".as_slice()));
        assert_eq!(value.size(), 5);
    }

    #[test]
    fn test_lob_value_locator_only() {
        let locator = LobLocator::new(vec![1, 2, 3, 4], 1000000, 8192, true);
        let value = LobValue::locator_only(locator);

        assert!(!value.has_data());
        assert_eq!(value.as_string(), None);
        assert_eq!(value.size(), 1000000);
    }

    #[test]
    fn test_lob_locator_flags() {
        // Create a locator with BLOB flag set at offset 4
        let mut locator_bytes = vec![0u8; 10];
        locator_bytes[TNS_LOB_LOC_OFFSET_FLAG_1] = TNS_LOB_LOC_FLAGS_BLOB;
        locator_bytes[TNS_LOB_LOC_OFFSET_FLAG_4] = TNS_LOB_LOC_FLAGS_TEMP;

        let locator = LobLocator::new(locator_bytes, 100, 8192, true);
        assert!(locator.is_blob());
        assert!(locator.is_temp());
        assert!(!locator.is_abstract());
    }
}
