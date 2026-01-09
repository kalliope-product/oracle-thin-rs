//! Internal column metadata from wire format.
//!
//! This struct preserves the raw Oracle wire format data.
//! For user-facing API, use `Column` which provides a cleaner interface.

/// Internal column metadata from wire format.
///
/// Use `Column` for user-facing API.
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// Column name.
    pub name: String,
    /// Oracle data type number (raw wire format).
    pub oracle_type: u8,
    /// Numeric precision.
    pub precision: i8,
    /// Numeric scale.
    pub scale: i8,
    /// Maximum size in bytes.
    pub max_size: u32,
    /// Buffer size for this column.
    pub buffer_size: u32,
    /// Whether NULL values are allowed.
    pub nullable: bool,
}

impl ColumnMetadata {
    /// Create new column metadata with minimal info.
    pub fn new(name: String, oracle_type: u8) -> Self {
        Self {
            name,
            oracle_type,
            precision: 0,
            scale: 0,
            max_size: 0,
            buffer_size: 0,
            nullable: true,
        }
    }
}
