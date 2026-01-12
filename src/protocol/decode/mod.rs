//! Data type decoders for Oracle wire protocol.
//!
//! Each supported datatype has its own module with decode functions.
//!
//! ## Currently Supported
//!
//! | Oracle Type | Module |
//! |-------------|--------|
//! | NUMBER      | `number` |
//! | BINARY_INTEGER | `number` |
//! | DATE        | `date` |
//!
//! String types (VARCHAR2, CHAR, LONG) use simple UTF-8 conversion
//! and don't require dedicated decoders.

mod date;
mod number;

pub use date::decode_oracle_date;
pub use number::decode_oracle_number;
