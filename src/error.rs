//! Error types for the Oracle thin client.

use std::io;
use thiserror::Error;
use std::panic::Location;

/// Result type alias for Oracle operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for Oracle thin client operations.
#[derive(Error, Debug)]
pub enum Error {
    /// I/O error during network communication.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Connection refused by the server.
    #[error("Connection refused: {message}")]
    ConnectionRefused { message: String },

    /// Invalid service name.
    #[error("Invalid service name: {service_name}")]
    InvalidServiceName { service_name: String },

    /// Invalid SID.
    #[error("Invalid SID: {sid}")]
    InvalidSid { sid: String },

    /// Server version not supported.
    #[error("Server version not supported (minimum required: {min_version})")]
    ServerVersionNotSupported { min_version: u16 },

    /// Authentication failed.
    #[error("Authentication failed: {message}")]
    AuthenticationFailed { message: String },

    /// Unsupported verifier type.
    #[error("Unsupported verifier type: {verifier_type:#x}")]
    UnsupportedVerifierType { verifier_type: u32 },

    /// Invalid server response during authentication.
    #[error("Invalid server response during authentication")]
    InvalidServerResponse,

    /// Protocol error.
    #[error("Protocol error: {message}")]
    Protocol { message: String },

    /// Unexpected packet type received.
    #[error("Unexpected packet type: expected {expected}, got {actual}")]
    UnexpectedPacketType { expected: u8, actual: u8 },

    /// Connection closed.
    #[error("Connection closed")]
    ConnectionClosed,

    /// Oracle database error.
    #[error("ORA-{code:05}: {message}")]
    Oracle { code: u32, message: String },

    /// Type conversion error.
    #[error("Type conversion error: {message}")]
    TypeConversion { message: String },

    /// Column not found.
    #[error("Column not found: {name}")]
    ColumnNotFound { name: String },

    /// Column index out of bounds.
    #[error("Column index {index} out of bounds (columns: {count})")]
    ColumnIndexOutOfBounds { index: usize, count: usize },

    /// Null value error.
    #[error("Unexpected NULL value in column {column}")]
    NullValue { column: String },

    /// Buffer too small.
    #[error("Buffer too small: need {needed} bytes, have {available} filed at {location}")]
    BufferTooSmall { needed: usize, available: usize, location: &'static Location<'static> },

    /// Invalid connect string.
    #[error("Invalid connect string: {message}")]
    InvalidConnectString { message: String },

    /// Unsupported Oracle data type.
    #[error("Unsupported Oracle data type: {type_num}")]
    UnsupportedType { type_num: u8 },

    /// Connection timed out during TCP connect.
    #[error("Connection to {host}:{port} timed out after {timeout:?}")]
    ConnectionTimeout {
        host: String,
        port: u16,
        timeout: std::time::Duration,
    },

    /// DNS resolution failed.
    #[error("Failed to resolve hostname '{hostname}': {message}")]
    DnsResolutionFailed { hostname: String, message: String },
}

impl Error {
    /// Create a protocol error.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    /// Create an Oracle database error.
    pub fn oracle(code: u32, message: impl Into<String>) -> Self {
        Self::Oracle {
            code,
            message: message.into(),
        }
    }

    /// Create a type conversion error.
    pub fn type_conversion(message: impl Into<String>) -> Self {
        Self::TypeConversion {
            message: message.into(),
        }
    }
}
