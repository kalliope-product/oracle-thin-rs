//! Oracle Thin Client for Rust
//!
//! A pure Rust implementation of an Oracle database thin client that connects
//! directly to Oracle databases without requiring Oracle Instant Client.
//!
//! # Example
//!
//! ```no_run
//! use oracle_thin_rs::{Connection, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Connect to the database
//!     let conn = Connection::connect(
//!         "localhost:1521/FREEPDB1",
//!         "username",
//!         "password"
//!     ).await?;
//!
//!     // Get server version
//!     if let Some(version) = conn.server_version() {
//!         println!("Connected to Oracle {}.{}.{}.{}.{}",
//!             version.0, version.1, version.2, version.3, version.4);
//!     }
//!
//!     // Close connection
//!     conn.close().await?;
//!
//!     Ok(())
//! }
//! ```

pub mod connection;
pub mod cursor;
pub mod error;
pub mod protocol;

// Re-export main types
pub use connection::{Connection, QueryResult};
pub use cursor::{Cursor, CursorStreamExt, RowCursor};
pub use error::{Error, Result};
pub use protocol::connect::ConnectParams;
pub use protocol::types::{Column, ColumnInfo, ColumnMetadata, OracleType, OracleValue, Row};
