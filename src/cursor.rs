//! Cursor for iterating over query results with buffering.
//!
//! The cursor module provides a trait-based interface for iterating over query
//! results. The `Cursor` trait defines the common interface, while `RowCursor`
//! provides a row-by-row iteration implementation.

use crate::connection::Connection;
use crate::error::Result;
use crate::protocol::buffer::ReadBuffer;
use crate::protocol::messages::FetchMessage;
use crate::protocol::response::parse_fetch_response;
use crate::protocol::types::{ColumnMetadata, Row};
use futures::Stream;
use std::future::Future;

/// Base trait for all cursor types.
///
/// Each cursor implementation specifies its Item type and implements
/// the async methods for fetching items. The cursor holds a mutable
/// reference to the connection, ensuring only one active cursor per
/// connection at a time.
///
/// # Example
///
/// ```no_run
/// use oracle_thin_rs::{Connection, Cursor, Row, RowCursor};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let mut conn = Connection::connect(
///         "localhost:1521/FREEPDB1",
///         "user",
///         "password"
///     ).await?;
///
///     // Generic function that works with any cursor type
///     async fn count_rows<C: Cursor<Item = Row>>(cursor: &mut C) -> u64 {
///         let mut count = 0;
///         while let Some(_) = cursor.next().await.unwrap() {
///             count += 1;
///         }
///         count
///     }
///
///     let mut cursor = conn.open_cursor("SELECT * FROM users").await?;
///     let total = count_rows(&mut cursor).await;
///
///     Ok(())
/// }
/// ```
pub trait Cursor {
    /// The type of item this cursor yields.
    type Item;

    /// Column metadata for this cursor.
    fn columns(&self) -> &[ColumnMetadata];

    /// Number of rows fetched so far.
    fn rowcount(&self) -> u64;

    /// Check if cursor is closed (cursor_id == 0).
    fn is_closed(&self) -> bool;

    /// Check if more items are available (buffered or on server).
    fn has_more(&self) -> bool;

    /// Fetch size - roundtrip agreement between client and server.
    fn fetch_size(&self) -> u32;

    /// Set fetch size for subsequent fetches.
    fn set_fetch_size(&mut self, size: u32);

    /// Close the cursor and release server resources.
    fn close(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Get the next item, fetching from server if buffer exhausted.
    ///
    /// Returns `Ok(None)` when exhausted.
    fn next(&mut self) -> impl Future<Output = Result<Option<Self::Item>>> + Send;

    /// Fetch all remaining items into a vector.
    ///
    /// This will make multiple fetch requests to the server until all items
    /// are retrieved. The cursor will be closed after this call.
    fn fetch_all(&mut self) -> impl Future<Output = Result<Vec<Self::Item>>> + Send;
}

/// Row-by-row cursor implementation.
///
/// Holds a mutable reference to the connection, ensuring only one
/// active cursor per connection at a time. The cursor owns the iteration
/// logic and handles fetching from the server when the buffer is exhausted.
///
/// # Lifecycle
///
/// 1. Created by `Connection::open_cursor()`
/// 2. Iterated via `next()` or `fetch_all()`
/// 3. Automatically closed when exhausted or explicitly via `close()`
///
/// # Example
///
/// ```no_run
/// use oracle_thin_rs::{Connection, Cursor};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let mut conn = Connection::connect(
///         "localhost:1521/FREEPDB1",
///         "user",
///         "password"
///     ).await?;
///
///     let mut cursor = conn.open_cursor("SELECT * FROM large_table").await?;
///
///     // Process rows one at a time
///     while let Some(row) = cursor.next().await? {
///         println!("{:?}", row);
///     }
///
///     // Or fetch all at once
///     // let rows = cursor.fetch_all().await?;
///
///     Ok(())
/// }
/// ```
pub struct RowCursor<'conn> {
    /// Mutable reference to connection.
    conn: &'conn mut Connection,
    /// Column metadata.
    columns: Vec<ColumnMetadata>,
    /// Cursor ID assigned by server (0 means closed).
    cursor_id: u32,
    /// Buffered rows from prefetch/fetch.
    buffer: Vec<Row>,
    /// Current position in buffer.
    buffer_pos: usize,
    /// Whether the server has more rows.
    more_rows: bool,
    /// Number of rows to fetch per request.
    fetch_size: u32,
    /// Total rows fetched so far.
    rows_fetched: u64,
    /// Server TTC field version.
    server_ttc_field_version: u8,
}

impl<'conn> RowCursor<'conn> {
    /// Create a new RowCursor from components.
    ///
    /// This is called by Connection::open_cursor().
    pub(crate) fn new(
        conn: &'conn mut Connection,
        columns: Vec<ColumnMetadata>,
        cursor_id: u32,
        rows: Vec<Row>,
        more_rows: bool,
        fetch_size: u32,
        server_ttc_field_version: u8,
    ) -> Self {
        let rows_fetched = rows.len() as u64;
        Self {
            conn,
            columns,
            cursor_id,
            buffer: rows,
            buffer_pos: 0,
            more_rows,
            fetch_size,
            rows_fetched,
            server_ttc_field_version,
        }
    }

    /// Internal: Perform a fetch from the server.
    async fn do_fetch(&mut self) -> Result<()> {
        // Reuse buffer capacity
        if self.buffer_pos >= self.buffer.len() {
            self.buffer.clear();
            self.buffer_pos = 0;
        }

        // Create fetch message
        let msg = FetchMessage::new(self.cursor_id, self.fetch_size);

        // Send and receive via Connection
        let response = self.conn.send_message_and_read_response(&msg).await?;

        // Parse response
        let mut buf = ReadBuffer::new(response.payload);
        let _data_flags = buf.read_u16_be()?;

        let fetch_response =
            parse_fetch_response(&mut buf, &self.columns, self.server_ttc_field_version)?;

        // Check for errors (1403 = ORA-01403 "no data found" = normal end)
        if fetch_response.error_info.error_num != 0 && fetch_response.error_info.error_num != 1403 {
            return Err(crate::error::Error::Oracle {
                code: fetch_response.error_info.error_num,
                message: fetch_response.error_info.message.unwrap_or_default(),
            });
        }

        // Update state
        self.rows_fetched += fetch_response.rows.len() as u64;
        self.buffer.extend(fetch_response.rows);
        self.more_rows = fetch_response.more_rows;

        Ok(())
    }

    /// Get column metadata.
    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get the number of columns.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Get the cursor ID.
    pub fn cursor_id(&self) -> u32 {
        self.cursor_id
    }

    /// Get the number of rows currently buffered.
    pub fn buffered_count(&self) -> usize {
        self.buffer.len().saturating_sub(self.buffer_pos)
    }
}

impl<'conn> Cursor for RowCursor<'conn> {
    type Item = Row;

    fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    fn rowcount(&self) -> u64 {
        self.rows_fetched
    }

    fn is_closed(&self) -> bool {
        self.cursor_id == 0
    }

    fn has_more(&self) -> bool {
        self.buffer_pos < self.buffer.len() || self.more_rows
    }

    fn fetch_size(&self) -> u32 {
        self.fetch_size
    }

    fn set_fetch_size(&mut self, size: u32) {
        self.fetch_size = size;
    }

    async fn close(&mut self) -> Result<()> {
        if self.cursor_id != 0 {
            // TODO: Send close message to server (Phase 1: just mark closed)
            self.cursor_id = 0;
            self.more_rows = false;
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        // Try buffered first
        if self.buffer_pos < self.buffer.len() {
            let row = self.buffer[self.buffer_pos].clone();
            self.buffer_pos += 1;
            return Ok(Some(row));
        }

        // No more rows?
        if !self.more_rows {
            self.cursor_id = 0;
            return Ok(None);
        }

        // Fetch more from server
        self.do_fetch().await?;

        // Try buffer again after fetch
        if self.buffer_pos < self.buffer.len() {
            let row = self.buffer[self.buffer_pos].clone();
            self.buffer_pos += 1;
            Ok(Some(row))
        } else {
            self.cursor_id = 0;
            Ok(None)
        }
    }

    async fn fetch_all(&mut self) -> Result<Vec<Self::Item>> {
        let mut all_rows = std::mem::take(&mut self.buffer);
        self.buffer_pos = 0;

        while self.more_rows {
            self.do_fetch().await?;
            all_rows.append(&mut self.buffer);
        }

        self.cursor_id = 0;
        Ok(all_rows)
    }
}

/// Extension trait for converting Cursor to Stream.
///
/// # Example
///
/// ```no_run
/// use oracle_thin_rs::{Connection, Cursor, CursorStreamExt};
/// use futures::stream::TryStreamExt;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let mut conn = Connection::connect(
///         "localhost:1521/FREEPDB1",
///         "user",
///         "password"
///     ).await?;
///
///     let mut cursor = conn.open_cursor("SELECT * FROM users").await?;
///
///     let names: Vec<String> = cursor.into_stream()
///         .map_ok(|row| row.get(1).unwrap().to_string())
///         .try_collect()
///         .await?;
///
///     Ok(())
/// }
/// ```
pub trait CursorStreamExt: Cursor + Sized {
    /// Convert this cursor into a Stream yielding `Result<Item>`.
    ///
    /// The stream takes ownership of the cursor. Each call to `poll_next`
    /// will call `cursor.next()` internally.
    fn into_stream(self) -> impl Stream<Item = Result<Self::Item>>;
}

impl<C: Cursor + Unpin> CursorStreamExt for C {
    fn into_stream(self) -> impl Stream<Item = Result<Self::Item>> {
        use futures::stream;

        stream::unfold(Some(self), |opt_cursor| async move {
            let mut cursor = opt_cursor?;
            match cursor.next().await {
                Ok(Some(item)) => Some((Ok(item), Some(cursor))),
                Ok(None) => None,
                Err(e) => Some((Err(e), Some(cursor))),
            }
        })
    }
}
