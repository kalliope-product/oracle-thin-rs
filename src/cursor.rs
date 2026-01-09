//! Cursor for iterating over query results with buffering.
//!
//! The Cursor handles prefetched rows and can fetch more rows from the server
//! when the buffer is exhausted.

use crate::protocol::types::{ColumnMetadata, Row};

/// A cursor for iterating over query results.
///
/// The cursor maintains a buffer of rows and can fetch additional rows
/// from the server when needed. It tracks the cursor ID assigned by the
/// server and knows whether more rows are available.
///
/// # Example
///
/// ```no_run
/// use oracle_thin_rs::Connection;
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
///     // Process all rows (fetches more as needed)
///     while let Some(row) = conn.next_row(&mut cursor).await? {
///         println!("{:?}", row);
///     }
///
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct Cursor {
    /// Column metadata from describe.
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
}

impl Cursor {
    /// Create a new cursor with initial prefetched data.
    ///
    /// # Arguments
    ///
    /// * `columns` - Column metadata from describe info
    /// * `cursor_id` - Cursor ID assigned by server
    /// * `rows` - Initial prefetched rows
    /// * `more_rows` - Whether server has more rows
    /// * `fetch_size` - Number of rows to fetch per request
    pub fn new(
        columns: Vec<ColumnMetadata>,
        cursor_id: u32,
        rows: Vec<Row>,
        more_rows: bool,
        fetch_size: u32,
    ) -> Self {
        let rows_fetched = rows.len() as u64;
        Self {
            columns,
            cursor_id,
            buffer: rows,
            buffer_pos: 0,
            more_rows,
            fetch_size,
            rows_fetched,
        }
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

    /// Check if there are more rows available (locally buffered or on server).
    pub fn has_more(&self) -> bool {
        self.buffer_pos < self.buffer.len() || self.more_rows
    }

    /// Check if the cursor is exhausted (no more rows anywhere).
    pub fn is_exhausted(&self) -> bool {
        self.buffer_pos >= self.buffer.len() && !self.more_rows
    }

    /// Get the number of rows currently buffered.
    pub fn buffered_count(&self) -> usize {
        self.buffer.len().saturating_sub(self.buffer_pos)
    }

    /// Get total rows fetched so far.
    pub fn rows_fetched(&self) -> u64 {
        self.rows_fetched
    }

    /// Get the fetch size.
    pub fn fetch_size(&self) -> u32 {
        self.fetch_size
    }

    /// Set the fetch size for subsequent fetches.
    pub fn set_fetch_size(&mut self, size: u32) {
        self.fetch_size = size;
    }

    /// Take the next row from the buffer without fetching more.
    ///
    /// Returns `None` if the buffer is exhausted. Use `next_row()` for
    /// automatic fetching.
    pub fn take_buffered(&mut self) -> Option<Row> {
        if self.buffer_pos < self.buffer.len() {
            let row = self.buffer[self.buffer_pos].clone();
            self.buffer_pos += 1;
            Some(row)
        } else {
            None
        }
    }

    /// Drain all buffered rows without fetching more.
    ///
    /// This returns an iterator over the remaining buffered rows.
    /// After calling this, `buffered_count()` will be 0.
    pub fn drain_buffer(&mut self) -> impl Iterator<Item = Row> + '_ {
        let remaining = self.buffer.split_off(self.buffer_pos);
        self.buffer.clear();
        self.buffer_pos = 0;
        remaining.into_iter()
    }

    /// Collect all buffered rows into a Vec without fetching more.
    pub fn collect_buffered(&mut self) -> Vec<Row> {
        self.drain_buffer().collect()
    }

    /// Clear the buffer and reset position.
    ///
    /// This reuses the buffer's capacity for the next fetch.
    #[allow(dead_code)]
    pub(crate) fn _clear_buffer(&mut self) {
        self.buffer.clear();
        self.buffer_pos = 0;
    }

    /// Add rows to the buffer (called by Connection after fetch).
    pub(crate) fn add_rows(&mut self, rows: Vec<Row>, more_rows: bool) {
        // Reuse buffer capacity by clearing first if we're at the end
        if self.buffer_pos >= self.buffer.len() {
            self.buffer.clear();
            self.buffer_pos = 0;
        }

        self.rows_fetched += rows.len() as u64;
        self.buffer.extend(rows);
        self.more_rows = more_rows;
    }

    /// Mark the cursor as having no more server rows.
    #[allow(dead_code)]
    pub(crate) fn _set_no_more_rows(&mut self) {
        self.more_rows = false;
    }

    /// Check if we need to fetch more from server.
    pub(crate) fn needs_fetch(&self) -> bool {
        self.buffer_pos >= self.buffer.len() && self.more_rows
    }

    /// Get fetch parameters for the next fetch.
    pub(crate) fn fetch_params(&self) -> (u32, u32) {
        (self.cursor_id, self.fetch_size)
    }
}

/// A synchronous iterator over buffered rows only.
///
/// This iterator does NOT fetch more rows from the server.
/// Use `Cursor::next_row()` for automatic fetching.
pub struct BufferedRowIter<'a> {
    cursor: &'a mut Cursor,
}

impl<'a> Iterator for BufferedRowIter<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.take_buffered()
    }
}

impl Cursor {
    /// Get an iterator over buffered rows only (no network fetching).
    pub fn buffered_iter(&mut self) -> BufferedRowIter<'_> {
        BufferedRowIter { cursor: self }
    }
}

/// Result from collecting all rows from a cursor.
#[derive(Debug)]
pub struct CollectedRows {
    /// Column metadata.
    pub columns: Vec<ColumnMetadata>,
    /// All collected rows.
    pub rows: Vec<Row>,
    /// Total row count.
    pub total_rows: u64,
}

impl CollectedRows {
    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Iterate over rows.
    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }
}

impl IntoIterator for CollectedRows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a> IntoIterator for &'a CollectedRows {
    type Item = &'a Row;
    type IntoIter = std::slice::Iter<'a, Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::*;
    use crate::protocol::types::{ColumnInfo, OracleValue};

    fn make_test_column_info() -> Arc<ColumnInfo> {
        Arc::new(ColumnInfo::from_metadata(&[ColumnMetadata::new("ID".to_string(), 2)]).unwrap())
    }

    fn make_test_row(id: i32, column_info: Arc<ColumnInfo>) -> Row {
        Row::new(
            vec![OracleValue::Number(id.to_string())],
            column_info,
        )
    }

    fn make_test_columns() -> Vec<ColumnMetadata> {
        vec![ColumnMetadata::new("ID".to_string(), 2)]
    }

    #[test]
    fn test_cursor_creation() {
        let col_info = make_test_column_info();
        let rows = vec![make_test_row(1, col_info.clone()), make_test_row(2, col_info)];
        let cursor = Cursor::new(make_test_columns(), 42, rows, true, 100);

        assert_eq!(cursor.cursor_id(), 42);
        assert_eq!(cursor.buffered_count(), 2);
        assert_eq!(cursor.fetch_size(), 100);
        assert!(cursor.has_more());
        assert!(!cursor.is_exhausted());
    }

    #[test]
    fn test_take_buffered() {
        let col_info = make_test_column_info();
        let rows = vec![make_test_row(1, col_info.clone()), make_test_row(2, col_info)];
        let mut cursor = Cursor::new(make_test_columns(), 42, rows, false, 100);

        assert_eq!(cursor.buffered_count(), 2);

        let row1 = cursor.take_buffered().unwrap();
        assert_eq!(row1.get(0).unwrap().to_i64(), Some(1));
        assert_eq!(cursor.buffered_count(), 1);

        let row2 = cursor.take_buffered().unwrap();
        assert_eq!(row2.get(0).unwrap().to_i64(), Some(2));
        assert_eq!(cursor.buffered_count(), 0);

        assert!(cursor.take_buffered().is_none());
        assert!(cursor.is_exhausted());
    }

    #[test]
    fn test_drain_buffer() {
        let col_info = make_test_column_info();
        let rows = vec![
            make_test_row(1, col_info.clone()),
            make_test_row(2, col_info.clone()),
            make_test_row(3, col_info),
        ];
        let mut cursor = Cursor::new(make_test_columns(), 42, rows, false, 100);

        // Consume one row first
        cursor.take_buffered();
        assert_eq!(cursor.buffered_count(), 2);

        // Drain remaining
        let drained: Vec<_> = cursor.drain_buffer().collect();
        assert_eq!(drained.len(), 2);
        assert_eq!(cursor.buffered_count(), 0);
    }

    #[test]
    fn test_add_rows_reuses_buffer() {
        let col_info = make_test_column_info();
        let rows = vec![make_test_row(1, col_info.clone())];
        let mut cursor = Cursor::new(make_test_columns(), 42, rows, true, 100);

        // Consume the buffer
        cursor.take_buffered();
        assert_eq!(cursor.buffered_count(), 0);

        // Add more rows (should reuse capacity)
        cursor.add_rows(
            vec![make_test_row(2, col_info.clone()), make_test_row(3, col_info)],
            false,
        );

        assert_eq!(cursor.buffered_count(), 2);
        assert_eq!(cursor.rows_fetched(), 3);
        assert!(!cursor.more_rows);
    }

    #[test]
    fn test_needs_fetch() {
        let col_info = make_test_column_info();
        let rows = vec![make_test_row(1, col_info)];
        let mut cursor = Cursor::new(make_test_columns(), 42, rows, true, 100);

        // Has buffered rows, shouldn't need fetch
        assert!(!cursor.needs_fetch());

        // Consume buffer
        cursor.take_buffered();

        // Now needs fetch (more_rows is true)
        assert!(cursor.needs_fetch());

        // Simulate end of data
        cursor._set_no_more_rows();
        assert!(!cursor.needs_fetch());
    }
}
