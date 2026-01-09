//! High-level Connection API for Oracle thin client.

use crate::cursor::{CollectedRows, Cursor};
use crate::error::{Error, Result};
use crate::protocol::auth::{authenticate, phase_two, AuthCredentials, SessionData};
use crate::protocol::buffer::ReadBuffer;
use crate::protocol::connect::{connect, exchange_data_types, fast_auth, ConnectParams};
use crate::protocol::constants::*;
use crate::protocol::messages::{
    ExecuteMessage, FetchMessage, MarkerMessage, TNS_MARKER_TYPE_RESET,
};
use crate::protocol::packet::{Capabilities, PacketStream};
use crate::protocol::response::{parse_execute_response, parse_fetch_response};
use crate::protocol::types::{ColumnMetadata, Row};
use tokio::net::TcpStream;

/// Result of a query execution.
#[derive(Debug)]
pub struct QueryResult {
    /// Column metadata.
    pub columns: Vec<ColumnMetadata>,
    /// Rows returned.
    pub rows: Vec<Row>,
    /// Total row count.
    pub row_count: u64,
    /// Whether more rows are available (for pagination).
    pub more_rows: bool,
}

impl QueryResult {
    /// Get the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if the result is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Iterate over rows.
    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }
}

impl IntoIterator for QueryResult {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a> IntoIterator for &'a QueryResult {
    type Item = &'a Row;
    type IntoIter = std::slice::Iter<'a, Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

/// An Oracle database connection.
pub struct Connection {
    /// Packet stream for communication.
    stream: PacketStream,
    /// Connection capabilities.
    caps: Capabilities,
    /// Session data from authentication.
    session: SessionData,
    /// Whether auto-commit is enabled.
    autocommit: bool,
}

impl Connection {
    /// Connect to an Oracle database.
    ///
    /// # Arguments
    ///
    /// * `conn_str` - Connection string in format "host:port/service_name"
    /// * `username` - Database username
    /// * `password` - Database password
    ///
    /// # Example
    ///
    /// ```no_run
    /// use oracle_thin_rs::Connection;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let conn = Connection::connect(
    ///         "localhost:1521/FREEPDB1",
    ///         "read_user",
    ///         "password"
    ///     ).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn connect(
        conn_str: &str,
        username: &str,
        password: &str,
    ) -> Result<Self> {
        // Parse connection string
        let params = ConnectParams::parse(conn_str)?;

        Self::connect_with_params(&params, username, password).await
    }

    /// Connect with explicit connection parameters.
    pub async fn connect_with_params(
        params: &ConnectParams,
        username: &str,
        password: &str,
    ) -> Result<Self> {
        // Establish TCP connection
        let addr = format!("{}:{}", params.host, params.port);
        let tcp_stream = TcpStream::connect(&addr).await?;

        // Set TCP_NODELAY for immediate packet transmission (matches Python oracledb)
        tcp_stream.set_nodelay(true)?;

        // Create packet stream
        let mut stream = PacketStream::new(tcp_stream);

        // Initialize capabilities
        let mut caps = Capabilities::new();

        // Perform TNS connect handshake
        connect(&mut stream, params, &mut caps).await?;

        // Note: Python's asyncio implementation also disables OOB (supports_oob = False)
        // so we don't need to send OOB break + RESET marker after ACCEPT

        // Create credentials
        let creds = AuthCredentials::new(username, password);

        // Use FastAuth for Oracle 23ai+, otherwise normal auth
        let session = if caps.supports_fast_auth {
            // FastAuth combines protocol, data types, and auth phase 1
            let mut session = fast_auth(&mut stream, &mut caps, &creds).await?;

            // Complete authentication with phase 2
            phase_two(&mut stream, &creds, &caps, &mut session).await?;

            session
        } else {
            // Exchange data types first
            exchange_data_types(&mut stream, &mut caps).await?;

            // Then authenticate
            authenticate(&mut stream, &creds, &caps).await?
        };

        Ok(Self {
            stream,
            caps,
            session,
            autocommit: false,
        })
    }

    /// Check if the connection is alive by sending a ping.
    pub async fn ping(&mut self) -> Result<()> {
        // TODO: Implement ping
        Ok(())
    }

    /// Close the connection.
    pub async fn close(self) -> Result<()> {
        // TODO: Send logoff message
        // For now, just drop the connection (TCP close)
        Ok(())
    }

    /// Get the protocol version.
    pub fn protocol_version(&self) -> u16 {
        self.caps.protocol_version
    }

    /// Get the SDU size.
    pub fn sdu(&self) -> u32 {
        self.caps.sdu
    }

    /// Set auto-commit mode.
    pub fn set_autocommit(&mut self, autocommit: bool) {
        self.autocommit = autocommit;
    }

    /// Get auto-commit mode.
    pub fn autocommit(&self) -> bool {
        self.autocommit
    }

    /// Execute a SELECT query and return the results.
    ///
    /// This is a simplified version that returns all prefetched rows.
    /// For large result sets, use `query_iter()` instead (not yet implemented).
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
    ///         "read_user",
    ///         "password"
    ///     ).await?;
    ///
    ///     let rows = conn.query("SELECT 'hello' FROM DUAL").await?;
    ///     for row in rows {
    ///         println!("{:?}", row);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        // Default prefetch size
        let prefetch_rows = 100u32;

        // Create execute message
        let msg = ExecuteMessage::new_query(sql, prefetch_rows, self.caps.ttc_field_version);

        // Debug: print the wire format and hex dump
        let wire_size = crate::protocol::message::Message::wire_size(&msg);
        // eprintln!("[DEBUG] Execute message wire size: {}", wire_size);
        // eprintln!("[DEBUG] TTC field version: {}", self.caps.ttc_field_version);

        // Serialize and dump hex
        let mut debug_buf = Vec::with_capacity(wire_size);
        crate::protocol::message::Message::write_to(&msg, &mut debug_buf)?;
        // eprintln!("[DEBUG] Execute message hex ({} bytes):", debug_buf.len());
        // for chunk in debug_buf.chunks(16) {
        //     let hex: Vec<String> = chunk.iter().map(|b| format!("{:02X}", b)).collect();
        //     eprintln!("  {}", hex.join(" "));
        // }

        // Send execute message
        self.stream.send_data_message(&msg).await?;

        // Read response, handling any control/marker packets
        let response = self.read_data_response().await?;

        let mut buf = ReadBuffer::new(response.payload);
        let _data_flags = buf.read_u16_be()?;

        let exec_response = parse_execute_response(
            &mut buf,
            self.caps.ttc_field_version,
            self.caps.server_ttc_field_version,
        )?;

        // Check for Oracle errors
        if exec_response.error_info.error_num != 0 && exec_response.error_info.error_num != 1403 {
            return Err(Error::Oracle {
                code: exec_response.error_info.error_num,
                message: exec_response.error_info.message.unwrap_or_default(),
            });
        }

        Ok(QueryResult {
            columns: exec_response.columns,
            rows: exec_response.rows,
            row_count: exec_response.error_info.row_count,
            more_rows: exec_response.more_rows,
        })
    }

    /// Open a cursor for a SELECT query.
    ///
    /// This returns a Cursor that can be used to iterate over results with
    /// automatic fetching when the buffer is exhausted.
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
    ///         "read_user",
    ///         "password"
    ///     ).await?;
    ///
    ///     let mut cursor = conn.open_cursor("SELECT * FROM large_table").await?;
    ///
    ///     while let Some(row) = conn.next_row(&mut cursor).await? {
    ///         println!("{:?}", row);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_cursor(&mut self, sql: &str) -> Result<Cursor> {
        self.open_cursor_with_fetch_size(sql, 100).await
    }

    /// Open a cursor with a specific fetch size.
    pub async fn open_cursor_with_fetch_size(
        &mut self,
        sql: &str,
        fetch_size: u32,
    ) -> Result<Cursor> {
        // Create execute message
        let msg = ExecuteMessage::new_query(sql, fetch_size, self.caps.ttc_field_version);

        // Send execute message
        self.stream.send_data_message(&msg).await?;

        // Read response
        let response = self.read_data_response().await?;

        // Parse response
        let mut buf = ReadBuffer::new(response.payload);
        let _data_flags = buf.read_u16_be()?;

        let exec_response = parse_execute_response(
            &mut buf,
            self.caps.ttc_field_version,
            self.caps.server_ttc_field_version,
        )?;

        // Check for Oracle errors
        if exec_response.error_info.error_num != 0 && exec_response.error_info.error_num != 1403 {
            return Err(Error::Oracle {
                code: exec_response.error_info.error_num,
                message: exec_response.error_info.message.unwrap_or_default(),
            });
        }

        Ok(Cursor::new(
            exec_response.columns,
            exec_response.error_info.cursor_id as u32,
            exec_response.rows,
            exec_response.more_rows,
            fetch_size,
        ))
    }

    /// Fetch more rows into a cursor.
    ///
    /// This sends a fetch request to the server and adds the returned rows
    /// to the cursor's buffer.
    pub async fn fetch_more(&mut self, cursor: &mut Cursor) -> Result<()> {
        if !cursor.needs_fetch() {
            return Ok(());
        }

        let (cursor_id, fetch_size) = cursor.fetch_params();

        // Create fetch message
        let msg = FetchMessage::new(cursor_id, fetch_size);

        // Send fetch message
        self.stream.send_data_message(&msg).await?;

        // Read response
        let response = self.read_data_response().await?;

        // Parse response
        let mut buf = ReadBuffer::new(response.payload);
        let _data_flags = buf.read_u16_be()?;

        let fetch_response =
            parse_fetch_response(&mut buf, cursor.columns(), self.caps.server_ttc_field_version)?;

        // Check for Oracle errors
        if fetch_response.error_info.error_num != 0 && fetch_response.error_info.error_num != 1403 {
            return Err(Error::Oracle {
                code: fetch_response.error_info.error_num,
                message: fetch_response.error_info.message.unwrap_or_default(),
            });
        }

        // Add rows to cursor
        cursor.add_rows(fetch_response.rows, fetch_response.more_rows);

        Ok(())
    }

    /// Get the next row from a cursor, fetching more if needed.
    ///
    /// Returns `Ok(None)` when all rows have been consumed.
    pub async fn next_row(&mut self, cursor: &mut Cursor) -> Result<Option<Row>> {
        // Try to get from buffer first
        if let Some(row) = cursor.take_buffered() {
            return Ok(Some(row));
        }

        // Need to fetch more?
        if cursor.needs_fetch() {
            self.fetch_more(cursor).await?;
            return Ok(cursor.take_buffered());
        }

        // No more rows
        Ok(None)
    }

    /// Fetch all remaining rows from a cursor.
    ///
    /// This will make multiple fetch requests to the server until all rows
    /// are retrieved.
    pub async fn fetch_all(&mut self, cursor: &mut Cursor) -> Result<CollectedRows> {
        let mut all_rows = cursor.collect_buffered();

        while cursor.needs_fetch() {
            self.fetch_more(cursor).await?;
            all_rows.extend(cursor.drain_buffer());
        }

        Ok(CollectedRows {
            columns: cursor.columns().to_vec(),
            rows: all_rows,
            total_rows: cursor.rows_fetched(),
        })
    }

    /// Helper to read a DATA response, handling control and marker packets.
    ///
    /// When we receive a MARKER packet (typically BREAK/RESET from server due to an error),
    /// we need to send a RESET marker back and wait for the server's RESET marker,
    /// then read the actual error response.
    async fn read_data_response(&mut self) -> Result<crate::protocol::packet::Packet> {
        loop {
            let packet = self.stream.read_packet().await?;

            match packet.packet_type {
                TNS_PACKET_TYPE_DATA => return Ok(packet),
                TNS_PACKET_TYPE_MARKER => {
                    // Server sent a MARKER packet (usually due to an error)
                    // Send RESET marker back
                    let msg = MarkerMessage::reset();
                    self.stream
                        .send_message(TNS_PACKET_TYPE_MARKER, &msg)
                        .await?;

                    // Read packets until we get a RESET marker back from server
                    loop {
                        let marker_packet = self.stream.read_packet().await?;
                        if marker_packet.packet_type == TNS_PACKET_TYPE_MARKER {
                            // Check if it's a RESET marker (payload[2] == 2)
                            if marker_packet.payload.len() >= 3
                                && marker_packet.payload[2] == TNS_MARKER_TYPE_RESET
                            {
                                break;
                            }
                            // Continue reading if it's not a RESET marker
                            continue;
                        } else if marker_packet.packet_type == TNS_PACKET_TYPE_DATA {
                            // Got the error response
                            return Ok(marker_packet);
                        }
                    }
                    // Continue to read the actual DATA response with error info
                    continue;
                }
                TNS_PACKET_TYPE_CONTROL => {
                    // Handle CONTROL packet - just continue
                    continue;
                }
                _ => {
                    return Err(Error::UnexpectedPacketType {
                        expected: TNS_PACKET_TYPE_DATA,
                        actual: packet.packet_type,
                    });
                }
            }
        }
    }

    /// Get the session parameter value.
    pub fn session_param(&self, key: &str) -> Option<&str> {
        self.session.params.get(key).map(|s| s.as_str())
    }

    /// Get the server version from session data.
    pub fn server_version(&self) -> Option<(u8, u8, u8, u8, u8)> {
        let version_str = self.session.params.get("AUTH_VERSION_NO")?;
        let version: u32 = version_str.parse().ok()?;

        // Parse version based on TTC field version
        if self.caps.ttc_field_version >= 11 {
            // 18.1+ format
            Some((
                ((version >> 24) & 0xFF) as u8,
                ((version >> 16) & 0xFF) as u8,
                ((version >> 12) & 0x0F) as u8,
                ((version >> 4) & 0xFF) as u8,
                (version & 0x0F) as u8,
            ))
        } else {
            // Legacy format
            Some((
                ((version >> 24) & 0xFF) as u8,
                ((version >> 20) & 0x0F) as u8,
                ((version >> 12) & 0x0F) as u8,
                ((version >> 8) & 0x0F) as u8,
                (version & 0x0F) as u8,
            ))
        }
    }

    /// Get the internal packet stream (for advanced use).
    #[allow(dead_code)]
    pub(crate) fn _stream(&self) -> &PacketStream {
        &self.stream
    }

    /// Get a mutable reference to the internal packet stream.
    #[allow(dead_code)]
    pub(crate) fn _stream_mut(&mut self) -> &mut PacketStream {
        &mut self.stream
    }

    /// Get the capabilities.
    #[allow(dead_code)]
    pub(crate) fn _capabilities(&self) -> &Capabilities {
        &self.caps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_params_parse() {
        let params = ConnectParams::parse("localhost:1521/FREEPDB1").unwrap();
        assert_eq!(params.host, "localhost");
        assert_eq!(params.port, 1521);
        assert_eq!(params.service_name, "FREEPDB1");

        // Test default port
        let params = ConnectParams::parse("localhost/ORCL").unwrap();
        assert_eq!(params.host, "localhost");
        assert_eq!(params.port, 1521);
        assert_eq!(params.service_name, "ORCL");
    }

    #[test]
    fn test_connect_string_build() {
        let params = ConnectParams::new("myhost", 1521, "MYSERVICE");
        let cs = params.build_connect_string();
        assert!(cs.contains("HOST=myhost"));
        assert!(cs.contains("PORT=1521"));
        assert!(cs.contains("SERVICE_NAME=MYSERVICE"));
    }
}
