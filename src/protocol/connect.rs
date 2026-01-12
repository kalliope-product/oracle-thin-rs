//! TNS connection establishment (CONNECT/ACCEPT handshake).

use crate::error::{Error, Result};
use crate::protocol::auth::{AuthCredentials, SessionData};
use crate::protocol::buffer::ReadBuffer;
use crate::protocol::constants::*;
use crate::protocol::messages::{
    AuthPhaseOneMessage, ConnectMessage, DataTypesMessage, FastAuthMessage, MarkerMessage,
    ProtocolMessage,
};
use crate::protocol::packet::{Capabilities, Packet, PacketStream};
use base64::Engine;
use rand::RngCore;
use std::time::Duration;

/// Connection parameters.
#[derive(Debug, Clone)]
pub struct ConnectParams {
    /// Host address.
    pub host: String,
    /// Port number.
    pub port: u16,
    /// Service name.
    pub service_name: String,
    /// SDU (Session Data Unit) size.
    pub sdu: u32,
    /// TCP connection timeout (default: 20 seconds, matching python-oracledb).
    pub connect_timeout: Duration,
}

impl ConnectParams {
    /// Create new connection parameters.
    pub fn new(host: impl Into<String>, port: u16, service_name: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port,
            service_name: service_name.into(),
            sdu: TNS_SDU_DEFAULT,
            connect_timeout: Duration::from_secs(20), // Python default
        }
    }

    /// Set the connection timeout.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum time to wait for TCP connection establishment
    ///
    /// # Example
    ///
    /// ```
    /// use oracle_thin_rs::ConnectParams;
    /// use std::time::Duration;
    ///
    /// let params = ConnectParams::new("localhost", 1521, "ORCL")
    ///     .with_connect_timeout(Duration::from_secs(5));
    /// ```
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Parse a connection string like "host:port/service_name".
    pub fn parse(conn_str: &str) -> Result<Self> {
        // Format: host:port/service_name or host/service_name (default port 1521)
        let (addr_part, service_name) =
            conn_str
                .split_once('/')
                .ok_or_else(|| Error::InvalidConnectString {
                    message: "Expected format: host:port/service_name".to_string(),
                })?;

        let (host, port) = if let Some((h, p)) = addr_part.split_once(':') {
            let port = p.parse::<u16>().map_err(|_| Error::InvalidConnectString {
                message: format!("Invalid port: {}", p),
            })?;
            (h.to_string(), port)
        } else {
            (addr_part.to_string(), 1521)
        };

        Ok(Self::new(host, port, service_name))
    }

    /// Build the connect descriptor string.
    pub fn build_connect_string(&self) -> String {
        // Get OS username
        let username = whoami::username();

        // Get local hostname (for CID, not the database server hostname)
        let local_hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "localhost".to_string());

        // Generate CONNECTION_ID: 16 random bytes, base64 encoded
        let mut connection_id_bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut connection_id_bytes);
        let connection_id = base64::engine::general_purpose::STANDARD.encode(connection_id_bytes);

        format!(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST={})(PORT={}))(CONNECT_DATA=(SERVICE_NAME={})(CID=(PROGRAM=oracle-thin-rs)(HOST={})(USER={}))(CONNECTION_ID={})))",
            self.host, self.port, self.service_name, local_hostname, username, connection_id
        )
    }
}

/// Send a CONNECT packet and handle the response.
pub async fn connect(
    stream: &mut PacketStream,
    params: &ConnectParams,
    caps: &mut Capabilities,
) -> Result<()> {
    // Build connect string
    let connect_string = params.build_connect_string();
    let connect_bytes = connect_string.as_bytes();

    // Build CONNECT message (zero-copy)
    let msg = ConnectMessage {
        connect_string: connect_bytes,
        sdu: params.sdu,
    };

    // Check if connect data needs to be sent separately
    let send_data_separately = !msg.connect_data_in_packet();

    // Send CONNECT packet using zero-copy path
    stream.send_message(TNS_PACKET_TYPE_CONNECT, &msg).await?;

    // If connect data is too large, send it in a separate DATA packet
    if send_data_separately {
        stream
            .send_data(bytes::Bytes::copy_from_slice(connect_bytes), 0)
            .await?;
    }

    // Handle response
    loop {
        let response = stream.read_packet().await?;

        match response.packet_type {
            TNS_PACKET_TYPE_ACCEPT => {
                return handle_accept(response, stream, caps);
            }
            TNS_PACKET_TYPE_REFUSE => {
                return handle_refuse(response, params);
            }
            TNS_PACKET_TYPE_REDIRECT => {
                // Handle redirect - for now, return an error
                // TODO: Implement redirect handling
                return Err(Error::protocol("Redirect not yet supported"));
            }
            TNS_PACKET_TYPE_RESEND => {
                // Resend the connect packet (rebuild message for simplicity)
                stream.send_message(TNS_PACKET_TYPE_CONNECT, &msg).await?;
                if send_data_separately {
                    stream
                        .send_data(bytes::Bytes::copy_from_slice(connect_bytes), 0)
                        .await?;
                }
            }
            _ => {
                return Err(Error::UnexpectedPacketType {
                    expected: TNS_PACKET_TYPE_ACCEPT,
                    actual: response.packet_type,
                });
            }
        }
    }
}

/// Handle ACCEPT packet.
fn handle_accept(packet: Packet, stream: &mut PacketStream, caps: &mut Capabilities) -> Result<()> {
    let mut buf = ReadBuffer::new(packet.payload);

    // Read protocol version
    let protocol_version = buf.read_u16_be()?;
    if protocol_version < TNS_VERSION_MIN_ACCEPTED {
        return Err(Error::ServerVersionNotSupported {
            min_version: TNS_VERSION_MIN_ACCEPTED,
        });
    }

    // Read protocol options
    let _protocol_options = buf.read_u16_be()?;

    // Skip some fields
    buf.skip(10)?;

    // Read NSI flags
    let nsi_flags1 = buf.read_u8()?;
    if (nsi_flags1 & TNS_NSI_NA_REQUIRED) != 0 {
        return Err(Error::protocol("Native Network Encryption not supported"));
    }

    // Skip more fields
    buf.skip(9)?;

    // Read SDU
    let sdu = buf.read_u32_be()?;
    caps.sdu = sdu;
    stream.set_sdu(sdu);

    // Read flags2 if protocol version supports it
    let mut flags2: u32 = 0;
    if protocol_version >= TNS_VERSION_MIN_OOB_CHECK {
        buf.skip(5)?;
        flags2 = buf.read_u32_be()?;
    }

    // Adjust capabilities
    caps.adjust_for_protocol(protocol_version, 0, flags2);

    // Enable large SDU (4-byte length headers) if protocol version supports it
    // Python uses 4-byte headers for protocol_version >= 315, regardless of SDU size
    let use_large_sdu = protocol_version >= TNS_VERSION_MIN_LARGE_SDU;
    stream.set_large_sdu(use_large_sdu);

    Ok(())
}

/// Send a RESET marker after ACCEPT (mimics Python's OOB negotiation).
/// Python sends an OOB break (!) + RESET marker, but we can only send the marker
/// since tokio doesn't support MSG_OOB.
pub async fn send_reset_marker(stream: &mut PacketStream) -> Result<()> {
    // RESET marker packet: type=12 (MARKER), payload=[01, 00, 02]
    let msg = MarkerMessage::reset();
    stream.send_message(TNS_PACKET_TYPE_MARKER, &msg).await?;
    Ok(())
}

/// Handle REFUSE packet.
fn handle_refuse(packet: Packet, params: &ConnectParams) -> Result<()> {
    // Try to extract error message from refuse data
    let payload = packet.payload;
    let message = String::from_utf8_lossy(&payload).to_string();

    // Check for specific error codes
    if message.contains("ERR=12514") {
        return Err(Error::InvalidServiceName {
            service_name: params.service_name.clone(),
        });
    }
    if message.contains("ERR=12505") {
        return Err(Error::InvalidSid {
            sid: params.service_name.clone(),
        });
    }

    Err(Error::ConnectionRefused { message })
}

/// Perform FastAuth protocol/data types/auth exchange for Oracle 23ai.
/// This combines protocol, data types, and auth phase 1 into a single round-trip.
pub async fn fast_auth(
    stream: &mut PacketStream,
    caps: &mut Capabilities,
    creds: &AuthCredentials,
) -> Result<SessionData> {
    // Get client info for auth
    let pid = std::process::id().to_string();
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let osuser = whoami::username();

    // For FastAuth, use 19.1 ext 1 field version in compile caps.
    // IMPORTANT: This also affects what format the server sends in responses!
    // The server will format column metadata according to this version, not its max.
    let mut fast_auth_compile_caps = caps.compile_caps.clone();
    fast_auth_compile_caps[TNS_CCAP_FIELD_VERSION] = TNS_CCAP_FIELD_VERSION_19_1_EXT_1;
    // Update our ttc_field_version to match what we're requesting
    caps.ttc_field_version = TNS_CCAP_FIELD_VERSION_19_1_EXT_1;

    // Build FastAuth message (zero-copy)
    let msg = FastAuthMessage {
        driver_name: b"oracle-thin-rs",
        compile_caps: &fast_auth_compile_caps,
        runtime_caps: &caps.runtime_caps,
        auth: AuthPhaseOneMessage {
            username: &creds.username,
            terminal: "unknown",
            program: "oracle-thin-rs",
            machine: &hostname,
            pid: &pid,
            sid: &osuser,
        },
    };

    stream.send_data_message(&msg).await?;

    // Read FastAuth response - contains Protocol, DataTypes, and Auth responses
    let response = stream.read_packet().await?;

    if response.packet_type != TNS_PACKET_TYPE_DATA {
        return Err(Error::UnexpectedPacketType {
            expected: TNS_PACKET_TYPE_DATA,
            actual: response.packet_type,
        });
    }

    // Parse FastAuth response
    let mut rbuf = ReadBuffer::new(response.payload);
    let _data_flags = rbuf.read_u16_be()?;

    // Process embedded messages in response
    let mut session = SessionData::default();

    while rbuf.remaining() > 0 {
        let msg_type = rbuf.read_u8()?;

        match msg_type {
            TNS_MSG_TYPE_PROTOCOL => {
                // Parse protocol response
                let _server_version = rbuf.read_u8()?;
                let _zero = rbuf.read_u8()?;

                // Read server banner
                let mut banner = Vec::new();
                loop {
                    let b = rbuf.read_u8()?;
                    if b == 0 {
                        break;
                    }
                    banner.push(b);
                }

                // Read charset
                let _charset_id = rbuf.read_u16_le()?;
                let _server_flags = rbuf.read_u8()?;

                // Skip elements
                let num_elem = rbuf.read_u16_le()?;
                if num_elem > 0 {
                    rbuf.skip((num_elem * 5) as usize)?;
                }

                // FDO length and data
                let fdo_length = rbuf.read_u16_be()?;
                rbuf.skip(fdo_length as usize)?;

                // Read server caps
                if let Some(server_compile_caps) = rbuf.read_bytes_with_length()? {
                    caps.adjust_for_server_caps(&server_compile_caps, &[]);
                }
                if let Some(server_runtime_caps) = rbuf.read_bytes_with_length()? {
                    caps.adjust_for_server_caps(&[], &server_runtime_caps);
                }
            }
            TNS_MSG_TYPE_DATA_TYPES => {
                // Skip data types response
                loop {
                    let data_type = rbuf.read_u16_be()?;
                    if data_type == 0 {
                        break;
                    }
                    let conv_data_type = rbuf.read_u16_be()?;
                    if conv_data_type != 0 {
                        rbuf.skip(4)?;
                    }
                }
            }
            TNS_MSG_TYPE_PARAMETER => {
                // Parse auth parameters
                // Format: num_params (ub2), then for each param:
                //   key_indicator (ub4) + key (str_with_length)
                //   value_indicator (ub4) + value (str_with_length)
                //   flags (ub4) - for AUTH_VFR_DATA this is verifier_type
                let num_params = rbuf.read_ub2()?;
                for _ in 0..num_params {
                    // Read key (ub4 indicator + str_with_length)
                    let _ = rbuf.read_ub4()?;
                    let key = rbuf.read_str_with_length()?.unwrap_or_default();

                    // Read value (ub4 indicator + str_with_length)
                    let _ = rbuf.read_ub4()?;
                    let value = rbuf.read_str_with_length()?.unwrap_or_default();

                    if key == "AUTH_VFR_DATA" {
                        session.verifier_type = rbuf.read_ub4()?;
                    } else {
                        rbuf.skip_ub4()?;
                    }

                    session.params.insert(key, value);
                }
            }
            TNS_MSG_TYPE_ERROR => {
                // TNS_MSG_TYPE_ERROR is a complex structure - it contains status info,
                // not necessarily an actual error. The real error number comes after
                // many other fields. For FastAuth, this is typically a success status.
                //
                // Structure (simplified):
                //   call_status (ub4), end_to_end_seq (ub2), row_number (ub4),
                //   error_num (ub2), array_errors (ub2 x2), cursor_id (ub2),
                //   error_pos (sb2), sql_type (ub1), fatal (ub1), flags (ub1 x3),
                //   rowid, os_error (ub4), stmt_num (ub1), call_num (ub1),
                //   padding (ub2), success_iters (ub4), oerrdd (ub4 + optional chunked),
                //   batch_errors (ub2 + array), batch_offsets (ub4 + array),
                //   **actual_error_num (ub4)**, rowcount (ub8), error_message (str)
                //
                // For now, skip the entire error info block since FastAuth typically
                // succeeds if we got the auth parameters.

                // Skip the complex error structure - just consume remaining bytes for this message
                // In a proper implementation, we'd parse all fields and check actual_error_num
                let _call_status = rbuf.read_ub4()?;
                let _end_to_end_seq = rbuf.read_ub2()?;
                let _row_number = rbuf.read_ub4()?;
                let _error_num_hint = rbuf.read_ub2()?; // Not the real error number
                let _array_elem_err1 = rbuf.read_ub2()?;
                let _array_elem_err2 = rbuf.read_ub2()?;
                let _cursor_id = rbuf.read_ub2()?;
                let _error_pos = rbuf.read_ub2()?; // Actually sb2 but we just skip
                rbuf.skip(4)?; // sql_type, fatal, flags x2
                               // Skip rowid (variable length)
                let rowid_len = rbuf.read_u8()?;
                if rowid_len > 0 && rowid_len != 0xFF {
                    rbuf.skip(rowid_len as usize)?;
                }
                let _os_error = rbuf.read_ub4()?;
                rbuf.skip(4)?; // stmt_num, call_num, padding
                let _success_iters = rbuf.read_ub4()?;
                let oerrdd_len = rbuf.read_ub4()?;
                if oerrdd_len > 0 {
                    rbuf.skip_raw_bytes_chunked()?;
                }
                let batch_error_count = rbuf.read_ub2()?;
                if batch_error_count > 0 {
                    // Skip batch error codes
                    let first_byte = rbuf.read_u8()?;
                    for _ in 0..batch_error_count {
                        if first_byte == 0xFE {
                            rbuf.skip_ub4()?;
                        }
                        rbuf.skip(2)?; // error code
                    }
                    if first_byte == 0xFE {
                        rbuf.skip(1)?;
                    }
                }
                let batch_offset_count = rbuf.read_ub4()?;
                if batch_offset_count > 0 {
                    // Skip batch offsets
                    for _ in 0..batch_offset_count {
                        rbuf.skip_ub4()?;
                    }
                }
                // Now read the ACTUAL error number
                let actual_error_num = rbuf.read_ub4()?;
                let _rowcount = rbuf.read_ub8()?;

                if actual_error_num != 0 {
                    // There's a real error - read the message
                    let message = rbuf.read_str_with_length()?.unwrap_or_default();
                    return Err(Error::Oracle {
                        code: actual_error_num,
                        message,
                    });
                }
                // No error - continue processing
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => {
                break;
            }
            _ => {
                break;
            }
        }
    }

    Ok(session)
}

/// Read a DATA packet, handling control packets along the way.
async fn read_data_packet(stream: &mut PacketStream, caps: &mut Capabilities) -> Result<Packet> {
    loop {
        let response = stream.read_packet().await?;
        match response.packet_type {
            TNS_PACKET_TYPE_DATA => return Ok(response),
            TNS_PACKET_TYPE_CONTROL => {
                // Handle control packet
                if response.payload.len() >= 2 {
                    let control_type =
                        u16::from_be_bytes([response.payload[0], response.payload[1]]);
                    if control_type == 9 {
                        // TNS_CONTROL_TYPE_RESET_OOB
                        caps.supports_oob = false;
                    }
                }
                // Continue reading for the actual DATA packet
                continue;
            }
            _ => {
                return Err(Error::UnexpectedPacketType {
                    expected: TNS_PACKET_TYPE_DATA,
                    actual: response.packet_type,
                });
            }
        }
    }
}

/// Exchange data types and capabilities with the server (non-FastAuth path).
/// This involves sending two separate messages: ProtocolMessage and DataTypesMessage.
pub async fn exchange_data_types(stream: &mut PacketStream, caps: &mut Capabilities) -> Result<()> {
    // Step 1: Send PROTOCOL message (zero-copy)
    let protocol_msg = ProtocolMessage::default();
    stream.send_data_message(&protocol_msg).await?;

    // Read PROTOCOL response (handling any control packets)
    let response = read_data_packet(stream, caps).await?;

    // Parse PROTOCOL response
    let mut rbuf = ReadBuffer::new(response.payload);
    let _data_flags = rbuf.read_u16_be()?;
    let msg_type = rbuf.read_u8()?;

    if msg_type == TNS_MSG_TYPE_PROTOCOL {
        let _server_version = rbuf.read_u8()?;
        let _zero = rbuf.read_u8()?;

        // Read server banner
        let mut banner = Vec::new();
        loop {
            let b = rbuf.read_u8()?;
            if b == 0 {
                break;
            }
            banner.push(b);
        }

        let _charset_id = rbuf.read_u16_le()?;
        let _server_flags = rbuf.read_u8()?;

        let num_elem = rbuf.read_u16_le()?;
        if num_elem > 0 {
            rbuf.skip((num_elem * 5) as usize)?;
        }

        let fdo_length = rbuf.read_u16_be()?;
        rbuf.skip(fdo_length as usize)?;

        if let Some(server_compile_caps) = rbuf.read_bytes_with_length()? {
            // TODO: Handle server_compile_caps.len() > 7 case if needed
            caps.adjust_for_server_caps(&server_compile_caps, &[]);
        }
        if let Some(server_runtime_caps) = rbuf.read_bytes_with_length()? {
            caps.adjust_for_server_caps(&[], &server_runtime_caps);
        }
    }

    // Step 2: Send DATA_TYPES message (zero-copy)
    let data_types_msg = DataTypesMessage {
        compile_caps: &caps.compile_caps,
        runtime_caps: &caps.runtime_caps,
    };
    stream.send_data_message(&data_types_msg).await?;

    // Read DATA_TYPES response (handling any control packets)
    let response = read_data_packet(stream, caps).await?;

    let mut rbuf = ReadBuffer::new(response.payload);
    let _data_flags = rbuf.read_u16_be()?;

    // Read message type (should be TNS_MSG_TYPE_DATA_TYPES = 2)
    let msg_type = rbuf.read_u8()?;

    if msg_type != TNS_MSG_TYPE_DATA_TYPES {
        return Err(Error::protocol(format!(
            "Expected DataTypes response (type {}), got type {}",
            TNS_MSG_TYPE_DATA_TYPES, msg_type
        )));
    }

    // Parse data types array - skip data type entries until we hit 0
    loop {
        let data_type = rbuf.read_u16_be()?;
        if data_type == 0 {
            break;
        }
        let conv_data_type = rbuf.read_u16_be()?;
        if conv_data_type != 0 {
            rbuf.skip(4)?;
        }
    }

    if rbuf.remaining() > 0 {
        let _remaining = rbuf.read_bytes(rbuf.remaining())?;
    }
    Ok(())
}
