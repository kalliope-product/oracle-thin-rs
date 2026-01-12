//! TNS packet structure and I/O.

use crate::error::{Error, Result};
use crate::protocol::buffer::WriteBuffer;
use crate::protocol::constants::*;
use crate::protocol::message::{write_packet_header, DataMessage, Message};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// TNS packet header size.
pub const HEADER_SIZE: usize = 8;

/// A TNS packet.
#[derive(Debug, Clone)]
pub struct Packet {
    /// Packet type.
    pub packet_type: u8,
    /// Packet flags.
    pub packet_flags: u8,
    /// Packet payload (excluding header).
    pub payload: Bytes,
}

impl Packet {
    /// Create a new packet with the given type and payload.
    pub fn new(packet_type: u8, payload: Bytes) -> Self {
        Self {
            packet_type,
            packet_flags: 0,
            payload,
        }
    }

    /// Create a new packet with flags.
    pub fn with_flags(packet_type: u8, packet_flags: u8, payload: Bytes) -> Self {
        Self {
            packet_type,
            packet_flags,
            payload,
        }
    }

    /// Check if this is a DATA packet with end-of-response flag.
    pub fn has_end_of_response(&self) -> bool {
        if self.packet_type != TNS_PACKET_TYPE_DATA || self.payload.len() < 2 {
            return false;
        }
        let flags = u16::from_be_bytes([self.payload[0], self.payload[1]]);
        (flags & TNS_DATA_FLAGS_END_OF_RESPONSE) != 0 || (flags & TNS_DATA_FLAGS_EOF) != 0
    }

    /// Get the total packet size (header + payload).
    pub fn total_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }

    /// Serialize the packet to bytes.
    pub fn to_bytes(&self, use_large_sdu: bool) -> Bytes {
        let total_len = self.total_size();
        let mut buf = WriteBuffer::with_capacity(total_len);

        if use_large_sdu {
            // 4-byte length for large SDU
            buf.write_u32_be(total_len as u32);
        } else {
            // 2-byte length + 2-byte checksum
            buf.write_u16_be(total_len as u16);
            buf.write_u16_be(0); // Checksum (unused)
        }

        buf.write_u8(self.packet_type);
        buf.write_u8(self.packet_flags);
        buf.write_u16_be(0); // Header checksum (unused)
        buf.write_bytes(&self.payload);

        buf.freeze()
    }
}

/// TNS packet reader/writer for a TCP stream.
pub struct PacketStream {
    stream: TcpStream,
    /// Whether to use 4-byte length (large SDU) or 2-byte length.
    use_large_sdu: bool,
    /// Maximum packet size (SDU).
    sdu: u32,
    /// Partial buffer for incomplete packets.
    partial_buf: BytesMut,
}

impl PacketStream {
    /// Create a new packet stream.
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            use_large_sdu: false,
            sdu: TNS_SDU_DEFAULT,
            partial_buf: BytesMut::new(),
        }
    }

    /// Set whether to use large SDU (4-byte length).
    pub fn set_large_sdu(&mut self, use_large_sdu: bool) {
        self.use_large_sdu = use_large_sdu;
    }

    /// Set the SDU size.
    pub fn set_sdu(&mut self, sdu: u32) {
        self.sdu = sdu;
    }

    /// Get the underlying TCP stream.
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    /// Get a mutable reference to the underlying TCP stream.
    pub fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    /// Read a packet from the stream.
    pub async fn read_packet(&mut self) -> Result<Packet> {
        // Read until we have at least the header
        while self.partial_buf.len() < HEADER_SIZE {
            let mut buf = [0u8; 4096];
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                return Err(Error::ConnectionClosed);
            }
            self.partial_buf.extend_from_slice(&buf[..n]);
        }

        // Parse packet length
        let packet_len = if self.use_large_sdu {
            u32::from_be_bytes([
                self.partial_buf[0],
                self.partial_buf[1],
                self.partial_buf[2],
                self.partial_buf[3],
            ]) as usize
        } else {
            u16::from_be_bytes([self.partial_buf[0], self.partial_buf[1]]) as usize
        };
        // Read until we have the full packet
        while self.partial_buf.len() < packet_len {
            let mut buf = [0u8; 4096];
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                return Err(Error::ConnectionClosed);
            }
            self.partial_buf.extend_from_slice(&buf[..n]);
        }

        // Extract the packet
        let packet_data = self.partial_buf.split_to(packet_len);
        let packet_type = packet_data[4];
        let packet_flags = packet_data[5];
        let payload = Bytes::copy_from_slice(&packet_data[HEADER_SIZE..]);

        Ok(Packet {
            packet_type,
            packet_flags,
            payload,
        })
    }

    /// Write a packet to the stream.
    pub async fn write_packet(&mut self, packet: &Packet) -> Result<()> {
        let bytes = packet.to_bytes(self.use_large_sdu);
        // if bytes.len() > 64 {
        // }
        // eprintln!("[DEBUG] Sending  packet type {} with size {}", packet.packet_type, bytes.len());
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Send a DATA packet (legacy - use send_data_message for new code).
    pub async fn send_data(&mut self, data: Bytes, data_flags: u16) -> Result<()> {
        let mut payload = WriteBuffer::with_capacity(data.len() + 2);
        payload.write_u16_be(data_flags);
        payload.write_bytes(&data);
        let packet = Packet::new(TNS_PACKET_TYPE_DATA, payload.freeze());
        self.write_packet(&packet).await
    }

    /// Send a message as a specific packet type (zero-copy).
    ///
    /// Uses the Message trait to calculate size and serialize in a single allocation.
    pub async fn send_message<M: Message>(&mut self, packet_type: u8, msg: &M) -> Result<()> {
        let payload_size = msg.wire_size();
        let total_size = HEADER_SIZE + payload_size;

        let mut buf = Vec::with_capacity(total_size);

        // Write packet header
        write_packet_header(&mut buf, packet_type, 0, total_size, self.use_large_sdu);

        // Write message content
        msg.write_to(&mut buf)?;

        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Send a DATA message (zero-copy).
    ///
    /// Uses the DataMessage trait to include data_flags and serialize in a single allocation.
    pub async fn send_data_message<M: DataMessage>(&mut self, msg: &M) -> Result<()> {
        let payload_size = msg.data_wire_size();
        let total_size = HEADER_SIZE + payload_size;

        let mut buf = Vec::with_capacity(total_size);

        // Write packet header
        write_packet_header(
            &mut buf,
            TNS_PACKET_TYPE_DATA,
            0,
            total_size,
            self.use_large_sdu,
        );

        // Write data flags
        buf.extend_from_slice(&msg.data_flags().to_be_bytes());

        // Write message content
        msg.write_to(&mut buf)?;
        // eprintln!("[DEBUG] Sending DATA message with size {}", buf.len());
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Flush the stream.
    pub async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;
        Ok(())
    }
}

/// Capabilities for the connection.
#[derive(Debug, Clone)]
pub struct Capabilities {
    /// Protocol version.
    pub protocol_version: u16,
    /// SDU size.
    pub sdu: u32,
    /// Whether OOB (out-of-band) is supported.
    pub supports_oob: bool,
    /// Whether end-of-response is supported.
    pub supports_end_of_response: bool,
    /// Whether fast auth is supported (Oracle 23ai+).
    pub supports_fast_auth: bool,
    /// TTC field version (for parsing - may differ from server's version for FastAuth).
    pub ttc_field_version: u8,
    /// Server's actual TTC field version (determines what fields server sends).
    pub server_ttc_field_version: u8,
    /// Compile-time capabilities.
    pub compile_caps: Vec<u8>,
    /// Runtime capabilities.
    pub runtime_caps: Vec<u8>,
}

impl Capabilities {
    /// Create default capabilities.
    /// These match Python oracledb's _init_compile_caps and _init_runtime_caps.
    pub fn new() -> Self {
        let ttc_field_version = TNS_CCAP_FIELD_VERSION_MAX;

        let mut compile_caps = vec![0u8; TNS_CCAP_MAX];
        compile_caps[TNS_CCAP_SQL_VERSION] = TNS_CCAP_SQL_VERSION_MAX;
        compile_caps[TNS_CCAP_LOGON_TYPES] = TNS_CCAP_O5LOGON
            | TNS_CCAP_O5LOGON_NP
            | TNS_CCAP_O7LOGON
            | TNS_CCAP_O8LOGON_LONG_IDENTIFIER
            | TNS_CCAP_O9LOGON_LONG_PASSWORD;
        compile_caps[TNS_CCAP_FEATURE_BACKPORT] =
            TNS_CCAP_CTB_IMPLICIT_POOL | TNS_CCAP_CTB_OAUTH_MSG_ON_ERR;
        compile_caps[TNS_CCAP_FIELD_VERSION] = ttc_field_version;
        compile_caps[TNS_CCAP_SERVER_DEFINE_CONV] = 1;
        compile_caps[TNS_CCAP_DEQUEUE_WITH_SELECTOR] = 1;
        compile_caps[TNS_CCAP_TTC1] =
            TNS_CCAP_FAST_BVEC | TNS_CCAP_END_OF_CALL_STATUS | TNS_CCAP_IND_RCD;
        compile_caps[TNS_CCAP_OCI1] = TNS_CCAP_FAST_SESSION_PROPAGATE | TNS_CCAP_APP_CTX_PIGGYBACK;
        compile_caps[TNS_CCAP_TDS_VERSION] = TNS_CCAP_TDS_VERSION_MAX;
        compile_caps[TNS_CCAP_RPC_VERSION] = TNS_CCAP_RPC_VERSION_MAX;
        compile_caps[TNS_CCAP_RPC_SIG] = TNS_CCAP_RPC_SIG_VALUE;
        compile_caps[TNS_CCAP_DBF_VERSION] = TNS_CCAP_DBF_VERSION_MAX;
        compile_caps[TNS_CCAP_LOB] = TNS_CCAP_LOB_UB8_SIZE
            | TNS_CCAP_LOB_ENCS
            | TNS_CCAP_LOB_PREFETCH_DATA
            | TNS_CCAP_LOB_TEMP_SIZE
            | TNS_CCAP_LOB_PREFETCH_LENGTH
            | TNS_CCAP_LOB_12C;
        compile_caps[TNS_CCAP_UB2_DTY] = 1;
        compile_caps[TNS_CCAP_LOB2] = TNS_CCAP_LOB2_QUASI | TNS_CCAP_LOB2_2GB_PREFETCH;
        compile_caps[TNS_CCAP_TTC3] = TNS_CCAP_IMPLICIT_RESULTS
            | TNS_CCAP_BIG_CHUNK_CLR
            | TNS_CCAP_KEEP_OUT_ORDER
            | TNS_CCAP_LTXID;
        compile_caps[TNS_CCAP_TTC2] = TNS_CCAP_ZLNP;
        compile_caps[TNS_CCAP_OCI2] = TNS_CCAP_DRCP;
        compile_caps[TNS_CCAP_CLIENT_FN] = TNS_CCAP_CLIENT_FN_MAX;
        compile_caps[TNS_CCAP_SESS_SIGNATURE_VERSION] = TNS_CCAP_FIELD_VERSION_12_2;
        compile_caps[TNS_CCAP_TTC4] = TNS_CCAP_INBAND_NOTIFICATION | TNS_CCAP_EXPLICIT_BOUNDARY;
        compile_caps[TNS_CCAP_TTC5] = TNS_CCAP_VECTOR_SUPPORT
            | TNS_CCAP_TOKEN_SUPPORTED
            | TNS_CCAP_PIPELINING_SUPPORT
            | TNS_CCAP_PIPELINING_BREAK
            | TNS_CCAP_TTC5_SESSIONLESS_TXNS;
        compile_caps[TNS_CCAP_VECTOR_FEATURES] =
            TNS_CCAP_VECTOR_FEATURE_BINARY | TNS_CCAP_VECTOR_FEATURE_SPARSE;
        compile_caps[TNS_CCAP_OCI3] = TNS_CCAP_OCI3_OCSSYNC;

        let mut runtime_caps = vec![0u8; TNS_RCAP_MAX];
        runtime_caps[TNS_RCAP_COMPAT] = TNS_RCAP_COMPAT_81;
        runtime_caps[TNS_RCAP_TTC] = TNS_RCAP_TTC_ZERO_COPY | TNS_RCAP_TTC_32K;

        Self {
            protocol_version: 0,
            sdu: TNS_SDU_DEFAULT,
            // For async implementations (tokio), OOB is not supported
            // Python's asyncio implementation also disables OOB
            supports_oob: false,
            supports_end_of_response: false,
            supports_fast_auth: false,
            // Initialize to match compile_caps so adjust_for_server_caps works correctly
            ttc_field_version,
            // Will be set when we receive server caps
            server_ttc_field_version: 0,
            compile_caps,
            runtime_caps,
        }
    }

    /// Adjust capabilities based on protocol negotiation.
    pub fn adjust_for_protocol(&mut self, version: u16, _options: u16, flags2: u32) {
        self.protocol_version = version;

        // Note: For async implementations (tokio), OOB remains disabled
        // We don't enable it from flags2 because we didn't advertise it in CONNECT
        // and we don't want to follow through with the OOB handshake

        if version >= TNS_VERSION_MIN_END_OF_RESPONSE {
            self.supports_end_of_response = (flags2 & TNS_ACCEPT_FLAG_HAS_END_OF_RESPONSE) != 0;
            if self.supports_end_of_response {
                // Update compile_caps to indicate END_OF_RESPONSE support
                self.compile_caps[TNS_CCAP_TTC4] |= TNS_CCAP_END_OF_RESPONSE;
            }
        }

        // Oracle 23ai fast auth support
        self.supports_fast_auth = (flags2 & TNS_ACCEPT_FLAG_FAST_AUTH) != 0;
    }

    /// Adjust capabilities after protocol exchange.
    /// Note: Unlike a naive implementation that minimizes all values,
    /// Python only adjusts specific fields (mainly TNS_CCAP_FIELD_VERSION).
    pub fn adjust_for_server_caps(
        &mut self,
        server_compile_caps: &[u8],
        server_runtime_caps: &[u8],
    ) {
        // Track server's actual field version - this determines what fields it sends
        if !server_compile_caps.is_empty() && server_compile_caps.len() > TNS_CCAP_FIELD_VERSION {
            let server_field_version = server_compile_caps[TNS_CCAP_FIELD_VERSION];
            // Always store the server's version
            self.server_ttc_field_version = server_field_version;
            // Only adjust OUR version if server's is lower
            if server_field_version < self.ttc_field_version {
                self.ttc_field_version = server_field_version;
                self.compile_caps[TNS_CCAP_FIELD_VERSION] = server_field_version;
            }
        }

        // Check for 32K string support from runtime caps
        if !server_runtime_caps.is_empty() && server_runtime_caps.len() > TNS_RCAP_TTC {
            // The max_string_size would be 32767 if TNS_RCAP_TTC_32K is set, else 4000
            // We don't store max_string_size currently, but we could add it
        }
    }
}

impl Default for Capabilities {
    fn default() -> Self {
        Self::new()
    }
}
