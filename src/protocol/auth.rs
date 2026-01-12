//! O5LOGON authentication implementation.

use crate::error::{Error, Result};
use crate::protocol::buffer::ReadBuffer;
use crate::protocol::constants::*;
use crate::protocol::crypto::{
    bytes_to_hex_upper, decrypt_cbc, derive_key_pbkdf2, encrypt_cbc, hex_to_bytes, md5_hash,
    random_bytes, sha1_hash, sha512_hash,
};
use crate::protocol::messages::{AuthPhaseOneMessage, AuthPhaseTwoMessage, MarkerMessage};
use crate::protocol::packet::{Capabilities, Packet, PacketStream};
use bytes::Bytes;
use std::collections::HashMap;

/// Authentication credentials.
#[derive(Debug, Clone)]
pub struct AuthCredentials {
    /// Username.
    pub username: String,
    /// Password.
    pub password: String,
}

impl AuthCredentials {
    /// Create new credentials.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

/// Session data from authentication.
#[derive(Debug, Default)]
pub struct SessionData {
    /// Key-value pairs from server.
    pub params: HashMap<String, String>,
    /// Verifier type.
    pub verifier_type: u32,
    /// Combo key for encryption.
    pub combo_key: Option<Vec<u8>>,
}

/// Perform two-phase O5LOGON authentication.
pub async fn authenticate(
    stream: &mut PacketStream,
    creds: &AuthCredentials,
    caps: &Capabilities,
) -> Result<SessionData> {
    // Phase 1: Send client info, receive verifier data
    // eprintln!("[DEBUG] Starting authentication phase one");
    let mut session = phase_one(stream, creds, caps).await?;

    // Phase 2: Generate verifier and complete authentication
    // eprintln!("[DEBUG] Starting authentication phase two");
    phase_two(stream, creds, caps, &mut session).await?;

    Ok(session)
}

/// Authentication phase 1: Send client info.
async fn phase_one(
    stream: &mut PacketStream,
    creds: &AuthCredentials,
    _caps: &Capabilities,
) -> Result<SessionData> {
    // Get client info
    let pid = std::process::id().to_string();
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let osuser = whoami::username();

    // Build and send phase 1 message (zero-copy)
    let msg = AuthPhaseOneMessage {
        username: &creds.username,
        terminal: "unknown",
        program: "oracle-thin-rs",
        machine: &hostname,
        pid: &pid,
        sid: &osuser,
    };

    stream.send_data_message(&msg).await?;
    // eprintln!("[DEBUG] Sent authentication phase one message, waiting for response");
    // Read response
    let response = stream.read_packet().await?;

    // Handle marker packet (server break)
    if response.packet_type == TNS_PACKET_TYPE_MARKER {
        // eprintln!("[DEBUG] Received marker packet in phase one, handling error");
        return handle_marker_and_get_error(stream, response).await;
    }

    if response.packet_type != TNS_PACKET_TYPE_DATA {
        return Err(Error::UnexpectedPacketType {
            expected: TNS_PACKET_TYPE_DATA,
            actual: response.packet_type,
        });
    }

    // Parse phase 1 response
    parse_auth_response(response.payload)
}

/// Handle a marker packet and retrieve the error message from server.
async fn handle_marker_and_get_error(
    stream: &mut PacketStream,
    marker_packet: Packet,
) -> Result<SessionData> {
    // Parse marker type from payload
    // Format: [data_flags (2 bytes), marker_type (1 byte)]
    let _marker_type = if marker_packet.payload.len() >= 3 {
        marker_packet.payload[2]
    } else if !marker_packet.payload.is_empty() {
        marker_packet.payload[0]
    } else {
        0
    };

    // Send reset marker to recover (zero-copy)
    let msg = MarkerMessage::reset();
    stream.send_message(TNS_PACKET_TYPE_MARKER, &msg).await?;

    // Read packets until we get a reset marker back
    loop {
        let pkt = stream.read_packet().await?;

        if pkt.packet_type == TNS_PACKET_TYPE_MARKER {
            // Check if it's a reset marker
            let pkt_marker_type = if pkt.payload.len() >= 3 {
                pkt.payload[2]
            } else if !pkt.payload.is_empty() {
                pkt.payload[0]
            } else {
                0
            };
            if pkt_marker_type == TNS_MARKER_TYPE_RESET {
                break;
            }
        } else if pkt.packet_type == TNS_PACKET_TYPE_DATA {
            // Got data packet - this should contain the error
            let mut rbuf = ReadBuffer::new(pkt.payload);
            let _data_flags = rbuf.read_u16_be()?;
            let msg_type = rbuf.read_u8()?;

            if msg_type == TNS_MSG_TYPE_ERROR {
                let error = parse_error(&mut rbuf)?;
                return Err(error);
            }
            break;
        }
    }

    // Skip any remaining marker packets until we get a DATA packet with the error
    // Some servers send multiple markers before the error packet
    let mut attempts = 0;
    loop {
        attempts += 1;
        if attempts > 10 {
            break;
        }

        let pkt = stream.read_packet().await?;

        if pkt.packet_type == TNS_PACKET_TYPE_DATA {
            let mut rbuf = ReadBuffer::new(pkt.payload);
            let _data_flags = rbuf.read_u16_be()?;
            let msg_type = rbuf.read_u8()?;

            if msg_type == TNS_MSG_TYPE_ERROR {
                let error = parse_error(&mut rbuf)?;
                return Err(error);
            }
            break;
        } else if pkt.packet_type != TNS_PACKET_TYPE_MARKER {
            // Unexpected packet type
            break;
        }
        // Continue loop to skip marker packets
    }

    Err(Error::protocol(
        "Protocol error: received break marker but couldn't retrieve error",
    ))
}

/// Marker type constants
#[allow(dead_code)]
const TNS_MARKER_TYPE_BREAK: u8 = 1;
const TNS_MARKER_TYPE_RESET: u8 = 2;

/// Handle a marker packet in phase 2 and retrieve the error message from server.
async fn handle_marker_and_get_error_phase2(
    stream: &mut PacketStream,
    marker_packet: Packet,
) -> Result<()> {
    // Parse marker type from payload
    let _marker_type = if marker_packet.payload.len() >= 3 {
        marker_packet.payload[2]
    } else if !marker_packet.payload.is_empty() {
        marker_packet.payload[0]
    } else {
        0
    };

    // Send reset marker to recover (zero-copy)
    let msg = MarkerMessage::reset();
    stream.send_message(TNS_PACKET_TYPE_MARKER, &msg).await?;

    // Read packets until we get a DATA packet with the error
    for _attempts in 0..10 {
        let pkt = stream.read_packet().await?;

        if pkt.packet_type == TNS_PACKET_TYPE_DATA {
            let mut rbuf = ReadBuffer::new(pkt.payload);
            let _data_flags = rbuf.read_u16_be()?;
            let msg_type = rbuf.read_u8()?;

            if msg_type == TNS_MSG_TYPE_ERROR {
                let error = parse_error(&mut rbuf)?;
                return Err(error);
            }
            break;
        } else if pkt.packet_type == TNS_PACKET_TYPE_MARKER {
            let pkt_marker_type = if pkt.payload.len() >= 3 {
                pkt.payload[2]
            } else if !pkt.payload.is_empty() {
                pkt.payload[0]
            } else {
                0
            };
            if pkt_marker_type == TNS_MARKER_TYPE_RESET {
                continue;
            }
        }
    }

    Err(Error::protocol(
        "Authentication failed: received break marker but couldn't retrieve error",
    ))
}

/// Authentication phase 2: Send verifier.
pub async fn phase_two(
    stream: &mut PacketStream,
    creds: &AuthCredentials,
    _caps: &Capabilities,
    session: &mut SessionData,
) -> Result<()> {
    // Generate the verifier based on type
    let (session_key, speedy_key, encoded_password) = generate_verifier(creds, session)?;

    // Timezone setting
    let tz_stmt = get_timezone_statement();

    // Build and send phase 2 message (zero-copy)
    let msg = AuthPhaseTwoMessage {
        username: &creds.username,
        session_key: &session_key,
        speedy_key: speedy_key.as_deref(),
        encoded_password: &encoded_password,
        timezone_stmt: &tz_stmt,
    };

    stream.send_data_message(&msg).await?;

    // Read response
    let response = stream.read_packet().await?;

    // Handle marker packet (server break)
    if response.packet_type == TNS_PACKET_TYPE_MARKER {
        return handle_marker_and_get_error_phase2(stream, response).await;
    }

    if response.packet_type != TNS_PACKET_TYPE_DATA {
        return Err(Error::UnexpectedPacketType {
            expected: TNS_PACKET_TYPE_DATA,
            actual: response.packet_type,
        });
    }

    // Parse phase 2 response
    let response_session = parse_auth_response(response.payload)?;

    // Verify server response
    if let Some(combo_key) = &session.combo_key {
        if let Some(svr_response) = response_session.params.get("AUTH_SVR_RESPONSE") {
            let encoded = hex_to_bytes(svr_response)
                .ok_or_else(|| Error::protocol("Invalid AUTH_SVR_RESPONSE hex"))?;
            let decrypted = decrypt_cbc(combo_key, &encoded);
            if decrypted.len() < 32 || &decrypted[16..32] != b"SERVER_TO_CLIENT" {
                return Err(Error::InvalidServerResponse);
            }
        }
    }

    // Merge session data
    for (k, v) in response_session.params {
        session.params.insert(k, v);
    }

    Ok(())
}

/// Generate the verifier for authentication.
fn generate_verifier(
    creds: &AuthCredentials,
    session: &mut SessionData,
) -> Result<(String, Option<String>, String)> {
    let verifier_data = session
        .params
        .get("AUTH_VFR_DATA")
        .ok_or_else(|| Error::protocol("Missing AUTH_VFR_DATA"))?;
    let verifier_bytes =
        hex_to_bytes(verifier_data).ok_or_else(|| Error::protocol("Invalid AUTH_VFR_DATA hex"))?;

    let password = creds.password.as_bytes();

    match session.verifier_type {
        TNS_VERIFIER_TYPE_12C => generate_12c_verifier(password, &verifier_bytes, session),
        TNS_VERIFIER_TYPE_11G_1 | TNS_VERIFIER_TYPE_11G_2 => {
            generate_11g_verifier(password, &verifier_bytes, session)
        }
        _ => Err(Error::UnsupportedVerifierType {
            verifier_type: session.verifier_type,
        }),
    }
}

/// Generate 12c verifier (PBKDF2-based).
fn generate_12c_verifier(
    password: &[u8],
    verifier_data: &[u8],
    session: &mut SessionData,
) -> Result<(String, Option<String>, String)> {
    let iterations_str = session
        .params
        .get("AUTH_PBKDF2_VGEN_COUNT")
        .ok_or_else(|| Error::protocol("Missing AUTH_PBKDF2_VGEN_COUNT"))?;
    let iterations: u32 = iterations_str
        .parse()
        .map_err(|_| Error::protocol("Invalid AUTH_PBKDF2_VGEN_COUNT"))?;

    let keylen = 32usize;

    // Derive password key using PBKDF2
    let mut salt = verifier_data.to_vec();
    salt.extend_from_slice(b"AUTH_PBKDF2_SPEEDY_KEY");
    let password_key = derive_key_pbkdf2(password, &salt, 64, iterations);

    // Create password hash using SHA-512
    let mut hash_input = password_key.clone();
    hash_input.extend_from_slice(verifier_data);
    let password_hash: Vec<u8> = sha512_hash(&hash_input)[..keylen].to_vec();

    // Decrypt server's session key part
    let server_sesskey = session
        .params
        .get("AUTH_SESSKEY")
        .ok_or_else(|| Error::protocol("Missing AUTH_SESSKEY"))?;
    let server_sesskey_bytes =
        hex_to_bytes(server_sesskey).ok_or_else(|| Error::protocol("Invalid AUTH_SESSKEY hex"))?;
    let session_key_part_a = decrypt_cbc(&password_hash, &server_sesskey_bytes);

    // Generate client's session key part (same length as part_a)
    let session_key_part_b = random_bytes(session_key_part_a.len());
    let encrypted_client_key = encrypt_cbc(&password_hash, &session_key_part_b, false);

    // Session key is first 32 bytes of encrypted client key as hex (64 chars)
    // Python: self.session_key = encoded_client_key.hex().upper()[:64]
    let session_key =
        bytes_to_hex_upper(&encrypted_client_key[..32.min(encrypted_client_key.len())]);

    // Derive combo key using PBKDF2
    let csk_salt = session
        .params
        .get("AUTH_PBKDF2_CSK_SALT")
        .ok_or_else(|| Error::protocol("Missing AUTH_PBKDF2_CSK_SALT"))?;
    let csk_salt_bytes = hex_to_bytes(csk_salt)
        .ok_or_else(|| Error::protocol("Invalid AUTH_PBKDF2_CSK_SALT hex"))?;
    let sder_count_str = session
        .params
        .get("AUTH_PBKDF2_SDER_COUNT")
        .ok_or_else(|| Error::protocol("Missing AUTH_PBKDF2_SDER_COUNT"))?;
    let sder_count: u32 = sder_count_str
        .parse()
        .map_err(|_| Error::protocol("Invalid AUTH_PBKDF2_SDER_COUNT"))?;

    // temp_key = session_key_part_b[:keylen] + session_key_part_a[:keylen]
    let mut temp_key = session_key_part_b[..keylen.min(session_key_part_b.len())].to_vec();
    temp_key.extend_from_slice(&session_key_part_a[..keylen.min(session_key_part_a.len())]);
    // combo_key = PBKDF2(temp_key.hex().upper().encode(), salt, keylen, iterations)
    let temp_key_hex = bytes_to_hex_upper(&temp_key);
    let combo_key = derive_key_pbkdf2(temp_key_hex.as_bytes(), &csk_salt_bytes, keylen, sder_count);

    // Generate speedy key
    // salt = random 16 bytes
    // speedy_key = encrypt_cbc(combo_key, salt + password_key)[:80].hex().upper()
    let speedy_salt = random_bytes(16);
    let mut speedy_plaintext = speedy_salt.clone();
    speedy_plaintext.extend_from_slice(&password_key);
    let speedy_encrypted = encrypt_cbc(&combo_key, &speedy_plaintext, false);
    // Python takes first 80 bytes -> 160 hex chars
    let speedy_key = bytes_to_hex_upper(&speedy_encrypted[..80.min(speedy_encrypted.len())]);

    // Store combo key for later password encryption
    session.combo_key = Some(combo_key.clone());

    // Encrypt password
    // salt = random 16 bytes
    // password_with_salt = salt + password
    // encrypted_password = encrypt_cbc(combo_key, password_with_salt).hex().upper()
    let password_salt = random_bytes(16);
    let mut password_with_salt = password_salt;
    password_with_salt.extend_from_slice(password);
    let encrypted_password = encrypt_cbc(&combo_key, &password_with_salt, false);
    let encoded_password = bytes_to_hex_upper(&encrypted_password);

    Ok((session_key, Some(speedy_key), encoded_password))
}

/// Generate 11g verifier (SHA1-based).
fn generate_11g_verifier(
    password: &[u8],
    verifier_data: &[u8],
    session: &mut SessionData,
) -> Result<(String, Option<String>, String)> {
    // Create password hash using SHA-1
    let mut hash_input = password.to_vec();
    hash_input.extend_from_slice(verifier_data);
    let mut password_hash = sha1_hash(&hash_input).to_vec();
    password_hash.extend_from_slice(&[0u8; 4]); // Pad to 24 bytes

    // Decrypt server's session key part
    let server_sesskey = session
        .params
        .get("AUTH_SESSKEY")
        .ok_or_else(|| Error::protocol("Missing AUTH_SESSKEY"))?;
    let server_sesskey_bytes =
        hex_to_bytes(server_sesskey).ok_or_else(|| Error::protocol("Invalid AUTH_SESSKEY hex"))?;
    let session_key_part_a = decrypt_cbc(&password_hash, &server_sesskey_bytes);

    // Generate client's session key part
    let session_key_part_b = random_bytes(session_key_part_a.len());
    let encrypted_client_key = encrypt_cbc(&password_hash, &session_key_part_b, false);
    let session_key =
        bytes_to_hex_upper(&encrypted_client_key[..48.min(encrypted_client_key.len())]);

    // Derive combo key using MD5
    let key_len = 24;
    let mut xor_result = vec![0u8; key_len];
    for i in 16..40.min(session_key_part_a.len().min(session_key_part_b.len()) + 16) {
        xor_result[i - 16] = session_key_part_a[i] ^ session_key_part_b[i];
    }

    let part1 = md5_hash(&xor_result[..16]);
    let part2 = md5_hash(&xor_result[16..]);
    let mut combo_key = part1.to_vec();
    combo_key.extend_from_slice(&part2[..8]);

    // Encrypt password
    let password_salt = random_bytes(16);
    let mut password_with_salt = password_salt;
    password_with_salt.extend_from_slice(password);
    let encrypted_password = encrypt_cbc(&combo_key, &password_with_salt, false);
    let encoded_password = bytes_to_hex_upper(&encrypted_password);

    // Store combo key for later verification
    session.combo_key = Some(combo_key);

    Ok((session_key, None, encoded_password))
}

/// Parse authentication response.
pub fn parse_auth_response(payload: Bytes) -> Result<SessionData> {
    let mut buf = ReadBuffer::new(payload);
    let mut session = SessionData::default();

    // Skip data flags
    let _data_flags = buf.read_u16_be()?;

    // Read message type
    let msg_type = buf.read_u8()?;

    // Handle different message types
    match msg_type {
        TNS_MSG_TYPE_PARAMETER => {
            // Read parameters
            // Format: num_params (ub2), then for each param:
            //   key_length (ub4), key (bytes_with_length),
            //   value_length (ub4), value (bytes_with_length),
            //   flags (ub4) - or for AUTH_VFR_DATA, this is verifier_type
            let num_params = buf.read_ub2()?;
            for _i in 0..num_params {
                // Check if we have enough buffer to continue
                if buf.remaining() < 3 {
                    break;
                }

                // Read key - handle errors gracefully
                let key_length_result = buf.read_ub4();
                if key_length_result.is_err() {
                    break;
                }

                let key_result = buf.read_str_with_length();
                let key = match key_result {
                    Ok(Some(k)) => k,
                    Ok(None) => String::new(),
                    Err(_) => {
                        break;
                    }
                };

                // Read value length and value - handle errors gracefully
                if buf.read_ub4().is_err() {
                    break;
                }
                let value = match buf.read_str_with_length() {
                    Ok(Some(v)) => v,
                    Ok(None) => String::new(),
                    Err(_) => {
                        break;
                    }
                };

                if key == "AUTH_VFR_DATA" {
                    // Read verifier type - stored in flags field
                    if let Ok(verifier_type) = buf.read_ub4() {
                        session.verifier_type = verifier_type;
                    }
                } else {
                    // Skip flags (ub4 format - read and discard)
                    if buf.read_ub4().is_err() {
                        break;
                    }
                }

                // Skip keys with null terminators in the middle (binary data)
                let clean_key = key.trim_end_matches('\0').to_string();
                if !clean_key.is_empty()
                    && clean_key
                        .chars()
                        .all(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
                {
                    session.params.insert(clean_key, value);
                }
            }
        }
        TNS_MSG_TYPE_ERROR => {
            // Read error
            return Err(parse_error(&mut buf)?);
        }
        TNS_MSG_TYPE_STATUS => {
            // Status message - may contain parameters after status
            let _status = buf.read_ub4()?;
            // Check if there are more messages
            if buf.remaining() > 0 {
                // Try to parse remaining as parameters
                if let Ok(msg_type2) = buf.read_u8() {
                    if msg_type2 == TNS_MSG_TYPE_PARAMETER {
                        let num_params = buf.read_ub2()?;
                        for _ in 0..num_params {
                            // Read key
                            let _key_length = buf.read_ub4()?;
                            let key = buf.read_str_with_length()?.unwrap_or_default();

                            // Read value
                            let _value_length = buf.read_ub4()?;
                            let value = buf.read_str_with_length()?.unwrap_or_default();

                            if key == "AUTH_VFR_DATA" {
                                session.verifier_type = buf.read_ub4()?;
                            } else {
                                // Skip flags (ub4 format - read and discard)
                                let _ = buf.read_ub4()?;
                            }

                            session.params.insert(key, value);
                        }
                    }
                }
            }
        }
        _ => {
            // Unknown message type - try to continue
        }
    }

    Ok(session)
}

/// Parse an error message.
fn parse_error(buf: &mut ReadBuffer) -> Result<Error> {
    // TNS_MSG_TYPE_ERROR format is complex. Try to extract ORA-XXXXX from payload.
    let remaining = buf.as_slice();

    // Look for "ORA-" pattern in the payload
    let ora_pattern = b"ORA-";
    if let Some(pos) = remaining.windows(4).position(|w| w == ora_pattern) {
        // Found ORA- pattern, extract the message
        let msg_start = pos;
        // Find the end of the message (null terminator or end of buffer)
        let msg_end = remaining[msg_start..]
            .iter()
            .position(|&b| b == 0)
            .map(|p| msg_start + p)
            .unwrap_or(remaining.len());
        let msg_bytes = &remaining[msg_start..msg_end];
        let message = String::from_utf8_lossy(msg_bytes).to_string();

        // Try to extract error code from ORA-XXXXX
        let code = if msg_bytes.len() > 4 {
            let code_str = &msg_bytes[4..];
            let code_end = code_str
                .iter()
                .position(|&b| b == b':')
                .unwrap_or(code_str.len());
            std::str::from_utf8(&code_str[..code_end])
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0)
        } else {
            0
        };

        return Ok(Error::Oracle { code, message });
    }

    // Fallback: couldn't find ORA- pattern
    Ok(Error::Oracle {
        code: 0,
        message: "Unknown Oracle error".to_string(),
    })
}

/// Get the ALTER SESSION statement for timezone.
fn get_timezone_statement() -> String {
    // Get local timezone offset
    let now = chrono::Local::now();
    let offset = now.offset();
    let hours = offset.local_minus_utc() / 3600;
    let minutes = (offset.local_minus_utc().abs() % 3600) / 60;

    let sign = if hours >= 0 { "+" } else { "-" };
    format!(
        "ALTER SESSION SET TIME_ZONE='{}{:02}:{:02}'\0",
        sign,
        hours.abs(),
        minutes
    )
}
