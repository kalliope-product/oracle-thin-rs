//! Authentication-related TNS messages.

use super::data_types;
use crate::error::Result;
use crate::protocol::constants::*;
use crate::protocol::message::{
    bytes_with_length_wire_size, key_value_wire_size, ub4_wire_size, DataMessage, Message, WriteExt,
};

// ============================================================================
// AuthPhaseOneMessage - Authentication Phase 1 (TNS_FUNC_AUTH_PHASE_ONE)
// ============================================================================

/// Authentication phase 1 message.
///
/// Sends client info (terminal, program, machine, pid, sid) to server.
/// Server responds with verifier data (AUTH_VFR_DATA, AUTH_SESSKEY, etc.)
pub struct AuthPhaseOneMessage<'a> {
    /// Username
    pub username: &'a str,
    /// Terminal name
    pub terminal: &'a str,
    /// Program name
    pub program: &'a str,
    /// Machine/hostname
    pub machine: &'a str,
    /// Process ID
    pub pid: &'a str,
    /// Session ID (OS username)
    pub sid: &'a str,
}

impl Message for AuthPhaseOneMessage<'_> {
    fn wire_size(&self) -> usize {
        let has_user = !self.username.is_empty();
        let user_bytes_len = self.username.len();

        let mut size = 0;
        size += 1; // message type (TNS_MSG_TYPE_FUNCTION)
        size += 1; // function code (TNS_FUNC_AUTH_PHASE_ONE)
        size += 1; // sequence number
        size += 1; // user presence flag
        size += ub4_wire_size(user_bytes_len as u32);
        size += ub4_wire_size(TNS_AUTH_MODE_LOGON);
        size += 1; // pointer to key/value pairs
        size += ub4_wire_size(5); // num_pairs
        size += 1; // authivl pointer
        size += 1; // authovln pointer

        if has_user {
            size += bytes_with_length_wire_size(user_bytes_len);
        }

        // Key-value pairs
        size += key_value_wire_size("AUTH_TERMINAL", self.terminal, 0);
        size += key_value_wire_size("AUTH_PROGRAM_NM", self.program, 0);
        size += key_value_wire_size("AUTH_MACHINE", self.machine, 0);
        size += key_value_wire_size("AUTH_PID", self.pid, 0);
        size += key_value_wire_size("AUTH_SID", self.sid, 0);

        size
    }

    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        let has_user = !self.username.is_empty();
        let user_bytes = self.username.as_bytes();

        buf.write_u8(TNS_MSG_TYPE_FUNCTION);
        buf.write_u8(TNS_FUNC_AUTH_PHASE_ONE);
        buf.write_u8(1); // sequence number

        buf.write_u8(if has_user { 1 } else { 0 });
        buf.write_ub4(user_bytes.len() as u32);
        buf.write_ub4(TNS_AUTH_MODE_LOGON);

        buf.write_u8(1); // pointer to key/value pairs
        buf.write_ub4(5); // num_pairs
        buf.write_u8(1); // authivl pointer
        buf.write_u8(1); // authovln pointer

        if has_user {
            buf.write_bytes_with_length(user_bytes);
        }

        // Key-value pairs
        buf.write_key_value("AUTH_TERMINAL", self.terminal, 0);
        buf.write_key_value("AUTH_PROGRAM_NM", self.program, 0);
        buf.write_key_value("AUTH_MACHINE", self.machine, 0);
        buf.write_key_value("AUTH_PID", self.pid, 0);
        buf.write_key_value("AUTH_SID", self.sid, 0);

        Ok(())
    }
}

impl DataMessage for AuthPhaseOneMessage<'_> {}

// ============================================================================
// AuthPhaseTwoMessage - Authentication Phase 2 (TNS_FUNC_AUTH_PHASE_TWO)
// ============================================================================

/// Authentication phase 2 message.
///
/// Sends encrypted password and session key to complete authentication.
pub struct AuthPhaseTwoMessage<'a> {
    /// Username
    pub username: &'a str,
    /// Encrypted session key (hex string)
    pub session_key: &'a str,
    /// Speedy key (for 12c verifier, hex string)
    pub speedy_key: Option<&'a str>,
    /// Encrypted password (hex string)
    pub encoded_password: &'a str,
    /// Timezone ALTER SESSION statement
    pub timezone_stmt: &'a str,
}

impl Message for AuthPhaseTwoMessage<'_> {
    fn wire_size(&self) -> usize {
        let has_user = !self.username.is_empty();
        let user_bytes_len = self.username.len();
        let auth_mode = TNS_AUTH_MODE_LOGON | TNS_AUTH_MODE_WITH_PASSWORD;

        let mut num_pairs = 6u32;
        if self.speedy_key.is_some() {
            num_pairs += 1;
        }

        let mut size = 0;
        size += 1; // message type
        size += 1; // function code
        size += 1; // sequence number
        size += 1; // user presence flag
        size += ub4_wire_size(user_bytes_len as u32);
        size += ub4_wire_size(auth_mode);
        size += 1; // pointer to key/value pairs
        size += ub4_wire_size(num_pairs);
        size += 1; // authivl pointer
        size += 1; // authovln pointer

        if has_user {
            size += bytes_with_length_wire_size(user_bytes_len);
        }

        // Key-value pairs
        size += key_value_wire_size("AUTH_SESSKEY", self.session_key, 1);
        if let Some(sk) = self.speedy_key {
            size += key_value_wire_size("AUTH_PBKDF2_SPEEDY_KEY", sk, 0);
        }
        size += key_value_wire_size("AUTH_PASSWORD", self.encoded_password, 0);
        size += key_value_wire_size("SESSION_CLIENT_CHARSET", "873", 0);
        size += key_value_wire_size("SESSION_CLIENT_DRIVER_NAME", "oracle-thin-rs : 0.1.0", 0);
        size += key_value_wire_size("SESSION_CLIENT_VERSION", "185599488", 0);
        size += key_value_wire_size("AUTH_ALTER_SESSION", self.timezone_stmt, 1);

        size
    }

    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        let has_user = !self.username.is_empty();
        let user_bytes = self.username.as_bytes();
        let auth_mode = TNS_AUTH_MODE_LOGON | TNS_AUTH_MODE_WITH_PASSWORD;

        let mut num_pairs = 6u32;
        if self.speedy_key.is_some() {
            num_pairs += 1;
        }

        buf.write_u8(TNS_MSG_TYPE_FUNCTION);
        buf.write_u8(TNS_FUNC_AUTH_PHASE_TWO);
        buf.write_u8(2); // sequence number

        buf.write_u8(if has_user { 1 } else { 0 });
        buf.write_ub4(user_bytes.len() as u32);
        buf.write_ub4(auth_mode);

        buf.write_u8(1); // pointer to key/value pairs
        buf.write_ub4(num_pairs);
        buf.write_u8(1); // authivl pointer
        buf.write_u8(1); // authovln pointer

        if has_user {
            buf.write_bytes_with_length(user_bytes);
        }

        // Key-value pairs
        buf.write_key_value("AUTH_SESSKEY", self.session_key, 1);
        if let Some(sk) = self.speedy_key {
            buf.write_key_value("AUTH_PBKDF2_SPEEDY_KEY", sk, 0);
        }
        buf.write_key_value("AUTH_PASSWORD", self.encoded_password, 0);
        buf.write_key_value("SESSION_CLIENT_CHARSET", "873", 0);
        buf.write_key_value("SESSION_CLIENT_DRIVER_NAME", "oracle-thin-rs : 0.1.0", 0);
        buf.write_key_value("SESSION_CLIENT_VERSION", "185599488", 0);
        buf.write_key_value("AUTH_ALTER_SESSION", self.timezone_stmt, 1);

        Ok(())
    }
}

impl DataMessage for AuthPhaseTwoMessage<'_> {}

// ============================================================================
// FastAuthMessage - Combined fast auth for Oracle 23ai
// ============================================================================

/// FastAuth message (Oracle 23ai+).
///
/// Combines protocol negotiation, data types, and auth phase 1 into a single message.
pub struct FastAuthMessage<'a> {
    /// Protocol message fields
    pub driver_name: &'a [u8],
    /// Compile-time capabilities
    pub compile_caps: &'a [u8],
    /// Runtime capabilities
    pub runtime_caps: &'a [u8],
    /// Auth phase 1 fields
    pub auth: AuthPhaseOneMessage<'a>,
}

impl Message for FastAuthMessage<'_> {
    fn wire_size(&self) -> usize {
        let mut size = 0;

        // FastAuth header
        size += 1; // message type (TNS_MSG_TYPE_FAST_AUTH)
        size += 1; // fast auth version
        size += 1; // flag 1
        size += 1; // flag 2

        // Embedded Protocol message
        size += 1; // message type
        size += 1; // protocol version
        size += 1; // array terminator
        size += self.driver_name.len();
        size += 1; // null terminator

        // Server charset info
        size += 2; // server charset
        size += 1; // server charset flag
        size += 2; // server ncharset

        // TTC field version
        size += 1;

        // Embedded DataTypes message
        size += 1; // message type
        size += 2; // charset
        size += 2; // ncharset
        size += 1; // encoding flags
        size += bytes_with_length_wire_size(self.compile_caps.len());
        size += bytes_with_length_wire_size(self.runtime_caps.len());
        size += data_types::data_types_array_wire_size();

        // Embedded Auth phase 1
        size += self.auth.wire_size();

        size
    }

    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        // FastAuth header
        buf.write_u8(TNS_MSG_TYPE_FAST_AUTH);
        buf.write_u8(1); // fast auth version
        buf.write_u8(TNS_SERVER_CONVERTS_CHARS); // flag 1
        buf.write_u8(0); // flag 2

        // Embedded Protocol message
        buf.write_u8(TNS_MSG_TYPE_PROTOCOL);
        buf.write_u8(6); // protocol version
        buf.write_u8(0); // array terminator
        buf.write_bytes(self.driver_name);
        buf.write_u8(0); // null terminator

        // Server charset info (unused, zeros)
        buf.write_u16_be(0); // server charset
        buf.write_u8(0); // server charset flag
        buf.write_u16_be(0); // server ncharset

        // TTC field version
        buf.write_u8(TNS_CCAP_FIELD_VERSION_19_1_EXT_1);

        // Embedded DataTypes message
        buf.write_u8(TNS_MSG_TYPE_DATA_TYPES);
        buf.write_u16_le(TNS_CHARSET_UTF8);
        buf.write_u16_le(TNS_CHARSET_UTF8);
        buf.write_u8(TNS_ENCODING_MULTI_BYTE | TNS_ENCODING_CONV_LENGTH);
        buf.write_bytes_with_length(self.compile_caps);
        buf.write_bytes_with_length(self.runtime_caps);
        data_types::write_data_types_array(buf);

        // Embedded Auth phase 1
        self.auth.write_to(buf)?;

        Ok(())
    }
}

impl DataMessage for FastAuthMessage<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_phase_one_wire_size() {
        let msg = AuthPhaseOneMessage {
            username: "test_user",
            terminal: "unknown",
            program: "oracle-thin-rs",
            machine: "localhost",
            pid: "12345",
            sid: "testuser",
        };

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
    }

    #[test]
    fn test_auth_phase_two_wire_size() {
        let msg = AuthPhaseTwoMessage {
            username: "test_user",
            session_key: "ABCD1234",
            speedy_key: Some("EFGH5678"),
            encoded_password: "ENCRYPTED_PASSWORD_HEX",
            timezone_stmt: "ALTER SESSION SET TIME_ZONE='+00:00'\0",
        };

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
    }

    #[test]
    fn test_auth_phase_two_no_speedy_key() {
        let msg = AuthPhaseTwoMessage {
            username: "test_user",
            session_key: "ABCD1234",
            speedy_key: None,
            encoded_password: "ENCRYPTED_PASSWORD_HEX",
            timezone_stmt: "ALTER SESSION SET TIME_ZONE='+00:00'\0",
        };

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
    }

    #[test]
    fn test_fast_auth_wire_size() {
        let compile_caps = vec![0u8; 64];
        let runtime_caps = vec![0u8; 16];

        let msg = FastAuthMessage {
            driver_name: b"oracle-thin-rs",
            compile_caps: &compile_caps,
            runtime_caps: &runtime_caps,
            auth: AuthPhaseOneMessage {
                username: "test_user",
                terminal: "unknown",
                program: "oracle-thin-rs",
                machine: "localhost",
                pid: "12345",
                sid: "testuser",
            },
        };

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
    }
}
