//! Execute message for SQL statement execution.

use crate::error::Result;
use crate::protocol::constants::*;
use crate::protocol::message::{
    bytes_with_length_wire_size, ub4_wire_size, DataMessage, Message, WriteExt,
};

/// Field version constant for 12.2 extended features.
const TNS_CCAP_FIELD_VERSION_12_2_EXT1: u8 = 9;

/// Execute message for a SQL statement.
///
/// This is used for initial query execution. Re-execution uses a different message.
pub struct ExecuteMessage<'a> {
    /// SQL statement bytes (UTF-8 encoded).
    pub sql_bytes: &'a [u8],
    /// Cursor ID (0 for new cursor).
    pub cursor_id: u32,
    /// Whether this is a query (SELECT).
    pub is_query: bool,
    /// Number of rows to prefetch.
    pub prefetch_rows: u32,
    /// TTC field version from capabilities.
    pub ttc_field_version: u8,
}

impl<'a> ExecuteMessage<'a> {
    /// Create a new execute message for a SELECT query.
    pub fn new_query(sql: &'a str, prefetch_rows: u32, ttc_field_version: u8) -> Self {
        Self {
            sql_bytes: sql.as_bytes(),
            cursor_id: 0,
            is_query: true,
            prefetch_rows,
            ttc_field_version,
        }
    }

    /// Calculate the options flags for this execution.
    fn calc_options(&self) -> u32 {
        let mut options: u32 = 0;

        // For new cursor, always parse
        if self.cursor_id == 0 {
            options |= TNS_EXEC_OPTION_PARSE;
        }

        // For queries, add execute and fetch
        if self.is_query {
            options |= TNS_EXEC_OPTION_EXECUTE;
            if self.prefetch_rows > 0 {
                options |= TNS_EXEC_OPTION_FETCH;
            }
        }

        // Not PL/SQL
        options |= TNS_EXEC_OPTION_NOT_PLSQL;

        options
    }

    /// Calculate exec_flags (al8i4[9]).
    fn calc_exec_flags(&self) -> u32 {
        let mut exec_flags: u32 = 0;

        // For queries with SQL, set implicit resultset flag
        if self.is_query && !self.sql_bytes.is_empty() {
            exec_flags |= TNS_EXEC_FLAGS_IMPLICIT_RESULTSET;
        }

        exec_flags
    }
}

impl Message for ExecuteMessage<'_> {
    fn wire_size(&self) -> usize {
        let sql_len = self.sql_bytes.len();
        let is_new_cursor = self.cursor_id == 0;

        let mut size = 0;

        // Function header
        size += 1; // message type (TNS_MSG_TYPE_FUNCTION)
        size += 1; // function code (TNS_FUNC_EXECUTE)
        size += 1; // sequence number

        // Options and cursor
        size += ub4_wire_size(self.calc_options());
        size += ub4_wire_size(self.cursor_id);

        // SQL pointer and length (or zeros if existing cursor)
        size += 1; // pointer
        size += ub4_wire_size(if is_new_cursor { sql_len as u32 } else { 0 });

        // Vector pointer and al8i4 length
        size += 1; // pointer (vector)
        size += ub4_wire_size(13); // al8i4 array length (always 13)

        // Various pointers
        size += 1; // al8o4 pointer
        size += 1; // al8o4l pointer

        // Prefetch settings
        size += ub4_wire_size(0); // prefetch buffer size
        size += ub4_wire_size(self.prefetch_rows); // prefetch rows
        size += ub4_wire_size(TNS_MAX_LONG_LENGTH); // max long size

        // Bind pointers (no binds in this implementation)
        size += 1; // binds pointer
        size += ub4_wire_size(0); // num binds

        // More pointers
        size += 1; // al8app
        size += 1; // al8txn
        size += 1; // al8txl
        size += 1; // al8kv
        size += 1; // al8kvl

        // Define pointers (no defines for initial query)
        size += 1; // al8doac pointer
        size += ub4_wire_size(0); // num defines

        // Registration and more pointers
        size += ub4_wire_size(0); // registration id
        size += 1; // al8objlist pointer
        size += 1; // al8objlen pointer
        size += 1; // al8blv pointer
        size += ub4_wire_size(0); // al8blvl
        size += 1; // al8dnam pointer
        size += ub4_wire_size(0); // al8dnaml
        size += ub4_wire_size(0); // al8regid_msb

        // DML rowcount pointers (no DML in SELECT)
        size += 1; // al8pidmlrc pointer
        size += ub4_wire_size(0); // al8pidmlrcbl
        size += 1; // al8pidmlrcl pointer

        // 12.2+ fields
        if self.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2 {
            size += 1; // al8sqlsig pointer
            size += ub4_wire_size(0); // SQL signature length
            size += 1; // SQL ID pointer
            size += ub4_wire_size(0); // SQL ID size
            size += 1; // SQL ID length pointer

            // 12.2 EXT1 fields
            if self.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2_EXT1 {
                size += 1; // chunk ids pointer
                size += ub4_wire_size(0); // num chunk ids
            }
        }

        // SQL bytes (for new cursor)
        if is_new_cursor {
            size += bytes_with_length_wire_size(sql_len);
        }

        // al8i4 array (13 elements)
        // [0] parse flag
        size += ub4_wire_size(if is_new_cursor { 1 } else { 0 });
        // [1] execution count (0 for new query)
        size += ub4_wire_size(0);
        // [2-4] zeros
        size += ub4_wire_size(0);
        size += ub4_wire_size(0);
        size += ub4_wire_size(0);
        // [5-6] SCN
        size += ub4_wire_size(0);
        size += ub4_wire_size(0);
        // [7] is_query flag
        size += ub4_wire_size(if self.is_query { 1 } else { 0 });
        // [8] zero
        size += ub4_wire_size(0);
        // [9] exec_flags
        size += ub4_wire_size(self.calc_exec_flags());
        // [10] fetch orientation
        size += ub4_wire_size(0);
        // [11] fetch pos
        size += ub4_wire_size(0);
        // [12] zero
        size += ub4_wire_size(0);

        size
    }

    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        let sql_len = self.sql_bytes.len();
        let is_new_cursor = self.cursor_id == 0;
        let options = self.calc_options();

        // Function header
        buf.write_u8(TNS_MSG_TYPE_FUNCTION);
        buf.write_u8(TNS_FUNC_EXECUTE);
        buf.write_u8(1); // sequence number

        // Options and cursor
        buf.write_ub4(options);
        buf.write_ub4(self.cursor_id);

        // SQL pointer and length
        if is_new_cursor {
            buf.write_u8(1); // has SQL
            buf.write_ub4(sql_len as u32);
        } else {
            buf.write_u8(0);
            buf.write_ub4(0);
        }

        // Vector pointer and al8i4 length
        buf.write_u8(1); // pointer (vector)
        buf.write_ub4(13); // al8i4 array length

        // Various pointers
        buf.write_u8(0); // al8o4 pointer
        buf.write_u8(0); // al8o4l pointer

        // Prefetch settings
        buf.write_ub4(0); // prefetch buffer size
        buf.write_ub4(self.prefetch_rows); // prefetch rows
        buf.write_ub4(TNS_MAX_LONG_LENGTH); // max long size

        // Bind pointers (no binds)
        buf.write_u8(0); // binds pointer
        buf.write_ub4(0); // num binds

        // More pointers
        buf.write_u8(0); // al8app
        buf.write_u8(0); // al8txn
        buf.write_u8(0); // al8txl
        buf.write_u8(0); // al8kv
        buf.write_u8(0); // al8kvl

        // Define pointers
        buf.write_u8(0); // al8doac pointer
        buf.write_ub4(0); // num defines

        // Registration and more pointers
        buf.write_ub4(0); // registration id
        buf.write_u8(0); // al8objlist pointer
        buf.write_u8(1); // al8objlen pointer (must be 1 per Python)
        buf.write_u8(0); // al8blv pointer
        buf.write_ub4(0); // al8blvl
        buf.write_u8(0); // al8dnam pointer
        buf.write_ub4(0); // al8dnaml
        buf.write_ub4(0); // al8regid_msb

        // DML rowcount pointers
        buf.write_u8(0); // al8pidmlrc pointer
        buf.write_ub4(0); // al8pidmlrcbl
        buf.write_u8(0); // al8pidmlrcl pointer

        // 12.2+ fields
        if self.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2 {
            buf.write_u8(0); // al8sqlsig pointer
            buf.write_ub4(0); // SQL signature length
            buf.write_u8(0); // SQL ID pointer
            buf.write_ub4(0); // SQL ID size
            buf.write_u8(0); // SQL ID length pointer

            // 12.2 EXT1 fields
            if self.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2_EXT1 {
                buf.write_u8(0); // chunk ids pointer
                buf.write_ub4(0); // num chunk ids
            }
        }

        // SQL bytes (for new cursor)
        if is_new_cursor {
            buf.write_bytes_with_length(self.sql_bytes);
        }

        // al8i4 array
        // [0] parse flag
        buf.write_ub4(if is_new_cursor { 1 } else { 0 });
        // [1] execution count (0 for new query)
        buf.write_ub4(0);
        // [2-4] zeros
        buf.write_ub4(0);
        buf.write_ub4(0);
        buf.write_ub4(0);
        // [5-6] SCN
        buf.write_ub4(0);
        buf.write_ub4(0);
        // [7] is_query flag
        buf.write_ub4(if self.is_query { 1 } else { 0 });
        // [8] zero
        buf.write_ub4(0);
        // [9] exec_flags
        buf.write_ub4(self.calc_exec_flags());
        // [10] fetch orientation
        buf.write_ub4(0);
        // [11] fetch pos
        buf.write_ub4(0);
        // [12] zero
        buf.write_ub4(0);

        Ok(())
    }
}

impl DataMessage for ExecuteMessage<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_message_wire_size() {
        let msg = ExecuteMessage::new_query("SELECT 'hello' FROM DUAL", 100, 12);

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
    }

    #[test]
    fn test_execute_message_options() {
        let msg = ExecuteMessage::new_query("SELECT 1 FROM DUAL", 100, 12);
        let options = msg.calc_options();

        // Should have PARSE, EXECUTE, FETCH, NOT_PLSQL
        assert!(options & TNS_EXEC_OPTION_PARSE != 0);
        assert!(options & TNS_EXEC_OPTION_EXECUTE != 0);
        assert!(options & TNS_EXEC_OPTION_FETCH != 0);
        assert!(options & TNS_EXEC_OPTION_NOT_PLSQL != 0);
    }
}
