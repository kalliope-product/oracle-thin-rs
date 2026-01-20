//! Execute message for SQL statement execution.

use crate::error::Result;
use crate::protocol::constants::*;
use crate::protocol::message::{
    bytes_with_length_wire_size, ub2_wire_size, ub4_wire_size, ub8_wire_size, DataMessage, Message,
    WriteExt,
};
use crate::protocol::types::FetchVarImpl;

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
    /// sequence number
    sequence_number: u8,
    /// Whether this is a query (SELECT).
    pub is_query: bool,
    /// Whether to only parse the statement (no execute).
    pub parse_only: bool,
    /// Number of rows to prefetch.
    pub prefetch_rows: u32,
    /// TTC field version from capabilities.
    pub ttc_field_version: u8,
    /// Fetch variable definitions (column metadata for defines).
    /// When set, TNS_EXEC_OPTION_DEFINE is included in options.
    pub fetch_var_impls: Option<&'a [FetchVarImpl]>,
}

impl<'a> ExecuteMessage<'a> {
    /// Create a new execute message for a SELECT query.
    ///
    /// This sends PARSE+EXECUTE+FETCH in a single message.
    /// Note: For queries with LOB columns, use `new_parse_execute()` followed
    /// by `new_define_fetch()` instead.
    pub fn new_query(sql: &'a str, prefetch_rows: u32, ttc_field_version: u8) -> Self {
        Self {
            sql_bytes: sql.as_bytes(),
            cursor_id: 0,
            is_query: true,
            sequence_number: 1,
            prefetch_rows,
            ttc_field_version,
            fetch_var_impls: None,
            parse_only: false,
        }
    }

    /// Create an execute message for a define operation on an existing cursor.
    ///
    /// This is used when re-executing a query with column definitions (e.g., for LOB prefetch).
    pub fn new_define(
        cursor_id: u32,
        fetch_var_impls: &'a [FetchVarImpl],
        prefetch_rows: u32,
        ttc_field_version: u8,
    ) -> Self {
        Self {
            sql_bytes: &[],
            cursor_id,
            is_query: true,
            sequence_number: 2,
            prefetch_rows: prefetch_rows,
            ttc_field_version,
            fetch_var_impls: Some(fetch_var_impls),
            parse_only: false,
        }
    }

    /// Calculate the options flags for this execution.
    fn calc_options(&self) -> u32 {
        let mut options: u32 = 0;
        if self.fetch_var_impls.is_some() {
            options |= TNS_EXEC_OPTION_DEFINE;
        } else if !self.parse_only {
            options |= TNS_EXEC_OPTION_EXECUTE
        }
        if self.cursor_id == 0 {
            // or stmt._is_ddl:
            options |= TNS_EXEC_OPTION_PARSE;
        }
        // Add Describe if parsing only
        if self.parse_only {
            options |= TNS_EXEC_OPTION_DESCRIBE;
        }

        // Add fetch if we have prefetch rows and either:
        // - we have defines (define+fetch), or
        // - this is a new cursor query (parse+execute+fetch)
        if self.is_query && self.prefetch_rows > 0 && self.fetch_var_impls.is_none() {
            options |= TNS_EXEC_OPTION_FETCH;
        }

        // Not PL/SQL
        if !self.parse_only {
            options |= TNS_EXEC_OPTION_NOT_PLSQL;
        }

        options
    }

    /// Calculate exec_flags (al8i4[9]).
    fn calc_exec_flags(&self) -> u32 {
        let mut exec_flags: u32 = 0;

        // For queries with SQL, set implicit resultset flag
        if self.is_query && !self.sql_bytes.is_empty() && !self.parse_only {
            exec_flags |= TNS_EXEC_FLAGS_IMPLICIT_RESULTSET;
        }

        exec_flags
    }

    /// Calculate the wire size for column metadata (fetch_var_impls).
    fn column_metadata_wire_size(&self) -> usize {
        let fetch_vars = match &self.fetch_var_impls {
            Some(vars) => vars,
            None => return 0,
        };

        let mut size = 0;
        for var in fetch_vars.iter() {
            size += 1; // ora_type_num
            size += 1; // flags
            size += 1; // precision (always 0)
            size += 1; // scale (always 0)
            size += ub4_wire_size(var.buffer_size);
            size += ub4_wire_size(var.max_num_elements);
            size += ub8_wire_size(var.cont_flag);
            size += ub4_wire_size(0); // OID length (always 0, no object types)
            size += ub2_wire_size(0); // OID version (ub2 when no OID)
            size += ub2_wire_size(var.charset_id); // charset_id (ub2)
            size += 1; // charset_form
            size += ub4_wire_size(var.lob_prefetch_length);
            if self.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2 {
                size += ub4_wire_size(0); // oaccolid
            }
        }
        size
    }

    /// Write column metadata (fetch_var_impls) to the buffer.
    ///
    /// Format per column (from Python `_write_column_metadata`):
    /// - ora_type_num: u8
    /// - flags: u8
    /// - precision: u8 (always 0)
    /// - scale: u8 (always 0)
    /// - buffer_size: ub4
    /// - max_num_elements: ub4
    /// - cont_flag: ub8
    /// - OID length: ub4 (0 if no object)
    /// - OID version: ub2 (when no OID)
    /// - charset_id: ub2
    /// - charset_form: u8
    /// - lob_prefetch_length: ub4
    /// - oaccolid: ub4 (if ttc_field_version >= 12.2)
    fn write_column_metadata(&self, buf: &mut Vec<u8>) {
        let fetch_vars = match &self.fetch_var_impls {
            Some(vars) => vars,
            None => return,
        };

        for var in fetch_vars.iter() {
            buf.write_u8(var.ora_type_num);
            buf.write_u8(var.flags);
            buf.write_u8(0); // precision (always 0)
            buf.write_u8(0); // scale (always 0)
            buf.write_ub4(var.buffer_size);
            buf.write_ub4(var.max_num_elements);
            buf.write_ub8(var.cont_flag);
            buf.write_ub4(0); // OID length (no object types supported yet)
            buf.write_ub2(0); // OID version (ub2 when no OID)
            buf.write_ub2(var.charset_id);
            buf.write_u8(var.charset_form);
            buf.write_ub4(var.lob_prefetch_length);
            if self.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2 {
                buf.write_ub4(0); // oaccolid
            }
        }
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

        // Define pointers
        size += 1; // al8doac pointer
        let num_defines = self.fetch_var_impls.map_or(0, |v| v.len() as u32);
        size += ub4_wire_size(num_defines);

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
        // [1] execution count: 0 for new cursor, prefetch_rows for existing cursor
        let exec_count = if is_new_cursor { 0 } else { self.prefetch_rows };
        size += ub4_wire_size(exec_count);
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

        // Column metadata (for defines)
        size += self.column_metadata_wire_size();

        size
    }

    fn write_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        let sql_len = self.sql_bytes.len();
        let is_new_cursor = self.cursor_id == 0;
        let options = self.calc_options();

        // Function header
        buf.write_u8(TNS_MSG_TYPE_FUNCTION);
        buf.write_u8(TNS_FUNC_EXECUTE);
        buf.write_u8(self.sequence_number); // sequence number
        if self.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1 {
            buf.write_u8(0); // extended sequence number
        }

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
        if let Some(fetch_vars) = &self.fetch_var_impls {
            buf.write_u8(1); // al8doac pointer (has defines)
            buf.write_ub4(fetch_vars.len() as u32);
        } else {
            buf.write_u8(0); // al8doac pointer (no defines)
            buf.write_ub4(0);
        }

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
        // [1] execution count: 0 for new cursor, prefetch_rows for existing cursor
        let exec_count = if is_new_cursor { 0 } else { self.prefetch_rows };
        buf.write_ub4(exec_count);
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

        // Column metadata (for defines)
        self.write_column_metadata(buf);

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

    #[test]
    fn test_execute_with_defines() {
        let fetch_vars = vec![
            FetchVarImpl::new(ORA_TYPE_NUM_VARCHAR as u8, 100, 1),
            FetchVarImpl::new_lob(ORA_TYPE_NUM_CLOB as u8, 4000, TNS_DEFAULT_LOB_PREFETCH_SIZE),
        ];

        let msg = ExecuteMessage::new_define(1, &fetch_vars, 100, 12);

        // Options should have DEFINE and FETCH, but not PARSE or EXECUTE
        let options = msg.calc_options();
        assert!(options & TNS_EXEC_OPTION_DEFINE != 0);
        assert!(options & TNS_EXEC_OPTION_FETCH == 0);
        assert!(options & TNS_EXEC_OPTION_PARSE == 0);
        assert!(options & TNS_EXEC_OPTION_EXECUTE == 0);
        assert!(options & TNS_EXEC_OPTION_NOT_PLSQL != 0);
    }

    #[test]
    fn test_execute_with_defines_wire_size() {
        let fetch_vars = vec![
            FetchVarImpl::new(ORA_TYPE_NUM_VARCHAR as u8, 100, 1),
            FetchVarImpl::new_lob(ORA_TYPE_NUM_CLOB as u8, 4000, TNS_DEFAULT_LOB_PREFETCH_SIZE),
        ];

        let msg = ExecuteMessage::new_define(1, &fetch_vars, 100, 12);

        let mut buf = Vec::with_capacity(msg.wire_size());
        msg.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), msg.wire_size());
    }
}
