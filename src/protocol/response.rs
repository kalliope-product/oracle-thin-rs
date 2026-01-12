//! Response parsing for query execution.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::protocol::buffer::ReadBuffer;
use crate::protocol::constants::*;
use crate::protocol::decode::{decode_oracle_date, decode_oracle_number};
use crate::protocol::types::{ColumnInfo, ColumnMetadata, OracleValue, Row};

/// Information extracted from error/end-of-call response.
#[derive(Debug, Default)]
pub struct ErrorInfo {
    /// Error number (0 = success).
    pub error_num: u32,
    /// Cursor ID assigned by server.
    pub cursor_id: u16,
    /// Row count for queries/DML.
    pub row_count: u64,
    /// Error message (if any).
    pub message: Option<String>,
}

/// Result from parsing an execute response.
#[derive(Debug)]
pub struct ExecuteResponse {
    /// Column metadata (for queries).
    pub columns: Vec<ColumnMetadata>,
    /// Prefetched rows.
    pub rows: Vec<Row>,
    /// Error/status information.
    pub error_info: ErrorInfo,
    /// Whether there are more rows to fetch.
    pub more_rows: bool,
}

impl ExecuteResponse {
    /// Create a new empty execute response.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            error_info: ErrorInfo::default(),
            more_rows: false,
        }
    }
}

impl Default for ExecuteResponse {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse the response from an execute message.
///
/// Reads from the buffer after the data flags (2 bytes already consumed).
///
/// # Arguments
/// * `buf` - The read buffer
/// * `ttc_field_version` - Field version for column metadata parsing (what we requested)
/// * `server_ttc_field_version` - Server's actual field version (determines error info format)
pub fn parse_execute_response(
    buf: &mut ReadBuffer,
    ttc_field_version: u8,
    server_ttc_field_version: u8,
) -> Result<ExecuteResponse> {
    let mut response = ExecuteResponse::new();
    let mut end_of_response = false;
    let mut num_columns: usize = 0;
    let mut column_info: Option<Arc<ColumnInfo>> = None;

    while buf.remaining() > 0 && !end_of_response {
        let msg_type = buf.read_u8()?;
        eprintln!("[DEBUG] msg_type={}, remaining={}", msg_type, buf.remaining());

        match msg_type {
            TNS_MSG_TYPE_DESCRIBE_INFO => {
                // Skip raw bytes before describe info (server sends additional data)
                buf.skip_raw_bytes_chunked()?;
                parse_describe_info(buf, &mut response, ttc_field_version)?;
                num_columns = response.columns.len();
                // Create shared column info for all rows
                column_info = Some(Arc::new(ColumnInfo::from_metadata(&response.columns)?));
            }
            TNS_MSG_TYPE_ROW_HEADER => {
                parse_row_header(buf)?;
            }
            TNS_MSG_TYPE_ROW_DATA => {
                // column_info should be set after DESCRIBE_INFO
                let info = column_info
                    .clone()
                    .ok_or_else(|| Error::protocol("Row data received before column metadata"))?;
                parse_row_data(buf, &response.columns, info, &mut response.rows)?;
            }
            TNS_MSG_TYPE_ERROR => {
                // Use server's field version to determine error info format
                parse_error_info(buf, &mut response.error_info, server_ttc_field_version)?;
                eprintln!("[DEBUG] error_info: error_num={}, cursor_id={}, row_count={}",
                    response.error_info.error_num, response.error_info.cursor_id, response.error_info.row_count);
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => {
                end_of_response = true;
            }
            TNS_MSG_TYPE_PARAMETER => {
                // Process return parameters (from Python's _process_return_parameters)
                parse_return_parameters(buf)?;
            }
            TNS_MSG_TYPE_BIT_VECTOR => {
                // Bit vector for duplicate column detection (for performance optimization)
                // We skip it as we're not implementing duplicate detection yet
                parse_bit_vector(buf, num_columns)?;
            }
            TNS_MSG_TYPE_STATUS => {
                // Simple status message (alternative to ERROR in some flows)
                parse_status_info(buf)?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                // Server-sent state updates (session changes, transaction IDs, etc.)
                parse_server_side_piggyback(buf)?;
            }
            _ => {
                return Err(Error::protocol(format!(
                    "Unexpected message type in execute response: {}",
                    msg_type
                )));
            }
        }
    }

    // Determine if there are more rows based on error info
    // Error 1403 (ORA-01403: no data found) means no more rows
    if response.error_info.error_num == 0 || response.error_info.error_num == 1403 {
        response.more_rows = response.error_info.error_num == 0;
    }

    Ok(response)
}

/// Result from parsing a fetch response.
#[derive(Debug)]
pub struct FetchResponse {
    /// Fetched rows.
    pub rows: Vec<Row>,
    /// Error/status information.
    pub error_info: ErrorInfo,
    /// Whether there are more rows to fetch.
    pub more_rows: bool,
}

impl FetchResponse {
    /// Create a new empty fetch response.
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            error_info: ErrorInfo::default(),
            more_rows: false,
        }
    }
}

impl Default for FetchResponse {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse the response from a fetch message.
///
/// Unlike execute response, fetch response doesn't include DESCRIBE_INFO
/// since column metadata was already received in the execute response.
pub fn parse_fetch_response(
    buf: &mut ReadBuffer,
    columns: &[ColumnMetadata],
    server_ttc_field_version: u8,
) -> Result<FetchResponse> {
    let mut response = FetchResponse::new();
    let mut end_of_response = false;
    let num_columns = columns.len();
    // Create shared column info for all rows
    let column_info = Arc::new(ColumnInfo::from_metadata(columns)?);

    while buf.remaining() > 0 && !end_of_response {
        let msg_type = buf.read_u8()?;

        match msg_type {
            TNS_MSG_TYPE_ROW_HEADER => {
                parse_row_header(buf)?;
            }
            TNS_MSG_TYPE_ROW_DATA => {
                parse_row_data(buf, columns, column_info.clone(), &mut response.rows)?;
            }
            TNS_MSG_TYPE_ERROR => {
                parse_error_info(buf, &mut response.error_info, server_ttc_field_version)?;
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => {
                end_of_response = true;
            }
            TNS_MSG_TYPE_PARAMETER => {
                parse_return_parameters(buf)?;
            }
            TNS_MSG_TYPE_BIT_VECTOR => {
                parse_bit_vector(buf, num_columns)?;
            }
            TNS_MSG_TYPE_STATUS => {
                parse_status_info(buf)?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                parse_server_side_piggyback(buf)?;
            }
            _ => {
                return Err(Error::protocol(format!(
                    "Unexpected message type in fetch response: {}",
                    msg_type
                )));
            }
        }
    }

    // Determine if there are more rows based on error info
    // Error 1403 means no more rows
    if response.error_info.error_num == 0 || response.error_info.error_num == 1403 {
        response.more_rows = response.error_info.error_num == 0;
    }

    Ok(response)
}

/// Parse describe info (column metadata).
fn parse_describe_info(
    buf: &mut ReadBuffer,
    response: &mut ExecuteResponse,
    ttc_field_version: u8,
) -> Result<()> {
    let _max_row_size = buf.read_ub4()?;
    let num_columns = buf.read_ub4()?;

    if num_columns > 0 {
        let _ = buf.read_u8()?; // skip flags byte
    }

    for _ in 0..num_columns {
        let metadata = parse_column_metadata(buf, ttc_field_version)?;
        response.columns.push(metadata);
    }

    // Skip remaining describe info fields
    let num_bytes = buf.read_ub4()?;
    if num_bytes > 0 {
        buf.skip_raw_bytes_chunked()?;
    }
    let _ = buf.read_ub4()?; // dcbflag
    let _ = buf.read_ub4()?; // dcbmdbz
    let _ = buf.read_ub4()?; // dcbmnpr
    let _ = buf.read_ub4()?; // dcbmxpr
    let num_bytes2 = buf.read_ub4()?;
    if num_bytes2 > 0 {
        buf.skip_raw_bytes_chunked()?;
    }

    Ok(())
}

/// Parse single column metadata.
fn parse_column_metadata(buf: &mut ReadBuffer, ttc_field_version: u8) -> Result<ColumnMetadata> {
    let oracle_type = buf.read_u8()?;
    let _ = buf.read_u8()?; // flags

    let precision = buf.read_u8()? as i8;
    let scale = buf.read_u8()? as i8;
    let buffer_size = buf.read_ub4()?;
    let _ = buf.read_ub4()?; // max array elements
    let _ = buf.read_ub8()?; // cont flags

    // OID
    let _ = buf.read_bytes_with_length()?;

    let _ = buf.read_ub2()?; // version
    let _ = buf.read_ub2()?; // charset id
    let _ = buf.read_u8()?; // charset form
    let max_size = buf.read_ub4()?;

    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2 {
        let _oaccolid = buf.read_ub4()?; // oaccolid
    }

    let nullable = buf.read_u8()? != 0;
    let _v7_len = buf.read_u8()?; // v7 length

    // Python's read_str_with_length reads: UB4 (indicator) + UB1 (length) + data
    // Our read_str_with_length only reads: UB1 (length) + data
    // So we need to read the UB4 indicator first
    let name = read_column_string(buf)?;
    let _schema = read_column_string(buf)?; // schema
    let _type_name = read_column_string(buf)?; // type name
    let _col_pos = buf.read_ub2()?; // column position
    let _uds_flags = buf.read_ub4()?; // uds flags

    // 23.1+ fields - domain schema/name
    // Note: Python's read_str_with_length has ub4 prefix, so use read_column_string here too
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1 {
        let _domain_schema = read_column_string(buf)?;
        let _domain_name = read_column_string(buf)?;
    }

    // 23.1 EXT3 fields - annotations
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_3 {
        let num_annotations = buf.read_ub4()?;
        if num_annotations > 0 {
            let _ = buf.read_u8()?;
            let actual_count = buf.read_ub4()?;
            let _ = buf.read_u8()?;
            for _ in 0..actual_count {
                let _ = read_column_string(buf)?; // key
                let _ = read_column_string(buf)?; // value
                let _ = buf.read_ub4()?; // flags
            }
            let _ = buf.read_ub4()?; // flags
        }
    }

    // 23.4 fields - vector
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_4 {
        let _ = buf.read_ub4()?; // vector dimensions
        let _ = buf.read_u8()?; // vector format
        let _ = buf.read_u8()?; // vector flags
    }

    Ok(ColumnMetadata {
        name,
        oracle_type,
        precision,
        scale,
        max_size,
        buffer_size,
        nullable,
    })
}

/// Parse row header (bit vector for duplicate detection).
fn parse_row_header(buf: &mut ReadBuffer) -> Result<()> {
    let _ = buf.read_u8()?; // flags
    let _ = buf.read_ub2()?; // num requests
    let _ = buf.read_ub4()?; // iteration number
    let _ = buf.read_ub4()?; // num iters
    let _ = buf.read_ub2()?; // buffer length

    // bit vector
    let num_bytes = buf.read_ub4()?;
    if num_bytes > 0 {
        let _ = buf.read_u8()?; // skip repeated length
        buf.skip((num_bytes - 1) as usize)?;
    }

    // rxhrid
    let num_bytes = buf.read_ub4()?;
    if num_bytes > 0 {
        buf.skip_raw_bytes_chunked()?;
    }

    Ok(())
}

/// Parse row data.
fn parse_row_data(
    buf: &mut ReadBuffer,
    columns: &[ColumnMetadata],
    column_info: Arc<ColumnInfo>,
    rows: &mut Vec<Row>,
) -> Result<()> {
    let mut values = Vec::with_capacity(columns.len());

    for col in columns {
        let value = parse_column_value(buf, col)?;
        values.push(value);
    }

    rows.push(Row::new(values, column_info));
    Ok(())
}

/// Parse a single column value.
fn parse_column_value(buf: &mut ReadBuffer, col: &ColumnMetadata) -> Result<OracleValue> {
    // Read length-prefixed data
    let data = buf.read_bytes_with_length()?;

    match data {
        None => Ok(OracleValue::Null),
        Some(bytes) => {
            match col.oracle_type as u16 {
                // VARCHAR2, CHAR, LONG
                ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => {
                    let s = String::from_utf8_lossy(&bytes).to_string();
                    Ok(OracleValue::String(s))
                }
                // NUMBER, BINARY_INTEGER
                ORA_TYPE_NUM_NUMBER | ORA_TYPE_NUM_BINARY_INTEGER => {
                    let num_str = decode_oracle_number(&bytes)?;
                    Ok(OracleValue::Number(num_str))
                }
                // DATE
                ORA_TYPE_NUM_DATE => {
                    let dt = decode_oracle_date(&bytes)?;
                    Ok(OracleValue::Date(dt))
                }
                // For other types, return as string for now
                _ => {
                    let s = String::from_utf8_lossy(&bytes).to_string();
                    Ok(OracleValue::String(s))
                }
            }
        }
    }
}

/// Parse error info from response.
fn parse_error_info(
    buf: &mut ReadBuffer,
    info: &mut ErrorInfo,
    ttc_field_version: u8,
) -> Result<()> {
    let _call_status = buf.read_ub4()?; // end of call status
    let _ = buf.read_ub2()?; // end to end seq#
    let _ = buf.read_ub4()?; // current row number
    let _error_num_hint = buf.read_ub2()?; // error number hint (not the real error!)
    let _ = buf.read_ub2()?; // array elem error
    let _ = buf.read_ub2()?; // array elem error
    info.cursor_id = buf.read_ub2()?;
    let _ = buf.read_ub2()?; // error position (sb2, but read as ub2)
    let _ = buf.read_u8()?; // sql type
    let _ = buf.read_u8()?; // fatal?
    let _ = buf.read_u8()?; // flags
    let _ = buf.read_u8()?; // user cursor options
    let _ = buf.read_u8()?; // UPI parameter
    let _ = buf.read_u8()?; // warning flags

    // rowid (18 bytes typically)
    let _ = parse_rowid(buf)?;

    let _ = buf.read_ub4()?; // OS error
    let _ = buf.read_u8()?; // statement number
    let _ = buf.read_u8()?; // call number
    let _ = buf.read_ub2()?; // padding
    let _ = buf.read_ub4()?; // success iters

    // oerrdd (logical rowid)
    let num_bytes = buf.read_ub4()?;
    if num_bytes > 0 {
        buf.skip_raw_bytes_chunked()?;
    }

    // batch error codes
    let num_errors = buf.read_ub2()?;
    if num_errors > 0 {
        let first_byte = buf.read_u8()?;
        for _ in 0..num_errors {
            if first_byte == TNS_LONG_LENGTH_INDICATOR {
                let _ = buf.read_ub4()?;
            }
            let _ = buf.read_ub2()?;
        }
        if first_byte == TNS_LONG_LENGTH_INDICATOR {
            buf.skip(1)?;
        }
    }

    // batch error offsets
    let num_offsets = buf.read_ub4()?;
    if num_offsets > 0 {
        let first_byte = buf.read_u8()?;
        for _ in 0..num_offsets {
            if first_byte == TNS_LONG_LENGTH_INDICATOR {
                let _ = buf.read_ub4()?;
            }
            let _ = buf.read_ub4()?;
        }
        if first_byte == TNS_LONG_LENGTH_INDICATOR {
            buf.skip(1)?;
        }
    }

    // batch error messages
    let temp = buf.read_ub2()?;
    if temp > 0 {
        buf.skip(1)?; // packed size
        for _ in 0..temp {
            let _ = buf.read_ub2()?; // chunk length
            let _ = buf.read_str_with_length()?;
            buf.skip(2)?; // end marker
        }
    }

    // Extended error info
    info.error_num = buf.read_ub4()?;
    info.row_count = buf.read_ub8()?;

    // 20c+ fields - only present if server's field version is 20.1+
    // The ttc_field_version parameter here should be server_ttc_field_version
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1 {
        let _sql_type = buf.read_ub4()?;
        let _server_checksum = buf.read_ub4()?;
    }

    // Error message (if error)
    // Note: Even for ORA-01403 "no data found", the server still sends the message
    if info.error_num != 0 {
        info.message = buf.read_str_with_length()?;
    }

    Ok(())
}

/// Parse return parameters (TNS_MSG_TYPE_PARAMETER).
/// Based on Python's _process_return_parameters in MessageWithData.
fn parse_return_parameters(buf: &mut ReadBuffer) -> Result<()> {
    // al8o4l - read num params and skip their ub4 values
    let num_params = buf.read_ub2()?;
    for _ in 0..num_params {
        let _ = buf.read_ub4()?;
    }

    // al8txl - skip bytes if present
    let num_bytes = buf.read_ub2()?;
    if num_bytes > 0 {
        buf.skip(num_bytes as usize)?;
    }

    // num key/value pairs
    let num_pairs = buf.read_ub2()?;
    for _ in 0..num_pairs {
        // text value
        let text_len = buf.read_ub2()?;
        if text_len > 0 {
            buf.skip(text_len as usize)?;
        }
        // binary value
        let bin_len = buf.read_ub2()?;
        if bin_len > 0 {
            buf.skip(bin_len as usize)?;
        }
        // keyword num
        let _ = buf.read_ub2()?;
    }

    // registration
    let num_bytes = buf.read_ub2()?;
    if num_bytes > 0 {
        buf.skip(num_bytes as usize)?;
    }

    Ok(())
}

/// Parse bit vector (TNS_MSG_TYPE_BIT_VECTOR).
///
/// The bit vector is used for duplicate column detection - it indicates which
/// columns in a row have the same value as the previous row. We skip this
/// optimization and just read all column data.
fn parse_bit_vector(buf: &mut ReadBuffer, num_columns: usize) -> Result<()> {
    // num_columns_sent - how many columns are actually sent in this batch
    let _num_columns_sent = buf.read_ub2()?;

    // Calculate bytes needed for bit vector: 1 bit per column
    let num_bytes = num_columns.div_ceil(8);

    // Skip the bit vector data
    if num_bytes > 0 {
        buf.skip(num_bytes)?;
    }

    Ok(())
}

/// Parse a ROWID value.
fn parse_rowid(buf: &mut ReadBuffer) -> Result<Option<String>> {
    // ROWID is variable length, read the parts
    let rba = buf.read_ub4()?;
    let partition_id = buf.read_ub2()?;
    let _ = buf.read_u8()?;
    let block_num = buf.read_ub4()?;
    let slot_num = buf.read_ub2()?;

    if rba == 0 && partition_id == 0 && block_num == 0 && slot_num == 0 {
        Ok(None)
    } else {
        // Encode as base64-like ROWID string (simplified)
        Ok(Some(format!(
            "{:08X}{:04X}{:08X}{:04X}",
            rba, partition_id, block_num, slot_num
        )))
    }
}

/// Read a column string (matches Python's read_str_with_length behavior).
/// Reads: UB4 indicator + UB1 length + bytes
fn read_column_string(buf: &mut ReadBuffer) -> Result<String> {
    let indicator = buf.read_ub4()?;
    if indicator == 0 {
        return Ok(String::new());
    }
    // Read the actual string data (UB1 length + bytes)
    match buf.read_str_with_length()? {
        Some(s) => Ok(s),
        None => Ok(String::new()),
    }
}

/// Parse status info (TNS_MSG_TYPE_STATUS).
///
/// This is a simple status message that can be sent instead of the full
/// ERROR structure in some flows. Based on Python's _process_status_info.
fn parse_status_info(buf: &mut ReadBuffer) -> Result<()> {
    let _call_status = buf.read_ub4()?;
    let _end_to_end_seq = buf.read_ub2()?;
    Ok(())
}

/// Parse server-side piggyback (TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK).
///
/// The server can send additional state updates (session changes, transaction IDs, etc.)
/// embedded in the response. For now we just skip the content.
/// Based on Python's _process_server_side_piggyback.
fn parse_server_side_piggyback(buf: &mut ReadBuffer) -> Result<()> {
    let opcode = buf.read_u8()?;

    // Different opcodes have different payloads
    // Python handles: LTXID (7), SESS_RET (4), AC_REPLAY_CONTEXT (8), EXT_SYNC (9), SESS_SIGNATURE (10)
    match opcode {
        4 => {
            // TNS_SERVER_PIGGYBACK_SESS_RET - DRCP session return
            let _sess_state = buf.read_ub4()?;
            let _sess_state_serial = buf.read_ub2()?;
        }
        7 => {
            // TNS_SERVER_PIGGYBACK_LTXID - Logical transaction ID
            let num_bytes = buf.read_ub4()?;
            if num_bytes > 0 {
                buf.skip_raw_bytes_chunked()?;
            }
        }
        8 => {
            // TNS_SERVER_PIGGYBACK_AC_REPLAY_CONTEXT
            let _flags = buf.read_ub4()?;
            let _error_code = buf.read_ub4()?;
            let num_bytes = buf.read_ub4()?;
            if num_bytes > 0 {
                buf.skip_raw_bytes_chunked()?;
            }
        }
        9 => {
            // TNS_SERVER_PIGGYBACK_EXT_SYNC - Extended sync (keyword/value pairs)
            let num_pairs = buf.read_ub2()?;
            for _ in 0..num_pairs {
                let key_len = buf.read_ub2()?;
                if key_len > 0 {
                    buf.skip(key_len as usize)?;
                }
                let value_len = buf.read_ub4()?;
                if value_len > 0 {
                    buf.skip_raw_bytes_chunked()?;
                }
            }
        }
        10 => {
            // TNS_SERVER_PIGGYBACK_SESS_SIGNATURE
            let num_bytes = buf.read_ub4()?;
            if num_bytes > 0 {
                buf.skip_raw_bytes_chunked()?;
            }
        }
        _ => {
            // Unknown opcode - log but don't fail
            eprintln!("[WARN] Unknown server piggyback opcode: {}", opcode);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_number_zero() {
        // Zero is represented as single byte 0x80
        assert_eq!(decode_oracle_number(&[0x80]).unwrap(), "0");
    }

    #[test]
    fn test_decode_number_positive_integer() {
        // 1: exp_byte=0xC1 (193), exponent=0, mantissa byte=0x02 (digit=1)
        // decimal_point_index = 0*2+2 = 2, digits=[1] -> "1" + trailing zeros -> "1"
        assert_eq!(decode_oracle_number(&[0xC1, 0x02]).unwrap(), "1");

        // 10: exp_byte=0xC1 (193), exponent=0, mantissa byte=0x0B (10)
        // digit_pair = 0x0B - 1 = 10, d1=1, d2=0
        // decimal_point_index=2, digits=[1,0] but trailing zeros removed -> [1]
        // But we need 2 digits for position 2 -> "10"
        // Actually: d1=1, d2=0, trailing zero removed, result = "1" + one trailing zero = "10"
        assert_eq!(decode_oracle_number(&[0xC1, 0x0B]).unwrap(), "10");

        // 100: exp_byte=0xC2 (194), exponent=1, mantissa byte=0x02 (1)
        // decimal_point_index = 1*2+2 = 4, digits=[1]
        // "1" + 3 trailing zeros = "1000" - wrong!
        // Actually 100 = [0xC2, 0x02] where 0x02-1=1, so digit=01
        // decimal_point_index=4, num_digits=1 -> "1" + "000" = "1000" - still wrong
        // Hmm, let me check: 100 in Oracle = exponent=1, mantissa=01 (base 100)
        // So value = 01 * 100^1 = 100
        // decimal_point_index = 2*1+2 = 4, digits from mantissa 0x02 = [0,1]
        // d1=0, d2=1 (since digit_pair = 1)
        // Leading zero: decimal_point_index-=1 -> 3, push d2=1 -> digits=[1]
        // num_digits=1, decimal_point_index=3 -> "1" + "00" = "100"
        assert_eq!(decode_oracle_number(&[0xC2, 0x02]).unwrap(), "100");
    }

    #[test]
    fn test_decode_number_negative_integer() {
        // -1: exp_byte=0x3E, exponent = ~0x3E - 193 = 0xC1 - 193 = 0
        // mantissa byte=0x64 (100), digit = 101 - 100 = 1, trailing 0x66 (102)
        // decimal_point_index = 0*2+2 = 2, digit_pair=1, d1=0, d2=1
        // Leading zero: decimal_point_index-=1 -> 1, push d2=1 -> digits=[1]
        // num_digits=1, decimal_point_index=1 -> "-1"
        assert_eq!(decode_oracle_number(&[0x3E, 0x64, 0x66]).unwrap(), "-1");
    }

    #[test]
    fn test_decode_number_decimal() {
        // 0.5: exp_byte=0xC0 (192), exponent=-1, mantissa=0x33 (50)
        // decimal_point_index = -1*2+2 = 0, digit_pair = 50-1 = 49, d1=4, d2=9
        // But 0.5 should be d1=5, d2=0 from mantissa 0x32 (50+1=51)
        // Actually: 0.5 = 50 * 100^-1 = 0.50
        // exp_byte for exp=-1: 192-1=191? No... exp_byte = exp + 193 = -1 + 193 = 192 = 0xC0
        // mantissa: 50 + 1 = 51 = 0x33
        // digit_pair = 51 - 1 = 50, d1=5, d2=0
        // decimal_point_index = 0, digits=[5] (trailing 0 removed)
        // result = "0." + "5" = "0.5"
        assert_eq!(decode_oracle_number(&[0xC0, 0x33]).unwrap(), "0.5");
    }
}
