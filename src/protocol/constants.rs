//! TNS protocol constants.
//!
//! These constants are derived from the python-oracledb thin client implementation.

// Packet types
pub const TNS_PACKET_TYPE_CONNECT: u8 = 1;
pub const TNS_PACKET_TYPE_ACCEPT: u8 = 2;
pub const TNS_PACKET_TYPE_REFUSE: u8 = 4;
pub const TNS_PACKET_TYPE_REDIRECT: u8 = 5;
pub const TNS_PACKET_TYPE_DATA: u8 = 6;
pub const TNS_PACKET_TYPE_RESEND: u8 = 11;
pub const TNS_PACKET_TYPE_MARKER: u8 = 12;
pub const TNS_PACKET_TYPE_CONTROL: u8 = 14;

// Packet flags
pub const TNS_PACKET_FLAG_REDIRECT: u8 = 0x04;
pub const TNS_PACKET_FLAG_TLS_RENEG: u8 = 0x08;

// Data flags
pub const TNS_DATA_FLAGS_EOF: u16 = 0x0040;
pub const TNS_DATA_FLAGS_END_OF_REQUEST: u16 = 0x0800;
pub const TNS_DATA_FLAGS_BEGIN_PIPELINE: u16 = 0x1000;
pub const TNS_DATA_FLAGS_END_OF_RESPONSE: u16 = 0x2000;

// Marker types
pub const TNS_MARKER_TYPE_BREAK: u8 = 1;
pub const TNS_MARKER_TYPE_RESET: u8 = 2;
pub const TNS_MARKER_TYPE_INTERRUPT: u8 = 3;

// Message types
pub const TNS_MSG_TYPE_PROTOCOL: u8 = 1;
pub const TNS_MSG_TYPE_DATA_TYPES: u8 = 2;
pub const TNS_MSG_TYPE_FUNCTION: u8 = 3;
pub const TNS_MSG_TYPE_ERROR: u8 = 4;
pub const TNS_MSG_TYPE_ROW_HEADER: u8 = 6;
pub const TNS_MSG_TYPE_ROW_DATA: u8 = 7;
pub const TNS_MSG_TYPE_PARAMETER: u8 = 8;
pub const TNS_MSG_TYPE_STATUS: u8 = 9;
pub const TNS_MSG_TYPE_IO_VECTOR: u8 = 11;
pub const TNS_MSG_TYPE_LOB_DATA: u8 = 14;
pub const TNS_MSG_TYPE_WARNING: u8 = 15;
pub const TNS_MSG_TYPE_DESCRIBE_INFO: u8 = 16;
pub const TNS_MSG_TYPE_PIGGYBACK: u8 = 17;
pub const TNS_MSG_TYPE_FLUSH_OUT_BINDS: u8 = 19;
pub const TNS_MSG_TYPE_BIT_VECTOR: u8 = 21;
pub const TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK: u8 = 23;
pub const TNS_MSG_TYPE_ONEWAY_FN: u8 = 26;
pub const TNS_MSG_TYPE_IMPLICIT_RESULTSET: u8 = 27;
pub const TNS_MSG_TYPE_RENEGOTIATE: u8 = 28;
pub const TNS_MSG_TYPE_END_OF_RESPONSE: u8 = 29;
pub const TNS_MSG_TYPE_TOKEN: u8 = 33;
pub const TNS_MSG_TYPE_FAST_AUTH: u8 = 34;

// Fast auth constants
pub const TNS_SERVER_CONVERTS_CHARS: u8 = 0x01;

// TTC functions
pub const TNS_FUNC_AUTH_PHASE_ONE: u8 = 118;
pub const TNS_FUNC_AUTH_PHASE_TWO: u8 = 115;
pub const TNS_FUNC_CLOSE_CURSORS: u8 = 105;
pub const TNS_FUNC_COMMIT: u8 = 14;
pub const TNS_FUNC_EXECUTE: u8 = 94;
pub const TNS_FUNC_FETCH: u8 = 5;
pub const TNS_FUNC_LOB_OP: u8 = 96;
pub const TNS_FUNC_LOGOFF: u8 = 9;
pub const TNS_FUNC_PING: u8 = 147;
pub const TNS_FUNC_ROLLBACK: u8 = 15;
pub const TNS_FUNC_REEXECUTE: u8 = 4;
pub const TNS_FUNC_REEXECUTE_AND_FETCH: u8 = 78;

// TTC authentication modes
pub const TNS_AUTH_MODE_LOGON: u32 = 0x00000001;
pub const TNS_AUTH_MODE_CHANGE_PASSWORD: u32 = 0x00000002;
pub const TNS_AUTH_MODE_SYSDBA: u32 = 0x00000020;
pub const TNS_AUTH_MODE_SYSOPER: u32 = 0x00000040;
pub const TNS_AUTH_MODE_WITH_PASSWORD: u32 = 0x00000100;
pub const TNS_AUTH_MODE_SYSASM: u32 = 0x00400000;
pub const TNS_AUTH_MODE_SYSBKP: u32 = 0x01000000;
pub const TNS_AUTH_MODE_SYSDGD: u32 = 0x02000000;
pub const TNS_AUTH_MODE_SYSKMT: u32 = 0x04000000;
pub const TNS_AUTH_MODE_SYSRAC: u32 = 0x08000000;
pub const TNS_AUTH_MODE_IAM_TOKEN: u32 = 0x20000000;

// Protocol versions
pub const TNS_VERSION_DESIRED: u16 = 319;
pub const TNS_VERSION_MINIMUM: u16 = 300;
pub const TNS_VERSION_MIN_ACCEPTED: u16 = 315; // 12.1
pub const TNS_VERSION_MIN_LARGE_SDU: u16 = 315;
pub const TNS_VERSION_MIN_OOB_CHECK: u16 = 318;
pub const TNS_VERSION_MIN_END_OF_RESPONSE: u16 = 319;

// Connect flags
pub const TNS_GSO_DONT_CARE: u16 = 0x0001;
pub const TNS_GSO_CAN_RECV_ATTENTION: u16 = 0x0400;
pub const TNS_NSI_NA_REQUIRED: u8 = 0x10;
pub const TNS_NSI_DISABLE_NA: u8 = 0x04;
pub const TNS_NSI_SUPPORT_SECURITY_RENEG: u8 = 0x80;

// Other connection constants
pub const TNS_PROTOCOL_CHARACTERISTICS: u16 = 0x4f98;
pub const TNS_CHECK_OOB: u32 = 0x01;
pub const TNS_MAX_CONNECT_DATA: u16 = 230;

// Accept flags
pub const TNS_ACCEPT_FLAG_CHECK_OOB: u32 = 0x00000001;
pub const TNS_ACCEPT_FLAG_FAST_AUTH: u32 = 0x10000000;
pub const TNS_ACCEPT_FLAG_HAS_END_OF_RESPONSE: u32 = 0x02000000;

// Character sets and encodings
pub const TNS_CHARSET_UTF8: u16 = 873;
pub const TNS_CHARSET_UTF16: u16 = 2000;
pub const TNS_ENCODING_MULTI_BYTE: u8 = 0x01;
pub const TNS_ENCODING_CONV_LENGTH: u8 = 0x02;

// Verifier types
pub const TNS_VERIFIER_TYPE_11G_1: u32 = 0xb152;
pub const TNS_VERIFIER_TYPE_11G_2: u32 = 0x1b25;
pub const TNS_VERIFIER_TYPE_12C: u32 = 0x4815;

// Execute options
pub const TNS_EXEC_OPTION_PARSE: u32 = 0x01;
pub const TNS_EXEC_OPTION_BIND: u32 = 0x08;
pub const TNS_EXEC_OPTION_DEFINE: u32 = 0x10;
pub const TNS_EXEC_OPTION_EXECUTE: u32 = 0x20;
pub const TNS_EXEC_OPTION_FETCH: u32 = 0x40;
pub const TNS_EXEC_OPTION_COMMIT: u32 = 0x100;
pub const TNS_EXEC_OPTION_COMMIT_REEXECUTE: u32 = 0x1;
pub const TNS_EXEC_OPTION_PLSQL_BIND: u32 = 0x400;
pub const TNS_EXEC_OPTION_NOT_PLSQL: u32 = 0x8000;
pub const TNS_EXEC_OPTION_DESCRIBE: u32 = 0x20000;
pub const TNS_EXEC_OPTION_NO_COMPRESSED_FETCH: u32 = 0x40000;
pub const TNS_EXEC_OPTION_BATCH_ERRORS: u32 = 0x80000;

// Execute flags (al8i4[9])
pub const TNS_EXEC_FLAGS_IMPLICIT_RESULTSET: u32 = 0x8000;
pub const TNS_EXEC_FLAGS_DML_ROWCOUNTS: u32 = 0x4000;
pub const TNS_EXEC_FLAGS_SCROLLABLE: u32 = 0x02;
pub const TNS_EXEC_FLAGS_NO_CANCEL_ON_EOF: u32 = 0x80;

// Bind flags
pub const TNS_BIND_USE_INDICATORS: u16 = 0x0001;
pub const TNS_BIND_ARRAY: u16 = 0x0040;

// Bind directions
pub const TNS_BIND_DIR_OUTPUT: u8 = 16;
pub const TNS_BIND_DIR_INPUT: u8 = 32;
pub const TNS_BIND_DIR_INPUT_OUTPUT: u8 = 48;

// Error codes
pub const TNS_ERR_INCONSISTENT_DATA_TYPES: u32 = 932;
pub const TNS_ERR_VAR_NOT_IN_SELECT_LIST: u32 = 1007;
pub const TNS_ERR_INBAND_MESSAGE: u32 = 12573;
pub const TNS_ERR_INVALID_SERVICE_NAME: u32 = 12514;
pub const TNS_ERR_INVALID_SID: u32 = 12505;
pub const TNS_ERR_NO_DATA_FOUND: u32 = 1403;
pub const TNS_ERR_SESSION_SHUTDOWN: u32 = 12572;

// Compile time capability indices
pub const TNS_CCAP_SQL_VERSION: usize = 0;
pub const TNS_CCAP_LOGON_TYPES: usize = 4;
pub const TNS_CCAP_FEATURE_BACKPORT: usize = 5;
pub const TNS_CCAP_FIELD_VERSION: usize = 7;
pub const TNS_CCAP_SERVER_DEFINE_CONV: usize = 8;
pub const TNS_CCAP_DEQUEUE_WITH_SELECTOR: usize = 9;
pub const TNS_CCAP_TTC1: usize = 15;
pub const TNS_CCAP_OCI1: usize = 16;
pub const TNS_CCAP_TDS_VERSION: usize = 17;
pub const TNS_CCAP_RPC_VERSION: usize = 18;
pub const TNS_CCAP_RPC_SIG: usize = 19;
pub const TNS_CCAP_DBF_VERSION: usize = 21;
pub const TNS_CCAP_LOB: usize = 23;
pub const TNS_CCAP_TTC2: usize = 26;
pub const TNS_CCAP_UB2_DTY: usize = 27;
pub const TNS_CCAP_OCI2: usize = 31;
pub const TNS_CCAP_CLIENT_FN: usize = 34;
pub const TNS_CCAP_OCI3: usize = 35;
pub const TNS_CCAP_TTC3: usize = 37;
pub const TNS_CCAP_SESS_SIGNATURE_VERSION: usize = 39;
pub const TNS_CCAP_TTC4: usize = 40;
pub const TNS_CCAP_LOB2: usize = 42;
pub const TNS_CCAP_TTC5: usize = 44;
pub const TNS_CCAP_VECTOR_FEATURES: usize = 52;
pub const TNS_CCAP_MAX: usize = 53;

// Compile time capability values
pub const TNS_CCAP_SQL_VERSION_MAX: u8 = 6;
pub const TNS_CCAP_FIELD_VERSION_12_2: u8 = 8;
pub const TNS_CCAP_FIELD_VERSION_18_1: u8 = 10;
pub const TNS_CCAP_FIELD_VERSION_19_1: u8 = 12;
pub const TNS_CCAP_FIELD_VERSION_19_1_EXT_1: u8 = 13;
pub const TNS_CCAP_FIELD_VERSION_20_1: u8 = 14;
pub const TNS_CCAP_FIELD_VERSION_21_1: u8 = 16;
pub const TNS_CCAP_FIELD_VERSION_23_1: u8 = 17;
pub const TNS_CCAP_FIELD_VERSION_23_1_EXT_3: u8 = 20;
pub const TNS_CCAP_FIELD_VERSION_23_4: u8 = 24;
pub const TNS_CCAP_FIELD_VERSION_MAX: u8 = 24;
pub const TNS_CCAP_O5LOGON: u8 = 8;
pub const TNS_CCAP_O5LOGON_NP: u8 = 2;
pub const TNS_CCAP_O7LOGON: u8 = 32;
pub const TNS_CCAP_O8LOGON_LONG_IDENTIFIER: u8 = 64;
pub const TNS_CCAP_O9LOGON_LONG_PASSWORD: u8 = 0x80;
pub const TNS_CCAP_CTB_IMPLICIT_POOL: u8 = 0x08;
pub const TNS_CCAP_CTB_OAUTH_MSG_ON_ERR: u8 = 0x10;
pub const TNS_CCAP_END_OF_CALL_STATUS: u8 = 0x01;
pub const TNS_CCAP_IND_RCD: u8 = 0x08;
pub const TNS_CCAP_FAST_BVEC: u8 = 0x20;
pub const TNS_CCAP_FAST_SESSION_PROPAGATE: u8 = 0x10;
pub const TNS_CCAP_APP_CTX_PIGGYBACK: u8 = 0x80;
pub const TNS_CCAP_TDS_VERSION_MAX: u8 = 3;
pub const TNS_CCAP_RPC_VERSION_MAX: u8 = 7;
pub const TNS_CCAP_RPC_SIG_VALUE: u8 = 3;
pub const TNS_CCAP_DBF_VERSION_MAX: u8 = 1;
pub const TNS_CCAP_LOB_UB8_SIZE: u8 = 0x01;
pub const TNS_CCAP_LOB_ENCS: u8 = 0x02;
pub const TNS_CCAP_LOB_PREFETCH_DATA: u8 = 0x04;
pub const TNS_CCAP_LOB_TEMP_SIZE: u8 = 0x08;
pub const TNS_CCAP_LOB_PREFETCH_LENGTH: u8 = 0x40;
pub const TNS_CCAP_LOB_12C: u8 = 0x80;
pub const TNS_CCAP_LOB2_QUASI: u8 = 0x01;
pub const TNS_CCAP_LOB2_2GB_PREFETCH: u8 = 0x04;
pub const TNS_CCAP_ZLNP: u8 = 0x04;
pub const TNS_CCAP_DRCP: u8 = 0x10;
pub const TNS_CCAP_LTXID: u8 = 0x08;
pub const TNS_CCAP_IMPLICIT_RESULTS: u8 = 0x10;
pub const TNS_CCAP_BIG_CHUNK_CLR: u8 = 0x20;
pub const TNS_CCAP_KEEP_OUT_ORDER: u8 = 0x80;
pub const TNS_CCAP_INBAND_NOTIFICATION: u8 = 0x04;
pub const TNS_CCAP_EXPLICIT_BOUNDARY: u8 = 0x40;
pub const TNS_CCAP_END_OF_RESPONSE: u8 = 0x20;
pub const TNS_CCAP_VECTOR_SUPPORT: u8 = 0x08;
pub const TNS_CCAP_TOKEN_SUPPORTED: u8 = 0x02;
pub const TNS_CCAP_PIPELINING_SUPPORT: u8 = 0x04;
pub const TNS_CCAP_PIPELINING_BREAK: u8 = 0x10;
pub const TNS_CCAP_TTC5_SESSIONLESS_TXNS: u8 = 0x20;
pub const TNS_CCAP_VECTOR_FEATURE_BINARY: u8 = 0x01;
pub const TNS_CCAP_VECTOR_FEATURE_SPARSE: u8 = 0x02;
pub const TNS_CCAP_OCI3_OCSSYNC: u8 = 0x20;
pub const TNS_CCAP_CLIENT_FN_MAX: u8 = 12;

// Runtime capability indices
pub const TNS_RCAP_COMPAT: usize = 0;
pub const TNS_RCAP_TTC: usize = 6;
pub const TNS_RCAP_MAX: usize = 11;

// Runtime capability values
pub const TNS_RCAP_COMPAT_81: u8 = 2;
pub const TNS_RCAP_TTC_ZERO_COPY: u8 = 0x01;
pub const TNS_RCAP_TTC_32K: u8 = 0x04;

// Other constants
pub const TNS_ESCAPE_CHAR: u8 = 253;
pub const TNS_LONG_LENGTH_INDICATOR: u8 = 254;
pub const TNS_NULL_LENGTH_INDICATOR: u8 = 0;
pub const TNS_MAX_LONG_LENGTH: u32 = 0x7fffffff;
pub const TNS_DURATION_SESSION: u8 = 10;
pub const PACKET_HEADER_SIZE: usize = 8;
pub const TNS_SDU_DEFAULT: u32 = 8192;

// Oracle data type numbers
pub const ORA_TYPE_NUM_BFILE: u16 = 114;
pub const ORA_TYPE_NUM_BINARY_DOUBLE: u16 = 101;
pub const ORA_TYPE_NUM_BINARY_FLOAT: u16 = 100;
pub const ORA_TYPE_NUM_BINARY_INTEGER: u16 = 3;
pub const ORA_TYPE_NUM_BLOB: u16 = 113;
pub const ORA_TYPE_NUM_BOOLEAN: u16 = 252;
pub const ORA_TYPE_NUM_CHAR: u16 = 96;
pub const ORA_TYPE_NUM_CLOB: u16 = 112;
pub const ORA_TYPE_NUM_CURSOR: u16 = 102;
pub const ORA_TYPE_NUM_DATE: u16 = 12;
pub const ORA_TYPE_NUM_INTERVAL_DS: u16 = 183;
pub const ORA_TYPE_NUM_INTERVAL_YM: u16 = 182;
pub const ORA_TYPE_NUM_JSON: u16 = 119;
pub const ORA_TYPE_NUM_LONG: u16 = 8;
pub const ORA_TYPE_NUM_LONG_RAW: u16 = 24;
pub const ORA_TYPE_NUM_NUMBER: u16 = 2;
pub const ORA_TYPE_NUM_OBJECT: u16 = 109;
pub const ORA_TYPE_NUM_RAW: u16 = 23;
pub const ORA_TYPE_NUM_ROWID: u16 = 11;
pub const ORA_TYPE_NUM_TIMESTAMP: u16 = 180;
pub const ORA_TYPE_NUM_TIMESTAMP_LTZ: u16 = 231;
pub const ORA_TYPE_NUM_TIMESTAMP_TZ: u16 = 181;
pub const ORA_TYPE_NUM_UROWID: u16 = 208;
pub const ORA_TYPE_NUM_VARCHAR: u16 = 1;
pub const ORA_TYPE_NUM_VECTOR: u16 = 127;