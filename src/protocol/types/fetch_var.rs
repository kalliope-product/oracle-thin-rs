//! Fetch variable implementation for column defines.
//!
//! When fetching data, the client tells the server what format it expects
//! for each column via fetch_var_impls. This is especially important for
//! LOB columns where the prefetch flag and size must be specified.

use crate::protocol::constants::*;

/// Metadata for a fetch variable (column define).
///
/// This is sent to the server as part of ExecuteMessage when
/// `TNS_EXEC_OPTION_DEFINE` is set.
#[derive(Debug, Clone)]
pub struct FetchVarImpl {
    /// Oracle data type number.
    pub ora_type_num: u8,
    /// Bind flags (TNS_BIND_USE_INDICATORS, TNS_BIND_ARRAY).
    pub flags: u8,
    /// Buffer size for this column.
    pub buffer_size: u32,
    /// Max array elements (0 if not array).
    pub max_num_elements: u32,
    /// Continuation flags (includes TNS_LOB_PREFETCH_FLAG for LOBs).
    pub cont_flag: u64,
    /// Character set ID (873 for UTF8, 0 for binary).
    pub charset_id: u16,
    /// Character set form (1 for NCHAR, 0 otherwise).
    pub charset_form: u8,
    /// LOB prefetch length in bytes.
    pub lob_prefetch_length: u32,
}

impl FetchVarImpl {
    /// Create a fetch var for a regular (non-LOB) column.
    ///
    /// # Arguments
    /// * `ora_type_num` - Oracle data type number
    /// * `buffer_size` - Buffer size for the column
    /// * `charset_form` - Character set form (1 for NCHAR, 0 otherwise)
    pub fn new(ora_type_num: u8, buffer_size: u32, charset_form: u8) -> Self {
        Self {
            ora_type_num,
            flags: TNS_BIND_USE_INDICATORS as u8,
            buffer_size,
            max_num_elements: 0,
            cont_flag: 0,
            charset_id: if charset_form != 0 {
                TNS_CHARSET_UTF8
            } else {
                0
            },
            charset_form,
            lob_prefetch_length: 0,
        }
    }

    /// Create a fetch var for a LOB column with prefetch enabled.
    ///
    /// # Arguments
    /// * `ora_type_num` - Oracle data type number (CLOB=112, BLOB=113)
    /// * `buffer_size` - Buffer size for the LOB column
    /// * `prefetch_length` - LOB prefetch length (max bytes to prefetch inline)
    pub fn new_lob(ora_type_num: u8, buffer_size: u32, prefetch_length: u32) -> Self {
        let is_clob = ora_type_num == ORA_TYPE_NUM_CLOB as u8;
        Self {
            ora_type_num,
            flags: TNS_BIND_USE_INDICATORS as u8,
            buffer_size,
            max_num_elements: 0,
            cont_flag: TNS_LOB_PREFETCH_FLAG,
            charset_id: if is_clob { TNS_CHARSET_UTF8 } else { 0 },
            charset_form: if is_clob { CS_FORM_IMPLICIT } else { 0 },
            lob_prefetch_length: prefetch_length,
        }
    }

    /// Create a fetch var for a JSON column.
    pub fn new_json() -> Self {
        Self {
            ora_type_num: ORA_TYPE_NUM_JSON as u8,
            flags: TNS_BIND_USE_INDICATORS as u8,
            buffer_size: TNS_JSON_MAX_LENGTH,
            max_num_elements: 0,
            cont_flag: TNS_LOB_PREFETCH_FLAG,
            charset_id: TNS_CHARSET_UTF8,
            charset_form: CS_FORM_IMPLICIT,
            lob_prefetch_length: TNS_JSON_MAX_LENGTH,
        }
    }

    /// Create a fetch var for a VECTOR column.
    pub fn new_vector() -> Self {
        Self {
            ora_type_num: ORA_TYPE_NUM_VECTOR as u8,
            flags: TNS_BIND_USE_INDICATORS as u8,
            buffer_size: TNS_VECTOR_MAX_LENGTH,
            max_num_elements: 0,
            cont_flag: TNS_LOB_PREFETCH_FLAG,
            charset_id: 0,
            charset_form: 0,
            lob_prefetch_length: TNS_VECTOR_MAX_LENGTH,
        }
    }

    /// Check if this is a LOB type (CLOB, BLOB, BFILE).
    pub fn is_lob(&self) -> bool {
        matches!(
            self.ora_type_num as u16,
            ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE
        )
    }

    /// Check if LOB prefetch is enabled.
    pub fn has_lob_prefetch(&self) -> bool {
        self.cont_flag & TNS_LOB_PREFETCH_FLAG != 0
    }
}

/// Create fetch var implementations from column metadata.
///
/// This is used after receiving DESCRIBE_INFO to build the define metadata
/// for a subsequent DEFINE+FETCH operation.
pub fn build_fetch_vars_from_metadata(
    columns: &[crate::protocol::types::ColumnMetadata],
    lob_prefetch_size: u32,
) -> Vec<FetchVarImpl> {
    columns
        .iter()
        .map(|col| {
            let ora_type_num = col.oracle_type;

            // Handle ROWID/UROWID - convert to VARCHAR
            if ora_type_num as u16 == ORA_TYPE_NUM_ROWID
                || ora_type_num as u16 == ORA_TYPE_NUM_UROWID
            {
                return FetchVarImpl::new(ORA_TYPE_NUM_VARCHAR as u8, TNS_MAX_UROWID_LENGTH, 0);
            }

            // Handle LOB types
            match ora_type_num as u16 {
                ORA_TYPE_NUM_CLOB => FetchVarImpl::new_lob(
                    ora_type_num,
                    if lob_prefetch_size > 0 {
                        lob_prefetch_size
                    } else {
                        col.buffer_size
                    },
                    lob_prefetch_size,
                ),
                ORA_TYPE_NUM_BLOB => FetchVarImpl::new_lob(
                    ora_type_num,
                    if lob_prefetch_size > 0 {
                        lob_prefetch_size
                    } else {
                        col.buffer_size
                    },
                    lob_prefetch_size,
                ),
                ORA_TYPE_NUM_BFILE => FetchVarImpl::new_lob(ora_type_num, col.buffer_size, 0),
                ORA_TYPE_NUM_JSON => FetchVarImpl::new_json(),
                ORA_TYPE_NUM_VECTOR => FetchVarImpl::new_vector(),
                // Character types get charset_form 1
                ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => {
                    FetchVarImpl::new(ora_type_num, col.buffer_size, 1)
                }
                // All other types (NUMBER, DATE, RAW, etc.) get charset_form 0
                _ => FetchVarImpl::new(ora_type_num, col.buffer_size, 0),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regular_column() {
        let var = FetchVarImpl::new(ORA_TYPE_NUM_VARCHAR as u8, 100, 1);
        assert_eq!(var.ora_type_num, ORA_TYPE_NUM_VARCHAR as u8);
        assert_eq!(var.buffer_size, 100);
        assert_eq!(var.charset_id, TNS_CHARSET_UTF8);
        assert_eq!(var.charset_form, 1);
        assert_eq!(var.cont_flag, 0);
        assert!(!var.is_lob());
        assert!(!var.has_lob_prefetch());
    }

    #[test]
    fn test_clob_column() {
        let var =
            FetchVarImpl::new_lob(ORA_TYPE_NUM_CLOB as u8, 4000, TNS_DEFAULT_LOB_PREFETCH_SIZE);
        assert_eq!(var.ora_type_num, ORA_TYPE_NUM_CLOB as u8);
        assert_eq!(var.charset_id, TNS_CHARSET_UTF8);
        assert_eq!(var.charset_form, 1);
        assert_eq!(var.lob_prefetch_length, TNS_DEFAULT_LOB_PREFETCH_SIZE);
        assert!(var.is_lob());
        assert!(var.has_lob_prefetch());
    }

    #[test]
    fn test_blob_column() {
        let var =
            FetchVarImpl::new_lob(ORA_TYPE_NUM_BLOB as u8, 4000, TNS_DEFAULT_LOB_PREFETCH_SIZE);
        assert_eq!(var.ora_type_num, ORA_TYPE_NUM_BLOB as u8);
        assert_eq!(var.charset_id, 0);
        assert_eq!(var.charset_form, 0);
        assert!(var.is_lob());
        assert!(var.has_lob_prefetch());
    }

    #[test]
    fn test_json_column() {
        let var = FetchVarImpl::new_json();
        assert_eq!(var.ora_type_num, ORA_TYPE_NUM_JSON as u8);
        assert_eq!(var.buffer_size, TNS_JSON_MAX_LENGTH);
        assert_eq!(var.lob_prefetch_length, TNS_JSON_MAX_LENGTH);
        assert!(var.has_lob_prefetch());
    }

    #[test]
    fn test_vector_column() {
        let var = FetchVarImpl::new_vector();
        assert_eq!(var.ora_type_num, ORA_TYPE_NUM_VECTOR as u8);
        assert_eq!(var.buffer_size, TNS_VECTOR_MAX_LENGTH);
        assert_eq!(var.lob_prefetch_length, TNS_VECTOR_MAX_LENGTH);
        assert!(var.has_lob_prefetch());
    }
}
