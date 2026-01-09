//! Column and ColumnInfo types for user-facing API.
//!
//! These types provide a clean interface for accessing column information
//! from query results, derived from the internal ColumnMetadata.

use crate::error::Result;

use super::metadata::ColumnMetadata;
use super::oracle_type::OracleType;

/// A column in a result set (user-facing representation).
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Whether NULL values are allowed.
    pub nullable: bool,
    /// Column data type.
    pub data_type: OracleType,
    /// Raw Oracle type number.
    pub oracle_type_num: u8,
}

impl Column {
    /// Create a column from metadata.
    ///
    /// Returns error if the Oracle type is not supported.
    pub fn from_metadata(meta: &ColumnMetadata) -> Result<Self> {
        Ok(Self {
            name: meta.name.clone(),
            nullable: meta.nullable,
            data_type: OracleType::from_raw(
                meta.oracle_type,
                meta.precision,
                meta.scale,
                meta.max_size,
            )?,
            oracle_type_num: meta.oracle_type,
        })
    }
}

/// Shared column information for all rows in a result set.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column definitions.
    pub columns: Vec<Column>,
}

impl ColumnInfo {
    /// Create new column info from columns.
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    /// Create column info from internal metadata.
    ///
    /// Returns error if any column has an unsupported Oracle type.
    pub fn from_metadata(metadata: &[ColumnMetadata]) -> Result<Self> {
        let columns: Result<Vec<Column>> = metadata.iter().map(Column::from_metadata).collect();
        Ok(Self { columns: columns? })
    }

    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get the number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Check if there are no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get column by index.
    pub fn get(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Find column index by name (case-insensitive).
    pub fn find_by_name(&self, name: &str) -> Option<usize> {
        let name_upper = name.to_uppercase();
        self.columns
            .iter()
            .position(|c| c.name.to_uppercase() == name_upper)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_metadata() -> Vec<ColumnMetadata> {
        vec![
            ColumnMetadata {
                name: "ID".to_string(),
                oracle_type: 2, // NUMBER
                precision: 10,
                scale: 0,
                max_size: 22,
                buffer_size: 22,
                nullable: false,
            },
            ColumnMetadata {
                name: "NAME".to_string(),
                oracle_type: 1, // VARCHAR2
                precision: 0,
                scale: 0,
                max_size: 100,
                buffer_size: 100,
                nullable: true,
            },
        ]
    }

    #[test]
    fn test_column_from_metadata() {
        let meta = &make_test_metadata()[0];
        let col = Column::from_metadata(meta).unwrap();

        assert_eq!(col.name, "ID");
        assert!(!col.nullable);
        assert_eq!(col.oracle_type_num, 2);

        if let OracleType::Number { precision, scale } = col.data_type {
            assert_eq!(precision, 10);
            assert_eq!(scale, 0);
        } else {
            panic!("Expected Number type");
        }
    }

    #[test]
    fn test_column_info_from_metadata() {
        let metadata = make_test_metadata();
        let info = ColumnInfo::from_metadata(&metadata).unwrap();

        assert_eq!(info.len(), 2);
        assert_eq!(info.column_names(), vec!["ID", "NAME"]);
        assert_eq!(info.find_by_name("name"), Some(1));
        assert_eq!(info.find_by_name("UNKNOWN"), None);
    }
}
