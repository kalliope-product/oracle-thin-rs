//! Row type for query results.

use std::sync::Arc;

use super::column::{Column, ColumnInfo};
use super::value::OracleValue;

/// A row of query results.
#[derive(Debug, Clone)]
pub struct Row {
    /// Column values.
    values: Vec<OracleValue>,
    /// Shared column information (reference counted).
    column_info: Arc<ColumnInfo>,
}

impl Row {
    /// Create a new row with values and shared column info.
    pub fn new(values: Vec<OracleValue>, column_info: Arc<ColumnInfo>) -> Self {
        Self {
            values,
            column_info,
        }
    }

    /// Get value by column index (0-based).
    pub fn get(&self, index: usize) -> Option<&OracleValue> {
        self.values.get(index)
    }

    /// Get value by column name (case-insensitive).
    pub fn get_by_name(&self, name: &str) -> Option<&OracleValue> {
        self.column_info
            .find_by_name(name)
            .and_then(|idx| self.values.get(idx))
    }

    /// Get the number of columns.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get all values.
    pub fn values(&self) -> &[OracleValue] {
        &self.values
    }

    /// Get column information.
    pub fn columns(&self) -> &[Column] {
        &self.column_info.columns
    }

    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.column_info.column_names()
    }

    /// Iterate over values.
    pub fn iter(&self) -> impl Iterator<Item = &OracleValue> {
        self.values.iter()
    }
}

impl IntoIterator for Row {
    type Item = OracleValue;
    type IntoIter = std::vec::IntoIter<OracleValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = &'a OracleValue;
    type IntoIter = std::slice::Iter<'a, OracleValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::OracleType;

    fn make_test_column_info() -> Arc<ColumnInfo> {
        Arc::new(ColumnInfo::new(vec![
            Column {
                name: "NAME".to_string(),
                nullable: true,
                data_type: OracleType::Varchar2 { max_size: 100 },
                oracle_type_num: 1,
            },
            Column {
                name: "VALUE".to_string(),
                nullable: false,
                data_type: OracleType::Number {
                    precision: 10,
                    scale: 0,
                },
                oracle_type_num: 2,
            },
        ]))
    }

    #[test]
    fn test_row_access() {
        let column_info = make_test_column_info();
        let row = Row::new(
            vec![
                OracleValue::String("test".to_string()),
                OracleValue::Number("42".to_string()),
            ],
            column_info,
        );

        assert_eq!(row.len(), 2);
        assert_eq!(
            row.get(0),
            Some(&OracleValue::String("test".to_string()))
        );
        assert_eq!(
            row.get_by_name("value"),
            Some(&OracleValue::Number("42".to_string()))
        );
        assert_eq!(row.get_by_name("VALUE"), row.get_by_name("value"));
    }

    #[test]
    fn test_row_columns() {
        let column_info = make_test_column_info();
        let row = Row::new(
            vec![
                OracleValue::String("test".to_string()),
                OracleValue::Number("42".to_string()),
            ],
            column_info,
        );

        let columns = row.columns();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "NAME");
        assert_eq!(columns[1].name, "VALUE");
    }
}
