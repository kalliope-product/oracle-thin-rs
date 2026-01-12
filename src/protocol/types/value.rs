//! Oracle value types for query results.

use chrono::NaiveDateTime;
use std::fmt;

/// Oracle value enum representing a single column value.
#[derive(Debug, Clone, PartialEq)]
pub enum OracleValue {
    /// NULL value.
    Null,
    /// String value (VARCHAR2, CHAR, CLOB, etc.).
    String(String),
    /// Number value as string (preserves precision).
    /// Can be converted to i64/f64 as needed.
    Number(String),
    /// Date/time value (DATE type).
    Date(NaiveDateTime),
}

impl OracleValue {
    /// Check if the value is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, OracleValue::Null)
    }

    /// Try to get the value as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            OracleValue::String(s) => Some(s),
            OracleValue::Number(s) => Some(s),
            _ => None,
        }
    }

    /// Try to convert to i64.
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            OracleValue::Number(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Try to convert to f64.
    pub fn to_f64(&self) -> Option<f64> {
        match self {
            OracleValue::Number(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Try to get the value as a NaiveDateTime.
    pub fn as_date(&self) -> Option<NaiveDateTime> {
        match self {
            OracleValue::Date(dt) => Some(*dt),
            _ => None,
        }
    }
}

impl fmt::Display for OracleValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OracleValue::Null => write!(f, "NULL"),
            OracleValue::String(s) => write!(f, "{}", s),
            OracleValue::Number(n) => write!(f, "{}", n),
            OracleValue::Date(dt) => write!(f, "{}", dt.format("%Y-%m-%d %H:%M:%S")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_value_null() {
        let val = OracleValue::Null;
        assert!(val.is_null());
        assert_eq!(val.as_str(), None);
        assert_eq!(format!("{}", val), "NULL");
    }

    #[test]
    fn test_oracle_value_string() {
        let val = OracleValue::String("hello".to_string());
        assert!(!val.is_null());
        assert_eq!(val.as_str(), Some("hello"));
        assert_eq!(format!("{}", val), "hello");
    }

    #[test]
    fn test_oracle_value_number() {
        let val = OracleValue::Number("123.45".to_string());
        assert!(!val.is_null());
        assert_eq!(val.as_str(), Some("123.45"));
        assert_eq!(val.to_i64(), None); // "123.45" doesn't parse as i64
        assert_eq!(val.to_f64(), Some(123.45));

        let int_val = OracleValue::Number("42".to_string());
        assert_eq!(int_val.to_i64(), Some(42));
        assert_eq!(int_val.to_f64(), Some(42.0));
    }
}
