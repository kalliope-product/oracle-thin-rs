//! Oracle value types for query results.

use super::LobValue;
use chrono::NaiveDateTime;
use std::fmt;

/// Oracle value enum representing a single column value.
#[derive(Debug, Clone, PartialEq)]
pub enum OracleValue {
    /// NULL value.
    Null,
    /// String value (VARCHAR2, CHAR, etc.).
    String(String),
    /// Number value as string (preserves precision).
    /// Can be converted to i64/f64 as needed.
    Number(String),
    /// Date/time value (DATE type).
    Date(NaiveDateTime),
    /// CLOB value (Character Large Object).
    /// May contain prefetched data or just a locator.
    Clob(LobValue),
    /// BLOB value (Binary Large Object).
    /// May contain prefetched data or just a locator.
    Blob(LobValue),
    /// Raw binary value (RAW type).
    Raw(Vec<u8>),
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

    /// Try to get the value as a CLOB.
    pub fn as_clob(&self) -> Option<&LobValue> {
        match self {
            OracleValue::Clob(lob) => Some(lob),
            _ => None,
        }
    }

    /// Try to get the value as a BLOB.
    pub fn as_blob(&self) -> Option<&LobValue> {
        match self {
            OracleValue::Blob(lob) => Some(lob),
            _ => None,
        }
    }

    /// Try to get the value as raw bytes.
    pub fn as_raw(&self) -> Option<&[u8]> {
        match self {
            OracleValue::Raw(bytes) => Some(bytes),
            OracleValue::Blob(lob) => lob.as_bytes(),
            _ => None,
        }
    }

    /// Get the string representation of a CLOB if data is prefetched.
    pub fn clob_string(&self) -> Option<String> {
        match self {
            OracleValue::Clob(lob) => lob.as_string(),
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
            OracleValue::Clob(lob) => {
                if let Some(data) = lob.as_string() {
                    write!(f, "{}", data)
                } else {
                    write!(f, "<CLOB: {} bytes>", lob.size())
                }
            }
            OracleValue::Blob(lob) => {
                if let Some(data) = lob.as_bytes() {
                    write!(f, "<BLOB: {} bytes>", data.len())
                } else {
                    write!(f, "<BLOB: {} bytes>", lob.size())
                }
            }
            OracleValue::Raw(bytes) => write!(f, "<RAW: {} bytes>", bytes.len()),
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
