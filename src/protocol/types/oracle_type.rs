//! Oracle data type enum with type-specific attributes.
//!
//! This enum represents the supported Oracle data types with their
//! type-specific metadata (precision, scale, max_size).
//!
//! Note: Nullability is a column property, not a type property.

use crate::error::{Error, Result};
use crate::protocol::constants::{
    ORA_TYPE_NUM_BINARY_INTEGER, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CHAR, ORA_TYPE_NUM_CLOB,
    ORA_TYPE_NUM_DATE, ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_VARCHAR,
};

/// Oracle data type with type-specific attributes.
#[derive(Debug, Clone, PartialEq)]
pub enum OracleType {
    /// VARCHAR2(max_length) - variable-length string.
    Varchar2 { max_size: u32 },
    /// NUMBER(precision, scale) - numeric type.
    Number { precision: i8, scale: i8 },
    /// BINARY_INTEGER - integer type.
    BinaryInteger,
    /// LONG - legacy large text type.
    Long,
    /// CHAR(size) - fixed-length string.
    Char { max_size: u32 },
    /// DATE - date/time (no timezone).
    Date,
    /// CLOB - Character Large Object.
    Clob,
    /// NCLOB - National Character Large Object.
    Nclob,
    /// BLOB - Binary Large Object.
    Blob,
}

impl OracleType {
    /// Create from raw Oracle type number and metadata.
    ///
    /// Returns `Err(Error::UnsupportedType)` for unsupported types.
    pub fn from_raw(oracle_type: u8, precision: i8, scale: i8, max_size: u32) -> Result<Self> {
        match oracle_type as u16 {
            ORA_TYPE_NUM_VARCHAR => Ok(OracleType::Varchar2 { max_size }),
            ORA_TYPE_NUM_NUMBER => Ok(OracleType::Number { precision, scale }),
            ORA_TYPE_NUM_BINARY_INTEGER => Ok(OracleType::BinaryInteger),
            ORA_TYPE_NUM_LONG => Ok(OracleType::Long),
            ORA_TYPE_NUM_CHAR => Ok(OracleType::Char { max_size }),
            ORA_TYPE_NUM_DATE => Ok(OracleType::Date),
            ORA_TYPE_NUM_CLOB => Ok(OracleType::Clob),
            ORA_TYPE_NUM_BLOB => Ok(OracleType::Blob),
            _ => Err(Error::UnsupportedType {
                type_num: oracle_type,
            }),
        }
    }

    /// Get the Oracle type number.
    pub fn type_num(&self) -> u8 {
        match self {
            OracleType::Varchar2 { .. } => ORA_TYPE_NUM_VARCHAR as u8,
            OracleType::Number { .. } => ORA_TYPE_NUM_NUMBER as u8,
            OracleType::BinaryInteger => ORA_TYPE_NUM_BINARY_INTEGER as u8,
            OracleType::Long => ORA_TYPE_NUM_LONG as u8,
            OracleType::Char { .. } => ORA_TYPE_NUM_CHAR as u8,
            OracleType::Date => ORA_TYPE_NUM_DATE as u8,
            OracleType::Clob | OracleType::Nclob => ORA_TYPE_NUM_CLOB as u8,
            OracleType::Blob => ORA_TYPE_NUM_BLOB as u8,
        }
    }

    /// Get precision (for Number types, 0 otherwise).
    pub fn precision(&self) -> i8 {
        match self {
            OracleType::Number { precision, .. } => *precision,
            _ => 0,
        }
    }

    /// Get scale (for Number types, 0 otherwise).
    pub fn scale(&self) -> i8 {
        match self {
            OracleType::Number { scale, .. } => *scale,
            _ => 0,
        }
    }

    /// Get max_size (for sized types like Varchar2/Char, 0 otherwise).
    pub fn max_size(&self) -> u32 {
        match self {
            OracleType::Varchar2 { max_size } => *max_size,
            OracleType::Char { max_size } => *max_size,
            _ => 0,
        }
    }
}

impl std::fmt::Display for OracleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OracleType::Varchar2 { max_size } => write!(f, "VARCHAR2({})", max_size),
            OracleType::Number { precision, scale } => {
                if *precision == 0 && *scale == 0 {
                    write!(f, "NUMBER")
                } else if *scale == 0 {
                    write!(f, "NUMBER({})", precision)
                } else {
                    write!(f, "NUMBER({},{})", precision, scale)
                }
            }
            OracleType::BinaryInteger => write!(f, "BINARY_INTEGER"),
            OracleType::Long => write!(f, "LONG"),
            OracleType::Char { max_size } => write!(f, "CHAR({})", max_size),
            OracleType::Date => write!(f, "DATE"),
            OracleType::Clob => write!(f, "CLOB"),
            OracleType::Nclob => write!(f, "NCLOB"),
            OracleType::Blob => write!(f, "BLOB"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_raw_varchar2() {
        let t = OracleType::from_raw(ORA_TYPE_NUM_VARCHAR as u8, 0, 0, 100);
        assert_eq!(t.unwrap(), OracleType::Varchar2 { max_size: 100 });
    }

    #[test]
    fn test_from_raw_number() {
        let t = OracleType::from_raw(ORA_TYPE_NUM_NUMBER as u8, 10, 2, 0);
        assert_eq!(
            t.unwrap(),
            OracleType::Number {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn test_from_raw_unsupported() {
        let t = OracleType::from_raw(255, 0, 0, 0);
        assert!(t.is_err());
        match t {
            Err(Error::UnsupportedType { type_num }) => assert_eq!(type_num, 255),
            _ => panic!("Expected UnsupportedType error"),
        }
    }

    #[test]
    fn test_type_num() {
        assert_eq!(
            OracleType::Varchar2 { max_size: 10 }.type_num(),
            ORA_TYPE_NUM_VARCHAR as u8
        );
        assert_eq!(
            OracleType::Number {
                precision: 5,
                scale: 2
            }
            .type_num(),
            ORA_TYPE_NUM_NUMBER as u8
        );
    }

    #[test]
    fn test_display() {
        assert_eq!(
            format!("{}", OracleType::Varchar2 { max_size: 50 }),
            "VARCHAR2(50)"
        );
        assert_eq!(
            format!(
                "{}",
                OracleType::Number {
                    precision: 10,
                    scale: 2
                }
            ),
            "NUMBER(10,2)"
        );
        assert_eq!(
            format!(
                "{}",
                OracleType::Number {
                    precision: 0,
                    scale: 0
                }
            ),
            "NUMBER"
        );
    }
}
