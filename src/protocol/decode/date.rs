//! Oracle DATE type decoder.
//!
//! Oracle DATE is encoded as 7 bytes (big-endian):
//! - byte[0]: century + 100
//! - byte[1]: year (in century) + 100
//! - byte[2]: month (1-12)
//! - byte[3]: day (1-31)
//! - byte[4]: hour + 1 (0-23)
//! - byte[5]: minute + 1 (0-59)
//! - byte[6]: second + 1 (0-59)

use crate::error::{Error, Result};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

/// Decode an Oracle DATE from 7 bytes.
///
/// Returns a `NaiveDateTime` representing the date/time value.
///
/// # Arguments
/// * `data` - 7-byte DATE value from Oracle
///
/// # Errors
/// Returns `Error::Protocol` if data is not exactly 7 bytes or contains invalid values.
///
/// # Example
/// ```ignore
/// let date = decode_oracle_date(&[0x7e, 0x64, 0x0a, 0x15, 0x0d, 0x3d, 0x26])?;
/// // Returns: 2024-10-21 12:36:05
/// ```
pub fn decode_oracle_date(data: &[u8]) -> Result<NaiveDateTime> {
    if data.len() != 7 {
        return Err(Error::protocol(format!(
            "DATE value must be exactly 7 bytes, got {}",
            data.len()
        )));
    }

    // Decode century and year
    let century = (data[0] - 100) as i32;
    let year_in_century = (data[1] - 100) as i32;
    let year = century * 100 + year_in_century;

    // Month and day are direct values
    let month = data[2];
    let day = data[3];

    // Hour, minute, second are stored as value + 1
    let hour = data[4] - 1;
    let minute = data[5] - 1;
    let second = data[6] - 1;

    // Validate ranges
    if !(1..=12).contains(&month) {
        return Err(Error::protocol(format!("Invalid month: {}", month)));
    }
    if !(1..=31).contains(&day) {
        return Err(Error::protocol(format!("Invalid day: {}", day)));
    }
    if hour > 23 {
        return Err(Error::protocol(format!("Invalid hour: {}", hour)));
    }
    if minute > 59 {
        return Err(Error::protocol(format!("Invalid minute: {}", minute)));
    }
    if second > 59 {
        return Err(Error::protocol(format!("Invalid second: {}", second)));
    }

    // Create NaiveDate and NaiveTime, then combine
    let date = NaiveDate::from_ymd_opt(year, month as u32, day as u32).ok_or_else(|| {
        Error::protocol(format!(
            "Invalid DATE: year={}, month={}, day={}",
            year, month, day
        ))
    })?;
    let time =
        NaiveTime::from_hms_opt(hour as u32, minute as u32, second as u32).ok_or_else(|| {
            Error::protocol(format!(
                "Invalid TIME: hour={}, minute={}, second={}",
                hour, minute, second
            ))
        })?;
    Ok(NaiveDateTime::new(date, time))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    #[test]
    fn test_decode_date_2024_10_21() {
        // 2024-10-21 12:36:05
        // century = 20, byte[0] = 20 + 100 = 120 = 0x78
        // year_in_cent = 24, byte[1] = 24 + 100 = 124 = 0x7C
        // month = 10 = 0x0A
        // day = 21 = 0x15
        // hour = 12 + 1 = 13 = 0x0D
        // minute = 36 + 1 = 37 = 0x25
        // second = 5 + 1 = 6 = 0x06
        let data = [0x78, 0x7C, 0x0A, 0x15, 0x0D, 0x25, 0x06];
        let result = decode_oracle_date(&data).unwrap();
        assert_eq!(result.year(), 2024);
        assert_eq!(result.month(), 10);
        assert_eq!(result.day(), 21);
        assert_eq!(result.hour(), 12);
        assert_eq!(result.minute(), 36);
        assert_eq!(result.second(), 5);
    }

    #[test]
    fn test_decode_date_midnight() {
        // 2024-01-15 00:00:00
        let data = [0x78, 0x7C, 0x01, 0x0F, 0x01, 0x01, 0x01];
        let result = decode_oracle_date(&data).unwrap();
        assert_eq!(result.year(), 2024);
        assert_eq!(result.month(), 1);
        assert_eq!(result.day(), 15);
        assert_eq!(result.hour(), 0);
        assert_eq!(result.minute(), 0);
        assert_eq!(result.second(), 0);
    }

    #[test]
    fn test_decode_date_last_second() {
        // 2024-12-31 23:59:59
        let data = [0x78, 0x7C, 0x0C, 0x1F, 0x18, 0x3C, 0x3C];
        let result = decode_oracle_date(&data).unwrap();
        assert_eq!(result.year(), 2024);
        assert_eq!(result.month(), 12);
        assert_eq!(result.day(), 31);
        assert_eq!(result.hour(), 23);
        assert_eq!(result.minute(), 59);
        assert_eq!(result.second(), 59);
    }

    #[test]
    fn test_decode_date_1999() {
        // 1999-06-15 12:30:45
        // century = 19, byte[0] = 19 + 100 = 119 = 0x77
        // year_in_cent = 99, byte[1] = 99 + 100 = 199 = 0xC7
        let data = [0x77, 0xC7, 0x06, 0x0F, 0x0D, 0x1F, 0x2E];
        let result = decode_oracle_date(&data).unwrap();
        assert_eq!(result.year(), 1999);
        assert_eq!(result.month(), 6);
        assert_eq!(result.day(), 15);
        assert_eq!(result.hour(), 12);
        assert_eq!(result.minute(), 30);
        assert_eq!(result.second(), 45);
    }

    #[test]
    fn test_decode_date_wrong_length() {
        let data = [0x78, 0x7C, 0x0A]; // Only 3 bytes
        assert!(decode_oracle_date(&data).is_err());
    }

    #[test]
    fn test_decode_date_invalid_month() {
        // Invalid month (13)
        let data = [0x78, 0x7C, 0x0D, 0x0F, 0x01, 0x01, 0x01];
        assert!(decode_oracle_date(&data).is_err());
    }

    #[test]
    fn test_decode_date_invalid_day() {
        // Invalid day (32)
        let data = [0x78, 0x7C, 0x01, 0x20, 0x01, 0x01, 0x01];
        assert!(decode_oracle_date(&data).is_err());
    }
}
