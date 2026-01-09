//! Oracle NUMBER type decoder.
//!
//! Oracle NUMBER is a variable-length format where:
//! - First byte is exponent (with sign info in high bit)
//! - Remaining bytes are base-100 digits of mantissa

use crate::error::Result;

/// Decode Oracle NUMBER format to string.
///
/// Preserves full precision by returning the number as a string.
/// Use `.parse::<i64>()` or `.parse::<f64>()` to convert.
pub fn decode_oracle_number(bytes: &[u8]) -> Result<String> {
    if bytes.is_empty() {
        return Ok("0".to_string());
    }

    let exp_byte = bytes[0];
    let is_positive = (exp_byte & 0x80) != 0;

    // Calculate exponent
    let exponent: i16 = if is_positive {
        exp_byte as i16 - 193
    } else {
        // Invert bits for negative numbers
        (!exp_byte) as i16 - 193
    };

    // decimal_point_index indicates where the decimal point goes
    let mut decimal_point_index: i16 = exponent * 2 + 2;

    // Handle zero and special cases
    if bytes.len() == 1 {
        if is_positive {
            return Ok("0".to_string());
        } else {
            // -1e126 (max negative value) - rare, return special
            return Ok("-1e126".to_string());
        }
    }

    // Check for trailing 102 byte for negative numbers
    let mantissa_end = if !is_positive && bytes[bytes.len() - 1] == 102 {
        bytes.len() - 1
    } else {
        bytes.len()
    };

    // Process mantissa bytes to extract base-100 digits
    let mut digits: Vec<u8> = Vec::with_capacity((mantissa_end - 1) * 2);

    for (i, &byte) in bytes.iter().enumerate().take(mantissa_end).skip(1) {
        let digit_pair = if is_positive {
            byte.wrapping_sub(1)
        } else {
            101u8.wrapping_sub(byte)
        };

        // First digit of the pair
        let d1 = digit_pair / 10;
        // Second digit of the pair
        let d2 = digit_pair % 10;

        // Handle leading zeros - they reduce decimal point index
        if digits.is_empty() && d1 == 0 {
            decimal_point_index -= 1;
            if d2 != 0 || i < mantissa_end - 1 {
                digits.push(d2);
            } else if d2 == 0 {
                decimal_point_index -= 1;
            }
        } else if d1 == 10 {
            // Overflow case (99+1=100) - rare
            digits.push(1);
            digits.push(0);
            decimal_point_index += 1;
        } else {
            digits.push(d1);
            // Only add trailing zero if not last byte
            if d2 != 0 || i < mantissa_end - 1 {
                digits.push(d2);
            }
        }
    }

    // Remove trailing zeros from digits
    while !digits.is_empty() && digits[digits.len() - 1] == 0 {
        digits.pop();
    }

    // If all digits were zeros
    if digits.is_empty() {
        return Ok("0".to_string());
    }

    // Build the string
    let mut result = String::new();

    if !is_positive {
        result.push('-');
    }

    let num_digits = digits.len() as i16;

    if decimal_point_index <= 0 {
        // Number is less than 1: 0.00...digits
        result.push('0');
        result.push('.');
        for _ in decimal_point_index..0 {
            result.push('0');
        }
        for d in &digits {
            result.push((b'0' + d) as char);
        }
    } else if decimal_point_index >= num_digits {
        // Number is an integer: digits + trailing zeros
        for d in &digits {
            result.push((b'0' + d) as char);
        }
        for _ in num_digits..decimal_point_index {
            result.push('0');
        }
    } else {
        // Number has decimal point in the middle
        for (i, d) in digits.iter().enumerate() {
            if i as i16 == decimal_point_index {
                result.push('.');
            }
            result.push((b'0' + d) as char);
        }
    }

    Ok(result)
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
        assert_eq!(decode_oracle_number(&[0xC1, 0x02]).unwrap(), "1");

        // 10: exp_byte=0xC1 (193), exponent=0, mantissa byte=0x0B (10)
        assert_eq!(decode_oracle_number(&[0xC1, 0x0B]).unwrap(), "10");

        // 100: exp_byte=0xC2 (194), exponent=1, mantissa byte=0x02 (1)
        assert_eq!(decode_oracle_number(&[0xC2, 0x02]).unwrap(), "100");
    }

    #[test]
    fn test_decode_number_negative_integer() {
        // -1: exp_byte=0x3E, mantissa byte=0x64 (100), trailing 0x66 (102)
        assert_eq!(decode_oracle_number(&[0x3E, 0x64, 0x66]).unwrap(), "-1");
    }

    #[test]
    fn test_decode_number_decimal() {
        // 0.5: exp_byte=0xC0 (192), exponent=-1, mantissa=0x33 (51)
        assert_eq!(decode_oracle_number(&[0xC0, 0x33]).unwrap(), "0.5");
    }
}
