//! Cryptographic functions for Oracle authentication.

use aes::cipher::{block_padding::NoPadding, BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use hmac::Hmac;
use md5::Md5;
use pbkdf2::pbkdf2;
use sha1::Sha1;
use sha2::Sha512;

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;
type Aes192CbcEnc = cbc::Encryptor<aes::Aes192>;
type Aes192CbcDec = cbc::Decryptor<aes::Aes192>;

/// Encrypt data using AES-CBC with the given key.
/// The IV is all zeros (as used by Oracle).
pub fn encrypt_cbc(key: &[u8], plaintext: &[u8], use_zero_padding: bool) -> Vec<u8> {
    let iv = [0u8; 16];
    let block_size = 16;

    // Pad the plaintext - ALWAYS add padding (1-16 bytes)
    // Python: n = block_size - len(plain_text) % block_size
    //         if n: plain_text += (bytes([n]) * n)  # n is always 1-16
    let mut padded = plaintext.to_vec();
    let padding_needed = block_size - (padded.len() % block_size);
    // padding_needed is always 1-16 (when len % 16 == 0, it's 16)
    if use_zero_padding {
        padded.extend(vec![0u8; padding_needed]);
    } else {
        // PKCS7 padding
        padded.extend(vec![padding_needed as u8; padding_needed]);
    }

    match key.len() {
        32 => {
            let encryptor = Aes256CbcEnc::new(key.into(), &iv.into());
            let mut buf = padded.clone();
            encryptor
                .encrypt_padded_mut::<NoPadding>(&mut buf, padded.len())
                .expect("encryption failed");
            buf
        }
        24 => {
            let encryptor = Aes192CbcEnc::new(key.into(), &iv.into());
            let mut buf = padded.clone();
            encryptor
                .encrypt_padded_mut::<NoPadding>(&mut buf, padded.len())
                .expect("encryption failed");
            buf
        }
        _ => panic!("Invalid key length: {}", key.len()),
    }
}

/// Decrypt data using AES-CBC with the given key.
/// The IV is all zeros (as used by Oracle).
pub fn decrypt_cbc(key: &[u8], ciphertext: &[u8]) -> Vec<u8> {
    let iv = [0u8; 16];

    match key.len() {
        32 => {
            let decryptor = Aes256CbcDec::new(key.into(), &iv.into());
            let mut buf = ciphertext.to_vec();
            decryptor
                .decrypt_padded_mut::<NoPadding>(&mut buf)
                .expect("decryption failed")
                .to_vec()
        }
        24 => {
            let decryptor = Aes192CbcDec::new(key.into(), &iv.into());
            let mut buf = ciphertext.to_vec();
            decryptor
                .decrypt_padded_mut::<NoPadding>(&mut buf)
                .expect("decryption failed")
                .to_vec()
        }
        _ => panic!("Invalid key length: {}", key.len()),
    }
}

/// Derive a key using PBKDF2 with SHA-512.
pub fn derive_key_pbkdf2(password: &[u8], salt: &[u8], length: usize, iterations: u32) -> Vec<u8> {
    let mut key = vec![0u8; length];
    pbkdf2::<Hmac<Sha512>>(password, salt, iterations, &mut key)
        .expect("PBKDF2 failed");
    key
}

/// Compute SHA-1 hash.
pub fn sha1_hash(data: &[u8]) -> [u8; 20] {
    use sha1::Digest;
    let mut hasher = Sha1::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute SHA-512 hash.
pub fn sha512_hash(data: &[u8]) -> [u8; 64] {
    use sha2::Digest;
    let mut hasher = Sha512::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute MD5 hash.
pub fn md5_hash(data: &[u8]) -> [u8; 16] {
    use md5::Digest;
    let mut hasher = Md5::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Generate cryptographically secure random bytes.
pub fn random_bytes(len: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

/// Convert bytes to uppercase hex string.
pub fn bytes_to_hex_upper(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{:02X}", b))
        .collect()
}

/// Convert hex string to bytes.
pub fn hex_to_bytes(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }

    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = [0u8; 32];
        let plaintext = b"Hello, Oracle!";

        let encrypted = encrypt_cbc(&key, plaintext, false);
        let decrypted = decrypt_cbc(&key, &encrypted);

        // Note: decrypted will have padding
        assert!(decrypted.starts_with(plaintext));
    }

    #[test]
    fn test_hex_conversion() {
        let bytes = [0xDE, 0xAD, 0xBE, 0xEF];
        let hex = bytes_to_hex_upper(&bytes);
        assert_eq!(hex, "DEADBEEF");

        let back = hex_to_bytes(&hex).unwrap();
        assert_eq!(back, bytes);
    }
}
