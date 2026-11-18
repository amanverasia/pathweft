//! Contact cards (what a QR code carries) and out-of-band verification codes.
//!
//! A card lets one person give another their public key in person:
//! `pathweft:contact/1/<64 hex key>/<4 hex checksum>[/<name>]`.
//! The checksum only catches typing mistakes; it is not authentication.
//! The name is an untrusted label chosen by the card's owner.
//!
//! A verification code lets two people who cannot meet confirm, by voice or in
//! person, that each holds the other's real key. It is built from two halves,
//! one derived from each key alone, shown in a fixed order. An attacker who
//! substitutes keys must find a key whose half matches a specific victim's
//! half (a preimage), rather than any two matching combined values (a much
//! cheaper birthday search). Each half is 15 decimal digits (~50 bits) after
//! 5200 SHA-256 rounds.

use crate::{Error, PublicKey};
use sha2::{Digest, Sha256};
use std::fmt;

const PREFIX: &str = "pathweft:contact/1/";
/// Longest accepted card text; checked before any parsing.
pub const MAX_CARD_CHARS: usize = 128;
const MAX_NAME_CHARS: usize = 32;
const CARD_DOMAIN: &[u8] = b"pathweft/contact-card/v1\0";
const CODE_DOMAIN: &[u8] = b"pathweft/verification-code/v1\0";
const CODE_ROUNDS: usize = 5200;
const HALF_CHUNKS: usize = 3; // 3 chunks x 5 digits = 15 digits per key

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContactCard {
    pub key: PublicKey,
    pub name: Option<String>,
}

fn valid_name(name: &str) -> bool {
    (1..=MAX_NAME_CHARS).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}

fn checksum(key: &PublicKey, name: Option<&str>) -> String {
    let mut hash = Sha256::new();
    hash.update(CARD_DOMAIN);
    hash.update(key.0);
    hash.update(name.unwrap_or("").as_bytes());
    hex::encode(&hash.finalize()[..2])
}

impl ContactCard {
    pub fn new(key: PublicKey, name: Option<String>) -> Result<Self, Error> {
        if name.as_deref().is_some_and(|name| !valid_name(name)) {
            return Err(Error::InvalidCard(
                "name must be 1-32 ASCII letters, digits, '_' or '-'",
            ));
        }
        Ok(Self { key, name })
    }

    /// Parse card text strictly. Rejects unknown versions, uppercase or
    /// malformed hex, bad checksums, invalid names and anything too long.
    pub fn parse(text: &str) -> Result<Self, Error> {
        if text.len() > MAX_CARD_CHARS {
            return Err(Error::InvalidCard("card is too long"));
        }
        let rest = text
            .strip_prefix(PREFIX)
            .ok_or(Error::InvalidCard("not a version 1 Pathweft contact card"))?;
        let mut parts = rest.split('/');
        let key = parts.next().unwrap_or("");
        let sum = parts.next().ok_or(Error::InvalidCard("missing checksum"))?;
        let name = parts.next();
        if parts.next().is_some() {
            return Err(Error::InvalidCard("too many fields"));
        }
        if key.len() != 64
            || !key
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Error::InvalidCard(
                "key must be 64 lowercase hex characters",
            ));
        }
        let key: PublicKey = key.parse()?;
        if name.is_some_and(|name| !valid_name(name)) {
            return Err(Error::InvalidCard(
                "name must be 1-32 ASCII letters, digits, '_' or '-'",
            ));
        }
        if sum != checksum(&key, name) {
            return Err(Error::InvalidCard(
                "checksum mismatch (typo or damaged card)",
            ));
        }
        Ok(Self {
            key,
            name: name.map(str::to_owned),
        })
    }
}

impl fmt::Display for ContactCard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{PREFIX}{}/{}",
            self.key,
            checksum(&self.key, self.name.as_deref())
        )?;
        if let Some(name) = &self.name {
            write!(f, "/{name}")?;
        }
        Ok(())
    }
}

/// 15 digits derived from one key only.
fn half(key: &PublicKey) -> String {
    let mut digest: [u8; 32] = Sha256::new()
        .chain_update(CODE_DOMAIN)
        .chain_update(key.0)
        .finalize()
        .into();
    for _ in 1..CODE_ROUNDS {
        digest = Sha256::new()
            .chain_update(digest)
            .chain_update(key.0)
            .finalize()
            .into();
    }
    (0..HALF_CHUNKS)
        .map(|i| {
            let chunk = digest[i * 5..i * 5 + 5]
                .iter()
                .fold(0_u64, |acc, byte| (acc << 8) | u64::from(*byte));
            format!("{:05}", chunk % 100_000)
        })
        .collect()
}

/// The code both people should see. Identical whichever side computes it.
pub fn verification_code(a: &PublicKey, b: &PublicKey) -> Result<String, Error> {
    if a == b {
        return Err(Error::InvalidCard("cannot verify a key against itself"));
    }
    let (first, second) = if a < b { (a, b) } else { (b, a) };
    let digits = half(first) + &half(second);
    Ok(digits
        .as_bytes()
        .chunks(5)
        .map(|group| std::str::from_utf8(group).unwrap())
        .collect::<Vec<_>>()
        .join(" "))
}
