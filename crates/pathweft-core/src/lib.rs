//! Controlled-prototype messages and routing; no sockets or platform radio APIs.
#![deny(unsafe_code)]

pub mod contact;
#[allow(unsafe_code)]
mod crypto;
pub mod discovery;
mod router;

pub use contact::{ContactCard, verification_code};
pub use crypto::{Identity, seal_anonymous};
pub use router::{Action, DropReason, Origin, Router};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};
use uuid::Uuid;

pub const VERSION: u8 = 1;
pub const DEFAULT_HOPS: u8 = 8;
pub const MAX_HOPS: u8 = 16;
pub const MAX_TEXT_BYTES: usize = 512;
pub const MAX_PLAINTEXT_BYTES: usize = 4096;
pub const MAX_CIPHERTEXT_BYTES: usize = MAX_PLAINTEXT_BYTES + 16;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid 32-byte key or identity")]
    InvalidKey,
    #[error("invalid envelope: {0}")]
    InvalidEnvelope(&'static str),
    #[error("message must contain 1 to {MAX_TEXT_BYTES} UTF-8 bytes")]
    InvalidText,
    #[error("cryptographic operation failed")]
    Crypto,
    #[error("packet is not addressed to this identity")]
    WrongRecipient,
    #[error("sender key does not match the packet source")]
    WrongSender,
    #[error("authenticated content does not match the envelope")]
    HeaderMismatch,
    #[error("invalid contact card: {0}")]
    InvalidCard(&'static str),
    #[error("invalid payload: {0}")]
    Json(#[from] serde_json::Error),
}

macro_rules! hex_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub(crate) [u8; 32]);
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&hex::encode(self.0))
            }
        }
        impl FromStr for $name {
            type Err = Error;
            fn from_str(s: &str) -> Result<Self, Error> {
                let bytes: [u8; 32] = hex::decode(s)
                    .map_err(|_| Error::InvalidKey)?
                    .try_into()
                    .map_err(|_| Error::InvalidKey)?;
                if bytes == [0; 32] {
                    return Err(Error::InvalidKey);
                }
                Ok(Self(bytes))
            }
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(&self.to_string())
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                String::deserialize(d)?.parse().map_err(de::Error::custom)
            }
        }
    };
}
hex_type!(NodeId);
hex_type!(PublicKey);

impl PublicKey {
    pub fn node_id(self) -> NodeId {
        NodeId(Sha256::digest(self.0).into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub version: u8,
    pub id: Uuid,
    pub source: NodeId,
    pub destination: NodeId,
    pub remaining_hops: u8,
    pub nonce: [u8; 24],
    pub ciphertext: Vec<u8>,
}

impl Envelope {
    /// Bind local deduplication records to authenticated content. Hop budget is
    /// mutable in transit and deliberately excluded; this hash is not a signature.
    pub fn content_hash(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"pathweft/stored-envelope/v1");
        hash.update([self.version]);
        hash.update(self.id.as_bytes());
        hash.update(self.source.0);
        hash.update(self.destination.0);
        hash.update(self.nonce);
        hash.update(&self.ciphertext);
        hash.finalize().into()
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.version != VERSION {
            return Err(Error::InvalidEnvelope("unsupported version"));
        }
        if self.id.is_nil() {
            return Err(Error::InvalidEnvelope("nil message id"));
        }
        if self.source == self.destination {
            return Err(Error::InvalidEnvelope("identical endpoints"));
        }
        if self.remaining_hops > MAX_HOPS {
            return Err(Error::InvalidEnvelope("excessive hop budget"));
        }
        if !(16..=MAX_CIPHERTEXT_BYTES).contains(&self.ciphertext.len()) {
            return Err(Error::InvalidEnvelope("invalid ciphertext size"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Payload {
    Text {
        text: String,
    },
    Receipt {
        message_id: Uuid,
    },
    ReceiptRequest {
        message_id: Uuid,
    },
    /// A fresh outer envelope carries the unchanged sealed text after an
    /// uncertain transmission. The recipient deduplicates the inner message.
    Redelivery {
        packet: Envelope,
    },
}

impl Payload {
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Text { text } if text.is_empty() || text.len() > MAX_TEXT_BYTES => {
                Err(Error::InvalidText)
            }
            Self::Receipt { message_id } | Self::ReceiptRequest { message_id }
                if message_id.is_nil() =>
            {
                Err(Error::InvalidEnvelope("nil receipt id"))
            }
            Self::Redelivery { packet } => {
                packet.validate()?;
                if packet.remaining_hops != DEFAULT_HOPS {
                    return Err(Error::InvalidEnvelope(
                        "redelivery must contain the original hop budget",
                    ));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProtectedContent {
    pub domain: String,
    pub version: u8,
    pub id: Uuid,
    pub source: NodeId,
    pub destination: NodeId,
    pub payload: Payload,
}

#[cfg(test)]
mod tests;
