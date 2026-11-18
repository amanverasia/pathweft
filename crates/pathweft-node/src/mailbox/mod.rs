use crate::storage;
use anyhow::{Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use pathweft_core::{DEFAULT_HOPS, Envelope, Identity, PublicKey, seal_anonymous};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{collections::HashSet, net::SocketAddr, path::PathBuf, time::Duration};
use uuid::Uuid;

pub(crate) mod protocol;
pub(crate) mod routed;
pub(crate) mod store;

pub const DEFAULT_WAIT_MS: u64 = 24 * 60 * 60 * 1000;
pub const MAX_WAIT_MS: u64 = 7 * DEFAULT_WAIT_MS;
const MAX_SEALED_BYTES: usize = 16 * 1024;
const DEPOSIT_DOMAIN: &str = "pathweft/mailbox/copy/v1";
const DIRECT_HOPS: u8 = 2;

pub(crate) fn direct_leg() -> u8 {
    1
}

pub(crate) fn is_direct_leg(hops: &u8) -> bool {
    *hops == direct_leg()
}

pub(crate) fn default_wait() -> u64 {
    DEFAULT_WAIT_MS
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MailboxConfig {
    #[serde(default = "default_wait")]
    pub max_wait_ms: u64,
    #[serde(default)]
    pub trusted_wall_clock: bool,
}

impl MailboxConfig {
    pub(crate) fn validate(self) -> Result<()> {
        ensure!(
            (1000..=MAX_WAIT_MS).contains(&self.max_wait_ms),
            "mailbox maximum wait must be between one second and seven days"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MailboxRelay {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<SocketAddr>,
    pub public_key: PublicKey,
}

impl MailboxRelay {
    pub(crate) fn validate(self, owner: pathweft_core::NodeId) -> Result<()> {
        ensure!(
            self.address
                .is_none_or(|address| address.ip().is_loopback() && address.port() != 0),
            "prototype mailbox relay requires a loopback address with a port"
        );
        ensure!(
            self.public_key.node_id() != owner,
            "mailbox relay must be another node"
        );
        Ok(())
    }
}

/// A provisioned discovery key used only to find and talk to the mailbox relay.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MailboxRoute {
    pub relay_key: PublicKey,
    pub identity: PathBuf,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PickupConfig {
    pub relay: MailboxRelay,
    pub identity: PathBuf,
    pub labels: Vec<Uuid>,
}

impl PickupConfig {
    pub(crate) fn validate(&self, owner: pathweft_core::NodeId) -> Result<()> {
        self.relay.validate(owner)?;
        let labels: HashSet<_> = self.labels.iter().collect();
        ensure!(
            (1..=16).contains(&labels.len())
                && labels.len() == self.labels.len()
                && !self.labels.iter().any(Uuid::is_nil),
            "pickup requires 1..16 distinct non-nil labels"
        );
        Ok(())
    }

    pub(crate) fn load_identity(&self, owner: pathweft_core::NodeId) -> Result<Identity> {
        let identity = storage::load_identity(&self.identity)?;
        ensure!(
            identity.id() != owner && identity.public_key() != self.relay.public_key,
            "pickup requires a separate mailbox identity"
        );
        Ok(identity)
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Copy {
    pub id: Uuid,
    pub hops_after_deposit: u8,
    #[serde(with = "encoded_bytes")]
    pub sealed: Vec<u8>,
}

impl Copy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.id.is_nil()
                && (1..DEFAULT_HOPS).contains(&self.hops_after_deposit)
                && (48..=MAX_SEALED_BYTES).contains(&self.sealed.len()),
            "invalid mailbox copy"
        );
        Ok(())
    }

    pub fn open(
        &self,
        recipient: &Identity,
        label: Uuid,
        deposit_hops: u8,
        pickup_hops: u8,
    ) -> Result<Envelope> {
        self.validate()?;
        let raw = recipient.open_anonymous(&self.sealed)?;
        let (domain, sealed_label, id, mut packet): (String, Uuid, Uuid, Envelope) =
            serde_json::from_slice(&raw)?;
        ensure!(
            domain == DEPOSIT_DOMAIN
                && label == sealed_label
                && self.id == id
                && packet.remaining_hops == self.hops_after_deposit + 1,
            "mailbox copy does not match its sealed metadata"
        );
        ensure!(
            deposit_hops > 0 && pickup_hops > 0,
            "mailbox copy did not traverse both legs"
        );
        packet.remaining_hops = packet
            .remaining_hops
            .checked_sub(deposit_hops.saturating_add(pickup_hops))
            .ok_or_else(|| anyhow::anyhow!("mailbox hop budget exhausted"))?;
        Ok(packet)
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Deposit {
    pub relay: MailboxRelay,
    pub label: Uuid,
    pub copy: Copy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<DepositResult>,
}

impl Deposit {
    pub fn new(
        relay: MailboxRelay,
        label: Uuid,
        key: PublicKey,
        packet: &Envelope,
    ) -> Result<Self> {
        ensure!(!label.is_nil(), "mailbox label must not be nil");
        ensure!(
            packet.remaining_hops >= DIRECT_HOPS,
            "mailbox requires two available hops"
        );
        relay.validate(packet.source)?;
        let id = Uuid::new_v4();
        let sealed = seal_anonymous(
            key,
            &serde_json::to_vec(&(DEPOSIT_DOMAIN, label, id, packet))?,
        )?;
        let copy = Copy {
            id,
            hops_after_deposit: packet.remaining_hops - 1,
            sealed,
        };
        copy.validate()?;
        Ok(Self {
            relay,
            label,
            copy,
            result: None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DepositResult {
    Stored,
    Retired,
    Full,
}

pub(crate) fn pickup_delay(consecutive_failures: u32, page_received: bool) -> Duration {
    let ms = match consecutive_failures {
        0 if page_received => 250,
        0 => 2000,
        failures => (2000_u64 << failures.min(4)).min(30_000),
    };
    Duration::from_millis(ms)
}

mod encoded_bytes {
    use super::*;

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(&encoded).map_err(de::Error::custom)
    }
}
