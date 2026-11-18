//! Shared desktop/Android TCP node service and private local control protocol.
mod inbox;
mod mailbox;
mod node;
mod outbox;
pub mod storage;
pub mod wire;
pub use mailbox::{
    DEFAULT_WAIT_MS, MAX_WAIT_MS, MailboxConfig, MailboxRelay, MailboxRoute, PickupConfig,
};
pub use node::{Config, DiscoveryLink, DiscoveryLinkKind, Node, Route, run};
use pathweft_core::{NodeId, PublicKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{net::SocketAddr, sync::Arc};
use uuid::Uuid;

pub type EventSink = Arc<dyn Fn(Value) + Send + Sync>;

#[derive(Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Send {
        to: NodeId,
        message: String,
        timeout_ms: u64,
    },
    Queue {
        to: NodeId,
        message: String,
        ttl_ms: u64,
    },
    QueueMailbox {
        to: NodeId,
        message: String,
        relay: MailboxRelay,
        label: Uuid,
        #[serde(default = "mailbox::default_wait")]
        ttl_ms: u64,
    },
    RegisterMailbox {
        label: Uuid,
        public_key: PublicKey,
    },
    Outbox,
    Inbox,
    InboxRead {
        message_id: Uuid,
    },
    InboxForget {
        message_id: Uuid,
    },
    Cancel {
        message_id: Uuid,
    },
    Forget {
        message_id: Uuid,
    },
    Status,
    Relay {
        enabled: bool,
    },
    Route {
        to: NodeId,
        via: SocketAddr,
    },
    /// Pin another endpoint's public key on a running node (in memory, like `route`).
    AddContact {
        public_key: PublicKey,
    },
    /// Report a neighbour address as reachable (needs discovery enabled).
    LinkUp {
        address: SocketAddr,
        kind: DiscoveryLinkKind,
    },
    /// Report a neighbour address as gone.
    LinkDown {
        address: SocketAddr,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
impl Response {
    pub fn success(result: Value) -> Self {
        Self {
            ok: true,
            result: Some(result),
            error: None,
        }
    }
    pub fn failure(error: impl std::fmt::Display) -> Self {
        Self {
            ok: false,
            result: None,
            error: Some(error.to_string()),
        }
    }
}
