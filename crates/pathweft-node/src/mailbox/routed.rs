//! One bounded mailbox exchange over individually forwarded discovery datagrams.
use crate::node::State;
use anyhow::{Context, Result, ensure};
use pathweft_core::{DEFAULT_HOPS, NodeId};
use std::{collections::HashMap, sync::Mutex};
use tokio::sync::mpsc;
use uuid::Uuid;

const MAX_SESSIONS: usize = 32;
const MAX_HELLO_BYTES: usize = 512;
const OPEN: u8 = 0;
const TO_RELAY: u8 = 1;
const TO_CLIENT: u8 = 2;
type Slot = (NodeId, Uuid, u8);
type Datagram = (Vec<u8>, u8);

pub(crate) struct Start {
    peer: NodeId,
    id: Uuid,
    pub hello: Vec<u8>,
}

pub(crate) struct Network {
    slots: Mutex<HashMap<Slot, mpsc::Sender<Datagram>>>,
    starts: mpsc::Sender<Start>,
}

impl Network {
    pub fn new() -> (Self, mpsc::Receiver<Start>) {
        let (starts, receive) = mpsc::channel(MAX_SESSIONS);
        (
            Self {
                slots: Mutex::new(HashMap::new()),
                starts,
            },
            receive,
        )
    }

    pub fn incoming(&self, peer: NodeId, bytes: Vec<u8>, hops: u8, allow_open: bool) {
        let Some((&kind, bytes)) = bytes.split_first() else {
            return;
        };
        let Some((id, payload)) = bytes.split_at_checked(16) else {
            return;
        };
        let Ok(id) = Uuid::from_slice(id) else { return };
        if id.is_nil() || payload.is_empty() {
            return;
        }
        if kind == OPEN && allow_open && payload.len() <= MAX_HELLO_BYTES {
            let _ = self.starts.try_send(Start {
                peer,
                id,
                hello: payload.to_vec(),
            });
        } else if matches!(kind, TO_RELAY | TO_CLIENT)
            && let Some(inbox) = self.slots.lock().unwrap().get(&(peer, id, kind))
        {
            let _ = inbox.try_send((payload.to_vec(), hops));
        }
    }
}

pub(crate) struct Circuit<'a> {
    node: &'a State,
    slot: Slot,
    inbox: mpsc::Receiver<Datagram>,
}

impl<'a> Circuit<'a> {
    pub fn client(node: &'a State, peer: NodeId) -> Result<Self> {
        Self::register(node, (peer, Uuid::new_v4(), TO_CLIENT))
    }

    pub fn relay(node: &'a State, start: &Start) -> Result<Self> {
        Self::register(node, (start.peer, start.id, TO_RELAY))
    }

    fn register(node: &'a State, slot: Slot) -> Result<Self> {
        let mut slots = node.mailbox_network.slots.lock().unwrap();
        ensure!(
            slots.len() < MAX_SESSIONS && !slots.contains_key(&slot),
            "mailbox circuit capacity reached or already open"
        );
        let (send, inbox) = mpsc::channel(1);
        slots.insert(slot, send);
        Ok(Self { node, slot, inbox })
    }

    pub async fn open(&self, bytes: Vec<u8>) -> Result<()> {
        self.send_packet(OPEN, bytes, DEFAULT_HOPS).await
    }

    pub async fn send(&self, bytes: Vec<u8>, max_hops: u8) -> Result<()> {
        let kind = if self.slot.2 == TO_CLIENT {
            TO_RELAY
        } else {
            TO_CLIENT
        };
        self.send_packet(kind, bytes, max_hops).await
    }

    async fn send_packet(&self, kind: u8, bytes: Vec<u8>, max_hops: u8) -> Result<()> {
        let (peer, id, _) = self.slot;
        let mut packet = Vec::with_capacity(bytes.len() + 17);
        packet.push(kind);
        packet.extend(id.as_bytes());
        packet.extend(bytes);
        self.node
            .send_mailbox_datagram(peer, packet, max_hops)
            .await
    }

    pub async fn receive(&mut self) -> Result<Datagram> {
        self.inbox.recv().await.context("mailbox circuit closed")
    }
}

impl Drop for Circuit<'_> {
    fn drop(&mut self) {
        self.node
            .mailbox_network
            .slots
            .lock()
            .unwrap()
            .remove(&self.slot);
    }
}
