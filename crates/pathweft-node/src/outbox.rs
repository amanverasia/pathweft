use crate::mailbox::{Deposit, DepositResult, MAX_WAIT_MS};
use crate::storage;
use anyhow::{Context, Result, ensure};
use pathweft_core::{DEFAULT_HOPS, Envelope, NodeId};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

pub(crate) const CAPACITY: usize = 64;
pub(crate) const MAX_TTL_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_FILE_BYTES: usize = 512 * 1024;
const RECEIPT_WAIT_MS: u64 = 10_000;

pub(crate) fn now_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeliveryState {
    Queued,
    InFlight,
    Unconfirmed,
    Delivered,
    Expired,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Failure {
    NoRoute,
    RecipientNotPinned,
    ConnectFailed,
    WriteFailed,
    ReceiptTimeout,
    Interrupted,
    QuerySendFailed,
    RedeliverySendFailed,
    MailboxUnavailable,
    MailboxFull,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    pub packet: Envelope,
    pub state: DeliveryState,
    created_at_ms: u64,
    expires_at_ms: u64,
    attempts: u32,
    last_attempt_ms: Option<u64>,
    next_attempt_ms: u64,
    last_error: Option<Failure>,
    receipt_id: Option<Uuid>,
    #[serde(default)]
    receipt_queries: u32,
    #[serde(default)]
    last_query_error: Option<Failure>,
    #[serde(default)]
    redeliveries: u32,
    #[serde(default)]
    last_redelivery_error: Option<Failure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox: Option<Deposit>,
}

impl Entry {
    fn summary(&self) -> Value {
        let mut result = json!({"message_id":self.packet.id,"recipient":self.packet.destination,"state":self.state,
            "created_at_ms":self.created_at_ms,"expires_at_ms":self.expires_at_ms,
            "attempts":self.attempts,"last_attempt_ms":self.last_attempt_ms,
            "last_error":self.last_error,"receipt_id":self.receipt_id,
            "receipt_queries":self.receipt_queries,"last_query_error":self.last_query_error,
            "redeliveries":self.redeliveries,"last_redelivery_error":self.last_redelivery_error});
        if let Some(mailbox) = &self.mailbox {
            result["mailbox"] = json!({"relay_result": mailbox.result});
        }
        result
    }

    pub fn lifetime(&self) -> (u64, u64) {
        (self.expires_at_ms, self.expires_at_ms - self.created_at_ms)
    }

    pub(crate) fn needs_receipt_query(&self) -> bool {
        self.receipt_queries == 0
    }

    /// True before any receipt query or redelivery was tried for this entry.
    pub(crate) fn first_recovery(&self) -> bool {
        self.receipt_queries == 0 && self.redeliveries == 0
    }

    fn schedule_recovery(&mut self, now: u64) {
        let attempts = self.receipt_queries.saturating_add(self.redeliveries);
        self.last_attempt_ms = Some(now);
        self.next_attempt_ms =
            now.saturating_add((1000_u64 << attempts.min(6).saturating_sub(1)).min(30_000));
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u8,
    owner: NodeId,
    entries: Vec<Entry>,
}

/// Only opened after the node has exclusively bound its private control socket.
pub(crate) struct Outbox {
    path: PathBuf,
    document: Document,
    failed: bool,
}

impl Outbox {
    pub fn path_for(control: &Path) -> Result<PathBuf> {
        storage::state_path(control, ".outbox.json")
    }

    #[cfg(test)]
    fn temporary(&self) -> PathBuf {
        storage::temporary_path(&self.path, self.document.owner)
    }

    pub fn open(path: PathBuf, owner: NodeId, now: u64) -> Result<Self> {
        let document = storage::read_private_json(&path, MAX_FILE_BYTES)?.unwrap_or(Document {
            version: 4,
            owner,
            entries: vec![],
        });
        Self::validate(&document, owner)?;
        storage::discard_private_temporary(&path, owner, MAX_FILE_BYTES)?;
        let mut outbox = Self {
            path,
            document,
            failed: false,
        };
        let mut next = outbox.document.clone();
        let mut changed = false;
        for entry in &mut next.entries {
            if entry.state == DeliveryState::InFlight {
                entry.state = DeliveryState::Unconfirmed;
                entry.last_error = Some(Failure::Interrupted);
                entry.next_attempt_ms = now;
                changed = true;
            } else if entry.state == DeliveryState::Queued {
                entry.next_attempt_ms = now;
                if now >= entry.expires_at_ms {
                    entry.state = DeliveryState::Expired;
                }
                changed = true;
            } else if entry.state == DeliveryState::Unconfirmed {
                entry.next_attempt_ms = now;
                changed = true;
            }
        }
        if changed {
            outbox.commit(next)?;
        }
        Ok(outbox)
    }

    fn validate(document: &Document, owner: NodeId) -> Result<()> {
        ensure!(
            matches!(document.version, 1..=4) && document.owner == owner,
            "outbox version/owner mismatch"
        );
        ensure!(
            document.entries.len() <= CAPACITY,
            "outbox entry limit exceeded"
        );
        let mut ids = HashSet::new();
        for entry in &document.entries {
            entry.packet.validate()?;
            ensure!(
                entry.packet.source == owner && entry.packet.remaining_hops == DEFAULT_HOPS,
                "invalid outgoing envelope owner/hops"
            );
            ensure!(ids.insert(entry.packet.id), "duplicate outbox message id");
            let max_ttl = if let Some(mailbox) = &entry.mailbox {
                ensure!(
                    document.version == 4
                        && !mailbox.label.is_nil()
                        && mailbox.copy.hops_after_deposit == DEFAULT_HOPS - 1
                        && mailbox.result != Some(DepositResult::Full),
                    "invalid outbox mailbox deposit"
                );
                mailbox.copy.validate()?;
                mailbox.relay.validate(owner)?;
                MAX_WAIT_MS
            } else {
                MAX_TTL_MS
            };
            ensure!(
                entry.expires_at_ms > entry.created_at_ms
                    && entry.expires_at_ms - entry.created_at_ms <= max_ttl,
                "invalid outbox expiry"
            );
            ensure!(entry.attempts <= 100_000, "invalid outbox attempts");
            ensure!(
                entry.receipt_queries <= 100_000 && entry.redeliveries <= 100_000,
                "invalid receipt recovery count"
            );
            ensure!(
                entry.receipt_id.is_some() == (entry.state == DeliveryState::Delivered),
                "invalid outbox receipt state"
            );
            ensure!(
                entry.receipt_id.is_none_or(|id| !id.is_nil()),
                "nil outbox receipt id"
            );
            ensure!(
                entry.last_attempt_ms.is_some() == (entry.attempts > 0),
                "invalid outbox attempt state"
            );
            ensure!(
                !matches!(
                    entry.state,
                    DeliveryState::InFlight | DeliveryState::Unconfirmed | DeliveryState::Delivered
                ) || entry.attempts > 0,
                "transmitted entry has no attempt"
            );
        }
        Ok(())
    }

    fn commit(&mut self, mut next: Document) -> Result<()> {
        ensure!(
            !self.failed,
            "outbox storage failed; restart before further changes"
        );
        next.version = 4;
        Self::validate(&next, self.document.owner)?;
        let bytes = serde_json::to_vec(&next)?;
        // Capacity rejection is not an I/O failure; callers may still forget history.
        ensure!(bytes.len() <= MAX_FILE_BYTES, "outbox exceeds size limit");
        let result =
            storage::write_private_state(&self.path, self.document.owner, &bytes, MAX_FILE_BYTES);
        if let Err(error) = result {
            // A failed directory sync may follow a successful rename. Stop all
            // further mutations rather than guessing which state survived.
            self.failed = true;
            return Err(error.context("outbox storage failed; restart to inspect persisted state"));
        }
        self.document = next;
        Ok(())
    }

    pub fn list(&self) -> Value {
        json!({"storage_error":self.failed,"capacity":CAPACITY,
            "entries":self.document.entries.iter().map(Entry::summary).collect::<Vec<_>>()})
    }

    pub fn summary(&self) -> Value {
        json!({"entries":self.document.entries.len(),"storage_error":self.failed,
            "queued":self.document.entries.iter().filter(|e|e.state==DeliveryState::Queued).count(),
            "in_flight":self.document.entries.iter().filter(|e|e.state==DeliveryState::InFlight).count(),
            "unconfirmed":self.document.entries.iter().filter(|e|e.state==DeliveryState::Unconfirmed).count(),
            "delivered":self.document.entries.iter().filter(|e|e.state==DeliveryState::Delivered).count()})
    }

    pub fn enqueue(&mut self, packet: Envelope, ttl_ms: u64, now: u64) -> Result<Value> {
        self.enqueue_entry(packet, ttl_ms, now, None)
    }

    pub fn enqueue_mailbox(
        &mut self,
        packet: Envelope,
        ttl_ms: u64,
        now: u64,
        deposit: Deposit,
    ) -> Result<Value> {
        self.enqueue_entry(packet, ttl_ms, now, Some(deposit))
    }

    fn enqueue_entry(
        &mut self,
        packet: Envelope,
        ttl_ms: u64,
        now: u64,
        mailbox: Option<Deposit>,
    ) -> Result<Value> {
        let max_ttl = if mailbox.is_some() {
            MAX_WAIT_MS
        } else {
            MAX_TTL_MS
        };
        ensure!(
            (1000..=max_ttl).contains(&ttl_ms),
            "ttl_ms must be 1000..{max_ttl}"
        );
        ensure!(
            self.document.entries.len() < CAPACITY,
            "outbox full; forget terminal entries first"
        );
        let entry = Entry {
            packet,
            state: DeliveryState::Queued,
            created_at_ms: now,
            expires_at_ms: now.checked_add(ttl_ms).context("outbox expiry overflow")?,
            attempts: 0,
            last_attempt_ms: None,
            next_attempt_ms: now,
            last_error: None,
            receipt_id: None,
            receipt_queries: 0,
            last_query_error: None,
            redeliveries: 0,
            last_redelivery_error: None,
            mailbox,
        };
        let summary = entry.summary();
        let mut next = self.document.clone();
        next.entries.push(entry);
        self.commit(next)?;
        Ok(summary)
    }

    /// Persist the attempted transmission before the worker contacts the relay.
    pub fn begin_mailbox_attempt(&mut self, id: Uuid, now: u64) -> Result<bool> {
        let mut next = self.document.clone();
        let Some(entry) = next.entries.iter_mut().find(|entry| {
            entry.packet.id == id
                && matches!(
                    entry.state,
                    DeliveryState::Queued | DeliveryState::Unconfirmed
                )
                && entry
                    .mailbox
                    .as_ref()
                    .is_some_and(|deposit| deposit.result.is_none())
                && now < entry.expires_at_ms
        }) else {
            return Ok(false);
        };
        entry.attempts = entry.attempts.saturating_add(1);
        entry.last_attempt_ms = Some(now);
        entry.next_attempt_ms =
            now.saturating_add((1000_u64 << entry.attempts.min(6).saturating_sub(1)).min(30_000));
        entry.state = DeliveryState::InFlight;
        entry.last_error = None;
        self.commit(next)?;
        Ok(true)
    }

    pub fn finish_mailbox_attempt(
        &mut self,
        id: Uuid,
        result: Option<DepositResult>,
    ) -> Result<()> {
        let mut next = self.document.clone();
        if let Some(entry) = next
            .entries
            .iter_mut()
            .find(|entry| entry.packet.id == id && entry.state == DeliveryState::InFlight)
        {
            let deposit = entry
                .mailbox
                .as_mut()
                .context("outbox entry has no mailbox deposit")?;
            entry.state = DeliveryState::Unconfirmed;
            match result {
                Some(DepositResult::Full) => entry.last_error = Some(Failure::MailboxFull),
                None => entry.last_error = Some(Failure::MailboxUnavailable),
                Some(accepted) => deposit.result = Some(accepted),
            }
            self.commit(next)?;
        }
        Ok(())
    }

    pub fn ready(&mut self, now: u64) -> Result<Option<Entry>> {
        ensure!(
            !self.failed,
            "outbox storage failed; restart before further changes"
        );
        let mut next = self.document.clone();
        let mut changed = false;
        for entry in &mut next.entries {
            if entry.state == DeliveryState::Queued && now >= entry.expires_at_ms {
                entry.state = DeliveryState::Expired;
                changed = true;
            } else if entry.state == DeliveryState::InFlight
                && now.saturating_sub(entry.last_attempt_ms.unwrap()) >= RECEIPT_WAIT_MS
            {
                entry.state = DeliveryState::Unconfirmed;
                entry.last_error = Some(Failure::ReceiptTimeout);
                entry.next_attempt_ms = now;
                changed = true;
            }
        }
        if changed {
            self.commit(next)?;
        }
        Ok(self
            .document
            .entries
            .iter()
            .filter(|e| {
                (e.state == DeliveryState::Queued
                    || (e.state == DeliveryState::Unconfirmed && now < e.expires_at_ms))
                    && e.next_attempt_ms <= now
            })
            .min_by_key(|e| (e.next_attempt_ms, e.created_at_ms, e.packet.id))
            .cloned())
    }

    pub fn attempt(&mut self, id: Uuid, now: u64, error: Option<Failure>) -> Result<()> {
        let mut next = self.document.clone();
        let entry = next
            .entries
            .iter_mut()
            .find(|e| e.packet.id == id && e.state == DeliveryState::Queued)
            .context("message is no longer queued")?;
        entry.attempts = entry
            .attempts
            .checked_add(1)
            .context("outbox attempt overflow")?;
        entry.last_attempt_ms = Some(now);
        entry.next_attempt_ms =
            now.saturating_add((1000_u64 << entry.attempts.min(6).saturating_sub(1)).min(30_000));
        entry.last_error = error;
        self.commit(next)
    }

    pub fn note_failure(&mut self, id: Uuid, reason: Failure) -> Result<()> {
        let mut next = self.document.clone();
        if let Some(entry) = next
            .entries
            .iter_mut()
            .find(|e| e.packet.id == id && e.state == DeliveryState::Queued)
        {
            entry.last_error = Some(reason);
            self.commit(next)?;
        }
        Ok(())
    }

    pub fn wait_ms(&self, now: u64) -> Option<u64> {
        if self.failed {
            return None;
        }
        self.document
            .entries
            .iter()
            .filter_map(|entry| match entry.state {
                DeliveryState::Queued => Some(entry.next_attempt_ms.min(entry.expires_at_ms)),
                DeliveryState::InFlight => Some(
                    entry
                        .last_attempt_ms
                        .unwrap()
                        .saturating_add(RECEIPT_WAIT_MS),
                ),
                DeliveryState::Unconfirmed if now < entry.expires_at_ms => {
                    Some(entry.next_attempt_ms.min(entry.expires_at_ms))
                }
                _ => None,
            })
            .min()
            .map(|deadline| deadline.saturating_sub(now).clamp(50, 30_000))
    }

    /// Commit this state before writing even one byte. Cancellation/expiry can
    /// win while the worker is awaiting its connection; in that case do not send.
    pub fn begin_transmission(&mut self, id: Uuid, now: u64) -> Result<bool> {
        let mut next = self.document.clone();
        let Some(entry) = next
            .entries
            .iter_mut()
            .find(|e| e.packet.id == id && e.state == DeliveryState::Queued)
        else {
            return Ok(false);
        };
        if now >= entry.expires_at_ms {
            entry.state = DeliveryState::Expired;
            self.commit(next)?;
            return Ok(false);
        }
        entry.state = DeliveryState::InFlight;
        entry.last_attempt_ms = Some(now);
        entry.last_error = None;
        self.commit(next)?;
        Ok(true)
    }

    pub fn unconfirmed(&mut self, id: Uuid, reason: Failure) -> Result<()> {
        let mut next = self.document.clone();
        if let Some(entry) = next
            .entries
            .iter_mut()
            .find(|e| e.packet.id == id && e.state == DeliveryState::InFlight)
        {
            entry.state = DeliveryState::Unconfirmed;
            entry.last_error = Some(reason);
            self.commit(next)?;
        }
        Ok(())
    }

    pub fn confirm(&mut self, id: Uuid, source: NodeId, receipt_id: Uuid) -> Result<bool> {
        let mut next = self.document.clone();
        let Some(entry) = next.entries.iter_mut().find(|e| {
            e.packet.id == id
                && e.packet.destination == source
                && matches!(
                    e.state,
                    DeliveryState::InFlight | DeliveryState::Unconfirmed
                )
        }) else {
            return Ok(false);
        };
        entry.state = DeliveryState::Delivered;
        entry.receipt_id = Some(receipt_id);
        entry.last_error = None;
        entry.last_query_error = None;
        entry.last_redelivery_error = None;
        self.commit(next)?;
        Ok(true)
    }

    pub fn query_attempt(&mut self, id: Uuid, now: u64, error: Option<Failure>) -> Result<bool> {
        let mut next = self.document.clone();
        let Some(entry) = next.entries.iter_mut().find(|e| {
            e.packet.id == id && e.state == DeliveryState::Unconfirmed && now < e.expires_at_ms
        }) else {
            return Ok(false);
        };
        entry.receipt_queries = entry
            .receipt_queries
            .checked_add(1)
            .context("receipt query count overflow")?;
        entry.schedule_recovery(now);
        entry.last_query_error = error;
        self.commit(next)?;
        Ok(true)
    }

    pub fn query_failed(&mut self, id: Uuid) -> Result<()> {
        let mut next = self.document.clone();
        if let Some(entry) = next
            .entries
            .iter_mut()
            .find(|e| e.packet.id == id && e.state == DeliveryState::Unconfirmed)
        {
            entry.last_query_error = Some(Failure::QuerySendFailed);
            self.commit(next)?;
        }
        Ok(())
    }

    pub fn redelivery_attempt(
        &mut self,
        id: Uuid,
        now: u64,
        error: Option<Failure>,
    ) -> Result<bool> {
        let mut next = self.document.clone();
        let Some(entry) = next.entries.iter_mut().find(|e| {
            e.packet.id == id && e.state == DeliveryState::Unconfirmed && now < e.expires_at_ms
        }) else {
            return Ok(false);
        };
        entry.redeliveries = entry
            .redeliveries
            .checked_add(1)
            .context("redelivery count overflow")?;
        entry.schedule_recovery(now);
        entry.last_redelivery_error = error;
        self.commit(next)?;
        Ok(true)
    }

    pub fn redelivery_failed(&mut self, id: Uuid) -> Result<()> {
        let mut next = self.document.clone();
        if let Some(entry) = next
            .entries
            .iter_mut()
            .find(|e| e.packet.id == id && e.state == DeliveryState::Unconfirmed)
        {
            entry.last_redelivery_error = Some(Failure::RedeliverySendFailed);
            self.commit(next)?;
        }
        Ok(())
    }

    pub fn cancel(&mut self, id: Uuid) -> Result<Value> {
        let mut next = self.document.clone();
        let entry = next
            .entries
            .iter_mut()
            .find(|e| e.packet.id == id)
            .context("unknown outbox message")?;
        ensure!(
            entry.state == DeliveryState::Queued,
            "only unsent queued messages can be cancelled"
        );
        entry.state = DeliveryState::Cancelled;
        let summary = entry.summary();
        self.commit(next)?;
        Ok(summary)
    }

    pub fn forget(&mut self, id: Uuid) -> Result<Value> {
        let mut next = self.document.clone();
        let index = next
            .entries
            .iter()
            .position(|e| e.packet.id == id)
            .context("unknown outbox message")?;
        ensure!(
            !matches!(
                next.entries[index].state,
                DeliveryState::Queued | DeliveryState::InFlight
            ),
            "cancel unsent messages before forgetting; in-flight messages must settle first"
        );
        next.entries.remove(index);
        self.commit(next)?;
        Ok(json!({"forgotten":id}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MailboxRelay;
    use pathweft_core::{Identity, Payload};
    use std::{fs, os::unix::fs::PermissionsExt};
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn fixture() -> (Directory, Identity, Identity, Outbox) {
        let directory =
            Directory(std::env::temp_dir().join(format!("pw-outbox-{}", Uuid::new_v4())));
        storage::private_directory(&directory.0).unwrap();
        let owner = Identity::generate().unwrap();
        let peer = Identity::generate().unwrap();
        let outbox = Outbox::open(directory.0.join("outbox.json"), owner.id(), 1000).unwrap();
        (directory, owner, peer, outbox)
    }
    fn packet(owner: &Identity, peer: &Identity) -> Envelope {
        owner
            .seal(
                peer.public_key(),
                Payload::Text {
                    text: "private durable marker".into(),
                },
            )
            .unwrap()
    }
    #[test]
    fn restart_preserves_ciphertext_and_marks_possible_transmission_unconfirmed() {
        let (_directory, owner, peer, mut outbox) = fixture();
        let packet = packet(&owner, &peer);
        let id = packet.id;
        outbox.enqueue(packet.clone(), 60_000, 1000).unwrap();
        let bytes = fs::read(&outbox.path).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("private durable marker"));
        assert_eq!(
            fs::metadata(&outbox.path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let mut reopened = Outbox::open(outbox.path.clone(), owner.id(), 2000).unwrap();
        assert_eq!(
            reopened.ready(2000).unwrap().unwrap().packet.ciphertext,
            packet.ciphertext
        );
        reopened.attempt(id, 2000, None).unwrap();
        assert!(reopened.begin_transmission(id, 2000).unwrap());
        let mut restarted = Outbox::open(outbox.path.clone(), owner.id(), 3000).unwrap();
        assert_eq!(
            restarted.ready(3000).unwrap().unwrap().state,
            DeliveryState::Unconfirmed
        );
        assert_eq!(restarted.list()["entries"][0]["state"], "unconfirmed");
        assert!(!restarted.confirm(id, owner.id(), Uuid::new_v4()).unwrap());
        assert!(restarted.confirm(id, peer.id(), Uuid::new_v4()).unwrap());
        let final_state = Outbox::open(outbox.path.clone(), owner.id(), 4000).unwrap();
        assert_eq!(final_state.list()["entries"][0]["state"], "delivered");
    }
    #[test]
    fn expiry_cancel_and_history_capacity_are_bounded() {
        let (_directory, owner, peer, mut outbox) = fixture();
        let expired = packet(&owner, &peer);
        outbox.enqueue(expired.clone(), 1000, 1000).unwrap();
        assert!(outbox.ready(2000).unwrap().is_none());
        assert_eq!(outbox.list()["entries"][0]["state"], "expired");
        outbox.forget(expired.id).unwrap();
        for _ in 0..CAPACITY {
            let p = packet(&owner, &peer);
            let id = p.id;
            outbox.enqueue(p, 10_000, 1000).unwrap();
            assert!(outbox.forget(id).is_err());
            outbox.cancel(id).unwrap();
            assert!(!outbox.begin_transmission(id, 1100).unwrap());
        }
        assert!(outbox.enqueue(packet(&owner, &peer), 10_000, 1000).is_err());
        assert_eq!(outbox.list()["entries"].as_array().unwrap().len(), CAPACITY);
        assert!(serde_json::to_vec(&outbox.list()).unwrap().len() < crate::wire::MAX_FRAME_BYTES);
    }
    #[test]
    fn corrupt_foreign_oversized_and_symlink_state_are_preserved_and_rejected() {
        let (_directory, owner, peer, mut outbox) = fixture();
        outbox.enqueue(packet(&owner, &peer), 10_000, 1000).unwrap();
        assert!(Outbox::open(outbox.path.clone(), peer.id(), 2000).is_err());
        let bytes = fs::read(&outbox.path).unwrap();
        fs::set_permissions(&outbox.path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Outbox::open(outbox.path.clone(), owner.id(), 2000).is_err());
        fs::set_permissions(&outbox.path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&outbox.path, b"{").unwrap();
        assert!(Outbox::open(outbox.path.clone(), owner.id(), 2000).is_err());
        assert_eq!(fs::read(&outbox.path).unwrap(), b"{");
        fs::write(&outbox.path, vec![b' '; MAX_FILE_BYTES + 1]).unwrap();
        assert!(Outbox::open(outbox.path.clone(), owner.id(), 2000).is_err());
        let target = outbox.path.with_extension("preserved");
        fs::write(&target, &bytes).unwrap();
        fs::remove_file(&outbox.path).unwrap();
        std::os::unix::fs::symlink(&target, &outbox.path).unwrap();
        assert!(Outbox::open(outbox.path.clone(), owner.id(), 2000).is_err());
        assert_eq!(fs::read(&target).unwrap(), bytes);
    }
    #[test]
    fn storage_failure_blocks_sending_and_further_mutation() {
        let (_directory, owner, peer, mut outbox) = fixture();
        let p = packet(&owner, &peer);
        let id = p.id;
        outbox.enqueue(p, 10_000, 1000).unwrap();
        outbox.attempt(id, 1000, None).unwrap();
        fs::create_dir(outbox.temporary()).unwrap(); // Force create_new to fail.
        assert!(outbox.begin_transmission(id, 1100).is_err());
        assert_eq!(outbox.list()["storage_error"], true);
        assert!(outbox.ready(1200).is_err());
        assert!(outbox.cancel(id).is_err());
        assert_eq!(outbox.list()["entries"][0]["state"], "queued");
    }
    #[test]
    fn missing_receipt_never_requeues_and_late_receipt_can_confirm() {
        let (_directory, owner, peer, mut outbox) = fixture();
        let p = packet(&owner, &peer);
        let id = p.id;
        outbox.enqueue(p, 60_000, 1000).unwrap();
        outbox.attempt(id, 1000, None).unwrap();
        outbox.begin_transmission(id, 1000).unwrap();
        assert!(outbox.cancel(id).is_err());
        assert!(outbox.forget(id).is_err());
        assert_eq!(
            outbox.ready(11_000).unwrap().unwrap().state,
            DeliveryState::Unconfirmed
        );
        assert_eq!(outbox.list()["entries"][0]["state"], "unconfirmed");
        assert!(outbox.confirm(id, peer.id(), Uuid::new_v4()).unwrap());
        outbox.forget(id).unwrap();
        assert!(outbox.list()["entries"].as_array().unwrap().is_empty());
    }

    #[test]
    fn receipt_recovery_schedule_preserves_the_original_and_stops_at_expiry() {
        let (_directory, owner, peer, mut outbox) = fixture();
        let p = packet(&owner, &peer);
        let id = p.id;
        let cipher = p.ciphertext.clone();
        outbox.enqueue(p, 60_000, 1000).unwrap();
        outbox.attempt(id, 1000, None).unwrap();
        outbox.begin_transmission(id, 1000).unwrap();
        assert_eq!(
            outbox.ready(11_000).unwrap().unwrap().state,
            DeliveryState::Unconfirmed
        );
        assert!(outbox.query_attempt(id, 11_000, None).unwrap());
        assert!(outbox.ready(11_500).unwrap().is_none());
        assert_eq!(
            outbox.ready(12_000).unwrap().unwrap().packet.ciphertext,
            cipher
        );
        assert_eq!(outbox.list()["entries"][0]["receipt_queries"], 1);
        assert_eq!(outbox.list()["entries"][0]["attempts"], 1);
        assert!(outbox.redelivery_attempt(id, 12_000, None).unwrap());
        assert!(outbox.ready(13_000).unwrap().is_none());
        assert_eq!(
            outbox.ready(14_000).unwrap().unwrap().packet.ciphertext,
            cipher
        );
        assert_eq!(outbox.list()["entries"][0]["redeliveries"], 1);
        assert_eq!(outbox.list()["entries"][0]["attempts"], 1);
        assert!(outbox.ready(61_000).unwrap().is_none());
        assert!(!outbox.query_attempt(id, 61_000, None).unwrap());
        assert!(outbox.wait_ms(61_000).is_none());
    }

    #[test]
    fn full_mailbox_preserves_the_original_deposit_and_waits_only_until_expiry() {
        let (_directory, owner, peer, mut outbox) = fixture();
        let relay = MailboxRelay {
            address: Some("127.0.0.1:4501".parse().unwrap()),
            public_key: Identity::generate().unwrap().public_key(),
        };
        let packet = packet(&owner, &peer);
        let id = packet.id;
        let label = Uuid::new_v4();
        let deposit = Deposit::new(relay, label, peer.public_key(), &packet).unwrap();
        let copy = deposit.copy.clone();
        outbox
            .enqueue_mailbox(packet, MAX_WAIT_MS, 1000, deposit)
            .unwrap();
        assert!(outbox.begin_mailbox_attempt(id, 1000).unwrap());
        outbox
            .finish_mailbox_attempt(id, Some(DepositResult::Full))
            .unwrap();
        assert_eq!(outbox.list()["entries"][0]["last_error"], "mailbox_full");
        assert!(outbox.ready(1999).unwrap().is_none());

        let mut reopened = Outbox::open(outbox.path.clone(), owner.id(), 2000).unwrap();
        let entry = reopened.ready(2000).unwrap().unwrap();
        assert_eq!(entry.lifetime(), (1000 + MAX_WAIT_MS, MAX_WAIT_MS));
        let retry = entry.mailbox.unwrap();
        assert_eq!(retry.copy.id, copy.id);
        assert_eq!(retry.copy.sealed, copy.sealed);
        assert!(retry.result.is_none());
        assert!(reopened.ready(MAX_WAIT_MS + 1000).unwrap().is_none());
        assert!(
            !reopened
                .begin_mailbox_attempt(id, MAX_WAIT_MS + 1000)
                .unwrap()
        );
    }

    #[test]
    fn version_one_records_upgrade_without_losing_pending_text() {
        let (_directory, owner, peer, mut outbox) = fixture();
        let p = packet(&owner, &peer);
        let id = p.id;
        outbox.enqueue(p, 60_000, 1000).unwrap();
        let mut old: Value = serde_json::from_slice(&fs::read(&outbox.path).unwrap()).unwrap();
        old["version"] = json!(1);
        let row = old["entries"][0].as_object_mut().unwrap();
        row.remove("receipt_queries");
        row.remove("last_query_error");
        row.remove("redeliveries");
        row.remove("last_redelivery_error");
        fs::write(&outbox.path, serde_json::to_vec(&old).unwrap()).unwrap();
        let mut resumed = Outbox::open(outbox.path.clone(), owner.id(), 2000).unwrap();
        assert_eq!(resumed.ready(2000).unwrap().unwrap().packet.id, id);
        assert_eq!(resumed.list()["entries"][0]["receipt_queries"], 0);
        assert_eq!(resumed.list()["entries"][0]["redeliveries"], 0);
        resumed.cancel(id).unwrap();
        let updated: Value = serde_json::from_slice(&fs::read(&outbox.path).unwrap()).unwrap();
        assert_eq!(updated["version"], 4);
    }
}
