use super::{Copy, DepositResult, MAX_WAIT_MS, MailboxConfig, direct_leg, is_direct_leg};
use crate::{outbox, storage};
use anyhow::{Context, Result, bail, ensure};
use pathweft_core::{NodeId, PublicKey};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::Instant,
};
use uuid::Uuid;

const MAX_FILE_BYTES: usize = 4 * 1024 * 1024;
const MAX_LABELS: usize = 128;
const MAX_RECORDS: usize = 128;
const MAX_LABEL_LIVE: usize = 8;
const MAX_LABEL_RECORDS: usize = 32;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    label: Uuid,
    id: Uuid,
    digest: [u8; 32],
    #[serde(default = "direct_leg", skip_serializing_if = "is_direct_leg")]
    deposit_hops: u8,
    hold_until_ms: u64,
    forget_at_ms: u64,
    copy: Option<Copy>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u8,
    owner: NodeId,
    written_at_ms: u64,
    trusted_wall_clock: bool,
    labels: HashMap<Uuid, PublicKey>,
    records: Vec<Record>,
}

pub(crate) struct Store {
    path: PathBuf,
    document: Document,
    settings: MailboxConfig,
    clock_anchor: (Instant, u64),
    failed: bool,
}

fn digest(copy: &Copy) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update([copy.hops_after_deposit]);
    hash.update(&copy.sealed);
    hash.finalize().into()
}

impl Store {
    pub fn open(control: &Path, owner: NodeId, settings: MailboxConfig) -> Result<Self> {
        let path = storage::state_path(control, ".mailbox.json")?;
        let now = if settings.trusted_wall_clock {
            outbox::now_ms()?
        } else {
            0
        };
        Self::open_at(path, owner, settings, now)
    }

    fn open_at(path: PathBuf, owner: NodeId, settings: MailboxConfig, now: u64) -> Result<Self> {
        let document = storage::read_private_json(&path, MAX_FILE_BYTES)?.unwrap_or(Document {
            version: 1,
            owner,
            written_at_ms: now,
            trusted_wall_clock: settings.trusted_wall_clock,
            labels: HashMap::new(),
            records: Vec::new(),
        });
        Self::validate(&document, owner)?;
        storage::discard_private_temporary(&path, owner, MAX_FILE_BYTES)?;
        let mut store = Self {
            path,
            document,
            settings,
            clock_anchor: (Instant::now(), now),
            failed: false,
        };
        if !store.document.records.is_empty()
            && (!settings.trusted_wall_clock
                || !store.document.trusted_wall_clock
                || now < store.document.written_at_ms)
        {
            // With unknown downtime assume the full local hold elapsed. Keep
            // suppression until the remaining sender window has also elapsed.
            let mut next = store.document.clone();
            for record in &mut next.records {
                record.copy = None;
                record.hold_until_ms = now;
                record.forget_at_ms =
                    now.saturating_add(record.forget_at_ms.saturating_sub(next.written_at_ms));
            }
            next.records.retain(|r| r.forget_at_ms > now);
            store.commit(next, now)?;
        } else {
            store.reap_at(now)?;
        }
        Ok(store)
    }

    fn now(&mut self) -> u64 {
        let instant = Instant::now();
        let (since, at_ms) = self.clock_anchor;
        let elapsed = at_ms.saturating_add(instant.duration_since(since).as_millis() as u64);
        if self.settings.trusted_wall_clock
            && let Ok(wall) = outbox::now_ms()
            && wall > elapsed
        {
            self.clock_anchor = (instant, wall);
            return wall;
        }
        elapsed
    }

    fn validate(document: &Document, owner: NodeId) -> Result<()> {
        ensure!(
            document.version == 1 && document.owner == owner,
            "mailbox state version/owner mismatch"
        );
        ensure!(
            document.labels.len() <= MAX_LABELS && document.records.len() <= MAX_RECORDS,
            "mailbox state exceeds its record bounds"
        );
        ensure!(
            document
                .labels
                .iter()
                .all(|(label, key)| !label.is_nil() && key.node_id() != owner),
            "invalid mailbox label registration"
        );
        let mut ids = HashSet::new();
        let mut counts = HashMap::<Uuid, (usize, usize)>::new();
        for record in &document.records {
            ensure!(
                !record.id.is_nil()
                    && document.labels.contains_key(&record.label)
                    && ids.insert((record.label, record.id))
                    && (1..pathweft_core::DEFAULT_HOPS).contains(&record.deposit_hops)
                    && record.hold_until_ms <= record.forget_at_ms
                    && record.forget_at_ms > document.written_at_ms
                    && record.forget_at_ms - document.written_at_ms <= MAX_WAIT_MS,
                "invalid mailbox state record"
            );
            let (live, total) = counts.entry(record.label).or_default();
            *total += 1;
            if let Some(copy) = &record.copy {
                copy.validate()?;
                ensure!(
                    copy.id == record.id
                        && digest(copy) == record.digest
                        && record.deposit_hops <= copy.hops_after_deposit
                        && record.hold_until_ms > document.written_at_ms,
                    "invalid mailbox state copy"
                );
                *live += 1;
            }
            ensure!(
                *live <= MAX_LABEL_LIVE && *total <= MAX_LABEL_RECORDS,
                "mailbox label exceeds its record bounds"
            );
        }
        Ok(())
    }

    fn commit(&mut self, mut next: Document, now: u64) -> Result<()> {
        ensure!(
            !self.failed,
            "mailbox storage failed; restart before further changes"
        );
        next.written_at_ms = now;
        next.trusted_wall_clock = self.settings.trusted_wall_clock;
        let bytes = serde_json::to_vec(&next)?;
        ensure!(
            bytes.len() <= MAX_FILE_BYTES,
            "mailbox exceeds its storage limit"
        );
        if let Err(error) =
            storage::write_private_state(&self.path, next.owner, &bytes, MAX_FILE_BYTES)
        {
            self.failed = true;
            return Err(error.context("mailbox storage failed; restart to inspect persisted state"));
        }
        self.document = next;
        Ok(())
    }

    pub fn key(&self, label: Uuid) -> Result<PublicKey> {
        ensure!(!self.failed, "mailbox storage unavailable");
        self.document
            .labels
            .get(&label)
            .copied()
            .context("unregistered mailbox label")
    }

    pub fn register(&mut self, label: Uuid, key: PublicKey) -> Result<()> {
        ensure!(!self.failed, "mailbox storage unavailable");
        ensure!(
            !label.is_nil() && key.node_id() != self.document.owner,
            "invalid mailbox registration"
        );
        if let Some(current) = self.document.labels.get(&label) {
            ensure!(
                *current == key,
                "mailbox label already registered to a different key"
            );
            return Ok(());
        }
        ensure!(
            self.document.labels.len() < MAX_LABELS,
            "mailbox registration limit reached"
        );
        let now = self.now();
        self.reap_at(now)?;
        let mut next = self.document.clone();
        next.labels.insert(label, key);
        self.commit(next, now)
    }

    pub fn reap(&mut self) -> Result<()> {
        let now = self.now();
        self.reap_at(now)
    }

    fn reap_at(&mut self, now: u64) -> Result<()> {
        ensure!(!self.failed, "mailbox storage unavailable");
        if !self.document.records.iter().any(|record| {
            now >= record.forget_at_ms || (record.copy.is_some() && now >= record.hold_until_ms)
        }) {
            return Ok(());
        }
        let mut next = self.document.clone();
        next.records.retain(|record| now < record.forget_at_ms);
        for record in &mut next.records {
            if now >= record.hold_until_ms {
                record.copy = None;
            }
        }
        self.commit(next, now)
    }

    pub fn deposit(
        &mut self,
        label: Uuid,
        copy: Copy,
        remaining_ms: u64,
        deposit_hops: u8,
    ) -> Result<DepositResult> {
        let now = self.now();
        self.deposit_at(label, copy, remaining_ms, deposit_hops, now)
    }

    fn deposit_at(
        &mut self,
        label: Uuid,
        copy: Copy,
        remaining_ms: u64,
        deposit_hops: u8,
        now: u64,
    ) -> Result<DepositResult> {
        self.key(label)?;
        copy.validate()?;
        ensure!(
            (1..=copy.hops_after_deposit).contains(&deposit_hops),
            "mailbox deposit leaves no pickup hops"
        );
        ensure!(
            (1..=MAX_WAIT_MS).contains(&remaining_ms),
            "invalid mailbox waiting allowance"
        );
        self.reap_at(now)?;
        let digest = digest(&copy);
        if let Some(record) = self
            .document
            .records
            .iter()
            .find(|r| r.label == label && r.id == copy.id)
        {
            ensure!(
                record.digest == digest,
                "mailbox copy ID conflicts with an existing deposit"
            );
            return Ok(if record.copy.is_some() {
                DepositResult::Stored
            } else {
                DepositResult::Retired
            });
        }
        let same_label = self.document.records.iter().filter(|r| r.label == label);
        if self.document.records.len() >= MAX_RECORDS
            || same_label.clone().count() >= MAX_LABEL_RECORDS
            || same_label.filter(|r| r.copy.is_some()).count() >= MAX_LABEL_LIVE
        {
            return Ok(DepositResult::Full);
        }
        let mut next = self.document.clone();
        next.records.push(Record {
            label,
            id: copy.id,
            digest,
            deposit_hops,
            hold_until_ms: now.saturating_add(remaining_ms.min(self.settings.max_wait_ms)),
            forget_at_ms: now.saturating_add(remaining_ms),
            copy: Some(copy),
        });
        self.commit(next, now)?;
        Ok(DepositResult::Stored)
    }

    pub fn pickup(&mut self, label: Uuid, after: Option<Uuid>) -> Result<Option<(Copy, u8)>> {
        self.reap()?;
        Ok(self
            .document
            .records
            .iter()
            .filter(|r| r.label == label && after.is_none_or(|id| r.id > id))
            .filter_map(|r| r.copy.as_ref().map(|copy| (copy, r.deposit_hops)))
            .min_by_key(|(copy, _)| copy.id)
            .map(|(copy, hops)| (copy.clone(), hops)))
    }

    pub fn acknowledge(&mut self, label: Uuid, id: Uuid) -> Result<()> {
        let now = self.now();
        self.reap_at(now)?;
        let Some(index) = self
            .document
            .records
            .iter()
            .position(|r| r.label == label && r.id == id)
        else {
            bail!("mailbox copy expired before acknowledgment");
        };
        if self.document.records[index].copy.is_some() {
            let mut next = self.document.clone();
            next.records[index].copy = None;
            self.commit(next, now)?;
        }
        Ok(())
    }

    pub fn summary(&mut self) -> Value {
        let now = self.now();
        let records = self
            .document
            .records
            .iter()
            .filter(|r| now < r.forget_at_ms);
        let stored = records
            .clone()
            .filter(|r| r.copy.is_some() && now < r.hold_until_ms)
            .count();
        json!({"registered": self.document.labels.len(), "stored": stored,
            "retired": records.count() - stored, "storage_error": self.failed})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pathweft_core::{DEFAULT_HOPS, Identity};
    use std::fs;

    struct Directory(PathBuf);

    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn fixture(settings: MailboxConfig, now: u64) -> (Directory, Store, Uuid, PublicKey) {
        let directory =
            Directory(std::env::temp_dir().join(format!("pw-mailbox-{}", Uuid::new_v4())));
        let owner = Identity::generate().unwrap();
        let mut store =
            Store::open_at(directory.0.join("store.json"), owner.id(), settings, now).unwrap();
        let label = Uuid::new_v4();
        let key = Identity::generate().unwrap().public_key();
        store.register(label, key).unwrap();
        (directory, store, label, key)
    }

    fn copy() -> Copy {
        Copy {
            id: Uuid::new_v4(),
            hops_after_deposit: DEFAULT_HOPS - 1,
            sealed: vec![23; 64],
        }
    }

    #[test]
    fn caps_admission_preserves_live_copies_and_never_renews_a_deposit() {
        let settings = MailboxConfig {
            max_wait_ms: 1000,
            trusted_wall_clock: false,
        };
        let (_directory, mut store, label, _) = fixture(settings, 0);
        let first = copy();
        assert_eq!(
            store
                .deposit_at(label, first.clone(), 4000, 1, 100)
                .unwrap(),
            DepositResult::Stored
        );
        for _ in 1..MAX_LABEL_LIVE {
            assert_eq!(
                store.deposit_at(label, copy(), 5000, 1, 200).unwrap(),
                DepositResult::Stored
            );
        }
        assert_eq!(
            store.deposit_at(label, copy(), 5000, 1, 300).unwrap(),
            DepositResult::Full
        );
        let mut conflict = first.clone();
        conflict.sealed[0] ^= 1;
        assert!(store.deposit_at(label, conflict, 7000, 1, 400).is_err());
        assert_eq!(
            store
                .deposit_at(label, first.clone(), 7000, 1, 1099)
                .unwrap(),
            DepositResult::Stored
        );
        assert_eq!(
            store
                .deposit_at(label, first.clone(), 7000, 1, 1100)
                .unwrap(),
            DepositResult::Retired
        );
        assert_eq!(
            store.deposit_at(label, copy(), 4000, 1, 1101).unwrap(),
            DepositResult::Stored
        );
        assert_eq!(
            store
                .document
                .records
                .iter()
                .filter(|record| record.copy.is_some())
                .count(),
            MAX_LABEL_LIVE
        );

        store.reap_at(4100).unwrap();
        assert!(
            store
                .document
                .records
                .iter()
                .all(|record| record.id != first.id)
        );
    }

    #[test]
    fn acknowledgment_is_durable_idempotent_and_bound_to_label_and_copy() {
        let settings = MailboxConfig {
            max_wait_ms: 60_000,
            trusted_wall_clock: true,
        };
        let (_directory, mut store, label, key) = fixture(settings, outbox::now_ms().unwrap());
        let first = copy();
        store.deposit(label, first.clone(), 30_000, 1).unwrap();
        let other = Uuid::new_v4();
        store.register(other, key).unwrap();
        assert!(store.acknowledge(other, first.id).is_err());
        assert!(store.acknowledge(label, Uuid::new_v4()).is_err());
        assert_eq!(store.pickup(label, None).unwrap().unwrap().0.id, first.id);
        store.acknowledge(label, first.id).unwrap();
        store.acknowledge(label, first.id).unwrap();

        let mut reopened = Store::open_at(
            store.path.clone(),
            store.document.owner,
            settings,
            outbox::now_ms().unwrap(),
        )
        .unwrap();
        assert!(reopened.pickup(label, None).unwrap().is_none());
        assert_eq!(
            reopened.deposit(label, first, 29_000, 1).unwrap(),
            DepositResult::Retired
        );
    }

    #[test]
    fn untrusted_restart_retires_the_payload_and_keeps_duplicate_suppression() {
        let settings = MailboxConfig {
            max_wait_ms: 60_000,
            trusted_wall_clock: false,
        };
        let (_directory, mut store, label, key) = fixture(settings, 0);
        let first = copy();
        store
            .deposit_at(label, first.clone(), 25_000, 1, 200)
            .unwrap();
        let mut reopened =
            Store::open_at(store.path.clone(), store.document.owner, settings, 0).unwrap();
        assert_eq!(reopened.key(label).unwrap(), key);
        assert!(reopened.pickup(label, None).unwrap().is_none());
        assert_eq!(
            reopened.deposit(label, first, 24_000, 1).unwrap(),
            DepositResult::Retired
        );
    }

    #[test]
    fn total_cap_leaves_existing_copies_in_place_and_expiry_releases_capacity() {
        let settings = MailboxConfig {
            max_wait_ms: 1000,
            trusted_wall_clock: false,
        };
        let (_directory, mut store, _, key) = fixture(settings, 0);
        for _ in 0..MAX_RECORDS / MAX_LABEL_LIVE {
            let label = Uuid::new_v4();
            store.register(label, key).unwrap();
            for _ in 0..MAX_LABEL_LIVE {
                assert_eq!(
                    store.deposit_at(label, copy(), 1000, 1, 10_000).unwrap(),
                    DepositResult::Stored
                );
            }
        }
        let label = Uuid::new_v4();
        store.register(label, key).unwrap();
        let excess = copy();
        assert_eq!(
            store
                .deposit_at(label, excess.clone(), 1000, 1, 10_001)
                .unwrap(),
            DepositResult::Full
        );
        assert_eq!(store.document.records.len(), MAX_RECORDS);
        assert_eq!(
            store.deposit_at(label, excess, 1000, 1, 11_000).unwrap(),
            DepositResult::Stored
        );
        assert_eq!(store.document.records.len(), 1);
    }
}
