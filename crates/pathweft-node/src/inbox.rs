use crate::storage;
use anyhow::{Context, Result, bail, ensure};
use pathweft_core::{Envelope, Identity, NodeId, Payload, PublicKey};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};
use uuid::Uuid;

const CAPACITY: usize = 64;
const FORGOTTEN_CAPACITY: usize = 1024;
const MAX_FILE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    packet: Envelope,
    sender_key: PublicKey,
    received_at_ms: u64,
}

impl Message {
    fn summary(&self) -> Value {
        json!({"message_id":self.packet.id,"source":self.packet.source,"received_at_ms":self.received_at_ms})
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Forgotten {
    message_id: Uuid,
    source: NodeId,
    content_hash: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u8,
    owner: NodeId,
    messages: Vec<Message>,
    forgotten: Vec<Forgotten>,
}

pub(crate) enum Acceptance {
    New { text: String },
    Duplicate,
    Forgotten,
}

pub(crate) struct Inbox {
    path: PathBuf,
    document: Document,
    failed: bool,
}

impl Inbox {
    pub fn path_for(control: &Path) -> Result<PathBuf> {
        storage::state_path(control, ".inbox.json")
    }

    pub fn open(path: PathBuf, owner: &Identity) -> Result<Self> {
        let document = storage::read_private_json(&path, MAX_FILE_BYTES)?.unwrap_or(Document {
            version: 1,
            owner: owner.id(),
            messages: vec![],
            forgotten: vec![],
        });
        Self::validate(&document, owner.id())?;
        for message in &document.messages {
            ensure!(
                matches!(
                    owner.open(message.sender_key, &message.packet)?,
                    Payload::Text { .. }
                ),
                "inbox contains a non-text payload"
            );
        }
        storage::discard_private_temporary(&path, owner.id(), MAX_FILE_BYTES)?;
        Ok(Self {
            path,
            document,
            failed: false,
        })
    }

    fn validate(document: &Document, owner: NodeId) -> Result<()> {
        ensure!(
            document.version == 1 && document.owner == owner,
            "inbox version/owner mismatch"
        );
        ensure!(
            document.messages.len() <= CAPACITY && document.forgotten.len() <= FORGOTTEN_CAPACITY,
            "inbox capacity exceeded"
        );
        let mut ids = HashSet::new();
        for message in &document.messages {
            message.packet.validate()?;
            ensure!(
                message.packet.destination == owner
                    && message.packet.source == message.sender_key.node_id(),
                "inbox endpoint/key mismatch"
            );
            ensure!(ids.insert(message.packet.id), "duplicate inbox message id");
        }
        for message in &document.forgotten {
            ensure!(
                !message.message_id.is_nil()
                    && message.source != owner
                    && ids.insert(message.message_id),
                "invalid forgotten-message record"
            );
        }
        Ok(())
    }

    fn commit(&mut self, next: Document) -> Result<()> {
        ensure!(
            !self.failed,
            "inbox storage failed; restart before further changes"
        );
        Self::validate(&next, self.document.owner)?;
        let bytes = serde_json::to_vec(&next)?;
        ensure!(bytes.len() <= MAX_FILE_BYTES, "inbox exceeds size limit");
        if let Err(error) =
            storage::write_private_state(&self.path, self.document.owner, &bytes, MAX_FILE_BYTES)
        {
            self.failed = true;
            return Err(error.context("inbox storage failed; restart to inspect persisted state"));
        }
        self.document = next;
        Ok(())
    }

    /// Called only with a currently pinned key. Authenticate again at the storage
    /// boundary so callers cannot save/acknowledge unverified or non-text content.
    pub fn accept(
        &mut self,
        owner: &Identity,
        sender_key: PublicKey,
        packet: &Envelope,
        now: u64,
    ) -> Result<Acceptance> {
        ensure!(
            !self.failed,
            "inbox storage failed; restart before further changes"
        );
        ensure!(owner.id() == self.document.owner, "inbox owner mismatch");
        let Payload::Text { text } = owner.open(sender_key, packet)? else {
            bail!("only text can enter the inbox")
        };
        let hash = packet.content_hash();
        if let Some(existing) = self
            .document
            .messages
            .iter()
            .find(|m| m.packet.id == packet.id)
        {
            ensure!(
                existing.packet.content_hash() == hash,
                "conflicting authenticated message id"
            );
            return Ok(Acceptance::Duplicate);
        }
        if let Some(existing) = self
            .document
            .forgotten
            .iter()
            .find(|m| m.message_id == packet.id)
        {
            ensure!(
                existing.source == packet.source && existing.content_hash == hash,
                "conflicting forgotten message id"
            );
            return Ok(Acceptance::Forgotten);
        }
        ensure!(
            self.document.messages.len() < CAPACITY,
            "inbox full; forget retained messages before accepting more"
        );
        let mut next = self.document.clone();
        next.messages.push(Message {
            packet: packet.clone(),
            sender_key,
            received_at_ms: now,
        });
        self.commit(next)?;
        Ok(Acceptance::New { text })
    }

    pub fn retained_from(&self, id: Uuid, source: NodeId) -> Result<bool> {
        ensure!(
            !self.failed,
            "inbox storage failed; restart before receipt recovery"
        );
        Ok(self
            .document
            .messages
            .iter()
            .any(|m| m.packet.id == id && m.packet.source == source))
    }

    pub fn list(&self) -> Value {
        json!({"storage_error":self.failed,"capacity":CAPACITY,"forgotten":self.document.forgotten.len(),
            "entries":self.document.messages.iter().map(Message::summary).collect::<Vec<_>>()})
    }

    pub fn summary(&self) -> Value {
        json!({"stored":self.document.messages.len(),"forgotten":self.document.forgotten.len(),"storage_error":self.failed})
    }

    pub fn read(&self, id: Uuid, owner: &Identity) -> Result<Value> {
        ensure!(
            !self.failed,
            "inbox storage failed; restart before reading retained messages"
        );
        let message = self
            .document
            .messages
            .iter()
            .find(|m| m.packet.id == id)
            .context("unknown retained message")?;
        let Payload::Text { text } = owner.open(message.sender_key, &message.packet)? else {
            bail!("inbox contains a non-text payload")
        };
        let mut result = message.summary();
        result["text"] = json!(text);
        Ok(result)
    }

    pub fn forget(&mut self, id: Uuid) -> Result<Value> {
        let mut next = self.document.clone();
        let index = next
            .messages
            .iter()
            .position(|m| m.packet.id == id)
            .context("unknown retained message")?;
        let removed = next.messages.remove(index);
        next.forgotten.push(Forgotten {
            message_id: id,
            source: removed.packet.source,
            content_hash: removed.packet.content_hash(),
        });
        if next.forgotten.len() > FORGOTTEN_CAPACITY {
            next.forgotten.remove(0);
        }
        self.commit(next)?;
        Ok(json!({"forgotten":id}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn fixture() -> (Directory, Identity, Identity, Inbox) {
        let directory =
            Directory(std::env::temp_dir().join(format!("pw-inbox-{}", Uuid::new_v4())));
        let owner = Identity::generate().unwrap();
        let sender = Identity::generate().unwrap();
        let inbox = Inbox::open(directory.0.join("inbox.json"), &owner).unwrap();
        (directory, owner, sender, inbox)
    }
    fn packet(sender: &Identity, owner: &Identity) -> Envelope {
        sender
            .seal(
                owner.public_key(),
                Payload::Text {
                    text: "private received 🌍".into(),
                },
            )
            .unwrap()
    }
    #[test]
    fn encrypted_inbox_survives_restart_and_deduplicates_changed_hop_budget() {
        let (_directory, owner, sender, mut inbox) = fixture();
        let mut p = packet(&sender, &owner);
        let id = p.id;
        assert!(matches!(
            inbox.accept(&owner, sender.public_key(), &p, 1000).unwrap(),
            Acceptance::New { .. }
        ));
        assert_eq!(
            fs::metadata(&inbox.path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(
            !fs::read_to_string(&inbox.path)
                .unwrap()
                .contains("private received")
        );
        let mut restarted = Inbox::open(inbox.path.clone(), &owner).unwrap();
        assert_eq!(
            restarted.read(id, &owner).unwrap()["text"],
            "private received 🌍"
        );
        p.remaining_hops = 0;
        assert!(matches!(
            restarted
                .accept(&owner, sender.public_key(), &p, 2000)
                .unwrap(),
            Acceptance::Duplicate
        ));
        assert_eq!(restarted.list()["entries"].as_array().unwrap().len(), 1);
        assert!(!restarted.retained_from(id, owner.id()).unwrap());
        assert!(restarted.retained_from(id, sender.id()).unwrap());
    }
    #[test]
    fn authentication_and_non_text_rejection_do_not_create_storage() {
        let (_directory, owner, sender, mut inbox) = fixture();
        let mut bad = packet(&sender, &owner);
        bad.ciphertext[0] ^= 1;
        assert!(
            inbox
                .accept(&owner, sender.public_key(), &bad, 1000)
                .is_err()
        );
        let receipt = sender
            .seal(
                owner.public_key(),
                Payload::Receipt {
                    message_id: Uuid::new_v4(),
                },
            )
            .unwrap();
        assert!(
            inbox
                .accept(&owner, sender.public_key(), &receipt, 1000)
                .is_err()
        );
        assert!(!inbox.path.exists());
        let p = packet(&sender, &owner);
        inbox.accept(&owner, sender.public_key(), &p, 1000).unwrap();
        let mut corrupt: Value = serde_json::from_slice(&fs::read(&inbox.path).unwrap()).unwrap();
        let old = corrupt["messages"][0]["packet"]["ciphertext"][0]
            .as_u64()
            .unwrap();
        corrupt["messages"][0]["packet"]["ciphertext"][0] = json!(old ^ 1);
        fs::write(&inbox.path, serde_json::to_vec(&corrupt).unwrap()).unwrap();
        assert!(Inbox::open(inbox.path.clone(), &owner).is_err());
    }
    #[test]
    fn capacity_forgetting_and_bounded_tombstones_prevent_immediate_replay() {
        let (_directory, owner, sender, mut inbox) = fixture();
        let first = packet(&sender, &owner);
        inbox
            .accept(&owner, sender.public_key(), &first, 1000)
            .unwrap();
        for _ in 1..CAPACITY {
            inbox
                .accept(&owner, sender.public_key(), &packet(&sender, &owner), 1000)
                .unwrap();
        }
        assert!(
            inbox
                .accept(&owner, sender.public_key(), &packet(&sender, &owner), 1000)
                .is_err()
        );
        assert!(serde_json::to_vec(&inbox.list()).unwrap().len() < crate::wire::MAX_FRAME_BYTES);
        inbox.forget(first.id).unwrap();
        let mut restarted = Inbox::open(inbox.path.clone(), &owner).unwrap();
        assert!(matches!(
            restarted
                .accept(&owner, sender.public_key(), &first, 2000)
                .unwrap(),
            Acceptance::Forgotten
        ));
        assert!(!restarted.retained_from(first.id, sender.id()).unwrap());
        assert!(restarted.read(first.id, &owner).is_err());
        assert_eq!(restarted.list()["forgotten"], 1);
        // Exercise FIFO eviction directly while keeping disk I/O bounded.
        let mut next = restarted.document.clone();
        next.messages.clear();
        next.forgotten = (0..FORGOTTEN_CAPACITY)
            .map(|_| Forgotten {
                message_id: Uuid::new_v4(),
                source: sender.id(),
                content_hash: [1; 32],
            })
            .collect();
        restarted.commit(next).unwrap();
        let p = packet(&sender, &owner);
        restarted
            .accept(&owner, sender.public_key(), &p, 2000)
            .unwrap();
        restarted.forget(p.id).unwrap();
        assert_eq!(restarted.document.forgotten.len(), FORGOTTEN_CAPACITY);
    }
    #[test]
    fn private_owner_and_io_failures_block_acknowledgement() {
        let (_directory, owner, sender, mut inbox) = fixture();
        let p = packet(&sender, &owner);
        inbox.accept(&owner, sender.public_key(), &p, 1000).unwrap();
        assert!(Inbox::open(inbox.path.clone(), &sender).is_err());
        fs::set_permissions(&inbox.path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Inbox::open(inbox.path.clone(), &owner).is_err());
        fs::set_permissions(&inbox.path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::create_dir(storage::temporary_path(&inbox.path, owner.id())).unwrap();
        assert!(inbox.forget(p.id).is_err());
        assert_eq!(inbox.summary()["storage_error"], true);
        assert!(inbox.retained_from(p.id, sender.id()).is_err());
        assert!(inbox.accept(&owner, sender.public_key(), &p, 2000).is_err());
    }
}
