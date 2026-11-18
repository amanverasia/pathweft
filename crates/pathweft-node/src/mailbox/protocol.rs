use super::routed::Circuit;
use super::store::Store;
use super::{Copy, Deposit, DepositResult, MailboxRelay, direct_leg, encoded_bytes, is_direct_leg};
use crate::{outbox, wire};
use anyhow::{Context, Result, bail, ensure};
use pathweft_core::{DEFAULT_HOPS, Identity, PublicKey};
use serde::{Deserialize, Serialize};
use std::{sync::Mutex, time::Duration};
use tokio::{net::TcpStream, time::timeout};
use uuid::Uuid;

// The direct listener dispatches the first frame here before decoding legacy traffic.
pub(crate) const PREFIX: &[u8] = b"PWM1";
pub(crate) const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(8);
const DOMAIN: &str = "pathweft/mailbox/exchange/v1";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Hello {
    label: Uuid,
    client_key: PublicKey,
    nonce: Uuid,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Boxed(#[serde(with = "encoded_bytes")] Vec<u8>);

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Protected {
    domain: String,
    label: Uuid,
    challenge: Uuid,
    body: Body,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Body {
    Challenge {
        session: Uuid,
    },
    Deposit {
        copy: Copy,
        remaining_ms: u64,
    },
    Deposited {
        id: Uuid,
        result: DepositResult,
    },
    Pickup {
        after: Option<Uuid>,
    },
    Page {
        copy: Option<Copy>,
        #[serde(default = "direct_leg", skip_serializing_if = "is_direct_leg")]
        deposit_hops: u8,
    },
    Saved {
        id: Uuid,
    },
    Released {
        id: Uuid,
    },
}

struct Channel<'a> {
    local: &'a Identity,
    peer: PublicKey,
    label: Uuid,
    challenge: Uuid,
}

impl Channel<'_> {
    fn seal(&self, body: Body) -> Result<Boxed> {
        let plaintext = serde_json::to_vec(&Protected {
            domain: DOMAIN.into(),
            label: self.label,
            challenge: self.challenge,
            body,
        })?;
        Ok(Boxed(self.local.box_to(self.peer, &plaintext)?))
    }

    fn open(&self, sealed: &Boxed) -> Result<Body> {
        let plaintext = self.local.box_from(self.peer, &sealed.0)?;
        let message: Protected = serde_json::from_slice(&plaintext)?;
        ensure!(
            message.domain == DOMAIN
                && message.label == self.label
                && message.challenge == self.challenge,
            "mailbox authentication context mismatch"
        );
        Ok(message.body)
    }
}

/// TCP connects directly on one handoff. A discovered circuit sends the same
/// protected messages as individually bounded datagrams.
pub(crate) enum Transport<'a> {
    Direct(TcpStream),
    Discovered(Circuit<'a>),
}

impl Transport<'_> {
    async fn hello(&mut self, hello: &Hello) -> Result<()> {
        let bytes = serde_json::to_vec(hello)?;
        match self {
            Self::Direct(stream) => {
                let mut message = PREFIX.to_vec();
                message.extend(bytes);
                wire::write_bytes(stream, &message).await
            }
            Self::Discovered(circuit) => circuit.open(bytes).await,
        }
    }

    async fn send(&mut self, sealed: Boxed, max_hops: u8) -> Result<()> {
        ensure!(max_hops > 0, "mailbox handoff allowance exhausted");
        match self {
            Self::Direct(stream) => wire::write(stream, &sealed).await,
            Self::Discovered(circuit) => circuit.send(sealed.0, max_hops).await,
        }
    }

    async fn receive(&mut self) -> Result<(Boxed, u8)> {
        match self {
            Self::Direct(stream) => Ok((wire::read(stream).await?, 1)),
            Self::Discovered(circuit) => {
                let (bytes, hops) = circuit.receive().await?;
                Ok((Boxed(bytes), hops))
            }
        }
    }
}

pub(crate) async fn serve(
    wire: Transport<'_>,
    identity: &Identity,
    store: &Mutex<Store>,
    hello: &[u8],
) -> Result<()> {
    let hello: Hello = serde_json::from_slice(hello)?;
    ensure!(!hello.nonce.is_nil(), "invalid mailbox client nonce");
    let pickup_key = store.lock().unwrap().key(hello.label)?;
    let mut session = Session {
        wire,
        channel: Channel {
            local: identity,
            peer: hello.client_key,
            label: hello.label,
            challenge: hello.nonce,
        },
    };
    let challenge = Uuid::new_v4();
    session
        .send(Body::Challenge { session: challenge }, DEFAULT_HOPS)
        .await?;
    session.channel.challenge = challenge;
    let (body, hops_used) = session.receive().await?;
    match body {
        Body::Deposit { copy, remaining_ms } => {
            let id = copy.id;
            let result =
                store
                    .lock()
                    .unwrap()
                    .deposit(hello.label, copy, remaining_ms, hops_used)?;
            session
                .send(Body::Deposited { id, result }, DEFAULT_HOPS)
                .await
        }
        Body::Pickup { after } => {
            ensure!(
                hello.client_key == pickup_key,
                "mailbox pickup credential mismatch"
            );
            let copy = store.lock().unwrap().pickup(hello.label, after)?;
            let Some((copy, deposit_hops)) = copy else {
                return session
                    .send(
                        Body::Page {
                            copy: None,
                            deposit_hops: 1,
                        },
                        DEFAULT_HOPS,
                    )
                    .await;
            };
            let max_pickup_hops = copy.hops_after_deposit + 1 - deposit_hops;
            let served = copy.id;
            session
                .send(
                    Body::Page {
                        copy: Some(copy),
                        deposit_hops,
                    },
                    max_pickup_hops,
                )
                .await?;
            let (Body::Saved { id }, _) = session.receive().await? else {
                bail!("expected mailbox save confirmation");
            };
            ensure!(id == served, "mailbox save confirmation copy mismatch");
            store.lock().unwrap().acknowledge(hello.label, id)?;
            session.send(Body::Released { id }, DEFAULT_HOPS).await
        }
        _ => bail!("unexpected mailbox request"),
    }
}

struct Session<'a> {
    wire: Transport<'a>,
    channel: Channel<'a>,
}

impl<'a> Session<'a> {
    async fn connect(
        mut wire: Transport<'a>,
        relay: MailboxRelay,
        label: Uuid,
        identity: &'a Identity,
    ) -> Result<Self> {
        let nonce = Uuid::new_v4();
        wire.hello(&Hello {
            label,
            client_key: identity.public_key(),
            nonce,
        })
        .await?;
        let mut session = Self {
            wire,
            channel: Channel {
                local: identity,
                peer: relay.public_key,
                label,
                challenge: nonce,
            },
        };
        let (Body::Challenge { session: challenge }, _) = session.receive().await? else {
            bail!("expected mailbox relay challenge");
        };
        ensure!(!challenge.is_nil(), "invalid mailbox relay challenge");
        session.channel.challenge = challenge;
        Ok(session)
    }

    async fn send(&mut self, body: Body, max_hops: u8) -> Result<()> {
        self.wire.send(self.channel.seal(body)?, max_hops).await
    }

    async fn receive(&mut self) -> Result<(Body, u8)> {
        let (sealed, hops) = self.wire.receive().await?;
        Ok((self.channel.open(&sealed)?, hops))
    }

    async fn exchange(&mut self, body: Body, max_hops: u8) -> Result<(Body, u8)> {
        self.send(body, max_hops).await?;
        self.receive().await
    }
}

pub(crate) async fn deposit(
    wire: Transport<'_>,
    deposit: &Deposit,
    expires_at_ms: u64,
    lifetime_ms: u64,
) -> Result<DepositResult> {
    timeout(EXCHANGE_TIMEOUT, async {
        let ephemeral = Identity::generate()?;
        let mut session = Session::connect(wire, deposit.relay, deposit.label, &ephemeral).await?;
        let remaining_ms = expires_at_ms
            .saturating_sub(outbox::now_ms()?)
            .min(lifetime_ms);
        ensure!(remaining_ms > 0, "outgoing mailbox copy expired");
        let (Body::Deposited { id, result }, _) = session
            .exchange(
                Body::Deposit {
                    copy: deposit.copy.clone(),
                    remaining_ms,
                },
                deposit.copy.hops_after_deposit,
            )
            .await?
        else {
            bail!("expected mailbox deposit result");
        };
        ensure!(
            id == deposit.copy.id,
            "mailbox deposit result copy mismatch"
        );
        Ok(result)
    })
    .await
    .context("mailbox deposit timed out")?
}

pub(crate) struct Page<'a> {
    session: Session<'a>,
    pub copy: Copy,
    pub deposit_hops: u8,
    pub pickup_hops: u8,
}

impl Page<'_> {
    pub async fn acknowledge(mut self) -> Result<()> {
        let id = self.copy.id;
        let (reply, _) = timeout(
            EXCHANGE_TIMEOUT,
            self.session.exchange(Body::Saved { id }, DEFAULT_HOPS),
        )
        .await??;
        ensure!(
            matches!(reply, Body::Released { id: released } if released == id),
            "mailbox release mismatch"
        );
        Ok(())
    }
}

pub(crate) async fn pickup<'a>(
    wire: Transport<'a>,
    relay: MailboxRelay,
    label: Uuid,
    identity: &'a Identity,
    after: Option<Uuid>,
) -> Result<Option<Page<'a>>> {
    timeout(EXCHANGE_TIMEOUT, async {
        let mut session = Session::connect(wire, relay, label, identity).await?;
        let (Body::Page { copy, deposit_hops }, pickup_hops) = session
            .exchange(Body::Pickup { after }, DEFAULT_HOPS)
            .await?
        else {
            bail!("expected mailbox pickup page");
        };
        if let Some(copy) = &copy {
            copy.validate()?;
        }
        Ok(copy.map(|copy| Page {
            session,
            copy,
            deposit_hops,
            pickup_hops,
        }))
    })
    .await
    .context("mailbox pickup timed out")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mailbox::{MAX_SEALED_BYTES, MAX_WAIT_MS, MailboxConfig};
    use crate::storage;
    use pathweft_core::{DEFAULT_HOPS, MAX_TEXT_BYTES, Payload};
    use std::{fs, path::PathBuf, sync::Arc};
    use tokio::net::TcpListener;

    #[test]
    fn max_copy_fits_authenticated_frames_and_max_text_round_trips() {
        let sender = Identity::generate().unwrap();
        let recipient = Identity::generate().unwrap();
        let relay = Identity::generate().unwrap();
        let target = MailboxRelay {
            public_key: relay.public_key(),
            address: Some("127.0.0.1:1".parse().unwrap()),
        };
        let label = Uuid::new_v4();
        let text = "\u{1}".repeat(MAX_TEXT_BYTES);
        let message = sender
            .seal(recipient.public_key(), Payload::Text { text: text.clone() })
            .unwrap();
        let deposit = Deposit::new(target, label, recipient.public_key(), &message).unwrap();
        let opened = deposit.copy.open(&recipient, label, 1, 1).unwrap();
        assert_eq!(opened.id, message.id);
        assert_eq!(opened.remaining_hops, DEFAULT_HOPS - 2);
        assert_eq!(
            recipient.open(sender.public_key(), &opened).unwrap(),
            Payload::Text { text }
        );
        assert!(deposit.copy.open(&recipient, Uuid::new_v4(), 1, 1).is_err());

        let channel = Channel {
            local: &sender,
            peer: relay.public_key(),
            label,
            challenge: Uuid::new_v4(),
        };
        let max_copy = Copy {
            sealed: vec![255; MAX_SEALED_BYTES],
            ..deposit.copy
        };
        for body in [
            Body::Deposit {
                copy: max_copy.clone(),
                remaining_ms: MAX_WAIT_MS,
            },
            Body::Page {
                copy: Some(max_copy),
                deposit_hops: 2,
            },
        ] {
            let size = serde_json::to_vec(&channel.seal(body).unwrap())
                .unwrap()
                .len();
            assert!(
                size <= wire::MAX_FRAME_BYTES,
                "authenticated frame was {size} bytes"
            );
        }
    }

    async fn connect_to(target: MailboxRelay) -> Transport<'static> {
        Transport::Direct(TcpStream::connect(target.address.unwrap()).await.unwrap())
    }

    struct Directory(PathBuf);

    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn pickup_requires_the_pinned_key_and_fresh_label_and_copy_bound_proof() {
        let directory =
            Directory(std::env::temp_dir().join(format!("pw-protocol-{}", Uuid::new_v4())));
        storage::private_directory(&directory.0).unwrap();
        let relay = Identity::generate().unwrap();
        let relay_key = relay.public_key();
        let owner = Identity::generate().unwrap();
        let wrong_key = Identity::generate().unwrap();
        let label = Uuid::new_v4();
        let other_label = Uuid::new_v4();
        let copy = Copy {
            id: Uuid::new_v4(),
            hops_after_deposit: DEFAULT_HOPS - 1,
            sealed: vec![11; 64],
        };
        let other_copy = Copy {
            id: Uuid::new_v4(),
            ..copy.clone()
        };
        let mut store = Store::open(
            &directory.0.join("relay.sock"),
            relay.id(),
            MailboxConfig {
                max_wait_ms: 60_000,
                trusted_wall_clock: false,
            },
        )
        .unwrap();
        store.register(label, owner.public_key()).unwrap();
        store.register(other_label, owner.public_key()).unwrap();
        store.deposit(label, copy.clone(), 60_000, 1).unwrap();
        store
            .deposit(other_label, other_copy.clone(), 60_000, 1)
            .unwrap();

        let store = Arc::new(Mutex::new(store));
        let served_store = store.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = MailboxRelay {
            address: Some(listener.local_addr().unwrap()),
            public_key: relay_key,
        };
        let server = tokio::spawn(async move {
            for _ in 0..6 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let bytes = wire::read_bytes(&mut stream).await.unwrap();
                let result = serve(
                    Transport::Direct(stream),
                    &relay,
                    &served_store,
                    bytes.strip_prefix(PREFIX).unwrap(),
                )
                .await;
                if let Err(error) = result {
                    assert!(!error.to_string().is_empty());
                }
            }
        });

        assert!(
            pickup(connect_to(target).await, target, label, &wrong_key, None)
                .await
                .is_err()
        );
        let mut first = Session::connect(connect_to(target).await, target, label, &owner)
            .await
            .unwrap();
        let request = first.channel.seal(Body::Pickup { after: None }).unwrap();
        first
            .wire
            .send(Boxed(request.0.clone()), DEFAULT_HOPS)
            .await
            .unwrap();
        let (
            Body::Page {
                copy: Some(found), ..
            },
            _,
        ) = first.receive().await.unwrap()
        else {
            panic!("copy should still be present");
        };
        assert_eq!(found.id, copy.id);
        let saved = first.channel.seal(Body::Saved { id: copy.id }).unwrap();
        assert!(
            first
                .exchange(Body::Saved { id: other_copy.id }, DEFAULT_HOPS)
                .await
                .is_err()
        );

        let mut fresh = Session::connect(connect_to(target).await, target, label, &owner)
            .await
            .unwrap();
        fresh.wire.send(request, DEFAULT_HOPS).await.unwrap();
        assert!(fresh.receive().await.is_err());

        let mut second = Session::connect(connect_to(target).await, target, label, &owner)
            .await
            .unwrap();
        assert!(matches!(
            second
                .exchange(Body::Pickup { after: None }, DEFAULT_HOPS)
                .await
                .unwrap(),
            (Body::Page { copy: Some(_), .. }, _)
        ));
        second.wire.send(saved, DEFAULT_HOPS).await.unwrap();
        assert!(second.receive().await.is_err());

        let mut other = Session::connect(connect_to(target).await, target, other_label, &owner)
            .await
            .unwrap();
        assert!(matches!(
            other
                .exchange(Body::Pickup { after: None }, DEFAULT_HOPS)
                .await
                .unwrap(),
            (Body::Page { copy: Some(_), .. }, _)
        ));
        let wrong_label = Channel {
            label,
            ..other.channel
        }
        .seal(Body::Saved { id: other_copy.id })
        .unwrap();
        other.wire.send(wrong_label, DEFAULT_HOPS).await.unwrap();
        assert!(other.wire.receive().await.is_err());

        let page = pickup(connect_to(target).await, target, label, &owner, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(page.copy.id, copy.id);
        page.acknowledge().await.unwrap();
        server.await.unwrap();
        let mut state = store.lock().unwrap();
        assert!(state.pickup(label, None).unwrap().is_none());
        assert_eq!(
            state.pickup(other_label, None).unwrap().unwrap().0.id,
            other_copy.id
        );
    }

    #[test]
    fn serial_pickup_delay_bounds_idle_and_failure_checks() {
        for (failures, page, expected_ms) in [
            (0, true, 250),
            (0, false, 2000),
            (1, false, 4000),
            (2, false, 8000),
            (u32::MAX, false, 30_000),
        ] {
            assert_eq!(
                super::super::pickup_delay(failures, page),
                Duration::from_millis(expected_ms)
            );
        }
    }
}
