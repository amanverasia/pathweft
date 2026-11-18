use crate::inbox::{Acceptance, Inbox};
use crate::mailbox::{self, Deposit, MailboxConfig, MailboxRelay, MailboxRoute, PickupConfig};
use crate::outbox::{self, DeliveryState, Failure, Outbox};
use crate::{EventSink, Request, Response, storage, wire};
use anyhow::{Context, Result, bail, ensure};
use pathweft_core::{
    Action, DropReason, Envelope, Identity, NodeId, Origin, Payload, PublicKey, Router,
    discovery::{
        CounterState, Discovery, Frame, Input, LinkId, LinkKind, NetworkFrame, Output,
        SodiumRandom, parse_network_frame,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, OpenOptions},
    net::SocketAddr,
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
    sync::{Notify, mpsc, oneshot},
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval, sleep, timeout, timeout_at},
};
use uuid::Uuid;

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(200);
const MAX_NETWORK_HANDLERS: usize = 32;
const MAX_CONTROL_HANDLERS: usize = 32;
const MAX_PENDING: usize = 64;
const MAX_CONTACTS: usize = 256;
const MAX_DISCOVERY_LINKS: usize = 16;
const DISCOVERY_TICK: Duration = Duration::from_millis(250);
const DISCOVERY_STATE_LIMIT: usize = 256 * 1024;
const MAX_DISCOVERY_STEPS: usize = 16;

fn default_listen() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}
fn default_relay() -> bool {
    true
}
fn default_powered() -> bool {
    true
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub name: String,
    pub identity: PathBuf,
    pub control_socket: PathBuf,
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    #[serde(default = "default_relay")]
    pub relay: bool,
    #[serde(default)]
    pub contacts: Vec<PublicKey>,
    #[serde(default)]
    pub routes: Vec<Route>,
    /// Opt-in discovery links (neighbour addresses). A non-empty list turns discovery on.
    #[serde(default)]
    pub links: Vec<DiscoveryLink>,
    /// Turn discovery on even with no static links; links then arrive with `link_up`.
    #[serde(default)]
    pub discovery: bool,
    /// Whether this node is on mains power (a discovery path-cost hint).
    #[serde(default = "default_powered")]
    pub powered: bool,
    /// Explicitly enable offline storage on a loopback prototype relay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox: Option<MailboxConfig>,
    /// Check one pinned relay automatically with a separate private mailbox identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pickup: Option<PickupConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox_route: Option<MailboxRoute>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mailbox_route_contacts: Vec<PublicKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DiscoveryLinkKind {
    Lan,
    Ble,
    Other,
}
impl From<DiscoveryLinkKind> for LinkKind {
    fn from(kind: DiscoveryLinkKind) -> Self {
        match kind {
            DiscoveryLinkKind::Lan => Self::Lan,
            DiscoveryLinkKind::Ble => Self::Ble,
            DiscoveryLinkKind::Other => Self::Other,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryLink {
    pub address: SocketAddr,
    pub kind: DiscoveryLinkKind,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub destination: NodeId,
    pub via: SocketAddr,
}

#[derive(Default)]
struct Counters {
    forwarded: AtomicU64,
    delivered: AtomicU64,
    rejected: AtomicU64,
    duplicates: AtomicU64,
    receipts: AtomicU64,
    ignored_receipts: AtomicU64,
    connect_retries: AtomicU64,
}
struct Pending {
    recipient: NodeId,
    reply: oneshot::Sender<Uuid>,
}
// A send can be cancelled at any await point, including during shutdown.
// Keep cleanup in Drop so every exit path releases the pending-send slot.
struct PendingSend<'a> {
    id: Uuid,
    pending: &'a Mutex<HashMap<Uuid, Pending>>,
}
impl Drop for PendingSend<'_> {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CounterFile {
    version: u8,
    owner: NodeId,
    counters: CounterState,
}

/// What a batch of discovery outputs asked of the node.
#[derive(Default)]
struct Summary {
    found: Vec<NodeId>,
    failed: Vec<NodeId>,
    no_route: bool,
    deliver: Vec<Envelope>,
    datagrams: Vec<(NodeId, Vec<u8>, u8)>,
    sent_datagram: bool,
}

struct Prepared {
    link: LinkId,
    frame: Frame,
    stream: TcpStream,
    addr: SocketAddr,
}
enum Opened {
    NoRoute,
    ConnectFailed,
    Ready(Box<Prepared>),
}

struct DiscoveryRuntime {
    engine: Mutex<Discovery>,
    links: Mutex<Vec<(LinkId, DiscoveryLink)>>,
    next_link: std::sync::atomic::AtomicU32,
    path: PathBuf,
    owner: NodeId,
    /// Woken whenever a route is found, fails or is invalidated.
    changed: Notify,
    started: std::time::Instant,
}

impl DiscoveryRuntime {
    fn open(
        config: &Config,
        identity: &Identity,
        contacts: impl IntoIterator<Item = PublicKey>,
    ) -> Result<Self> {
        let owner = identity.id();
        let path = storage::state_path(&config.control_socket, ".discovery.json")?;
        storage::discard_private_temporary(&path, owner, DISCOVERY_STATE_LIMIT)?;
        let counters =
            match storage::read_private_json::<CounterFile>(&path, DISCOVERY_STATE_LIMIT)? {
                Some(file) => {
                    ensure!(
                        file.version == 1 && file.owner == owner,
                        "discovery state belongs to another identity or version"
                    );
                    file.counters
                }
                None => CounterState::default(),
            };
        let mut engine = Discovery::new(
            identity,
            contacts,
            config.relay,
            config.powered,
            counters,
            Box::new(SodiumRandom),
        )?;
        for &key in &config.mailbox_route_contacts {
            engine.add_contact(key);
        }
        if let Some(route) = &config.mailbox_route {
            let alias = storage::load_identity(&route.identity)?;
            ensure!(
                alias.id() != owner && alias.public_key() != route.relay_key,
                "mailbox discovery requires a distinct route identity"
            );
            engine.set_route_identity(route.relay_key, alias);
        }
        Ok(Self {
            engine: Mutex::new(engine),
            links: Mutex::new(
                config
                    .links
                    .iter()
                    .enumerate()
                    .map(|(i, link)| (LinkId(i as u32), *link))
                    .collect(),
            ),
            next_link: std::sync::atomic::AtomicU32::new(config.links.len() as u32),
            path,
            owner,
            changed: Notify::new(),
            started: std::time::Instant::now(),
        })
    }

    fn now(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn address(&self, link: LinkId) -> Option<SocketAddr> {
        self.links
            .lock()
            .unwrap()
            .iter()
            .find(|(id, _)| *id == link)
            .map(|(_, l)| l.address)
    }

    fn persist(&self, counters: &CounterState) -> Result<()> {
        let bytes = serde_json::to_vec(&CounterFile {
            version: 1,
            owner: self.owner,
            counters: counters.clone(),
        })?;
        storage::write_private_state(&self.path, self.owner, &bytes, DISCOVERY_STATE_LIMIT)
    }
}

async fn send_bytes(addr: SocketAddr, bytes: &[u8]) -> Result<()> {
    timeout(IO_TIMEOUT, async {
        let mut stream = TcpStream::connect(addr).await.context("connect to link")?;
        wire::write_bytes(&mut stream, bytes).await
    })
    .await
    .context("link connection/write timed out")?
}

pub(crate) struct State {
    name: String,
    identity: Identity,
    listen: SocketAddr,
    router: Mutex<Router<SocketAddr>>,
    /// Pinned endpoint keys by node ID. Writable only through `add_contact`, which never replaces a key.
    contacts: RwLock<HashMap<NodeId, PublicKey>>,
    pending: Mutex<HashMap<Uuid, Pending>>,
    outbox: Mutex<Outbox>,
    inbox: Mutex<Inbox>,
    outbox_wake: Notify,
    counters: Counters,
    events: EventSink,
    discovery: Option<DiscoveryRuntime>,
    mailbox: Option<Mutex<mailbox::store::Store>>,
    pickup: Option<PickupRuntime>,
    pub(crate) mailbox_network: mailbox::routed::Network,
    mailbox_route_key: Option<PublicKey>,
    mailbox_route_contacts: HashSet<NodeId>,
}

struct PickupRuntime {
    relay: MailboxRelay,
    identity: Identity,
    labels: Vec<Uuid>,
}

impl State {
    fn new(
        config: &Config,
        identity: Identity,
        listen: SocketAddr,
        events: EventSink,
        outbox: Outbox,
        inbox: Inbox,
        mailbox_network: mailbox::routed::Network,
    ) -> Result<Self> {
        let (contacts, router) = config.routing(identity.id())?;
        let discovery = if config.links.is_empty() && !config.discovery {
            None
        } else {
            Some(DiscoveryRuntime::open(
                config,
                &identity,
                contacts.values().copied(),
            )?)
        };
        let mailbox = config
            .mailbox
            .map(|settings| {
                mailbox::store::Store::open(&config.control_socket, identity.id(), settings)
                    .map(Mutex::new)
            })
            .transpose()?;
        let pickup = if let Some(pickup) = &config.pickup {
            Some(PickupRuntime {
                relay: pickup.relay,
                identity: pickup.load_identity(identity.id())?,
                labels: pickup.labels.clone(),
            })
        } else {
            None
        };
        Ok(Self {
            name: config.name.clone(),
            identity,
            listen,
            router: Mutex::new(router),
            contacts: RwLock::new(contacts),
            pending: Mutex::new(HashMap::new()),
            outbox: Mutex::new(outbox),
            inbox: Mutex::new(inbox),
            outbox_wake: Notify::new(),
            counters: Counters::default(),
            events,
            discovery,
            mailbox,
            pickup,
            mailbox_network,
            mailbox_route_key: config.mailbox_route.as_ref().map(|route| route.relay_key),
            mailbox_route_contacts: config
                .mailbox_route_contacts
                .iter()
                .map(|key| key.node_id())
                .collect(),
        })
    }

    fn contact(&self, id: &NodeId) -> Option<PublicKey> {
        self.contacts.read().unwrap().get(id).copied()
    }

    /// Pin a new endpoint key while running. Idempotent for an existing key; a key can never
    /// replace another because each key has its own node ID. Not written to the config file.
    fn add_contact(&self, key: PublicKey) -> Result<Value> {
        let id = key.node_id();
        ensure!(
            id != self.identity.id(),
            "cannot add your own key as a contact"
        );
        let mut contacts = self.contacts.write().unwrap();
        if contacts.contains_key(&id) {
            return Ok(json!({"id":id, "public_key":key, "added":false}));
        }
        ensure!(contacts.len() < MAX_CONTACTS, "too many contacts");
        contacts.insert(id, key);
        drop(contacts);
        if let Some(rt) = &self.discovery {
            rt.engine.lock().unwrap().add_contact(key);
        }
        self.emit("contact_added", json!({"id":id}));
        Ok(json!({"id":id, "public_key":key, "added":true}))
    }

    fn emit(&self, event: &str, details: Value) {
        (self.events)(json!({"node":self.name,"event":event,"details":details}));
    }

    fn status(&self) -> Value {
        let (routes, relay) = {
            let r = self.router.lock().unwrap();
            (r.route_count(), r.relay_enabled())
        };
        json!({"name":self.name, "id":self.identity.id(), "public_key":self.identity.public_key(),
            "listen":self.listen, "relay":relay, "routes":routes,
            "pending":self.pending.lock().unwrap().len(),
            "forwarded":self.counters.forwarded.load(Ordering::Relaxed),
            "delivered_texts":self.counters.delivered.load(Ordering::Relaxed),
            "rejected":self.counters.rejected.load(Ordering::Relaxed),
            "duplicates":self.counters.duplicates.load(Ordering::Relaxed),
            "receipts":self.counters.receipts.load(Ordering::Relaxed),
            "ignored_receipts":self.counters.ignored_receipts.load(Ordering::Relaxed),
            "connect_retries":self.counters.connect_retries.load(Ordering::Relaxed),
            "discovery":self.discovery_status(),
            "mailbox":self.mailbox.as_ref().map(|store| store.lock().unwrap().summary()),
            "outbox":self.outbox.lock().unwrap().summary(),
            "inbox":self.inbox.lock().unwrap().summary()})
    }

    fn discovery_status(&self) -> Value {
        match &self.discovery {
            None => json!({"enabled":false,"links":0,"neighbours":0,"routes":0}),
            Some(rt) => {
                let engine = rt.engine.lock().unwrap();
                json!({"enabled":true,"links":rt.links.lock().unwrap().len(),
                    "neighbours":engine.neighbour_count(rt.now()),"routes":engine.route_count()})
            }
        }
    }

    /// Add (or refresh) a live discovery link and let the engine send its first hello.
    async fn link_up(&self, address: SocketAddr, kind: DiscoveryLinkKind) -> Result<Value> {
        let rt = self
            .discovery
            .as_ref()
            .context("discovery is not enabled on this node")?;
        ensure!(address.port() != 0, "discovery link port must not be zero");
        let link = {
            let mut links = rt.links.lock().unwrap();
            if let Some(entry) = links.iter_mut().find(|(_, l)| l.address == address) {
                entry.1.kind = kind;
                entry.0
            } else {
                ensure!(
                    links.len() < MAX_DISCOVERY_LINKS,
                    "too many discovery links"
                );
                let id = LinkId(rt.next_link.fetch_add(1, Ordering::Relaxed));
                links.push((id, DiscoveryLink { address, kind }));
                id
            }
        };
        self.discover(Input::LinkUp {
            link,
            kind: kind.into(),
        })
        .await;
        Ok(json!({"address":address,"kind":kind}))
    }

    async fn link_down(&self, address: SocketAddr) -> Result<Value> {
        let rt = self
            .discovery
            .as_ref()
            .context("discovery is not enabled on this node")?;
        let link = {
            let mut links = rt.links.lock().unwrap();
            let at = links.iter().position(|(_, l)| l.address == address);
            at.map(|i| links.remove(i).0)
        };
        if let Some(link) = link {
            self.discover(Input::LinkDown { link }).await;
        }
        Ok(json!({"address":address,"removed":link.is_some()}))
    }

    fn dropped(&self, packet: &Envelope, reason: DropReason) -> Result<()> {
        if reason == DropReason::Duplicate {
            self.counters.duplicates.fetch_add(1, Ordering::Relaxed);
        }
        self.emit(
            "dropped",
            json!({"message_id":packet.id, "reason":reason.as_str()}),
        );
        if reason == DropReason::Duplicate {
            Ok(())
        } else {
            bail!("packet dropped: {}", reason.as_str())
        }
    }

    async fn transmit(&self, packet: &Envelope, via: SocketAddr, relay: bool) -> Result<()> {
        timeout(IO_TIMEOUT, async {
            let stream = TcpStream::connect(via)
                .await
                .context("connect to next hop")?;
            self.transmit_connected(packet, via, relay, stream).await
        })
        .await
        .context("next-hop connection/write timed out")?
    }

    async fn transmit_connected(
        &self,
        packet: &Envelope,
        via: SocketAddr,
        relay: bool,
        mut stream: TcpStream,
    ) -> Result<()> {
        timeout(IO_TIMEOUT, wire::write(&mut stream, packet))
            .await
            .context("next-hop write timed out")??;
        if relay {
            self.counters.forwarded.fetch_add(1, Ordering::Relaxed);
        }
        self.emit(
            if relay { "forwarded" } else { "originated" },
            json!({"message_id":packet.id,"source":packet.source,"destination":packet.destination,
                "remaining_hops":packet.remaining_hops,"via":via}),
        );
        Ok(())
    }

    async fn connect_for_send(
        &self,
        via: SocketAddr,
        message_id: Uuid,
        deadline: Instant,
    ) -> Result<TcpStream> {
        // Retry only before any packet bytes can be written. Once connected,
        // write/receipt failures are ambiguous and must not cause a resend.
        timeout_at(deadline, async {
            loop {
                if let Ok(Ok(stream)) = timeout(IO_TIMEOUT, TcpStream::connect(via)).await {
                    return stream;
                }
                sleep(CONNECT_RETRY_DELAY).await;
                self.counters
                    .connect_retries
                    .fetch_add(1, Ordering::Relaxed);
                self.emit("connect_retry", json!({"message_id":message_id,"via":via}));
            }
        })
        .await
        .context("next hop unavailable before send deadline")
    }

    /// True when this node uses discovery for a destination that has no manual route.
    fn discovery_applies(&self, action: &Action<SocketAddr>, destination: &NodeId) -> bool {
        self.discovery.is_some()
            && matches!(action, Action::Drop(DropReason::NoRoute))
            && self.contact(destination).is_some()
    }

    /// Send a locally created envelope. Returns whether a discovered path was used.
    async fn originate(&self, mut packet: Envelope, deadline: Option<Instant>) -> Result<bool> {
        let action = self
            .router
            .lock()
            .unwrap()
            .route(&mut packet, Origin::Local)?;
        if self.discovery_applies(&action, &packet.destination) {
            self.originate_discovered(&packet, deadline.is_some())
                .await?;
            return Ok(true);
        }
        match action {
            Action::Forward(via) => {
                if let Some(deadline) = deadline {
                    let stream = self.connect_for_send(via, packet.id, deadline).await?;
                    self.transmit_connected(&packet, via, false, stream).await?;
                } else {
                    self.transmit(&packet, via, false).await?;
                }
                Ok(false)
            }
            Action::Drop(reason) => self.dropped(&packet, reason).map(|()| false),
            Action::Deliver => bail!("local self-delivery is not supported"),
        }
    }

    async fn originate_discovered(&self, packet: &Envelope, wait: bool) -> Result<()> {
        let rt = self.discovery.as_ref().context("discovery is disabled")?;
        let destination = packet.destination;
        loop {
            // Register interest first so a route event during the attempt is not missed.
            let changed = rt.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.open_discovered(packet).await? {
                Opened::Ready(prepared) => return self.finish_discovered(packet, prepared).await,
                Opened::ConnectFailed if wait => sleep(CONNECT_RETRY_DELAY).await,
                Opened::ConnectFailed => bail!("discovered next hop unreachable"),
                Opened::NoRoute => {
                    let summary = self.discover(Input::Want { destination }).await;
                    if !wait {
                        bail!("no route yet (discovering)");
                    }
                    if summary.failed.contains(&destination) {
                        bail!("route discovery failed");
                    }
                    if !summary.found.contains(&destination) {
                        changed.await;
                    }
                }
            }
        }
    }

    /// Feed one input to the state machine and carry out everything it asks for.
    /// `Deliver` outputs are returned for the caller to process.
    async fn discover(&self, input: Input) -> Summary {
        let mut summary = Summary::default();
        let mut queue = VecDeque::from([input]);
        self.discover_queue(&mut queue, &mut summary).await;
        summary
    }

    async fn discover_queue(&self, queue: &mut VecDeque<Input>, summary: &mut Summary) {
        let mut steps = 0;
        while let Some(input) = queue.pop_front() {
            steps += 1;
            if steps > MAX_DISCOVERY_STEPS {
                break;
            }
            let relaying = matches!(input, Input::Frame { .. });
            let outputs = self.step(input);
            self.apply(outputs, relaying, queue, summary).await;
        }
    }

    /// Run the state machine; counters are persisted before any send of the same batch.
    fn step(&self, input: Input) -> Vec<Output> {
        let Some(rt) = &self.discovery else {
            return Vec::new();
        };
        let mut engine = rt.engine.lock().unwrap();
        let mut outputs = engine.handle(rt.now(), input);
        if outputs.iter().any(|o| matches!(o, Output::CountersChanged))
            && let Err(error) = rt.persist(&engine.counters())
        {
            outputs.retain(|o| !matches!(o, Output::Send { .. }));
            self.emit(
                "discovery_error",
                json!({"error":format!("could not persist discovery counters: {error:#}")}),
            );
        }
        outputs
    }

    async fn apply(
        &self,
        outputs: Vec<Output>,
        relaying: bool,
        queue: &mut VecDeque<Input>,
        summary: &mut Summary,
    ) {
        let Some(rt) = &self.discovery else {
            return;
        };
        let mut sends = JoinSet::new();
        for output in outputs {
            match output {
                Output::Send { link, frame } => {
                    let (Some(addr), Ok(bytes)) = (rt.address(link), frame.to_bytes()) else {
                        continue;
                    };
                    // A relay records only the label and size: it never opens a hop.
                    let relayed = match (&frame, relaying) {
                        (Frame::Hop { label, sealed, .. }, true) => {
                            Some((hex::encode(label.0), sealed.len()))
                        }
                        _ => None,
                    };
                    sends.spawn(async move {
                        let sent = send_bytes(addr, &bytes).await.is_ok();
                        (link, frame, addr, relayed, sent)
                    });
                }
                Output::RouteFound { destination } => {
                    summary.found.push(destination);
                    self.emit("route_found", json!({"destination":destination}));
                    rt.changed.notify_waiters();
                }
                Output::RouteFailed { destination } => {
                    summary.failed.push(destination);
                    self.emit("route_failed", json!({"destination":destination}));
                    rt.changed.notify_waiters();
                }
                Output::RouteInvalidated { destination } => {
                    self.emit("route_invalidated", json!({"destination":destination}));
                    rt.changed.notify_waiters();
                }
                Output::NoRoute { .. } => summary.no_route = true,
                Output::Deliver { envelope } => summary.deliver.push(envelope),
                Output::DeliverDatagram {
                    source,
                    bytes,
                    hops_used,
                } => summary.datagrams.push((source, bytes, hops_used)),
                Output::CountersChanged => {}
            }
        }
        while let Some(done) = sends.join_next().await {
            let Ok((link, frame, addr, relayed, sent)) = done else {
                continue;
            };
            if sent {
                if matches!(
                    frame,
                    Frame::Hop {
                        hops_left: Some(_),
                        ..
                    }
                ) {
                    summary.sent_datagram = true;
                }
                if let Some((label, sealed_bytes)) = relayed {
                    self.counters.forwarded.fetch_add(1, Ordering::Relaxed);
                    self.emit(
                        "forwarded",
                        json!({"label":label,"sealed_bytes":sealed_bytes,"via":addr,"discovered":true}),
                    );
                }
            } else if matches!(frame, Frame::Hop { .. }) {
                queue.push_back(Input::ForwardFailed { link, frame });
            }
        }
    }

    /// Ask discovery to wrap and address `packet`, then connect to the first link
    /// before anything is written, so a failed connect never risks a duplicate.
    async fn open_discovered(&self, packet: &Envelope) -> Result<Opened> {
        let rt = self.discovery.as_ref().context("discovery is disabled")?;
        let outputs = self.step(Input::SendEnvelope {
            destination: packet.destination,
            envelope: packet.clone(),
        });
        let mut hop = None;
        let mut rest = Vec::new();
        for output in outputs {
            match output {
                Output::Send {
                    link,
                    frame: frame @ Frame::Hop { .. },
                } if hop.is_none() => hop = Some((link, frame)),
                other => rest.push(other),
            }
        }
        let mut summary = Summary::default();
        let mut queue = VecDeque::new();
        self.apply(rest, false, &mut queue, &mut summary).await;
        self.discover_queue(&mut queue, &mut summary).await;
        let Some((link, frame)) = hop else {
            return Ok(Opened::NoRoute);
        };
        let addr = rt.address(link).context("unknown discovery link")?;
        match timeout(IO_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => Ok(Opened::Ready(Box::new(Prepared {
                link,
                frame,
                stream,
                addr,
            }))),
            _ => {
                self.discover(Input::ForwardFailed { link, frame }).await;
                Ok(Opened::ConnectFailed)
            }
        }
    }

    async fn finish_discovered(&self, packet: &Envelope, prepared: Box<Prepared>) -> Result<()> {
        let Prepared {
            link,
            frame,
            mut stream,
            addr,
        } = *prepared;
        let written = async {
            let bytes = frame.to_bytes()?;
            timeout(IO_TIMEOUT, wire::write_bytes(&mut stream, &bytes))
                .await
                .context("link write timed out")?
        }
        .await;
        if let Err(error) = written {
            self.discover(Input::ForwardFailed { link, frame }).await;
            return Err(error.context("discovered next-hop write failed"));
        }
        self.emit(
            "originated",
            json!({"message_id":packet.id,"source":packet.source,"destination":packet.destination,
                "remaining_hops":packet.remaining_hops,"via":addr,"discovered":true}),
        );
        Ok(())
    }

    /// Handle one discovery frame from the network; deliveries go through the normal receive path.
    async fn discovery_frame(&self, frame: Frame) -> Result<()> {
        ensure!(self.discovery.is_some(), "discovery is not enabled");
        let summary = self.discover(Input::Frame { link: None, frame }).await;
        for (source, bytes, hops) in summary.datagrams {
            let allow_open =
                self.mailbox.is_some() && self.mailbox_route_contacts.contains(&source);
            self.mailbox_network
                .incoming(source, bytes, hops, allow_open);
        }
        let mut result = Ok(());
        for envelope in summary.deliver {
            if let Err(error) = self.received(envelope).await {
                result = Err(error);
            }
        }
        result
    }

    pub(crate) async fn send_mailbox_datagram(
        &self,
        peer: NodeId,
        bytes: Vec<u8>,
        max_hops: u8,
    ) -> Result<()> {
        let rt = self
            .discovery
            .as_ref()
            .context("mailbox discovery is disabled")?;
        loop {
            let changed = rt.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if rt.engine.lock().unwrap().has_route(peer) {
                break;
            }
            ensure!(
                self.mailbox_route_key
                    .is_some_and(|key| key.node_id() == peer),
                "mailbox return route is closed"
            );
            let summary = self.discover(Input::Want { destination: peer }).await;
            ensure!(
                !summary.failed.contains(&peer),
                "mailbox relay route pending"
            );
            if !summary.found.contains(&peer) {
                changed.await;
            }
        }
        let summary = self
            .discover(Input::SendDatagram {
                destination: peer,
                bytes,
                max_hops,
            })
            .await;
        ensure!(summary.sent_datagram, "mailbox next hop unavailable");
        Ok(())
    }

    async fn mailbox_transport(
        &self,
        relay: MailboxRelay,
    ) -> Result<mailbox::protocol::Transport<'_>> {
        if let Some(address) = relay.address {
            let stream = timeout(IO_TIMEOUT, TcpStream::connect(address)).await??;
            return Ok(mailbox::protocol::Transport::Direct(stream));
        }
        ensure!(
            self.mailbox_route_key == Some(relay.public_key),
            "pin a mailbox discovery route and its separate identity first"
        );
        let circuit = mailbox::routed::Circuit::client(self, relay.public_key.node_id())?;
        Ok(mailbox::protocol::Transport::Discovered(circuit))
    }

    fn accept_receipt(&self, message_id: Uuid, source: NodeId, receipt_id: Uuid) -> Result<()> {
        let immediate = {
            let mut pending = self.pending.lock().unwrap();
            if pending
                .get(&message_id)
                .is_some_and(|p| p.recipient == source)
            {
                if let Some(p) = pending.remove(&message_id) {
                    let _ = p.reply.send(receipt_id);
                }
                true
            } else {
                false
            }
        };
        let durable = if immediate {
            false
        } else {
            self.outbox
                .lock()
                .unwrap()
                .confirm(message_id, source, receipt_id)?
        };
        if immediate || durable {
            if durable {
                self.outbox_wake.notify_one();
            }
            self.counters.receipts.fetch_add(1, Ordering::Relaxed);
            self.emit(
                "delivery_confirmed",
                json!({"message_id":message_id,"recipient":source}),
            );
        } else {
            self.counters
                .ignored_receipts
                .fetch_add(1, Ordering::Relaxed);
            self.emit(
                "ignored_receipt",
                json!({"message_id":message_id,"source":source}),
            );
        }
        Ok(())
    }

    fn save_text(
        &self,
        key: PublicKey,
        packet: &Envelope,
        redelivery_id: Option<Uuid>,
    ) -> Result<bool> {
        let accepted =
            self.inbox
                .lock()
                .unwrap()
                .accept(&self.identity, key, packet, outbox::now_ms()?)?;
        self.router.lock().unwrap().confirm_delivery(packet.id);
        if let Some(redelivery_id) = redelivery_id {
            self.emit(
                "redelivery_received",
                json!({"message_id":packet.id,"redelivery_id":redelivery_id}),
            );
        }
        match accepted {
            Acceptance::New { text } => {
                self.counters.delivered.fetch_add(1, Ordering::Relaxed);
                self.emit(
                    "text_received",
                    json!({"message_id":packet.id,"source":packet.source,"text":text}),
                );
            }
            Acceptance::Duplicate => {
                self.counters.duplicates.fetch_add(1, Ordering::Relaxed);
            }
            Acceptance::Forgotten => {
                self.counters.duplicates.fetch_add(1, Ordering::Relaxed);
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn accept_text(
        &self,
        key: PublicKey,
        packet: &Envelope,
        redelivery_id: Option<Uuid>,
    ) -> Result<()> {
        if self.save_text(key, packet, redelivery_id)? {
            self.issue_receipt(key, packet.id).await?;
        }
        Ok(())
    }

    async fn received(&self, mut packet: Envelope) -> Result<()> {
        if packet.destination == self.identity.id() {
            let key = self
                .contact(&packet.source)
                .context("sender has no pinned contact key")?;
            let payload = self.identity.open(key, &packet)?;
            match payload {
                Payload::Text { .. } => {
                    self.accept_text(key, &packet, None).await?;
                }
                Payload::Redelivery { packet: original } => {
                    ensure!(
                        original.source == packet.source
                            && original.destination == packet.destination
                            && original.id != packet.id,
                        "redelivery endpoint/id mismatch"
                    );
                    self.accept_text(key, &original, Some(packet.id)).await?;
                    self.router.lock().unwrap().confirm_delivery(packet.id);
                }
                Payload::Receipt { message_id } => {
                    if self.router.lock().unwrap().confirm_delivery(packet.id) {
                        self.accept_receipt(message_id, packet.source, packet.id)?;
                    } else {
                        self.dropped(&packet, DropReason::Duplicate)?;
                    }
                }
                Payload::ReceiptRequest { message_id } => {
                    if !self.router.lock().unwrap().confirm_delivery(packet.id) {
                        return self.dropped(&packet, DropReason::Duplicate);
                    }
                    let known = self
                        .inbox
                        .lock()
                        .unwrap()
                        .retained_from(message_id, packet.source)?;
                    self.emit(
                        "receipt_requested",
                        json!({"message_id":message_id,"requester":packet.source,"known":known}),
                    );
                    if known {
                        self.issue_receipt(key, message_id).await?;
                    }
                }
            }
            return Ok(());
        }
        let action = self
            .router
            .lock()
            .unwrap()
            .route(&mut packet, Origin::Network)?;
        match action {
            Action::Forward(via) => self.transmit(&packet, via, true).await,
            Action::Drop(reason) => self.dropped(&packet, reason),
            Action::Deliver => bail!("unexpected transit delivery"),
        }
    }

    async fn issue_receipt(&self, key: PublicKey, message_id: Uuid) -> Result<()> {
        let receipt = self.identity.seal(key, Payload::Receipt { message_id })?;
        self.originate(receipt, None).await.map(|_| ())
    }

    async fn send(&self, to: NodeId, message: String, timeout_ms: u64) -> Result<Value> {
        ensure!(
            (100..=10_000).contains(&timeout_ms),
            "timeout_ms must be 100..10000"
        );
        let key = self
            .contact(&to)
            .context("recipient has no pinned contact key")?;
        let packet = self.identity.seal(key, Payload::Text { text: message })?;
        let id = packet.id;
        let (reply, receive) = oneshot::channel();
        {
            let mut pending = self.pending.lock().unwrap();
            ensure!(pending.len() < MAX_PENDING, "pending-send limit reached");
            pending.insert(
                id,
                Pending {
                    recipient: to,
                    reply,
                },
            );
        }
        let _pending = PendingSend {
            id,
            pending: &self.pending,
        };
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let discovered = std::sync::atomic::AtomicBool::new(false);
        let result = timeout_at(deadline, async {
            let used = self.originate(packet, Some(deadline)).await?;
            discovered.store(used, Ordering::Relaxed);
            receive.await.context("delivery confirmation was cancelled")
        })
        .await;
        if !matches!(result, Ok(Ok(_))) && discovered.load(Ordering::Relaxed) {
            // No receipt on a discovered path: forget it so the next attempt rediscovers.
            self.discover(Input::Invalidate { destination: to }).await;
        }
        let receipt_id = result.context("delivery not confirmed before timeout")??;
        Ok(json!({"status":"delivered", "message_id":id,"recipient":to,"receipt_id":receipt_id}))
    }

    async fn request(&self, request: Request) -> Response {
        let result = match request {
            Request::Status => Ok(self.status()),
            Request::Relay { enabled } => {
                self.router.lock().unwrap().set_relay(enabled);
                if let Some(rt) = &self.discovery {
                    rt.engine.lock().unwrap().set_relay(enabled);
                }
                Ok(json!({"relay":enabled}))
            }
            Request::Send {
                to,
                message,
                timeout_ms,
            } => self.send(to, message, timeout_ms).await,
            Request::Queue {
                to,
                message,
                ttl_ms,
            } => self.queue_text(to, message, ttl_ms, None),
            Request::QueueMailbox {
                to,
                message,
                relay,
                label,
                ttl_ms,
            } => self.queue_text(to, message, ttl_ms, Some((relay, label))),
            Request::RegisterMailbox { label, public_key } => (|| -> Result<Value> {
                self.mailbox
                    .as_ref()
                    .context("mailbox storage is disabled")?
                    .lock()
                    .unwrap()
                    .register(label, public_key)?;
                Ok(json!({"registered": true}))
            })(),
            Request::Outbox => Ok(self.outbox.lock().unwrap().list()),
            Request::Inbox => Ok(self.inbox.lock().unwrap().list()),
            Request::InboxRead { message_id } => {
                self.inbox.lock().unwrap().read(message_id, &self.identity)
            }
            Request::InboxForget { message_id } => self.inbox.lock().unwrap().forget(message_id),
            Request::Cancel { message_id } => self.outbox.lock().unwrap().cancel(message_id),
            Request::Forget { message_id } => self.outbox.lock().unwrap().forget(message_id),
            Request::AddContact { public_key } => self.add_contact(public_key),
            Request::LinkUp { address, kind } => self.link_up(address, kind).await,
            Request::LinkDown { address } => self.link_down(address).await,
            Request::Route { to, via } => {
                if via.port() == 0 {
                    return Response::failure("next-hop port must not be zero");
                }
                self.router
                    .lock()
                    .unwrap()
                    .set_route(to, via)
                    .map(|_| json!({"destination":to,"via":via}))
                    .map_err(Into::into)
            }
        };
        match result {
            Ok(value) => Response::success(value),
            Err(error) => Response::failure(error),
        }
    }

    fn queue_text(
        &self,
        to: NodeId,
        message: String,
        ttl_ms: u64,
        target: Option<(MailboxRelay, Uuid)>,
    ) -> Result<Value> {
        let key = self
            .contact(&to)
            .context("recipient has no pinned contact key")?;
        let packet = self.identity.seal(key, Payload::Text { text: message })?;
        let now = outbox::now_ms()?;
        let deposit = target
            .map(|(relay, label)| {
                ensure!(
                    relay.address.is_some() || self.mailbox_route_key == Some(relay.public_key),
                    "pin a mailbox discovery route and its separate identity first"
                );
                Deposit::new(relay, label, key, &packet)
            })
            .transpose()?;
        let result = {
            let mut outbox = self.outbox.lock().unwrap();
            if let Some(deposit) = deposit {
                outbox.enqueue_mailbox(packet, ttl_ms, now, deposit)?
            } else {
                outbox.enqueue(packet, ttl_ms, now)?
            }
        };
        self.emit("queued", result.clone());
        self.outbox_wake.notify_one();
        Ok(result)
    }

    async fn pickup_mailbox_page(
        &self,
        pickup: &PickupRuntime,
        label: Uuid,
        cursor: &mut Option<Uuid>,
    ) -> Result<bool> {
        let Some(page) = mailbox::protocol::pickup(
            self.mailbox_transport(pickup.relay).await?,
            pickup.relay,
            label,
            &pickup.identity,
            *cursor,
        )
        .await?
        else {
            *cursor = None;
            return Ok(false);
        };
        // Advance even if a page cannot be saved, so it does not block the next copy.
        *cursor = Some(page.copy.id);
        let packet = page
            .copy
            .open(&self.identity, label, page.deposit_hops, page.pickup_hops)?;
        let key = self
            .contact(&packet.source)
            .context("mailbox sender has no pinned contact key")?;
        if self.save_text(key, &packet, None)? {
            let released = page.acknowledge().await;
            let _ = self.issue_receipt(key, packet.id).await;
            released?;
        }
        Ok(true)
    }

    async fn run_pickup(&self, pickup: &PickupRuntime) {
        let mut cursors = vec![None; pickup.labels.len()];
        let mut failures = 0_u32;
        loop {
            for (&label, cursor) in pickup.labels.iter().zip(&mut cursors) {
                let got_page = match self.pickup_mailbox_page(pickup, label, cursor).await {
                    Ok(got_page) => {
                        failures = 0;
                        got_page
                    }
                    Err(_) => {
                        if failures == 0 {
                            self.emit("mailbox_pickup_failed", json!({}));
                        }
                        failures = failures.saturating_add(1);
                        false
                    }
                };
                sleep(mailbox::pickup_delay(failures, got_page)).await;
            }
        }
    }

    async fn recover_receipt(&self, recipient: NodeId, message_id: Uuid, now: u64) -> Result<()> {
        let Some(key) = self.contact(&recipient) else {
            self.outbox.lock().unwrap().query_attempt(
                message_id,
                now,
                Some(Failure::RecipientNotPinned),
            )?;
            return Ok(());
        };
        let mut query = self
            .identity
            .seal(key, Payload::ReceiptRequest { message_id })?;
        let action = self
            .router
            .lock()
            .unwrap()
            .route(&mut query, Origin::Local)?;
        if self.discovery_applies(&action, &recipient) {
            let opened = self.open_discovered(&query).await?;
            if matches!(opened, Opened::NoRoute) {
                self.discover(Input::Want {
                    destination: recipient,
                })
                .await;
                self.outbox.lock().unwrap().query_attempt(
                    message_id,
                    now,
                    Some(Failure::NoRoute),
                )?;
                return Ok(());
            }
            if !self
                .outbox
                .lock()
                .unwrap()
                .query_attempt(message_id, now, None)?
            {
                return Ok(());
            }
            let sent = match opened {
                Opened::Ready(prepared) => self.finish_discovered(&query, prepared).await.is_ok(),
                _ => false,
            };
            if sent {
                self.emit(
                    "receipt_request_sent",
                    json!({"message_id":message_id,"recipient":recipient}),
                );
            } else {
                self.outbox.lock().unwrap().query_failed(message_id)?;
            }
            return Ok(());
        }
        let Action::Forward(via) = action else {
            self.outbox
                .lock()
                .unwrap()
                .query_attempt(message_id, now, Some(Failure::NoRoute))?;
            return Ok(());
        };
        if !self
            .outbox
            .lock()
            .unwrap()
            .query_attempt(message_id, now, None)?
        {
            return Ok(());
        }
        if self.transmit(&query, via, false).await.is_err() {
            self.outbox.lock().unwrap().query_failed(message_id)?;
        } else {
            self.emit(
                "receipt_request_sent",
                json!({"message_id":message_id,"recipient":recipient}),
            );
        }
        Ok(())
    }

    async fn redeliver(&self, recipient: NodeId, original: Envelope, now: u64) -> Result<()> {
        let id = original.id;
        let Some(key) = self.contact(&recipient) else {
            self.outbox.lock().unwrap().redelivery_attempt(
                id,
                now,
                Some(Failure::RecipientNotPinned),
            )?;
            return Ok(());
        };
        let mut retry = match self
            .identity
            .seal(key, Payload::Redelivery { packet: original })
        {
            Ok(packet) => packet,
            Err(_) => {
                self.outbox.lock().unwrap().redelivery_attempt(
                    id,
                    now,
                    Some(Failure::RedeliverySendFailed),
                )?;
                return Ok(());
            }
        };
        let redelivery_id = retry.id;
        let action = self
            .router
            .lock()
            .unwrap()
            .route(&mut retry, Origin::Local)?;
        if self.discovery_applies(&action, &recipient) {
            let opened = self.open_discovered(&retry).await?;
            if matches!(opened, Opened::NoRoute) {
                self.discover(Input::Want {
                    destination: recipient,
                })
                .await;
                self.outbox
                    .lock()
                    .unwrap()
                    .redelivery_attempt(id, now, Some(Failure::NoRoute))?;
                return Ok(());
            }
            if !self
                .outbox
                .lock()
                .unwrap()
                .redelivery_attempt(id, now, None)?
            {
                return Ok(());
            }
            let sent = match opened {
                Opened::Ready(prepared) => self.finish_discovered(&retry, prepared).await.is_ok(),
                _ => false,
            };
            if sent {
                self.emit(
                    "redelivery_sent",
                    json!({"message_id":id,"redelivery_id":redelivery_id,"recipient":recipient}),
                );
            } else {
                self.outbox.lock().unwrap().redelivery_failed(id)?;
            }
            return Ok(());
        }
        let Action::Forward(via) = action else {
            self.outbox
                .lock()
                .unwrap()
                .redelivery_attempt(id, now, Some(Failure::NoRoute))?;
            return Ok(());
        };
        if !self
            .outbox
            .lock()
            .unwrap()
            .redelivery_attempt(id, now, None)?
        {
            return Ok(());
        }
        if self.transmit(&retry, via, false).await.is_err() {
            self.outbox.lock().unwrap().redelivery_failed(id)?;
        } else {
            self.emit(
                "redelivery_sent",
                json!({"message_id":id,"redelivery_id":redelivery_id,"recipient":recipient}),
            );
        }
        Ok(())
    }

    /// A queued message with no manual route: use a discovered path, or start discovery
    /// and let the outbox's normal retry schedule come back.
    async fn process_discovered(&self, id: Uuid, now: u64, packet: &Envelope) -> Result<()> {
        match self.open_discovered(packet).await? {
            Opened::NoRoute => {
                self.discover(Input::Want {
                    destination: packet.destination,
                })
                .await;
                self.outbox
                    .lock()
                    .unwrap()
                    .attempt(id, now, Some(Failure::NoRoute))
            }
            Opened::ConnectFailed => {
                let mut outbox = self.outbox.lock().unwrap();
                outbox.attempt(id, now, None)?;
                outbox.note_failure(id, Failure::ConnectFailed)
            }
            Opened::Ready(prepared) => {
                self.outbox.lock().unwrap().attempt(id, now, None)?;
                if !self
                    .outbox
                    .lock()
                    .unwrap()
                    .begin_transmission(id, outbox::now_ms()?)?
                {
                    return Ok(());
                }
                if self.finish_discovered(packet, prepared).await.is_err() {
                    self.outbox
                        .lock()
                        .unwrap()
                        .unconfirmed(id, Failure::WriteFailed)?;
                }
                Ok(())
            }
        }
    }

    async fn process_outbox(&self) -> Result<()> {
        let now = outbox::now_ms()?;
        let ready = self.outbox.lock().unwrap().ready(now)?;
        let Some(mut entry) = ready else {
            return Ok(());
        };
        let id = entry.packet.id;
        if let Some(deposit) = &entry.mailbox {
            if deposit.result.is_some() {
                return self
                    .recover_receipt(entry.packet.destination, id, now)
                    .await;
            }
            if !self.outbox.lock().unwrap().begin_mailbox_attempt(id, now)? {
                return Ok(());
            }
            let (expires_at, lifetime) = entry.lifetime();
            let result = match self.mailbox_transport(deposit.relay).await {
                Ok(transport) => {
                    mailbox::protocol::deposit(transport, deposit, expires_at, lifetime)
                        .await
                        .ok()
                }
                Err(_) => None,
            };
            return self
                .outbox
                .lock()
                .unwrap()
                .finish_mailbox_attempt(id, result);
        }
        if entry.state == DeliveryState::Unconfirmed {
            if entry.first_recovery() && self.discovery.is_some() {
                // The receipt never came: do not trust the discovered path for the retries.
                let destination = entry.packet.destination;
                self.discover(Input::Invalidate { destination }).await;
            }
            if entry.needs_receipt_query() {
                return self
                    .recover_receipt(entry.packet.destination, id, now)
                    .await;
            }
            return self
                .redeliver(entry.packet.destination, entry.packet, now)
                .await;
        }
        if self.contact(&entry.packet.destination).is_none() {
            return self
                .outbox
                .lock()
                .unwrap()
                .attempt(id, now, Some(Failure::RecipientNotPinned));
        }
        let action = self
            .router
            .lock()
            .unwrap()
            .resume_local(&mut entry.packet)?;
        if self.discovery_applies(&action, &entry.packet.destination) {
            return self.process_discovered(id, now, &entry.packet).await;
        }
        let Action::Forward(via) = action else {
            return self
                .outbox
                .lock()
                .unwrap()
                .attempt(id, now, Some(Failure::NoRoute));
        };
        self.outbox.lock().unwrap().attempt(id, now, None)?;
        let stream = match timeout(IO_TIMEOUT, TcpStream::connect(via)).await {
            Ok(Ok(stream)) => stream,
            _ => {
                return self
                    .outbox
                    .lock()
                    .unwrap()
                    .note_failure(id, Failure::ConnectFailed);
            }
        };
        if !self
            .outbox
            .lock()
            .unwrap()
            .begin_transmission(id, outbox::now_ms()?)?
        {
            return Ok(());
        }
        if self
            .transmit_connected(&entry.packet, via, false, stream)
            .await
            .is_err()
        {
            self.outbox
                .lock()
                .unwrap()
                .unconfirmed(id, Failure::WriteFailed)?;
        }
        Ok(())
    }
}

struct SocketGuard {
    path: PathBuf,
    inode: u64,
    device: u64,
    _state_lock: fs::File,
}
impl SocketGuard {
    fn new(path: PathBuf, state_lock: fs::File) -> Result<Self> {
        let m = fs::symlink_metadata(&path)?;
        Ok(Self {
            path,
            inode: m.ino(),
            device: m.dev(),
            _state_lock: state_lock,
        })
    }
}

fn lock_node_state(control: &Path) -> Result<fs::File> {
    let mut name = control
        .file_name()
        .context("control socket needs a file name")?
        .to_os_string();
    name.push(".lock");
    let path = control.with_file_name(name);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => ensure!(
            metadata.is_file()
                && !metadata.file_type().is_symlink()
                && metadata.permissions().mode() & 0o077 == 0,
            "node lock must be a private regular file"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)?;
    let metadata = fs::symlink_metadata(&path)?;
    let opened = file.metadata()?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.permissions().mode() & 0o077 == 0
            && metadata.ino() == opened.ino()
            && metadata.dev() == opened.dev(),
        "node lock must be a private regular file"
    );
    file.try_lock().map_err(|_| {
        anyhow::anyhow!("node state is already locked or file locking is unavailable")
    })?;
    Ok(file)
}

async fn bind_control(control: &Path) -> Result<UnixListener> {
    match UnixListener::bind(control) {
        Ok(listener) => Ok(listener),
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            let metadata = fs::symlink_metadata(control)?;
            ensure!(
                metadata.file_type().is_socket() && metadata.permissions().mode() & 0o077 == 0,
                "existing control path must be a private socket"
            );
            match timeout(IO_TIMEOUT, UnixStream::connect(control)).await {
                Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => {}
                _ => bail!("existing control socket is live or could not be checked"),
            }
            let current = fs::symlink_metadata(control)?;
            ensure!(
                current.ino() == metadata.ino() && current.dev() == metadata.dev(),
                "control socket changed during recovery"
            );
            fs::remove_file(control)?;
            UnixListener::bind(control).context("rebind stale control socket")
        }
        Err(error) => Err(error.into()),
    }
}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Ok(m) = fs::symlink_metadata(&self.path)
            && m.file_type().is_socket()
            && m.ino() == self.inode
            && m.dev() == self.device
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// A running instance of the same service hosted by desktop or Android.
pub struct Node {
    state: Arc<State>,
    stop: Option<oneshot::Sender<()>>,
    job: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl Config {
    /// Validate settings without opening listeners or interrupting a running node.
    pub fn validate(&self, id: NodeId) -> Result<()> {
        if let Some(mailbox) = self.mailbox {
            ensure!(
                self.listen.ip().is_loopback(),
                "prototype mailbox storage requires a loopback listener"
            );
            mailbox.validate()?;
        }
        if let Some(pickup) = &self.pickup {
            pickup.validate(id)?;
            ensure!(
                pickup.relay.address.is_some()
                    || self
                        .mailbox_route
                        .as_ref()
                        .is_some_and(|route| route.relay_key == pickup.relay.public_key),
                "discovered pickup requires a matching mailbox route identity"
            );
        }
        let discovered_mailbox =
            self.mailbox_route.is_some() || !self.mailbox_route_contacts.is_empty();
        ensure!(
            !discovered_mailbox || self.discovery || !self.links.is_empty(),
            "mailbox routes require discovery"
        );
        ensure!(
            self.mailbox_route
                .as_ref()
                .is_none_or(|route| route.relay_key.node_id() != id),
            "mailbox relay must be another node"
        );
        let aliases: HashSet<_> = self.mailbox_route_contacts.iter().collect();
        ensure!(
            aliases.len() <= 128
                && aliases.len() == self.mailbox_route_contacts.len()
                && aliases.iter().all(|key| key.node_id() != id)
                && (aliases.is_empty() || self.mailbox.is_some()),
            "invalid mailbox discovery contacts"
        );
        self.routing(id).map(|_| ())
    }

    fn routing(&self, id: NodeId) -> Result<(HashMap<NodeId, PublicKey>, Router<SocketAddr>)> {
        ensure!(
            !self.name.is_empty()
                && self.name.len() <= 32
                && self
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b)),
            "node name must be 1..32 ASCII letters, digits, underscores or hyphens"
        );
        ensure!(self.contacts.len() <= MAX_CONTACTS, "too many contacts");
        let mut contacts = HashMap::new();
        for &key in &self.contacts {
            ensure!(
                contacts.insert(key.node_id(), key).is_none(),
                "duplicate contact"
            );
        }
        ensure!(
            self.links.len() <= MAX_DISCOVERY_LINKS,
            "too many discovery links"
        );
        let mut addresses = HashSet::new();
        for link in &self.links {
            ensure!(
                link.address.port() != 0,
                "discovery link port must not be zero"
            );
            ensure!(addresses.insert(link.address), "duplicate discovery link");
        }
        let mut router = Router::new(id, self.relay);
        for route in &self.routes {
            ensure!(route.via.port() != 0, "next-hop port must not be zero");
            router.set_route(route.destination, route.via)?;
        }
        Ok((contacts, router))
    }

    pub fn read(path: &Path) -> Result<Self> {
        use std::io::Read;
        let mut bytes = Vec::new();
        fs::File::open(path)
            .context("open config")?
            .take(128 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 128 * 1024, "config exceeds size limit");
        let mut config: Self = serde_json::from_slice(&bytes).context("read node config")?;
        let base = path.parent().unwrap_or(Path::new("."));
        if config.identity.is_relative() {
            config.identity = base.join(&config.identity);
        }
        if config.control_socket.is_relative() {
            config.control_socket = base.join(&config.control_socket);
        }
        if let Some(pickup) = &mut config.pickup
            && pickup.identity.is_relative()
        {
            pickup.identity = base.join(&pickup.identity);
        }
        if let Some(route) = &mut config.mailbox_route
            && route.identity.is_relative()
        {
            route.identity = base.join(&route.identity);
        }
        Ok(config)
    }
}

impl Node {
    pub async fn start(config: Config, events: EventSink) -> Result<Self> {
        let identity = storage::load_identity(&config.identity)?;
        config.validate(identity.id())?;
        let parent = config
            .control_socket
            .parent()
            .context("control socket needs a parent directory")?;
        storage::private_directory(parent)?;
        let state_lock = lock_node_state(&config.control_socket)?;
        let tcp = TcpListener::bind(config.listen)
            .await
            .context("bind node TCP listener")?;
        let control = bind_control(&config.control_socket)
            .await
            .context("bind local control socket")?;
        let guard = SocketGuard::new(config.control_socket.clone(), state_lock)?;
        fs::set_permissions(&config.control_socket, fs::Permissions::from_mode(0o600))?;
        let inbox = Inbox::open(Inbox::path_for(&config.control_socket)?, &identity)?;
        let outbox = Outbox::open(
            Outbox::path_for(&config.control_socket)?,
            identity.id(),
            outbox::now_ms()?,
        )?;
        let (mailbox_network, mailbox_starts) = mailbox::routed::Network::new();
        let state = Arc::new(State::new(
            &config,
            identity,
            tcp.local_addr()?,
            events,
            outbox,
            inbox,
            mailbox_network,
        )?);
        let (stop, receive) = oneshot::channel();
        let served = state.clone();
        let job = tokio::spawn(async move {
            serve(tcp, control, guard, served, receive, mailbox_starts).await
        });
        state.emit("ready", state.status());
        Ok(Self {
            state,
            stop: Some(stop),
            job: Some(job),
        })
    }

    pub fn status(&self) -> Value {
        self.state.status()
    }

    pub async fn request(&self, request: Request) -> Response {
        self.state.request(request).await
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(job) = self.job.take() {
            job.await.context("node task failed")??;
        }
        Ok(())
    }
}

pub async fn run(path: &Path) -> Result<()> {
    let node = Node::start(Config::read(path)?, Arc::new(|value| println!("{value}"))).await?;
    tokio::signal::ctrl_c()
        .await
        .context("wait for shutdown signal")?;
    node.shutdown().await
}

async fn serve(
    tcp: TcpListener,
    control: UnixListener,
    _guard: SocketGuard,
    state: Arc<State>,
    mut stop: oneshot::Receiver<()>,
    mut mailbox_starts: mpsc::Receiver<mailbox::routed::Start>,
) -> Result<()> {
    // Sends hold control handlers while waiting for network receipts. Give the
    // two interfaces independent capacity so control load cannot exclude ACKs.
    let mut network_jobs = JoinSet::new();
    let mut control_jobs = JoinSet::new();
    let mut background_jobs = JoinSet::new();
    let mut mailbox_jobs = JoinSet::new();
    let worker = state.clone();
    background_jobs.spawn(async move {
        let mut reported_error = false;
        loop {
            let wake = worker.outbox_wake.notified();
            match worker.process_outbox().await {
                Ok(()) => reported_error = false,
                Err(_) if !reported_error => {
                    worker.emit("outbox_error", json!({"error":"outbox processing failed; inspect outbox status and persisted state"}));
                    reported_error = true;
                }
                Err(_) => {}
            }
            let wait_ms = worker.outbox.lock().unwrap().wait_ms(outbox::now_ms().unwrap_or(0));
            if let Some(wait_ms) = wait_ms {
                tokio::select! { _ = sleep(Duration::from_millis(wait_ms)) => {}, _ = wake => {} }
            } else {
                wake.await;
            }
        }
    });
    if let Some(rt) = &state.discovery {
        let node = state.clone();
        let links: Vec<_> = rt
            .links
            .lock()
            .unwrap()
            .iter()
            .map(|(id, link)| (*id, LinkKind::from(link.kind)))
            .collect();
        background_jobs.spawn(async move {
            for (link, kind) in links {
                node.discover(Input::LinkUp { link, kind }).await;
            }
            let mut ticks = interval(DISCOVERY_TICK);
            ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                node.discover(Input::Tick).await;
            }
        });
    }
    if state.mailbox.is_some() {
        let node = state.clone();
        background_jobs.spawn(async move {
            let mut ticks = interval(Duration::from_secs(5));
            ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut reported_error = false;
            loop {
                ticks.tick().await;
                let result = node.mailbox.as_ref().unwrap().lock().unwrap().reap();
                if result.is_err() && !reported_error {
                    node.emit("mailbox_storage_error", json!({}));
                }
                reported_error = result.is_err();
            }
        });
    }
    if state.pickup.is_some() {
        let node = state.clone();
        background_jobs.spawn(async move {
            node.run_pickup(node.pickup.as_ref().unwrap()).await;
        });
    }
    loop {
        // Completed tasks still occupy JoinSet entries. Reap them on every
        // iteration and count all remaining entries against each fixed bound.
        while network_jobs.try_join_next().is_some() {}
        while control_jobs.try_join_next().is_some() {}
        while mailbox_jobs.try_join_next().is_some() {}
        tokio::select! {
            result = tcp.accept() => {
                let (mut stream, _) = result?;
                if network_jobs.len() < MAX_NETWORK_HANDLERS {
                    let state = state.clone();
                    network_jobs.spawn(async move {
                        let result: Result<()> = async {
                            let bytes = timeout(IO_TIMEOUT, wire::read_bytes(&mut stream)).await.context("incoming frame timed out")??;
                            if let Some(hello) = bytes.strip_prefix(mailbox::protocol::PREFIX) {
                                let store = state.mailbox.as_ref().context("mailbox storage is disabled")?;
                                return timeout(mailbox::protocol::EXCHANGE_TIMEOUT,
                                    mailbox::protocol::serve(mailbox::protocol::Transport::Direct(stream), &state.identity, store, hello))
                                    .await.context("mailbox exchange timed out")?
                                    .map_err(|_| anyhow::anyhow!("mailbox exchange rejected"));
                            }
                            match parse_network_frame(&bytes)? {
                                NetworkFrame::Legacy(packet) => state.received(packet).await,
                                NetworkFrame::Discovery(frame) => state.discovery_frame(frame).await,
                            }
                        }.await;
                        if let Err(error) = result {
                            state.counters.rejected.fetch_add(1, Ordering::Relaxed);
                            state.emit("rejected", json!({"error":format!("{error:#}")}));
                        }
                    });
                }
            },
            Some(start) = mailbox_starts.recv(), if mailbox_jobs.len() < MAX_NETWORK_HANDLERS => {
                let state = state.clone();
                mailbox_jobs.spawn(async move {
                    let result = async {
                        let store = state.mailbox.as_ref().context("mailbox storage is disabled")?;
                        let circuit = mailbox::routed::Circuit::relay(&state, &start)?;
                        mailbox::protocol::serve(mailbox::protocol::Transport::Discovered(circuit),
                            &state.identity, store, &start.hello).await
                    };
                    if !matches!(timeout(mailbox::protocol::EXCHANGE_TIMEOUT, result).await, Ok(Ok(()))) {
                        state.counters.rejected.fetch_add(1, Ordering::Relaxed);
                        state.emit("rejected", json!({"error":"discovered mailbox exchange failed"}));
                    }
                });
            },
            result = control.accept() => {
                let (mut stream, _) = result?;
                if control_jobs.len() < MAX_CONTROL_HANDLERS {
                    let state = state.clone();
                    control_jobs.spawn(async move {
                        let reply = match timeout(IO_TIMEOUT, wire::read::<Request, _>(&mut stream)).await {
                            Ok(Ok(request)) => state.request(request).await,
                            Ok(Err(error)) => Response::failure(error),
                            Err(_) => Response::failure("control frame timed out"),
                        };
                        let _ = timeout(IO_TIMEOUT, wire::write(&mut stream, &reply)).await;
                    });
                }
            },
            _ = network_jobs.join_next(), if !network_jobs.is_empty() => {},
            _ = control_jobs.join_next(), if !control_jobs.is_empty() => {},
            _ = mailbox_jobs.join_next(), if !mailbox_jobs.is_empty() => {},
            result = background_jobs.join_next() => {
                result.context("background worker disappeared")?.context("background worker failed")?;
                bail!("background worker stopped unexpectedly");
            },
            _ = &mut stop => break,
        }
    }
    network_jobs.abort_all();
    control_jobs.abort_all();
    background_jobs.abort_all();
    mailbox_jobs.abort_all();
    while network_jobs.join_next().await.is_some() {}
    while control_jobs.join_next().await.is_some() {}
    while background_jobs.join_next().await.is_some() {}
    while mailbox_jobs.join_next().await.is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixStream;

    struct TestDirectory(PathBuf);
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    async fn test_node(peer: PublicKey, via: SocketAddr) -> (TestDirectory, Node) {
        let directory = TestDirectory(PathBuf::from(format!("/tmp/pw-test-{}", Uuid::new_v4())));
        storage::private_directory(&directory.0).unwrap();
        let identity = directory.0.join("identity.json");
        storage::save_identity(&identity, &Identity::generate().unwrap()).unwrap();
        let config = Config {
            name: "test".into(),
            identity,
            control_socket: directory.0.join("control.sock"),
            listen: default_listen(),
            relay: true,
            contacts: vec![peer],
            routes: vec![Route {
                destination: peer.node_id(),
                via,
            }],
            links: Vec::new(),
            discovery: false,
            powered: true,
            mailbox: None,
            pickup: None,
            mailbox_route: None,
            mailbox_route_contacts: Vec::new(),
        };
        fs::write(
            directory.0.join("config.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
        let node = Node::start(config, Arc::new(|_| {})).await.unwrap();
        (directory, node)
    }

    async fn wait_for_outbox(node: &Node, id: Uuid, state: &str) {
        timeout(Duration::from_secs(5), async {
            loop {
                let list = node.state.outbox.lock().unwrap().list();
                if list["entries"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| e["message_id"] == id.to_string() && e["state"] == state)
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "outbox did not become {state}: {}",
                node.state.outbox.lock().unwrap().list()
            )
        });
    }

    async fn queue_text(node: &Node, to: NodeId) -> Uuid {
        let response = node
            .request(Request::Queue {
                to,
                message: "durable private 🌍".into(),
                ttl_ms: 60_000,
            })
            .await;
        assert!(response.ok, "{:?}", response.error);
        response.result.unwrap()["message_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    }

    #[tokio::test]
    async fn contacts_can_be_added_live_without_replacing_or_self_pinning() {
        let peer = Identity::generate().unwrap();
        let newcomer = Identity::generate().unwrap();
        let reservation = TcpListener::bind(default_listen()).await.unwrap();
        let (_directory, node) =
            test_node(peer.public_key(), reservation.local_addr().unwrap()).await;
        let queue = |to| Request::Queue {
            to,
            message: "hello".into(),
            ttl_ms: 60_000,
        };
        assert!(
            !node.request(queue(newcomer.id())).await.ok,
            "unknown recipient is refused"
        );
        let own = node.state.identity.public_key();
        assert!(
            !node
                .request(Request::AddContact { public_key: own })
                .await
                .ok
        );
        let added = node
            .request(Request::AddContact {
                public_key: newcomer.public_key(),
            })
            .await;
        assert_eq!(added.result.unwrap()["added"], true);
        let again = node
            .request(Request::AddContact {
                public_key: newcomer.public_key(),
            })
            .await;
        assert_eq!(
            again.result.unwrap()["added"],
            false,
            "adding twice is harmless"
        );
        assert_eq!(
            node.state.contact(&peer.id()),
            Some(peer.public_key()),
            "existing contact untouched"
        );
        assert!(
            node.request(queue(newcomer.id())).await.ok,
            "new contact usable without a restart"
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn queued_message_survives_restart_and_a_changed_manual_route() {
        let peer = Identity::generate().unwrap();
        let reservation = TcpListener::bind(default_listen()).await.unwrap();
        let via = reservation.local_addr().unwrap();
        let (directory, node) = test_node(peer.public_key(), via).await;
        drop(reservation);
        let id = queue_text(&node, peer.id()).await;
        let owner_key = node.state.identity.public_key();
        let persisted = Outbox::path_for(&directory.0.join("control.sock")).unwrap();
        assert!(
            !fs::read_to_string(&persisted)
                .unwrap()
                .contains("durable private")
        );
        node.shutdown().await.unwrap();
        let stale =
            std::os::unix::net::UnixListener::bind(directory.0.join("control.sock")).unwrap();
        fs::set_permissions(
            directory.0.join("control.sock"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        drop(stale);
        let restarted = Node::start(
            Config::read(&directory.0.join("config.json")).unwrap(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        let recipient = TestDirectory(PathBuf::from(format!(
            "/tmp/pw-recipient-{}",
            Uuid::new_v4()
        )));
        storage::private_directory(&recipient.0).unwrap();
        storage::save_identity(&recipient.0.join("identity.json"), &peer).unwrap();
        let receiving = Node::start(
            Config {
                name: "recipient".into(),
                identity: recipient.0.join("identity.json"),
                control_socket: recipient.0.join("control.sock"),
                listen: default_listen(),
                relay: false,
                contacts: vec![owner_key],
                routes: vec![Route {
                    destination: owner_key.node_id(),
                    via: restarted.state.listen,
                }],
                links: Vec::new(),
                discovery: false,
                powered: true,
                mailbox: None,
                pickup: None,
                mailbox_route: None,
                mailbox_route_contacts: Vec::new(),
            },
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        assert!(
            restarted
                .request(Request::Route {
                    to: peer.id(),
                    via: receiving.state.listen
                })
                .await
                .ok
        );
        wait_for_outbox(&restarted, id, "delivered").await;
        assert_eq!(receiving.status()["delivered_texts"], 1);
        assert_eq!(restarted.status()["receipts"], 1);
        restarted.shutdown().await.unwrap();
        let again = Node::start(
            Config::read(&directory.0.join("config.json")).unwrap(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        wait_for_outbox(&again, id, "delivered").await;
        sleep(Duration::from_millis(300)).await;
        assert_eq!(receiving.status()["delivered_texts"], 1);
        again.shutdown().await.unwrap();
        receiving.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_queue_entry_never_reaches_a_later_listener() {
        let peer = Identity::generate().unwrap();
        let reservation = TcpListener::bind(default_listen()).await.unwrap();
        let via = reservation.local_addr().unwrap();
        let (_directory, node) = test_node(peer.public_key(), via).await;
        drop(reservation);
        let id = queue_text(&node, peer.id()).await;
        assert!(node.request(Request::Cancel { message_id: id }).await.ok);
        let listener = TcpListener::bind(via).await.unwrap();
        assert!(
            timeout(Duration::from_millis(1200), listener.accept())
                .await
                .is_err()
        );
        wait_for_outbox(&node, id, "cancelled").await;
        assert!(node.request(Request::Forget { message_id: id }).await.ok);
        assert!(
            node.state.outbox.lock().unwrap().list()["entries"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn late_receipt_after_restart_prevents_redelivery_and_is_persisted() {
        let peer = Identity::generate().unwrap();
        let listener = TcpListener::bind(default_listen()).await.unwrap();
        let (directory, node) = test_node(peer.public_key(), listener.local_addr().unwrap()).await;
        let id = queue_text(&node, peer.id()).await;
        let (mut stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let packet: Envelope = timeout(IO_TIMEOUT, wire::read(&mut stream))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet.id, id);
        assert!(peer.open(node.state.identity.public_key(), &packet).is_ok());
        wait_for_outbox(&node, id, "in_flight").await;
        let owner_key = node.state.identity.public_key();
        node.shutdown().await.unwrap();
        let restarted = Node::start(
            Config::read(&directory.0.join("config.json")).unwrap(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        wait_for_outbox(&restarted, id, "unconfirmed").await;
        let (mut query_stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let query: Envelope = timeout(IO_TIMEOUT, wire::read(&mut query_stream))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(query.id, id);
        assert_eq!(
            peer.open(owner_key, &query).unwrap(),
            Payload::ReceiptRequest { message_id: id }
        );
        let receipt = peer
            .seal(owner_key, Payload::Receipt { message_id: id })
            .unwrap();
        let mut incoming = TcpStream::connect(restarted.state.listen).await.unwrap();
        wire::write(&mut incoming, &receipt).await.unwrap();
        wait_for_outbox(&restarted, id, "delivered").await;
        assert_eq!(restarted.status()["receipts"], 1);
        assert_eq!(
            restarted.state.outbox.lock().unwrap().list()["entries"][0]["redeliveries"],
            0
        );
        restarted.shutdown().await.unwrap();
        let history = Outbox::open(
            Outbox::path_for(&directory.0.join("control.sock")).unwrap(),
            owner_key.node_id(),
            outbox::now_ms().unwrap(),
        )
        .unwrap();
        assert_eq!(history.list()["entries"][0]["state"], "delivered");
    }

    #[tokio::test]
    async fn unanswered_receipt_query_leads_to_a_fresh_redelivery_envelope() {
        let peer = Identity::generate().unwrap();
        let listener = TcpListener::bind(default_listen()).await.unwrap();
        let (directory, node) = test_node(peer.public_key(), listener.local_addr().unwrap()).await;
        let id = queue_text(&node, peer.id()).await;
        let (mut original_stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let original: Envelope = timeout(IO_TIMEOUT, wire::read(&mut original_stream))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.id, id);
        wait_for_outbox(&node, id, "in_flight").await;
        let owner_key = node.state.identity.public_key();
        node.shutdown().await.unwrap();
        let restarted = Node::start(
            Config::read(&directory.0.join("config.json")).unwrap(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        wait_for_outbox(&restarted, id, "unconfirmed").await;
        let (mut query_stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let query: Envelope = timeout(IO_TIMEOUT, wire::read(&mut query_stream))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            peer.open(owner_key, &query).unwrap(),
            Payload::ReceiptRequest { message_id: id }
        );
        let (mut retry_stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let retry: Envelope = timeout(IO_TIMEOUT, wire::read(&mut retry_stream))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(retry.id, id);
        let Payload::Redelivery { packet } = peer.open(owner_key, &retry).unwrap() else {
            panic!("wrong payload")
        };
        assert_eq!(packet.id, original.id);
        assert_eq!(packet.source, original.source);
        assert_eq!(packet.destination, original.destination);
        assert_eq!(packet.nonce, original.nonce);
        assert_eq!(packet.ciphertext, original.ciphertext);
        assert_eq!(packet.remaining_hops, pathweft_core::DEFAULT_HOPS);
        let receipt = peer
            .seal(owner_key, Payload::Receipt { message_id: id })
            .unwrap();
        let mut incoming = TcpStream::connect(restarted.state.listen).await.unwrap();
        wire::write(&mut incoming, &receipt).await.unwrap();
        wait_for_outbox(&restarted, id, "delivered").await;
        let entry = restarted.state.outbox.lock().unwrap().list()["entries"][0].clone();
        assert_eq!(entry["receipt_queries"], 1);
        assert_eq!(entry["redeliveries"], 1);
        assert_eq!(entry["attempts"], 1);
        restarted.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn redelivery_requires_matching_endpoints_and_authenticated_text() {
        let peer = Identity::generate().unwrap();
        let other = Identity::generate().unwrap();
        let reservation = TcpListener::bind(default_listen()).await.unwrap();
        let (_directory, node) =
            test_node(peer.public_key(), reservation.local_addr().unwrap()).await;
        drop(reservation);
        let wrong_recipient = peer
            .seal(
                other.public_key(),
                Payload::Text {
                    text: "must not enter the inbox".into(),
                },
            )
            .unwrap();
        let outer = peer
            .seal(
                node.state.identity.public_key(),
                Payload::Redelivery {
                    packet: wrong_recipient,
                },
            )
            .unwrap();
        assert!(node.state.received(outer).await.is_err());
        let receipt = peer
            .seal(
                node.state.identity.public_key(),
                Payload::Receipt {
                    message_id: Uuid::new_v4(),
                },
            )
            .unwrap();
        let outer = peer
            .seal(
                node.state.identity.public_key(),
                Payload::Redelivery { packet: receipt },
            )
            .unwrap();
        assert!(node.state.received(outer).await.is_err());
        assert_eq!(node.state.inbox.lock().unwrap().summary()["stored"], 0);
        assert_eq!(node.state.status()["delivered_texts"], 0);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn second_node_cannot_touch_the_active_socket_or_outbox() {
        let peer = Identity::generate().unwrap();
        let reservation = TcpListener::bind(default_listen()).await.unwrap();
        let (directory, node) =
            test_node(peer.public_key(), reservation.local_addr().unwrap()).await;
        drop(reservation);
        let id = queue_text(&node, peer.id()).await;
        assert!(node.request(Request::Cancel { message_id: id }).await.ok);
        let control = directory.0.join("control.sock");
        let inode = fs::symlink_metadata(&control).unwrap().ino();
        let persisted = Outbox::path_for(&control).unwrap();
        let bytes = fs::read(&persisted).unwrap();
        assert!(
            Node::start(
                Config::read(&directory.0.join("config.json")).unwrap(),
                Arc::new(|_| {})
            )
            .await
            .is_err()
        );
        assert_eq!(fs::symlink_metadata(&control).unwrap().ino(), inode);
        assert_eq!(fs::read(&persisted).unwrap(), bytes);
        assert!(node.request(Request::Status).await.ok);
        node.shutdown().await.unwrap();
    }

    async fn wait_for_connect_retry(node: &Node) {
        timeout(IO_TIMEOUT, async {
            while node.state.counters.connect_retries.load(Ordering::Relaxed) == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("an unavailable next hop should be retried");
    }

    #[tokio::test]
    async fn send_recovers_when_next_hop_returns_and_requires_authenticated_receipt() {
        let peer = Identity::generate().unwrap();
        let reservation = TcpListener::bind(default_listen()).await.unwrap();
        let via = reservation.local_addr().unwrap();
        let (_directory, node) = test_node(peer.public_key(), via).await;
        drop(reservation);
        let state = node.state.clone();
        let to = peer.id();
        let message = "🌍".repeat(128); // Maximum 512 UTF-8 bytes.
        let expected = message.clone();
        let sending = tokio::spawn(async move { state.send(to, message, 2000).await });
        wait_for_connect_retry(&node).await;
        let listener = TcpListener::bind(via).await.unwrap();
        let (mut stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let packet: Envelope = timeout(IO_TIMEOUT, wire::read(&mut stream))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            peer.open(node.state.identity.public_key(), &packet)
                .unwrap(),
            Payload::Text { text: expected }
        );
        assert!(!sending.is_finished(), "a successful write is not delivery");
        let receipt = peer
            .seal(
                node.state.identity.public_key(),
                Payload::Receipt {
                    message_id: packet.id,
                },
            )
            .unwrap();
        let mut incoming = TcpStream::connect(node.state.listen).await.unwrap();
        wire::write(&mut incoming, &receipt).await.unwrap();
        let result = sending.await.unwrap().unwrap();
        assert_eq!(result["status"], "delivered");
        assert_eq!(result["message_id"], packet.id.to_string());
        assert_eq!(result["receipt_id"], receipt.id.to_string());
        assert_eq!(node.status()["pending"], 0);
        assert_eq!(node.status()["receipts"], 1);
        assert!(
            timeout(CONNECT_RETRY_DELAY * 2, listener.accept())
                .await
                .is_err()
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn connection_retry_expires_with_the_send_and_never_sends_later() {
        let peer = Identity::generate().unwrap();
        let reservation = TcpListener::bind(default_listen()).await.unwrap();
        let via = reservation.local_addr().unwrap();
        let (_directory, node) = test_node(peer.public_key(), via).await;
        drop(reservation);
        let started = Instant::now();
        assert!(
            node.state
                .send(peer.id(), "expire".into(), 350)
                .await
                .is_err()
        );
        assert!(started.elapsed() >= CONNECT_RETRY_DELAY);
        assert!(started.elapsed() < IO_TIMEOUT);
        assert_eq!(node.status()["pending"], 0);
        let retries = node.status()["connect_retries"].clone();
        assert!(retries.as_u64().unwrap() > 0);
        let listener = TcpListener::bind(via).await.unwrap();
        assert!(
            timeout(CONNECT_RETRY_DELAY * 2, listener.accept())
                .await
                .is_err()
        );
        assert_eq!(node.status()["connect_retries"], retries);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_while_connecting_releases_pending_and_stops_retries() {
        let peer = Identity::generate().unwrap();
        let reservation = TcpListener::bind(default_listen()).await.unwrap();
        let via = reservation.local_addr().unwrap();
        let (_directory, node) = test_node(peer.public_key(), via).await;
        drop(reservation);
        let state = node.state.clone();
        let sending =
            tokio::spawn(async move { state.send(peer.id(), "cancel".into(), 2000).await });
        wait_for_connect_retry(&node).await;
        sending.abort();
        assert!(sending.await.unwrap_err().is_cancelled());
        assert_eq!(node.status()["pending"], 0);
        let retries = node.status()["connect_retries"].clone();
        let listener = TcpListener::bind(via).await.unwrap();
        assert!(
            timeout(CONNECT_RETRY_DELAY * 2, listener.accept())
                .await
                .is_err()
        );
        assert_eq!(node.status()["connect_retries"], retries);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn missing_receipt_does_not_resend_after_a_successful_write() {
        let peer = Identity::generate().unwrap();
        let listener = TcpListener::bind(default_listen()).await.unwrap();
        let (_directory, node) = test_node(peer.public_key(), listener.local_addr().unwrap()).await;
        let state = node.state.clone();
        let sending = tokio::spawn(async move { state.send(peer.id(), "once".into(), 350).await });
        let (mut stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let _: Envelope = timeout(IO_TIMEOUT, wire::read(&mut stream))
            .await
            .unwrap()
            .unwrap();
        assert!(sending.await.unwrap().is_err());
        assert_eq!(node.status()["pending"], 0);
        assert_eq!(node.status()["receipts"], 0);
        assert_eq!(node.status()["connect_retries"], 0);
        assert!(
            timeout(CONNECT_RETRY_DELAY * 2, listener.accept())
                .await
                .is_err()
        );
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_send_releases_its_pending_entry() {
        let peer = Identity::generate().unwrap();
        let listener = TcpListener::bind(default_listen()).await.unwrap();
        let (_directory, node) = test_node(peer.public_key(), listener.local_addr().unwrap()).await;
        let state = node.state.clone();
        let sending =
            tokio::spawn(async move { state.send(peer.id(), "cancel me".into(), 10_000).await });
        let (mut stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let _: Envelope = wire::read(&mut stream).await.unwrap();
        assert_eq!(node.status()["pending"], 1);
        sending.abort();
        assert!(sending.await.unwrap_err().is_cancelled());
        assert_eq!(node.status()["pending"], 0);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn full_control_send_pool_still_accepts_network_receipts() {
        let peer = Identity::generate().unwrap();
        let listener = TcpListener::bind(default_listen()).await.unwrap();
        let (directory, node) = test_node(peer.public_key(), listener.local_addr().unwrap()).await;
        let mut clients = JoinSet::new();
        for _ in 0..MAX_CONTROL_HANDLERS {
            let socket = directory.0.join("control.sock");
            let to = peer.id();
            clients.spawn(async move {
                let mut stream = UnixStream::connect(socket).await.unwrap();
                wire::write(
                    &mut stream,
                    &Request::Send {
                        to,
                        message: "concurrent delivery".into(),
                        timeout_ms: 10_000,
                    },
                )
                .await
                .unwrap();
                wire::read::<Response, _>(&mut stream).await.unwrap()
            });
        }
        // Hold every receipt until all control sends are waiting for delivery.
        let mut packets = Vec::new();
        for _ in 0..MAX_CONTROL_HANDLERS {
            let (mut stream, _) = timeout(IO_TIMEOUT, listener.accept())
                .await
                .unwrap()
                .unwrap();
            packets.push(wire::read::<Envelope, _>(&mut stream).await.unwrap());
        }
        assert_eq!(node.status()["pending"], MAX_CONTROL_HANDLERS);
        for packet in packets {
            let receipt = peer
                .seal(
                    node.state.identity.public_key(),
                    Payload::Receipt {
                        message_id: packet.id,
                    },
                )
                .unwrap();
            let mut stream = TcpStream::connect(node.state.listen).await.unwrap();
            wire::write(&mut stream, &receipt).await.unwrap();
        }
        timeout(Duration::from_secs(4), async {
            while let Some(reply) = clients.join_next().await {
                let reply = reply.unwrap();
                assert!(reply.ok, "{:?}", reply.error);
            }
        })
        .await
        .expect("receipts must complete even when every control slot is busy");
        assert_eq!(node.status()["pending"], 0);
        assert_eq!(node.status()["receipts"], MAX_CONTROL_HANDLERS);
        node.shutdown().await.unwrap();
        assert!(!directory.0.join("control.sock").exists());
    }

    #[tokio::test]
    async fn shutdown_cancels_in_flight_sends_and_removes_control_socket() {
        let peer = Identity::generate().unwrap();
        let listener = TcpListener::bind(default_listen()).await.unwrap();
        let (directory, node) = test_node(peer.public_key(), listener.local_addr().unwrap()).await;
        let mut control = UnixStream::connect(directory.0.join("control.sock"))
            .await
            .unwrap();
        wire::write(
            &mut control,
            &Request::Send {
                to: peer.id(),
                message: "stop while pending".into(),
                timeout_ms: 10_000,
            },
        )
        .await
        .unwrap();
        let (mut stream, _) = timeout(IO_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let _: Envelope = wire::read(&mut stream).await.unwrap();
        let state = node.state.clone();
        timeout(IO_TIMEOUT, node.shutdown()).await.unwrap().unwrap();
        assert!(wire::read::<Response, _>(&mut control).await.is_err());
        assert!(state.pending.lock().unwrap().is_empty());
        assert!(!directory.0.join("control.sock").exists());
    }

    #[test]
    fn receipt_must_come_from_the_intended_pinned_recipient() {
        let a = Identity::generate().unwrap();
        let b = Identity::generate().unwrap();
        let c = Identity::generate().unwrap();
        let config = Config {
            name: "A".into(),
            identity: "a".into(),
            control_socket: "a.sock".into(),
            listen: default_listen(),
            relay: true,
            contacts: vec![b.public_key(), c.public_key()],
            routes: vec![],
            links: vec![],
            discovery: false,
            powered: true,
            mailbox: None,
            pickup: None,
            mailbox_route: None,
            mailbox_route_contacts: Vec::new(),
        };
        let directory =
            TestDirectory(std::env::temp_dir().join(format!("pw-receipt-{}", Uuid::new_v4())));
        let outbox = Outbox::open(
            directory.0.join("outbox.json"),
            a.id(),
            outbox::now_ms().unwrap(),
        )
        .unwrap();
        let inbox = Inbox::open(directory.0.join("inbox.json"), &a).unwrap();
        let state = State::new(
            &config,
            a,
            default_listen(),
            Arc::new(|_| {}),
            outbox,
            inbox,
            mailbox::routed::Network::new().0,
        )
        .unwrap();
        let message_id = Uuid::new_v4();
        let receipt_id = Uuid::new_v4();
        let (reply, mut receive) = oneshot::channel();
        state.pending.lock().unwrap().insert(
            message_id,
            Pending {
                recipient: c.id(),
                reply,
            },
        );
        state
            .accept_receipt(message_id, b.id(), Uuid::new_v4())
            .unwrap();
        assert!(matches!(
            receive.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        state
            .accept_receipt(message_id, c.id(), receipt_id)
            .unwrap();
        assert_eq!(receive.try_recv().unwrap(), receipt_id);
    }

    // ----- discovery integration -----

    type Events = Arc<Mutex<Vec<Value>>>;

    struct Peer {
        dir: TestDirectory,
        name: String,
        addr: SocketAddr,
        key: PublicKey,
        id: NodeId,
        events: Events,
        node: Option<Node>,
    }

    impl Peer {
        fn new(name: &str) -> Self {
            let dir = TestDirectory(PathBuf::from(format!("/tmp/pw-test-{}", Uuid::new_v4())));
            storage::private_directory(&dir.0).unwrap();
            let identity = Identity::generate().unwrap();
            storage::save_identity(&dir.0.join("identity.json"), &identity).unwrap();
            // Reserve a real loopback port, then release it for the node to bind.
            let addr = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap();
            Self {
                dir,
                name: name.into(),
                addr,
                key: identity.public_key(),
                id: identity.id(),
                events: Arc::default(),
                node: None,
            }
        }

        fn counters_path(&self) -> PathBuf {
            self.dir.0.join("control.sock.discovery.json")
        }

        async fn start(&mut self, contacts: &[&Peer], links: &[&Peer]) {
            self.start_with(contacts, links, false).await;
        }

        async fn start_with(&mut self, contacts: &[&Peer], links: &[&Peer], discovery: bool) {
            let config = Config {
                name: self.name.clone(),
                identity: self.dir.0.join("identity.json"),
                control_socket: self.dir.0.join("control.sock"),
                listen: self.addr,
                relay: true,
                contacts: contacts.iter().map(|p| p.key).collect(),
                routes: Vec::new(),
                links: links
                    .iter()
                    .map(|p| DiscoveryLink {
                        address: p.addr,
                        kind: DiscoveryLinkKind::Lan,
                    })
                    .collect(),
                discovery,
                powered: true,
                mailbox: None,
                pickup: None,
                mailbox_route: None,
                mailbox_route_contacts: Vec::new(),
            };
            let events = self.events.clone();
            self.node = Some(
                Node::start(config, Arc::new(move |v| events.lock().unwrap().push(v)))
                    .await
                    .unwrap(),
            );
        }

        async fn stop(&mut self) {
            self.node.take().unwrap().shutdown().await.unwrap();
        }

        fn node(&self) -> &Node {
            self.node.as_ref().unwrap()
        }

        async fn wait_bound(&self, neighbours: usize) {
            let node = self.node();
            timeout(Duration::from_secs(8), async {
                while node.status()["discovery"]["neighbours"].as_u64().unwrap() < neighbours as u64
                {
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{} never bound {neighbours} neighbours", self.name));
            // Let the other side finish its half of the hello exchange.
            sleep(Duration::from_millis(250)).await;
        }

        async fn send(&self, to: &Peer, text: &str, timeout_ms: u64) -> Response {
            self.node()
                .request(Request::Send {
                    to: to.id,
                    message: text.into(),
                    timeout_ms,
                })
                .await
        }

        fn events_named(&self, name: &str) -> Vec<Value> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e["event"] == name)
                .cloned()
                .collect()
        }

        fn counters(&self) -> CounterState {
            let rt = self.node().state.discovery.as_ref().unwrap();
            rt.engine.lock().unwrap().counters()
        }
    }

    /// A -- B -- C with zero routes. A and C pin each other; B is a pure relay.
    /// B starts with A, then waits so that its per-link hello reply limit (1 s)
    /// never delays C's introduction.
    async fn line() -> (Peer, Peer, Peer) {
        let (mut a, mut b, mut c) = (Peer::new("A"), Peer::new("B"), Peer::new("C"));
        a.start(&[&c], &[&b]).await;
        b.start(&[], &[&a, &c]).await;
        sleep(Duration::from_millis(1100)).await;
        c.start(&[&a], &[&b]).await;
        a.wait_bound(1).await;
        b.wait_bound(2).await;
        c.wait_bound(1).await;
        (a, b, c)
    }

    fn texts(peer: &Peer) -> Vec<String> {
        peer.events_named("text_received")
            .iter()
            .map(|e| e["details"]["text"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn discovery_delivers_both_ways_through_a_relay_without_routes() {
        let (a, b, c) = line().await;
        let secret = "discovered text 🌍 for C only";
        let reply = a.send(&c, secret, 8000).await;
        assert!(reply.ok, "{:?}", reply.error);
        assert_eq!(reply.result.as_ref().unwrap()["status"], "delivered");
        assert_eq!(texts(&c), vec![secret.to_string()], "C receives it once");
        assert_eq!(a.node().status()["routes"], 0, "no manual routes anywhere");

        let back = "reply from C to A";
        let reply = c.send(&a, back, 8000).await;
        assert!(reply.ok, "{:?}", reply.error);
        assert_eq!(texts(&a), vec![back.to_string()]);
        assert_eq!(texts(&c).len(), 1);

        // B relayed hop frames (text and receipt each way) but never saw plaintext.
        let status = b.node().status();
        assert!(status["forwarded"].as_u64().unwrap() >= 4, "{status}");
        assert_eq!(status["delivered_texts"], 0);
        let log = serde_json::to_string(&*b.events.lock().unwrap()).unwrap();
        assert!(!log.contains("discovered text") && !log.contains("reply from C"));
        assert!(b.events_named("text_received").is_empty());
        assert_eq!(status["discovery"]["enabled"], true);
        assert_eq!(a.node().status()["discovery"]["routes"], 1);
        assert!(!a.events_named("route_found").is_empty());

        // The relay learns neither who is talking nor which messages they sent.
        let mut secrets: Vec<String> = Vec::new();
        for peer in [&a, &c] {
            secrets.push(
                serde_json::to_string(&peer.id)
                    .unwrap()
                    .trim_matches('"')
                    .into(),
            );
            secrets.push(
                serde_json::to_string(&peer.key)
                    .unwrap()
                    .trim_matches('"')
                    .into(),
            );
            for event in peer.events.lock().unwrap().iter() {
                collect_uuids(event, &mut secrets);
            }
        }
        assert!(
            secrets.len() >= 4 + 4,
            "expected message and receipt ids from the endpoints: {secrets:?}"
        );
        let relay_view = format!(
            "{log}{}",
            serde_json::to_string(&b.node().status()).unwrap()
        );
        for secret in &secrets {
            assert!(!relay_view.contains(secret), "relay saw {secret}");
        }
        assert!(
            !b.events_named("forwarded").is_empty(),
            "relay must still record forwarding"
        );
    }

    fn collect_uuids(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::String(text) if Uuid::parse_str(text).is_ok() => out.push(text.clone()),
            Value::Array(items) => items.iter().for_each(|v| collect_uuids(v, out)),
            Value::Object(map) => map.values().for_each(|v| collect_uuids(v, out)),
            _ => {}
        }
    }

    #[tokio::test]
    async fn discovery_counters_persist_across_restart_and_stale_counters_get_silence() {
        let (mut a, b, mut c) = line().await;
        assert!(a.send(&c, "first", 8000).await.ok);
        let file: Value = serde_json::from_slice(&fs::read(a.counters_path()).unwrap()).unwrap();
        assert_eq!(file["owner"], a.id.to_string());
        assert_eq!(file["counters"]["outgoing"][c.id.to_string()], 1);
        assert_eq!(
            fs::metadata(a.counters_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(c.counters().highest_seen[&a.id], 1);

        // Restart A and C (B stays). Counters come back from disk.
        a.stop().await;
        c.stop().await;
        // B limits hello replies to one per second per link; a newcomer inside that
        // window would wait for B's next 30 s hello (see the final report).
        sleep(Duration::from_millis(1100)).await;
        a.start(&[&c], &[&b]).await;
        a.wait_bound(1).await;
        sleep(Duration::from_millis(1100)).await;
        c.start(&[&a], &[&b]).await;
        c.wait_bound(1).await;
        assert_eq!(a.counters().outgoing[&c.id], 1, "A reloaded its counter");
        assert_eq!(c.counters().highest_seen[&a.id], 1, "C reloaded its record");
        let reply = a.send(&c, "after restart", 8000).await;
        assert!(reply.ok, "{:?}", reply.error);
        assert_eq!(a.counters().outgoing[&c.id], 2);
        assert_eq!(c.counters().highest_seen[&a.id], 2);

        // An A that lost its counter would reuse an old value; C answers with silence.
        a.stop().await;
        fs::remove_file(a.counters_path()).unwrap();
        sleep(Duration::from_millis(1100)).await;
        a.start(&[&c], &[&b]).await;
        a.wait_bound(1).await;
        let reply = a.send(&c, "stale counter", 5000).await;
        assert!(!reply.ok, "a replayed counter must get no route");
        assert_eq!(a.counters().outgoing[&c.id], 1);
        assert_eq!(c.counters().highest_seen[&a.id], 2);
        assert_eq!(texts(&c).len(), 2, "the stale request delivered nothing");
    }

    #[tokio::test]
    async fn queued_message_uses_the_discovered_path() {
        let (a, _b, c) = line().await;
        let id = queue_text(a.node(), c.id).await;
        wait_for_outbox(a.node(), id, "delivered").await;
        assert_eq!(texts(&c), vec!["durable private 🌍".to_string()]);
        let entry = a.node().state.outbox.lock().unwrap().list();
        let attempts = entry["entries"][0]["attempts"].as_u64().unwrap();
        assert!(
            (1..=3).contains(&attempts),
            "discovering is a retry, not a flood: {entry}"
        );
    }

    #[tokio::test]
    async fn live_links_enable_discovery_without_static_links() {
        let (mut a, mut b) = (Peer::new("A"), Peer::new("B"));
        a.start_with(&[&b], &[], true).await;
        b.start_with(&[&a], &[], true).await;
        let status = a.node().status();
        assert_eq!(status["discovery"]["enabled"], true);
        assert_eq!(status["discovery"]["links"], 0);

        let up = |peer: &Peer| Request::LinkUp {
            address: peer.addr,
            kind: DiscoveryLinkKind::Lan,
        };
        assert!(a.node().request(up(&b)).await.ok);
        assert!(b.node().request(up(&a)).await.ok);
        // Repeating an address reuses its link instead of adding another.
        assert!(a.node().request(up(&b)).await.ok);
        assert_eq!(a.node().status()["discovery"]["links"], 1);
        a.wait_bound(1).await;
        b.wait_bound(1).await;

        let reply = a.send(&b, "found it live", 8000).await;
        assert!(reply.ok, "{:?}", reply.error);
        assert_eq!(texts(&b), vec!["found it live".to_string()]);

        let reply = a
            .node()
            .request(Request::LinkDown { address: b.addr })
            .await;
        assert!(reply.ok);
        assert_eq!(a.node().status()["discovery"]["links"], 0);
        assert_eq!(a.node().status()["discovery"]["neighbours"], 0);
        // Removing an unknown address is harmless.
        assert!(
            a.node()
                .request(Request::LinkDown { address: b.addr })
                .await
                .ok
        );

        // Validation: no port zero and at most 16 links.
        let zero = SocketAddr::from(([127, 0, 0, 1], 0));
        let reply = a
            .node()
            .request(Request::LinkUp {
                address: zero,
                kind: DiscoveryLinkKind::Ble,
            })
            .await;
        assert!(!reply.ok && reply.error.unwrap().contains("port"));
        for port in 1..=16u16 {
            let reply = a
                .node()
                .request(Request::LinkUp {
                    address: SocketAddr::from(([127, 0, 0, 1], 40000 + port)),
                    kind: DiscoveryLinkKind::Other,
                })
                .await;
            assert!(reply.ok);
        }
        let reply = a
            .node()
            .request(Request::LinkUp {
                address: SocketAddr::from(([127, 0, 0, 1], 41000)),
                kind: DiscoveryLinkKind::Other,
            })
            .await;
        assert!(!reply.ok && reply.error.unwrap().contains("too many"));
        a.stop().await;
        b.stop().await;
    }

    #[tokio::test]
    async fn live_link_requests_are_refused_when_discovery_is_off() {
        let mut a = Peer::new("A");
        a.start(&[], &[]).await;
        let reply = a
            .node()
            .request(Request::LinkUp {
                address: SocketAddr::from(([127, 0, 0, 1], 9)),
                kind: DiscoveryLinkKind::Lan,
            })
            .await;
        assert!(!reply.ok && reply.error.unwrap().contains("not enabled"));
        let reply = a
            .node()
            .request(Request::LinkDown {
                address: SocketAddr::from(([127, 0, 0, 1], 9)),
            })
            .await;
        assert!(!reply.ok);
        assert!(!a.counters_path().exists());
        a.stop().await;
    }

    #[tokio::test]
    async fn empty_links_means_no_discovery_traffic_or_state() {
        let peer = Identity::generate().unwrap();
        // A sink stands in for the only next hop; it must see exactly the one envelope.
        let sink = TcpListener::bind(default_listen()).await.unwrap();
        let via = sink.local_addr().unwrap();
        let (directory, node) = test_node(peer.public_key(), via).await;
        sleep(Duration::from_millis(700)).await;
        assert!(
            timeout(Duration::from_millis(50), sink.accept())
                .await
                .is_err(),
            "an idle node with no links connects nowhere"
        );
        let id = queue_text(&node, peer.id()).await;
        let (mut stream, _) = timeout(Duration::from_secs(5), sink.accept())
            .await
            .unwrap()
            .unwrap();
        let bytes = wire::read_bytes(&mut stream).await.unwrap();
        assert!(matches!(
            parse_network_frame(&bytes).unwrap(),
            NetworkFrame::Legacy(packet) if packet.id == id
        ));
        sleep(Duration::from_millis(700)).await;
        assert!(
            timeout(Duration::from_millis(50), sink.accept())
                .await
                .is_err(),
            "no further connections"
        );
        assert!(!directory.0.join("control.sock.discovery.json").exists());
        let status = node.status();
        assert_eq!(status["discovery"]["enabled"], false);

        // A discovery frame is counted as rejected, exactly as old builds do.
        let hello = Frame::Hello {
            link: pathweft_core::discovery::Code([7; 16]),
            heard: Vec::new(),
            powered: true,
        };
        let mut stream = TcpStream::connect(node.state.listen).await.unwrap();
        wire::write_bytes(&mut stream, &hello.to_bytes().unwrap())
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            while node.status()["rejected"] == 0 {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(!directory.0.join("control.sock.discovery.json").exists());
    }

    #[test]
    fn discovery_config_defaults_and_validation() {
        let minimal: Config =
            serde_json::from_str(r#"{"name":"n","identity":"i","control_socket":"c"}"#).unwrap();
        assert!(minimal.links.is_empty() && minimal.powered);
        let link = |port: u16| DiscoveryLink {
            address: SocketAddr::from(([127, 0, 0, 1], port)),
            kind: DiscoveryLinkKind::Ble,
        };
        let with = |links: Vec<DiscoveryLink>| {
            let mut config = minimal_config();
            config.links = links;
            config.validate(Identity::generate().unwrap().id())
        };
        assert!(with(vec![link(4000), link(4001)]).is_ok());
        assert!(with(vec![link(0)]).is_err());
        assert!(with(vec![link(4000), link(4000)]).is_err());
        assert!(with((1..=17).map(|p| link(4000 + p)).collect()).is_err());
        assert!(with((1..=16).map(|p| link(4000 + p)).collect()).is_ok());
        let parsed: Config = serde_json::from_str(
            r#"{"name":"n","identity":"i","control_socket":"c","powered":false,
                "links":[{"address":"127.0.0.1:9","kind":"lan"},{"address":"127.0.0.1:10","kind":"other"}]}"#,
        )
        .unwrap();
        assert!(!parsed.powered && parsed.links.len() == 2);
        assert!(
            serde_json::from_str::<Config>(
                r#"{"name":"n","identity":"i","control_socket":"c","links":[{"address":"127.0.0.1:9","kind":"wifi"}]}"#
            )
            .is_err()
        );
    }

    fn minimal_config() -> Config {
        serde_json::from_str(r#"{"name":"n","identity":"i","control_socket":"c"}"#).unwrap()
    }
}
