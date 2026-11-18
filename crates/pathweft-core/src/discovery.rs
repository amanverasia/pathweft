//! Deterministic route-discovery state machine (see `docs/discovery-spec.md`).
//!
//! No sockets, threads or clocks: the node feeds [`Input`]s with an explicit
//! millisecond timestamp and performs the [`Output`]s it gets back.
use crate::{Envelope, Error, Identity, NodeId, PublicKey, crypto};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;

pub const MAX_FRAME_BYTES: usize = 32 * 1024;
pub const MAX_CONTROL_FRAME_BYTES: usize = 2048;
pub const HELLO_INTERVAL_MS: u64 = 30_000;
pub const BINDING_TTL_MS: u64 = 90_000;
pub const MAX_HEARD: usize = 16;
pub const MAX_BINDINGS: usize = 64;
pub const MAX_HOPS_LEFT: u8 = 8;
pub const REQUEST_CACHE_CAPACITY: usize = 1024;
pub const REQUEST_CACHE_TTL_MS: u64 = 60_000;
pub const MAX_FORWARDED_COPIES: usize = 3;
pub const TARGET_WAIT_MS: u64 = 300;
pub const MAX_LABELS: usize = 256;
pub const MAX_LABELS_PER_REQ: u8 = 3;
pub const MAX_LABELS_PER_NEIGHBOUR: usize = 32;
pub const LABEL_IDLE_MS: u64 = 600_000;
pub const REQUEST_RATE_PER_SEC: u64 = 10;
pub const REQUEST_BURST: u64 = 20;
pub const DISCOVERY_TIMEOUT_MS: u64 = 3_000;
pub const BACKOFF_MS: [u64; 6] = [1_000, 2_000, 4_000, 8_000, 16_000, 30_000];
pub const UNPOWERED_RELAY_COST: u32 = 2;
const MAX_SEALED_BYTES: usize = 512;
/// Purpose string that starts the plaintext of every sealed hop.
pub const HOP_PURPOSE: &[u8] = b"pathweft/hop/v1";
/// Largest accepted sealed hop. This covers ordinary envelopes and mailbox
/// datagrams while leaving room for base64 and metadata in a 32 KiB frame.
pub const MAX_SEALED_HOP_BYTES: usize = 24_000;
const DATAGRAM_PURPOSE: &[u8] = b"pathweft/discovery/datagram/v1";
const MAX_PROOF_BYTES: usize = 256;
const SWEEP_INTERVAL_MS: u64 = 1_000;

/// Local handle for one listed discovery link, chosen by the node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LinkId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkKind {
    Lan,
    Ble,
    Other,
}
impl LinkKind {
    pub fn cost(self) -> u32 {
        match self {
            Self::Lan => 1,
            Self::Ble | Self::Other => 4,
        }
    }
}

/// 16 random bytes (link codes, request ids, labels, tags), hex on the wire.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Code(pub [u8; 16]);
impl fmt::Debug for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Code({})", hex::encode(self.0))
    }
}
impl Serialize for Code {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(self.0))
    }
}
impl<'de> Deserialize<'de> for Code {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        let bytes = hex::decode(&text).map_err(de::Error::custom)?;
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| de::Error::custom("expected 16 bytes"))?;
        Ok(Self(bytes))
    }
}

/// Standard base64 with padding, strict on decode. Written here because the
/// sealed hop must be compact enough for the 32 KiB frame bound (hex doubles it).
mod b64vec {
    use super::*;
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(v: &[u8]) -> String {
        let mut out = String::with_capacity(v.len().div_ceil(3) * 4);
        for chunk in v.chunks(3) {
            let n = (u32::from(chunk[0]) << 16)
                | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
                | u32::from(*chunk.get(2).unwrap_or(&0));
            for i in 0..4 {
                if i <= chunk.len() {
                    out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
                } else {
                    out.push('=');
                }
            }
        }
        out
    }

    pub fn decode(text: &str) -> Result<Vec<u8>, &'static str> {
        let bytes = text.as_bytes();
        if !bytes.len().is_multiple_of(4) {
            return Err("invalid base64 length");
        }
        let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
        for (index, quad) in bytes.chunks(4).enumerate() {
            let last = (index + 1) * 4 == bytes.len();
            let pad = quad.iter().rev().take_while(|b| **b == b'=').count();
            if pad > 2 || (pad > 0 && !last) {
                return Err("invalid base64 padding");
            }
            let mut n = 0_u32;
            for (i, b) in quad.iter().enumerate() {
                let value = if i >= 4 - pad {
                    0
                } else {
                    ALPHABET
                        .iter()
                        .position(|a| a == b)
                        .ok_or("invalid base64 character")? as u32
                };
                n = (n << 6) | value;
            }
            // Unused low bits must be zero, so each value has one encoding.
            if (pad == 1 && n & 0xff != 0) || (pad == 2 && n & 0xffff != 0) {
                return Err("non-canonical base64");
            }
            out.extend_from_slice(&n.to_be_bytes()[1..4 - pad]);
        }
        Ok(out)
    }

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&encode(v))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        decode(&String::deserialize(d)?).map_err(de::Error::custom)
    }
}

mod hexvec {
    use super::*;
    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(v))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        hex::decode(String::deserialize(d)?).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    TowardTarget,
    TowardRequester,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case", deny_unknown_fields)]
pub enum Frame {
    Hello {
        link: Code,
        heard: Vec<Code>,
        powered: bool,
    },
    RouteRequest {
        req: Code,
        link: Code,
        tag: Code,
        #[serde(with = "hexvec")]
        sealed: Vec<u8>,
        hops_left: u8,
        cost: u32,
        powered_path: bool,
    },
    RouteReply {
        req: Code,
        link: Code,
        label: Code,
        cost: u32,
        #[serde(with = "hexvec")]
        proof: Vec<u8>,
    },
    RouteError {
        link: Code,
        label: Code,
    },
    Hop {
        link: Code,
        label: Code,
        dir: Direction,
        /// Present for a datagram, and decremented at every forwarding handoff.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hops_left: Option<u8>,
        /// `crypto_box_seal` to the destination endpoint: `HOP_PURPOSE` then the
        /// envelope's JSON bytes. Relays never open it. Base64 on the wire.
        #[serde(with = "b64vec")]
        sealed: Vec<u8>,
    },
}

impl Frame {
    /// The sender's link code carried by every frame.
    pub fn sender_code(&self) -> Code {
        match self {
            Self::Hello { link, .. }
            | Self::RouteRequest { link, .. }
            | Self::RouteReply { link, .. }
            | Self::RouteError { link, .. }
            | Self::Hop { link, .. } => *link,
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Hello { heard, .. } if heard.len() > MAX_HEARD => {
                Err(Error::InvalidEnvelope("too many heard codes"))
            }
            Self::RouteRequest {
                sealed, hops_left, ..
            } if sealed.is_empty()
                || sealed.len() > MAX_SEALED_BYTES
                || *hops_left > MAX_HOPS_LEFT =>
            {
                Err(Error::InvalidEnvelope("invalid route request"))
            }
            Self::RouteReply { proof, .. } if proof.is_empty() || proof.len() > MAX_PROOF_BYTES => {
                Err(Error::InvalidEnvelope("invalid route reply"))
            }
            Self::Hop {
                sealed, hops_left, ..
            } if sealed.is_empty()
                || sealed.len() > MAX_SEALED_HOP_BYTES
                || hops_left.is_some_and(|left| left >= crate::DEFAULT_HOPS) =>
            {
                Err(Error::InvalidEnvelope("invalid sealed hop"))
            }
            _ => Ok(()),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)?;
        let limit = if matches!(self, Self::Hop { .. }) {
            MAX_FRAME_BYTES
        } else {
            MAX_CONTROL_FRAME_BYTES
        };
        if bytes.len() > limit {
            return Err(Error::InvalidEnvelope("frame too large"));
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NetworkFrame {
    Legacy(Envelope),
    Discovery(Frame),
}

/// Classify wire bytes: a top-level `"frame"` key means a discovery frame,
/// otherwise a legacy bare envelope exactly as today.
pub fn parse_network_frame(bytes: &[u8]) -> Result<NetworkFrame, Error> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(Error::InvalidEnvelope("frame too large"));
    }
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    if value.as_object().is_some_and(|o| o.contains_key("frame")) {
        let frame: Frame = serde_json::from_value(value)?;
        if !matches!(frame, Frame::Hop { .. }) && bytes.len() > MAX_CONTROL_FRAME_BYTES {
            return Err(Error::InvalidEnvelope("frame too large"));
        }
        frame.validate()?;
        Ok(NetworkFrame::Discovery(frame))
    } else {
        Ok(NetworkFrame::Legacy(serde_json::from_value(value)?))
    }
}

/// Injected randomness so tests are deterministic.
pub trait Random: Send {
    fn fill(&mut self, buf: &mut [u8]);
}
/// Production randomness from libsodium.
pub struct SodiumRandom;
impl Random for SodiumRandom {
    fn fill(&mut self, buf: &mut [u8]) {
        crypto::random_fill(buf);
    }
}

/// Persistent per-contact counters, keyed by the contact's node ID.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterState {
    /// Last counter value this node used when asking each contact.
    pub outgoing: BTreeMap<NodeId, u64>,
    /// Highest counter value accepted from each contact.
    pub highest_seen: BTreeMap<NodeId, u64>,
}

#[derive(Clone, Debug)]
pub enum Input {
    LinkUp {
        link: LinkId,
        kind: LinkKind,
    },
    LinkDown {
        link: LinkId,
    },
    /// `link` is the arrival link when the node knows it; `None` for a shared listener.
    Frame {
        link: Option<LinkId>,
        frame: Frame,
    },
    Tick,
    /// Start discovery for a pinned contact.
    Want {
        destination: NodeId,
    },
    /// Wrap an envelope in `hop` along a known path.
    SendEnvelope {
        destination: NodeId,
        envelope: Envelope,
    },
    /// Send application bytes over a discovered path with a handoff limit.
    SendDatagram {
        destination: NodeId,
        bytes: Vec<u8>,
        max_hops: u8,
    },
    /// A previously emitted `Send` could not be written.
    ForwardFailed {
        link: LinkId,
        frame: Frame,
    },
    /// The node suspects the path (e.g. receipt timeout): forget it so `Want` rediscovers.
    Invalidate {
        destination: NodeId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    Send {
        link: LinkId,
        frame: Frame,
    },
    RouteFound {
        destination: NodeId,
    },
    RouteFailed {
        destination: NodeId,
    },
    RouteInvalidated {
        destination: NodeId,
    },
    NoRoute {
        destination: NodeId,
    },
    Deliver {
        envelope: Envelope,
    },
    DeliverDatagram {
        source: NodeId,
        bytes: Vec<u8>,
        hops_used: u8,
    },
    /// Counters changed; persist [`Discovery::counters`] before performing later sends.
    CountersChanged,
}

struct LinkState {
    kind: LinkKind,
    code: Code,
    next_hello: u64,
    last_hello_reply: Option<u64>,
}
struct Binding {
    link: LinkId,
    expires: u64,
    #[allow(dead_code)]
    powered: bool,
}
struct Bucket {
    milli: u64,
    at: u64,
}
struct ReqEntry {
    born: u64,
    own: bool,
    target: bool,
    copies: Vec<(LinkId, u32)>,
    /// Labels installed for this request (relay) so far.
    installs: u8,
}
struct Pending {
    destination: NodeId,
    deadline: u64,
}
struct TargetPending {
    requester: PublicKey,
    in_link: LinkId,
    cost: u32,
    due: u64,
}
enum Role {
    Relay {
        toward_target: LinkId,
        toward_requester: LinkId,
    },
    Endpoint {
        link: LinkId,
        peer: NodeId,
        dir: Direction,
    },
}
struct LabelEntry {
    role: Role,
    last_used: u64,
}
impl LabelEntry {
    fn neighbour(&self) -> LinkId {
        match self.role {
            Role::Relay { toward_target, .. } => toward_target,
            Role::Endpoint { link, .. } => link,
        }
    }
}
#[derive(Default)]
struct Attempt {
    inflight: Option<Code>,
    failures: u8,
    next_ok: u64,
}

pub struct Discovery {
    me: Identity,
    id: NodeId,
    relay: bool,
    powered: bool,
    rng: Box<dyn Random>,
    by_id: HashMap<NodeId, PublicKey>,
    by_key: HashSet<PublicKey>,
    route_identities: HashMap<NodeId, Identity>,
    counters: CounterState,
    links: BTreeMap<LinkId, LinkState>,
    bindings: BTreeMap<Code, Binding>,
    limiters: BTreeMap<LinkId, Bucket>,
    cache: BTreeMap<Code, ReqEntry>,
    pending: BTreeMap<Code, Pending>,
    targets: BTreeMap<Code, TargetPending>,
    labels: BTreeMap<Code, LabelEntry>,
    routes: HashMap<NodeId, Code>,
    attempts: HashMap<NodeId, Attempt>,
    heard: VecDeque<(Code, u64)>,
    next_sweep: u64,
}

fn hash16(parts: &[&[u8]]) -> Code {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p);
    }
    let full = h.finalize();
    let mut out = [0_u8; 16];
    out.copy_from_slice(&full[..16]);
    Code(out)
}
fn echo(heard: &Code, own: &Code) -> Code {
    hash16(&[b"pathweft/heard/v1", &heard.0, &own.0])
}
fn request_tag(target: &PublicKey, req: &Code) -> Code {
    hash16(&[b"pathweft/route-request/v1", &target.0, &req.0])
}
fn auth_plain(req: &Code, counter: u64) -> Vec<u8> {
    let mut v = b"pathweft/route-auth/v1".to_vec();
    v.extend(req.0);
    v.extend(counter.to_be_bytes());
    v
}
fn proof_plain(req: &Code, label: &Code, cost: u32) -> Vec<u8> {
    let mut v = b"pathweft/route-proof/v1".to_vec();
    v.extend(req.0);
    v.extend(label.0);
    v.extend(cost.to_be_bytes());
    v
}

impl Discovery {
    pub fn new(
        identity: &Identity,
        contacts: impl IntoIterator<Item = PublicKey>,
        relay: bool,
        powered: bool,
        counters: CounterState,
        rng: Box<dyn Random>,
    ) -> Result<Self, Error> {
        let me = Identity::from_secret(*identity.secret_bytes())?;
        let mut d = Self {
            id: me.id(),
            me,
            relay,
            powered,
            rng,
            by_id: HashMap::new(),
            by_key: HashSet::new(),
            route_identities: HashMap::new(),
            counters,
            links: BTreeMap::new(),
            bindings: BTreeMap::new(),
            limiters: BTreeMap::new(),
            cache: BTreeMap::new(),
            pending: BTreeMap::new(),
            targets: BTreeMap::new(),
            labels: BTreeMap::new(),
            routes: HashMap::new(),
            attempts: HashMap::new(),
            heard: VecDeque::new(),
            next_sweep: 0,
        };
        for c in contacts {
            d.add_contact(c);
        }
        Ok(d)
    }

    pub fn add_contact(&mut self, key: PublicKey) {
        if key.node_id() != self.id {
            self.by_id.insert(key.node_id(), key);
            self.by_key.insert(key);
        }
    }
    pub fn remove_contact(&mut self, key: PublicKey) {
        self.by_id.remove(&key.node_id());
        self.by_key.remove(&key);
    }
    /// Configure the separate local identity used to open a route to a pinned peer.
    /// Set this before requesting routes. Incoming discovery still uses the node identity.
    pub fn set_route_identity(&mut self, peer: PublicKey, identity: Identity) {
        self.add_contact(peer);
        self.route_identities.insert(peer.node_id(), identity);
    }

    fn route_identity(&self, destination: NodeId) -> &Identity {
        self.route_identities.get(&destination).unwrap_or(&self.me)
    }
    pub fn counters(&self) -> CounterState {
        self.counters.clone()
    }
    /// This node's current link code on a listed link (for diagnostics and tests).
    pub fn link_code(&self, link: LinkId) -> Option<Code> {
        self.links.get(&link).map(|l| l.code)
    }
    pub fn has_route(&self, destination: NodeId) -> bool {
        self.endpoint_route(destination).is_some()
    }
    pub fn label_count(&self) -> usize {
        self.labels.len()
    }
    /// Live bound neighbours (distinct links with an unexpired binding), for status output.
    pub fn neighbour_count(&self, now: u64) -> usize {
        self.bound_links(now).len()
    }
    /// Destinations with a usable discovered path, for status output.
    pub fn route_count(&self) -> usize {
        self.routes
            .keys()
            .filter(|d| self.endpoint_route(**d).is_some())
            .count()
    }
    /// Follow a live relay on/off change; the state machine applies it to forwarding.
    pub fn set_relay(&mut self, enabled: bool) {
        self.relay = enabled;
    }

    pub fn handle(&mut self, now: u64, input: Input) -> Vec<Output> {
        let mut outs = Vec::new();
        self.timers(now, &mut outs);
        match input {
            Input::Tick => {}
            Input::LinkUp { link, kind } => self.link_up(now, link, kind, &mut outs),
            Input::LinkDown { link } => self.link_down(link, &mut outs),
            Input::Frame { link, frame } => self.on_frame(now, link, frame, &mut outs),
            Input::Want { destination } => self.want(now, destination, &mut outs),
            Input::SendEnvelope {
                destination,
                envelope,
            } => self.send_envelope(now, destination, envelope, &mut outs),
            Input::SendDatagram {
                destination,
                bytes,
                max_hops,
            } => self.send_datagram(now, destination, bytes, max_hops, &mut outs),
            Input::ForwardFailed { link, frame } => self.forward_failed(link, frame, &mut outs),
            Input::Invalidate { destination } => {
                if let Some(label) = self.routes.remove(&destination) {
                    self.labels.remove(&label);
                }
                if let Some(a) = self.attempts.get_mut(&destination) {
                    a.failures = 0;
                    a.next_ok = 0;
                }
            }
        }
        outs
    }

    // ---------- helpers ----------

    fn rand16(&mut self) -> Code {
        let mut b = [0_u8; 16];
        self.rng.fill(&mut b);
        Code(b)
    }
    fn rand_below(&mut self, n: u8) -> u8 {
        let mut b = [0_u8; 1];
        self.rng.fill(&mut b);
        b[0] % n
    }
    fn send(&self, outs: &mut Vec<Output>, link: LinkId, build: impl FnOnce(Code) -> Frame) {
        if let Some(l) = self.links.get(&link) {
            outs.push(Output::Send {
                link,
                frame: build(l.code),
            });
        }
    }
    fn resolve(&self, now: u64, code: &Code) -> Option<LinkId> {
        self.bindings
            .get(code)
            .filter(|b| b.expires > now && self.links.contains_key(&b.link))
            .map(|b| b.link)
    }
    fn bound_links(&self, now: u64) -> Vec<LinkId> {
        let set: BTreeSet<LinkId> = self
            .bindings
            .values()
            .filter(|b| b.expires > now && self.links.contains_key(&b.link))
            .map(|b| b.link)
            .collect();
        set.into_iter().collect()
    }
    fn link_cost(&self, link: LinkId) -> u32 {
        self.links.get(&link).map_or(4, |l| l.kind.cost())
    }
    fn can_install(&self, neighbour: LinkId) -> bool {
        self.labels.len() < MAX_LABELS
            && self
                .labels
                .values()
                .filter(|l| l.neighbour() == neighbour)
                .count()
                < MAX_LABELS_PER_NEIGHBOUR
    }
    fn endpoint_route(&self, destination: NodeId) -> Option<(Code, LinkId, Direction)> {
        let label = *self.routes.get(&destination)?;
        match self.labels.get(&label)?.role {
            Role::Endpoint { link, dir, .. } if self.links.contains_key(&link) => {
                Some((label, link, dir))
            }
            _ => None,
        }
    }
    fn allow_request(&mut self, now: u64, link: LinkId) -> bool {
        let b = self.limiters.entry(link).or_insert(Bucket {
            milli: REQUEST_BURST * 1000,
            at: now,
        });
        let elapsed = now.saturating_sub(b.at);
        b.at = now;
        b.milli = (b.milli + elapsed * REQUEST_RATE_PER_SEC).min(REQUEST_BURST * 1000);
        if b.milli >= 1000 {
            b.milli -= 1000;
            true
        } else {
            false
        }
    }
    fn fail_discovery(&mut self, now: u64, dest: NodeId, outs: &mut Vec<Output>) {
        let a = self.attempts.entry(dest).or_default();
        a.inflight = None;
        a.failures = a.failures.saturating_add(1);
        let idx = (usize::from(a.failures) - 1).min(BACKOFF_MS.len() - 1);
        a.next_ok = now + BACKOFF_MS[idx];
        outs.push(Output::RouteFailed { destination: dest });
    }

    // ---------- timers ----------

    fn timers(&mut self, now: u64, outs: &mut Vec<Output>) {
        if now >= self.next_sweep {
            self.next_sweep = now + SWEEP_INTERVAL_MS;
            self.sweep(now, outs);
        }
        let due: Vec<Code> = self
            .targets
            .iter()
            .filter(|(_, t)| t.due <= now)
            .map(|(r, _)| *r)
            .collect();
        for req in due {
            if let Some(t) = self.targets.remove(&req) {
                self.reply_as_target(now, req, t, outs);
            }
        }
        let expired: Vec<Code> = self
            .pending
            .iter()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(r, _)| *r)
            .collect();
        for req in expired {
            if let Some(p) = self.pending.remove(&req) {
                self.fail_discovery(now, p.destination, outs);
            }
        }
        let hello_due: Vec<LinkId> = self
            .links
            .iter()
            .filter(|(_, l)| l.next_hello <= now)
            .map(|(k, _)| *k)
            .collect();
        for link in hello_due {
            self.send_hello(now, link, outs);
            if let Some(l) = self.links.get_mut(&link) {
                l.next_hello = now + HELLO_INTERVAL_MS;
            }
        }
    }

    fn sweep(&mut self, now: u64, outs: &mut Vec<Output>) {
        self.bindings.retain(|_, b| b.expires > now);
        self.cache
            .retain(|_, e| now.saturating_sub(e.born) < REQUEST_CACHE_TTL_MS);
        self.heard
            .retain(|(_, t)| now.saturating_sub(*t) < BINDING_TTL_MS);
        let idle: Vec<Code> = self
            .labels
            .iter()
            .filter(|(_, l)| now.saturating_sub(l.last_used) >= LABEL_IDLE_MS)
            .map(|(c, _)| *c)
            .collect();
        for label in idle {
            if let Some(LabelEntry {
                role: Role::Endpoint { peer, .. },
                ..
            }) = self.labels.remove(&label)
                && self.routes.get(&peer) == Some(&label)
            {
                self.routes.remove(&peer);
                outs.push(Output::RouteInvalidated { destination: peer });
            }
        }
    }

    // ---------- links and hello ----------

    fn link_up(&mut self, now: u64, link: LinkId, kind: LinkKind, outs: &mut Vec<Output>) {
        if self.links.contains_key(&link) {
            self.link_down(link, outs);
        }
        let code = self.rand16();
        self.links.insert(
            link,
            LinkState {
                kind,
                code,
                next_hello: now + HELLO_INTERVAL_MS,
                last_hello_reply: None,
            },
        );
        self.send_hello(now, link, outs);
    }

    fn link_down(&mut self, link: LinkId, outs: &mut Vec<Output>) {
        self.links.remove(&link);
        self.limiters.remove(&link);
        self.bindings.retain(|_, b| b.link != link);
        self.targets.retain(|_, t| t.in_link != link);
        let affected: Vec<Code> = self
            .labels
            .iter()
            .filter(|(_, l)| match l.role {
                Role::Relay {
                    toward_target,
                    toward_requester,
                } => toward_target == link || toward_requester == link,
                Role::Endpoint { link: l, .. } => l == link,
            })
            .map(|(c, _)| *c)
            .collect();
        for label in affected {
            self.drop_label(label, Some(link), outs);
        }
    }

    /// Remove a label; tell the far side (relay) or the node (endpoint).
    /// `failed` is the link that broke, which gets no error frame.
    fn drop_label(&mut self, label: Code, failed: Option<LinkId>, outs: &mut Vec<Output>) {
        let Some(entry) = self.labels.remove(&label) else {
            return;
        };
        match entry.role {
            Role::Relay {
                toward_target,
                toward_requester,
            } => {
                for other in [toward_target, toward_requester] {
                    if Some(other) != failed {
                        self.send(outs, other, |link| Frame::RouteError { link, label });
                    }
                }
            }
            Role::Endpoint { peer, .. } => {
                if self.routes.get(&peer) == Some(&label) {
                    self.routes.remove(&peer);
                    outs.push(Output::RouteInvalidated { destination: peer });
                }
            }
        }
    }

    fn send_hello(&mut self, now: u64, link: LinkId, outs: &mut Vec<Output>) {
        let Some(l) = self.links.get(&link) else {
            return;
        };
        let own = l.code;
        let heard: Vec<Code> = self
            .heard
            .iter()
            .filter(|(_, t)| now.saturating_sub(*t) < BINDING_TTL_MS)
            .map(|(c, _)| echo(c, &own))
            .collect();
        outs.push(Output::Send {
            link,
            frame: Frame::Hello {
                link: own,
                heard,
                powered: self.powered,
            },
        });
    }

    fn on_hello(
        &mut self,
        now: u64,
        code: Code,
        heard: &[Code],
        powered: bool,
        outs: &mut Vec<Output>,
    ) {
        self.heard
            .retain(|(_, t)| now.saturating_sub(*t) < BINDING_TTL_MS);
        let is_new = if let Some(e) = self.heard.iter_mut().find(|(c, _)| *c == code) {
            e.1 = now;
            false
        } else {
            if self.heard.len() >= MAX_HEARD {
                self.heard.pop_front();
            }
            self.heard.push_back((code, now));
            true
        };
        // Bind only where the sender echoed *our* code for that link.
        let matched: Vec<LinkId> = self
            .links
            .iter()
            .filter(|(_, l)| heard.contains(&echo(&l.code, &code)))
            .map(|(k, _)| *k)
            .collect();
        if let Some(&p) = matched.first()
            && (self.bindings.contains_key(&code) || self.bindings.len() < MAX_BINDINGS)
        {
            self.bindings.insert(
                code,
                Binding {
                    link: p,
                    expires: now + BINDING_TTL_MS,
                    powered,
                },
            );
        }
        if is_new {
            let ids: Vec<LinkId> = self.links.keys().copied().collect();
            for link in ids {
                let ok = self.links.get(&link).is_some_and(|l| {
                    l.last_hello_reply
                        .is_none_or(|t| now.saturating_sub(t) >= 1000)
                });
                if ok {
                    if let Some(l) = self.links.get_mut(&link) {
                        l.last_hello_reply = Some(now);
                    }
                    self.send_hello(now, link, outs);
                } else if let Some(l) = self.links.get_mut(&link) {
                    // Rate-limited: owe this link a hello as soon as the limit allows, rather
                    // than leaving a second new neighbour unbound until the 30 s hello.
                    let allowed = l.last_hello_reply.map_or(now, |t| t + 1000);
                    l.next_hello = l.next_hello.min(allowed);
                }
            }
        }
    }

    // ---------- frames ----------

    fn on_frame(&mut self, now: u64, link: Option<LinkId>, frame: Frame, outs: &mut Vec<Output>) {
        if link.is_some_and(|l| !self.links.contains_key(&l)) || frame.validate().is_err() {
            return;
        }
        if let Frame::Hello {
            link: code,
            heard,
            powered,
        } = &frame
        {
            self.on_hello(now, *code, heard, *powered, outs);
            return;
        }
        let Some(from) = self.resolve(now, &frame.sender_code()) else {
            return;
        };
        match frame {
            Frame::Hello { .. } => {}
            Frame::RouteRequest {
                req,
                tag,
                sealed,
                hops_left,
                cost,
                powered_path,
                ..
            } => self.on_request(
                now,
                from,
                req,
                tag,
                sealed,
                hops_left,
                cost,
                powered_path,
                outs,
            ),
            Frame::RouteReply {
                req,
                label,
                cost,
                proof,
                ..
            } => self.on_reply(now, from, req, label, cost, proof, outs),
            Frame::RouteError { label, .. } => self.on_error(from, label, outs),
            Frame::Hop {
                label,
                dir,
                sealed,
                hops_left,
                ..
            } => self.on_hop(now, from, label, dir, sealed, hops_left, outs),
        }
    }

    fn cache_insert(&mut self, now: u64, req: Code, entry: ReqEntry) -> bool {
        if self.cache.len() >= REQUEST_CACHE_CAPACITY {
            self.cache
                .retain(|_, e| now.saturating_sub(e.born) < REQUEST_CACHE_TTL_MS);
            if self.cache.len() >= REQUEST_CACHE_CAPACITY {
                return false;
            }
        }
        self.cache.insert(req, entry);
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn on_request(
        &mut self,
        now: u64,
        from: LinkId,
        req: Code,
        tag: Code,
        sealed: Vec<u8>,
        hops_left: u8,
        cost: u32,
        powered_path: bool,
        outs: &mut Vec<Output>,
    ) {
        if !self.allow_request(now, from) || hops_left == 0 {
            return;
        }
        let arrive_cost = cost.saturating_add(self.link_cost(from));
        let live = self
            .cache
            .get(&req)
            .is_some_and(|e| now.saturating_sub(e.born) < REQUEST_CACHE_TTL_MS);
        if live {
            let e = self.cache.get(&req).expect("checked above");
            if e.own {
                return;
            }
            if e.target {
                if let Some(t) = self.targets.get_mut(&req)
                    && arrive_cost < t.cost
                {
                    t.cost = arrive_cost;
                    t.in_link = from;
                }
                return;
            }
            if !self.relay {
                return;
            }
            let fwd_cost = self.forward_cost(arrive_cost);
            let better = e.copies.iter().all(|(_, c)| fwd_cost < *c);
            if e.copies.len() < MAX_FORWARDED_COPIES && better {
                self.forward_request(
                    now,
                    from,
                    req,
                    tag,
                    sealed,
                    hops_left,
                    fwd_cost,
                    powered_path,
                    outs,
                );
                if let Some(e) = self.cache.get_mut(&req) {
                    e.copies.push((from, fwd_cost));
                }
            }
            return;
        }
        let is_target = tag == request_tag(&self.me.public_key(), &req);
        if is_target {
            let entry = ReqEntry {
                born: now,
                own: false,
                target: true,
                copies: Vec::new(),
                installs: MAX_LABELS_PER_REQ,
            };
            if !self.cache_insert(now, req, entry) {
                return;
            }
            if let Some(requester) = self.check_sealed(&req, &sealed, outs) {
                self.targets.insert(
                    req,
                    TargetPending {
                        requester,
                        in_link: from,
                        cost: arrive_cost,
                        due: now + TARGET_WAIT_MS,
                    },
                );
            }
        } else if self.relay {
            let fwd_cost = self.forward_cost(arrive_cost);
            let entry = ReqEntry {
                born: now,
                own: false,
                target: false,
                copies: vec![(from, fwd_cost)],
                installs: 0,
            };
            if !self.cache_insert(now, req, entry) {
                return;
            }
            self.forward_request(
                now,
                from,
                req,
                tag,
                sealed,
                hops_left,
                fwd_cost,
                powered_path,
                outs,
            );
        }
    }

    fn forward_cost(&self, arrive_cost: u32) -> u32 {
        if self.powered {
            arrive_cost
        } else {
            arrive_cost.saturating_add(UNPOWERED_RELAY_COST)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn forward_request(
        &mut self,
        now: u64,
        from: LinkId,
        req: Code,
        tag: Code,
        sealed: Vec<u8>,
        hops_left: u8,
        cost: u32,
        powered_path: bool,
        outs: &mut Vec<Output>,
    ) {
        if hops_left <= 1 {
            return;
        }
        let powered_path = powered_path && self.powered;
        for p in self.bound_links(now) {
            if p != from {
                let sealed = sealed.clone();
                self.send(outs, p, |link| Frame::RouteRequest {
                    req,
                    link,
                    tag,
                    sealed,
                    hops_left: hops_left - 1,
                    cost,
                    powered_path,
                });
            }
        }
    }

    /// Open and check a request aimed at us. `None` means silence.
    fn check_sealed(
        &mut self,
        req: &Code,
        sealed: &[u8],
        outs: &mut Vec<Output>,
    ) -> Option<PublicKey> {
        let inner = self.me.open_anonymous(sealed).ok()?;
        if inner.len() < 40 {
            return None;
        }
        let key = PublicKey(inner[..32].try_into().ok()?);
        let counter = u64::from_be_bytes(inner[32..40].try_into().ok()?);
        if !self.by_key.contains(&key) {
            return None;
        }
        let plain = self.me.box_from(key, &inner[40..]).ok()?;
        if plain.as_slice() != auth_plain(req, counter).as_slice() {
            return None;
        }
        let seen = self
            .counters
            .highest_seen
            .get(&key.node_id())
            .copied()
            .unwrap_or(0);
        if counter <= seen {
            return None;
        }
        self.counters.highest_seen.insert(key.node_id(), counter);
        outs.push(Output::CountersChanged);
        Some(key)
    }

    fn reply_as_target(&mut self, now: u64, req: Code, t: TargetPending, outs: &mut Vec<Output>) {
        if !self.links.contains_key(&t.in_link) || !self.can_install(t.in_link) {
            return;
        }
        let label = loop {
            let c = self.rand16();
            if !self.labels.contains_key(&c) {
                break c;
            }
        };
        let Ok(proof) = self
            .me
            .box_to(t.requester, &proof_plain(&req, &label, t.cost))
        else {
            return;
        };
        let peer = t.requester.node_id();
        if let Some(old) = self.routes.insert(peer, label) {
            self.labels.remove(&old);
        }
        self.labels.insert(
            label,
            LabelEntry {
                role: Role::Endpoint {
                    link: t.in_link,
                    peer,
                    dir: Direction::TowardRequester,
                },
                last_used: now,
            },
        );
        let cost = t.cost;
        self.send(outs, t.in_link, |link| Frame::RouteReply {
            req,
            link,
            label,
            cost,
            proof,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn on_reply(
        &mut self,
        now: u64,
        from: LinkId,
        req: Code,
        label: Code,
        cost: u32,
        proof: Vec<u8>,
        outs: &mut Vec<Output>,
    ) {
        if let Some(p) = self.pending.get(&req) {
            let dest = p.destination;
            let Some(&target) = self.by_id.get(&dest) else {
                return;
            };
            let ok = self
                .route_identity(dest)
                .box_from(target, &proof)
                .is_ok_and(|plain| plain.as_slice() == proof_plain(&req, &label, cost).as_slice());
            if !ok || self.labels.contains_key(&label) || !self.can_install(from) {
                return;
            }
            self.pending.remove(&req);
            if let Some(old) = self.routes.insert(dest, label) {
                self.labels.remove(&old);
            }
            self.labels.insert(
                label,
                LabelEntry {
                    role: Role::Endpoint {
                        link: from,
                        peer: dest,
                        dir: Direction::TowardTarget,
                    },
                    last_used: now,
                },
            );
            let a = self.attempts.entry(dest).or_default();
            *a = Attempt::default();
            outs.push(Output::RouteFound { destination: dest });
            return;
        }
        if !self.relay {
            return;
        }
        let Some(e) = self.cache.get(&req) else {
            return;
        };
        if e.own
            || e.target
            || e.installs >= MAX_LABELS_PER_REQ
            || now.saturating_sub(e.born) >= REQUEST_CACHE_TTL_MS
        {
            return;
        }
        let Some(&(toward_requester, _)) = e
            .copies
            .iter()
            .filter(|(l, _)| *l != from)
            .min_by_key(|(_, c)| *c)
        else {
            return;
        };
        if self.labels.contains_key(&label) || !self.can_install(from) {
            return;
        }
        if let Some(e) = self.cache.get_mut(&req) {
            e.installs += 1;
        }
        self.labels.insert(
            label,
            LabelEntry {
                role: Role::Relay {
                    toward_target: from,
                    toward_requester,
                },
                last_used: now,
            },
        );
        self.send(outs, toward_requester, |link| Frame::RouteReply {
            req,
            link,
            label,
            cost,
            proof,
        });
    }

    fn on_error(&mut self, from: LinkId, label: Code, outs: &mut Vec<Output>) {
        let valid = match self.labels.get(&label).map(|l| &l.role) {
            Some(Role::Relay {
                toward_target,
                toward_requester,
            }) => from == *toward_target || from == *toward_requester,
            Some(Role::Endpoint { link, .. }) => from == *link,
            None => false,
        };
        if valid {
            self.drop_label(label, Some(from), outs);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_hop(
        &mut self,
        now: u64,
        from: LinkId,
        label: Code,
        dir: Direction,
        sealed: Vec<u8>,
        hops_left: Option<u8>,
        outs: &mut Vec<Output>,
    ) {
        if sealed.is_empty() || sealed.len() > MAX_SEALED_HOP_BYTES {
            return;
        }
        let Some(entry) = self.labels.get_mut(&label) else {
            self.send(outs, from, |link| Frame::RouteError { link, label });
            return;
        };
        match entry.role {
            Role::Relay {
                toward_target,
                toward_requester,
            } => {
                let (expect_from, forward_to) = match dir {
                    Direction::TowardTarget => (toward_requester, toward_target),
                    Direction::TowardRequester => (toward_target, toward_requester),
                };
                if !self.relay || from != expect_from || hops_left == Some(0) {
                    return;
                }
                entry.last_used = now;
                self.send(outs, forward_to, |link| Frame::Hop {
                    link,
                    label,
                    dir,
                    hops_left: hops_left.map(|hops| hops - 1),
                    sealed,
                });
            }
            Role::Endpoint {
                link,
                dir: ours,
                peer,
            } => {
                let incoming_ok = dir != ours && from == link;
                if !incoming_ok {
                    return;
                }
                let identity = if ours == Direction::TowardTarget {
                    self.route_identities.get(&peer).unwrap_or(&self.me)
                } else {
                    &self.me
                };
                let Ok(plain) = identity.open_anonymous(&sealed) else {
                    return;
                };
                if let Some(left) = hops_left {
                    let Some((&max_hops, bytes)) = plain
                        .strip_prefix(DATAGRAM_PURPOSE)
                        .and_then(|body| body.split_first())
                    else {
                        return;
                    };
                    if !bytes.is_empty() && max_hops <= crate::DEFAULT_HOPS && left < max_hops {
                        entry.last_used = now;
                        outs.push(Output::DeliverDatagram {
                            source: peer,
                            bytes: bytes.to_vec(),
                            hops_used: max_hops - left,
                        });
                    }
                    return;
                }
                let Some(body) = plain.strip_prefix(HOP_PURPOSE) else {
                    return;
                };
                let Ok(envelope) = serde_json::from_slice::<Envelope>(body) else {
                    return;
                };
                if envelope.validate().is_ok() && envelope.destination == self.id {
                    entry.last_used = now;
                    outs.push(Output::Deliver { envelope });
                }
            }
        }
    }

    // ---------- local actions ----------

    fn want(&mut self, now: u64, destination: NodeId, outs: &mut Vec<Output>) {
        let Some(&target) = self.by_id.get(&destination) else {
            outs.push(Output::RouteFailed { destination });
            return;
        };
        if self.endpoint_route(destination).is_some() {
            outs.push(Output::RouteFound { destination });
            return;
        }
        let attempt = self.attempts.entry(destination).or_default();
        if attempt.inflight.is_some() {
            return;
        }
        if now < attempt.next_ok {
            outs.push(Output::RouteFailed { destination });
            return;
        }
        let links = self.bound_links(now);
        if links.is_empty() {
            self.fail_discovery(now, destination, outs);
            return;
        }
        let counter = self
            .counters
            .outgoing
            .get(&destination)
            .copied()
            .unwrap_or(0)
            + 1;
        let req = self.rand16();
        let sealed = (|| -> Result<Vec<u8>, Error> {
            let identity = self.route_identity(destination);
            let mut inner = identity.public_key().0.to_vec();
            inner.extend(counter.to_be_bytes());
            inner.extend(identity.box_to(target, &auth_plain(&req, counter))?);
            crypto::seal_anonymous(target, &inner)
        })();
        let Ok(sealed) = sealed else {
            self.fail_discovery(now, destination, outs);
            return;
        };
        let entry = ReqEntry {
            born: now,
            own: true,
            target: false,
            copies: Vec::new(),
            installs: MAX_LABELS_PER_REQ,
        };
        if !self.cache_insert(now, req, entry) {
            self.fail_discovery(now, destination, outs);
            return;
        }
        self.counters.outgoing.insert(destination, counter);
        outs.push(Output::CountersChanged);
        self.attempts.entry(destination).or_default().inflight = Some(req);
        self.pending.insert(
            req,
            Pending {
                destination,
                deadline: now + DISCOVERY_TIMEOUT_MS,
            },
        );
        let hops_left = 6 + self.rand_below(3);
        let cost = u32::from(self.rand_below(3));
        let tag = request_tag(&target, &req);
        let powered_path = self.powered;
        for p in links {
            let sealed = sealed.clone();
            self.send(outs, p, |link| Frame::RouteRequest {
                req,
                link,
                tag,
                sealed,
                hops_left,
                cost,
                powered_path,
            });
        }
    }

    fn send_envelope(
        &mut self,
        now: u64,
        destination: NodeId,
        envelope: Envelope,
        outs: &mut Vec<Output>,
    ) {
        let Some((label, link, dir)) = self.endpoint_route(destination) else {
            outs.push(Output::NoRoute { destination });
            return;
        };
        let sealed = self.by_id.get(&destination).and_then(|key| {
            let mut plain = HOP_PURPOSE.to_vec();
            plain.extend(serde_json::to_vec(&envelope).ok()?);
            crypto::seal_anonymous(*key, &plain).ok()
        });
        let Some(sealed) = sealed else {
            outs.push(Output::NoRoute { destination });
            return;
        };
        if let Some(l) = self.labels.get_mut(&label) {
            l.last_used = now;
        }
        self.send(outs, link, |code| Frame::Hop {
            link: code,
            label,
            dir,
            hops_left: None,
            sealed,
        });
    }

    fn send_datagram(
        &mut self,
        now: u64,
        destination: NodeId,
        bytes: Vec<u8>,
        max_hops: u8,
        outs: &mut Vec<Output>,
    ) {
        let route = self
            .endpoint_route(destination)
            .zip(self.by_id.get(&destination).copied());
        let Some(((label, link, dir), key)) = route else {
            outs.push(Output::NoRoute { destination });
            return;
        };
        if !(1..=crate::DEFAULT_HOPS).contains(&max_hops)
            || bytes.is_empty()
            || DATAGRAM_PURPOSE.len() + 1 + bytes.len() + 48 > MAX_SEALED_HOP_BYTES
        {
            return;
        }
        let mut plain = DATAGRAM_PURPOSE.to_vec();
        plain.push(max_hops);
        plain.extend(bytes);
        let Ok(sealed) = crypto::seal_anonymous(key, &plain) else {
            return;
        };
        if let Some(entry) = self.labels.get_mut(&label) {
            entry.last_used = now;
        }
        self.send(outs, link, |code| Frame::Hop {
            link: code,
            label,
            dir,
            hops_left: Some(max_hops - 1),
            sealed,
        });
    }

    fn forward_failed(&mut self, link: LinkId, frame: Frame, outs: &mut Vec<Output>) {
        if let Frame::Hop { label, .. } = frame {
            self.drop_label(label, Some(link), outs);
            // A relay still owes the other side an error; `drop_label` skipped
            // only the failed link, so nothing more to do here.
        }
    }
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;
