use crate::{Envelope, Error, NodeId};
use std::collections::{HashMap, HashSet, VecDeque};
use uuid::Uuid;

pub const SEEN_CAPACITY: usize = 1024;
pub const MAX_ROUTES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Local,
    Network,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    Duplicate,
    HopsExhausted,
    RelayDisabled,
    NoRoute,
}
impl DropReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Duplicate => "duplicate",
            Self::HopsExhausted => "hops_exhausted",
            Self::RelayDisabled => "relay_disabled",
            Self::NoRoute => "no_route",
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum Action<NextHop> {
    Deliver,
    Forward(NextHop),
    Drop(DropReason),
}

pub struct Router<NextHop> {
    id: NodeId,
    relay: bool,
    routes: HashMap<NodeId, NextHop>,
    seen: HashSet<Uuid>,
    order: VecDeque<Uuid>,
}

impl<NextHop: Clone> Router<NextHop> {
    pub fn new(id: NodeId, relay: bool) -> Self {
        Self {
            id,
            relay,
            routes: HashMap::new(),
            seen: HashSet::new(),
            order: VecDeque::new(),
        }
    }
    pub fn set_route(&mut self, destination: NodeId, next: NextHop) -> Result<(), Error> {
        if destination == self.id {
            return Err(Error::InvalidEnvelope("route to self"));
        }
        if self.routes.len() >= MAX_ROUTES && !self.routes.contains_key(&destination) {
            return Err(Error::InvalidEnvelope("route limit reached"));
        }
        self.routes.insert(destination, next);
        Ok(())
    }
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }
    pub fn relay_enabled(&self) -> bool {
        self.relay
    }

    pub fn set_relay(&mut self, enabled: bool) {
        self.relay = enabled;
    }

    pub fn route(
        &mut self,
        envelope: &mut Envelope,
        origin: Origin,
    ) -> Result<Action<NextHop>, Error> {
        self.route_inner(envelope, origin, false)
    }

    /// Resume a trusted local envelope known not to have been written yet.
    /// The caller supplies its original hop budget on each connection attempt.
    /// Network duplicates still use `route` and are never exempted.
    pub fn resume_local(&mut self, envelope: &mut Envelope) -> Result<Action<NextHop>, Error> {
        self.route_inner(envelope, Origin::Local, true)
    }

    fn route_inner(
        &mut self,
        envelope: &mut Envelope,
        origin: Origin,
        resume_local: bool,
    ) -> Result<Action<NextHop>, Error> {
        envelope.validate()?;
        if origin == Origin::Local && envelope.source != self.id {
            return Err(Error::InvalidEnvelope(
                "local source does not match this node",
            ));
        }
        if self.seen.contains(&envelope.id) && !resume_local {
            return Ok(Action::Drop(DropReason::Duplicate));
        }
        if envelope.destination == self.id {
            return Ok(Action::Deliver);
        }
        if origin == Origin::Network && !self.relay {
            return Ok(Action::Drop(DropReason::RelayDisabled));
        }
        if envelope.remaining_hops == 0 {
            return Ok(Action::Drop(DropReason::HopsExhausted));
        }
        let Some(next) = self.routes.get(&envelope.destination).cloned() else {
            return Ok(Action::Drop(DropReason::NoRoute));
        };
        envelope.remaining_hops -= 1;
        self.remember(envelope.id);
        Ok(Action::Forward(next))
    }

    /// Commit only after endpoint decryption succeeds, so unauthenticated input
    /// cannot consume that endpoint's delivery slot for a valid packet.
    pub fn confirm_delivery(&mut self, id: Uuid) -> bool {
        if self.seen.contains(&id) {
            return false;
        }
        self.remember(id);
        true
    }
    fn remember(&mut self, id: Uuid) {
        if self.seen.insert(id) {
            self.order.push_back(id);
            if self.order.len() > SEEN_CAPACITY
                && let Some(old) = self.order.pop_front()
            {
                self.seen.remove(&old);
            }
        }
    }
}
