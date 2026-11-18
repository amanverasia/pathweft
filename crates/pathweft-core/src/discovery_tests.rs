use super::*;
use crate::Payload;

struct TestRng(u64);
impl Random for TestRng {
    fn fill(&mut self, buf: &mut [u8]) {
        for b in buf {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            *b = (z ^ (z >> 31)) as u8;
        }
    }
}

struct SimNode {
    d: Discovery,
    ident: Identity,
    id: NodeId,
    relay: bool,
    powered: bool,
    seed: u64,
    links: Vec<(LinkId, LinkKind)>,
    delivered: Vec<Envelope>,
    datagrams: Vec<(NodeId, Vec<u8>, u8)>,
    found: Vec<NodeId>,
    failed: Vec<NodeId>,
    invalidated: Vec<NodeId>,
    no_route: Vec<NodeId>,
    sent: Vec<(LinkId, Frame)>,
}

struct Net {
    nodes: Vec<SimNode>,
    peers: HashMap<(usize, LinkId), (usize, LinkId)>,
    cut: HashSet<(usize, usize)>,
    drop_hops_at: HashSet<usize>,
    queue: VecDeque<(usize, LinkId, Vec<u8>)>,
    now: u64,
}

impl Net {
    fn new(config: &[(bool, bool)]) -> Self {
        let nodes = config
            .iter()
            .enumerate()
            .map(|(i, &(relay, powered))| {
                let ident = Identity::generate().unwrap();
                let seed = 1000 + i as u64;
                SimNode {
                    d: Discovery::new(
                        &ident,
                        [],
                        relay,
                        powered,
                        CounterState::default(),
                        Box::new(TestRng(seed)),
                    )
                    .unwrap(),
                    id: ident.id(),
                    ident,
                    relay,
                    powered,
                    seed,
                    links: Vec::new(),
                    delivered: vec![],
                    datagrams: vec![],
                    found: vec![],
                    failed: vec![],
                    invalidated: vec![],
                    no_route: vec![],
                    sent: vec![],
                }
            })
            .collect();
        Self {
            nodes,
            peers: HashMap::new(),
            cut: HashSet::new(),
            drop_hops_at: HashSet::new(),
            queue: VecDeque::new(),
            now: 1_000,
        }
    }
    fn simple(n: usize) -> Self {
        Self::new(&vec![(true, true); n])
    }
    fn pin_one(&mut self, a: usize, b: usize) {
        let pk = self.nodes[b].ident.public_key();
        self.nodes[a].d.add_contact(pk);
    }
    fn pin(&mut self, a: usize, b: usize) {
        self.pin_one(a, b);
        self.pin_one(b, a);
    }
    fn connect(&mut self, a: usize, b: usize, kind: LinkKind) -> (LinkId, LinkId) {
        let la = LinkId(self.nodes[a].links.len() as u32);
        let lb = LinkId(self.nodes[b].links.len() as u32);
        self.nodes[a].links.push((la, kind));
        self.nodes[b].links.push((lb, kind));
        self.peers.insert((a, la), (b, lb));
        self.peers.insert((b, lb), (a, la));
        self.input(a, Input::LinkUp { link: la, kind });
        self.input(b, Input::LinkUp { link: lb, kind });
        self.drain();
        (la, lb)
    }
    fn cut(&mut self, a: usize, b: usize) {
        self.cut.insert((a, b));
        self.cut.insert((b, a));
    }
    fn heal(&mut self, a: usize, b: usize) {
        self.cut.remove(&(a, b));
        self.cut.remove(&(b, a));
    }
    fn input(&mut self, n: usize, input: Input) {
        let outs = self.nodes[n].d.handle(self.now, input);
        for out in outs {
            match out {
                Output::Send { link, frame } => {
                    self.nodes[n].sent.push((link, frame.clone()));
                    let hop = matches!(frame, Frame::Hop { .. });
                    if hop && self.drop_hops_at.contains(&n) {
                        continue;
                    }
                    let Some(&(peer, plink)) = self.peers.get(&(n, link)) else {
                        continue;
                    };
                    if self.cut.contains(&(n, peer)) {
                        if hop {
                            self.input(n, Input::ForwardFailed { link, frame });
                        }
                        continue;
                    }
                    self.queue
                        .push_back((peer, plink, frame.to_bytes().unwrap()));
                }
                Output::RouteFound { destination } => self.nodes[n].found.push(destination),
                Output::RouteFailed { destination } => self.nodes[n].failed.push(destination),
                Output::RouteInvalidated { destination } => {
                    self.nodes[n].invalidated.push(destination)
                }
                Output::NoRoute { destination } => self.nodes[n].no_route.push(destination),
                Output::Deliver { envelope } => self.nodes[n].delivered.push(envelope),
                Output::DeliverDatagram {
                    source,
                    bytes,
                    hops_used,
                } => self.nodes[n].datagrams.push((source, bytes, hops_used)),
                Output::CountersChanged => {}
            }
        }
    }
    fn drain(&mut self) {
        while let Some((n, _l, bytes)) = self.queue.pop_front() {
            match parse_network_frame(&bytes).unwrap() {
                NetworkFrame::Discovery(frame) => self.input(n, Input::Frame { link: None, frame }),
                NetworkFrame::Legacy(_) => panic!("unexpected legacy frame"),
            }
        }
    }
    fn advance(&mut self, ms: u64) {
        let end = self.now + ms;
        while self.now < end {
            self.now += 50;
            for n in 0..self.nodes.len() {
                self.input(n, Input::Tick);
            }
            self.drain();
        }
    }
    fn want(&mut self, n: usize, dest: usize) {
        let destination = self.nodes[dest].id;
        self.input(n, Input::Want { destination });
        self.drain();
    }
    fn send(&mut self, from: usize, to: usize) -> Envelope {
        let env = self.nodes[from]
            .ident
            .seal(
                self.nodes[to].ident.public_key(),
                Payload::Text { text: "hi".into() },
            )
            .unwrap();
        let destination = self.nodes[to].id;
        self.input(
            from,
            Input::SendEnvelope {
                destination,
                envelope: env.clone(),
            },
        );
        self.drain();
        env
    }
    fn discover(&mut self, a: usize, b: usize) {
        self.want(a, b);
        self.advance(1_000);
        assert!(
            self.nodes[a].found.contains(&self.nodes[b].id),
            "route not found"
        );
    }
    fn count_sent(&self, n: usize, pred: impl Fn(&Frame) -> bool) -> usize {
        self.nodes[n].sent.iter().filter(|(_, f)| pred(f)).count()
    }
    fn restart(&mut self, n: usize, counters: CounterState) {
        let node = &mut self.nodes[n];
        node.seed += 100;
        let contacts: Vec<PublicKey> = node.d.by_key.iter().copied().collect();
        node.d = Discovery::new(
            &node.ident,
            contacts,
            node.relay,
            node.powered,
            counters,
            Box::new(TestRng(node.seed)),
        )
        .unwrap();
        for (link, kind) in node.links.clone() {
            self.input(n, Input::LinkUp { link, kind });
        }
        self.drain();
    }
}

fn is_request(f: &Frame) -> bool {
    matches!(f, Frame::RouteRequest { .. })
}
fn is_reply(f: &Frame) -> bool {
    matches!(f, Frame::RouteReply { .. })
}

#[test]
fn line_finds_route_and_carries_envelopes_both_ways() {
    let mut net = Net::simple(4);
    for i in 0..3 {
        net.connect(i, i + 1, LinkKind::Lan);
    }
    net.pin(0, 3);
    net.discover(0, 3);
    let env = net.send(0, 3);
    assert_eq!(net.nodes[3].delivered, vec![env.clone()]);
    assert!(
        net.nodes[3]
            .ident
            .open(net.nodes[0].ident.public_key(), &env)
            .is_ok()
    );
    let back = net.send(3, 0);
    assert_eq!(net.nodes[0].delivered, vec![back]);
    // Relays hold one label each and nothing was delivered to them.
    assert_eq!(net.nodes[1].d.label_count(), 1);
    assert!(net.nodes[1].delivered.is_empty() && net.nodes[2].delivered.is_empty());
}

#[test]
fn cheaper_path_is_chosen() {
    let mut net = Net::simple(4);
    net.connect(0, 1, LinkKind::Ble);
    net.connect(1, 3, LinkKind::Ble);
    let (via_c, _) = net.connect(0, 2, LinkKind::Lan);
    net.connect(2, 3, LinkKind::Lan);
    net.pin(0, 3);
    net.discover(0, 3);
    let (_, link, _) = net.nodes[0].d.endpoint_route(net.nodes[3].id).unwrap();
    assert_eq!(link, via_c);
    let env = net.send(0, 3);
    assert_eq!(net.nodes[3].delivered, vec![env]);
    assert_eq!(net.nodes[1].d.label_count(), 0);
}

#[test]
fn unpowered_relay_is_penalised() {
    // Equal link kinds; only the relay's power differs (cost 4 versus 2).
    let mut net = Net::new(&[(true, true), (true, false), (true, true), (true, true)]);
    net.connect(0, 1, LinkKind::Lan);
    net.connect(1, 3, LinkKind::Lan);
    let (via_powered, _) = net.connect(0, 2, LinkKind::Lan);
    net.connect(2, 3, LinkKind::Lan);
    net.pin(0, 3);
    net.discover(0, 3);
    let (_, link, _) = net.nodes[0].d.endpoint_route(net.nodes[3].id).unwrap();
    assert_eq!(link, via_powered);
}

#[test]
fn partition_then_heal_rediscovers() {
    let mut net = Net::simple(4);
    for i in 0..3 {
        net.connect(i, i + 1, LinkKind::Lan);
    }
    net.pin(0, 3);
    net.discover(0, 3);
    net.cut(1, 2);
    net.send(0, 3);
    assert!(net.nodes[3].delivered.is_empty());
    let d_id = net.nodes[3].id;
    assert!(
        net.nodes[0].invalidated.contains(&d_id),
        "route_error reached the requester"
    );
    assert!(!net.nodes[0].d.has_route(d_id));
    net.want(0, 3);
    net.advance(5_000);
    assert!(net.nodes[0].failed.contains(&d_id));
    net.heal(1, 2);
    net.want(0, 3);
    net.advance(1_000);
    assert!(net.nodes[0].d.has_route(d_id));
    let env = net.send(0, 3);
    assert_eq!(net.nodes[3].delivered, vec![env]);
}

#[test]
fn relay_off_answers_for_itself_but_never_forwards() {
    let mut net = Net::new(&[(true, true), (false, true), (true, true)]);
    net.connect(0, 1, LinkKind::Lan);
    net.connect(1, 2, LinkKind::Lan);
    net.pin(0, 2);
    net.pin(0, 1);
    net.want(0, 2);
    net.advance(4_000);
    assert!(net.nodes[0].failed.contains(&net.nodes[2].id));
    assert_eq!(net.count_sent(1, is_request), 0);
    assert_eq!(net.nodes[2].d.cache.len(), 0);
    net.want(0, 1);
    net.advance(1_000);
    assert!(net.nodes[0].d.has_route(net.nodes[1].id));
    let env = net.send(0, 1);
    assert_eq!(net.nodes[1].delivered, vec![env]);
}

#[test]
fn relay_dropping_hops_blocks_delivery_but_allows_retry() {
    let mut net = Net::simple(3);
    net.connect(0, 1, LinkKind::Lan);
    net.connect(1, 2, LinkKind::Lan);
    net.pin(0, 2);
    net.discover(0, 2);
    let dest = net.nodes[2].id;
    let first_label = net.nodes[0].d.routes[&dest];
    net.drop_hops_at.insert(1);
    net.send(0, 2);
    assert!(net.nodes[2].delivered.is_empty());
    net.drop_hops_at.clear();
    // The node's receipt timeout fires: it invalidates and rediscovers.
    net.input(0, Input::Invalidate { destination: dest });
    assert!(!net.nodes[0].d.has_route(dest));
    net.want(0, 2);
    net.advance(1_000);
    assert!(net.nodes[0].d.has_route(dest));
    assert_ne!(net.nodes[0].d.routes[&dest], first_label);
    let env = net.send(0, 2);
    assert_eq!(net.nodes[2].delivered, vec![env]);
}

#[test]
fn flooding_neighbour_is_rate_limited_and_others_still_work() {
    // 0=Q, 1=R(relay), 2=T, 3=X (flooder)
    let mut net = Net::simple(4);
    net.connect(0, 1, LinkKind::Lan);
    let (r_to_t, _) = {
        let (a, b) = net.connect(1, 2, LinkKind::Lan);
        (a, b)
    };
    let (x_link, r_from_x) = net.connect(3, 1, LinkKind::Lan);
    net.pin(0, 2);
    let xcode = net.nodes[3].d.link_code(x_link).unwrap();
    for i in 0..100_u32 {
        let mut req = [0_u8; 16];
        req[..4].copy_from_slice(&(i + 1).to_be_bytes());
        let frame = Frame::RouteRequest {
            req: Code(req),
            link: xcode,
            tag: Code([9; 16]),
            sealed: vec![7; 100],
            hops_left: 6,
            cost: 0,
            powered_path: true,
        };
        net.input(
            1,
            Input::Frame {
                link: Some(r_from_x),
                frame,
            },
        );
    }
    net.drain();
    let forwarded = net.nodes[1]
        .sent
        .iter()
        .filter(|(l, f)| *l == r_to_t && is_request(f))
        .count();
    assert_eq!(
        forwarded, 20,
        "burst of 20 then nothing at the same instant"
    );
    // T's own per-neighbour bucket for R was drained by the 20 forwarded copies,
    // so a legitimate request must wait for the 10/s refill.
    net.advance(1_000);
    net.discover(0, 2);
}

#[test]
fn unpinned_requester_gets_silence() {
    let mut net = Net::simple(3);
    net.connect(0, 1, LinkKind::Lan);
    net.connect(1, 2, LinkKind::Lan);
    net.pin_one(0, 2); // requester knows the target; target does not pin the requester
    net.want(0, 2);
    net.advance(4_000);
    assert!(net.nodes[0].found.is_empty());
    assert!(net.nodes[0].failed.contains(&net.nodes[2].id));
    assert_eq!(net.count_sent(2, is_reply), 0);
    assert!(net.nodes[2].d.counters().highest_seen.is_empty());
    assert_eq!(net.nodes[2].d.label_count(), 0);
}

#[test]
fn replayed_request_after_cache_expiry_is_silent() {
    let mut net = Net::simple(3);
    let (_, m_from_r) = net.connect(0, 1, LinkKind::Lan);
    net.connect(1, 2, LinkKind::Lan);
    net.pin(0, 2);
    net.discover(0, 2);
    let captured = net.nodes[0]
        .sent
        .iter()
        .find(|(_, f)| is_request(f))
        .unwrap()
        .1
        .clone();
    net.advance(70_000);
    let replies = net.count_sent(2, is_reply);
    let forwarded = net.count_sent(1, is_request);
    net.input(
        1,
        Input::Frame {
            link: Some(m_from_r),
            frame: captured,
        },
    );
    net.drain();
    net.advance(1_000);
    assert!(
        net.count_sent(1, is_request) > forwarded,
        "relay did forward the replay"
    );
    assert_eq!(net.count_sent(2, is_reply), replies, "target stayed silent");
    assert_eq!(net.nodes[2].d.counters().highest_seen[&net.nodes[0].id], 1);
}

#[test]
fn forged_reply_is_rejected() {
    let mut net = Net::simple(3); // 0=A, 1=X attacker, 2=D (unconnected)
    let (la, lx) = net.connect(0, 1, LinkKind::Lan);
    net.pin_one(0, 2);
    net.want(0, 2);
    let req = match net.nodes[0]
        .sent
        .iter()
        .find(|(_, f)| is_request(f))
        .unwrap()
        .1
        .clone()
    {
        Frame::RouteRequest { req, .. } => req,
        _ => unreachable!(),
    };
    let xcode = net.nodes[1].d.link_code(lx).unwrap();
    let label = Code([5; 16]);
    let a_key = net.nodes[0].ident.public_key();
    let good_shape = net.nodes[1]
        .ident
        .box_to(a_key, &proof_plain(&req, &label, 1))
        .unwrap();
    for proof in [vec![1_u8; 99], good_shape] {
        let frame = Frame::RouteReply {
            req,
            link: xcode,
            label,
            cost: 1,
            proof,
        };
        net.input(
            0,
            Input::Frame {
                link: Some(la),
                frame,
            },
        );
    }
    net.drain();
    assert!(net.nodes[0].found.is_empty());
    assert!(!net.nodes[0].d.has_route(net.nodes[2].id));
    assert_eq!(net.nodes[0].d.label_count(), 0);
}

#[test]
fn copied_hello_binds_nothing_to_the_copier() {
    let mut net = Net::simple(3);
    let (a_b, _) = net.connect(0, 1, LinkKind::Lan);
    let (a_c, _) = net.connect(0, 2, LinkKind::Lan);
    let (hello, b_code) = net.nodes[1]
        .sent
        .iter()
        .rev()
        .find_map(|(_, f)| match f {
            Frame::Hello { link, .. } => Some((f.clone(), *link)),
            _ => None,
        })
        .unwrap();
    assert_eq!(net.nodes[0].d.bindings[&b_code].link, a_b);
    // C replays B's hello verbatim at A over C's link.
    net.input(
        0,
        Input::Frame {
            link: Some(a_c),
            frame: hello,
        },
    );
    assert_eq!(net.nodes[0].d.bindings[&b_code].link, a_b);
    assert_eq!(net.nodes[0].d.resolve(net.now, &b_code), Some(a_b));

    // A hello that echoes nothing of ours (B talking to C only, copied to A) binds nothing.
    let mut net = Net::simple(3);
    net.connect(1, 2, LinkKind::Lan);
    let (a_c, _) = net.connect(0, 2, LinkKind::Lan);
    let (hello, b_code) = net.nodes[1]
        .sent
        .iter()
        .rev()
        .find_map(|(_, f)| match f {
            Frame::Hello { link, .. } => Some((f.clone(), *link)),
            _ => None,
        })
        .unwrap();
    net.input(
        0,
        Input::Frame {
            link: Some(a_c),
            frame: hello,
        },
    );
    assert!(!net.nodes[0].d.bindings.contains_key(&b_code));
}

#[test]
fn bindings_expire_without_hellos() {
    let mut net = Net::simple(2);
    net.connect(0, 1, LinkKind::Lan);
    assert_eq!(net.nodes[0].d.bound_links(net.now).len(), 1);
    net.cut(0, 1);
    net.advance(100_000);
    assert!(net.nodes[0].d.bound_links(net.now).is_empty());
}

/// Expected behaviour only: the simulator brings both ends of a link up together, so it does not
/// reproduce the start-up race behind the owed-hello fix. scripts/discovery_demo.py (nodes started
/// back to back) is the regression check: it fails without the fix and passes with it.
#[test]
fn two_neighbours_arriving_together_are_both_bound_quickly() {
    let mut net = Net::simple(3);
    net.connect(0, 1, LinkKind::Lan);
    net.advance(200); // well inside the 1 s reply limit
    net.connect(0, 2, LinkKind::Lan);
    net.advance(1_500); // the owed hello goes out once the limit allows
    assert_eq!(
        net.nodes[0].d.bound_links(net.now).len(),
        2,
        "node 0 binds both within ~1.5 s"
    );
    for n in [1, 2] {
        assert_eq!(
            net.nodes[n].d.bound_links(net.now).len(),
            1,
            "node {n} binds node 0 within ~1.5 s, not 30 s"
        );
    }
}

#[test]
fn backoff_limits_repeat_discovery() {
    let mut net = Net::simple(3);
    net.connect(0, 1, LinkKind::Lan);
    net.pin_one(0, 2); // never reachable
    for _ in 0..5 {
        net.want(0, 2);
    }
    assert_eq!(net.count_sent(0, is_request), 1, "one discovery in flight");
    net.advance(3_100);
    assert_eq!(net.nodes[0].failed.len(), 1);
    net.want(0, 2);
    assert_eq!(net.count_sent(0, is_request), 1, "inside the 1 s backoff");
    net.advance(1_100);
    net.want(0, 2);
    assert_eq!(net.count_sent(0, is_request), 2);
}

#[test]
fn legacy_and_discovery_frames_are_classified_and_bounded() {
    let a = Identity::generate().unwrap();
    let b = Identity::generate().unwrap();
    let env = a
        .seal(b.public_key(), Payload::Text { text: "x".into() })
        .unwrap();
    let legacy = serde_json::to_vec(&env).unwrap();
    assert_eq!(
        parse_network_frame(&legacy).unwrap(),
        NetworkFrame::Legacy(env.clone())
    );

    let hello = Frame::Hello {
        link: Code([1; 16]),
        heard: vec![Code([2; 16])],
        powered: true,
    };
    let bytes = hello.to_bytes().unwrap();
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains("\"frame\":\"hello\"")
    );
    assert_eq!(
        parse_network_frame(&bytes).unwrap(),
        NetworkFrame::Discovery(hello)
    );

    let hop = Frame::Hop {
        link: Code([1; 16]),
        label: Code([3; 16]),
        dir: Direction::TowardTarget,
        hops_left: None,
        sealed: crypto::seal_anonymous(b.public_key(), b"sealed").unwrap(),
    };
    let hop_bytes = hop.to_bytes().unwrap();
    assert!(!hop_bytes.is_empty());
    assert_eq!(
        parse_network_frame(&hop_bytes).unwrap(),
        NetworkFrame::Discovery(hop)
    );

    let good =
        br#"{"frame":"hello","link":"01010101010101010101010101010101","heard":[],"powered":true}"#;
    assert!(parse_network_frame(good).is_ok());
    let unknown = br#"{"frame":"hello","link":"01010101010101010101010101010101","heard":[],"powered":true,"x":1}"#;
    assert!(parse_network_frame(unknown).is_err());
    let unknown_kind = br#"{"frame":"bogus"}"#;
    assert!(parse_network_frame(unknown_kind).is_err());
    let short_code = br#"{"frame":"hello","link":"0101","heard":[],"powered":true}"#;
    assert!(parse_network_frame(short_code).is_err());
    let mut padded = good.to_vec();
    padded.extend(std::iter::repeat_n(b' ', MAX_CONTROL_FRAME_BYTES));
    assert!(parse_network_frame(&padded).is_err());
    let mut huge = legacy.clone();
    huge.extend(std::iter::repeat_n(b' ', MAX_FRAME_BYTES));
    assert!(parse_network_frame(&huge).is_err());
    let many: Vec<String> = (0..17)
        .map(|_| format!("\"{}\"", "ab".repeat(16)))
        .collect();
    let too_many = format!(
        r#"{{"frame":"hello","link":"{}","heard":[{}],"powered":true}}"#,
        "01".repeat(16),
        many.join(",")
    );
    assert!(parse_network_frame(too_many.as_bytes()).is_err());
}

#[test]
fn counters_survive_restart_and_still_reject_replays() {
    let mut net = Net::simple(3);
    let (_, b_from_a) = net.connect(0, 1, LinkKind::Lan);
    net.connect(1, 2, LinkKind::Lan);
    net.pin(0, 2);
    net.discover(0, 2);
    let (a_id, d_id) = (net.nodes[0].id, net.nodes[2].id);
    let a_counters = net.nodes[0].d.counters();
    let d_counters = net.nodes[2].d.counters();
    assert_eq!(a_counters.outgoing[&d_id], 1);
    assert_eq!(d_counters.highest_seen[&a_id], 1);
    // The state serialises for the node to save.
    let saved: CounterState =
        serde_json::from_str(&serde_json::to_string(&d_counters).unwrap()).unwrap();
    assert_eq!(saved, d_counters);
    let captured = net.nodes[0]
        .sent
        .iter()
        .find(|(_, f)| is_request(f))
        .unwrap()
        .1
        .clone();

    net.advance(61_000);
    net.restart(2, saved);
    net.advance(2_000);
    let replies = net.count_sent(2, is_reply);
    net.input(
        1,
        Input::Frame {
            link: Some(b_from_a),
            frame: captured,
        },
    );
    net.drain();
    net.advance(1_000);
    assert_eq!(
        net.count_sent(2, is_reply),
        replies,
        "replay rejected after restart"
    );

    // A restarted requester that lost its counter is ignored; one that kept it succeeds.
    net.restart(0, CounterState::default());
    net.advance(2_000);
    net.want(0, 2);
    net.advance(4_000);
    assert!(!net.nodes[0].d.has_route(d_id));
    net.restart(0, a_counters);
    net.advance(2_000);
    net.want(0, 2);
    net.advance(1_000);
    assert!(net.nodes[0].d.has_route(d_id));
    assert_eq!(net.nodes[0].d.counters().outgoing[&d_id], 2);
}

#[test]
fn fake_reply_racing_the_real_one_does_not_block_discovery() {
    // 0=R requester, 1=M relay, 2=T target, 3=F malicious neighbour of M.
    let mut net = Net::simple(4);
    net.connect(0, 1, LinkKind::Lan);
    net.connect(1, 2, LinkKind::Lan);
    let (f_link, _) = net.connect(3, 1, LinkKind::Lan);
    net.pin(0, 2);
    net.want(0, 2);
    let req = match net.nodes[0].sent.iter().find(|(_, f)| is_request(f)) {
        Some((_, Frame::RouteRequest { req, .. })) => *req,
        _ => panic!("R never sent the request"),
    };
    let fcode = net.nodes[3].d.link_code(f_link).unwrap();
    // Fake reply reaches M before T's 300 ms wait is over.
    let fake = Frame::RouteReply {
        req,
        link: fcode,
        label: Code([0xEE; 16]),
        cost: 1,
        proof: vec![3; 99],
    };
    net.input(
        1,
        Input::Frame {
            link: None,
            frame: fake,
        },
    );
    net.drain();
    assert_eq!(
        net.nodes[1].d.label_count(),
        1,
        "relay installed the fake label"
    );
    assert!(net.nodes[0].found.is_empty());
    assert!(
        net.nodes[0].d.pending.contains_key(&req),
        "fake reply must not clear pending"
    );
    net.advance(1_000);
    assert!(net.nodes[0].d.has_route(net.nodes[2].id));
    let env = net.send(0, 2);
    assert_eq!(net.nodes[2].delivered, vec![env]);
    // Fake label forwards only toward the requester and leads nowhere for hops.
    assert_eq!(net.nodes[1].d.label_count(), 2);
}

/// A real hop frame as it reached node 3 on the line 0-1-2-3, plus its envelope.
fn hop_fixture() -> (Net, Frame) {
    let mut net = Net::simple(4);
    for i in 0..3 {
        net.connect(i, i + 1, LinkKind::Lan);
    }
    net.pin(0, 3);
    net.discover(0, 3);
    let env = net.send(0, 3);
    assert_eq!(net.nodes[3].delivered, vec![env]);
    let genuine = net.nodes[2]
        .sent
        .iter()
        .find_map(|(_, f)| matches!(f, Frame::Hop { .. }).then(|| f.clone()))
        .unwrap();
    (net, genuine)
}

fn with_sealed(frame: &Frame, new_sealed: Vec<u8>) -> Frame {
    let Frame::Hop {
        link,
        label,
        dir,
        hops_left,
        ..
    } = frame.clone()
    else {
        panic!("not a hop")
    };
    Frame::Hop {
        link,
        label,
        dir,
        hops_left,
        sealed: new_sealed,
    }
}

fn hop_plain(envelope: &Envelope, purpose: &[u8]) -> Vec<u8> {
    let mut plain = purpose.to_vec();
    plain.extend(serde_json::to_vec(envelope).unwrap());
    plain
}

#[test]
fn endpoint_accepts_only_hops_sealed_to_it_with_the_right_purpose_and_destination() {
    let (mut net, genuine) = hop_fixture();
    let key3 = net.nodes[3].ident.public_key();
    let key0 = net.nodes[0].ident.public_key();
    let mine = net.nodes[0]
        .ident
        .seal(
            key3,
            Payload::Text {
                text: "again".into(),
            },
        )
        .unwrap();
    let to_other = net.nodes[0]
        .ident
        .seal(
            net.nodes[1].ident.public_key(),
            Payload::Text {
                text: "misdirected".into(),
            },
        )
        .unwrap();
    let before = net.nodes[3].delivered.len();

    // Rejected: sealed to another node's key, wrong purpose, no purpose, garbage,
    // inner destination is someone else, oversized.
    let wrong_key = crypto::seal_anonymous(key0, &hop_plain(&mine, HOP_PURPOSE)).unwrap();
    let wrong_purpose =
        crypto::seal_anonymous(key3, &hop_plain(&mine, b"pathweft/hop/v2")).unwrap();
    let no_purpose = crypto::seal_anonymous(key3, &serde_json::to_vec(&mine).unwrap()).unwrap();
    let garbage = crypto::seal_anonymous(key3, HOP_PURPOSE).unwrap();
    let wrong_dest = crypto::seal_anonymous(key3, &hop_plain(&to_other, HOP_PURPOSE)).unwrap();
    let oversized = crypto::seal_anonymous(key3, &vec![b'x'; MAX_SEALED_HOP_BYTES]).unwrap();
    assert!(oversized.len() > MAX_SEALED_HOP_BYTES);
    for bad in [
        wrong_key,
        wrong_purpose,
        no_purpose,
        garbage,
        wrong_dest,
        oversized,
    ] {
        let frame = with_sealed(&genuine, bad);
        net.input(3, Input::Frame { link: None, frame });
        net.drain();
    }
    assert_eq!(
        net.nodes[3].delivered.len(),
        before,
        "nothing was delivered"
    );

    // The oversized frame is also refused before it is even parsed on the wire.
    let too_big = with_sealed(&genuine, vec![0; MAX_SEALED_HOP_BYTES + 1]);
    assert!(too_big.to_bytes().is_err());

    // Control: the same label, with a correct box, is delivered.
    let good = crypto::seal_anonymous(key3, &hop_plain(&mine, HOP_PURPOSE)).unwrap();
    net.input(
        3,
        Input::Frame {
            link: None,
            frame: with_sealed(&genuine, good),
        },
    );
    net.drain();
    assert_eq!(net.nodes[3].delivered.last(), Some(&mine));
    assert_eq!(net.nodes[3].delivered.len(), before + 1);
}

#[test]
fn relays_forward_sealed_hops_unchanged_without_opening_them() {
    let (net, genuine) = hop_fixture();
    let Frame::Hop { sealed, .. } = &genuine else {
        panic!()
    };
    // Every relay sent the same opaque bytes on, and cannot open them.
    for relay in [1, 2] {
        let hops: Vec<&Vec<u8>> = net.nodes[relay]
            .sent
            .iter()
            .filter_map(|(_, f)| match f {
                Frame::Hop { sealed, .. } => Some(sealed),
                _ => None,
            })
            .collect();
        assert_eq!(hops, vec![sealed]);
        assert!(net.nodes[relay].ident.open_anonymous(sealed).is_err());
    }
}

#[test]
fn largest_realistic_envelopes_fit_in_a_sealed_hop_frame() {
    let a = Identity::generate().unwrap();
    let b = Identity::generate().unwrap();
    let original = a
        .seal(
            b.public_key(),
            Payload::Text {
                text: "🌍".repeat(crate::MAX_TEXT_BYTES / 4),
            },
        )
        .unwrap();
    assert!(original.ciphertext.len() > 500);
    // The node builds a redelivery as a fresh envelope around the original.
    let redelivery = a
        .seal(
            b.public_key(),
            Payload::Redelivery {
                packet: original.clone(),
            },
        )
        .unwrap();
    for (name, envelope) in [("text", &original), ("redelivery", &redelivery)] {
        let sealed =
            crypto::seal_anonymous(b.public_key(), &hop_plain(envelope, HOP_PURPOSE)).unwrap();
        assert!(sealed.len() <= MAX_SEALED_HOP_BYTES, "{name}");
        let frame = Frame::Hop {
            link: Code([1; 16]),
            label: Code([2; 16]),
            dir: Direction::TowardRequester,
            hops_left: None,
            sealed,
        };
        let bytes = frame.to_bytes().unwrap();
        println!(
            "{name}: envelope {} B, frame {} B",
            serde_json::to_vec(envelope).unwrap().len(),
            bytes.len()
        );
        assert!(
            bytes.len() <= MAX_FRAME_BYTES,
            "{name} frame is {} bytes",
            bytes.len()
        );
        assert_eq!(
            parse_network_frame(&bytes).unwrap(),
            NetworkFrame::Discovery(frame)
        );
    }
    // Hop frames beyond the cap are refused at both ends of the wire.
    let worst = Frame::Hop {
        link: Code([1; 16]),
        label: Code([2; 16]),
        dir: Direction::TowardTarget,
        hops_left: None,
        sealed: vec![0xff; MAX_SEALED_HOP_BYTES],
    };
    assert!(worst.to_bytes().unwrap().len() <= MAX_FRAME_BYTES);
}

#[test]
fn base64_round_trips_and_rejects_non_canonical_input() {
    for len in 0..40_usize {
        let data: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
        let text = b64vec::encode(&data);
        assert_eq!(b64vec::decode(&text).unwrap(), data);
    }
    assert_eq!(b64vec::encode(b"Man"), "TWFu");
    assert_eq!(b64vec::encode(b"Ma"), "TWE=");
    assert_eq!(b64vec::encode(b"M"), "TQ==");
    for bad in [
        "TWF", "TWE", "T===", "TW=u", "TWE*", "TWF=\n", "TQ=A", "TWF u", "TR==", "TWF=TWFu",
    ] {
        assert!(b64vec::decode(bad).is_err(), "{bad}");
    }
}

#[test]
fn contact_asking_first_restores_reachability_after_a_counter_loss() {
    let mut net = Net::simple(3);
    net.connect(0, 1, LinkKind::Lan);
    net.connect(1, 2, LinkKind::Lan);
    net.pin(0, 2);
    net.discover(0, 2);
    let (a_id, d_id) = (net.nodes[0].id, net.nodes[2].id);
    let d_counters = net.nodes[2].d.counters();
    assert_eq!(d_counters.highest_seen[&a_id], 1);

    // A loses its counter file but keeps its identity; D keeps its state.
    net.restart(0, CounterState::default());
    net.restart(2, d_counters);
    net.advance(2_000);
    net.want(0, 2);
    net.advance(4_000);
    assert!(!net.nodes[0].d.has_route(d_id), "A is ignored by D");

    // D asks first: A has no memory of D, accepts, and both can now send.
    net.advance(30_000);
    net.want(2, 0);
    net.advance(1_000);
    assert!(net.nodes[2].d.has_route(a_id));
    assert!(net.nodes[0].d.has_route(d_id));
    let env = net.send(0, 2);
    assert_eq!(net.nodes[2].delivered.last(), Some(&env));
}

#[test]
fn datagrams_count_each_handoff_and_return_to_the_separate_route_key() {
    let mut net = Net::simple(3);
    net.connect(0, 1, LinkKind::Lan);
    net.connect(1, 2, LinkKind::Lan);
    let alias = Identity::generate().unwrap();
    let alias_key = alias.public_key();
    let target = net.nodes[2].ident.public_key();
    net.nodes[0].d.set_route_identity(target, alias);
    net.nodes[2].d.add_contact(alias_key);
    net.discover(0, 2);
    assert!(!net.nodes[2].d.has_route(net.nodes[0].id));
    assert!(net.nodes[2].d.has_route(alias_key.node_id()));

    net.input(
        0,
        Input::SendDatagram {
            destination: target.node_id(),
            bytes: vec![1],
            max_hops: 1,
        },
    );
    net.drain();
    assert!(net.nodes[2].datagrams.is_empty());

    net.input(
        0,
        Input::SendDatagram {
            destination: target.node_id(),
            bytes: vec![2],
            max_hops: 2,
        },
    );
    net.drain();
    assert_eq!(
        net.nodes[2].datagrams,
        vec![(alias_key.node_id(), vec![2], 2)]
    );
    assert!(net.nodes[1].datagrams.is_empty());

    net.input(
        2,
        Input::SendDatagram {
            destination: alias_key.node_id(),
            bytes: vec![3],
            max_hops: 3,
        },
    );
    net.drain();
    assert_eq!(net.nodes[0].datagrams, vec![(target.node_id(), vec![3], 2)]);

    net.nodes[1].d.set_relay(false);
    net.input(
        0,
        Input::SendDatagram {
            destination: target.node_id(),
            bytes: vec![4],
            max_hops: 2,
        },
    );
    net.drain();
    assert_eq!(net.nodes[2].datagrams.len(), 1);
}
