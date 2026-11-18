use super::*;
use uuid::Uuid;

fn message(a: &Identity, c: &Identity) -> Envelope {
    a.seal(
        c.public_key(),
        Payload::Text {
            text: "secret test text".into(),
        },
    )
    .unwrap()
}

#[test]
fn encrypted_roundtrip_and_private_key_import() {
    let a = Identity::generate().unwrap();
    let c = Identity::generate().unwrap();
    let restored = Identity::from_secret(*a.secret_bytes()).unwrap();
    assert_eq!(a.id(), restored.id());
    let packet = message(&a, &c);
    assert!(!packet.ciphertext.windows(6).any(|w| w == b"secret"));
    assert_eq!(
        c.open(a.public_key(), &packet).unwrap(),
        Payload::Text {
            text: "secret test text".into()
        }
    );
}

#[test]
fn relay_cannot_decrypt_and_tampering_fails() {
    let a = Identity::generate().unwrap();
    let b = Identity::generate().unwrap();
    let c = Identity::generate().unwrap();
    let packet = message(&a, &c);
    assert!(b.open(a.public_key(), &packet).is_err());
    let mut retargeted = packet.clone();
    retargeted.destination = b.id();
    assert!(b.open(a.public_key(), &retargeted).is_err());
    let mut tampered = packet.clone();
    tampered.ciphertext[0] ^= 1;
    assert!(c.open(a.public_key(), &tampered).is_err());
    let mut changed_id = packet.clone();
    changed_id.id = Uuid::new_v4();
    assert!(matches!(
        c.open(a.public_key(), &changed_id),
        Err(Error::HeaderMismatch)
    ));
    let mut changed_nonce = packet.clone();
    changed_nonce.nonce[0] ^= 1;
    assert!(c.open(a.public_key(), &changed_nonce).is_err());
    assert!(c.open(b.public_key(), &packet).is_err());
}

#[test]
fn bounds_and_key_validation() {
    let a = Identity::generate().unwrap();
    let c = Identity::generate().unwrap();
    for size in [0, MAX_TEXT_BYTES + 1] {
        assert!(
            a.seal(
                c.public_key(),
                Payload::Text {
                    text: "a".repeat(size)
                }
            )
            .is_err()
        );
    }
    assert!(
        a.seal(
            c.public_key(),
            Payload::Text {
                text: "a".repeat(MAX_TEXT_BYTES)
            }
        )
        .is_ok()
    );
    assert!(
        a.seal(
            a.public_key(),
            Payload::Receipt {
                message_id: Uuid::new_v4()
            }
        )
        .is_err()
    );
    assert!(Identity::from_secret([0; 32]).is_err());
    assert!("abc".parse::<PublicKey>().is_err());
    let mut packet = message(&a, &c);
    packet.version += 1;
    assert!(packet.validate().is_err());
    packet.version = VERSION;
    packet.remaining_hops = MAX_HOPS + 1;
    assert!(packet.validate().is_err());
    packet.remaining_hops = 1;
    packet.ciphertext.resize(MAX_CIPHERTEXT_BYTES + 1, 0);
    assert!(packet.validate().is_err());
}

#[test]
fn routing_budget_duplicate_and_unconfirmed_delivery() {
    let a = Identity::generate().unwrap();
    let b = Identity::generate().unwrap();
    let c = Identity::generate().unwrap();
    let mut relay = Router::new(b.id(), true);
    relay.set_route(c.id(), "C").unwrap();
    let mut packet = message(&a, &c);
    packet.remaining_hops = 1;
    let mut duplicate = packet.clone();
    assert_eq!(
        relay.route(&mut packet, Origin::Network).unwrap(),
        Action::Forward("C")
    );
    assert_eq!(packet.remaining_hops, 0);
    assert_eq!(
        relay.route(&mut duplicate, Origin::Network).unwrap(),
        Action::Drop(DropReason::Duplicate)
    );
    let mut destination = Router::<&str>::new(c.id(), false);
    assert_eq!(
        destination.route(&mut packet, Origin::Network).unwrap(),
        Action::Deliver
    );
    assert_eq!(
        destination.route(&mut packet, Origin::Network).unwrap(),
        Action::Deliver
    );
    c.open(a.public_key(), &packet).unwrap();
    assert!(destination.confirm_delivery(packet.id));
    assert!(!destination.confirm_delivery(packet.id));
    assert_eq!(
        destination.route(&mut packet, Origin::Network).unwrap(),
        Action::Drop(DropReason::Duplicate)
    );
    let mut exhausted = message(&a, &c);
    exhausted.remaining_hops = 0;
    assert_eq!(
        relay.route(&mut exhausted, Origin::Network).unwrap(),
        Action::Drop(DropReason::HopsExhausted)
    );
}

#[test]
fn opt_out_preserves_endpoint_use_and_no_route_is_explicit() {
    let a = Identity::generate().unwrap();
    let b = Identity::generate().unwrap();
    let c = Identity::generate().unwrap();
    let mut router = Router::new(b.id(), false);
    router.set_route(c.id(), "C").unwrap();
    assert_eq!(
        router.route(&mut message(&a, &c), Origin::Network).unwrap(),
        Action::Drop(DropReason::RelayDisabled)
    );
    assert_eq!(
        router.route(&mut message(&b, &c), Origin::Local).unwrap(),
        Action::Forward("C")
    );
    assert_eq!(
        router.route(&mut message(&a, &b), Origin::Network).unwrap(),
        Action::Deliver
    );
    let mut other = Router::<&str>::new(a.id(), true);
    assert_eq!(
        other.route(&mut message(&a, &c), Origin::Local).unwrap(),
        Action::Drop(DropReason::NoRoute)
    );
    assert!(other.route(&mut message(&b, &c), Origin::Local).is_err());
}

#[test]
fn unsent_local_resume_uses_current_route_but_network_duplicates_still_drop() {
    let a = Identity::generate().unwrap();
    let b = Identity::generate().unwrap();
    let mut router = Router::new(a.id(), false);
    router.set_route(b.id(), "old").unwrap();
    let original = message(&a, &b);
    let mut first = original.clone();
    assert_eq!(
        router.resume_local(&mut first).unwrap(),
        Action::Forward("old")
    );
    router.set_route(b.id(), "new").unwrap();
    let mut resumed = original.clone();
    assert_eq!(
        router.resume_local(&mut resumed).unwrap(),
        Action::Forward("new")
    );
    assert_eq!(resumed.remaining_hops, DEFAULT_HOPS - 1);
    assert_eq!(
        router.route(&mut resumed, Origin::Network).unwrap(),
        Action::Drop(DropReason::Duplicate)
    );
    assert!(router.resume_local(&mut message(&b, &a)).is_err());
}

#[test]
fn receipt_queries_are_authenticated_and_content_hash_ignores_only_hop_budget() {
    let a = Identity::generate().unwrap();
    let b = Identity::generate().unwrap();
    let id = Uuid::new_v4();
    let query = a
        .seal(b.public_key(), Payload::ReceiptRequest { message_id: id })
        .unwrap();
    assert_eq!(
        b.open(a.public_key(), &query).unwrap(),
        Payload::ReceiptRequest { message_id: id }
    );
    assert!(
        a.seal(
            b.public_key(),
            Payload::ReceiptRequest {
                message_id: Uuid::nil()
            }
        )
        .is_err()
    );
    let hash = query.content_hash();
    let mut changed = query.clone();
    changed.remaining_hops = 0;
    assert_eq!(hash, changed.content_hash());
    changed.nonce[0] ^= 1;
    assert_ne!(hash, changed.content_hash());
    changed = query.clone();
    changed.ciphertext[0] ^= 1;
    assert_ne!(hash, changed.content_hash());
    assert!(b.open(a.public_key(), &changed).is_err());
}

#[test]
fn redelivery_wraps_maximum_text_without_changing_the_original_message() {
    let a = Identity::generate().unwrap();
    let b = Identity::generate().unwrap();
    let original = a
        .seal(
            b.public_key(),
            Payload::Text {
                text: "🌍".repeat(MAX_TEXT_BYTES / 4),
            },
        )
        .unwrap();
    let retry = a
        .seal(
            b.public_key(),
            Payload::Redelivery {
                packet: original.clone(),
            },
        )
        .unwrap();
    assert_ne!(retry.id, original.id);
    let Payload::Redelivery { packet } = b.open(a.public_key(), &retry).unwrap() else {
        panic!("wrong payload")
    };
    assert_eq!(packet, original);
    assert_eq!(
        b.open(a.public_key(), &packet).unwrap(),
        Payload::Text {
            text: "🌍".repeat(MAX_TEXT_BYTES / 4)
        }
    );
    let mut changed_budget = original.clone();
    changed_budget.remaining_hops -= 1;
    assert!(
        a.seal(
            b.public_key(),
            Payload::Redelivery {
                packet: changed_budget
            }
        )
        .is_err()
    );
}

#[test]
fn forwarding_cache_is_bounded() {
    let a = Identity::generate().unwrap();
    let b = Identity::generate().unwrap();
    let c = Identity::generate().unwrap();
    let mut router = Router::new(b.id(), true);
    router.set_route(c.id(), "C").unwrap();
    let mut packet = message(&a, &c);
    let first = packet.clone();
    for _ in 0..=crate::router::SEEN_CAPACITY {
        router.route(&mut packet, Origin::Network).unwrap();
        packet.id = Uuid::new_v4();
        packet.remaining_hops = DEFAULT_HOPS;
    }
    assert_eq!(
        router.route(&mut first.clone(), Origin::Network).unwrap(),
        Action::Forward("C")
    );
}

fn key_from(bytes: [u8; 32]) -> PublicKey {
    hex::encode(bytes).parse().unwrap()
}

#[test]
fn contact_card_round_trips_and_matches_independent_vector() {
    let a = key_from(std::array::from_fn(|i| i as u8 + 1));
    // Expected values computed with an independent Python implementation.
    let named = "pathweft:contact/1/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20/89cc/alice";
    let plain =
        "pathweft:contact/1/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20/7df4";
    let card = ContactCard::new(a, Some("alice".into())).unwrap();
    assert_eq!(card.to_string(), named);
    assert_eq!(ContactCard::parse(named).unwrap(), card);
    assert_eq!(ContactCard::new(a, None).unwrap().to_string(), plain);
    assert_eq!(ContactCard::parse(plain).unwrap().name, None);
}

#[test]
fn contact_card_rejects_hostile_or_damaged_input() {
    let good = "pathweft:contact/1/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20/89cc/alice";
    let bad = [
        "",
        "pathweft:contact/2/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20/89cc/alice", // version
        "pathweft:contact/1/0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20/89cc/alice", // uppercase
        "pathweft:contact/1/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f21/89cc/alice", // typo in key
        "pathweft:contact/1/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20/89cc/alicf", // name changed
        "pathweft:contact/1/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20/89cc", // name dropped
        "pathweft:contact/1/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20/89cc/al ice",
        "pathweft:contact/1/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20/89cc/alice/x",
        "pathweft:contact/1/0000000000000000000000000000000000000000000000000000000000000000/0000", // zero key
        "pathweft:contact/1/0102/89cc",
    ];
    for text in bad {
        assert!(ContactCard::parse(text).is_err(), "accepted: {text}");
    }
    let long = format!("{good}{}", "x".repeat(MAX_CARD_CHARS_FOR_TEST));
    assert!(ContactCard::parse(&long).is_err());
    assert!(
        ContactCard::new(
            Identity::generate().unwrap().public_key(),
            Some("x".repeat(33))
        )
        .is_err()
    );
}
const MAX_CARD_CHARS_FOR_TEST: usize = contact::MAX_CARD_CHARS;

#[test]
fn verification_code_is_symmetric_per_key_and_matches_independent_vector() {
    let a = key_from(std::array::from_fn(|i| i as u8 + 1));
    let b = key_from(std::array::from_fn(|i| i as u8 + 100));
    let code = verification_code(&a, &b).unwrap();
    assert_eq!(code, "66208 71023 81874 21548 40491 21067");
    assert_eq!(
        verification_code(&b, &a).unwrap(),
        code,
        "same on both devices"
    );
    assert!(verification_code(&a, &a).is_err());
    // Each half depends only on its own key: changing b leaves a's half unchanged.
    let c = key_from(std::array::from_fn(|i| i as u8 + 200));
    let with_c = verification_code(&a, &c).unwrap();
    assert_eq!(
        &with_c[..17],
        &code[..17],
        "a sorts first in both, so its 15-digit half is identical"
    );
    assert_ne!(&with_c[18..], &code[18..]);
    // Domain separation: the code is not just the node ID's digits.
    assert!(
        !code
            .replace(' ', "")
            .contains(&a.node_id().to_string()[..6])
    );
}
