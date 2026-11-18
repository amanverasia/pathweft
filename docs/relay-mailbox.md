# Relay mailbox prototype

Alice sends, a designated relay stores, and Bob collects automatically when his node is running. The prototype supports direct loopback connections and paths discovered over configured neighbour links. Both modes use privately provisioned credentials.

## Run the prototype

Run `python3 scripts/mailbox_demo.py` from the repository root for a local proof. It uses isolated state directories and simulates lost replies between the processes. Run `python3 scripts/mailbox_demo.py --discovered` for the five-process proof with transit nodes on both sides of the mailbox.

To configure your own three processes:

1. Add `"mailbox": {"trusted_wall_clock": true}` to the relay's config and give it a fixed loopback listen port. Set `max_wait_ms` to change the relay's 24-hour holding limit. Set `trusted_wall_clock` only on a host that can track elapsed time across restarts.
2. On Bob's node, use `pathweft identity new --out pickup.key` to create a separate private pickup identity, and `pathweft mailbox new-label` to create a label for Alice. On the relay host, run `pathweft mailbox register --socket relay.sock --label <label> --public-key <pickup-public-key>`.
3. Add the following to Bob's config, substituting the correct address, key and label. Keep `pickup.key` private to Bob. Alice and Bob must pin each other's ordinary contact keys.

   ```json
   {
     "pickup": {
       "relay": {"address": "127.0.0.1:4501", "public_key": "<relay-public-key>"},
       "identity": "pickup.key",
       "labels": ["<label>"]
     }
   }
   ```

4. On Alice's node, run `pathweft mailbox queue --socket alice.sock --to <bob-id> --message "hello" --relay-address 127.0.0.1:4501 --relay-key <relay-public-key> --label <label>`. Use `--ttl-seconds` to change the 24-hour default. `pathweft outbox --socket alice.sock list` shows the relay's storage result and Alice's delivery status.
5. Start Bob's node and check `pathweft inbox --socket bob.sock list`. The prototype uses the ordinary receipt path. Add a route from Bob to Alice to get a delivery receipt. Alice can also use an ordinary route to Bob to request a missing receipt.

## Use discovered paths

1. Configure only adjacent `links` on the nodes. Alice and Bob pin each other as ordinary contacts for message authentication and end-to-end receipts.
2. Generate a separate route identity for Alice with `pathweft identity new --out mailbox-route.key`. Add `"mailbox_route": {"relay_key": "<relay-public-key>", "identity": "mailbox-route.key"}` to her node config. Bob can use his existing pickup identity for the same purpose by setting the filename to `pickup.key`.
3. Add the public route keys for Alice and Bob to `mailbox_route_contacts` on the storage relay. The relay pins those keys for discovery and associates each registered deposit label with Bob's pickup public key. The relay can link repeated traffic from a provisioned route key.
4. Leave the address out of Bob's `pickup.relay` configuration and omit `--relay-address` from Alice's `pathweft mailbox queue` command. The normal receipt path can also be discovered when both contacts are reachable.

## Deposit, pickup and confirmation

1. **Prepare a mailbox.** Bob chooses the relay and pins its public key. Pathweft creates a mailbox key pair and a deposit label for Alice. Bob keeps the private key and shares only the relay details and deposit label with Alice. For this prototype, provision the relay with the label and mailbox public key through its private local control interface. The relay gains no permanent personal key from this provisioning record. Unregistered labels are rejected before storage is allocated.
2. **Deposit while Bob is away.** Alice's outbox sends one encrypted copy to the relay with the label, a random copy ID and the remaining waiting allowance. Wrap the existing authenticated message envelope in a sealed box to Bob so the relay cannot read the endpoint IDs from the envelope. Persist the exact copy ID and sealed bytes before sending so retries can reuse both. The relay checks bounds, clears expired copies and either persists the new copy or declines it. The reply means the relay accepted storage and Alice keeps her outgoing message pending.
3. **Pick up.** Bob's service checks its configured relay when it has a path and on a paced schedule while it stays connected. The relay checks the mailbox credential before serving copies. Bob opens each copy, verifies Alice using the existing pinned-contact policy and passes the original message ID and envelope to the durable inbox. Receiving the same valid message again reuses the saved record.
4. **Confirm saving.** After the inbox has saved or recognized the retained message, Bob acknowledges that specific copy using the mailbox credential. The relay checks the acknowledgment and persists deletion. The sender still needs the ordinary end-to-end receipt to mark its outgoing message delivered. If the receipt has no path back, delivery status stays pending until it can be confirmed. Receipt recovery continues to use the original message ID.

## Credential and copy binding

The prototype uses existing libsodium operations. The recipient generates a separate key pair for collection, and the relay uses authenticated boxes between that key and its own pinned key. Each exchange binds the operation, mailbox label and fresh challenge in the protected data. Each acknowledgment must match the copy served on that connection. Network enrollment, revocation and rotation need a separate design before general deployment.

Deposit labels allow submission only. Each deposit exchange uses an ephemeral key with the relay's pinned key and authenticates the reply against the challenge and copy ID. Collection and deletion need Bob's mailbox credential even when someone else learns the label. An authenticated acknowledgment expresses Bob's approval to delete one copy. The recipient saves the copy durably before acknowledging it.

The relay can see a label, the associated mailbox public key, copy sizes and activity timing. It can correlate repeated operations on a label and infer relationships between labels collected together. The sealed copy protects the original message and endpoint IDs from the mailbox relay. Manually routed end-to-end receipts can disclose endpoint IDs to nodes along that route.

## Lifetime, retries and bounds

- **Waiting allowance.** Apps can choose from one second to seven days, with 24 hours as the default. Each retry carries the remaining allowance without extending a previously accepted copy's deadline. The relay uses elapsed time and, when configured, a trusted wall clock to account for downtime. On an untrusted restart or clock rollback behind the last saved state, the relay expires previous payloads and retains bounded duplicate-suppression records.
- **Duplicate deposits.** Identical repeats of a live copy return the existing acceptance. The relay rejects conflicts and keeps a compact record after delivery or local expiry through the original sender allowance estimated on admission. That record suppresses a delayed deposit retry even if the relay's holding limit was shorter. Replay protection lasts while the record is retained.
- **Per-label and relay-wide caps.** Each label can have eight live copies and 32 records including retired copies. The relay has 128 registered labels and 128 records total, with a 4 MiB encoded storage ceiling. Each page carries one copy of up to 16 KiB sealed. Tests check that its protected wire frame fits within 32 KiB.
- **Pickup scheduler.** One worker checks up to 16 labels for the configured relay in rotation. It checks at startup, waits 250 ms after a page or two seconds after an empty page, and backs off from four to 30 seconds on failures. A scheduled cycle can request discovery if the route is missing.
- **Hops.** Direct deposit and pickup each use one handoff. For discovery, the relay records the measured deposit hops on first admission. Each intermediate node subtracts one hop, and the relay sends the copy to the recipient with only its remaining allowance. The recipient checks the combined count against the original sealed budget before saving. A relay that falsifies the mutable hop count can misreport distance.
- **Exchange bounds.** A routed exchange uses up to 32 local session slots, one buffered response per session and the same bounded authenticated messages as direct TCP. Transit nodes forward sealed data and have no mailbox session state.

Outbox version 4 keeps the stable sealed copy and original lifetime for retries across sender restarts. It reads versions 1 through 3. The preexisting direct outbox path retains its 24-hour maximum and current defaults.

## Acceptance tests

| Case | Expected result |
|---|---|
| Bob is offline when Alice deposits. The relay restarts with a trusted clock. Bob starts later. | The same sealed copy survives and automatic pickup saves exactly one durable inbox record. |
| Bob stops after receiving a copy but before saving it, or the inbox rejects it. | The relay still holds the copy. No deletion acknowledgment or endpoint delivery receipt is sent. |
| Bob saves the message and the acknowledgment is lost. | A later pickup deduplicates the message and can acknowledge it again. |
| A client has Alice's deposit label but supplies the wrong mailbox key or repeats an old challenge. | Pickup and deletion fail, and the record stays. |
| A valid acknowledgment refers to another label or copy ID. | The other copy stays. A duplicate valid acknowledgment is harmless. |
| The label or relay reaches its quota. | Expired copies are cleared first. Accepted unexpired copies stay and an oversized or excess deposit is declined. Alice's outbox retains the message. |
| The same deposit returns after delivery or a retry arrives immediately before expiry. | The copy is suppressed while its deletion record is retained. A repeat never resets the original deadline. |
| Connectivity repeatedly changes or the relay is busy. | Pickup requests respect the configured pacing, and only one cycle runs at a time. |
| A relay examines its stored records or normal logs. | Records contain opaque routing metadata and sealed bytes. They exclude message text, permanent personal IDs and private keys. |

## Verification and next scope

The three-process test covers offline deposit, abrupt sender and relay restarts, wrong pickup credentials, rejection before inbox save, lost save acknowledgment, inbox deduplication after recipient restart, end-to-end receipt, and relay metadata checks. The five-process test covers discovered routes, offline deposit and pickup through different transit nodes, relay restart, four counted hops and an end-to-end receipt without manual routes. Rust tests cover copy and challenge binding, store limits, expiry, clockless restart, pacing intervals and frame size. Real transport and Android mailbox validation are separate.
