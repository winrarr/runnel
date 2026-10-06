# Reusing a publish request ID with changed content

- Status: contract accepted in [ADR 0034](../decisions/0034-publish-request-id-content-contract.md); runtime implementation remains open
- Last reviewed: 2026-10-06
- Baseline: `ebcf6624809caa784aed7823d69886dc133f8c64`
- Related outcome: [Make client interactions dependable and evolvable](../backlog.md#make-client-interactions-dependable-and-evolvable)
- Related decisions: [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md), [ADR 0026](../decisions/0026-semantic-engine-error-classification.md), and [ADR 0029](../decisions/0029-local-typed-dead-letter-move-identities.md)

This note compares the current local and clustered behavior with two
reference approaches. The accepted Runnel contract is recorded in ADR 0034;
this note does not change runtime behavior or make a compatibility promise.

## Current Runnel behavior

**Local engine:** [`Broker::publish_with_request_id`](../../crates/runnel-core/src/broker.rs#L171)
looks up the public ID in the stream's request index and returns its stored
offset before comparing the incoming key or payload. The request-aware log
stores the ID, key bytes, and payload. Its record reader represents a
zero-byte key as `None`, so it does not distinguish an absent key from
`Some("")`. The local tests
[`repeated_request_id_returns_original_offset_without_appending`](../../crates/runnel-core/src/lib.rs#L451)
and [`request_id_deduplication_survives_restart`](../../crates/runnel-core/src/lib.rs#L551)
assert that even changed key and payload return the original offset, both
before and after restart.

**Clustered engine:** replicated state maps `dedup[stream][request_id]` to an
offset and stores each message's key, payload, and generated timestamp in
stream state. [`Command::Publish`](../../crates/runnel-raft/src/state_machine.rs#L301)
returns the mapped offset before comparing incoming message fields. The dedup
map and stream messages are persisted in state-machine and snapshot state.
The real-process test
[`three_process_cluster_replicates_and_recovers_after_failures`](../../crates/runnel-server/tests/cluster_smoke.rs#L175)
retries identical content from another node; it does not exercise a mismatch.
The mismatch behavior follows from the state-machine branch, but lacks
focused cluster process coverage.

In both engines, public request IDs are scoped per stream and have no
time-based window. The current local log and clustered state retain request
identity with message history; no message-retention policy defines an expiry
boundary. ADR 0029 separately distinguishes local public IDs from internal
dead-letter move IDs.

The [client README](../../crates/runnel-client/README.md#run-the-application-example)
guides callers to reuse one ID for a logical publish and retry with the same
bytes. ADR 0004 says a stable ID returns the original offset but leaves changed
inputs undefined. Existing local tests and a code comment intentionally
preserve first-use-wins behavior. These are evidence of implementation
intent, not a prior cross-engine contract decision.

## Reference behavior

**Stripe API:** Stripe compares parameters for a reused idempotency key and
errors if they differ. It saves a result once endpoint execution begins and
may prune keys after at least 24 hours, after which reuse starts a new
request. Validation failures and concurrent execution conflicts are not saved
as idempotent results. This makes changed input visible, but its HTTP
operation-result boundary and pruning policy do not prescribe Runnel's
retained-message lifetime. See
[Stripe's idempotent request contract](https://docs.stripe.com/api/idempotent_requests).

**NATS JetStream:** current documentation describes `Nats-Msg-Id` duplicate
suppression within a stream's duplicate window and acknowledges a duplicate
with the original sequence. The NATS maintainers' archived model deep dive
gives a specific example with repeated IDs and different bodies, says the
implementation consults only the message ID, and documents a two-minute
default window. This is a closer broker-publish comparison, but its
configurable sliding window differs from Runnel's current retained mapping.
See [current JetStream publishing guidance](https://docs.nats.io/learn/jetstream/publishing#avoiding-duplicate-writes),
and the [archived ID-only example](https://github.com/nats-io/nats.docs/blob/master/using-nats/jetstream/model_deep_dive.md#message-deduplication).

These references demonstrate viable but different policies. They do not
establish a universal rule or determine Runnel's ID lifetime.

## Runnel-specific decision and consequences

ADR 0034 accepts exact-input retries and confirmed rejection for changed
content while a `(stream, request_id)` mapping remains retained. Comparison
uses exact decoded payload bytes and key bytes; an absent key and an empty key
are equivalent because the existing local record format decodes both as zero
key bytes. This is a request-ID comparison limitation, not a general claim
that the inputs have the same ordering semantics: `Some("")` can express an
ordering key and `None` does not. The accepted conflict guarantee applies to
payload differences and key differences the current local format can
represent. A future format may preserve the key-presence distinction, but
that change is not part of this decision. Non-empty keys are compared
byte-for-byte. The broker-generated timestamp is excluded. Equal retries
return the original offset; a differing representable key or payload appends
no message and must surface as a `Rejected` semantic outcome. Exact wire codes
remain part of the provisional protocol and are not selected by the ADR.

This choice prevents a confirmed offset from being mistaken for acceptance of
different content. It retains exact retry resolution after a lost response
and applies independently to each record in the ordered publish-batch result.
It does not make publish batches atomic. Request IDs stay stream-scoped.
Their lifetime remains tied to retained deduplication state: no TTL or future
retention policy is accepted. Any retention change must keep enough original
input to compare a retained identity or remove its message and identity
coherently.

No persistent-format conversion is needed for currently retained messages:
the local request-aware record has the content fields used by the contract,
with zero-byte keys canonicalized as described above; clustered persisted
state carries both the offset mapping and message. Old binaries, downgrade,
and broader upgrade compatibility remain undefined. A local duplicate lookup
may require reading the original payload, while the clustered engine can
compare its materialized message. This potential cost has not been measured
and is not a performance claim.

## Remaining evidence and planning disposition

- Local and clustered engines need shared exact-retry and mismatch tests for
  representable key-only and payload-only differences, the absent/empty key
  comparison edge, binary bytes, stream scope, and IDs that survive restart.
  The tests must keep the distinct ordering-key intent visible.
- Clustered real-process coverage needs a mismatch through a follower and
  after leader change or restart. State-machine and snapshot recovery checks
  must prove the retained message remains available for comparison.
- Publish-batch tests need duplicate IDs in one ordered batch, mixed exact and
  conflicting records, per-record rejection, and continued outcomes for
  independent records. Typed clients need to classify this as a rejected
  result. Existing timeout and lost-response tests should continue to resolve
  exact retries to the original offset.
- Update client documentation to say that retry preserves the original key
  and payload bytes. Wire vocabulary, storage compatibility, and the ID's
  lifetime under future retention remain open implementation or planning work.

The client-interactions backlog outcome remains open. Its progress now records
that the semantic contract is accepted while the old first-use-wins runtime,
wire mapping, and end-to-end mismatch tests remain unimplemented. No tracker
item was added: this decision clarifies an existing child outcome rather than
establishing a separate product outcome or implementation shortcut.
