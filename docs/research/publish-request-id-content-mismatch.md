# Reusing a publish request ID with changed content

- Status: contract accepted in [ADR 0034](../decisions/0034-publish-request-id-content-contract.md) and implemented across local and clustered engines; the wire vocabulary remains provisional
- Last reviewed: 2026-10-06
- Baseline: `ebcf6624809caa784aed7823d69886dc133f8c64`
- Related outcome: [Make client interactions dependable and evolvable](../backlog.md#make-client-interactions-dependable-and-evolvable)
- Related decisions: [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md), [ADR 0026](../decisions/0026-semantic-engine-error-classification.md), and [ADR 0029](../decisions/0029-local-typed-dead-letter-move-identities.md)

This note compares local and clustered behavior at the recorded baseline with
two reference approaches, then records the implementation. The accepted
Runnel contract is recorded in ADR 0034; its implementation does not make a
compatibility promise.

## Behavior at research baseline

**Local engine:** at the supplied baseline, [`Broker::publish_with_request_id`](../../crates/runnel-core/src/broker.rs#L171)
looks up the public ID in the stream's request index and returns its stored
offset before comparing the incoming key or payload. The request-aware log
stores the ID, key bytes, and payload. Its record reader represents a
zero-byte key as `None`, so it does not distinguish an absent key from
`Some("")`. At that baseline, local tests asserted that changed key and
payload returned the original offset, before and after restart.

**Clustered engine:** at the supplied baseline, replicated state mapped
`dedup[stream][request_id]` to an offset and stored each message's key,
payload, and generated timestamp in stream state.
[`Command::Publish`](../../crates/runnel-raft/src/state_machine.rs#L301)
returned the mapped offset before comparing incoming message fields. The
dedup map and stream messages were persisted in state-machine and snapshot
state. Its real-process coverage retried identical content from another node
but did not exercise a mismatch.

In both engines, public request IDs are scoped per stream and have no
time-based window. The current local log and clustered state retain request
identity with message history; no message-retention policy defines an expiry
boundary. ADR 0029 separately distinguishes local public IDs from internal
dead-letter move IDs.

The client README at the baseline guided callers to reuse one ID for a logical
publish and retry with the same bytes. ADR 0004 said a stable ID returns the
original offset but left changed inputs undefined. Existing baseline local
tests and a code comment intentionally preserved first-use-wins behavior.
These were evidence of implementation intent, not a prior cross-engine
contract decision.

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

## Runtime implementation evidence

The accepted contract is implemented in the local engine's request-aware log
lookup and the clustered engine's replicated publish transition. Both compare
the stored key representation and exact payload bytes before returning the
original offset. A mismatch produces the engine kind
`RequestIdContentConflict`, classified as `Rejected`, and maps through the
provisional v1 wire code `request_id_content_conflict`. The clustered mapping
is preserved when a follower forwards a publish to the leader. If the
comparison record cannot be read locally, or a clustered dedup entry has no
corresponding retained message, the operation fails closed as an error rather
than accepting a new publish or claiming an exact retry.

The shared engine contract checks exact retry and absent/empty-key comparison,
key-only and payload-only conflicts, stream scope, no offset allocation or
consumer-state change, per-record ordered publish-batch outcomes, and
concurrent conflicting reuse. A typed-client real-server test checks rejected
classification, batch outcomes, and persistence across restart. The
three-process test sends a mismatch through a follower and repeats it after a
leader change; the expected next ordinary publish confirms that the conflict
did not consume an offset. These checks establish semantic behavior at the
tested broker boundaries; they do not establish power-loss behavior or a
cross-release compatibility promise.

The local duplicate path reads the original payload to establish equality.
That adds work to repeated-ID publishes, but no performance claim is made and
the cost has not been measured. The request-ID retention lifetime remains
unresolved until a message-retention policy is selected. No separate backlog
or tech-debt item is needed: this implementation fulfills the accepted child
contract under the existing client-interactions outcome, while those broader
compatibility and retention questions remain tracked there.

No persistent-format conversion is needed for currently retained messages:
the local request-aware record has the content fields used by the contract,
with zero-byte keys canonicalized as described above; clustered persisted
state carries both the offset mapping and message. Old binaries, downgrade,
and broader upgrade compatibility remain undefined. A local duplicate lookup
may require reading the original payload, while the clustered engine can
compare its materialized message. This potential cost has not been measured
and is not a performance claim.

## Remaining evidence and planning disposition

The focused engine, local persistence, typed-client, and real-process cluster
checks cover the accepted behavior. Existing ambiguous-outcome coverage still
needs to remain green so a conflict is never substituted for an unresolved
publish result. Snapshot-installation and state-machine corruption paths are
fail-closed by construction but do not each have a new conflict-specific
real-process scenario. Actual storage/device failure and power-loss behavior
are outside these checks.

The client-interactions backlog outcome remains open for a versioned
interoperability contract, external-application evidence, and a retention
lifetime decision. No additional tracker item was added: the accepted child
behavior is implemented, and the remaining questions are already in scope of
that parent outcome. The provisional wire code and lack of a compatibility
promise should be revisited with protocol versioning, not treated as stable
client API.
