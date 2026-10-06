# ADR 0034: Reject mismatched reuse of publish request IDs

- Status: accepted
- Date: 2026-10-06
- Revalidated against baseline: `ebcf6624809caa784aed7823d69886dc133f8c64`
- Primary evidence class: design/research
- Related outcome: [Make client interactions dependable and evolvable](../backlog.md#make-client-interactions-dependable-and-evolvable)
- Research: [Reusing a publish request ID with changed content](../research/publish-request-id-content-mismatch.md)
- Related decisions: [ADR 0004](0004-multi-raft-first-distributed-engine.md), [ADR 0026](0026-semantic-engine-error-classification.md), [ADR 0029](0029-local-typed-dead-letter-move-identities.md), and [ADR 0030](0030-consume-batch-contract.md)

## Context

At this baseline, the local engine indexes each public request ID to its first
offset in a stream and returns that offset before comparing a retry's key or
payload. Its request-aware log records the ID, key bytes, and payload, and
decodes an empty key as absent; local tests assert first-use-wins behavior even
when a retry changes both fields and after restart. The clustered state
machine likewise indexes IDs to offsets, returns the earlier offset before
examining new content, and persists both the index and the stream messages in
state and snapshots. Existing cluster process coverage retries identical
content, but does not exercise a mismatch. Neither behavior has yet been
chosen as a cross-engine contract.

The client guidance says to retry with the same ID and bytes. ADR 0004
establishes that stable IDs resolve retries to their original offset, but does
not define content equality. ADR 0026 already distinguishes confirmed
rejection from retryable and unknown failures; this decision uses that
semantic outcome boundary without selecting a wire code. ADR 0029 keeps local
dead-letter move IDs separate from caller-supplied public IDs; this decision
applies only to the latter.

Stripe rejects reuse of an idempotency key when operation parameters differ,
while NATS JetStream recognizes message IDs within a duplicate window without
comparing message bodies. These are useful alternatives, not direct Runnel
contracts: Stripe's HTTP result caching and key pruning differ from retained
message storage, and JetStream's configurable finite window differs from
Runnel's current persisted mapping. The
[source-backed comparison](../research/publish-request-id-content-mismatch.md#reference-behavior)
records the limits of that comparison.

## Decision

Treat a public publish identity as the pair `(stream, request_id)`. While that
identity remains in the stream's retained deduplication state:

- Equality is exact equality of the message key's UTF-8 bytes and the opaque
  payload bytes after protocol decoding. For key comparison, an absent key and
  an empty string both have zero key bytes and are equivalent; this matches
  the existing local durable representation, which cannot distinguish them.
  This is a storage limitation and semantic edge for request-ID comparison:
  `Some("")` can express ordering intent while `None` does not, so this rule
  does not make them generally equivalent messages or change ordering
  semantics. The accepted conflict guarantee applies to payload differences
  and key differences the current local format can represent. A future format
  may preserve key presence, but this ADR does not require or accept that
  change. Non-empty keys are compared byte-for-byte, without case folding or
  Unicode normalization. Payload bytes are not parsed or normalized. The
  broker-generated publish timestamp is excluded. If the public message model
  gains another caller-supplied, persisted field that affects the message,
  its equality semantics must be specified before that field is accepted.
- Reusing the identity with equal comparison inputs is a content-equivalent
  retry. Return the original offset as confirmed success and append no second
  stream record.
- Reusing the identity with different canonical key bytes or payload bytes is
  a confirmed rejection with a specific request-ID content-conflict semantic
  category.
  Append no stream record, allocate no stream message offset, replace no
  identity mapping, and change no consumer state. The clustered engine may
  still carry the request through its existing replicated-command path to
  order the comparison. The retry cannot resolve that conflict by repeating
  the changed input; use the original inputs to resolve an ambiguous earlier
  publish, or use a new request ID for a genuinely new message.
- A publish without a request ID is unchanged. Public IDs remain scoped to one
  stream; using the same string on another stream is independent. Internal
  dead-letter move identities remain governed by ADR 0029.
- The conflict is evaluated in the same per-stream serialized ordering as
  accepted publishes. For concurrent requests using one identity, the first
  request that crosses the engine's existing acceptance boundary determines
  the stored content. Content-equivalent later requests return its offset;
  differing later requests are rejected. A request that fails before
  acceptance does not claim the identity.
- Initial publish success retains each engine's existing durability boundary:
  local confirmation follows its durable log append, and clustered
  confirmation follows quorum commit and durable state-machine application.
  An exact retry resolves to the already accepted record. The engine may
  report the content conflict only when its serialized lookup proves that the
  incoming record was not appended. Failures that do not establish a safe
  outcome retain the existing conservative retryable or unknown
  classification; they are not converted into a conflict response. In the
  clustered engine, conflict confirmation follows the existing committed and
  applied command boundary.

The semantic rejection is distinct from a malformed request. A later runtime
change should expose it as a stable engine-level kind with the `Rejected`
outcome under ADR 0026. The exact v1 error code, response shape, and typed
client variant are not selected here: the current protocol vocabulary remains
provisional, and protocol-versioning work owns that representation. This
decision does not add a compatibility guarantee.

The same semantic unit applies to a record published alone or as one member of
`publish_batch`. Batch outcomes stay ordered and per record; this decision does
not make a batch all-or-nothing. An earlier record in a batch can establish an
identity for a later record: a content-equivalent later duplicate returns
that offset and a changed-content duplicate is rejected. Other records keep
their own outcomes. This is separate from the consume-batch runtime work and
does not alter consume-batch semantics.

The request-ID lifetime remains tied to retained deduplication state. Runnel
currently has no request-ID expiry window or message-retention policy, so
there is no finite lifetime selected here. While a mapping is retained, the
engine must retain enough original input to compare the canonical key bytes
and exact payload bytes. Any future retention or expiry decision must define
how it preserves that comparison or removes the message and its identity
coherently. Once an identity is deliberately removed under a future policy,
subsequent reuse is a new publish; this ADR sets neither that policy nor its
timing.

## Rationale and alternatives

Returning the first offset for every reuse is cheap and matches JetStream's
identity-only model, but a successful response can be mistaken for acceptance
of content the broker never stored. Rejecting a mismatch makes accidental ID
reuse visible while preserving the existing recovery path for exact retries.
It also gives callers a clear action: repeat the original request to resolve
an ambiguous success, or choose a new ID for different content.

This decision does not require a new durable content fingerprint while the
original record remains retained. Locally, the request-aware log associates
the ID and offset with the persisted key and payload; the comparison can read
that record. Clustered state already stores the original message by offset
alongside its dedup map, including in persisted snapshots, so the replicated
transition can make the same deterministic comparison. The local duplicate
path may need an additional payload read, while the clustered path compares
materialized state. Their costs have not been measured; no performance claim
or optimization is implied. If future retention removes payloads while
retaining IDs, it must retain sufficient comparison data or change the
identity lifetime in the same accepted retention policy.

The alternatives considered were:

1. **Keep first use wins for all content.** This preserves existing mismatch
   behavior and a cheap lookup, but knowingly hides changed-input reuse from
   clients. Rejected because that confirmed offset describes a different
   record from the one represented by the incoming request.
2. **Bind identity to a caller-derived content ID.** This shifts canonical
   encoding, hash choice, and collision handling to clients and does not
   report accidental reuse of an explicitly supplied ID. It also changes the
   meaning of the existing request-ID field. Rejected.
3. **Expire IDs after a fixed or sliding interval.** Stripe and JetStream
   show viable expiry policies, but the current Runnel retained-storage model
   has no accepted retention contract from which to choose a duration.
   Deferred to that retention decision. Until then, an existing mapping
   remains effective for its retained lifetime.

## Consequences and implementation evidence

- Local and clustered engines must return the same offset or confirmed
  conflict for exact and changed-content retries, including when routed
  through a follower and after restart, snapshot installation, or leader
  change. Missing or corrupt comparison data must fail closed; it must not be
  treated as a new identity or an exact match.
- Existing persisted IDs need no conversion while their original messages
  remain available: the local request-aware record carries the fields needed
  for comparison, and clustered state retains both the offset mapping and
  message. Old first-use-wins records remain stored as written; after upgrade,
  their original content is the comparison authority. This does not promise
  compatibility with older binaries, migration, downgrade, or a particular
  future storage format.
- Runtime implementation now replaces the prior first-use-wins mismatch
  behavior. The shared engine contract covers exact retries, non-empty
  key-only and payload-only conflicts, absent/empty key comparison, binary
  payloads, stream scope, concurrent reuse, and no offset/consumer-state
  mutation while preserving distinct ordering-key intent. Separate local and
  real-server tests cover persistence across restart.
- Publish-batch coverage checks mixed records, duplicate IDs in an ordered
  batch, exact retry offsets, per-record conflict rejection, and continuation
  of independent records. Typed-client real-server coverage checks rejected
  classification and local restart. Three-process coverage exercises a
  mismatch through a follower and after leader change. These tests establish
  correctness at the tested boundaries, not batch atomicity or power-loss
  behavior.
- Client guidance now requires retries to preserve the original key and
  payload bytes. The wire code and representation remain provisional and do
  not establish compatibility across releases.

The remaining material risks are the unselected lifetime under future
retention and the local duplicate-path comparison cost. The former must be
resolved with the retention policy before message pruning is implemented; the
latter needs measurement only if duplicate-retry cost is material. The
contract is accepted and implemented; broader client interoperability and
retention evidence remain open in the client-interactions backlog outcome.

## References

- [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests)
- [NATS JetStream publishing and duplicate acknowledgements](https://docs.nats.io/learn/jetstream/publishing#avoiding-duplicate-writes)
- [NATS maintainers' archived model deep dive: message-ID-only deduplication](https://github.com/nats-io/nats.docs/blob/master/using-nats/jetstream/model_deep_dive.md#message-deduplication)
