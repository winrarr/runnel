# Public request IDs and dead-letter move identity

- Status: typed identity and local storage policy accepted in [ADR 0029](../decisions/0029-local-typed-dead-letter-move-identities.md); public request-ID content behavior accepted in [ADR 0034](../decisions/0034-publish-request-id-content-contract.md) and implemented across local and clustered engines
- Last reviewed: 2026-10-06
- Baseline: `f999c1b9ad5d22408bbbe6c6276a42e825cd62ef`
- Related debt: [TD-002](../tech-debt.md#td-002-one-file-and-a-startup-scan-per-local-stream) and [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records)
- Related design: [Dead-letter recovery across durable boundaries](../design/dead-letter-recovery.md)
- Related decisions: [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md), [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md), [ADR 0029](../decisions/0029-local-typed-dead-letter-move-identities.md), and [ADR 0034](../decisions/0034-publish-request-id-content-contract.md)

This note compares ways to distinguish a client-supplied publish identity from
the local engine's identity for one dead-letter move. The accepted typed
identity and storage decision is recorded in ADR 0029. Public request-ID
content equality is a separate contract accepted later in ADR 0034 and now
implemented across engines. The named
baseline describes the pre-change identity behavior; the implementation and
compatibility sections record the resulting local format and its consequences.

## Finding

At the supplied baseline, Runnel stored both kinds of identity as the same
request-ID string in one per-stream index. The local dead-letter append added a
key and payload comparison to avoid acknowledging the source against a
conflicting record, but that comparison could not establish who created a
matching record. PR #346 added baseline core and real-server coverage for this
same-content case; the tests are updated by this change to assert typed
identity behavior.

**Recommendation (inference accepted in ADR 0029):** persist a typed identity
domain for new local request-aware records. Keep caller-supplied IDs as public
IDs and local source-to-dead-letter IDs as moves. Keep the public request-ID
field and namespace distinct from internal move identity. The accepted
content-equivalence and mismatch outcomes are now defined by ADR 0034, and
runtime behavior follows that contract. A move retry reconciles only against
the same typed move identity
and still verifies key and payload before source progress advances.

The accepted implementation uses a new request-aware frame version to persist
the identity kind. Recovery indexes every untyped RNL3 version-1 ID in the
`PublicRequestId` bucket as a compatibility lookup policy, not as proof of
public provenance. Historical internal move records remain addressable through
that bucket as under the former shared namespace, while typed move lookup does
not consult it. If a pre-upgrade move append exists without durable source
progress, retry appends one new typed move and then advances the source. That
can leave one duplicate at the upgrade boundary, but preserves at-least-once
progress without treating an ambiguous legacy record as proof of a new typed
move. The reader supports mixed legacy and new request-aware frames; older
binaries fail closed on the new frame version, so downgrade after writing one
is unsupported. See [ADR 0029](../decisions/0029-local-typed-dead-letter-move-identities.md).

## Observed Runnel behavior

- Public publish IDs are scoped to one stream. At this research baseline, the
  original offset was returned even when a retry supplied a different key or
  payload. That historical first-use-wins behavior has been replaced by ADR
  0034's contract. Current tests are linked from the
  [request-ID content research](publish-request-id-content-mismatch.md); the
  baseline behavior was covered by
  [`repeated_request_id_returns_original_offset_without_appending`](../../crates/runnel-core/src/lib.rs#L451)
  and
  [`request_id_deduplication_survives_restart`](../../crates/runnel-core/src/lib.rs#L551).
- The local move ID is a deterministic, length-prefixed string containing the
  source stream, consumer, and offset. Its value is bounded by the existing
  1,024-byte request-ID limit. See
  [`dead_letter_move_id`](../../crates/runnel-core/src/lib.rs#L227).
- At the baseline, `StreamLog` stored public IDs and move IDs in the same
  `HashMap<String, Offset>`. RNL3 version 1 had a zero flags byte and one
  request-ID field, with no public/internal discriminator. The accepted
  implementation writes RNL3 version 2 and rebuilds separate public and move
  buckets from its identity-kind flag. See [`StreamLog`](../../crates/runnel-core/src/stream_log.rs),
  [`append_with_move_id`](../../crates/runnel-core/src/stream_log.rs), and
  [`read_request_id_record`](../../crates/runnel-core/src/stream_log.rs).
- At the baseline, a public record using a move ID blocked source progress if
  its content differed, but a same-content record could impersonate the move.
  Current core tests assert that both matching and mismatching public records
  remain separate from a typed move, and that public retry still resolves its
  original offset: [`public_move_id_collision_with_different_content_does_not_block_source_progress`](../../crates/runnel-core/src/lib.rs),
  [`same_content_public_move_id_does_not_impersonate_move_across_restart`](../../crates/runnel-core/src/lib.rs).
- A retry of an existing typed move still rejects different key or payload
  after recovery, while a matching retry resolves the existing move:
  [`dead_letter_move_identity_rejects_different_content_after_restart`](../../crates/runnel-core/src/lib.rs).
- Core restart coverage includes a completed RNL3 version-1 public collision
  and an interrupted legacy move. In the latter case, recovery indexes the
  ambiguous version-1 record in the public-ID bucket as a compatibility lookup,
  appends one typed move, injects a source acknowledgement failure, reopens, and
  confirms the typed move is reused:
  [`legacy_public_move_id_remains_public_and_does_not_satisfy_new_move`](../../crates/runnel-core/src/lib.rs),
  [`interrupted_legacy_move_retries_as_typed_move_once_after_restart`](../../crates/runnel-core/src/lib.rs).
- Local movement persists the target append before the source acknowledgement.
  Under the baseline shared namespace, a conflicting public identity blocked
  source progress, while same-content content could stand in for an internal
  move. With typed identities, either kind of public collision remains separate
  and the target move is appended before source progress. The clustered path
  currently appends the derived record and advances source progress in one
  replicated transition within the same data group; it does not use this local
  string identity.
- `docs/tech-debt.md` already records that the location cache is bounded but
  the request-identity map grows with distinct retained identities (TD-002).
  ID lengths are bounded; the number of retained distinct IDs is not. The
  implementation keeps two namespace buckets so each entry retains one offset
  value, at a fixed second-map header cost per stream with separate capacity
  slack. Same-text cross-namespace entries retain a key in each bucket. This is
  a memory-accounting tradeoff, not a latency claim or a bound on total index
  cardinality.

## Reference behavior

### Apache Kafka

[KIP-98](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98%2B-%2BExactly+Once+Delivery+and+Transactional+Messaging)
uses broker-assigned producer IDs and per-partition sequence numbers for
idempotent writes. It also defines control-message markers separately from
application messages and changes the message format to carry protocol
identity. Its idempotent producer guarantee is scoped to one producer session.

**Transfer:** durable protocol metadata can carry operation identity separately
from application key/value, and a format discriminator can keep internal
control identity out of application identity. This is a useful precedent for
typed local record metadata.

**Does not transfer:** Runnel's public request ID is caller supplied and
replayable after restart while its record remains retained. Kafka's producer
session and sequence contract has a different identity scope; Kafka
transactions also do not automatically provide a local two-file transaction
for Runnel.

### NATS JetStream

The [header reference](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/headers.md)
defines `Nats-Msg-Id` as a client-defined publish header used for deduplication
inside a configured duplicate window, and reserves the `Nats-` namespace for
protocol/server use. The [stream reference](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md)
documents the sliding duplicate window.

**Transfer:** keep operation metadata in a distinct metadata channel and make
namespace ownership explicit. The reserved namespace shows that a public
interface can document server-owned fields.

**Does not transfer:** reserving a string prefix inside Runnel's existing
request-ID field is weaker than a type discriminator, and JetStream's duplicate
window does not preserve Runnel's current retained-log replay horizon.

### Amazon SQS FIFO

The [deduplication ID terms](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-key-terms.html)
specify a five-minute tracking interval, including after receive and delete.

**Transfer:** the deduplication horizon and state-retention policy are part of
the identity contract and must be explicit.

**Does not transfer:** SQS's time-bounded deduplication is not evidence for
Runnel's replay behavior across restarts while records remain retained.

These sources support separating identity domains; they do not prescribe a
Runnel frame layout or establish a Runnel durability guarantee.

RabbitMQ's current quorum-queue documentation also provides a useful dead-letter
failure reference: its at-least-once path retains source messages until target
publisher confirms, and retries can leave duplicates at the target. This
supports the Runnel invariant that source progress follows confirmed target
durability and that a possible duplicate is safer than advancing on ambiguous
identity. RabbitMQ's replicated queue and confirmation model differs from
Runnel's local append/checkpoint pair, so it does not establish Runnel's crash
guarantee ([Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)).

## Durable identity alternatives

### Keep a shared string identity and compare content

This is the current behavior. Strict key/payload matching prevents a
different-content record from advancing the source, but identical content under
the same public ID is accepted as the move. A payload hash has the same
limitation: matching bytes prove content equality, not that the broker created
the record for this move. **Assessment:** insufficient for the accepted identity contract.

### Reserve the current textual prefix

The broker could reject public request IDs that start with `runnel-dlq/` and
continue storing move IDs in the string field. This reduces future accidental
collisions without changing the durable frame. It also changes which public IDs
are accepted, depends on every public write path enforcing the reservation, and
does not distinguish old RNL3 public records that already use the prefix from
old internal moves. The string remains a convention instead of durable type
information. **Assessment:** possible compatibility guard, but not a complete
identity contract.

### Persist a typed identity domain

New local request-aware records could persist a discriminator for either a
public request ID or an internal move identity. The internal value can remain
the source stream, consumer, and source offset tuple (or a canonical encoding
of that tuple), rather than passing through the public string namespace. On
recovery, the index uses the discriminator as part of its key. Public replay
continues to look up only public IDs; move reconciliation looks up only move
identity and then checks key and payload.

The accepted shape is one logical typed identity index implemented with
separate public and internal buckets. This keeps one offset value per retained
identity at the cost of a second map header per stream; a same-text cross-kind
collision stores a key in both buckets. The combined entry count grows with
distinct identities and can exceed the former string-key count. Field sizes
can be bounded, but total index cardinality is not bounded today. Bounded RAM
with the current replay behavior would require a disk-backed lookup or log
scan, adding lookup or recovery cost. A hard lifetime bound on identity
metadata would require an explicit retention or expiry contract, changing
current replay behavior. These resource choices overlap TD-002 and are not
resolved here.

**Assessment:** strongest fit for the identity separation goal. ADR 0029 adopts
the frame/upgrade policy and treatment of legacy RNL3 records.

### Add a local transaction or recovery journal

A transaction or intent journal could coordinate target append and source
progress, reducing ambiguity around their separate persistence boundaries. It
does not by itself make a public request ID a different type from a move ID; an
atomic transaction could still accept the wrong existing target record if the
identity lookup remained shared. **Assessment:** complementary to TD-017, not a
substitute for identity separation.

## Legacy compatibility and safety tradeoff

RNL1 and RNL2 records do not carry request IDs. RNL3 records carry an ID string
but no public/internal discriminator. A reader can continue reading those
frames and preserve public request-ID replay, but it cannot infer whether an
RNL3 ID with the `runnel-dlq/v1/` shape came from a public publish or an
internal move. Matching key and payload cannot resolve this because the
same-content collision is now directly reproduced.

**Accepted policy (ADR 0029):** index every legacy RNL3 version-1 ID in the
public-ID bucket for lookup compatibility. This does not assert public origin:
some such records were written by the old internal move path and remain
addressable through public replay as under the former shared namespace. A new
typed move is not satisfied by an ambiguous legacy record. If upgrade occurs
after an old move append but before source progress is durable, retry may
append one new typed move before advancing the source. This bounded
upgrade-boundary duplicate preserves at-least-once progress; accepting a
same-content legacy record as proof would preserve impersonation ambiguity. No
migration can classify every such record without trusted provenance that was
not persisted.

RNL3 version 2 reuses the existing request-aware header layout and assigns the
reserved flags byte to the identity kind. Version 1 continues to require zero
flags and its IDs use the public-ID bucket as a compatibility lookup policy,
not as proof of public provenance; version 2 accepts only the defined public
and internal-move kinds. The checksum already covers
the full header, so the kind is protected by the existing checksum. New code
must read mixed version-1/version-2 histories. An older binary rejects the new
version rather than silently losing the typed identity; downgrade is therefore
unsupported after the first version-2 record. The new version changes only
request-aware append records; RNL1/RNL2 handling is unchanged. This is a
forward-read choice, not a general storage-format compatibility policy; ADR
0001 and TD-007 continue to leave long-lived upgrade support open.

## Implementation evidence and remaining boundaries

Focused core tests cover these accepted identity behaviors:

1. Public and internal identities with identical text coexist as separate
   records. Current public retry resolves an exact content match or rejects a
   representable mismatch under ADR 0034; move retry resolves only the typed
   move and verifies its content. Public request-ID contract coverage is
   detailed in the linked request-ID content research.
2. Mismatching and same-content public IDs both allow the source move to append
   independently, then source progress advances only after move durability.
   A source-ack failure followed by reopen retries the typed move without
   another target append.
3. Legacy RNL3 version-1 IDs use the public-ID bucket as a compatibility lookup
   policy, not as proof of public provenance. Old internal moves remain
   addressable through public replay, and an ambiguous pending legacy move can
   produce one additional target record before source progress advances.
   RNL1/RNL2 and mixed RNL3 histories remain readable by new code;
   malformed new frames fail closed and incomplete tails retain current
   truncation behavior.
4. A version-2 record is not readable by old code; downgrade after such a write
   is unsupported. This is a storage compatibility boundary, not a wire change.
5. The two namespace buckets retain one offset per identity, plus a fixed map
   cost per stream. Their combined entry count remains unbounded under current
   retention; TD-002 continues to track this existing metadata growth.
6. Source stream name, consumer name, and source offset remain the move key
   under the current no-delete stream lifecycle. A future delete/recreate or
   incarnation feature needs to extend the key before it is introduced.

Real-server tests exercise same-content and mismatching public collisions,
public replay behavior, source progress, and restart through the wire protocol.
The typed-client and cluster-process contract tests are detailed in the linked
request-ID content research. Core tests cover typed-move content
validation, completed-v1-public collision, interrupted-v1-move retry, version-2
move deduplication/recovery, invalid versions/flags, and checksum protection.
The previous version-1-only reader
rejects version 2 by its version guard, so downgrade after writing a version-2
record is unsupported. These tests do not establish actual filesystem/device
sync failure or power-loss behavior. Clustered behavior remains covered by its
existing atomic transition and is not a local-identity guarantee.

## Disposition

TD-029's typed local identity and legacy lookup outcome is selected in ADR
0029 and implemented in the local storage path. Public request-ID content
semantics remain a distinct contract owned by ADR 0034 and implemented across
both engines, with its conformance evidence recorded in the linked research.
No separate tracker is needed for the already-selected typed identity policy
or the implemented request-ID contract. The broader client-interactions
outcome remains open for interoperability, retention, and external-application
evidence. The index remains proportional to
retained identities under existing behavior, a limitation already tracked by
TD-002. Two namespace buckets avoid increasing the per-identity offset value
size, while adding fixed per-stream map overhead and possible duplicate string
storage for a cross-namespace collision. TD-002 remains open for unbounded
retained identity cardinality; the local move/source-ack boundary remains open
under TD-017.

## References

- Apache Kafka, [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98%2B-%2BExactly+Once+Delivery+and+Transactional+Messaging).
- NATS, [JetStream headers](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/headers.md) and [JetStream streams](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md).
- Amazon Web Services, [Amazon SQS FIFO queue key terms](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-key-terms.html).
- RabbitMQ, [Quorum Queues: at-least-once dead-lettering](https://www.rabbitmq.com/docs/quorum-queues).
