# TD-029: Public request IDs and dead-letter move identity

- Status: source-backed identity contract proposed in [ADR 0029](../decisions/0029-local-typed-dead-letter-move-identities.md); focused core and isolated real-server verification passed
- Last reviewed: 2026-10-05
- Baseline: `f999c1b9ad5d22408bbbe6c6276a42e825cd62ef`
- Related debt: [TD-002](../tech-debt.md#td-002-one-file-and-a-startup-scan-per-local-stream), [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records), and [TD-029](../tech-debt.md#td-029-public-request-ids-can-collide-with-local-dead-letter-move-ids)
- Related design: [Dead-letter recovery across durable boundaries](../design/dead-letter-recovery.md)
- Related decisions: [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md) and [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md)

This note compares ways to distinguish a client-supplied publish identity from
the local engine's identity for one dead-letter move. The source-backed
recommendation is recorded in proposed ADR 0029. The named baseline describes
the pre-change behavior; the proposal and implementation sections record the
resulting local format and compatibility policy for review.

## Finding

At the supplied baseline, Runnel stored both kinds of identity as the same
request-ID string in one per-stream index. The local dead-letter append added a
key and payload comparison to avoid acknowledging the source against a
conflicting record, but that comparison could not establish who created a
matching record. PR #346 added baseline core and real-server coverage for this
same-content case; the tests are updated by this change to assert typed
identity behavior.

**Recommendation (inference, proposed in ADR 0029):** persist a typed identity
domain for new local request-aware records. Keep caller-supplied IDs as public
IDs and local source-to-dead-letter IDs as moves. Keep the public request-ID API
and replay behavior unchanged. A move retry reconciles only against the same
typed move identity and still verifies key and payload before source progress
advances.

Use a new request-aware frame version to persist the identity kind. Read the
current untyped RNL3 version as `PublicRequestId`, including records whose text
looks like an old internal move ID. If a pre-upgrade move append exists without
durable source progress, append one new typed move and then advance the source.
That can leave one duplicate at the upgrade boundary, but preserves at-least-once
progress without treating a matching public record as proof of an internal
operation. The reader must support mixed legacy and new request-aware frames;
older binaries fail closed on the new frame version, so downgrade after writing
one is unsupported. The concrete proposal is in [ADR 0029](../decisions/0029-local-typed-dead-letter-move-identities.md).

## Observed Runnel behavior

- Public publish IDs are scoped to one stream. Reusing an ID returns its
  original offset even when the retry supplies a different key or payload; the
  behavior is covered by
  [`repeated_request_id_returns_original_offset_without_appending`](../../crates/runnel-core/src/lib.rs#L451)
  and
  [`request_id_deduplication_survives_restart`](../../crates/runnel-core/src/lib.rs#L551).
- The local move ID is a deterministic, length-prefixed string containing the
  source stream, consumer, and offset. Its value is bounded by the existing
  1,024-byte request-ID limit. See
  [`dead_letter_move_id`](../../crates/runnel-core/src/lib.rs#L227).
- At the baseline, `StreamLog` stored public IDs and move IDs in the same
  `HashMap<String, Offset>`. RNL3 version 1 had a zero flags byte and one
  request-ID field, with no public/internal discriminator. The proposed
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
  and an interrupted legacy move. In the latter case, recovery reads the
  ambiguous version-1 record as public, appends one typed move, injects a source
  acknowledgement failure, reopens, and confirms the typed move is reused:
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
the record for this move. **Assessment:** insufficient for the TD-029 goal.

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

The proposed shape is one logical typed identity index implemented with
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

**Assessment:** strongest fit for the identity separation goal. ADR 0029
accepts the frame/upgrade policy and treatment of legacy RNL3 records.

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

**Proposed policy (ADR 0029):** treat every
legacy RNL3 version-1 ID as public for new move reconciliation. This preserves
the old observable public replay result even though some such records were
written by the old internal path. A new typed move is not satisfied by an
ambiguous legacy record. If upgrade occurs after an old move append but before
source progress is durable, retry may append a second dead-letter record before
advancing the source. This bounded upgrade-boundary duplicate is at-least-once
safe; accepting a same-content legacy record as proof would preserve
impersonation ambiguity. No migration can classify every such record without
trusted provenance that was not persisted.

The proposed new request-aware frame version reuses the existing header layout
and assigns the current reserved flags byte to the identity kind. Version 1
continues to require zero flags and is read as public identity; version 2 accepts
only the defined public and internal-move kinds. The checksum already covers
the full header, so the kind is protected by the existing checksum. New code
must read mixed version-1/version-2 histories. An older binary rejects the new
version rather than silently losing the typed identity; downgrade is therefore
unsupported after the first version-2 record. The new version changes only
request-aware append records; RNL1/RNL2 handling is unchanged. This is a
forward-read choice, not a general storage-format compatibility policy; ADR
0001 and TD-007 continue to leave long-lived upgrade support open.

## Implementation evidence and remaining boundaries

Focused core tests cover these proposed identity behaviors:

1. Public and internal identities with identical text coexist as separate
   records. Public retry resolves only the original public record (including
   the established mismatched-content replay behavior); move retry resolves
   only the typed move and verifies its content.
2. Mismatching and same-content public IDs both allow the source move to append
   independently, then source progress advances only after move durability.
   A source-ack failure followed by reopen retries the typed move without
   another target append.
3. Legacy RNL3 version-1 IDs remain public for replay, and an ambiguous pending
   legacy move can produce one additional target record before source progress
   advances. RNL1/RNL2 and mixed RNL3 histories remain readable by new code;
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
Core tests cover typed-move content validation, completed-v1-public collision,
interrupted-v1-move retry, version-2 move deduplication/recovery, invalid
versions/flags, and checksum protection. The previous version-1-only reader rejects version 2 by its version
guard, so downgrade after writing a version-2 record is unsupported. These
tests do not establish actual filesystem/device sync failure or power-loss
behavior. Clustered behavior remains covered by its existing atomic transition
and is not a local-identity guarantee.

## Disposition

This source review informed the proposed typed local identity contract and
legacy policy in ADR 0029. The runtime implementation is local to runnel-core,
with real-server coverage in runnel-server; the clustered engine and public
wire schema are unchanged. No separate resource tracker is warranted because
the index remains proportional to retained identities under existing
behavior, a limitation already tracked by TD-002. Two namespace buckets avoid
increasing the per-identity offset value size, while adding fixed per-stream
map overhead and possible duplicate string storage for a cross-namespace
collision. ADR acceptance remains pending review.

## References

- Apache Kafka, [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98%2B-%2BExactly+Once+Delivery+and+Transactional+Messaging).
- NATS, [JetStream headers](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/headers.md) and [JetStream streams](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md).
- Amazon Web Services, [Amazon SQS FIFO queue key terms](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-key-terms.html).
- RabbitMQ, [Quorum Queues: at-least-once dead-lettering](https://www.rabbitmq.com/docs/quorum-queues).
