# TD-029: Public request IDs and dead-letter move identity

- Status: exploratory research; the storage identity contract is not accepted
- Last reviewed: 2026-10-02
- Baseline: `6b7178e508c609b4df0a566f8654721f23f36b68` (includes PRs #346 and #348; refreshed across #350, #353, and #359)
- Related debt: [TD-002](../tech-debt.md#td-002-one-file-and-a-startup-scan-per-local-stream), [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records), and [TD-029](../tech-debt.md#td-029-public-request-ids-can-collide-with-local-dead-letter-move-ids)
- Related design: [Dead-letter recovery across durable boundaries](../design/dead-letter-recovery.md)
- Related decisions: [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md) and [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md)

This note compares ways to distinguish a client-supplied publish identity from
the local engine's identity for one dead-letter move. It records an evidence-
based recommendation, not an accepted format or compatibility decision. The
current Rust code and tests at the named baseline remain authoritative.

## Finding

Runnel currently stores both kinds of identity as the same request-ID string
in one per-stream index. The local dead-letter append adds a key and payload
comparison to avoid acknowledging the source against a conflicting record, but
that comparison cannot establish who created a matching record. PR #346 now
tests this exact same-content case through the core and real server paths.

**Recommendation (inference):** persist a typed identity domain for new local
records, with public request IDs and dead-letter move identities represented as
distinct durable variants. Keep the public request-ID API and replay behavior
unchanged. Reuse one identity index keyed by the typed identity rather than
adding a second independently growing map. A move retry should reconcile only
against the same typed move identity and should still verify that its key and
payload match before source progress advances.

This avoids future public-ID impersonation without treating equal message
content as proof of operation provenance. It requires a new on-disk discriminator
or equivalent typed metadata. It does not retroactively disambiguate RNL3
records, because those frames store only an untyped request-ID string. That
upgrade ambiguity needs an explicit compatibility policy before implementation.

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
- `StreamLog` stores public IDs and move IDs in the same
  `HashMap<String, Offset>`. Recovery rebuilds that map from complete
  request-aware frames. Current RNL3 frames have a zero flags byte and one
  UTF-8 request-ID field; there is no field that labels an ID as public or
  internal. See [`StreamLog`](../../crates/runnel-core/src/stream_log.rs#L60),
  [`append_with_move_id`](../../crates/runnel-core/src/stream_log.rs#L332), and
  [`read_request_id_record`](../../crates/runnel-core/src/stream_log.rs#L913).
- On retry, a local move ID that already exists is accepted only when the
  target key and payload also match. Different content returns invalid data and
  leaves source progress unadvanced. Equal content returns the existing
  offset. This distinction is covered by
  [`dead_letter_move_content_mismatch_is_storage_error_without_acknowledgement`](../../crates/runnel-core/src/lib.rs#L1442)
  and
  [`dead_letter_move_same_content_public_id_reconciles_after_restart`](../../crates/runnel-core/src/lib.rs#L1481).
- The core test injects source-ack persistence failure after a public publish
  used the exact move ID and matching content, then checks recovery and repeated
  reopen. The real-process test
  [`network_protocol_reconciles_same_content_public_dead_letter_id_after_restart`](../../crates/runnel-server/tests/server_smoke.rs#L1053)
  confirms the same public protocol ID is accepted as the completed move across
  server restart. Together these establish behavior, not provenance.
- Local movement persists the target append before the source acknowledgement.
  This preserves at-least-once safety when a target identity conflicts: source
  progress is not advanced. The same-content case can instead make a public
  record stand in for an internal move. The clustered path currently appends
  the derived record and advances source progress in one replicated transition
  within the same data group; it does not use this local string identity.
- `docs/tech-debt.md` already records that the location cache is bounded but
  the request-identity map grows with distinct retained identities (TD-002).
  ID lengths are bounded; the number of retained distinct IDs is not. One typed
  index avoids a second index with separate cardinality growth, but it does not
  make the existing total index bounded.

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

The preferred shape is one discriminated identity index, with one entry per
distinct typed identity rather than separate public and internal maps. The
entry count still grows with distinct identities and can exceed the current
string-key count when a public ID and move ID have the same text. Field sizes
can be bounded, but total index cardinality is not bounded today. Bounded RAM
with the current replay behavior would require a disk-backed lookup or log
scan, adding lookup or recovery cost. A hard lifetime bound on identity
metadata would require an explicit retention or expiry contract, changing
current replay behavior. These resource choices overlap TD-002 and are not
resolved here.

**Assessment:** strongest fit for the identity separation goal, conditional on
an accepted frame/upgrade policy and explicit treatment of legacy RNL3 records.

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

**Recommendation (inference):** treat legacy RNL3 IDs as untyped legacy/public
identities for new move reconciliation. A new typed move would not be satisfied
by an ambiguous RNL3 record. If an upgrade occurs after an old move's target
append but before its source acknowledgement is durable, retrying can append a
second dead-letter record before advancing the source. Such a duplicate is
at-least-once safe, whereas accepting a matching legacy public record as proof
of the internal operation preserves the impersonation ambiguity. No migration
can classify every ambiguous legacy record without additional trusted history.
The accepted policy must state whether this one-time duplicate possibility is
allowed, or select a more involved migration or refusal boundary.

This is not a claim that all upgrade paths currently support mixed RNL3/new
typed frames. The implementation must define version recognition, checksum and
length validation, startup behavior, and whether older binaries may open a log
after new typed frames exist. The [Kafka KIP-98 format and upgrade discussion](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98%2B-%2BExactly+Once+Delivery+and+Transactional+Messaging)
is a reference for format-versioning consequences, not a migration recipe for
Runnel.

## Open questions and evidence gates

Before implementation, an ADR should settle:

1. The durable representation for a typed public ID versus internal move
   identity, and the exact read-forward / downgrade policy for RNL1, RNL2, RNL3,
   and new records.
2. The upgrade outcome when a legacy RNL3 move append exists but the source
   acknowledgement does not: accept a possible duplicate target record, reject
   or block startup, or provide a separately verified migration.
3. Whether the resource goal is “no new identity index beyond the current
   one,” bounded RAM with disk-backed lookup, or a finite deduplication horizon.
   Permanent replay, unlimited retention, and a strict bound on all identity
   state cannot all be assumed without choosing a storage/expiry mechanism.
4. Whether source stream name, consumer name, and offset remain a unique move
   key if future stream deletion/recreation or retention changes stream
   incarnation semantics.

The implementation evidence should include real-server public-protocol tests
that: publish a same-content public record using the old predictable ID and
prove it remains a separate record from the internal move; publish a
mismatching record and prove the source is not advanced on error; retry and
reopen after target append/source-ack failure and prove one new typed move is
reconciled; preserve public ID replay across restart; read legacy RNL1/RNL2/RNL3
fixtures; and exercise malformed or incomplete typed frames without unsafe
allocation or source progress. Any migration-specific duplicate policy needs
its own recovery test. Clustered behavior should remain covered by its existing
atomic transition and must not be described as a local-identity guarantee.

## Disposition

This research supports the existing TD-029 outcome and retirement criteria;
the newly merged same-content tests strengthen the evidence but do not settle
provenance. TD-002 already records the unbounded request-index cardinality, so
no new tracker item is warranted. No implementation or accepted decision is
proposed in this change.

## References

- Apache Kafka, [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98%2B-%2BExactly+Once+Delivery+and+Transactional+Messaging).
- NATS, [JetStream headers](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/headers.md) and [JetStream streams](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md).
- Amazon Web Services, [Amazon SQS FIFO queue key terms](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-key-terms.html).
