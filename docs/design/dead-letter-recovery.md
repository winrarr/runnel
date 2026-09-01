# Dead-letter recovery across durable boundaries

- Status: exploratory design note; no runtime semantics are changed here
- Date: 2026-09-01
- Related debt: [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records)
- Related decisions: [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md) and [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md)

This note investigates the failure boundary in which dead-letter movement
updates a source consumer and a derived stream. It is a design input for a
future implementation, not an acceptance of new protocol behavior.

## Outcome and non-goals

The smallest useful local improvement is recovery reconciliation with a stable
dead-letter move identity:

1. Keep the existing safety order: durably append the derived record before
   durably advancing the source consumer.
2. Give each logical move a stable identity derived from the source stream,
   source consumer, and source offset. The consumer name is required because
   independent consumers may legitimately dead-letter the same source record.
3. Make the derived-stream append idempotent for that identity. A retry after
   an uncertain append must resolve an existing record with the same identity,
   rather than append another record.
4. Advance the source only after the target append is known to be durable. If
   the target result is uncertain or cannot be reconciled, leave source
   progress unchanged and return a storage failure; a later poll/recovery
   attempt retries the same identity.

The repository already has a durable request-aware record identity that is
rebuilt from the stream log on open. Reusing that mechanism for an internal
move identity is a hypothesis to validate, not a new public request contract.
The identity must be scoped to the target stream and must reject a same-ID
key/payload mismatch as corruption or an explicit storage error. The current
public publish request-ID behavior, which intentionally ignores mismatched
payloads for compatibility, is not sufficient for this internal invariant.

This direction provides at most one durable derived record per move identity
once implemented. It does not provide exactly-once delivery or exactly-once
application processing: a dead-letter consumer can still be redelivered, and
its external side effects remain the consumer’s responsibility.

## Current behavior and crash boundary

The local engine has two independent durable objects. In
`runnel-core`, `poll_group` checks the attempt limit, `dead_letter_record`
appends the original key and payload to `<source>.dead-letter`, and only then
persists a source `Acknowledge` event. Stream appends call `sync_data`; source
consumer events append to a bounded journal and call `sync_all` before the
operation continues. Recovery reconstructs the source state from its
checkpoint and journal and reconstructs target records by scanning the target
stream log. See the [local engine](../../crates/runnel-core/src/lib.rs), in
particular `poll_group`, `dead_letter_record`, and
`persist_consumer_event`.

The resulting durable order is intentional:

```text
source message reaches attempt limit
        |
        v
target append + sync_data
        |
        v
source consumer event + sync_all
        |
        v
source progress advances
```

The meaningful process-crash states are:

| Crash point | Durable state after recovery | Current result |
| --- | --- | --- |
| Before the target append reaches its durable point | Source remains eligible. Depending on the filesystem outcome, the target may be absent, have a torn tail that recovery truncates, or appear complete despite an uncertain sync. | Safe retry, no known loss; the operation may return an I/O error. The current path can duplicate even when recovery finds a complete-looking record. |
| After the target append is durable, before the source event is durable | Target contains the copied record; source attempt state has not advanced. | The source is retried and a second target record can be appended. This is the TD-017 duplicate window. |
| After the source event is durable | Target contains the record and source progress advances during recovery. | No second move is required for that source consumer and offset. |
| Target write or source event has an ambiguous I/O result | The result depends on which bytes and sync boundaries reached durable storage. | The source must not be advanced on an unknown target result; recovery must inspect or retry using the same move identity. |

The current dead-letter record carries only the original key and payload. It
does not carry a source offset, source consumer, attempt history, or an
idempotency identity. Those are separate provenance work tracked by TD-018;
the recovery identity proposed here can remain internal even if provenance is
later exposed to clients.

The clustered engine has a different boundary. A grouped `PollGroup` is one
Raft command. In `apply_group_poll`, the original message is appended to the
derived stream held in the same `SnapshotState` as the source consumer state,
then the source offset is advanced before the command response is returned.
The state-machine journal is persisted before applying the command and is
replayed after a restart. The derived dead-letter stream is resolved back to
the source data group when addressed by the protocol. See the
[clustered engine](../../crates/runnel-raft/src/lib.rs), in particular
`StateMachineStore::apply`, `apply_group_poll`, and
`data_group_for_stream`.

Consequently, the current clustered path has one replicated logical transition
for source progress plus the derived record. A committed transition is not
split by a process crash or leader change under the data-group durability
guarantee. This is stronger than the local path, but it still does not make a
dead-letter consumer’s processing exactly once. If a future layout puts the
derived stream in a separate Raft data group, the cross-group problem returns;
the current clustered behavior must not be generalized to that layout without
a transaction or reconciliation design.

## Recommended smallest direction

Use a deterministic, internal `move_id` for the local derived append. A
conceptual identity is:

```text
runnel-dlq/v1/<source-stream>/<source-consumer>/<source-offset>
```

The encoding must be bounded and unambiguous; it must not be a filesystem path
or a public topology identifier. It may use a length-prefixed or hashed
representation if the validated names would exceed the storage identity
limit. The target stream remains the normal derived stream selected by the
existing name rule.

The local operation should then behave as follows:

1. Read the source record and derive the move ID from the source identity.
2. Append the copied key and payload to the target using a durable
   move-ID-aware append. If the target already has that move ID with the same
   content, treat it as the successful result of the earlier attempt. If the
   content differs, stop with an explicit corruption/storage error.
3. Only after the target append or idempotent lookup has reached its durable
   point, persist the source acknowledgement event. Preserve the current
   checkpoint-before-journal-truncation ordering as well.
4. If the process dies between steps 2 and 3, the next source poll performs
   step 2 with the same move ID, observes the existing target record, and
   persists the source acknowledgement without appending another target
   record.

This is lazy reconciliation: no background scanner is required for the
correctness property, because an unadvanced source remains a candidate for
the same move. A later implementation may add a durable pending marker if
operators need progress without another source poll, but a pending marker
alone is not sufficient; the target append still needs a stable deduplication
identity. If the target stream is unavailable, the source remains unadvanced
and the pending work continues to represent at-least-once delivery.

The existing request-aware stream frames are a plausible storage primitive for
the target identity because they retain the identity in the record and
rebuild an ID-to-offset index on restart. That primitive must be checked for
all supported durable formats, journal truncation, retention, and malformed
record recovery before it is reused. An equivalent dedicated move index is
acceptable if it has the same durable lookup and recovery properties.

The semantic contract for both engines should remain:

- a source record is not considered dead-lettered until a durable target
  record for its move identity exists;
- source progress never advances past a move whose target outcome is unknown;
- retries may occur after crashes and uncertain responses;
- a given source-consumer/offset move has one logical target record after
  reconciliation, while distinct source consumers retain distinct moves; and
- consumers of the dead-letter stream remain at least once.

## Alternatives considered

| Alternative | Benefit | Cost or reason not selected as the smallest direction |
| --- | --- | --- |
| Keep append-then-checkpoint without an identity | Minimal code and already preserves no-loss ordering. | The known duplicate window remains; dead-letter consumers must deduplicate opaque copies. This is the current interim behavior, not a retirement of TD-017. |
| Full local two-phase commit across source state and target log | Can make target visibility and source advancement one all-or-none transaction. | Requires a transaction coordinator or shared commit log, prepare/commit markers, recovery of prepared work, locking/order rules across two stream files, and format/version migration. It is not justified as the smallest first repair. |
| Durable source outbox or pending-move journal plus a relay | Makes the intent to move recoverable even if the process stops before sending to the target and can provide operator-visible backlog. | Adds another durable state machine and relay lifecycle. Without target move-ID deduplication, a crash after target append still duplicates. It is a possible follow-on if lazy reconciliation is operationally insufficient. |
| Put the local derived record in the source log or a single combined transaction log | Gives one physical durability boundary, similar to the current clustered state machine. | Changes local stream layout, target offsets, retention, recovery, and the separation between source and derived streams. It would also make a local storage choice dictate the future engine contract. |
| Use a saga/compensating delete | Breaks the move into local transactions without a coordinator. | A compensation after a visible target append can itself be lost or race with a dead-letter consumer. It favors eventual cleanup, not a simple no-loss and duplicate-safe invariant. |
| Add a cross-group distributed transaction to clustered delivery | Preserves atomicity if source and target groups are split later. | Requires a replicated coordinator and participant protocol, transaction timeout/recovery rules, and client visibility/isolation semantics. Keep the current same-data-group atomic transition while the layout remains unchanged. |

The recommendation follows two established patterns. Apache Kafka’s
transaction design combines produced records and consumed offsets in an
atomic unit, uses a persistent transaction log, and requires a stable
transaction identity to recover unfinished work; it also explicitly limits
the guarantee to the transactional broker/consumer boundary rather than
arbitrary external processing ([KIP-98: Exactly Once Delivery and
Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)).
That is a useful reference for the full-transaction alternative, but adopting
it locally would be a much larger storage and protocol change.

RabbitMQ documents the opposite side of the trade-off. Ordinary dead-letter
exchange republishing removes a message without publisher confirms and can
lose it when the target is unavailable; quorum-queue at-least-once
dead-lettering retains the source until the target confirms, but retries can
produce duplicates and retained pending messages consume source resources
([Dead Letter Exchanges](https://www.rabbitmq.com/docs/next/dlx), [Quorum
Queues: at-least-once dead-lettering](https://www.rabbitmq.com/docs/quorum-queues)).
Runnel should retain the no-loss ordering and add a bounded identity-based
retry, while making target-unavailable pressure and pending work observable in
a future implementation.

The primary research points in the same direction. Garcia-Molina and Salem’s
original Sagas paper models a long operation as interleaved local transactions
with compensating actions ([Sagas, Princeton technical report](https://www.cs.princeton.edu/techreports/1987/070.pdf);
[ACM DOI](https://doi.org/10.1145/38713.38742)). A compensation is not a safe
substitute for an atomic dead-letter move because deleting a target copy can
race with its consumer. Helland’s CIDR position paper describes the practical
alternative: independent durable entities manage uncertainty as workflow and
remember unique messages so repeated delivery is harmless ([Life beyond
Distributed Transactions](https://ics.uci.edu/~cs223/papers/cidr07p15.pdf)).
That supports a stable move identity and reconciliation, but does not prove
exactly-once processing for Runnel or any external consumer.

## Acceptance and verification gates for implementation

The future implementation should not be accepted until all of these are
demonstrated:

1. **Durable identity:** a move ID is stable across retries and process
   restarts, distinct for independent source consumers, bounded by the chosen
   storage format, and never exposed as a filesystem path.
2. **No-loss ordering:** fault injection at every target-write and source-event
   sync boundary shows that source progress never advances without a durable
   target record. An uncertain target result leaves source progress eligible
   for retry.
3. **Duplicate-safe recovery:** restart after a successful target append and
   before source-event persistence, then repeat the move. The target contains
   one record for the move ID, with the original key and payload, and source
   progress advances exactly once.
4. **Corruption handling:** a same-ID target record with different content,
   malformed identity, torn frame, or an unavailable target produces an
   explicit storage/corruption outcome and does not advance source progress.
5. **Recovery bounds:** recovery and reconciliation use bounded indexes or
   journals, do not scan unrelated streams without a documented bound, and
   preserve the existing checkpoint/journal truncation guarantees.
6. **Real process coverage:** a local broker process restart test exercises the
   crash window through the public protocol. The existing three-node cluster
   tests continue to verify atomic clustered dead-letter movement, restart,
   leader change, and follower recovery.
7. **Future split-group coverage:** if the target ever moves to another
   durable group, add tests for participant failure, coordinator/retry
   recovery, duplicate commands, and ambiguous client outcomes before calling
   the operation atomic.
8. **Semantic wording:** tests and protocol documentation say at-least-once
   for source-to-target and target-consumer delivery. Exactly-once is not
   claimed unless a later decision proves the complete boundary, including
   application side effects.

## Hypotheses and unresolved risks

- **Hypothesis:** the existing request-aware target record and rebuilt ID index
  can provide the required local deduplication without a second pending-move
  journal. This needs fault-injected tests, especially around partial writes
  and journal/checkpoint compaction.
- **Hypothesis:** lazy reconciliation on the next source poll is sufficient
  for correctness. A background reconciler may be needed for operational
  visibility or to make progress when no consumer polls, but it must not
  advance source state on an unconfirmed target.
- A future retention policy must not delete a target record or its move-ID
  evidence while the source move is still unacknowledged. This is a direct
  coupling to the retention work and must be made a durable fence, not a
  best-effort scan.
- The current local and clustered records do not preserve source provenance.
  Adding provenance later must preserve the internal identity and distinguish
  intentionally separate moves by independent consumers.
- A target append can be durable while its response is lost, and filesystem
  durability can differ from process-crash behavior. The tests must model
  returned I/O errors, process termination, incomplete frames, and restart;
  a clean in-memory retry is insufficient evidence.
- If source and target storage are placed on different filesystems or devices
  in a future deployment, even an ordered pair of sync calls has no common
  durability boundary. The identity/reconciliation protocol remains useful,
  but full atomicity would require a different accepted design.
