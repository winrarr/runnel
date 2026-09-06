# Dead-letter recovery across durable boundaries

- Status: exploratory design note; local move reconciliation is implemented, while broader failure-boundary and provenance evidence remains open
- Last reviewed: 2026-09-06
- Baseline: `fa51d9789b7ce5b284eda62903641597b02f96e0`
- Reading guide: [design-note conventions](README.md)
- Related debt: [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records) and [TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse)
- Related decisions: [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md), [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md), and [ADR 0026](../decisions/0026-semantic-engine-error-classification.md)
- Related boundaries: [durability and delivery policy](durability-delivery-policy.md), [application-aware retry policy](application-aware-retry-policy.md), and [clustered outcomes](clustered-outcome-contract.md)

This note separates the observed local append/reconcile behavior from the
clustered same-group transition and from future cross-group choices. It is a
design input, not an acceptance of new protocol behavior. Rust code and tests
at the named baseline remain authoritative; the implementation sketches and
evidence gates below are not requirements to introduce a particular API,
storage format, or transaction protocol.

## Scope and non-goals

The current local slice uses a stable dead-letter move identity and retains
the existing no-loss ordering:

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
   progress unchanged and return a backend failure with the conservative
   `Unknown` outcome; a later poll/recovery attempt retries the same identity.

The local implementation reuses the durable request-aware record identity,
rebuilt from the target stream log on open, for this internal move identity.
This is not a new public request contract. A same-ID key/payload mismatch
returns an explicit invalid-data storage error; the public publish request-ID
behavior, which intentionally ignores mismatched payloads for compatibility,
is not sufficient for this internal invariant.

The current clustered implementation does not use this local move identity.
It appends the derived record and advances source progress in one replicated
state-machine transition while both logical streams remain in the source
stream's data group. That co-location is an accepted property of the current
static-cluster slice, not a general cross-group transaction guarantee.

Neither engine exposes source provenance or a redrive operation. The recovery
goal is at most one durable local derived record per move identity after
reconciliation, not exactly-once delivery or exactly-once application
processing. A dead-letter consumer can still be redelivered, and its external
side effects remain the consumer's responsibility.

## Observed local append and reconciliation behavior

The local engine has two independent durable objects. In
[`Broker::poll_group`](../../crates/runnel-core/src/broker.rs#L233), an
exhausted delivery calls [`Broker::dead_letter_record`](../../crates/runnel-core/src/broker.rs#L505),
which reads the source record, appends its key and payload to the derived
stream, and only then persists a source `Acknowledge` event. Stream appends
call `sync_data`; source consumer events append to a bounded journal and call
`sync_all` before the operation continues. Recovery reconstructs consumer
state from its checkpoint and journal and rebuilds request-aware target
identity by scanning complete target-log frames. The relevant persistence
boundaries are [`StreamLog::append_with_move_id`](../../crates/runnel-core/src/stream_log.rs#L314)
and [`persist_consumer_event`](../../crates/runnel-core/src/consumer_state.rs#L91).

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

The internal move ID is currently a bounded, length-prefixed textual value:

```text
runnel-dlq/v1/<source-stream-length>:<source-stream>/<source-consumer-length>:<source-consumer>/<source-offset>
```

[`dead_letter_move_id`](../../crates/runnel-core/src/lib.rs#L198) derives it
from the validated source stream, source consumer, and source offset. The ID
is stored in an `RNL3` request-aware target frame and is looked up only within
that target stream's in-memory request-ID index. The writer enforces the
request-aware key, payload, and identity limits. On reopen, complete request-
aware frames rebuild that index; an incomplete trailing frame is discarded,
while a complete checksum or format failure is reported rather than silently
treated as a successful move.

The meaningful process-crash states are:

| Crash point | Durable state after recovery | Current result |
| --- | --- | --- |
| Before the target append reaches its durable point | Source remains eligible. The target may be absent, have an incomplete trailing frame that recovery truncates, or appear complete despite an uncertain sync. | The operation can return an I/O error. Under [ADR 0026](../decisions/0026-semantic-engine-error-classification.md), a generic storage result is `Unknown`; a complete-looking record alone is not a durability proof. Target-write and sync fault injection remain open in [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records). |
| After the target append is durable, before the source event is durable | Target contains the copied record and internal move identity; source progress has not advanced. | A retry reuses the same-content target record before advancing source progress. The injected source-persistence failure and reopen path are covered; target-write/sync ambiguity and legacy records remain open in TD-017. |
| After the source event is durable | Target contains the record and source progress advances during recovery. | No second move is required for that source consumer and offset. |
| Target write or source event has an ambiguous I/O result | The result depends on which bytes and sync boundaries reached durable storage. | The source must not be advanced on an unknown target result. A later source poll may reconcile the target using the same move ID; callers must use the engine's semantic outcome rather than blindly replaying an externally visible mutation. |

The local target append checks an existing move ID's key and payload before
reusing it. The focused tests cover stable/scoped identity
([identity](../../crates/runnel-core/src/lib.rs#L1037)), reuse after reopen
([reopen](../../crates/runnel-core/src/lib.rs#L1083)), source-ack persistence
failure ([source-ack failure](../../crates/runnel-core/src/lib.rs#L1129)), and
same-ID content mismatch ([mismatch](../../crates/runnel-core/src/lib.rs#L1191)).
The source-ack failure test exercises the case where the target append has
completed and the source event fails; the next poll then reconciles the
existing target record and persists source progress. This is a duplicate-safe
local slice, not proof that every filesystem failure mode is safe.

## Observed clustered same-group movement

The clustered engine has a different boundary. A grouped `PollGroup` is one
Raft command. In [`apply_group_poll`](../../crates/runnel-raft/src/delivery.rs#L49),
the original message is appended to the derived stream held in the same
`SnapshotState` as the source consumer state, then source progress is
advanced before the command response is returned. The state-machine journal
is persisted before applying the command and replayed after a restart through
[`StateMachineStore::apply`](../../crates/runnel-raft/src/state_machine_store.rs#L743).
The derived stream is resolved back to the source data group by
[`data_group_for_stream`](../../crates/runnel-raft/src/group_manager.rs#L338)
when it is addressed through the public protocol.

Consequently, the current clustered path has one replicated logical transition
for source progress plus the derived record. A committed transition is not
split by a process crash or leader change under the data-group durability
guarantee, and a replayed state-machine journal entry restores the same
transition rather than applying an independent local target write. This is
stronger than the local path, but it is still only broker-side movement and
does not make a dead-letter consumer's processing exactly once. The clustered
transition has no move-ID or source provenance field, so its duplicate-safety
comes from source progress and replicated command application, not from a
cross-stream identity index.

The current evidence covers unit and persistent-engine retry/dead-letter
behavior, plus real three-process reassignment and dead-letter recovery in
[`cluster_smoke`](../../crates/runnel-server/tests/cluster_smoke.rs#L911).
It does not establish a transaction across independent data groups. If a
future placement policy puts the derived stream in another group, the
cross-group problem returns and the current same-group claim must not be
generalized without a separately accepted transaction or reconciliation
design.

## Current provenance and redrive boundary

Both engines currently expose only the copied key and payload as dead-letter
content (along with the target's ordinary offset, timestamp, and delivery
metadata). They do not expose the source stream incarnation, source consumer,
source offset, attempt history, policy version, terminal reason, or a move
identity through the public message/protocol model. Local `RNL3` frames carry
the internal move ID for reconciliation, but it is not public provenance;
clustered `StoredMessage` values carry no equivalent move ID. Existing
dead-letter records therefore cannot be retroactively enriched with reliable
origin metadata.

There is also no broker redrive operation. An application can consume a
dead-letter stream and publish a new record, but that is a new operation with
new offset and retry state; it is not an atomic source-to-target move and
cannot preserve provenance by inference. Provenance, explicit terminal
outcomes, and redrive remain the separate application-aware policy work in
[TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse).

## Future cross-group recovery choices

The current clustered same-group transition is the accepted first layout. No
cross-group transaction or reconciliation design has been accepted. If
placement or named dead-letter targets later put source progress and the
derived record in different durable groups, the implementation must choose a
new boundary rather than inherit the current claim of atomicity.

The outcome boundary to preserve across any future choice is:

- a source record is not considered dead-lettered until a durable target
  record for its stable operation identity exists;
- source progress never advances past a move whose target outcome is unknown;
- retries may occur after crashes and uncertain responses;
- a given source-consumer/offset move has one logical target record after
  reconciliation, while distinct source consumers retain distinct moves; and
- consumers of the dead-letter stream remain at least once.

These are outcome and evidence requirements, not a required command shape or
storage layout. A future design also needs to define how provenance and
redrive interact with an uncertain move, and how pending work is bounded when
the target group is unavailable.

### Candidate mechanisms and trade-offs

| Alternative | Benefit | Cost or unresolved concern |
| --- | --- | --- |
| Preserve same-group placement | Retains the current single replicated transition and avoids a cross-group protocol. | Constrains placement, named targets, balancing, and independent scaling. It is the current accepted boundary, not a general solution. |
| Stable identity plus reconciliation | Lets independently durable groups retry an uncertain move without creating a second logical target record. | Requires durable identity/provenance, target lookup, retention fences, bounded pending work, and explicit handling for target unavailability; source and target are still not one atomic commit. |
| Durable source outbox or pending-move journal plus a relay | Makes the intent recoverable even if the process stops before sending to the target and can provide operator-visible backlog. | Adds another durable state machine and relay lifecycle. Without target identity reconciliation, a crash after target append still duplicates; with it, eventual completion and cleanup semantics remain to be specified. |
| Coordinator-driven cross-group transaction | Can make source progress and target visibility one all-or-none replicated operation. | Requires coordinator and participant state, prepare/commit records, timeout and recovery rules, fencing, and client visibility for in-doubt work. It also expands the public compatibility surface. |
| Combined physical transaction log | Gives source and target one durability boundary without a distributed coordinator. | Changes local/clustered layout, target offsets, retention, recovery, and the separation between source and derived streams; it makes one storage choice dictate the engine contract. |
| Saga or compensating delete | Breaks the move into local transactions without a coordinator. | A compensation after a visible target append can itself be lost or race with a dead-letter consumer. It favors eventual cleanup, not a simple no-loss and duplicate-safe invariant. |
| Keep append-then-checkpoint without an identity | Preserves the original local behavior with minimal code. | The duplicate window remains and legacy records stay opaque. This is only a compatibility baseline, not a candidate for a stronger cross-group guarantee. |

### Reference evidence

Apache Kafka's transaction design combines produced records and consumed
offsets in an atomic unit, uses a persistent transaction log, and requires a
stable transaction identity to recover unfinished work. It also explicitly
limits the guarantee to the transactional broker/consumer boundary rather
than arbitrary external processing ([KIP-98: Exactly Once Delivery and
Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)).
This is evidence for the coordinator-driven alternative, not a requirement
for Runnel's local implementation; adopting it would be a substantially
larger storage and protocol change.

RabbitMQ documents the opposite side of the trade-off. Ordinary dead-letter
exchange republishing removes a message without publisher confirms and can
lose it when the target is unavailable; quorum-queue at-least-once
dead-lettering retains the source until the target confirms, but retries can
produce duplicates and retained pending messages consume source resources
([Dead Letter Exchanges](https://www.rabbitmq.com/docs/next/dlx), [Quorum
Queues: at-least-once dead-lettering](https://www.rabbitmq.com/docs/quorum-queues)).
The comparison makes the trade-off explicit: retaining source progress until
the target is confirmed protects against loss, while retries can duplicate
records and pending work consumes source resources. Any future Runnel design
needs an explicit bound and observability model for that pressure.

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

## Remaining evidence gates

The implemented local slice has focused identity, restart, source-ack failure,
and mismatch coverage. The gates below apply before claiming a broader
duplicate-free local guarantee or any cross-group atomicity; they are not a
retroactive implementation checklist:

1. **Target durability faults:** fault injection at target writes and
   `sync_data` boundaries shows that source progress never advances without a
   durable target record. An uncertain target result leaves source progress
   eligible for retry and is classified conservatively under [ADR 0026](../decisions/0026-semantic-engine-error-classification.md).
2. **Duplicate-safe local recovery:** restart after a successful target append
   and before source-event persistence, then repeat the move. The target has
   one record for the move ID with the original key and payload, and source
   progress advances once. The existing injected source-persistence test is a
   focused slice; exact process-crash timing remains open.
3. **Corruption and format handling:** a same-ID target record with different
   content, malformed or torn target data, an unsupported durable format, or
   an unavailable target produces an explicit storage/corruption outcome and
   does not advance source progress. Legacy target records without an identity
   must remain readable without being falsely reconciled.
4. **Recovery and retention bounds:** identity lookup and reconciliation use
   bounded or explicitly accounted-for indexes/journals, do not scan unrelated
   streams without a documented bound, and retain move evidence until source
   progress no longer depends on it.
5. **Real-process local coverage:** a broker-process test exercises the
   target/source crash window through the public protocol, not only response
   loss after the whole poll has committed. Existing restart and ambiguous
   response tests remain useful but do not establish that exact window.
6. **Cluster same-group coverage:** the existing three-node tests continue to
   verify committed same-group dead-letter movement, restart, leader change,
   follower recovery, and stale-delivery fencing. The result must be described
   as same-data-group atomicity, not general cross-group atomicity.
7. **Future split-group coverage:** if the target ever moves to another
   durable group, add participant failure, coordinator/retry recovery,
   duplicate-command, retention, and ambiguous-client-outcome tests before
   calling the operation atomic.
8. **Semantic wording:** tests and protocol documentation say at-least-once
   for source-to-target and target-consumer delivery. Exactly-once is not
   claimed unless a later decision proves the complete boundary, including
   application side effects.

## Hypotheses and unresolved risks

- The current request-aware target record and rebuilt ID index provide the
  focused local deduplication slice without a second pending-move journal.
  Whether that remains sufficient through partial writes, journal/checkpoint
  compaction, retention, and future format changes is still unverified.
- **Hypothesis:** lazy reconciliation on the next source poll is sufficient
  for correctness. A background reconciler may be needed for operational
  visibility or to make progress when no consumer polls, but it must not
  advance source state on an unconfirmed target.
- A future retention policy must not delete a target record or its move-ID
  evidence while the source move is still unacknowledged. This is a direct
  coupling to the retention work and must be made a durable fence, not a
  best-effort scan.
- Adding provenance later must preserve the local internal identity and
  distinguish intentionally separate moves by independent consumers. It must
  also define how old records with no provenance are represented rather than
  inferring origin from target offsets.
- A target append can be durable while its response is lost, and filesystem
  durability can differ from process-crash behavior. The tests must model
  returned I/O errors, process termination, incomplete frames, and restart;
  a clean in-memory retry is insufficient evidence.
- If source and target storage are placed on different filesystems or devices
  in a future deployment, even an ordered pair of sync calls has no common
  durability boundary. The identity/reconciliation protocol remains useful,
  but full atomicity would require a different accepted design.
- A future named target or placement change can split the current clustered
  data-group boundary. Co-location, reconciliation, an outbox, and a
  coordinator transaction have different failure, retention, and client
  outcome semantics; none is accepted yet.
