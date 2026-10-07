# ADR 0043: Migrate local state to a fresh static cluster offline

- Status: accepted; implementation deferred
- Date: 2026-10-06
- Baseline: `66cacafc8545f010dc710b49a1947c65e8dad53d`
- Primary evidence class: design/research; secondary: correctness/recovery, public-contract
- Related: [growth-from-one-node backlog outcome](../backlog.md#make-growth-from-one-node-to-a-cluster-non-disruptive), [migration design and evidence](../design/single-node-to-cluster-migration.md), [ADR 0004](0004-multi-raft-first-distributed-engine.md), [ADR 0018](0018-safe-replica-recovery-boundary.md), [ADR 0019](0019-clustered-storage-identity.md), [ADR 0027](0027-consumer-scoped-retry-policy.md), [ADR 0029](0029-local-typed-dead-letter-move-identities.md), [ADR 0031](0031-protocol-v2-contract.md), [ADR 0032](0032-static-cluster-peer-mutual-tls.md), [ADR 0033](0033-fixed-consumer-retry-delay.md), [ADR 0034](0034-publish-request-id-content-contract.md), [ADR 0035](0035-first-application-client-security.md), [ADR 0036](0036-retained-history-and-disk-pressure-contract.md), and [ADR 0037](0037-offline-side-by-side-storage-upgrades.md)

## Context

The local and clustered engines implement the same messaging intent with
different durable representations. Local stream frames and consumer journals
cannot be installed as clustered groups or OpenRaft snapshots. Startup selects
an engine for the process lifetime, and the current repository has no
cross-engine export/import schema, durable source fence, migration status, or
target activation protocol. ADR 0004 defers live engine migration. ADR 0037
accepts offline side-by-side storage conversion and a durable first-write
rollback boundary, but leaves engine migration to a separate decision.

At the supplied baseline, the local reader recognizes `RNL1`, `RNL2`, and
`RNL3`, and the baseline writer still emits `RNL1` ordinary records. No source
or migration runtime is eligible yet: this baseline has no migration fence or
export/import path, and its writer format is outside the accepted source
boundary. The future migration-aware release must use `RNL3` as its single
local stream format and emit the state schema accepted by that release's
migration implementation. Stores containing `RNL1` or `RNL2` frames, including
mixed histories, are refused unchanged even if a historical reader can decode
them. There is no deployed user base or backward-compatibility goal that
justifies extending this first product capability to old binaries or
reader-only on-disk formats. The migration-aware source will also store
consumer progress, out-of-order acknowledgements, attempts, configured policy,
and per-offset policy snapshots. Active delivery tokens and local lease
deadlines are volatile. A clustered target stores logical messages in
per-stream data groups, with replicated consumer state and request-ID
deduplication. Current evidence covers recovery within each engine, not
conversion between them.

Relevant reference systems establish different parts of the boundary. The
versioned [PostgreSQL 18 `pg_upgrade` procedure](https://www.postgresql.org/docs/18/pgupgrade.html)
checks compatibility, stops both servers, and defaults to copying so the old
cluster remains available until the new one is used. Kafka 4.3 separates
rolling binary upgrades from metadata finalization and explicitly disallows
metadata downgrade when a release changed metadata; its
[MirrorMaker 2 documentation](https://kafka.apache.org/43/operations/geo-replication-cross-cluster-data-mirroring/)
also treats consumer-group checkpointing as a separate part of cross-cluster
replication. [etcd's learner design](https://etcd.io/docs/v3.6/learning/design-learner/)
keeps a new member non-voting, rejects ordinary client reads and writes until
promotion, and permits promotion only after catch-up, while
[RocksDB's MANIFEST/CURRENT design](https://github.com/facebook/rocksdb/wiki/MANIFEST)
selects a complete durable generation explicitly. Runnel does not need an
old-format bridge for an existing deployed population, so its first
migration source boundary is intentionally the future supported `RNL3`-only
writer format. The [Raft paper](https://raft.github.io/raft.pdf)
explains why a local append log is not a committed replicated-log prefix, and
Google's [F1 asynchronous schema-change paper](https://research.google/pubs/online-asynchronous-schema-change-in-f1/)
is evidence that live mixed-version reads and writes need explicit transition
compatibility. Their different data models and protocols do not establish
Runnel's migration guarantees.

## Decision

### First supported migration mode

The first local-to-cluster migration is an **offline, all-stream, logical
export/import** from one supported local deployment into a fresh, three-voter
static cluster. The target has a new cluster identity, no unrelated streams,
and the peer/application security accepted by ADRs 0032 and 0035. A populated
target, deployment merge, in-place directory conversion, or partial per-stream
cutover is refused.

After read-only planning and capacity checks, the migration stops application
service on the source, acquires and durably records one deployment-wide writer
fence, drains operations admitted before that fence, and captures the final
source boundary. The source stays fenced for the entire export, import,
validation, activation, and endpoint change. This makes outage duration
proportional to copying and validating retained state; the first contract does
not promise a short fence or zero downtime. A live prefix copy, tail catch-up,
dual write, or online mirror is deferred until Runnel has a durable cross-
engine sequence and a separate ordering, acknowledgement, failure, and
fencing proof.

The fence is a durable migration epoch and source-generation marker honored by
every supported source-broker startup and every mutating path. An ordinary
restart of a fenced source must fail closed. Only the migration controller may
resume that migration or explicitly abort it. A stale process or binary that
cannot validate the marker is not a supported writer for the marked source.
The fence covers stream creation, publish, poll/attempt, acknowledgement,
consumer-policy mutation, retry scheduling, and dead-letter movement. Work
already admitted at the fence boundary either completes durably and is in the
captured inventory, or receives a definitive no-effect rejection; the
boundary cannot leave an unknown operation outside both generations.

The final source inventory is made after fencing. It records stream names,
earliest and next logical offsets, retained records and byte counts,
consumer identities and state, policy configuration, request-identity
mappings, source schema descriptors, and content digests. The migration
requires a verified, independently restorable source recovery artifact made
from that frozen boundary before target import begins. Preflight verifies
capacity before accepting the fence; the actual artifact is created and
restore-verified after the final source boundary is frozen. If creation or
verification fails, target activation is forbidden and the source remains
fenced until an explicit abort revalidates it. The source generation itself
remains untouched except for migration authority metadata; it is retained
through the operator-selected recovery window and is not treated as an
independent backup.

### Logical state to preserve

Import preserves, for every stream, its logical name and exact offset range,
record order, publish timestamp, key bytes, opaque payload bytes, and
request-identity mappings. It preserves `RNL3` public request IDs and their
original offsets, comparison inputs, and typed identity; exact retries resolve
to the original offset, and changed key or payload bytes are rejected under
ADR 0034. Any current-schema record without a public request ID remains
without one; migration never invents IDs. An ambiguous pre-fence publish
without a request ID remains ambiguous and is not inferred, deduplicated, or
retried by migration. Internal dead-letter move identities
remain distinct from public request IDs under ADR 0029. Existing dead-letter
streams and any already durable duplicate records are imported as ordinary
history.

The importer preserves logical offsets rather than assigning new ones through
public `Publish`. It verifies the complete source and target inventory before
activation. Unknown or corrupt complete source data, invalid names, consumer
state outside the stream range, conflicting request identities, obsolete
source formats, or any record or state the target schema cannot represent
causes a non-destructive preflight refusal. `RNL1` and `RNL2` are outside the
supported source boundary regardless of historical parser support. An
`RNL3` record from the future supported source outside target limits is also
refused. Migration must not
truncate, skip, rewrite, or make the local source unreadable. The accepted
contract does not turn current parser compatibility into a cross-release
migration promise.

For each `(stream, consumer)`, import preserves committed progress,
out-of-order durable acknowledgements, maximum persisted attempts, configured
policy and its version, and every per-offset policy snapshot. Policy values
include acknowledgement timeout, attempt limit, and ADR 0033 retry delay. Any
durably scheduled retry delay is preserved as bounded remaining delay at the
fence and rebased onto target time; an in-flight local token and its monotonic
lease deadline are not portable. Fencing invalidates local receipts. Their
attempt count and pinned policy remain, and the target applies its normal
post-recovery expiry/redelivery rule before issuing a fresh target receipt.
Source and target broker-wide fallbacks must match wherever a source consumer
or an attempt without a pinned policy depends on them, unless the importer can
preserve the effective values in verified durable target state. A policy
mismatch is a preflight refusal, not a silent policy change.

The target preserves source timestamps and offset order. Any derived timestamp
index is rebuilt from imported records; under ADR 0038, a timestamp query must
still select the lowest matching logical offset even when publish timestamps
regress, and must retain its specified no-match/history-completeness outcomes.
The current unlimited-retention behavior transfers the full `[0, next)` range.
If a future source has retention floors or replay pins, migration must preserve
the floor, next offset, replay eligibility, and pins or refuse it. Migration
does not change retention, ordering, acknowledgement, retry, or dead-letter
policy.

### Authority, activation, and rollback

Authority follows one durable chain:

1. **Before the fence:** the source is the only writable and serving
   generation; target preparation and planning are read-only with respect to
   source state.
2. **Fenced and importing:** the source is frozen and ordinary source startup
   refuses service. The target is a staging generation and serves no
   application traffic. An interrupted import resumes from a verified bounded
   checkpoint or is discarded as unreferenced staging; it never appears as an
   empty or partially migrated cluster.
3. **Validated:** source and target logical inventories, schemas, per-stream
   digests, consumer state, and request identities agree. Every configured
   target voter has recovered the same migration generation and imported
   state. Until that all-node check passes, target readiness and activation
   are false.
4. **Activated and reversible:** a durable target metadata record selects the
   target generation and records that it is read-only. The endpoint owner
   explicitly records and routes to that generation, then verifies readiness.
   The source remains fenced. If activation or endpoint ownership is
   ambiguous, service remains unavailable; reachability does not select an
   authority.
5. **First target mutation:** before any target-only durable mutation can be
   accepted, a cluster-wide durable `write-pending` transition closes the
   source rollback path. This includes stream lifecycle and consumer-state
   changes, poll assignments/attempts, acknowledgements, retry schedules,
   publishes, and dead-letter outcomes. Mutations are serialized behind that
   transition. A confirmed durable effect advances to `active-committed`; a
   proven no-effect result may durably return to `active-reversible`; an
   ambiguous result remains `write-pending`, fails closed to further mutation,
   and forbids source rollback until the target outcome is reconciled.
6. **After the first target-only durable mutation:** recovery is forward-only
   from the target's replicated state or a verified target recovery artifact.
   The source remains a stale retained copy, not a rollback option. Recovery
   through a new migration or separately accepted reverse conversion is a
   different operation. Cleanup of the source is explicit and may occur only
   after the configured recovery window and backup policy permit it.

This applies ADR 0037's offline source immutability, explicit activation,
durable pending/commit boundary, and fail-closed ambiguous-outcome rules to an
engine migration. The storage-upgrade artifact compatibility matrix is not
reused as proof that local and clustered schemas are interchangeable; this ADR
adds the cross-engine logical-state equality and all-voter activation gates.
Target reads during `active-reversible` may be used for validation, but no
target operation that can change durable state is accepted before the
write-pending gate. The source cannot serve reads or writes after its fence.

The endpoint owner is explicit deployment configuration, outside a Raft
transaction. A lost coordinator after target activation leaves source fenced
and target read-only until the owner reconciles the durable migration record
and endpoint generation. The procedure prefers bounded downtime over allowing
two writable generations. Old source binaries, automatic downgrade, implicit
directory selection, and pointer rollback after target state changes are not
supported.

### Bounded work and operator evidence

Migration work is proportional to retained bytes, but concurrent work,
in-memory buffering, per-chunk state, retry queues, and temporary storage are
bounded by explicit configured limits and preflight reserve checks. Records and
consumer state are streamed in checksummed, idempotent chunks; no whole stream
or deployment is buffered in memory. The importer is throttled so it cannot
consume all target request, peer, or storage capacity. It refuses insufficient
source-backup, target-replica, journal/snapshot, or staging reserve before the
source fence is accepted. No migration-duration, throughput, or arbitrary
retained-size claim is made until measured under a resource-scoped workload.

Operator status identifies source/target generations and schemas, migration
phase and fence epoch, source boundary, per-stream and consumer progress,
digests, target-voter readiness, last failure, endpoint generation, backup and
reserve status, and whether rollback remains eligible. Metrics use bounded
labels; detailed stream, consumer, and migration identities are available only
in bounded status output or structured logs. Status is not inferred from a
listening socket or process health.

Application traffic after cutover uses the negotiated protocol and outcome
contract in ADR 0031 with the TLS and authorization boundary in ADR 0035; peer
traffic follows ADR 0032. Migration control and status are operator-authorized
operations and do not add storage paths, Raft identities, or physical layout
to the application protocol. No v1 or mixed-version fallback is implied.

### Explicit non-promises

The first migration does not promise zero downtime, short downtime, live
copy/tail, dual-write, merge into a populated target, partial stream
activation, dynamic membership, automatic replica replacement, arbitrary
historical-record support, mixed-version operation, automatic downgrade, or a
public API exposing paths, Raft identity, or physical layout. It does not
claim production availability for the early static clustered backend beyond
its separately accepted and tested recovery guarantees.

## Rationale and alternatives

The offline fence is deliberately stricter than the exploratory design's
short-final-fence suggestion. Current local state has per-stream operation
serialization but no durable deployment-wide sequence or migration epoch, and
consumer journals, message appends, and dead-letter moves do not share one
cross-engine commit boundary. A live prefix plus tail would have to prove a
consistent cut across all of them, including stream creation and request-ID
acceptance. Until that protocol exists, the full offline boundary is the
smallest path that gives one auditable source snapshot and no dual writers.
The cost is an outage proportional to transfer and validation time; benchmark
evidence will establish its operational range before support is advertised.

PostgreSQL's stopped-server, preflighted copy path informs the maintenance
window and source-preserving generation choice, but unlike `pg_upgrade`, Runnel
must logically translate data and consumer state between different engines.
Kafka MirrorMaker demonstrates separate record and consumer-checkpoint
transfer, but its source and target share Kafka's topic/partition and offset
vocabulary; Runnel has to preserve logical offsets and attempt/policy state
across dissimilar local and Raft schemas. etcd learners inform readiness before
promotion, while RocksDB's manifest informs explicit-generation selection, but
Runnel's target also needs an all-stream activation record and broker-level
first-write fence. Raft snapshots cannot serve as source interchange because
the local engine has no Raft log index, term, membership, or committed
snapshot boundary. F1's transition compatibility lesson supports deferring
live mixed writers until every operation has a proven compatibility path.

The alternatives were:

1. **Live pre-copy with final catch-up:** deferred. It could reduce outage,
   but Runnel lacks a durable cross-engine change sequence, tail cursor,
   fencing epoch, and tests for racing message and consumer-state changes.
2. **Dual-write:** rejected for this slice. Independent local and Raft commits
   cannot atomically acknowledge the same publish, acknowledgement, attempt,
   or dead-letter move.
3. **Copy files, install a local log as an OpenRaft snapshot, or republish via
   public `Publish`:** rejected. These do not preserve clustered identity,
   source timestamps/offsets, consumer state, request identities, or the
   target's normal replicated-commit boundary.
4. **Asynchronous mirror or generic common snapshot format:** deferred. A
   mirror needs a sequence and reconciliation contract; a common interchange
   format has no second concrete engine consumer yet.

## Consequences and implementation gates

This decision accepts a migration behavior contract, not runtime support. The
current source and target engines remain unchanged, and the backlog item stays
open until the following evidence exists:

- versioned source/target schema compatibility and read-only preflight accept
  stores from the future `RNL3`-only source release and reject `RNL1`, `RNL2`,
  mixed histories, obsolete consumer-state schemas, corrupt, mismatched, and oversized state
  without source mutation; fixtures cover request identities, timestamp
  regressions, consumer progress, attempts, and pinned policies;
- migration preserves logical bytes, exact offsets/order/timestamps, retained
  range, request-ID conflict behavior, ordinary and grouped consumer state,
  retry timing, and dead-letter history, with all target voters recovering one
  validated generation before readiness;
- real-process local and three-node tests inject termination at fence, every
  chunk/checkpoint boundary, consumer import, per-voter validation, target
  activation, endpoint update, first-write pending, and cleanup; every restart
  either resumes verified work or fails closed with one explicit authority;
- stale source restarts and delayed source writes/acknowledgements are rejected
  after fencing; stale target receipts cannot advance imported progress;
- the target remains read-only until its generation is explicitly selected,
  and tests establish successful, proven-no-effect, and ambiguous first-write
  resolution plus the exact rollback boundary;
- the status surface, bounded transfer/queue/memory behavior, disk reserve
  refusal, backup verification, cancellation, and orphan cleanup are tested;
- the actual static-cluster recovery path preserves imported state across
  process restart, leader change, and snapshot recovery. Test-only replacement
  behavior is not advertised as production support; and
- a sequential, resource-scoped migration benchmark measures fence duration,
  copy/validation cost, peak memory and temporary space, per-node target cost,
  resume work, and post-migration behavior before an operational size or
  duration range is published.

The design note carries the implementation outcome matrix, fault scenarios,
reference evidence, and cost benchmark plan. Those mechanisms remain
illustrative until implementation unless this ADR states the behavior above.

## References

- [PostgreSQL 18 `pg_upgrade`](https://www.postgresql.org/docs/18/pgupgrade.html)
- [Apache Kafka 4.3 upgrade guide](https://kafka.apache.org/43/getting-started/upgrade/)
- [Apache Kafka 4.3 Geo-Replication / MirrorMaker 2](https://kafka.apache.org/43/operations/geo-replication-cross-cluster-data-mirroring/)
- [etcd 3.6 learner design](https://etcd.io/docs/v3.6/learning/design-learner/)
- [RocksDB MANIFEST and CURRENT](https://github.com/facebook/rocksdb/wiki/MANIFEST)
- [Raft: In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Online, Asynchronous Schema Change in F1](https://research.google/pubs/online-asynchronous-schema-change-in-f1/)
