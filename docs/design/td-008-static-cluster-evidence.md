# TD-008 static-cluster evidence and retirement gates

- Status: scoped evidence review; no runtime or compatibility decision
- Last reviewed: 2026-09-06
- Baseline: `origin/main` `1619035024efbac1aa2b6ea623d0477e8b46c511`
- Scope: the current three-node Multi-Raft slice and the outcome evidence needed before membership, placement, or replica replacement can be treated as supported
- Related: [TD-008](../tech-debt.md#td-008-distributed-raft-backend-is-an-early-static-cluster-implementation), [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md), [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md), [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md), and [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md)

This note makes the current clustered evidence legible and separates it from
the production outcomes that TD-008 still lacks. It does not authorize a
membership protocol, a placement algorithm, a replacement API, or a public
topology concept. Code and tests at the baseline remain authoritative.

## Observed baseline

### What the current slice establishes

| Area | Evidence in the baseline | What that evidence does not establish |
| --- | --- | --- |
| Group topology | `GroupManager` opens one metadata group and lazily opens one data group per stream. Each newly initialized group uses the configured peer map as its membership. [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L112-L182) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L369-L427) | The configured peer map is not a dynamic membership authority. There is no supported add, remove, learner, promotion, or replacement lifecycle. |
| Stream lifecycle | Metadata records `Creating`; the configured nodes prepare the data group, the group initializes its stream state, and metadata is then activated. Retries can reconcile an already-created state. [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L358-L427) [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md) | Metadata and data-group activation are not one atomic transaction. The current path does not cover membership or placement changes during creation. |
| Topology-free access | Requests may enter any node. The engine resolves the relevant group's leader and forwards to a configured peer, with bounded attempts and no public group or node assignment. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L888-L1085) [`forwarding.rs`](../../crates/runnel-raft/src/forwarding.rs#L32-L118) | A forwarding timeout can occur after proposal or apply. The provisional protocol does not expose a stage-aware outcome or a general operation-resolution protocol. |
| Durable messaging path | The data-group state machine applies publishes, consumer progress, attempts, delivery ownership, and dead-letter transitions through the replicated group. Publish request IDs are retained for the current deduplication path. [`state_machine.rs`](../../crates/runnel-raft/src/state_machine.rs#L164-L380) | The current request-ID scope and fingerprint policy are not a final compatibility contract. An unacknowledged client operation can still be ambiguous at the wire boundary. |
| Process restart and one-node failure | `three_process_cluster_replicates_and_recovers_after_failures` publishes and consumes through different nodes, restarts a follower, stops the leader, continues on a new leader, and observes the committed records. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L153-L597) | This is one controlled process scenario, not a complete partition, disk, corruption, delayed-response, or repeated-failure matrix. It does not prove recovery after an empty or inconsistent replica is started with the same voter identity. |
| Shared-consumer recovery | The cluster tests cover replicated grouped leases and tokens across follower restart, reassignment after node failure, stale-token rejection, and post-rejoin terminal acknowledgement. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L599-L908) [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L910-L1124) | Consumer membership is still transient request state. Durable member registration, graceful leave, bounded churn, and placement-aware rebalancing remain open. |
| Snapshot recovery | Snapshot chunks are bounded. The opt-in replacement experiment retries interrupted transfers from byte zero and checks that the replacement eventually recovers records, consumer state, and post-recovery leader failure. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1226-L1429) [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) | The experiment is test-only. It does not make an erased directory a supported replacement, and it does not provide resumable transfer, promotion, fencing, or an operational recovery procedure. |
| Identity and storage safety | Clustered storage records cluster and node identity and preflight rejects unsupported or contradictory layouts before opening them. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L47-L93) [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L804-L846) | Identity validation prevents accidental reuse; it is not membership reconfiguration or proof that a replacement has caught up. |

The strongest current conclusion is therefore narrow: a configured three-node
cluster is a useful correctness baseline for replicated stream operations,
leader failover, preserved-state restart, and an explicitly experimental
snapshot transfer. It is not evidence for elastic capacity, automatic
placement, or replacing a lost replica.

### Accepted constraints and deliberate limits

- [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md) accepts
  three static voters, a metadata group, and one data group per stream as the
  first distributed direction.
- [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md) accepts a
  reconciled stream lifecycle and keeps group and placement identities out of
  the public model.
- [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) keeps empty
  replica recovery test-only until a controlled replacement lifecycle exists.
- [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md)
  accepts hidden movable placement units as a future boundary while leaving
  their mapping, replication engine, and split policy undecided.

The [Raft follower recovery and replacement research](../research/raft-recovery-and-replacement.md)
provides the relevant external comparison. OpenRaft's documented follower-log
rollback warning supports keeping permissive rollback out of the default
build; Kafka, Redpanda, and learner-based reconfiguration designs support
treating normal restart and controlled replacement as separate operations.
Those sources inform the gates below but do not define Runnel's protocol.

## Inferences and recommendations

### Preserve the current sequence

The safest dependency order is:

1. establish a supported recovery/replacement boundary for one existing data
   group;
2. establish durable membership transitions and their failure semantics;
3. introduce placement movement or splitting only after the first two are
   independently proven;
4. measure group density, skew, recovery cost, and resource bounds before
   replacing one-group-per-stream placement.

This is a recommendation from the observed coupling between group membership,
snapshot recovery, and stream placement. It is not an implementation task
list. Placement work that starts before replacement and fencing are defined
would make a lost-replica recovery problem depend on an additional movement
protocol.

### Keep application intent stable

Future membership and placement work should preserve stream names, logical
offsets, consumer progress, delivery fencing, replay intent, and producer
retry identity. Node IDs, Raft groups, replica sets, placement units, and
epochs may be useful internal evidence, but they must not become required
application inputs. This is consistent with the accepted engine boundary and
the placement identity decision; the exact representation is intentionally
deferred.

## Outcome and evidence gates

The following gates are outcome requirements. A future implementation may use
different internal mechanisms if it proves the same safety and operational
boundaries.

### Membership and failover

**M1 — authoritative identity and configuration.** Cluster identity, node
incarnation, group identity, and the active membership are durable and
validated. An old, unknown, or contradictory participant cannot become an
authority by reusing a node ID or editing a local configuration. Evidence must
include restart and mismatch cases before and after a membership transition.

**M2 — safe transition.** Adding, promoting, demoting, or removing a
participant preserves a failure-surviving quorum at every committed boundary.
The transition must define the overlap or joint-configuration rule, the
behavior when the target is unavailable, and the point at which the old
participant is fenced. A partitioned old configuration must not commit a
conflicting record.

**M3 — real-process lifecycle.** A three-process test must cover a target
joining, catching up, becoming eligible to serve, and leaving or being
replaced. Kill, pause, restart, delayed-message, and response-loss points
must be exercised around catch-up and activation. The public stream and
consumer operations must remain usable without operator-supplied placement.

**M4 — bounded operation.** Transition bandwidth, storage amplification,
retry work, queueing, and recovery time are observable and bounded for a
documented workload. The cluster must fail closed or report an explicit
operational condition when the required quorum or recovery budget is absent.

### Placement and scale

**P1 — hidden, durable placement map.** The placement unit is independent of
the public stream name, has a durable identity and generation, and is
resolved from committed metadata. A client does not need a node, group, shard,
or partition assignment. Existing streams retain their logical identity while
their physical assignment changes.

**P2 — prepare, cut over, fence.** A move or split makes the target state
complete and verifiable before activation. The cutover has a committed
generation or equivalent fencing boundary; stale writers, readers, delivery
owners, and routes receive explicit outcomes. A crash before, during, or after
cutover leaves one authoritative map and a recoverable source or target.

**P3 — semantic preservation.** Movement preserves acknowledged records,
logical offsets, replay eligibility, consumer checkpoints, delivery attempts,
stale-token rejection, and request-identity resolution. If an ordering domain
is split, its ordering rule and cross-unit consumer/replay behavior must be
explicit before the split is enabled.

**P4 — measured distribution.** Uniform, skewed, many-cold-stream, and
hot-ordering-domain workloads measure placement balance, movement volume,
leadership concentration, tail latency, memory, file descriptors, storage,
and recovery time. A placement mechanism is not accepted merely because it
reduces the number of groups in a synthetic case.

### Replica recovery

**R1 — supported restart.** Restarting a process with its intact durable state
returns the same cluster and node identity, catches up from the authoritative
log or snapshot, and serves only committed state. A follower restart and a
leader restart must preserve acknowledged progress and must not regress
consumer checkpoints or deduplication outcomes.

**R2 — controlled replacement.** A missing or inconsistent replica is not
immediately treated as a voter. It must enter a documented non-authoritative
recovery state, receive validated state, catch up, and become eligible only
after identity, progress, and serving checks pass. The cluster must define
what happens if recovery is interrupted, repeated, or impossible.

**R3 — snapshot and journal boundaries.** Snapshot installation, journal
replay, compaction, and retained-history recovery have explicit validation and
atomic activation boundaries. A failed or partial transfer cannot erase the
last authoritative copy, expose a gap, or make an old state authoritative.
Repeated interruptions must have bounded work or a documented operator
fallback; retrying from byte zero is evidence of correctness, not proof of
efficient recovery.

**R4 — fault matrix and observability.** Real-process tests cover process
failure, partition, delayed and dropped peer traffic, disk-full or write
failure, corruption, response loss, and restart at each recovery boundary.
They record readiness, leader/quorum state, recovery progress, bytes, retries,
and final serving status without relying on a child-process handle as proof
of liveness.

## Retirement recommendation

TD-008 remains open. The current evidence is sufficient to keep the static
three-node backend as a development and correctness baseline, but not to
claim production-grade clustering. The next implementation decision should
select and test one controlled replacement lifecycle (M1–M4 and R1–R4) before
attempting dynamic placement (P1–P4). The existing backlog outcomes for
membership/failover, placement, storage growth, overload, compatibility,
security, and observability remain separate retirement work; passing the
current cluster smoke test cannot close them collectively.

## Refactor and planning assessment

The touched implementation was reviewed at the `GroupManager`, forwarding,
state-machine, and real-process test boundaries. No safe local refactor is
included: the apparent extractions—a durable membership authority, a
replacement state machine, and a placement map—are the substantive design
changes already owned by TD-008 and the related backlog/ADR records. Splitting
them out now would create an unaccepted abstraction rather than reduce risk.

## Verification

This update changes documentation only. The focused evidence gate is the
existing `just isolated cluster-test` real-process workflow plus
`cargo test -p runnel-raft`; no benchmark result is implied by this note.

## Sources

- [OpenRaft feature flags](https://docs.rs/openraft/latest/openraft/docs/feature_flags/)
- [OpenRaft FAQ: lost data and follower replacement](https://docs.rs/openraft/latest/openraft/docs/faq/)
- [Apache Kafka replication design](https://kafka.apache.org/42/design/design/)
- [Redpanda Raft group reconfiguration](https://docs.redpanda.com/streaming/25.1/manage/raft-group-reconfiguration/)
- [etcd learner design](https://etcd.io/docs/v3.6/learning/design-learner/)
- [Runnel's recovery and replacement research](../research/raft-recovery-and-replacement.md)
