# TD-008 static-cluster evidence and retirement gates

- Status: scoped evidence review; no runtime or compatibility decision
- Last reviewed: 2026-10-02
- Baseline: `origin/main` `a42fbfd207b1a26f4b52fce8110311453d55a009`
- Scope: the current static Multi-Raft slice and the outcome evidence needed before membership, placement, or replica replacement can be treated as supported
- Related: [TD-008](../tech-debt.md#td-008-distributed-raft-backend-is-an-early-static-cluster-implementation), [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md), [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md), [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md), [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md), [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md), [ADR 0019](../decisions/0019-clustered-storage-identity.md), [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md), and [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md)

This note makes the current clustered evidence legible and separates it from
the production outcomes that TD-008 still lacks. It does not authorize a
membership protocol, a placement algorithm, a replacement API, or a public
topology concept. Code and tests at the baseline remain authoritative.

## Observed baseline

### What the current slice establishes

| Area | Evidence in the baseline | What that evidence does not establish |
| --- | --- | --- |
| Group topology | `GroupManager` opens the metadata group, restores persisted data groups, and materializes a data group per stream as needed. A newly initialized group takes its voter set from the configured peer map. The accepted initial direction is three static voters, but server configuration requires a nonempty peer list containing the local node and does not enforce exactly three; the process tests below use three nodes. [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L94-L181) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L184-L215) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L358-L433) [`bootstrap.rs`](../../crates/runnel-server/src/bootstrap.rs#L101-L122) [`bootstrap.rs`](../../crates/runnel-server/src/bootstrap.rs#L158-L178) [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md) | The peer map is static runtime configuration, not a supported membership authority or reconfiguration API. OpenRaft membership is persisted in group state, but there is no supported add, remove, learner, promotion, or replacement lifecycle. |
| Stream lifecycle | The metadata group records `Creating`; the manager prepares the stream data group on each configured peer, initializes it with that same peer set, initializes the stream state, then records `Active`. Retried creation resumes this idempotent reconciliation path. [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L358-L433) [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md) | Metadata and data-group activation are separate replicated transitions, not one atomic transaction. A partial create is retryable, but the path does not coordinate membership or placement changes during creation. |
| Topology-free access | Requests can enter any configured broker node. The engine resolves the operation's group leader and forwards to a configured peer; forwarding has bounded attempts and per-RPC timeouts, and group/node assignment remains internal. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L971-L1204) [`forwarding.rs`](../../crates/runnel-raft/src/forwarding.rs#L10-L119) | A forwarding timeout can occur after proposal or apply. The provisional protocol does not expose a stage-aware outcome or a general operation-resolution protocol. |
| Durable messaging path | Data-group state includes retained messages, ordinary consumer progress, grouped progress and out-of-order acknowledgements, per-consumer retry policy, attempts, in-flight member ownership and tokens, a replicated lease-clock floor, request-ID deduplication, and dead-letter counters. Applied state is journaled before materialization; checkpoints and full state-machine snapshots provide recovery inputs. A real-process test updates the consumer policy from version 1 to 2 while a delivery is in flight, fails the leader, then confirms version 2 is present on the new leader while the old delivery still uses its pinned version-1 attempt and expiry limits. [`state_machine.rs`](../../crates/runnel-raft/src/state_machine.rs#L195-L211) [`state_machine.rs`](../../crates/runnel-raft/src/state_machine.rs#L296-L335) [`delivery.rs`](../../crates/runnel-raft/src/delivery.rs#L11-L33) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L438-L475) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L710-L750) [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1243-L1446) | Request IDs are currently scoped per stream and map to an offset without a stored payload fingerprint; conflict handling is not a final compatibility contract. An unacknowledged client operation can still be ambiguous at the wire boundary. |
| Process restart and one-node failure | `three_process_cluster_replicates_and_recovers_after_failures` publishes and consumes through different nodes, retries a publish ID, restarts a follower with its state, stops the leader, continues through the new leader, and observes later records. The separate group-delivery failure test also restarts a failed node and checks terminal acknowledgement after rejoin. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L153-L597) [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L927-L1239) | These are controlled three-process scenarios, not a complete partition, disk, corruption, delayed-response, or repeated-failure matrix. The empty-directory case is covered only by the separately gated experiment below, not by the default restart path. |
| Shared-consumer recovery | `three_process_cluster_preserves_group_delivery_through_replica_restart` checks durable grouped leases and tokens across follower restart, out-of-order acknowledgement, expired-token rejection, redelivery, and durable progress after another restart. `three_process_cluster_reassigns_group_delivery_after_node_failure` covers reassignment after leader failure, stale-token rejection, and terminal acknowledgement after rejoin. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L599-L925) [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L927-L1239) [`delivery.rs`](../../crates/runnel-raft/src/delivery.rs#L11-L33) | Delivery ownership for a member is persisted only while an offset is in flight; there is no separate durable member registration, incarnation, graceful-leave, churn, or placement-aware rebalance protocol. |
| Snapshot recovery | Snapshots compact consensus history while retaining the complete materialized group state; the current policy snapshots after 32 log entries, keeps a four-entry suffix, and caps peer chunks at 64 KiB. The explicitly enabled replacement experiment interrupts transfer repeatedly and checks retained records, consumer progress, metrics, subsequent failover, and process liveness. [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L23-L31) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L710-L750) [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1448-L1649) [`justfile`](../../justfile#L47-L51) [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md) [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) | The experiment requires `test-replacement-recovery`, which enables permissive follower-log rollback only for this test. It exercises one empty directory with the same voter ID, but does not make that an allowed operator replacement procedure or provide resumable transfer, promotion, fencing, or an operational recovery process. [`runnel-raft/Cargo.toml`](../../crates/runnel-raft/Cargo.toml#L1-L12) [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1448-L1450) |
| Identity and storage safety | `storage.json` records format version, cluster name, and node ID; startup rejects mismatched identity, legacy layout, missing identity on existing clustered data, malformed or contradictory group manifests, and invalid persisted Raft/state-machine structures before opening groups. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L40-L110) [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L887-L929) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L671-L757) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L680-L700) [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs#L93-L95) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L839-L872) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1638-L1692) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L2093-L2116) [ADR 0019](../decisions/0019-clustered-storage-identity.md) | This guards a persisted directory against configured cluster/node mismatches; it is not a node-incarnation or membership transition. A new empty directory can initialize with an existing node ID, and identity validation does not prove that its data is caught up or safe to serve. |

The strongest current conclusion is therefore narrow: the configured-peer
static Multi-Raft slice is a useful correctness baseline. Current real-process
evidence uses three nodes and covers replicated stream operations, leader
failover, preserved-state restart, and an explicitly experimental snapshot
transfer. The test topology does not establish a universal three-node engine
invariant or failure tolerance for other peer counts. It is not evidence for
elastic capacity, automatic placement, or supported replacement of a lost
replica.

### Accepted constraints and deliberate limits

- [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md) accepts
  three static voters, a metadata group, and one data group per stream as the
  first distributed direction.
- [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md) accepts a
  reconciled stream lifecycle and keeps group and placement identities out of
  the public model.
- [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md) accepts
  snapshot-based consensus-history compaction and recovery mechanics, while
  leaving snapshot tuning and resumable transfer unfinished.
- [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md) accepts
  replicated delivery leases, tokens, attempts, and progress; it does not add
  durable consumer-member registration.
- [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md) accepts a
  durable consumer-scoped retry policy that is applied through the same
  replicated data-group state.
- [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) keeps empty
  replica recovery test-only until a controlled replacement lifecycle exists.
- [ADR 0019](../decisions/0019-clustered-storage-identity.md) requires
  clustered storage identity to fail closed on mismatches; this local marker
  does not record a node incarnation or membership transition.
- [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md)
  accepts hidden movable placement units as a future boundary while leaving
  their mapping, replication engine, and split policy undecided.

The [Raft follower recovery and replacement research](../research/raft-recovery-and-replacement.md)
records the relevant external comparison and distinguishes a three-process
development profile from an engine-wide topology invariant. That note's source
baseline predates this review, so the implementation claims above were checked
against this document's baseline. OpenRaft's documented follower-log rollback
warning supports keeping permissive rollback out of the default build; Kafka,
Redpanda, and learner-based reconfiguration designs support treating normal
restart and controlled replacement as separate operations. Those sources
inform the gates below but do not define Runnel's protocol.

## Inferences and recommendations

### Preserve the current sequence

The safest dependency order is:

1. define and prove a supported recovery/replacement boundary for an existing
   data group, including the identity and membership transitions that keep a
   replica non-authoritative until validated catch-up;
2. define the failure semantics and quorum boundary for those transitions;
3. introduce placement movement or splitting only after recovery and
   membership safety are proven;
4. measure group density, skew, recovery cost, and resource bounds before
   replacing one-group-per-stream placement.

This is a recommendation from the observed coupling between group membership,
snapshot recovery, and stream placement. The recovery and membership items are
related outcomes, not independent prerequisites: replacement cannot make a
replica authoritative before its identity, catch-up, and fencing boundary is
defined. This is not an implementation task list. Placement work that starts
before replacement and fencing are defined would make a lost-replica recovery
problem depend on an additional movement protocol.

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
peer-map backend as a development and correctness baseline, but not to claim
production-grade clustering. The three-process tests are evidence for that
configured test topology, not proof of a fixed engine-wide voter count. The
next implementation decision should define and test one controlled replacement
lifecycle, including identity, membership, catch-up, and fencing transitions
(M1–M4 and R1–R4), before attempting dynamic placement (P1–P4). The existing
backlog outcomes for
membership/failover, placement, storage growth, overload, compatibility,
security, and observability remain separate retirement work; passing the
current cluster smoke test cannot close them collectively.

## Refactor and planning assessment

The current `GroupManager`, forwarding, state-machine, and real-process test
boundaries were reviewed. No independent local refactor is identified: a
durable membership authority, a replacement lifecycle, and a placement map
would each be substantive design changes already owned by TD-008 and the
related backlog/ADR records. Introducing part of one during an evidence refresh
would create an unaccepted boundary rather than reduce risk.

No backlog or tech-debt update is needed. TD-008 and its linked outcomes
already capture the unfinished membership, replacement, placement, storage,
and observability work; this refresh updates observed evidence without
retiring or expanding those commitments.

## Verification

This refresh changes documentation only. It was checked with `git diff
--check` and a local Markdown path/fragment scan. No runtime tests, cluster
tests, or benchmarks were run for this document update. The linked default
three-process evidence is in `cluster_smoke`; `just cluster-test` is the
documented real-process workflow, while `just cluster-replacement-test` is the
separate opt-in experiment and does not establish a supported replacement
contract.

## Sources

- [OpenRaft feature flags](https://docs.rs/openraft/latest/openraft/docs/feature_flags/)
- [OpenRaft FAQ: lost data and follower replacement](https://docs.rs/openraft/latest/openraft/docs/faq/)
- [Apache Kafka replication design](https://kafka.apache.org/42/design/design/)
- [Redpanda Raft group reconfiguration](https://docs.redpanda.com/streaming/25.1/manage/raft-group-reconfiguration/)
- [etcd learner design](https://etcd.io/docs/v3.6/learning/design-learner/)
- [Runnel's recovery and replacement research](../research/raft-recovery-and-replacement.md)
