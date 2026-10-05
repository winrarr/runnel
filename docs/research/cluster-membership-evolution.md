# Cluster membership evolution in the early Multi-Raft backend

- Status: exploratory source-backed research; no membership API, policy, or
  runtime change is accepted
- Last reviewed: 2026-10-05
- Baseline: `8188d7910158d3316fa6fa2032bd58d7c2c4f341`
- Primary evidence class: source-backed design research
- Scope: safe addition and removal of nodes from Runnel's current metadata and
  per-stream Raft groups, including reconfiguration progress across groups
- Related: [Make membership and failover behavior safe](../backlog.md#make-membership-and-failover-behavior-safe),
  [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md),
  [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md),
  [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md),
  [ADR 0019](../decisions/0019-clustered-storage-identity.md),
  [TD-008](../tech-debt.md#td-008-distributed-raft-backend-is-an-early-static-cluster-implementation),
  [TD-008 static-cluster evidence](../design/td-008-static-cluster-evidence.md),
  and [Raft follower recovery and replacement](raft-recovery-and-replacement.md)

This note compares primary consensus and broker documentation with the code at
the recorded baseline. It makes the distinction between node identity, a
group's committed membership, and a cluster-wide membership operation
explicit. Its recommendations are hypotheses for later review, not a Runnel
compatibility promise or implementation plan.

## Question and conclusion

What evidence and mechanisms would be needed before a Runnel operator could
safely add or remove a broker from the current static cluster?

At this baseline, the configured peer map is a bootstrap and routing input;
each initialized OpenRaft group persists its own membership. Runnel has no
supported learner, promotion, voter removal, address update, or cluster-wide
membership operation. The existing tests establish preserved-state restart
and leader-failure behavior for a static three-process test profile, not
membership change or network-partition safety.

The pinned OpenRaft 0.9.25 offers learner and joint-membership primitives, so a
restricted experiment is technically near-term. It does not provide the
missing Runnel coordination across one metadata group and every existing
stream data group, nor does it settle process identity, transport routing,
stream creation during a transition, or operator recovery. A useful future
direction to investigate is learner catch-up followed by the library's
committed joint/final membership change, with a durable Runnel-level operation
coordinating group-by-group progress. This is a recommendation, not an
accepted membership design.

## Observed Runnel baseline

| Concern | Observed at the baseline | What remains unestablished |
| --- | --- | --- |
| Node and disk identity | `storage.json` records a format version, cluster name, and node ID; startup rejects mismatches. [ADR 0019](../decisions/0019-clustered-storage-identity.md) and [`PersistedStorageMetadata`](../../crates/runnel-raft/src/engine.rs#L40-L50) | It does not identify a process incarnation, a membership generation, or prove that a process using the same ID is the unique live owner of that identity. |
| Initial membership | `PersistentEngine::open_with_config` initializes the metadata group from the configured peer map. A new stream data group is initialized from the same map. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L931-L945) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L394-L450) | The map is not an API for changing the membership already persisted by an initialized group. There is no supported add/remove/learner/promotion path. |
| Group topology | The accepted initial topology is one metadata group and one data group per stream, initially sharing static voters. [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md) [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md) | A node change affects multiple independently committed Raft memberships; there is no atomic transaction spanning all groups. |
| Stream creation | Metadata records `Creating`, peers prepare a data group, that group is initialized, and metadata later records `Active`. [`reconcile_stream`](../../crates/runnel-raft/src/group_manager.rs#L394-L450) | This existing reconciliation does not serialize creation against a membership operation. A newly created group could otherwise start from a stale peer map while older groups have changed. |
| Network descriptors | OpenRaft's `BasicNode` address is used by the TCP network factory, with a configured-peer-map fallback. [`outbound.rs`](../../crates/runnel-raft/src/network/outbound.rs#L60-L75) | An updated consensus descriptor alone would not update every Runnel address source, peer preparation path, or forwarding path. Address-to-node identity must be verified; OpenRaft warns that a route to the wrong node can cause inconsistency or data loss. |
| Process tests | `three_process_cluster_replicates_and_recovers_after_failures` restarts a follower with its original directory, commits while it is down, then stops a leader and continues through a survivor. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L165) | This is process-stop evidence with static membership. It does not test partitions, membership changes, stale duplicate identities, or partial transitions across groups. |

The configuration's peer count is not an enforced engine-wide three-node
invariant. The three-node test profile must not be read as a guarantee for
other cluster sizes or failure conditions. TD-008 separately tracks dynamic
membership, replacement, fencing, authentication, and production operations
as unfinished work.

## Keep identity and membership concepts distinct

The sources and current implementation point to four related but different
things that a future workflow would need to represent and verify:

1. **Stable node identity** names the logical broker in persisted Raft logs,
   storage, and membership. Reusing a node ID for an empty or cloned directory
   is not proof that it is the original participant or that its state is
   current.
2. **Process incarnation** distinguishes one running owner of a stable node
   identity from an old process that returns, or a second process started with
   copied identity. The current storage marker has no incarnation field.
3. **Node descriptor** supplies network information for reaching that identity.
   An address change is not itself a voter-set change, and a correct voter ID
   sent to the wrong process does not preserve consensus safety.
4. **Per-group membership** is the committed voter/learner configuration for
   one metadata or data Raft group. It is replicated state and can differ
   temporarily across groups while a cluster-level operation is in progress.

An operator request such as “remove node 2” therefore needs a fifth, higher
level: a durable cluster operation whose desired outcome is to update the
relevant per-group memberships and routing/creation state. That coordinator is
not present today. This is an inference from the accepted multi-group topology
and its separately committed group state, not a proposed API shape.

## Source comparison

| Source | Sourced mechanism or operational lesson | Relevance and limit for Runnel |
| --- | --- | --- |
| Ongaro and Ousterhout, [Raft §6](https://raft.github.io/raft.pdf#page=10) | Directly switching old and new voter sets is unsafe because nodes may see the switch at different times and form independent majorities. Joint consensus replicates to both sets and requires a majority of each for agreement; after the final new configuration commits, the old set is no longer needed. The paper first brings new servers in as non-voting members so they can catch up without changing quorum. A leader removed by the final set steps down at that commit; removed servers can continue disrupting elections unless the implementation handles stale vote requests. | This supplies the consensus safety argument for per-group transitions and identifies learner catch-up, leader removal, and stale-node behavior as separate gates. It does not define Runnel's multi-group orchestration, RPC authentication, or operator workflow. |
| Pinned [OpenRaft 0.9.25 dynamic-membership docs](https://docs.rs/openraft/0.9.25/openraft/docs/cluster_control/dynamic_membership/index.html) and [`Raft` API docs](https://docs.rs/openraft/0.9.25/openraft/raft/struct.Raft.html#method.add_learner) | `add_learner` starts replication without granting a vote; blocking mode can wait for the learner to catch up. `change_membership` requires intended new voters to be learners and returns when the new membership is effective and committed. OpenRaft uses joint memberships and may retain removed voters as learners or drop them. Its documentation warns that `SetNodes` address changes can cause a split-brain if the new address belongs to another node; the network factory must reach the correct node. | These APIs match Runnel's pinned dependency and are a plausible consensus-group primitive. Runnel has no call sites for them. They do not change its static peer map, update all groups, or coordinate new groups. OpenRaft's [`Raft::new`](https://docs.rs/openraft/0.9.25/openraft/raft/struct.Raft.html#method.new) documentation assigns stable `NodeId` persistence to the application. |
| [etcd v3.7 runtime reconfiguration](https://etcd.io/docs/v3.7/op-guide/runtime-configuration/) and [learner design](https://etcd.io/docs/v3.7/learning/design-learner/) | Reconfiguration needs a current majority and changes are sequential. etcd adds learners as non-voters, promotes only when caught up, limits learners to bound leader replication work, and has a strict reconfiguration check to reject a change that would leave fewer than a quorum of started members. Removing a leader causes a brief election interruption. Its learner design documents that an incorrectly committed peer URL followed by quorum loss may require manual cluster recreation rather than a simple configuration rollback. | These are operational examples of admission checks, promotion gates, catch-up resource bounds, and honest recovery behavior. etcd's validation and admin API semantics are product choices, not a required Runnel policy. |
| [Redpanda Raft group reconfiguration](https://docs.redpanda.com/streaming/current/manage/raft-group-reconfiguration/) and [broker decommissioning](https://docs.redpanda.com/streaming/current/manage/cluster-maintenance/decommission-brokers/) | Redpanda describes learner catch-up, promotion, joint configuration, and removal as group-level work. Broker decommissioning adds a controller-owned plan across many partition groups, bounds concurrent replica moves, records per-group completion, and tolerates controller leadership transfer; the broker is removed only after every allocation completes. | The multi-group progress record is especially relevant to Runnel's many independent groups. Redpanda also has placement, balancing, and replica-movement machinery that Runnel does not; its workflow is an analogy, not an implementation prescription. |

## Inferences and candidate direction

### Addition and catch-up

Raft's paper and OpenRaft/etcd documentation separate copying state from giving a
node a vote. A new process can be reachable and replicate as a learner without
changing the old quorum. Promotion should depend on observed replication
progress, not merely on a node appearing in desired configuration. OpenRaft's
blocking catch-up is one candidate signal; a future Runnel gate would also
need to establish that state-machine application and snapshot installation are
complete enough for that group's role.

Runnel would need to decide how to apply those steps to the metadata group and
each existing data group, how to bound learner replication so it does not
starve active traffic, and whether a joining node can serve client requests
before every relevant group is ready. Any such serving rule must be consistent
with topology-free routing and with group-specific membership.

### Voter change and leader behavior

The core safety property is per-group: a transition cannot let old and new
voter sets make independent decisions. Use the consensus library's documented
membership operation rather than changing startup peer lists or rewriting
configuration out of band. In joint state, both old and new majorities matter;
loss of quorum on either side can stop progress. A leader can change during
either phase. If the current leader is excluded from the final set, election
downtime and the unknown outcome of an interrupted request must be accounted
for.

The Raft paper's stale-voter discussion is also important: a process removed
from committed membership must not regain disruptive authority merely because
it restarts with old state or has an old address. The exact protections depend
on the implementation, stable identity, transport routing, and process
fencing. ADR 0019's storage marker catches a directory configured for the
wrong cluster or node ID, but it does not fence a duplicate or stale process
using the same values. Empty-replica recovery and ID reuse remain a separate
question documented in [the replacement research](raft-recovery-and-replacement.md)
and [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md).

### Orchestration across groups

Applying membership changes to one group does not atomically change the
others. The accepted stream-creation protocol already treats metadata and data
group work as reconciled transitions because Raft groups cannot commit one
cross-group transaction. A node removal that updates the metadata group and
only some stream groups is therefore a partially completed operation, not an
atomic cluster state change.

An inferred coordination boundary would need to serialize membership work
against stream creation and group materialization, retain enough durable
progress to resume after coordinator or leader restart, prevent new groups
from initializing with an obsolete peer set, and avoid stopping/removing a
node until the final membership is committed in every relevant group. A
Redpanda-style durable plan and per-group status is one reference option. The
storage location, owner group, and routing behavior during partial progress
remain unresolved.

### Failure and rollback boundaries

Useful test and operational states include: learner added but behind; snapshot
or log catch-up interrupted; learner caught up but not promoted; joint config
proposed but not committed; joint config committed but final config pending;
final config committed but an API response lost; leader change during each
stage; and a removed process returning after completion. Each stage needs
restart recovery and an unambiguous way to inspect actual per-group committed
membership.

Before a transition begins, the current membership must still form quorum.
Once an entry has been committed, editing `--cluster-node`, retrying a
different desired set blindly, deleting local data, or attempting to “undo”
the config is not a safe substitute for reading replicated state and resuming
or rejecting the operation. If quorum is lost during joint membership, the
safe action may be to stop and require a separately designed recovery
procedure; the etcd documentation shows why a bad address and lost quorum may
not have an automatic rollback. A lost response after commit must be treated
as an unknown operation outcome until current state is inspected.

## Alternatives

| Alternative | Assessment from current evidence |
| --- | --- |
| Keep static membership and support only preserved-state restart | Matches accepted ADRs and current tests. Appropriate current behavior; does not meet the open add/remove outcome. |
| Rewrite peer configuration or directly replace a voter set | Reject as a safety mechanism. It is not a replicated transition, does not ensure overlapping quorum, and does not reconcile all Runnel groups. |
| Invoke OpenRaft learner and membership APIs per group, with an operator manually coordinating groups | Useful as a bounded experiment, but incomplete as a supported cluster operation: crashes can leave groups on different memberships and configuration can drift. |
| Add a durable cluster-level coordinator for learner/catch-up and per-group transitions | Candidate for later design. It aligns with independently committed groups and broker precedents, but adds new persisted state, concurrency rules, routing behavior, and recovery semantics that need design and tests. |
| Delegate membership safety to Kubernetes or another deployment controller | Does not satisfy the backlog constraint that correctness must not depend on Kubernetes availability; orchestration may trigger work, but committed consensus state must define authority. |
| Implement custom Raft reconfiguration | No evidence currently justifies replacing pinned OpenRaft membership primitives. Reconsider only if a tested limitation or performance result requires it. |

## Near-term disposition and evidence gates

The consensus primitive is available in the exact OpenRaft version already
used, so a one-group learner and membership experiment is implementable in the
near term. The end-to-end add/remove outcome is not yet an implementation
slice: Runnel first needs an accepted answer for durable multi-group progress,
creation serialization, address updates, stale-process fencing, and recovery
when quorum or an operation response is lost. Keep the existing membership
backlog outcome open and use this note as its source-backed progress record.
TD-008 already tracks dynamic membership and production fencing; this evidence
does not justify duplicating it as a new debt item. No ADR is warranted until
an operational membership policy and its consequences are selected.

A future design or implementation should provide evidence for these outcomes:

- State the failure model, quorum requirements, supported peer counts, and
  behavior while old/new configurations overlap. Verify that no transition
  can allow conflicting committed writes.
- Exercise catch-up and promotion in a real multi-process cluster while
  traffic continues; verify that learners do not vote or lead and promotion
  cannot occur before the selected applied-state/snapshot gate.
- Inject process loss, network partition, delayed or lost RPC responses,
  leader transfer, and restart at every membership boundary, including joint
  committed/final pending. Inspect committed membership after each restart.
- Cover add and remove across the metadata group and multiple stream groups,
  including stream creation during the operation. Prove that retries resume
  safely, new groups do not get a stale voter set, and removal is not reported
  complete until all required groups converge.
- Test a removed or stale process returning, duplicate use of one node ID,
  wrong-address routing, and storage identity mismatch. Prove stale processes
  cannot commit, vote into authority, or serve operations after losing their
  assigned role; decide separately how peer authentication and incarnation
  fencing are provided.
- Define deterministic rejection and operator recovery for absent quorum,
  incompatible node descriptors, learner lag, partial group completion, and
  unknown request outcomes. Do not rely on editing static config or erasing
  logs as rollback.
- Measure reconfiguration duration and leader CPU/network/storage pressure
  while learners catch up. Keep catch-up bounded and report operational
  effects without turning those measurements into hot-path claims.

Open decisions include the operator surface and authorization boundary; which
replicated group owns resumable cluster-operation progress; how per-group
membership and client routing behave during partial completion; what proves a
learner is safe to promote; whether removed voters remain as learners; how
address and process-incarnation changes are fenced; and what supported action
exists after quorum is lost mid-transition. The evidence above can narrow
these choices, but the current code and sources do not choose them for Runnel.

Until those gates and an accepted decision are complete, Runnel's supported
cluster behavior remains the static topology and the explicitly tested
preserved-state restart path. This is a statement of the observed baseline,
not a promise that every three-node deployment tolerates every one-node
failure.
