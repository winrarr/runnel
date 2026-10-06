# Raft follower recovery and replacement

- Status: exploratory evidence note; no replacement implementation authorized
- Last reviewed: 2026-10-05
- Baseline: `9655ee3981aa7ebd7d9a70f8358b817421b67837`
- Scope: the early static Multi-Raft backend, ordinary process restart,
  snapshot transfer, and replacement of a node whose local state is missing
  or inconsistent
- Related: [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md),
  [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md),
  [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md),
  [ADR 0019](../decisions/0019-clustered-storage-identity.md),
  [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md),
  [TD-007 storage compatibility evidence](../design/td-007-storage-compatibility-evidence.md),
  [TD-008 static-cluster evidence](../design/td-008-static-cluster-evidence.md),
  [TD-009 snapshot evidence](../design/td-009-snapshot-evidence.md),
  [TD-010 retained-state evidence](../design/td-010-retained-state-evidence.md),
  and [stable work placement](../design/stable-work-placement.md)

This note records external recovery evidence and the current Runnel boundary.
It is not a production replacement procedure, a public protocol proposal, or
an accepted membership design. Code and tests at the recorded baseline remain
authoritative.

## Question and current conclusion

What should Runnel promise across a preserved-state restart, a temporary node
outage while the cluster continues committing, missing or corrupt local state,
and a process that returns with an old or reused identity?

The current answer is deliberately narrow:

- restart against the same preserved directory and configured identity is the
  supported recovery path; the real-process test also covers one stopped
  follower missing a committed write and catching up after it returns;
- startup checks persisted cluster and node identity, layout, log shape,
  checkpoint, snapshot, and journal input, then rejects unsupported or
  contradictory state rather than repairing it;
- an empty-directory snapshot experiment exists only behind the explicit
  `test-replacement-recovery` feature; it uses the same configured voter ID
  and proves a narrow transfer-and-restart scenario, not a production
  replacement procedure; and
- no current implementation defines stale-process fencing, learner status,
  serving eligibility, or a durable remove/recover/promote lifecycle.

The important distinction is between committed Raft state, materialized broker
state, and a replacement participant. A snapshot can provide a bounded state
transfer, but snapshot installation alone does not define replica identity,
membership, serving readiness, stale-writer fencing, rollback, or cleanup.

## Observed Runnel implementation

### Ordinary restart with preserved state

`PersistentEngine::open_with_config` rejects the legacy single-group layout,
checks `storage.json`'s version, cluster name, and node ID, and runs
clustered-storage preflight before opening Raft groups. A new empty directory
gets a marker for the configured cluster and node; an existing clustered
layout without a marker is rejected. The marker has no process-incarnation or
membership generation. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L887-L945)

`GroupManager` opens the metadata group first and restores data groups from
their validated `group.json` manifests. A stream data group may also be
materialized lazily from committed metadata when a peer requests an unknown
group, which is necessary for the snapshot experiment. This is group
bootstrap, not a supported replica-replacement or membership protocol.
[`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L112-L216)
[`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L228-L335)
[`inbound.rs`](../../crates/runnel-raft/src/network/inbound.rs#L50-L115)

The current persistent artifacts have separate roles:

| Artifact | Current boundary | Recovery meaning |
| --- | --- | --- |
| `storage.json` | Version-1 cluster and node identity metadata | Prevents accidental reuse of a directory for another configured identity; it is not a membership or incarnation record. |
| `groups/<group>/raft-log.json` | Version-1 JSON Raft log containing vote, committed progress, purge boundary, and retained consensus entries | Consensus history; it may be compacted after a state-machine snapshot and is not the retained broker stream log. |
| `groups/<group>/state-machine/state-machine.json` | Version-3 JSON materialized checkpoint; older versions fail closed without mutation | Checkpoint for applied broker state and membership. |
| `groups/<group>/state-machine/state-machine.log` | Version-2 length-prefixed JSON apply journal with a 64 MiB record limit; older record versions fail closed | Write-ahead apply history replayed after the selected checkpoint or snapshot; an incomplete final frame is truncated during normal open. |
| `groups/<group>/state-machine/snapshot.json` | OpenRaft snapshot metadata plus a version-3 JSON materialized-state payload; older payload versions fail closed | State-machine recovery image used after consensus-log compaction or lag beyond the retained suffix. |

The Raft log's committed pointer and entry indexes are checked for impossible
or contradictory combinations, including gaps, entry/index mismatches,
and committed progress that is beyond both the retained log and the purge
boundary. An entry compacted behind a recorded purge boundary is an expected
recovery case, not an invalid missing entry. [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs#L67-L205)

State-machine open chooses a newer snapshot over an older checkpoint when the
snapshot's applied log is after the checkpoint, then replays journal entries
after that boundary. Preflight parses the journal without mutating it;
ordinary open truncates only an incomplete trailing frame. Invalid records,
unsupported versions, and oversized journal records fail rather than being
silently discarded. [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L386-L436)
[`state_machine_journal.rs`](../../crates/runnel-raft/src/state_machine_journal.rs#L42-L110)

These checks improve restart safety, but they do not prove that a restarted
process has caught up before client serving or voting. The configured peer map
remains static. OpenRaft membership is persisted in group state, but Runnel has
no durable node incarnation or supported membership-transition and
replacement lifecycle in its public or operational interface. The code rejects
a configured cluster or node ID that disagrees with the local marker. The
focused open test exercises a cluster-name mismatch; no test that changes only
the configured node ID is present in this baseline, although ADR 0019 says both
mismatches are covered. This missing case is recorded in the [replacement
backlog acceptance criteria](../backlog.md#make-missing-replica-replacement-safe).
A process or copied directory with the expected cluster and node IDs is not
distinguished by an incarnation token.

### Static topology and placement boundary

The current cluster uses one metadata group and one data group per stream. A
newly initialized group takes its voters from the configured peer map, and the
three-process scenarios are a development profile rather than an engine
invariant: configuration does not establish that every deployment has three
nodes or tolerates one failure. There is no supported add, remove, learner,
promotion, or dynamic replica assignment operation. Claims about quorum or
failure tolerance must therefore name the configured membership and the
observed scenario. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L931-L945)
[`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L358-L433)
[`TD-008 static-cluster evidence`](../design/td-008-static-cluster-evidence.md#observed-baseline)

Replica placement and shared-consumer work placement are separate concerns.
The current backend has no durable placement map, movement, split, balancing,
or replica handoff. [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md)
accepts a future hidden placement identity distinct from the public stream,
but implementation is deferred. The [stable work placement design](../design/stable-work-placement.md)
is an exploratory scheduler proposal for consumer execution lanes; it does not
establish replica placement, membership, or replacement safety. Recovery and
membership/fencing evidence must be established before placement movement is
treated as supported.

### Failure classes and what current tests establish

These cases are related but are not interchangeable. In particular, returning
after an outage with the original disk is not the same operation as starting a
new process with an empty directory and a reused voter ID.

| Case | Current evidence | Boundary that remains open |
| --- | --- | --- |
| Preserved-state process restart | A real three-process test stops a follower, restarts it against the same data directory and configured identity, and verifies it can observe the record committed while it was down. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L512-L550) | This demonstrates recovery of those tested state files and commands; it is not a mixed-version, corruption-repair, or full storage-failure matrix. |
| Temporary follower outage while peers continue | In the same process test, the stopped node is excluded while another configured node publishes; the restarted follower then observes the committed record. The test later stops the original leader and continues through a survivor. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L512-L607) | This is one process-stop case with a three-peer static configuration. It does not simulate a network partition, delayed or dropped peer RPCs, or a changing quorum configuration. |
| Missing local state | The feature-gated replacement experiment stops one node after survivors create a snapshot and purge its retained Raft-log prefix, switches that node to a new empty directory under the same node ID, interrupts snapshot installation three times, then permits a full install. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1924-L1965) | The default binary does not enable permissive log rollback. The test does not add a learner, remove the old voter, prove a before-catch-up serving gate, or test leader failure while transfer is incomplete. |
| Corrupt or inconsistent local state | Startup preflight and storage readers reject malformed, unsupported, or contradictory metadata, manifests, logs, checkpoints, snapshots, and journal records instead of silently accepting them. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L918-L928) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L671-L757) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L386-L436) | Fail-closed rejection is not recovery. There is no automatic reconstruction, operator restore workflow, or evidence that every cross-file corruption is detected. |
| Stale or reused process identity | The local marker rejects configured cluster/node IDs that differ from persisted values. A new empty directory is allowed to acquire the configured identity; the test uses that behavior. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L50-L105) [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1950-L1960) | There is no incarnation/fencing protocol for an old process or cloned directory presenting the same node ID, and no test for two processes with that identity. |

The default process test uses `Child::kill`, so it exercises process loss and
reopen with preserved files rather than a graceful shutdown protocol. Its
helper checks unexpected child exits during retries and at scenario end. The
replacement experiment checks the expected multi-chunk and completed-install
metrics and, after installation, stops the original leader, commits another
record on a survivor, and restarts the original leader. It therefore shows
that the recovered state participates in this later static-cluster failure
scenario. It does not show safe quorum participation or client serving during
the transfer itself.

### Snapshot build, transfer, and installation

The current state-machine snapshot includes the complete materialized state of
a metadata or stream data group: stream identity and lifecycle, retained
messages, ordinary and grouped consumer progress, in-flight ownership and
delivery tokens, attempts, lease-clock floor, producer request-ID
deduplication, and redelivery/dead-letter counters. OpenRaft metadata carries
the applied log ID, membership, and snapshot ID separately. The payload and
metadata therefore form one recovery image; neither is a replacement identity
protocol.

Snapshot creation serializes a borrowed view while holding the state read lock,
then persists the complete encoded image and compacts the apply journal. This
avoids cloning every retained message before encoding but remains proportional
to the complete materialized state; the JSON wrapper also adds representation
overhead beyond the encoded payload. The current defaults trigger snapshots
after 32 log entries, retain four entries, and bound peer chunks at 64 KiB;
these are conservative development defaults, not measured production tuning.
[`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L23-L31)
[`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L687-L729)

Peer snapshot RPCs are framed and bounded. The receiver buffers the incoming
snapshot in memory, validates the complete payload, persists the snapshot and
checkpoint, compacts the journal, and only then publishes the new in-memory
state and snapshot cache. A failed install therefore leaves the prior
in-memory state and current snapshot visible in the tested failure boundary.
If a later durable step fails after an earlier atomic write, restart may
recover a complete newer snapshot even though the live process retained its
previous state; this is a previous-valid-or-complete-newer boundary, not an
all-files-unchanged rollback guarantee.
[`outbound.rs`](../../crates/runnel-raft/src/network/outbound.rs#L557-L583)
[`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L826-L884)

An interrupted transfer currently starts again from byte zero. Metrics expose
build, install, received-chunk, received-byte, final-chunk, and in-progress
counters, but they do not provide a durable resume point, a per-replica
incarnation, or proof that the replacement is safe to serve. The complete
snapshot representation, repeated-interruption cost, and bounded receiver
memory remain tracked by [TD-009](../design/td-009-snapshot-evidence.md).

### Empty-replica experiment

`test-replacement-recovery` maps to OpenRaft's
`loosen-follower-log-revert`; the feature is absent from the default dependency
configuration and is enabled only by the opt-in server integration command.
That OpenRaft switch permits follower-log rollback for special test scenarios.
It does not itself add a node as a learner, change membership, validate a new
incarnation, or fence a stale participant. [`Cargo.toml`](../../crates/runnel-raft/Cargo.toml#L4-L6)
[`justfile`](../../justfile#L47-L51)

The feature-gated real-process test starts three statically configured nodes,
stops one follower, then publishes 48 records while that node is excluded. A
survivor has built a snapshot and purged its earlier Raft log before the test
switches the stopped process to a newly created empty data directory with the
same configured voter ID and peer address. The test kills and restarts that
process three times while a non-final snapshot chunk is present; the next
attempt completes installation. It then checks a retained record, an
independent consumer's read of the seed, the worker consumer's progress to
offset 1 after its earlier acknowledgement of offset 0, successful
acknowledgements, multi-chunk and completed-install metrics, and final process
liveness. After transfer has completed, it stops the original leader, commits
another record through a survivor, separately acknowledges an earlier pending
worker record, restarts the original leader, and observes later replicated
data on it. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1858-L2058)

This proves that, in this three-process setup with the original leader
available during transfer, the test-only permissive follower-revert path can
eventually install the tested current snapshot after repeated process
interruptions, recover selected record and consumer state, and later remain in
the tested static cluster after a leader failure. It does not prove that an
operator may erase a voter directory; that the empty node is prevented from
serving clients or affecting elections before installation; that the old
process is fenced; or that membership and quorum remain safe if leadership
fails during transfer. Nor does it test partitions, corrupted replacement
state, disk-full failures, response loss at individual persistence steps,
resumable transfer, rollback or cleanup, or mixed-version operation. It is not
a universal three-voter durability guarantee. The default process test instead
covers one stopped follower rejoining with its original state and a later
leader failure; it does not enable the permissive feature.

## External evidence and alternatives

The repository pins OpenRaft `0.9.25`. Its FAQ says local storage loss or a
damaged snapshot falls outside its reliable-storage assumption and has no
predictable outcome. It warns that wiping a follower and restarting it can
panic the leader and, under a later election, can lose a previously committed
entry; `loosen-follower-log-revert` allows the rollback for special cases but
does not define a safe replacement lifecycle. OpenRaft's log-pointer and
replication docs describe commit progress and snapshot/log synchronization as
storage and protocol responsibilities. [`OpenRaft FAQ`](https://docs.rs/openraft/0.9.25/openraft/docs/faq/)
[`feature flags`](https://docs.rs/openraft/0.9.25/openraft/docs/feature_flags/)
[`log pointers`](https://docs.rs/openraft/0.9.25/openraft/docs/data/log_pointers/)
[`log replication`](https://docs.rs/openraft/0.9.25/openraft/docs/protocol/replication/log_replication/)

OpenRaft also documents a lifecycle the current Runnel adapter does not use:
add the target as a learner, start replication, then call `change_membership`
to commit a two-phase joint membership. Proposed voters must already be
learners. Its docs state that the removed voter can be stopped after the new
membership is committed. The Raft paper's joint-consensus rule explains why
the transition needs overlapping majorities of old and new configurations;
catch-up alone does not change voting authority safely. Runnel's cluster
bootstrap calls `initialize` with the configured peer set and has no call to
`add_learner` or `change_membership`. This makes the OpenRaft API a relevant
candidate lifecycle, not evidence that it is already present or that it
resolves serving and process identity. [`OpenRaft dynamic membership`](https://docs.rs/openraft/0.9.25/openraft/docs/cluster_control/dynamic_membership/)
[`Raft paper`](https://raft.github.io/raft.pdf)
[`Runnel bootstrap`](../../crates/runnel-raft/src/engine.rs#L931-L945)

Redpanda documents replacement at the broker-log level as a staged Raft-group
reconfiguration: a new member starts as a non-voting learner, catches up, and
is promoted; joint consensus changes the voter set, and the old member is
demoted before removal. Redpanda also specifies quorum-preserving voter counts
for its own replication factor and operation. These details are a useful
reference for separating data transfer, voter eligibility, and old-member
removal; Redpanda's implementation-specific availability guarantee cannot be
transferred to Runnel's static all-peer groups without Runnel-specific proofs.
[`Redpanda Raft group reconfiguration`](https://docs.redpanda.com/streaming/26.1/manage/raft-group-reconfiguration/)

etcd's learner design provides a second documented promotion boundary: the
learner receives replication without voting, cannot lead, and promotion is
rejected until it has caught up. The initial design leaves promotion to an
operator and limits concurrent learners. This illustrates a conservative
promotion gate, but does not decide whether Runnel should expose a learner,
what the broker may serve during catch-up, or how a stale same-ID process is
fenced. [`etcd learner design`](https://etcd.io/docs/v3.6/learning/design-learner/)

Kafka's replicated-log model makes committed visibility and the in-sync
replica set part of the write durability contract. It is a different
replication architecture and is relevant here only as evidence that restart,
replica eligibility, and accepted-write guarantees are explicit design
boundaries; ISR rules do not substitute for a Runnel Raft membership
transition. [`Kafka replication design`](https://kafka.apache.org/43/design/design/)

## Implications and candidate directions

The following conclusions are inferences from the implementation and sources,
not accepted design decisions:

- Preserve durable state for ordinary process restart. Do not enable
  permissive follower-log rollback in the default build to conceal an invalid
  local state transition.
- Treat an empty or inconsistent replica as non-authoritative until a future
  lifecycle establishes its identity, source of truth, validated progress,
  serving boundary, and fencing behavior.
- Keep committed Raft state separate from retained broker history. Consensus-log
  compaction must not delete retained records or consumer state needed by the
  public stream model.
- Use snapshots as a bounded recovery primitive, not as an implicit migration,
  membership, or backup protocol.
- Preserve explicit failure outcomes. A transfer or recovery request that
  times out after the source may have committed must not be silently treated as
  an uncommitted operation.
- Keep recovery and membership evidence ahead of placement movement. A future
  hidden placement unit or consumer work lane must not be used to imply that a
  replica has been fenced, caught up, or promoted.

Candidate directions still to evaluate before implementation are:

1. preserved-state restart only, with explicit operator handling for an
   irrecoverable replica;
2. a controlled replacement lifecycle that adds a participant as non-voting,
   transfers validated state, catches it up, verifies serving readiness, and
   promotes it through a durable membership transition;
3. an explicitly opt-in test harness for empty-replica experiments while the
   production binary remains conservative; and
4. stronger storage fault-injection and recovery checks for append, truncate,
   purge, committed-progress persistence, snapshot installation, checkpoint
   replacement, journal compaction, and restart ordering.

Direction (2) is an open design question, not a selected proposal. The current
test-only experiment is direction (3); it should remain an evidence tool while
the default build rejects missing or inconsistent state. Direction (4) can
improve confidence in storage recovery, but cannot by itself define node
identity, voting, serving, or fencing behavior.

## Evidence gates for a supported replacement

A future implementation should satisfy all applicable gates before Runnel
claims safe replica replacement. These are outcome requirements; they do not
mandate a particular OpenRaft API or file layout.

### Identity and membership

- Cluster identity, group identity, node incarnation, and active membership
  are durable and validated. Reusing a node ID cannot make an old or
  contradictory participant authoritative.
- The claimed quorum and failure tolerance name the configured membership; the
  current three-process development profile is not a universal engine
  invariant. Replica placement and consumer work placement remain distinct.
- Add, remove, learner, promotion, and fencing transitions preserve a
  failure-surviving quorum at every committed boundary and define behavior
  when the target is unavailable or recovery is interrupted.
- The public protocol remains topology-free; clients do not supply a Raft
  group, replica, node, or placement assignment.

### State and serving safety

- A preserved-state restart catches up from the authoritative log or snapshot,
  serves only committed state, and preserves acknowledged records, consumer
  progress, delivery fencing, and request-ID deduplication.
- A missing or inconsistent replica first enters a documented
  non-authoritative recovery state. It becomes eligible to serve or vote only
  after identity, snapshot/checkpoint, log progress, and readiness checks pass.
- A crash before, during, or after transfer leaves either the last valid state
  or a complete newer state. It cannot expose an incomplete image or erase the
  only authoritative copy.

### Fault and operational evidence

- Real-process tests cover process stop, restart, delayed and dropped peer
  traffic, response loss, corrupt or unsupported state, write failure or
  disk-full behavior, transfer interruption, and recovery retry.
- Tests verify final process liveness, leader/quorum state, retained records,
  consumer checkpoints, deduplication outcomes, and explicit serving status;
  child-process handles alone are not sufficient evidence.
- Recovery bytes, chunks, retries, cleanup, transfer duration, and first
  post-recovery operation are observable and bounded for documented workloads.
  Restarting from byte zero is a correctness result, not evidence of efficient
  or resumable recovery.
- Mixed-version, downgrade, backup/restore, and operator rollback behavior is
  either tested and documented or explicitly unsupported. Snapshot transfer
  must not become an accidental storage-migration path.

## Open questions

- Which identity and incarnation model prevents an erased or partitioned node
  from returning with stale authority?
- Is learner-based recovery sufficient, or does retained broker state require a
  separate payload-transfer and activation protocol?
- What quorum and serving guarantees are required while a replacement catches
  up, and what is the operator fallback when it cannot catch up?
- How should recovery handle a snapshot that is valid but stale relative to
  the current membership or retained-state policy?
- Which fault-injection points and recovery metrics are necessary to make
  repeated interruption, cleanup, and ambiguous outcomes operationally safe?

## Refactor and planning assessment

No runtime refactor is included. The current `PersistentEngine`, `GroupManager`,
`LogStore`, `StateMachineStore`, and peer transport boundaries are sufficiently
clear for evidence work. Introducing a replacement state machine, incarnation
type, learner abstraction, or resumable snapshot format before an accepted
decision would create a second compatibility surface without resolving the
open membership and fencing questions.

The missing-replica child in [the backlog](../backlog.md#make-missing-replica-replacement-safe)
is updated in this change to state which scenarios current tests cover and
which acceptance criteria remain open. No tech-debt entry is added: the
replacement lifecycle remains an intended outcome in that backlog item and in
[TD-008](../tech-debt.md#td-008-distributed-raft-backend-is-an-early-static-cluster-implementation),
while [TD-009](../design/td-009-snapshot-evidence.md) tracks snapshot
representation and recovery-cost limits. The permissive feature remains
test-only under [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md),
so there is no new runtime shortcut to register. No ADR is added because the
membership and identity alternatives remain unselected.

## Verification

This update changes documentation only. The claims were checked against the
source, process tests, and accepted ADRs linked above. Focused document checks
are `git diff --check`, `just fmt-check`, and `just doc-test`; the external
reference URLs are checked for availability. Runtime tests are not rerun
because this change does not alter code, and no benchmark or production
replacement guarantee is implied.

## Sources

- [OpenRaft 0.9.25 feature flags](https://docs.rs/openraft/0.9.25/openraft/docs/feature_flags/)
- [OpenRaft 0.9.25 FAQ: lost data and follower replacement](https://docs.rs/openraft/0.9.25/openraft/docs/faq/)
- [OpenRaft 0.9.25 dynamic membership](https://docs.rs/openraft/0.9.25/openraft/docs/cluster_control/dynamic_membership/)
- [OpenRaft 0.9.25 log pointers and committed progress](https://docs.rs/openraft/0.9.25/openraft/docs/data/log_pointers/)
- [OpenRaft 0.9.25 log replication](https://docs.rs/openraft/0.9.25/openraft/docs/protocol/replication/log_replication/)
- [Raft consensus paper](https://raft.github.io/raft.pdf)
- [Apache Kafka 4.3 replication design](https://kafka.apache.org/43/design/design/)
- [Redpanda 26.1 Raft-group reconfiguration](https://docs.redpanda.com/streaming/26.1/manage/raft-group-reconfiguration/)
- [etcd learner design](https://etcd.io/docs/v3.6/learning/design-learner/)
- [ADR 0007: snapshot-based replica recovery](../decisions/0007-snapshot-based-replica-recovery.md)
- [ADR 0018: keep permissive empty-replica recovery test-only](../decisions/0018-safe-replica-recovery-boundary.md)
