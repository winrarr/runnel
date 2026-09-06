# Raft follower recovery and replacement

- Status: exploratory evidence note; no replacement implementation authorized
- Last reviewed: 2026-09-06
- Baseline: `origin/main` `8dc9eb2955fbaffc0b7dae158acb0fb88300841a`
- Scope: the early static Multi-Raft backend, ordinary process restart,
  snapshot transfer, and replacement of a node whose local state is missing
  or inconsistent
- Related: [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md),
  [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md),
  [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md),
  [TD-008 static-cluster evidence](../design/td-008-static-cluster-evidence.md),
  and [TD-009 snapshot evidence](../design/td-009-snapshot-evidence.md)

This note records external recovery evidence and the current Runnel boundary.
It is not a production replacement procedure, a public protocol proposal, or
an accepted membership design. Code and tests at the recorded baseline remain
authoritative.

## Question and current conclusion

What should Runnel promise when a Raft process restarts with its durable state,
when a replica's state is incomplete or corrupt, and when an operator wants to
replace a lost replica with an empty directory?

The current answer is deliberately narrow:

- ordinary restart is the supported recovery path when the node's durable
  state and cluster identity are preserved;
- clustered storage now validates identity, layout, log shape, checkpoint,
  snapshot, and journal input before opening it, and fails closed on
  unsupported or contradictory state;
- the empty-replica snapshot experiment is available only through the explicit
  `test-replacement-recovery` feature and is not an operational replacement
  contract; and
- no current implementation proves a safe lifecycle for removing, recovering,
  fencing, and promoting a lost voter.

The important distinction is between committed Raft state, materialized broker
state, and a replacement participant. A snapshot can provide a bounded state
transfer, but snapshot installation alone does not define replica identity,
membership, serving readiness, stale-writer fencing, rollback, or cleanup.

## Observed Runnel implementation

### Ordinary restart with preserved state

`PersistentEngine::open` creates only the requested data-directory boundary,
rejects the legacy single-group layout, verifies `storage.json`'s version,
cluster name, and node ID, and runs clustered-storage preflight before opening
Raft groups. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L804-L862)

`GroupManager` opens the metadata group first and restores data groups from
their validated `group.json` manifests. A stream data group may also be
materialized lazily from committed metadata when a peer requests an unknown
group, which is necessary for the snapshot experiment. [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L112-L216)
[`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L228-L335)
[`inbound.rs`](../../crates/runnel-raft/src/network/inbound.rs#L50-L115)

The current persistent artifacts have separate roles:

| Artifact | Current boundary | Recovery meaning |
| --- | --- | --- |
| `storage.json` | Version-1 cluster and node identity metadata | Prevents accidental reuse of a directory for another configured identity; it is not a membership or incarnation record. |
| `groups/<group>/raft-log.json` | Version-1 JSON Raft log containing vote, committed progress, purge boundary, and retained consensus entries | Consensus history; it may be compacted after a state-machine snapshot and is not the retained broker stream log. |
| `groups/<group>/state-machine/state-machine.json` | Version-2 JSON materialized checkpoint; version 1 is read forward in memory | Checkpoint for applied broker state and membership. |
| `groups/<group>/state-machine/state-machine.log` | Version-1 length-prefixed JSON apply journal with a 64 MiB record limit | Write-ahead apply history replayed after the selected checkpoint or snapshot; an incomplete final frame is truncated during normal open. |
| `groups/<group>/state-machine/snapshot.json` | OpenRaft snapshot metadata plus a versioned JSON materialized-state payload | State-machine recovery image used after consensus-log compaction or lag beyond the retained suffix. |

The Raft log's committed pointer and entry indexes are checked for impossible
or contradictory combinations, including gaps, entry/index mismatches,
committed progress beyond the persisted log, and a committed entry with no
persisted log. [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs#L67-L205)

State-machine open chooses a newer snapshot over an older checkpoint when the
snapshot's applied log is after the checkpoint, then replays journal entries
after that boundary. Preflight parses the journal without mutating it;
ordinary open truncates only an incomplete trailing frame. Invalid records,
unsupported versions, and oversized journal records fail rather than being
silently discarded. [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L386-L436)
[`state_machine_journal.rs`](../../crates/runnel-raft/src/state_machine_journal.rs#L42-L110)

These checks improve restart safety, but they do not establish that a node has
the current cluster membership or that a restarted process is eligible to
serve before it catches up. The configured peer map remains a static runtime
configuration; although OpenRaft membership is persisted as part of group
state, there is no durable node incarnation or supported membership-transition
and replacement lifecycle in the current public or operational interface.

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
to the complete materialized state. The current defaults trigger snapshots
after 32 log entries, retain four entries, and bound peer chunks at 64 KiB;
these are conservative development defaults, not measured production tuning.
[`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L23-L31)
[`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L687-L729)

Peer snapshot RPCs are framed and bounded. The receiver buffers the incoming
snapshot in memory, validates the complete payload, persists the snapshot and
checkpoint, compacts the journal, and only then publishes the new in-memory
state and snapshot cache. A failed install therefore leaves the prior
in-memory state and current snapshot visible in the tested failure boundary.
[`outbound.rs`](../../crates/runnel-raft/src/network/outbound.rs#L557-L583)
[`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L781-L855)

An interrupted transfer currently starts again from byte zero. Metrics expose
build, install, received-chunk, received-byte, final-chunk, and in-progress
counters, but they do not provide a durable resume point, a per-replica
incarnation, or proof that the replacement is safe to serve. The complete
snapshot representation, repeated-interruption cost, and bounded receiver
memory remain tracked by [TD-009](../design/td-009-snapshot-evidence.md).

### Empty-replica experiment

The crate feature `test-replacement-recovery` maps directly to OpenRaft's
`loosen-follower-log-revert` feature. It is not enabled by the default broker
build. [`crates/runnel-raft/Cargo.toml`](../../crates/runnel-raft/Cargo.toml#L1-L12)

The opt-in real-process test:

1. starts three statically configured nodes;
2. stops one node and publishes enough retained data to force a snapshot and
   consensus-log purge on a survivor;
3. starts the stopped node with a new empty directory but the same configured
   voter ID;
4. interrupts snapshot transfer repeatedly, then allows it to complete; and
5. checks retained messages, consumer progress, snapshot metrics, a later
   leader failure, and the rejoined node's recovery.

The test also checks process liveness during retry loops, so a child-process
exit cannot be mistaken for a successful protocol response. It is valuable
evidence that this particular permissive OpenRaft experiment can transfer the
current state while the original leader remains available. It is not evidence
that an operator may erase a voter directory and restart it with the same
identity in a production cluster. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1225-L1426)
[`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1590-L1641)
[`justfile`](../../justfile#L47-L51)

The experiment does not test partitions, corruption during transfer,
disk-full behavior, response loss at each persistence boundary, durable
membership changes, resumable transfer, promotion fencing, rollback, or
mixed-version operation. The default `cluster-test` path deliberately excludes
the permissive feature and covers preserved-state follower restart, leader
failure, forwarding, and clustered consumer recovery instead.

## External evidence

OpenRaft treats follower log rollback as an exceptional or special-case
condition. Its FAQ warns that erasing a follower and allowing a leader to
replicate into the same identity can cause a panic or create data-loss risk if
that node later becomes leader. Its log-pointer and replication documentation
also make monotonic committed/applied progress and snapshot recovery explicit
parts of the storage contract. The relevant dependency is pinned to OpenRaft
`0.9.25`, so the references below are version-pinned rather than `latest`.

Kafka's replication design separates committed visibility, replica liveness,
and recovery. Its log-recovery documentation describes validating and
truncating an incomplete tail before a broker serves the recovered log. This
supports treating ordinary restart and controlled replica reconfiguration as
different operational paths; it does not prescribe Kafka's protocol for
Runnel.

Redpanda documents Raft-group reconfiguration and majority-based replication
as explicit cluster operations. The useful comparison is the separation of
normal replication from controlled membership/recovery work, not a claim that
Runnel should copy Redpanda's placement or storage implementation.

etcd's learner design is a useful reference for a future non-voting catch-up
phase: a participant can receive state before it is allowed to affect quorum
decisions. It is a reference mechanism and does not settle Runnel's identity,
promotion, fencing, or public operational contract.

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

Candidate directions to compare before implementation are:

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

Direction (2) is the production question to resolve next. Direction (3) is
already the current test boundary. Direction (4) should strengthen evidence
without enabling rollback or silently changing the replacement contract.

## Evidence gates for a supported replacement

A future implementation should satisfy all applicable gates before Runnel
claims safe replica replacement. These are outcome requirements; they do not
mandate a particular OpenRaft API or file layout.

### Identity and membership

- Cluster identity, group identity, node incarnation, and active membership
  are durable and validated. Reusing a node ID cannot make an old or
  contradictory participant authoritative.
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

No backlog or tech-debt entry needed a change: [TD-008](../tech-debt.md#td-008-distributed-raft-backend-is-an-early-static-cluster-implementation),
[TD-009](../tech-debt.md#td-009-snapshots-rewrite-the-complete-materialized-group-state),
and the [safe replica recovery boundary](../decisions/0018-safe-replica-recovery-boundary.md)
already own the unfinished outcomes and current test-only constraint. The
existing records are linked above rather than duplicated here.

## Verification

This update changes documentation only. The factual claims were checked against
the current source and tests listed above. Focused verification for the
documentation change is `git diff --check`, `just fmt-check`, and
`just doc-test`. The relevant runtime evidence remains `cargo test --locked -p
runnel-raft` and the real-process `just isolated cluster-test`; the opt-in
empty-replica experiment is `just isolated cluster-replacement-test`. No
benchmark result or production replacement guarantee is implied by this note.

## Sources

- [OpenRaft 0.9.25 feature flags](https://docs.rs/openraft/0.9.25/openraft/docs/feature_flags/)
- [OpenRaft 0.9.25 FAQ: lost data and follower replacement](https://docs.rs/openraft/0.9.25/openraft/docs/faq/)
- [OpenRaft 0.9.25 log pointers and committed progress](https://docs.rs/openraft/0.9.25/openraft/docs/data/log_pointers/)
- [OpenRaft 0.9.25 log replication](https://docs.rs/openraft/0.9.25/openraft/docs/protocol/replication/log_replication/)
- [Apache Kafka replication design](https://kafka.apache.org/42/design/design/)
- [Apache Kafka log recovery](https://kafka.apache.org/10/implementation/log/)
- [Redpanda Raft-group reconfiguration](https://docs.redpanda.com/streaming/25.1/manage/raft-group-reconfiguration/)
- [Redpanda architecture](https://docs.redpanda.com/streaming/24.2/get-started/architecture/)
- [etcd learner design](https://etcd.io/docs/v3.6/learning/design-learner/)
- [ADR 0007: snapshot-based replica recovery](../decisions/0007-snapshot-based-replica-recovery.md)
- [ADR 0018: keep permissive empty-replica recovery test-only](../decisions/0018-safe-replica-recovery-boundary.md)
