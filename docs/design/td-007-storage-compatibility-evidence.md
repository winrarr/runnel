# TD-007: Storage compatibility evidence

- Status: exploratory evidence note; no migration implementation authorized
- Last reviewed: 2026-09-06
- Baseline: `55b4714dcfb1343e11652d9e63323ad1b96c2451`
- Scope: local durable files, clustered durable artifacts, and the boundary
  between read-forward recovery and supported upgrades
- Related debt: [TD-007](../tech-debt.md#td-007-storage-format-compatibility-is-not-yet-defined)
- Related outcome: [Make durable storage upgrades safe](../backlog.md#make-durable-storage-upgrades-safe)
- Related designs: [Durable storage upgrade policy](storage-upgrade-policy.md)
  and [safe durable storage upgrades](storage-upgrade-safety-plan.md)
- Related decisions: [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md),
  [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md), and
  [ADR 0019](../decisions/0019-clustered-storage-identity.md)

This note records the compatibility behavior actually exercised by the current
tests. It does not turn a parser's ability to read old bytes into a release
promise, and it does not authorize a migration command, a rolling upgrade, or
a downgrade procedure.

## Current conclusion

Runnel has independent local and clustered storage formats rather than one
global schema. The current code has a sound early safety boundary for known
cases:

- local stream readers accept the recognized `RNL1`, `RNL2`, and `RNL3` frame
  families, including a tested mixture of legacy and versioned frames;
- current clustered checkpoint and snapshot readers accept narrow version-1
  read-forward fixtures and materialize them into the current representation;
- existing clustered directories are checked for identity, layout, manifest,
  log, checkpoint, journal, and snapshot errors before group recovery opens;
  and
- unsupported or unmarked clustered state is rejected without creating a new
  grouped layout or silently serving an empty store.

Those are recovery and refusal properties for this binary. They do not prove
that two releases can write together, that a new writer preserves an old
reader's semantics, that a directory can be converted in place, or that an
older binary can safely start after a new representation becomes active.

The phrase “read-only preflight” must also be scoped carefully. For an empty
directory, startup intentionally creates the directory and writes
`storage.json`. For existing marked or unmarked clustered state, validation
refuses unsupported input before `GroupManager` opens groups or creates
authoritative grouped state. The latter is the compatibility evidence; empty
directory initialization is not a migration operation.

## Evidence matrix

| Boundary | Current evidence | What remains unproven |
| --- | --- | --- |
| Local stream history | `StreamLog` dispatches by `RNL1`/`RNL2`/`RNL3` magic, enforces frame/version/size rules, verifies checksums for versioned frames, and truncates only an incomplete suffix. [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L13-L28) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1690-L1835) | No root generation marker, converter, cross-release writer matrix, or proof that an old binary can interpret newer target-only frames. |
| Local consumer state | Checkpoint and bounded event-journal recovery preserve persisted progress and attempts; a partial final journal line is discarded while complete malformed or oversized state fails. [`consumer_state.rs`](../../crates/runnel-core/src/consumer_state.rs#L69-L200) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1264-L1373) | Delivery tokens/deadlines are volatile, and there is no migration manifest or cross-release writer contract for checkpoint/journal state. |
| Cluster root identity | `storage.json` is version 1 with exact cluster and node identity. Unknown versions, malformed metadata, and mismatches fail before group open. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L29-L115) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1550-L1604) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1771-L1802) | The metadata file is an ownership marker, not an active-generation selector, migration record, or rollback authority. |
| Clustered layout preflight | A current split-layout fixture passes validation without changing fixture bytes or creating state-machine files; a contradictory data-group manifest is rejected without rewriting the fixture. [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L789-L828) | The fixture is a structural validation check, not a conversion from the earlier single-group layout and not proof of crash-safe activation. |
| Legacy and partial layouts | Legacy single-group paths, an unmarked grouped directory, and partial grouped layouts fail closed without creating the current layout or guessing identity. [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1708-L1768) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1866-L2028) | There is no adoption, rejoin, replacement, or operator migration path for these layouts. |
| Cluster log/checkpoint/journal/snapshot versions | Unsupported Raft logs, state-machine checkpoints, journals, and snapshots fail with file context before authoritative recovery; complete current and legacy read-forward fixtures are covered. [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs#L93-L190) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1919-L2003) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L2235-L2315) | Version checks and read-forward parsing do not establish mixed-version command, snapshot, deduplication, consumer, or peer semantics. |
| Snapshot replacement | Snapshot install validates the payload and keeps in-memory state unchanged until durable replacement steps succeed; interrupted transfer retries from byte zero. [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L787-L824) [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md) | Snapshot transfer is a replica-recovery primitive, not a general format migration, resumable copy protocol, or supported empty-replica replacement lifecycle. |

## Compatibility classification

The current cases should be classified as follows:

| Classification | Supported today? | Evidence boundary |
| --- | --- | --- |
| Current-binary reopen | Yes, for the formats and layouts covered by the current readers and identity checks | Focused recovery and preflight tests; not a cross-release guarantee. |
| Narrow additive read-forward | Observed for version-1 clustered checkpoint/snapshot fixtures and local mixed frame families | Old bytes are read into current memory; current writers, mixed writers, and semantic equivalence are not tested as a release pair. |
| Converted layout or encoding | No | No source/target generations, conversion boundary, activation selector, or recovery artifact exists. |
| Clustered rolling binary upgrade | No | Peer frames have no compatibility handshake and no real old/new binary matrix exists. |
| Downgrade after target-only state | No | Retaining source files is not enough to preserve later acknowledgements, attempts, deduplication, retention effects, or semantic changes. |
| Local-to-cluster engine migration | No | This changes topology and replication as well as storage; its separate design remains exploratory. |

Read-forward must therefore be reported with the artifact, versions, fields,
and exact fixture covered. “Backward compatible storage” is too broad for the
current evidence.

## Gates before claiming supported compatibility

Before an ADR or release compatibility promise, a future change must satisfy
the following gates for each affected artifact:

1. **Identity and discovery:** identify engine, cluster/node, stream/group,
   source generation, and every expected artifact before opening a replacement
   or selecting a directory by name or parseability.
2. **Bounded validation:** reject unknown, malformed, contradictory,
   unsupported, or identity-mismatched data before mutation; bound frame,
   record, payload, and decompression allocations; validate checksums and
   offset/applied-boundary continuity.
3. **Logical equality:** compare offsets, timestamps, keys, opaque payloads,
   stream/group identity, consumer progress, out-of-order acknowledgements,
   delivery attempts, lease state, and producer request identity—not only
   serialized shape or record counts.
4. **Explicit conversion boundary:** build a side-by-side target from a
   recorded source boundary, retain the source as authority until validation
   completes, and make target activation durable and deterministic.
5. **Mutation fencing:** prevent stale writers, acknowledgements, forwarded
   requests, leaders, and cleanup owners from mutating or deleting state across
   activation.
6. **Interruption and rollback:** test preflight, copy, validation, activation,
   restart, cleanup, and process/node failure. Either a verified source or a
   verified target remains authoritative; downgrade is supported only with a
   tested inverse or recovery artifact.
7. **Mixed-version operation:** use real old/new binaries to exercise peer,
   command, snapshot, checkpoint, journal, publish, acknowledgement, replay,
   deduplication, leader/follower recovery, and response-loss behavior.
8. **Operational evidence:** expose bounded phase, identity, progress, failure,
   rollback, and orphan/cleanup diagnostics, and measure transfer workspace and
   recovery cost for representative retained streams.

The existing [storage-upgrade safety plan](storage-upgrade-safety-plan.md)
contains the fuller proposed migration state machine and acceptance matrix.
These gates are a classification aid for future changes, not an instruction
to implement the proposed API or layout now.

## Refactor and planning assessment

No safe runtime refactor is included in this evidence-only review. The current
preflight and reader boundaries are explicit enough for focused compatibility
fixtures. Introducing generation selectors, migration ownership, or shared
format abstractions before the compatibility policy is accepted would add
runtime surface without retiring TD-007. The existing TD-007 and storage
upgrade backlog records remain the appropriate planning items; no additional
tech-debt entry is warranted.

## Verification

This update changes documentation only. The focused evidence gate is the
existing `cargo test -p runnel-core` and `cargo test -p runnel-raft` coverage,
with the real-process `just isolated cluster-test` workflow remaining the
required clustered recovery check when runtime behavior changes. No migration
or rolling-upgrade benchmark is implied by this note.
