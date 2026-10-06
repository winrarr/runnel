# ADR 0037: Accept offline side-by-side durable storage upgrades

- Status: accepted; implementation deferred
- Date: 2026-10-06
- Baseline: `9f64146169bb525221f99b231b0c3deee784cc59`
- Primary evidence class: design/research; secondary: correctness/recovery
- Related: [TD-007](../tech-debt.md#td-007-storage-conversion-and-artifact-compatibility-remain-open), [storage-upgrade backlog](../backlog.md#make-durable-storage-upgrades-safe), [storage-upgrade policy](../design/storage-upgrade-policy.md), [safe storage-upgrade contract](../design/storage-upgrade-safety-plan.md), [TD-007 evidence](../design/td-007-storage-compatibility-evidence.md), [ADR 0007](0007-snapshot-based-replica-recovery.md), [ADR 0018](0018-safe-replica-recovery-boundary.md), [ADR 0019](0019-clustered-storage-identity.md), and [ADR 0023](0023-independent-retained-storage-and-placement.md)

## Context

Runnel has separate local and clustered durable artifacts, each with its own
format and recovery rules. The local reader recognizes RNL1, RNL2, and RNL3
stream frames, while local consumer checkpoints and journals do not have a
cross-release writer contract. The clustered engine has strict identity and
layout checks, versioned Raft logs and journals, and narrow read-forward
support for selected version-1 checkpoint and snapshot fixtures. These are
current-binary recovery facts; they do not establish mixed-release operation,
format conversion, or downgrade safety. [TD-007 evidence](../design/td-007-storage-compatibility-evidence.md)
records the tested boundary.

Storage upgrades can affect acknowledged messages, logical offsets, consumer
progress, delivery attempts, retry identities, committed Raft boundaries,
and group identity. Parseability or a matching version number cannot prove
that all of those meanings survived. The current cluster also has no
compatibility handshake or rolling-upgrade promise. A safe first policy must
therefore establish a frozen source, an independently validated target, an
explicit authority change, and a precise point after which the source is too
stale to roll back to.

Reference systems make different tradeoffs. [Kafka's upgrade procedure](https://kafka.apache.org/43/getting-started/upgrade/)
allows a rolling binary phase against the old compatibility level, then a
separate finalization; Kafka 4.3 documents that metadata downgrade is not
supported when intervening releases changed metadata. [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/current/pgupgrade.html)
uses a preflight and offline conversion; copy and clone modes keep the old
cluster intact, while link or swap modes move the rollback boundary earlier.
[etcd's 3.5-to-3.6 procedure](https://etcd.io/docs/v3.6/upgrades/upgrade_3_6/)
requires a snapshot, supports a mixed-version phase at the lowest common
version, and distinguishes that phase from completed upgrade; its rollback
and storage handling are more elaborate than Runnel can currently prove.
[RocksDB's MANIFEST and CURRENT design](https://github.com/facebook/rocksdb/wiki/MANIFEST)
shows how a small selector can identify a complete file generation, but does
not define Runnel's delivery or recovery contract. These are design evidence,
not guarantees Runnel inherits.

## Decision

### Compatibility and authority

Compatibility is declared per artifact and operation: `read`, `write`,
`mixed`, and `migrate`. A single global format version is insufficient for
local logs, consumer state, Raft logs, state-machine journals, snapshots,
peer frames, and semantic changes. Unknown, malformed, contradictory, or
identity-mismatched state fails closed before the affected scope serves or
mutates. Read-forward parsing by itself does not authorize an older binary to
write or operate against that state.

This policy does not create support for old broker binaries or reader-only
historical formats. A conversion source must be produced by the currently
supported writer and accepted by the currently supported reader for that
artifact. For local stream logs, the migration-aware current format is `RNL3`;
`RNL1` and `RNL2` stores are refused unchanged even if a historical reader can
decode them. Extending a conversion to another source generation requires an
explicitly accepted artifact-specific decision; parser compatibility alone is
not eligibility.

For a conversion, recovery selects exactly one complete source or target
generation. It never chooses by directory order, timestamps, or the first
parsable file, and never treats an invalid store as empty. A generation
descriptor identifies its engine, layout and artifact versions, applicable
cluster/node/group/stream identities, source boundary, and content validation
result.

### First supported conversion boundary

The first supported format or layout conversion is offline and side-by-side:

1. Stop service for the conversion scope and fence every writer and
   acknowledgement path. A per-stream fence may narrow local downtime only
   after tests prove that no other path can mutate or observe the selected
   stream's state; otherwise stop the whole local broker. A clustered storage
   conversion requires a whole-cluster maintenance window. No old and new
   binaries may serve together during conversion.
2. Validate the source layout, identity, supported reader, logical boundary,
   and a restorable recovery artifact before copying. Keep the selected source
   immutable and authoritative. The source copy on the same storage device is
   a rollback generation, not an independent backup.
3. Write a separate target in bounded batches. Persist enough migration
   identity and verified progress to resume only when the source boundary and
   target prefix still match. Never overwrite, truncate, or repair the
   authoritative source as part of conversion.
4. Reopen and validate the complete target independently against the source
   logical state, including offsets, payload bytes, consumer progress,
   attempts, retry identities, and engine-specific committed/applied
   boundaries. Conversion is not ready until all relevant artifacts agree.
5. Show the operator a `ready` result while the source remains selected.
   Activation requires an explicit operator action and a durable, deterministic
   selector change. Keep service stopped until the target is reopened through
   its normal recovery path and reports one unambiguous active generation.
   Resume service only with the currently supported binary that declares and
   proves read, write, and semantic support for the target generation; no
   previous-binary startup or rollback compatibility is implied. An
   incompatible binary fails closed even while source rollback remains
   eligible.
6. Retain the source and migration evidence after activation. Cleanup is an
   explicit later operation; it may not remove the selected generation or the
   recovery artifact required by the operator's retention policy.

If conversion is interrupted before activation, the source remains the only
authority. Resume only from a checksummed bounded checkpoint whose source
identity/boundary and target prefix validate; otherwise quarantine or discard
only unreferenced target staging and restart from the source. A disagreement
between the selector, migration record, or either generation makes the
affected scope unavailable until an operator resolves it. Recovery must not
guess.

For a clustered conversion, no member may serve or lead while nodes disagree
about the selected generation or target readiness. Until a cluster-wide
activation mechanism and its failure tests exist, the clustered path remains
unsupported; the contract does not imply per-node rolling conversion or
rolling binary compatibility. Moving local state to the clustered engine is a
separate decision under [ADR 0043](0043-offline-local-to-cluster-migration.md)
and is not covered by this generic artifact-conversion contract.

### Rollback boundary

Before activation, aborting the conversion leaves the source active and
usable. After activation but before any target-only durable mutation, rollback
to the source is permitted only as an explicit offline operation: stop every
target process, verify the durable migration state proves that no target-only
mutation was accepted, restore the source selector, and reopen the source with
its declared-compatible binary. A pointer change while writers are active is
never rollback.

After activation the target remains read-only while source rollback is still
eligible. Before any target-only durable mutation can commit, the implementation
must durably enter a `write-pending` state, or atomically record that state with
the mutation. This applies to every durable transition, not only message
publish: acknowledgements, delivery-attempt state, deduplication identity,
configuration or lifecycle changes, and (in a clustered engine) consensus or
state-machine mutations. A successful mutation durably closes rollback. A
failure proven to have made no durable change may durably return to the
reversible state. An ambiguous outcome remains `write-pending`, blocks source
rollback, and fails closed until target state is reconciled. This boundary
prevents a crash from hiding acknowledged target state behind a stale source.

Once a target-only mutation is accepted, the target remains authoritative and
source rollback is closed. Repointing an old binary at the stale source would
hide target-only durable writes and is forbidden. Recovery then requires a
target-aware binary, a separately tested reverse conversion that includes all
logical state, or a verified recovery artifact. Editing version fields,
deleting markers, or renaming directories is not downgrade.

### Operator-visible state and limits

The implementation must expose the source and target generation identities,
artifact versions, migration phase, source boundary, bounded progress,
validation result, last failure, selected generation, and whether source
rollback remains eligible. The operator can distinguish `copying`,
`validating`, `ready`, `activating`, `active-reversible`, `write-pending`, `active-committed`,
`failed`, `aborted`, and `cleanup-pending` (or equivalent states). Ambiguous
activation, invalid selected state, incomplete target validation, and
cluster-generation disagreement make readiness false and block data service.
Metrics must use bounded labels; detailed stream/group and migration identity
belongs in bounded diagnostic output or structured logs.

The migration must bound memory, per-batch work, and recovery work, and check
temporary-space headroom before it changes migration state. Insufficient space
or an unsupported source record is a refusal, not permission to fall back to
in-place rewrite. Side-by-side conversion can require space for source,
target, journals/checkpoints, and an independent recovery artifact. No online
copy, dual-write tail, automatic downgrade, or public physical-path API is
accepted by this decision.

## Consequences and implementation gates

The accepted semantics are implementable without waiting for a specific
selector file, command, library, or directory layout. Those mechanisms remain
implementation choices, but they must uphold source immutability, logical
equality, activation, rollback closure, and fail-closed recovery. The existing
files and selectors do not yet implement these semantics; no migration command
or release compatibility promise is created here.

Before any format conversion is described as supported, tests must establish
per-artifact read/write/mixed/migrate behavior; exact logical-state equality;
bounded and interruption-safe transfer; activation recovery at every
file/selector sync boundary; stale-writer and stale-ack fencing; proof of the
first-write rollback barrier; old-binary refusal after target mutation; and
real-process local recovery. A clustered implementation additionally needs
all-node generation agreement, leader/follower failure, snapshot and
replacement boundaries, and proof that an unready replica cannot serve.
Filesystem crash guarantees must name the tested filesystems and distinguish
process termination from power-loss evidence. Benchmarks measure resource
cost and headroom, not semantic safety.

This decision does not change current local or clustered bytes, current
read-forward behavior, startup refusal behavior, snapshot recovery, public
protocols, or deployment operations. TD-007 remains partially open until the
accepted contract is implemented and per-artifact compatibility evidence
retires the runtime gaps.
