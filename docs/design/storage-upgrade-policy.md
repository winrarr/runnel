# Durable storage upgrade policy (proposal)

- Status: exploratory design proposal; not an accepted compatibility decision
- Last reviewed: 2026-09-03
- Baseline: `5bed1e052fcf907d2ab8ce3aa22da961b38540f1`
- Scope: [Make durable storage upgrades safe](../backlog.md#make-durable-storage-upgrades-safe) and [TD-007](../tech-debt.md#td-007-storage-format-compatibility-is-not-yet-defined)
- Detailed contract: [Safe durable storage upgrades](storage-upgrade-safety-plan.md)
- Related boundary: [Single-node to clustered migration](single-node-to-cluster-migration.md)

## Status and boundary

This document records the current observed compatibility boundary and a
proposal for the next implementation slice. It is not an administration API,
a release compatibility promise, or an ADR. No migration command, generation
selector, writer fence, supported downgrade, or end-to-end rolling-upgrade
path exists today.

The proposal separates three operations that must not be combined implicitly:

1. **Binary upgrade:** replace a process while the active durable and peer
   representations remain compatible.
2. **Format migration:** validate and convert one durable representation to a
   new generation, then activate it at an explicit durable boundary.
3. **Engine migration:** move local state to clustered state. This changes
   topology, replication, and producer-retry identity and has its own [design
   boundary](single-node-to-cluster-migration.md).

The safety objective is that a supported operation either preserves the same
acknowledged logical state or fails closed with a recoverable diagnosis. It
must not expose a valid store as empty, serve partial target state, or let a
stale writer append or acknowledge after activation.

## Current observed compatibility

Runnel has no global storage schema. The local and clustered engines own
different artifacts, and each artifact has its own parser and recovery
boundary. The [safety plan](storage-upgrade-safety-plan.md#compatibility-matrix)
contains the implementation-ready matrix; this summary records only what the
current code demonstrates.

| Artifact | Observed current behavior | What it does not establish |
| --- | --- | --- |
| Local stream history | `streams/<stream>.log` can contain legacy `RNL1`, versioned checksummed `RNL2`, and request-aware checksummed `RNL3` frames. The reader dispatches by magic; a partial final frame is discarded on normal open. | No durable root generation marker, offset-continuity proof, cross-release mixed-writer guarantee, or conversion path. |
| Local consumer state | A JSON checkpoint stores contiguous progress, out-of-order acknowledgements, and persisted delivery attempts. A bounded JSON-lines journal records events; its incomplete final line is recoverable. | In-flight delivery tokens and deadlines are volatile, and checkpoint/journal bytes have no migration manifest or cross-release writer contract. |
| Cluster root and groups | `storage.json` binds cluster and node identity. `groups/metadata` and per-stream `groups/data/<hex-stream>` groups are validated before groups open. Unsupported versions, identities, legacy paths, and partial layouts fail closed in the tested paths. | The identity marker is not an active-generation selector. Validation is not a migration, backup, or rollback workflow. |
| Clustered state | Checkpoint and snapshot payloads accept the current version 2 and a narrow version-1 read-forward form. The Raft log and state-machine journal have separate version 1 formats and separate persistence boundaries. | Read-forward parsing does not prove mixed-version command, snapshot, peer, consumer, or producer-deduplication semantics. |
| Peer and snapshot transfer | Peer frames are length-bounded JSON without a version handshake. OpenRaft snapshot chunks are bounded; the current receiver retries an interrupted transfer from byte zero. | Successful decoding is not a rolling-upgrade contract, and snapshot replacement is not a general format migration. |

The clustered preflight evidence is in [runnel-raft](../../crates/runnel-raft/src/lib.rs)
and its tests; local format and consumer behavior are in [runnel-core](../../crates/runnel-core/src/lib.rs)
and [consumer_state.rs](../../crates/runnel-core/src/consumer_state.rs).
These links describe implementation evidence, not promises for future releases.

## Compatibility and downgrade policy

Every future release pair must describe compatibility independently for each
artifact and operation:

| Relation | Required question |
| --- | --- |
| `read` | Can the reader decode every required field and bound every allocation without guessing or changing logical meaning? |
| `write` | Can the writer emit bytes that every binary still allowed to run against the active generation can read and interpret? |
| `mixed` | Can old and new binaries operate together without violating committed ordering, acknowledgement progress, recovery, deduplication, or fencing? |
| `migrate` | Is an explicit conversion required, what source/target identity and boundary does it use, and what recovery artifact makes rollback possible? |

The current supported opening behavior is deliberately narrow:

- a current binary can reopen the current local or split clustered layout,
  subject to its existing validation and identity checks;
- `runnel-core` can read the recognized `RNL1`, `RNL2`, and `RNL3` families and
  recover its documented incomplete-tail case;
- `runnel-raft` can read the tested version-1 checkpoint and snapshot payloads
  through its version-2 in-memory representation; and
- no current binary pair has a supported clustered rolling-upgrade contract.

These are observed behaviors. A future implementation must add representative
fixtures before relying on any one as a release guarantee.

The proposed first migration has this rollback boundary:

- Before activation, the source generation remains authoritative. The target
  may be abandoned or retried after identity and checksum validation. Target
  bytes written in staging are not served and do not by themselves make an
  old binary unsafe.
- Once the target generation is activated, an old binary must fail closed if
  it cannot prove that it can represent all active state. The operator must
  use a target-aware binary, a tested reverse converter, or a verified
  pre-upgrade recovery artifact. Editing a version field, deleting a marker,
  renaming directories, or pointing an old binary at the target is not a
  downgrade procedure.
- Retaining source files is not equivalent to retaining a rollback guarantee:
  acknowledged writes, consumer progress, attempts, request identities,
  retention decisions, and semantic changes must all be representable by the
  rollback target. Automatic downgrade is unsupported until that inverse is
  tested.
- Local-to-cluster movement remains outside this policy. It needs a separate
  protocol for replication, producer identity, cutover, and recovery.

The first physical migration should therefore be offline and side-by-side,
with bounded batches, a per-stream maintenance fence, a durable migration
record, a validated target, and a small atomic generation selector. A later
online copy or dual-write design is a hypothesis, not an implicit extension of
this policy.

## Reference designs and implications

These sources are evidence for constraints, not specifications Runnel has
adopted. The detailed plan records alternatives, hypotheses, and unresolved
risks in [References and design evidence](storage-upgrade-safety-plan.md#references-and-design-evidence).

| Reference | Relevant source behavior | Difference that matters to Runnel |
| --- | --- | --- |
| [Apache Kafka rolling upgrades](https://kafka.apache.org/32/getting-started/upgrade/) and [protocol design](https://kafka.apache.org/38/design/protocol/) | Kafka holds the old inter-broker/message representation during a rolling binary upgrade, verifies the new binaries, and advances an explicit protocol gate. Its clients negotiate API versions. | Runnel needs the same binary-versus-format distinction, but must also preserve acknowledged consumer progress and producer request identity. Its current static cluster has no negotiated compatibility level. |
| [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/17/pgupgrade.html) | Preflight precedes mutation; copy/clone modes retain a separate old cluster while link modes move the rollback boundary earlier. | Side-by-side conversion is the safer first Runnel model. A space-saving or in-place mode must explicitly weaken rollback and prove filesystem behavior. |
| [etcd 3.5→3.6 upgrade](https://etcd.io/docs/v3.6/upgrades/upgrade_3_6/) and [downgrade procedure](https://etcd.io/docs/v3.7/downgrades/downgrading-etcd/) | etcd uses a cluster-wide compatibility/downgrade mode, snapshot backup, version-aware storage handling, and status reporting; replacing binaries alone is not downgrade. | Runnel should require target validation and a verified recovery artifact, while avoiding an online cluster schema claim until it has the corresponding protocol and failure evidence. |
| [OpenRaft snapshot replication](https://docs.rs/openraft/latest/openraft/docs/protocol/replication/snapshot_replication/) and [storage traits](https://docs.rs/openraft/latest/openraft/storage/) | Snapshot metadata carries an applied log boundary and membership, while log, state-machine, and snapshot persistence have separate interfaces. | Runnel should preserve committed/applied boundaries and validate application state, stream/group identity, consumer progress, attempts, and deduplication. Snapshot installation is not format migration. |
| [RocksDB MANIFEST and `CURRENT`](https://github.com/facebook/rocksdb/wiki/MANIFEST) | A transactional version-edit log and a small `CURRENT` pointer select complete referenced file sets; old files can remain until no live version references them. | This supports a generation/selector model, but Runnel must add engine identity, delivery semantics, bounded validation, and a writer fence. |
| [Online asynchronous schema change in F1](https://research.google/pubs/online-asynchronous-schema-change-in-f1/) | Online readers and writers require explicit compatibility between transition states; parseability alone does not prove safety. | A future live-tail migration needs pairwise proofs for publish, acknowledgement, replay, and recovery. The first plan avoids that proof with a fence. |

The resulting design is an inference from these references and Runnel’s
at-least-once contract. It is not an accepted decision; no ADR is added by
this proposal.

## Implementation gate

The next implementation must satisfy the [acceptance matrix](storage-upgrade-safety-plan.md#acceptance-matrix)
before an ADR or a public compatibility promise is considered. In particular,
it must prove read-only validation, exact logical state preservation, bounded
and restart-safe transfer, durable activation, stale-writer fencing, explicit
pre-activation rollback, post-activation old-binary refusal, bounded
observability, and real-process local and three-node clustered verification.

The current publish, recovery, and cluster benchmarks do not exercise a
migration path. No runtime benchmark is required for this documentation-only
proposal; migration resource measurements become a gate only when migration
code exists.
