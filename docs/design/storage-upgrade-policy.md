# Durable storage upgrade policy

- Status: accepted behavior; implementation deferred by [ADR 0037](../decisions/0037-offline-side-by-side-storage-upgrades.md)
- Last reviewed: 2026-10-06
- Baseline: `9f64146169bb525221f99b231b0c3deee784cc59`
- Primary evidence class: design/research; secondary: correctness/recovery
- Scope: [Make durable storage upgrades safe](../backlog.md#make-durable-storage-upgrades-safe) and [TD-007](../tech-debt.md#td-007-storage-conversion-and-artifact-compatibility-remain-open)
- Detailed contract: [Safe durable storage upgrades](storage-upgrade-safety-plan.md)
- Related boundary: [Single-node to clustered migration](single-node-to-cluster-migration.md)
- Current evidence: [TD-007 storage compatibility evidence](td-007-storage-compatibility-evidence.md)

## Status and boundary

This document records the current observed compatibility boundary and the
accepted first safe upgrade behavior. [ADR 0037](../decisions/0037-offline-side-by-side-storage-upgrades.md)
owns the decision; [the detailed contract](storage-upgrade-safety-plan.md)
specifies implementation evidence. The decision does not define an
administration API or claim runtime support. No migration command, generation
selector, or writer fence exists today.

The accepted policy separates three operations that must not be combined
implicitly:

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

For the precise distinction between existing-storage validation and
empty-directory initialization, see the [TD-007 storage compatibility evidence
note](td-007-storage-compatibility-evidence.md). The clustered preflight
evidence is in [runnel-raft](../../crates/runnel-raft/src/lib.rs) and its
tests; local format and consumer behavior are in
[runnel-core](../../crates/runnel-core/src/lib.rs) and
[consumer_state.rs](../../crates/runnel-core/src/consumer_state.rs).
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

The accepted contract requires offline side-by-side conversion, an unchanged
source authority until a fully validated target is explicitly activated, and
an explicit source rollback only before any target-only durable mutation. The
target remains read-only while rollback is eligible. Before a mutation can
commit, a durable `write-pending` state blocks rollback; a successful write
closes rollback, a proven no-effect failure can restore eligibility, and an
ambiguous outcome remains pending and fails closed. After activation, only
binaries that declare and pass the target generation's read, write, and
semantic compatibility matrix may serve, even while source rollback remains
eligible. The source is retained but becomes stale after target-only writes,
so selecting it again would hide acknowledged state. A tested reverse
conversion or verified recovery artifact is required after that boundary. The complete
operator states and interruption rules are in the [accepted safety contract](storage-upgrade-safety-plan.md#interruption-and-rollback-contract).

The first implementation is offline and side-by-side, with bounded batches,
writer fencing, a durable migration record, independent target validation,
explicit activation, and retained source state. Per-stream local downtime may
be used only after isolation tests prove the fence is complete; otherwise the
broker stops. Clustered format conversion requires whole-cluster maintenance
and remains unsupported until all-node validation and activation are proven.
Online copy, dual-write, rolling binary upgrades, and local-to-cluster
movement are separate work and are not implied by this policy.

## Reference designs and implications

These sources are evidence for constraints, not specifications Runnel has
adopted. The detailed plan records alternatives, hypotheses, and unresolved
risks in [References and design evidence](storage-upgrade-safety-plan.md#references-and-design-evidence).

| Reference | Relevant source behavior | Difference that matters to Runnel |
| --- | --- | --- |
| [Apache Kafka 4.3 upgrade](https://kafka.apache.org/43/getting-started/upgrade/) and [protocol design](https://kafka.apache.org/43/design/protocol/) | Kafka separates a rolling binary phase from finalizing a feature/metadata version; its upgrade guide disallows metadata downgrade when metadata changes. | Runnel needs the same binary-versus-format distinction, but must also preserve consumer progress and producer request identity. Its current static cluster lacks the compatibility negotiation and committed gate needed to copy Kafka's rolling procedure. |
| [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/current/pgupgrade.html) | `--check` performs preflight; copy is the default. Copy/clone modes keep the old cluster intact, while link/swap modes can make it unusable or destructive after the new cluster starts or transfer begins. PostgreSQL also requires both clusters stopped for upgrade. | Runnel adopts the safer source-preserving boundary and rejects in-place/shared-file conversion for the first path. A stale source is not a rollback once the target has accepted new writes, even if its files remain intact. |
| [etcd 3.5→3.6 upgrade](https://etcd.io/docs/v3.6/upgrades/upgrade_3_6/) and [downgrade procedure](https://etcd.io/docs/v3.7/downgrades/downgrading-etcd/) | etcd requires a snapshot, runs a mixed-version cluster at the lowest common version, and permits binary rollback only during that phase; after all members upgrade, recovery needs the snapshot or formal downgrade process. | Runnel adopts explicit state, verified recovery evidence, and a clear irreversible boundary. It does not adopt rolling mixed-version operation until its peer protocol and state transitions have a tested compatibility gate. |
| [OpenRaft 0.9.25 snapshot replication](https://docs.rs/openraft/0.9.25/openraft/docs/protocol/replication/snapshot_replication/) and [storage traits](https://docs.rs/openraft/0.9.25/openraft/storage/) | Snapshot metadata carries an applied log boundary and membership, while log, state-machine, and snapshot persistence have separate interfaces. | Runnel should preserve committed/applied boundaries and validate application state, stream/group identity, consumer progress, attempts, and deduplication. Snapshot installation is not format migration. |
| [RocksDB MANIFEST and `CURRENT`](https://github.com/facebook/rocksdb/wiki/MANIFEST) | A transactional version-edit log and a small `CURRENT` pointer select complete referenced file sets; old files can remain until no live version references them. | This supports a generation/selector model, but Runnel must add engine identity, delivery semantics, bounded validation, and a writer fence. |
| [Online asynchronous schema change in F1](https://research.google/pubs/online-asynchronous-schema-change-in-f1/) | Online readers and writers require explicit compatibility between transition states; parseability alone does not prove safety. | A future live-tail migration needs pairwise proofs for publish, acknowledgement, replay, and recovery. The first plan avoids that proof with a fence. |

ADR 0037 accepts the source-preserving offline policy because it minimizes the
number of concurrently authoritative representations and avoids inferring
compatibility from binary version or parseability. The referenced systems
inform the separation of compatibility gates, copy-versus-link tradeoffs,
recovery artifacts, and generation selectors; their procedures and guarantees
do not transfer to Runnel.

## Implementation gate

The policy is accepted, but no conversion is supported until the
[implementation acceptance matrix](storage-upgrade-safety-plan.md#implementation-acceptance-matrix)
passes for the affected artifacts. In particular, implementation must prove
read-only validation, exact logical-state preservation, bounded and
restart-safe transfer, durable activation, stale-writer fencing, the
first-write rollback barrier, old-binary refusal after target mutation,
bounded observability, and applicable real-process verification. A later
decision is needed only to change this boundary or adopt distinct behavior
such as online conversion, rolling compatibility, or downgrade support.

The current publish, recovery, and cluster benchmarks do not exercise a
migration path. No runtime benchmark is required for this documentation-only
decision; migration resource measurements become an implementation gate when
conversion code exists.
