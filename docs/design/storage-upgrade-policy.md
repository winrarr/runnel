# Durable storage upgrade policy (proposal)

- Status: exploratory design proposal; not an accepted compatibility decision
- Last reviewed: 2026-09-03
- Baseline: `0824844302ba0e14bcbb1bc4cb1dc4c64492e147`
- Scope: [Make durable storage upgrades safe](../backlog.md#make-durable-storage-upgrades-safe) and [TD-007](../tech-debt.md#td-007-storage-format-compatibility-is-not-yet-defined)
- Related designs: [Safe durable storage upgrades](storage-upgrade-safety-plan.md) and [Single-node to clustered migration boundary](single-node-to-cluster-migration.md)

## Proposal summary

Runnel should make a durable upgrade an explicit, staged operation with a
read-only preflight, a durable source/target identity, a validated activation
point, and a stated rollback boundary. A binary upgrade, a durable-format
conversion, and a move from the local engine to the clustered engine are
different operations and must not be combined implicitly.

The immediate proposal is deliberately conservative:

- retain the current fail-closed startup behavior and its narrow legacy-read
  cases;
- define compatibility per artifact and per operation (`read`, `write`,
  `mixed`, and `migrate`), rather than treating one format number as a global
  promise;
- make the first migration implementation offline and side-by-side, with a
  short writer fence and a durable generation record; and
- do not support downgrade by starting an older binary against a directory
  after target-only bytes or semantics have been written.

This document is a design input, not an implementation, an administration
API, or an ADR. It does not promise compatibility for any future release.

## Current persisted formats and preflight boundary

The current implementation has no single storage schema. The local and
clustered engines own different artifacts and have different open behavior.
The inventory below is the compatibility surface that a future matrix must
cover.

### Local engine (`runnel-core`)

| Artifact | Current representation and behavior | Upgrade-relevant limitation |
| --- | --- | --- |
| Stream history | `streams/<stream>.log`. `RNL1` is the legacy 28-byte header and key/payload frame. `RNL2` is a version-1, 44-byte checksummed frame with bounded key/body lengths and no compression. `RNL3` is a version-1, 48-byte request-aware frame with a request ID and checksum. New writes select `RNL1` or versioned writes through `DurableFormat`; readers dispatch by magic and can read the supported families in one file. | There is no root marker recording the selected family. A successful parse is not a proof of offset continuity or semantic compatibility; migration must validate logical offsets, request-ID mappings, and payload bytes explicitly. |
| Stream recovery/indexes | `StreamLog::open` scans complete frames, rebuilds a bounded recent cache and sparse index, and truncates an incomplete trailing frame. A malformed complete frame fails. The indexes are process-local and not a durable authority. | Local open is not a separate read-only preflight. It creates `streams/` and `consumers/` and may truncate an incomplete tail, so a migration must establish a source boundary before copying and distinguish an incomplete tail from corruption. |
| Ordinary consumer state | `consumers/<stream>/<consumer>.json` stores the stream and consumer names, contiguous `committed_offset`, out-of-order acknowledgements, and persisted delivery attempts. The checkpoint has no explicit version field. | The checkpoint is logical state, not a portable clustered snapshot. The converter must preserve committed progress and attempts, reject impossible offsets, and not use the highest seen offset as a replacement for contiguous progress. |
| Consumer event journal | The historical `<consumer>.json.tmp` path is an append-only JSON-lines journal of delivery attempts and acknowledgements, bounded at 64 KiB. A partial final line is truncated; complete malformed events fail. When the bound is reached, a checkpoint is atomically replaced before the journal is truncated. | The checkpoint replacement syncs the temporary file before rename but does not sync its parent directory in this function. Filesystem crash guarantees therefore need an explicit test and must not be inferred from the filename or JSON parse. |
| Volatile state | In-flight member ownership, delivery tokens, deadlines, and indexes are process memory. Attempts are persisted before delivery is returned, but local delivery tokens do not survive restart. Counters are process-lifetime observations. | Do not copy local tokens or `Instant` deadlines as durable state. A cutover must define redelivery and acknowledgement races, and may reset process-lifetime counters while reporting that fact. |

The local engine validates stream and consumer names before using them in paths,
but it has no durable generation or writer-fence marker. A future format change
cannot safely be introduced by selecting a different `DurableFormat` alone.

### Clustered engine (`runnel-raft`)

The clustered layout is rooted at a data directory containing `storage.json`
and `groups/`. `groups/metadata/` is the metadata Raft group; each stream data
group is under `groups/data/<hex-encoded-stream>/` and has a `group.json`
manifest. Each group contains a local `raft-log.json` and a
`state-machine/` directory.

| Artifact | Current representation and behavior | Upgrade-relevant limitation |
| --- | --- | --- |
| Root identity | `storage.json` is a denied-unknown-field JSON object with metadata version `1`, `cluster_name`, and `node_id`. Existing mismatches, unknown versions, malformed metadata, and unmarked state fail closed. A fresh directory may initialize this file. | This identifies the current owner; it is not a migration manifest or generation selector. It must remain authoritative for any future staging or activation record. |
| Per-group Raft log | `raft-log.json` is a denied-unknown-field JSON object with format version `1`, purge boundary, log entries, committed ID, and vote. `LogStore` rewrites it atomically after mutations and rejects unknown versions. | The consensus log is not retained stream history. Its format/version cannot describe state-machine or message compatibility, and a log conversion must preserve committed/applied boundaries. |
| State-machine checkpoint | `state-machine/state-machine.json` is a denied-unknown-field JSON object at version `2`. Version `1` is read forward; legacy stream arrays are converted in memory to stream identity/lifecycle state. Current writes emit version `2`. | Read-forward recovery is not a migration promise: the old bytes remain until a normal checkpoint, and the version-1 conversion does not define mixed writers, downgrade, or a directory-level cutover. |
| Snapshot | `state-machine/snapshot.json` wraps OpenRaft snapshot metadata and a serialized state payload. The payload accepts version `1` (including the legacy omitted-version default) and current version `2`; current snapshots emit version `2`. Installation validates the payload before replacing state. | A snapshot carries a committed state boundary and membership for replica recovery; it is not a general storage-format converter. Its retained messages, consumer state, attempts, lease-clock floor, counters, and request-ID deduplication must be checked as one logical image. |
| State-machine journal | `state-machine/state-machine.log` is a length-prefixed JSON journal with record version `1`, bounded individual records, and committed Raft entries. Recovery truncates only a partial final length/record; complete malformed or unsupported records fail without truncation during validation. | Journaling and checkpoint compaction provide per-file crash boundaries, not resumable directory migration. The journal's Raft log IDs and the checkpoint's applied state must agree before a target can be activated. |
| Data-group manifest | `group.json` records `stream`, `stream_id`, and `group_id`, with no version field. Preflight validates the hex path, derived identity, manifest shape, group files, and state-machine files. | A manifest can identify a group but cannot select among source/target generations or record migration phase. It must not be overloaded without a compatibility revision. |
| Peer protocol and snapshot transfer | Internal peer RPCs are big-endian length-prefixed JSON frames capped at 64 MiB. Requests carry group IDs but no explicit protocol/version handshake. Snapshot chunks are bounded at 64 KiB; an interrupted receiver currently retries from byte zero. | Rolling upgrades cannot infer mixed-version safety from successful JSON decoding. Peer RPC, command, snapshot, and storage compatibility need separate gates; retry-from-zero is recovery behavior, not resumable migration. |

Cluster startup first rejects known legacy single-group paths, checks or
initializes `storage.json`, then validates existing groups, manifests, Raft
logs, checkpoints, snapshots, and journals before `GroupManager` opens groups.
An unsupported existing state is not opened as an empty store. The fresh
directory initialization exception and the individual journal-tail truncation
performed during normal recovery are the only current mutations around this
boundary. There is no active-generation pointer, migration record, rollback
window, or operational upgrade command.

## Compatibility and downgrade policy

The proposed compatibility matrix should record four relations for every
artifact and every release pair:

| Relation | Question that must be answered |
| --- | --- |
| `read` | Can the binary decode the artifact without guessing, dropping fields, or changing logical meaning? |
| `write` | Can it write bytes that every binary still allowed to run against the store can safely read? |
| `mixed` | Can old and new binaries operate at once without breaking committed ordering, acknowledgement, recovery, deduplication, or fencing? |
| `migrate` | Is a deliberate conversion required, and what exact source/target and rollback boundaries does it have? |

The current supported opening contract is intentionally narrow:

1. A current binary may reopen state it wrote in the current local or
   clustered layout, subject to the existing identity and format checks.
2. `runnel-core` may read the explicitly recognized `RNL1`, `RNL2`, and `RNL3`
   families and recover a partial final frame according to its current rules.
3. `runnel-raft` may read version-1 state-machine checkpoints and snapshots
   through the existing read-forward conversion, as well as current version-2
   payloads. This does not include the earlier single-group directory layout.
4. No current binary-to-binary rolling-upgrade guarantee exists for the
   clustered peer protocol, command schema, or state-machine writers.

These are observed behaviors, not a promise that future releases preserve
them. Before changing any one of these artifacts, the change must add a
fixture for the old bytes and state whether the relation is `read`, `write`,
`mixed`, or `migrate` compatible.

The explicit downgrade policy for the proposed first migration is:

- Before activation and before any target-only write, a stopped new writer may
  discard or abandon the staged target and resume from a validated source
  generation. This is rollback of an unserved staging operation, not a general
  downgrade.
- After target-only bytes, offsets, consumer progress, deduplication entries,
  retention decisions, or semantic changes have been committed, an older
  binary must fail closed. It may not be pointed at the active directory by
  editing a version field, removing a marker, or renaming paths.
- Post-activation recovery requires either a target-aware binary, a tested
  reverse converter, or a verified pre-upgrade snapshot/backup. If none exists,
  the supported operator action is to migrate forward or restore the compatible
  recovery artifact; automatic downgrade is unsupported.
- Local-to-cluster movement is not a format downgrade or upgrade. It is a
  topology and producer-identity migration with its own [design boundary](single-node-to-cluster-migration.md).

This policy keeps the acceptance criteria open. A later ADR may narrow or
expand it only after implementation and failure evidence establish the
guarantees.

## Proposed operation and interruption model

The first physical conversion should be offline and side-by-side. It should
read a validated source through a compatibility reader, write immutable target
units in bounded batches, validate a complete logical image, and activate the
target by replacing a small durable generation selector. The source remains
available for rollback until the documented window expires.

An internal migration record (exact schema is open) should bind:

- migration ID, source and target generation IDs, and source/target format
  descriptors;
- cluster, node, stream/group, and engine identity;
- writer/activation epoch and the owner of the fence;
- phase, source boundary, bounded record/byte progress, checksums/counts, and
  last error; and
- rollback eligibility, backup/recovery-artifact identity, and cleanup state.

The minimum phase machine is:

`planned → copying → validating → ready → activated → complete`

with durable `failed` and `aborted` outcomes. Startup must reject an
ambiguous active migration rather than select a directory by name or by which
file parses first.

| Interruption point | Required result |
| --- | --- |
| Preflight or before copy | Source remains the only active generation; no marker or source byte is rewritten. |
| During copy | Source remains authoritative. A restart resumes from a durable bounded checkpoint or discards only unreferenced target bytes. |
| During validation | Source remains authoritative; an incomplete or mismatched target cannot be served. |
| After `ready`, before activation | Source remains authoritative; the validated target can be retried after identity and checksum verification. |
| During activation | Recovery chooses exactly one generation from a durable marker/commit protocol, never directory order or fallback-to-empty behavior. |
| After activation, during cleanup | Target remains authoritative. Orphaned source/temporary bytes are retained for a bounded cleanup pass and reported as space use. |

For local storage, the first fence may pause writes for the affected stream
while allowing unrelated streams to proceed. Existing in-flight deliveries
may be redelivered after cutover; acknowledged progress must not move
backwards. For clustered storage, the activation epoch must be a committed
metadata/group fact, every serving replica must validate the target, and a
stale leader, old writer, or delayed migration owner must be fenced before it
can append or acknowledge. Replica replacement and OpenRaft snapshot install
remain separate recovery workflows.

## Reference designs and implications for Runnel

The following primary/reference designs are evidence for constraints, not
templates Runnel has adopted.

| Reference | Relevant behavior | Difference that matters to Runnel |
| --- | --- | --- |
| [OpenRaft snapshot replication](https://docs.rs/openraft/latest/openraft/docs/protocol/replication/snapshot_replication/) and [storage traits](https://docs.rs/openraft/latest/openraft/storage/) | A snapshot carries a committed log boundary and membership. OpenRaft can remove conflicting non-committed logs because the snapshot point is committed; the storage traits separate log persistence, state-machine persistence, and snapshot installation. | Runnel should reuse the committed-boundary discipline, but must validate its application-state schema, stream/group identity, consumer progress, attempts, and producer deduplication before serving. Snapshot transfer is not a format migration or a local-to-cluster import. |
| [Apache Kafka rolling upgrades](https://kafka.apache.org/33/getting-started/upgrade/) and [KIP-384 persistent metadata versioning](https://cwiki.apache.org/confluence/display/KAFKA/KIP-384%3A%2BAdd%2Bconfig%2Bfor%2Bincompatible%2Bchanges%2Bto%2Bpersistent%2Bmetadata) | Kafka keeps the old inter-broker and message format during a rolling binary upgrade, verifies the new binaries, and advances a separate protocol/metadata gate. After an incompatible persistent-format gate, downgrade is no longer available. | Runnel needs the same distinction between installing a binary and enabling a durable/peer format, but its current static cluster lacks Kafka's negotiated protocol and metadata-version controls. Runnel must also protect acknowledged consumer and producer state, not just topic records. |
| [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/current/pgupgrade.html) | Preflight runs before mutation. The default copy/clone modes preserve an old cluster for rollback; link/swap modes save space or time but make the old cluster unusable at an earlier boundary. Old data is deleted only after the operator accepts the upgrade. | Side-by-side copying is the right first rollback model for Runnel. Space-sharing or in-place rewrites may be useful later, but they must explicitly weaken or move the rollback boundary and require filesystem-specific evidence. |
| [etcd downgrade procedure](https://etcd.io/docs/v3.7/downgrades/downgrading-etcd/) and [3.5→3.6 upgrade](https://etcd.io/docs/v3.6/upgrades/upgrade_3_6/) | etcd validates a downgrade target, takes a snapshot backup, enables a cluster-wide downgrade mode, migrates storage schema, and exposes storage/version status while rolling members. Simply replacing an old binary is not the downgrade procedure. | Runnel should require a verified recovery artifact and explicit target validation. Its smaller static cluster can start with offline maintenance, but must not imply etcd-like online schema migration until it has a cluster protocol and progress/health evidence. |
| [Online asynchronous schema change in F1](https://research.google/pubs/online-asynchronous-schema-change-in-f1/) | The paper models compatibility between old/new readers and writers and bounds the transition to adjacent schema versions; common changes can corrupt data when compatibility is assumed from parseability alone. | A future live-tail or dual-write Runnel migration needs pairwise operation proofs for publish, acknowledgement, replay, and recovery. The first proposal avoids that risk with a fence and one active writer. |

The inference from these references is a staged gate: first prove that new
software can coexist while the old durable representation remains active,
then commit an explicit format or generation change, and retain a recovery
path until that change is no longer reversible. None of these references
establishes Runnel's guarantees or resolves its filesystem and delivery
semantics.

## Smallest next implementation slice

The next code change should stop short of copying or rewriting messages. It
should establish a testable foundation inside the storage adapters:

1. Define an internal compatibility descriptor/matrix covering local record
   families, consumer checkpoint/journal, root metadata, group manifest, Raft
   log, state-machine checkpoint, snapshot payload, journal, and peer frames.
   Include logical identity, schema/encoding versions, reader/writer ranges,
   and whether mixed operation is allowed.
2. Add a versioned migration manifest parser and read-only preflight result
   with source/target generations, identity, phase, and bounded progress. Do
   not expose physical paths or offsets in the public protocol.
3. Add fixtures/tests that prove unknown or contradictory versions, malformed
   metadata, identity mismatch, missing metadata, partial layouts, stale
   manifests, and orphan staging bytes fail closed without creating an empty
   replacement or rewriting authoritative files.
4. Add a deterministic state-image comparison helper for a representative
   stream: exact payload/key/timestamp/offset order, ordinary and grouped
   consumer progress, delivery attempts, and request-ID mappings. Include the
   legacy checkpoint/snapshot read-forward cases and an explicit unsupported
   downgrade fixture.

The following slice can implement one local side-by-side conversion using a
read-only source view and the manifest. It should use bounded batches, a source fence,
checksums, restart/resume or safe discard, atomic activation, and retained
source state. Only after those tests pass should a clustered activation fact,
rolling binary fixture, or public diagnostic operation be designed.

### Verifiable test gates for the later converter

- process interruption during copy, validation, activation, restart, and
  cleanup leaves exactly one authoritative generation and never an empty store;
- rerunning a migration with the same ID is idempotent, while a conflicting
  source/target identity is rejected;
- post-copy state comparison proves logical offsets, exact opaque payloads,
  consumer progress, attempts, and deduplication are preserved;
- a stale writer and a duplicate migration owner receive a fencing outcome
  before they can append or acknowledge;
- clustered tests cover leader loss, follower restart, snapshot installation,
  an unready replica, and old-binary refusal after target-only state is active;
- diagnostics and metrics expose phase, outcome, bytes/records copied and
  validated, last error, rollback eligibility, fence rejections, readiness,
  and orphan/cleanup bytes with bounded labels; and
- a resource test measures temporary-space headroom, batch memory, recovery
  work, and behavior when the required reserve is unavailable.

## Alternatives, hypotheses, and unresolved risks

### Alternatives considered

- **Rewrite in place:** smallest temporary footprint, but an interruption can
  destroy the only source and makes rollback indistinguishable from recovery.
  Reject for the first implementation.
- **Live dual-write with a tail:** minimizes a maintenance window, but writes
  to two physical representations are not atomic and require duplicate
  resolution, acknowledgement barriers, and stale-writer fencing. Keep as a
  later hypothesis.
- **Treat snapshots as the migration format:** useful for replacing a replica
  at a committed Raft boundary, but it loses the local engine's layout,
  consumer journal, and producer identity semantics. Keep snapshot recovery
  separate.
- **Support arbitrary binary rollback:** convenient operationally, but unsafe
  after target-only state is durable. Require a reverse converter or verified
  recovery artifact instead.

### Hypotheses to test

- A short per-stream maintenance fence is sufficient for the first local
  format conversion and keeps the correctness proof smaller than live copying.
- A generation selector plus retained source is enough to make process-kill
  recovery deterministic on the supported filesystem, provided file and
  directory sync boundaries are fault-tested.
- Checksummed bounded batches can keep migration memory bounded without
  changing foreground publish/consume behavior; the resource reserve may still
  require admission or throttling.
- A cluster-wide compatibility gate can be added without exposing Raft groups
  or placement in the public engine contract, but this depends on first adding
  explicit peer/version negotiation.

### Unresolved risks

- Filesystem rename and directory-sync durability differ across supported
  filesystems. Process-kill tests are necessary but may not model power loss;
  the supported environment and backup requirement must be explicit.
- Side-by-side conversion can require nearly two retained histories plus
  journal/checkpoint headroom. The broker needs a bounded policy for pause,
  throttle, or reject when the reserve is unavailable.
- Clustered state stores stream offsets implicitly by vector position while
  local records store offsets in frames. The converter must reject gaps,
  duplicates, and truncation ambiguity rather than silently renumbering.
- Active local delivery tokens and deadlines are volatile, while clustered
  grouped delivery state is replicated. The migration barrier must specify
  which acknowledgements are accepted and which deliveries are redelivered.
- Current peer JSON frames have no negotiated version and current snapshots
  restart interrupted transfers from byte zero. A rolling-upgrade fixture may
  require protocol work before it can provide useful evidence.
- A metadata-group activation fact and per-replica target installation can
  diverge during a crash. The clustered state machine needs a model and fault
  tests proving an unready replica cannot serve or lead with incompatible
  state.
- The current implementation has no retention policy, so a migration cannot
  yet promise a bounded source-retention window. Retention and storage
  pressure must be coordinated before cleanup is automatic.

## Evidence classification and benchmark applicability

Primary evidence class: design/research. Secondary tags: storage/recovery,
compatibility/migration, operability, and resource-safety.

No runtime benchmark is applicable to this documentation-only proposal. It
changes no code, encoding, lock scope, I/O path, scheduling, resource limit,
or workload. The later converter must add focused failure/resource evidence;
the current publish, recovery, and cluster benchmarks would exercise the
unchanged baseline and cannot establish upgrade safety.

## Sources and repository evidence

Primary sources are linked in the comparison table. Repository evidence is the
current [local engine](../../crates/runnel-core/src/lib.rs), [clustered
engine](../../crates/runnel-raft/src/lib.rs), [Raft log store](../../crates/runnel-raft/src/log_store.rs),
[architecture](../architecture.md), [ADR 0007 snapshot recovery](../decisions/0007-snapshot-based-replica-recovery.md),
and [ADR 0019 storage identity](../decisions/0019-clustered-storage-identity.md).
The earlier storage-safety plan remains useful historical context, while this
follow-up records the current baseline and keeps the compatibility promise
unsettled.
