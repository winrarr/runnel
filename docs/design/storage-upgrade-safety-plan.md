# Safe durable storage upgrades

- Status: exploratory design proposal; not an accepted compatibility decision
- Last reviewed: 2026-09-03
- Scope: backlog outcome “Make durable storage upgrades safe” and TD-007
- Baseline: `3b22e2398dc54d55c5a8ad7887d2b9954eda7eda`

## Purpose and boundary

This slice turns the storage-upgrade outcome into a testable compatibility and
migration contract. It is a proposal for the next implementation slice, not an
ADR. It does not accept a public administration API, a final durable schema, an
online migration protocol, or general downgrade support. Those choices require
a later decision record after the evidence gates below pass.

The safety objective is: an upgrade either continues to expose the same
acknowledged logical state, or fails closed with enough information to recover;
it must never make a valid store appear empty, serve a partially migrated
store, or let an old writer mutate a new representation.

## Current boundary

At this baseline, compatibility is split across several independently parsed
artifacts:

| Layer | Current behavior | Upgrade implication |
| --- | --- | --- |
| Local records | `runnel-core` reads legacy `RNL1` frames and versioned `RNL2`/request-aware `RNL3` families. The versioned writer is opt-in. An incomplete tail is truncated to the last complete frame. | A future segmented layout must identify each segment and preserve logical offsets, timestamps, keys, payload bytes, and request identity. A reader must not guess a new layout from a filename. |
| Local consumer state | Checkpoints and delivery-attempt state are separate durable state; active deliveries are recoverable as redeliveries after restart. | A rewrite must carry contiguous progress, attempts, and any producer deduplication state. The highest observed offset is not a safe substitute for committed progress. |
| Cluster root identity | `storage.json` records the current storage metadata version, cluster identity, and node identity. Mismatches and unmarked clustered state fail closed. | Any migration must retain this identity boundary. Copying state between nodes or clusters is not a migration procedure. |
| Raft log | The local JSON log has its own format version and stores log, vote, committed, and purge state. | Consensus-log compatibility is separate from retained message compatibility. A new state-machine format must not be inferred from a Raft-log version. |
| Clustered state | State-machine checkpoints and snapshots accept a limited legacy version range; the journal has a separate version. Metadata and per-stream data groups are distinct. | A layout or state-schema change needs a compatibility gate across all groups and snapshot/replacement tests, not only a startup parser test. |

The current refusal of the earlier single-group clustered layout is the safe
default. It is an explicit unsupported migration, not evidence that a future
in-place conversion is safe.

## Implemented compatibility contract

The current implementation supports opening a fresh directory and reopening a
directory in the current split metadata/data-group layout. It also supports a
narrow read-forward case for legacy version-1 state-machine checkpoints and
snapshot payloads. Opening those legacy payloads does not rewrite them during
preflight; a later normal checkpoint or snapshot write may emit the current
version-2 payload. This is artifact compatibility, not a general migration
promise.

The following matrix is the compatibility boundary implemented and tested in
`runnel-raft`:

| Artifact or layout | Current representation | Supported legacy representation | Unsupported or refused representation | Evidence |
| --- | --- | --- | --- | --- |
| Cluster directory | `storage.json` plus `groups/metadata` and optional `groups/data/<stream>/` directories | None | Earlier root-level single-group paths, unmarked grouped state, and a partial grouped layout without metadata storage | `persistent_engine_recovers_committed_state_after_reopen`, `legacy_cluster_layout_is_rejected_without_creating_new_layout`, `unmarked_clustered_layout_is_rejected_without_guessing_identity`, `partial_cluster_layout_is_rejected_without_opening_as_empty` |
| Root identity marker | Metadata version `1` with matching cluster and node identities | None | Unknown/malformed version or identity mismatch; existing grouped state without a marker | `unsupported_storage_metadata_version_is_rejected_before_opening_groups`, `persisted_storage_rejects_cluster_identity_mismatch_without_rewriting_data`, `unmarked_clustered_layout_is_rejected_without_guessing_identity` |
| Raft log | Log format version `1` | None | Unknown versions and malformed records | `supported_raft_log_format_recovers_without_rewriting`, `unsupported_raft_log_format_is_rejected_without_rewriting` |
| State-machine checkpoint | Payload version `2` | Payload version `1` | Unknown versions, malformed JSON, and unknown required fields | `legacy_state_machine_format_recovers_metadata_messages_and_progress`, `unsupported_state_machine_checkpoint_is_rejected_without_creating_journal` |
| Snapshot payload | Payload version `2` | Payload version `1` (including the legacy omitted-version default) | Unknown versions, malformed JSON, and unknown required fields | `legacy_snapshot_format_recovers_metadata_messages_and_progress`, `unsupported_snapshot_version_is_rejected_without_creating_journal` |
| State-machine journal | Framed record version `1` | None | Unknown versions, malformed complete records, and oversized records. An incomplete final frame is the one supported crash-recovery exception and is truncated to the last complete frame. | `state_machine_journal_replays_and_discards_a_partial_tail`, `unsupported_state_machine_journal_is_rejected_without_truncating` |
| Data-group manifest | Current stream, stream identity, group identity, and path agreement | None | Missing/malformed manifest, path mismatch, identity mismatch, or unknown fields | `unsupported_data_group_log_is_rejected_before_opening_new_groups` and the persisted data-group validation paths |

Preflight validates existing state before opening Raft groups. Refusal leaves
the inspected files unchanged and must not create an empty replacement group.
The fresh-directory case is the only case that initializes `storage.json`.
In particular, a current binary must not interpret an unsupported, unmarked, or
partial directory as a new empty store.

### Supported upgrade and unsupported downgrade

The supported upgrade contract is intentionally small: install a binary that
can validate the current split layout and every artifact in the matrix, then
allow it to read the supported version-1 checkpoint/snapshot payloads. The
current writer may advance those individual payloads to version 2 during
ordinary checkpointing or snapshotting. No root-layout rewrite, Raft-log
conversion, dual-write period, or online migration is implied.

There is no supported downgrade in the current implementation. In particular,
the old single-group layout cannot be opened as the split layout, and an older
binary is not promised to read a version-2 payload or any target-only state
written by a newer binary. The code has no reverse converter, migration
manifest, activation marker, or backup-restore command. Operators must retain
and restore a verified compatible recovery artifact or perform a separately
designed migration; changing a version number, deleting the marker, or
pointing an older binary at the directory is not a downgrade procedure.

### Interrupted-operation boundary and diagnostics

The implementation has recovery behavior for individual persistence writes,
but no interrupted-migration protocol. Atomic file writes sync a temporary file
and its parent before rename; an abandoned temporary file is never selected as
active state. A journal with an incomplete final frame is truncated during
normal recovery after read-only validation, while a complete malformed or
unsupported frame fails closed without truncation. Snapshot payloads are
validated before they replace in-memory state. These are file and replay
boundaries, not evidence that a directory rewrite can resume safely.

If a process stops while a layout conversion is being performed outside the
current implementation, the only supported action is to preserve the original
directory and recover with a compatible binary or recovery copy. There is no
current activation marker from which startup can choose between source and
target generations, so a staged directory must never be guessed as active or
served as an empty store. The new partial-layout test covers the corresponding
in-repository refusal boundary for a missing metadata group.

Current diagnostics are startup errors, not an operational migration API.
They include the artifact kind, path, observed version, supported version(s),
identity mismatch, or the legacy paths that caused refusal. There are no
active-generation, migration-phase, rollback-window, progress, or orphan-byte
diagnostics yet; those remain requirements for a future migration design and
must not be represented by invented metrics in this slice.

## Evidence from reference systems and research

These systems solve related problems with different availability and data
models. Their behavior is evidence for the proposal, not a compatibility target
for Runnel.

| Reference | Observed design | Difference that matters to Runnel |
| --- | --- | --- |
| [Apache Kafka rolling upgrades](https://kafka.apache.org/32/getting-started/upgrade/) and [protocol version negotiation](https://kafka.apache.org/38/design/protocol/) | Brokers are upgraded one at a time while the inter-broker protocol and message format remain at the old compatible level. A later protocol-version change is an explicit gate after validation; Kafka documents that downgrade is possible before that gate and not after it. Clients negotiate an API version and the server rejects unsupported versions. | Separate “new binary is installed” from “new durable/protocol behavior is enabled.” Runnel needs the same two-stage boundary for a static cluster, while also preserving consumer checkpoints and producer retry identity that Kafka’s topic-log discussion does not cover. |
| [etcd 3.5→3.6 upgrade](https://etcd.io/docs/v3.6/upgrades/upgrade_3_6/) and [downgrade model](https://etcd.io/docs/v3.7/downgrades/downgrading-etcd/) | Mixed versions use the lowest common protocol. Operators take a snapshot before upgrade. Downgrade is available while the cluster remains mixed; after full upgrade, binary rollback is no longer sufficient and the documented path uses a schema-aware downgrade or snapshot restore. | A cluster-wide compatibility level and a pre-upgrade snapshot are operational requirements. A local copy of the old files is not enough once new writes have changed their meaning. |
| [etcd member replacement and corruption recovery](https://etcd.io/docs/v3.6/op-guide/data_corruption/) and [learners](https://etcd.io/docs/v3.6/learning/design-learner/) | A lost or corrupt member is stopped, backed up, removed, and added again; a learner does not vote or count toward quorum until it is caught up and promoted. | Upgrade migration and replica replacement are different workflows. A stale or empty node must not regain authority merely because its configured node ID matches. |
| [RocksDB compatibility](https://github.com/facebook/rocksdb/wiki/RocksDB-Compatibility-Between-Different-Releases), [MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST), and [options verification](https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File) | Compatibility depends on both release and options. `MANIFEST` is a transactional version-edit log; `CURRENT` points to the latest manifest, and complete atomic edit groups are applied during recovery. Old and new files can coexist while obsolete files remain referenced by live versions. | A single format integer cannot describe reader, writer, configuration, and layout compatibility. Runnel needs a generation manifest, atomic activation, retained source generations, and preflight checks for configuration-dependent incompatibility. |
| [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/17/pgupgrade.html) | Preflight checks run before mutation. The default path creates a separate destination cluster; the old cluster remains usable until the destination is started. Link mode has a smaller disk cost but loses the old-cluster rollback property after shared files are written. | Side-by-side conversion gives a useful rollback boundary. Space-saving sharing must not be adopted without explicitly changing the rollback guarantee. |
| [OpenRaft snapshot replication](https://docs.rs/openraft/latest/openraft/docs/protocol/replication/snapshot_replication/) and [storage interfaces](https://docs.rs/openraft/latest/openraft/storage/) | A snapshot carries the last applied log identity and membership. Installation saves the snapshot, replaces state, and then purges obsolete logs; conflicting non-committed logs are removed. | Snapshot installation is a recovery primitive, not a general format migration. Runnel must validate application-state schema and stream/group identity before activation and keep consensus-log compatibility distinct. |
| [F1 online asynchronous schema change](https://research.google/pubs/online-asynchronous-schema-change-in-f1/) | The paper shows that asynchronous readers and writers can corrupt shared state when schema versions are not mutually compatible. It uses intermediate states and bounds the system to at most two schema versions at a time. | If Runnel later supports online rewrites, every old/new operation pair needs a compatibility proof and a bounded version window. “The new reader can decode old bytes” is not enough to make old writers safe. |

The consistent lesson is a staged transition with an explicit activation point,
not a startup-time rewrite of the only copy. The proposed Runnel design below
is an inference from those references, constrained by Runnel’s at-least-once
delivery and topology-free public model.

## Proposed compatibility vocabulary

Every durable artifact should declare the dimensions that affect opening,
reading, writing, and serving. The exact field names are intentionally open,
but the model should distinguish at least:

- **identity:** cluster, node, stream, and data-group identity;
- **layout:** the physical directory/segment arrangement and engine family;
- **schema:** the serialized fields and state-machine interpretation;
- **record encoding:** frame version, limits, checksum, and compression;
- **peer protocol:** versions of Raft RPC and internal forwarding messages;
- **generation:** the immutable logical state selected by the active manifest;
- **writer epoch:** the fencing value that prevents a stale process or migration
  owner from appending after cutover.

For each pair of versions, document four separate relations:

1. `read`: can the binary decode the bytes without loss or guessing?
2. `write`: can it append without producing bytes an older supported reader
   cannot safely interpret?
3. `mixed`: can old and new binaries operate concurrently without violating
   ordering, acknowledgement, recovery, or fencing invariants?
4. `migrate`: is a deliberate conversion required, and what is its rollback
   boundary?

Suggested compatibility classes:

| Class | Compatibility rule | Downgrade boundary |
| --- | --- | --- |
| Patch-preserving | No durable or peer-schema change. Old and new binaries use the same active representation. | Binary rollback is allowed while the same representation remains active. |
| Additive | New readers accept old data; new fields have safe defaults. During mixed operation, writers continue emitting the old representation unless every writer has opted into the new capability. | Roll back before the activation gate or before a new-only field is durably emitted. |
| Layout/encoding major | The old reader cannot safely open the target representation. Convert side-by-side and activate a new generation only after validation. | Pointer rollback is valid only before target-only writes. Afterward use a reverse conversion, compatible binary, or backup restore. |
| Semantic | The interpretation of offsets, acknowledgements, retention, retry identity, or ordering changes. Treat it as a major change even if the serialized shape is additive. | No automatic downgrade. Require an explicit compatibility proof and a recovery artifact. |

Unknown versions, unknown required fields, invalid identity, and impossible
version combinations must fail before mutating or serving the store. Serde
defaults and a successful parse are not, by themselves, proof of semantic
compatibility.

## Proposed migration shape

The first supported physical rewrite should be side-by-side and resumable. It
may require a short per-stream maintenance fence; an online dual-write path is
left as a later hypothesis because two physical stores cannot be made atomic by
writing both in sequence.

### Durable migration state

Represent the operation with a durable migration record or manifest containing
the migration ID, source and target generations, source and target format
descriptors, identity tuple, writer epoch, phase, and bounded progress. The
target is staged outside the active generation and is never opened for normal
traffic while the phase is below `ready`.

Use these phases, or an equivalent state machine:

`planned → copying → validating → ready → cutover → complete`

An explicit `failed`/`aborted` outcome is preferable to deleting evidence.
Progress should be recorded at a bounded record/segment/chunk boundary so a
restart can resume without rescanning or trusting partially written output.

### Local path

1. **Preflight.** Check the source identity and supported version matrix, take a
   verified backup or equivalent recovery copy, check free space for source,
   target, temporary metadata, and the largest legal durable write, and reject
   an already active or ambiguous migration.
2. **Fence.** Acquire the broker’s writer ownership and advance a persisted
   writer epoch. For the first implementation, pause new writes for the stream
   at a durable boundary; existing in-flight deliveries may be redelivered, but
   their acknowledged progress must not move backwards.
3. **Copy and validate.** Read the source through its compatibility reader and
   write immutable target units in bounded batches. Preserve logical stream
   identity, offsets, publish timestamps, keys, opaque payload bytes, consumer
   checkpoints, delivery attempts, and producer/request deduplication state.
   Validate counts, offset continuity, per-unit checksums, state checksums, and
   identity before marking `ready`.
4. **Cut over.** Persist and sync the target manifest and its parent directory,
   then atomically activate the new generation through a small manifest/current
   pointer. Sync the parent directory again before reporting success. The
   source remains read-only and retained after activation.
5. **Reopen and retire.** Reopen the target through the normal recovery path,
   verify diagnostics, then release the writer fence. Retire the old generation
   only after the rollback window and backup policy permit it; failure to clean
   up is an observable space leak, not permission to remove the active target.

The exact filesystem primitive and directory-sync guarantees need an
implementation experiment. The invariant is that recovery sees either the
previous active manifest or the fully validated target, never a half-written
manifest or an empty fallback.

### Clustered path

Separate a rolling binary upgrade from a physical migration:

1. Upgrade nodes one at a time while they read and write the old compatible
   format. Keep the peer protocol, state-machine command encoding, and snapshot
   encoding at the old compatibility level during this mixed phase.
2. Before enabling a new format, verify that every voting node and every
   replacement path supports it. Record a cluster-wide compatibility level in
   the metadata group; do not infer readiness from process versions alone.
3. For a physical rewrite, create the target generation from a committed
   snapshot or equivalent logical boundary. Each replica validates the target
   locally and reports readiness keyed by migration ID and group identity.
4. Commit an activation epoch only after the required replicas are ready. A
   replica that has not activated its target must not serve the group as if it
   had; it must finish activation or rejoin through the controlled replacement
   path. New commands after the epoch must be applicable by every supported
   binary during the declared compatibility window.
5. Retain the source generation until the cluster-wide rollback boundary has
   passed and the target’s recovery has been exercised. Cleanup is independent
   of the replicated activation fact.

This is deliberately conservative for the current static cluster. An online
copy with a live tail, per-stream traffic migration, or mixed local engines
needs a separate protocol for ordering, deduplication, and stale-writer
fencing; it must not be smuggled in as a storage-file rewrite.

## Interruption and rollback contract

| Interruption point | Required recovery result |
| --- | --- |
| Before or during copy | The old generation remains active and usable. A restart resumes from recorded progress or discards only unreferenced staging output. |
| After target validation, before activation | The old generation remains authoritative. The validated target may be retried or discarded after identity checks. |
| During activation | Recovery chooses the old or new generation from the durable activation marker. It must never choose by directory ordering or “first file that parses.” |
| After activation, during cleanup | The new generation remains active. Old files and cleanup orphans may remain until a later bounded cleanup pass. |

Rollback is a state transition, not a promise that the old pointer can always be
restored:

- During a rolling binary phase with no new-only durable behavior, restore the
  previous binary and keep the old format active.
- Before target-only writes, stop the new writer and activate the retained old
  generation after validation. The migration record must say that this is
  allowed.
- After target-only writes or a semantic activation, an old binary must fail
  closed. Use a reverse migrator, a compatible target-aware binary, or a
  verified pre-upgrade snapshot. A pointer rollback that hides acknowledged
  target writes is data loss and is forbidden.
- A downgrade must never silently undo retention, consumer-progress, retry, or
  identity changes. If no safe inverse exists, the supported boundary is
  “restore or migrate forward,” not “start the old binary.”

## Identity and fencing requirements

The current clustered identity checks remain the outer boundary. A migration
must additionally bind every staged artifact and activation record to:

- the cluster and node identity that owns it;
- the logical stream and data-group identity;
- a unique migration ID and monotonic source/target generations; and
- the writer or activation epoch that authorized the transition.

For local storage, a stale process must lose the writer lock/epoch before the
new generation can accept writes. For clustered storage, the activation epoch
must be a committed group/metadata fact, and a stale leader or delayed
migration owner must receive a retryable or fencing error rather than append.
An empty, copied, or unmarked directory must not acquire authority solely from
matching configuration.

Replica replacement remains separate: a missing node is recovered as a
controlled replacement, validates the snapshot’s group identity and applied
log boundary, and becomes eligible to serve only after the consensus recovery
protocol says it is safe. The permissive OpenRaft follower-log rollback feature
is not a migration mechanism.

## Observability and operator workflow

The exact command and metric names are open, but a usable implementation must
make the following visible without requiring file inspection:

- active generation, layout/schema/record versions, supported reader/writer
  range, and identity summary;
- migration ID, source/target generations, phase, start and last-progress
  timestamps, records/bytes copied, records/bytes validated, and the last
  failure reason;
- rollback eligibility and its expiry/retention condition;
- writer-fence and stale-owner rejections, activation attempts, cutover result,
  cleanup progress, and orphan bytes; and
- clustered per-group readiness, lagging replicas, snapshot source boundary,
  and whether a node is serving, recovering, or blocked by compatibility.

Metrics should use bounded labels such as engine, phase, outcome, and reason.
Stream, group, migration, and consumer identifiers belong in structured logs or
an explicitly bounded diagnostic response, not unbounded Prometheus labels.
Readiness must be false, or the process must refuse to serve, when required
metadata is corrupt, an active migration is ambiguous, or a target is not
validated. “Started successfully” is not a recovery result.

## Actionable first implementation slice

The next code slice should be intentionally narrow:

1. Define a Runnel-owned compatibility matrix for the root identity, Raft log,
   state-machine checkpoint/journal, snapshot, and local record layers. Add
   fixture tests for supported old versions, unknown versions, malformed
   metadata, and identity mismatch.
2. Add a versioned migration manifest with source/target generations and
   `planned/copying/validating/ready/activated/complete` state. Keep it internal
   to the storage adapter; do not expose paths or physical offsets in the public
   protocol.
3. Implement a local read-only, side-by-side migration for one representative
   layout change. Make it idempotent and bounded, and preserve payloads,
   logical offsets, consumer progress, attempts, and deduplication state.
4. Add crash injection at every phase and at manifest/directory sync boundaries.
   Assert old-state usability before activation, new-state usability after
   activation, no empty fallback, and safe restart/resume.
5. Add a clustered rolling-upgrade fixture that keeps old encodings during the
   mixed phase, refuses a new format before the compatibility gate, and rejects
   an old binary after target-only data is activated. Include leader loss,
   follower restart, snapshot installation, and replacement identity checks.
6. Add diagnostics and bounded metrics before calling the migration complete.

Do not include local-to-cluster migration, live dual writes, dynamic placement,
or an automatic downgrade in this slice. They need separate invariants and
failure models.

## Acceptance criteria

This proposal is ready for a later ADR only when all of the following are
verifiable in code or operational tests:

- The compatibility matrix identifies every durable and peer artifact, defines
  reader/writer/mixed-operation behavior, and fails closed for unknown or
  contradictory versions before mutation.
- Old representative local and clustered layouts recover with identical logical
  records, stream/group identity, acknowledged consumer progress, delivery
  attempts, and producer request identity; invalid or partial layouts never
  appear as an empty store.
- A migration can be interrupted at copy, validation, activation, restart, and
  cleanup boundaries. It resumes idempotently or leaves the last active
  generation usable, and no staged orphan is served as active state.
- Activation is atomic from recovery’s perspective and is protected by a
  writer/activation epoch. Delayed old processes, stale leaders, and duplicate
  migration owners cannot append or acknowledge against the new generation.
- A real three-process rolling-upgrade test proves that old-compatible encoding
  persists during mixed binaries, that the activation gate is durable, and that
  leader/follower restart and replacement preserve acknowledged data and
  consumer progress.
- Downgrade tests demonstrate the supported pre-activation rollback and the
  post-activation fail-closed boundary. Documentation names the required
  reverse migration or snapshot-restore path where pointer rollback is unsafe.
- Diagnostics expose versions, identities, migration phase/outcome, progress,
  failure reason, rollback eligibility, and cleanup/orphan state with bounded
  resource usage and labels.
- A focused failure test and a resource test establish that migration headroom,
  batch size, temporary storage, and recovery work remain bounded for a large
  retained stream. A benchmark report must state workload and durability point
  if the implementation changes hot-path I/O or encoding.

## Unresolved risks and evidence needed

- **Online availability:** a per-stream maintenance fence is simpler but may
  not satisfy future uptime goals. Compare it with a versioned live-tail or
  dual-write design using fault tests for duplicate publish and acknowledgement
  outcomes before expanding scope.
- **Filesystem durability:** atomic rename and directory synchronization vary
  across filesystems and deployment environments. Test the supported filesystem
  matrix with power-loss or process-kill injection; do not claim crash safety
  from a parser-only test.
- **Space amplification:** side-by-side conversion can require nearly two
  retained copies plus recovery headroom. Measure admission behavior and define
  whether migration pauses, throttles, or rejects when that reserve is absent.
- **State-machine evolution:** additive JSON fields currently have different
  parser behavior across types. Build old/new fixtures for checkpoints, journal
  entries, snapshots, and command payloads instead of assuming `serde(default)`
  establishes mixed-version safety.
- **In-flight delivery:** active lease state is partly volatile today. Define
  the migration barrier’s redelivery and acknowledgement behavior before
  claiming uninterrupted consumer processing.
- **Cluster cutover:** a metadata-group activation fact and per-replica local
  activation can diverge during a crash. Model the recovery state machine and
  prove that an unready replica cannot serve or become leader with an
  incompatible representation.
- **Downgrade support:** keeping old generations is useful only while all
  writes remain representable by the old reader. Verify backup freshness and
  reverse-conversion cost before advertising a rollback window.
- **Scope boundary:** moving from local to clustered storage or changing the
  engine is a data migration with topology and producer-identity consequences,
  not a format upgrade. It remains unsupported until its own versioned
  cutover protocol exists.

## Benchmark applicability

No runtime benchmark is applicable to this documentation-only change. The
artifact changes no code, serialization, lock scope, I/O path, scheduling,
resource limit, or benchmark workload. The acceptance criteria intentionally
call for targeted migration/resource measurements only after an implementation
exists; running the current publish or cluster benchmarks now would measure the
unchanged baseline and provide no evidence about upgrade safety.

## References and evidence notes

The competitor and research links above are the primary references for the
claims in this proposal. The repository’s existing [Raft follower recovery and
replacement research](../research/raft-recovery-and-replacement.md), [ADR
0019 on clustered storage identity](../decisions/0019-clustered-storage-identity.md),
and [ADR 0007 on snapshot recovery](../decisions/0007-snapshot-based-replica-recovery.md)
provide the current Runnel-specific boundary. None of those references accepts
the migration proposal in this file; implementation and a dated ADR remain
required before it becomes a compatibility promise.
