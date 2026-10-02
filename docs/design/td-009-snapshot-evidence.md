# TD-009: Clustered snapshot scalability and compatibility evidence

- Status: exploratory evidence note; no implementation authorized
- Last reviewed: 2026-10-02
- Baseline: `821f2c24b6b8feafdc6563cc6eb76746f75c34d1`
- Scope: OpenRaft state-machine snapshot creation, transfer, installation,
  recovery, and the path toward incremental or streaming snapshots
- Related debt: [TD-009](../tech-debt.md)
- Related outcomes: [Make retained-state growth independent of the hot path](../backlog.md), [Make missing-replica replacement safe](../backlog.md), and [Make durable storage upgrades safe](../backlog.md)
- Related decisions: [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md), [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md), and [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md)
- Related evidence: [TD-007 storage compatibility](td-007-storage-compatibility-evidence.md), [TD-010 retained-state materialization](td-010-retained-state-evidence.md), and [TD-026 clustered log and snapshot observations](../research/td-026-log-store-persistence-baseline.md)

This note records what the current clustered backend proves and what it does
not prove about snapshot cost and compatibility. It is not an implementation
plan, a public snapshot API, or authorization to change the format. [ADR
0023](../decisions/0023-independent-retained-storage-and-placement.md) accepts
the architectural outcome that replicated snapshots should not require copying
all retained payloads; this note does not choose a representation, transfer
protocol, or migration procedure. Those details remain exploratory until their
evidence gates pass and the affected compatibility decisions are explicit.

## Question and current conclusion

The current snapshot is a tested first recovery primitive, but it is not a
scalable retained-history representation or a supported replica-replacement
workflow. Every successful build serializes the complete materialized state of
one Raft group. Every successful install receives and validates a complete
snapshot before replacing the group state. The consensus log becomes bounded
after compaction, but retained message bytes remain in the state-machine
snapshot and in the in-memory message vectors.

The immediate conclusion is therefore two-sided:

- focused tests establish recovery slices and narrow version-1 read-forward
  behavior, but do not prove mixed-release operation, full cross-artifact
  consistency, or safety at every crash point; and
- current measurements expose snapshot-file growth during one live clustered
  workload, but do not justify treating the 32-entry snapshot threshold,
  64 KiB transfer chunk, or JSON representation as production tuning for
  large retained streams.

One candidate consistent with the accepted architectural boundary is a
versioned snapshot manifest whose immutable retained data can be transferred
or referenced in bounded extents, with replicated semantic state kept separate
from physical payload movement. The manifest format, extent identity, checks,
and transfer procedure are unselected hypotheses, not an implementation
commitment.

## Observed implementation boundary

The relevant persisted state for each metadata or stream data group is under
the group directory:

| Artifact | Current representation | Recovery role |
| --- | --- | --- |
| `raft-log.json` | Version-1 JSON Raft log with committed, vote, purge, and retained entries | Consensus history; may be purged after a snapshot. It is not retained broker history. |
| `state-machine/state-machine.json` | Version-2 JSON checkpoint containing applied log, membership, and materialized state; version 1 is read forward in memory | Full checkpoint fallback and restart recovery. |
| `state-machine/state-machine.log` | Length-prefixed JSON apply journal, record version 1, with a 64 MiB per-record limit | Durable apply record replayed after the selected checkpoint or snapshot. Only an incomplete final frame is truncated. |
| `state-machine/snapshot.json` | JSON `StoredSnapshot` wrapper containing OpenRaft `SnapshotMeta` and a JSON snapshot payload | Current snapshot cache and persisted recovery image. The atomic replacement syncs the file and parent directory. |

On [`StateMachineStore::open`](../../crates/runnel-raft/src/state_machine_store.rs#L391-L442), recovery loads the checkpoint, validates and selects the snapshot only when its applied log boundary is newer, then reads the journal and replays entries strictly after the selected boundary. Journal reading materializes its contents before replay; only a partial final frame is truncated, while a complete malformed or unsupported record fails startup. Cluster identity, group manifest, and persisted-artifact preflight are owned by the surrounding clustered storage layer. The [TD-007 evidence note](td-007-storage-compatibility-evidence.md) records that preflight validates artifact shapes and identities but does not prove mixed-release compatibility or every cross-file boundary.

The checkpoint and snapshot payload are version 2 on write and accept version
1 on read; an omitted snapshot version defaults to version 1. Missing legacy
fields such as grouped-consumer state and the lease-clock floor receive their
declared defaults. The version-2 snapshot materialized body includes:

- stream IDs, group IDs, lifecycle state, and every retained message's
  timestamp, key, and opaque payload;
- ordinary consumer checkpoints and grouped-consumer state, including
  out-of-order acknowledgements, in-flight member/token/deadline, delivery
  attempts, configured consumer policy, and the policy pinned to an in-flight
  offset;
- the replicated lease-clock floor;
- producer request-ID deduplication offsets; and
- redelivery and dead-letter counters.

OpenRaft metadata separately carries the last applied log ID, membership, and
snapshot ID. The snapshot payload does not carry the applied boundary by
itself; the metadata and payload must be treated as one image. The outer image
does not bind itself to a cluster, node, or data-group identity. Cluster
identity and stream/group mapping live in surrounding storage metadata and
group manifests; compatibility preflight validates persisted artifacts, but
payload parsing alone does not prove cross-artifact agreement. See the
[TD-007 compatibility evidence](td-007-storage-compatibility-evidence.md).

The exact reader boundary is narrow rather than a release guarantee: version
checks accept only 1 and 2, and current writers emit version 2. Top-level
checkpoint/snapshot envelopes and several stream/consumer wrappers reject
unknown fields, while nested retained-message and grouped-delivery structs do
not uniformly declare that restriction. Legacy stream arrays are mapped to
generated stream/group identity and active lifecycle in memory. Snapshot
payload validation checks JSON shape and version; it does not attach a
checksum or bind payload contents to the outer group/cluster identity. Peer
group resolution and startup storage checks provide that surrounding context,
but parsing a snapshot alone does not prove those identities agree. The
version-1 journal has no read-forward path. No mixed-version writer, rolling
upgrade, or downgrade matrix is established.

Sources: [`PersistedSnapshotState`, stream adapters, and snapshot wrapper](../../crates/runnel-raft/src/state_machine_store.rs#L58-L250), [`GroupConsumerState`](../../crates/runnel-raft/src/delivery.rs#L21-L33), [checkpoint version validation](../../crates/runnel-raft/src/state_machine_store.rs#L314-L339), and [snapshot format validation](../../crates/runnel-raft/src/state_machine_store.rs#L897-L912).

### Build path

[`StateMachineStore::build_snapshot`](../../crates/runnel-raft/src/state_machine_store.rs#L726-L768)
holds a read lock while it serializes a borrowed view of the complete
`SnapshotState` into one `Vec<u8>`. Borrowed views avoid cloning each
`StoredMessage` before encoding, but they do not make the operation
incremental: serialization, allocation, and traversal are all proportional to
the complete materialized state.

The encoded bytes are then cloned into the cached `StoredSnapshot`, written
through an atomic temporary-file-and-rename operation, and retained as the
OpenRaft snapshot stream. The persisted wrapper is JSON, so its `Vec<u8>` data
is encoded as a JSON array rather than as a raw file. This creates additional
serialization and storage overhead beyond the snapshot payload itself.
Journal compaction then reads the journal and rewrites the suffix after the
snapshot boundary. The snapshot file is durably replaced before journal
compaction and before the in-memory current-snapshot cache changes. Therefore,
a compaction error can leave a valid newer `snapshot.json` on disk while the
running cache remains old; startup may select that durable snapshot by its
applied-log boundary. A failed build does not publish a new runtime cache, but
it does not imply that no newer snapshot file was written.

### Install path

[`begin_receiving_snapshot`](../../crates/runnel-raft/src/state_machine_store.rs#L820-L824)
returns an empty `Cursor<Vec<u8>>`, so OpenRaft's receiver accumulates the
complete transfer in memory. Then
[`StateMachineStore::install_snapshot`](../../crates/runnel-raft/src/state_machine_store.rs#L826-L884)
owns the complete byte vector, validates the version and JSON payload,
materializes a complete `SnapshotState`, and holds the state write lock while it:

1. atomically persists `snapshot.json`;
2. atomically persists the complete `state-machine.json` checkpoint;
3. compacts the apply journal; and
4. replaces in-memory state and publishes the current-snapshot cache.

In-memory state and the current-snapshot cache remain unchanged until all
durable steps succeed. A rejected payload does not mutate state, and focused
tests cover both a first snapshot-file write failure and a test-only checkpoint
failure after the new snapshot is atomically persisted. In the latter case,
the injected error occurs after checkpoint encoding but before its atomic file
replacement. The install returns an error, the old checkpoint remains
byte-for-byte intact, and the running state and snapshot cache remain old;
reopening selects the complete newer snapshot because its applied-log boundary
is later. A checked install error therefore does not promise that the prior
image will remain authoritative after restart. This exercises the state-machine
store's recovery selection from the resulting files, not an abrupt process
crash, an actual checkpoint file-write failure, device-level failure, or
OpenRaft log-store/commit interaction. Journal-compaction failure and abrupt
termination at later boundaries remain untested. More
generally, a failure after an earlier atomic write can leave a newer durable
snapshot or checkpoint for restart recovery, so tests do not establish “every
failed install leaves every file untouched.” Install latency and temporary
memory/workspace demand scale with the complete snapshot, and ordinary group
operations wait behind the state write lock.

### Transfer and cadence

The current OpenRaft configuration in
[`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L23-L31)
uses:

- automatic snapshots after 32 committed log entries;
- 4 log entries retained after a snapshot;
- a replication-lag threshold of 64 entries; and
- a maximum snapshot chunk size of 64 KiB.

Peer frames have a 64 MiB limit in
[`network/framing.rs`](../../crates/runnel-raft/src/network/framing.rs#L9-L44).
Snapshot chunks are carried over the group-addressed framed peer protocol. The
feature-gated real-process replacement experiment in
[`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1244-L1398)
uses a 256 KiB payload to force multiple chunks, kills the receiver during
three non-final transfer attempts, and verifies recovery after a final retry.
The receiver retries from byte zero rather than persisting partial transfer
state. Per-process aggregate metrics expose build/install counts and failures,
installed bytes, chunks, final chunks, received bytes, and installs in
progress, but not build/install duration, peak memory, lock wait, transfer
duration, retry waste, or retained-state size. The replacement test is
experimental: [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md)
keeps permissive empty-replica recovery out of the default binary and does not
make erasing a replica directory with the same voter identity supported.

The 32-entry cadence is intended to bound consensus-log growth while snapshot
and purge progress normally; it does not establish a hard bound if that
progress is delayed or fails, and it does not bound snapshot size or
retained-history growth. A busy group with large messages can produce a large
snapshot after only 32 entries, while a quiet group may retain a larger log
interval before another snapshot. Cadence and chunk size are therefore
operational defaults, not evidence-backed SLO settings.

## Recovery and compatibility invariants

The current tests establish these properties:

1. **Complete retained history is preserved.** A 256-message state-machine
   snapshot can be installed into an empty store, reopened, and read from the
   first through last logical message.
2. **Snapshot and journal boundaries agree.** After a build, retained journal
   entries are strictly after the snapshot's last applied log ID, and reopen
   can recover state from the snapshot plus any later journal entries.
3. **Invalid snapshots fail closed.** Invalid JSON, unsupported payload
   versions, and malformed persisted snapshots are rejected before serving
   state or creating a new journal.
4. **Checked install failures preserve the running image, with restart
   selection depending on the durable boundary.** Invalid payload validation
   and a failure on the first snapshot-file write leave the prior in-memory
   state and current-snapshot cache available; the first-write fixture reopens
   the previous checkpoint. A test-only checkpoint failure after snapshot
   persistence leaves the old checkpoint and running image intact, but reopen
   selects the complete newer snapshot. This confirms a complete recoverable
   state-machine image at that checked failure point; it does not establish
   OpenRaft log-store interaction, process-crash or device-failure behavior,
   journal-compaction failure recovery, or every install boundary.
5. **Legacy read-forward is narrow.** Version-1 snapshot payloads and legacy
   stream arrays are converted in memory to current stream identity/lifecycle
   state; missing grouped-delivery and lease-clock fields use defaults.
   Current writers emit version 2. This is not a mixed-version writer
   contract or a downgrade guarantee.
6. **Consensus compaction is separate from broker retention.** Purging
   `raft-log.json` does not remove messages from the materialized snapshot.
7. **Transfer interruption is safe but not resumable.** A receiver that is
   interrupted during a multi-chunk transfer does not serve partial state and
   can retry from the beginning. Repeated interruption cost is not bounded by
   durable receiver progress.
8. **Identity remains external to the payload.** Group manifests, cluster
   storage metadata, OpenRaft snapshot metadata, and the state-machine payload
   must agree. A valid JSON payload alone does not authorize it for a group or
   cluster.

These properties do not establish a checksum or digest for snapshot payloads,
streaming validation, bounded decoding memory, partial snapshot resume, crash
injection at every atomic-write boundary, cross-release mixed writers, or a
supported empty-replica replacement lifecycle. The replacement experiment
remains test-only under [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md).

### Existing test coverage

The direct state-machine tests are in
[`runnel-raft/src/lib.rs`](../../crates/runnel-raft/src/lib.rs) and
[`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs):

| Test | Evidence provided | Not established |
| --- | --- | --- |
| `snapshots_bound_consensus_history_and_recover_state` | Builds after 40 publishes, checks the remaining journal is strictly after the snapshot log boundary, reopens, and reads the earliest retained record. | A maximum retained-state size, bounded build latency, or every log/snapshot/purge interleaving. |
| `retained_history_survives_snapshot_install_and_reopen` | Builds and installs a 256-message image, reopens both stores, and checks first/last payloads. | Large-stream scaling, full state equality across every field, or process failure during install. |
| `rejected_snapshot_install_preserves_existing_state` | Invalid transfer bytes are rejected without changing the current polled state; install-failure counters return to an idle gauge. | Filesystem or process failures after persistence begins. |
| `failed_snapshot_persistence_keeps_previous_state_and_recovers_checkpoint` | Forces the initial `snapshot.json` write to fail, checks the previous in-memory snapshot/state, then reopens the previous checkpoint. | Later persistence failures, journal compaction, or cache publication. |
| `checkpoint_failure_after_snapshot_persist_recovers_new_snapshot` | Injects a test-only checkpoint-persist error after the new `snapshot.json` is durably replaced and checkpoint encoding completes but before checkpoint replacement; checks the old checkpoint bytes and running state/cache, then reopens and verifies selection of the complete newer snapshot. | Actual checkpoint file-write failure, abrupt process/device failure, the surrounding OpenRaft log-store interaction, journal-compaction failure, and other install boundaries. |
| `legacy_snapshot_format_recovers_metadata_messages_and_progress` and `legacy_snapshot_defaults_lease_floor_and_applied_commands_advance_it` | Exercise version-1 legacy stream arrays, consumer offset, absent lease-clock default, and current replay after read-forward. | Mixed-version writers, old/new binary interoperability, or downgrade. |
| `grouped_lease_clock_floor_survives_snapshot_recovery_and_backward_time` and related grouped-delivery state-machine tests | Round-trip the current payload while checking lease-clock floor, in-flight delivery, attempts, and token fencing. | Snapshot filesystem install/recovery of every grouped-policy transition or crash timing. |
| `unsupported_snapshot_version_is_rejected_without_creating_journal` and `invalid_persisted_snapshot_is_rejected_before_startup` | Unsupported or malformed persisted snapshot data fails startup; the unsupported-version test also checks that no state-machine journal is created. | Identity/boundary agreement across otherwise parseable checkpoint, snapshot, journal, and Raft-log files. |

The only real-process multi-chunk interruption coverage is
`replacement_node_recovers_after_repeated_snapshot_interruptions` in
[`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs). It is
compiled only with `test-replacement-recovery` and run via
`just cluster-replacement-test`; the default `just verify`/required CI path
does not make it a production replacement guarantee. Regular cluster restart
coverage exercises preserved storage, not empty-voter replacement.

## Known cost model and evidence gaps

For a group with `M` retained messages, payload/key bytes `B`, consumer and
dedup metadata `S`, and journal suffix `J` after the snapshot boundary, the
current operations have the following qualitative shape:

| Operation | Current work and temporary state | Evidence currently available |
| --- | --- | --- |
| Build | Traverse and JSON-encode `O(B + S)` materialized state while holding a read lock; retain an encoded copy for the snapshot cache; write a second JSON wrapper; then read/rewrite the journal suffix | The [TD-026 live cluster sample](../research/td-026-log-store-persistence-baseline.md) observes snapshot file-size deltas and completed build counts during two single runs; no build-duration, lock-wait, or snapshot-attributed peak-memory measurement. |
| Transfer | Send the complete encoded snapshot in chunks; the current receiver assembles the complete transfer in an in-memory cursor, and retrying starts at byte zero | Real-process multi-chunk and repeated-interruption test; no retry-waste or concurrent-transfer resource matrix. |
| Install | Decode and materialize the complete state, then write a complete snapshot and checkpoint and compact journal while holding the state write lock | Failure-preservation tests, including recovery selection after an injected post-snapshot checkpoint error, and 256-message install/reopen test; no real-process incoming-install latency, temporary workspace, lock-wait, or large-payload matrix. |
| Reopen | Read/validate the checkpoint and snapshot, choose the newer applied boundary, read the full journal, and replay entries after that boundary | TD-026 reports restart readiness and acknowledgement of offset 0 after a snapshot/purge cycle in two single runs; no snapshot-size matrix, controlled cold-start distribution, or recovery memory profile. |

### Current measured evidence

The [TD-026 live clustered sample](../research/td-026-log-store-persistence-baseline.md)
provides an end-to-end observation of snapshot/purge activity, not an isolated
snapshot benchmark. On code revision
`49652a19cbd11fe68f79c602df3522a42dfaceba`, it ran one 256-publish, three-node
durable-quorum workload for each of 100-byte and 1-KiB payloads under a 2-CPU,
2-GiB scope. Both runs observed eight snapshot builds and purge advances, then
restarted node 3 and read/acknowledged offset 0. The reported per-node snapshot
file-size deltas were 354,752 B and 3,160,025 B; restart-ready durations were
53.3 ms and 108.2 ms. The observer accounted for 16.9% and 32.3% of the
measured workload interval. These are single descriptive runs, not a size
matrix or snapshot-build/install timing distribution.

The separate isolated `LogStore` persistence experiment in that same TD-026
note does not construct a `StateMachineStore`, build snapshots, or purge. Its
Raft-log rewrite timings must not be attributed to snapshot work. The live
sample improves file-growth and restart context, but neither experiment
isolates snapshot serialization, transfer, install, or lock contention.

The exact peak memory multiplier depends on allocator capacity, JSON shape,
OpenRaft buffering, and payload distribution, so it should be measured rather
than stated as a fixed number. The current code nevertheless makes two costs
unavoidable for large snapshots: a complete encoded payload must exist before
the build returns, and a complete replacement image must be available before
install can validate and apply it. The JSON wrapper also means the bytes on
disk and wire are not a direct measure of logical retained payload bytes.

No current result should be interpreted as proving that snapshots improve
hot-path performance. Snapshot builds take a read lock during encoding;
installs take a write lock across multiple durable operations; and the
consensus-log benefit can coexist with growing state-machine memory and
recovery work. The available live measurements observe these paths alongside
publish and restart work and do not attribute their costs to an individual
snapshot operation.

## Candidate future directions

These are outcome-level alternatives, not implementation instructions:

| Direction | Potential benefit | Cost or unresolved risk |
| --- | --- | --- |
| Keep complete snapshots and tune cadence | Lowest compatibility and implementation risk; easy recovery oracle | Snapshot build/install/recovery remain proportional to retained state; cadence cannot solve large-history transfer cost. |
| Versioned metadata snapshot plus immutable data extents | Lets recovery transfer a small semantic image and only the missing payload extents; opens independent retention and validation units | Requires extent identity, checksums, manifest generations, cleanup fencing, and agreement between replicated logical floors and physical data. |
| Incremental snapshots/deltas | Avoids rewriting unchanged state between snapshots | Delta chains need bounded depth, compaction, base identity, ordering, crash recovery, and a rule for a missing/corrupt ancestor. |
| Streaming snapshot encode/validate/install | Bounds encoder and receiver buffers and permits progress metrics while bytes move | Streaming cannot by itself avoid rewriting all retained bytes; atomic serving still needs a complete validated image or an equivalent durable cutover. |
| External or remote snapshot storage | Can reduce local replacement transfer pressure and support larger histories | Adds availability, credentials, consistency, garbage collection, and cross-node failure modes; there is not yet evidence that it addresses the measured bottleneck. |

The highest-value next experiment is a controlled comparison of the current
complete snapshot against one bounded candidate representation, using the same
logical state and failure schedule. It should keep the public stream and
consumer model unchanged and measure whether the extra format complexity buys
material recovery or resource improvements.

## Evidence gates for changing the representation

Do not replace the current format or mark TD-009 retired until all applicable
gates are satisfied:

### Size, latency, and resource evidence

- A repeatable matrix varies retained record count, payload size, key
  cardinality, consumer/dedup metadata, and snapshot cadence.
- Build, transfer, install, reopen, and first post-recovery operation report
  p50, p99, and p99.9 where meaningful, plus logical bytes, encoded bytes,
  physical bytes written, CPU, peak resident memory, temporary workspace, and
  lock-wait time.
- Runs compare the current complete snapshot with the candidate on the same
  host, filesystem, toolchain, resource scope, and durability settings, with
  repeated samples and median/observed-range reporting.
- The candidate does not regress ordinary publish, consume, acknowledgement,
  or leader/follower recovery within the documented product-fit budget.
- An interrupted transfer reports bytes/chunks attempted, bytes/chunks
  repeated, and time to recovery. A failure or noisy run is inconclusive, not
  a claimed win.

### Semantic and crash evidence

- Snapshot and restore preserve stream/group identity, contiguous offsets,
  opaque payload bytes, timestamps, request-ID deduplication, consumer
  checkpoints, out-of-order acknowledgements, attempts, lease-clock floor,
  in-flight fencing, and dead-letter state.
- Real process-failure tests cover build, transfer, decode, snapshot write,
  checkpoint write, journal compaction, activation/cache publication, and
  restart at each documented boundary. The result is either the prior valid
  image or the complete newer image; partial state is never served.
- Missing, corrupt, unsupported, contradictory, and stale base/manifest data
  fail closed or follow an explicitly tested rebuild rule. The recovery path
  never turns an invalid or incomplete image into an empty stream.
- A streaming or incremental receiver has bounded buffers and observable
  cancellation/cleanup behavior. It does not claim resumability unless the
  resume point is durably authenticated and tested after process and node
  failure.

### Compatibility and operations evidence

- The snapshot payload, metadata, manifest, and peer transport each have
  explicit version and compatibility rules. Read-forward support is tested
  separately from mixed-version writer interoperability and downgrade.
- A future format migration preserves the current source image until the
  target is complete, validated, and activated under the storage-upgrade
  safety contract. Snapshot transfer is not reused as an implicit migration
  protocol.
- Metrics distinguish build, transfer, install, validation, retry, cleanup,
  and activation outcomes without unbounded stream/group labels. Diagnostics
  make recovery readiness and incomplete transfer state visible.
- A supported replacement lifecycle has explicit identity, fencing, serving,
  membership, rollback, and cleanup semantics. Until then, empty-replica
  recovery remains an experiment rather than an operational promise.

## Refactor and planning assessment

No runtime refactor is included in this test-and-evidence change. The added
failure seam is compiled only for tests; the existing install ordering remains
unchanged. TD-009 already owns the snapshot install and recovery gates, so this
result updates its evidence and debt context without creating a separate
backlog item or changing the accepted snapshot architecture. The test narrows
one checked persistence boundary but does not meet TD-009's retirement
criteria. TD-010 covers retained-state materialization; TD-007 covers storage
compatibility and migration. ADR 0023 already accepts the high-level
retained-data boundary; this note leaves format and migration choices open.
A future runtime change should update the existing records and accepted
decision as its scope requires.

## References

- [OpenRaft storage interfaces](https://docs.rs/openraft/0.9.25/openraft/storage/)
- [OpenRaft integration guide](https://docs.rs/openraft/0.9.25/openraft/docs/getting_started/)
- [The Raft paper](https://raft.github.io/raft.pdf)
- [Snapshot-based replica recovery](../decisions/0007-snapshot-based-replica-recovery.md)
- [Independent retained storage and placement](../decisions/0023-independent-retained-storage-and-placement.md)
- [Safe durable storage upgrades](storage-upgrade-safety-plan.md)
- [Current state-machine snapshot implementation](../../crates/runnel-raft/src/state_machine_store.rs)
- [Focused snapshot and recovery tests](../../crates/runnel-raft/src/lib.rs)
- [Real-process interrupted-transfer test](../../crates/runnel-server/tests/cluster_smoke.rs)
