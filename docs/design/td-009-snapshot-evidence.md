# TD-009: Clustered snapshot scalability and compatibility evidence

- Status: exploratory evidence note; no implementation authorized
- Last reviewed: 2026-09-06
- Baseline: `49a5b53dbdff000dfcf3899d1aa8eb1be79cd6c4`
- Scope: OpenRaft state-machine snapshot creation, transfer, installation,
  recovery, and the path toward incremental or streaming snapshots
- Related debt: [TD-009](../tech-debt.md)
- Related outcomes: [Make retained-state growth independent of the hot path](../backlog.md), [Make missing-replica replacement safe](../backlog.md), and [Make durable storage upgrades safe](../backlog.md)
- Related decisions: [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md) and [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md)

This note records what the current clustered backend proves and what it does
not prove about snapshot cost. It is not an accepted storage design, a public
snapshot API, or an implementation plan. A future incremental, manifest, or
streaming representation needs a new accepted decision after the evidence
gates below pass.

## Question and current conclusion

The current snapshot is a correct first recovery primitive, but it is not a
scalable retained-history representation. Every successful build serializes
the complete materialized state of one Raft group. Every successful install
receives and validates a complete snapshot before replacing the group state.
The consensus log becomes bounded after compaction, but retained message bytes
remain in the state-machine snapshot and in the in-memory message vectors.

The immediate conclusion is therefore two-sided:

- correctness and compatibility boundaries are sufficiently explicit to keep
  the current implementation as a baseline; and
- no current benchmark justifies treating the 32-entry snapshot threshold,
  64 KiB transfer chunk, or JSON representation as production tuning for
  large retained streams.

The likely direction is a versioned snapshot manifest whose immutable retained
data can be transferred or referenced in bounded extents, with replicated
semantic state kept separate from physical payload movement. That is a
conditional hypothesis, not a commitment to a particular segment, index,
checksum, or storage library.

## Observed implementation boundary

The relevant persisted state for each metadata or stream data group is under
the group directory:

| Artifact | Current representation | Recovery role |
| --- | --- | --- |
| `raft-log.json` | Version-1 JSON Raft log with committed, vote, purge, and retained entries | Consensus history; may be purged after a snapshot. It is not retained broker history. |
| `state-machine/state-machine.json` | Version-2 JSON checkpoint containing applied log, membership, and materialized state; version 1 is read forward in memory | Full checkpoint fallback and restart recovery. |
| `state-machine/state-machine.log` | Length-prefixed JSON apply journal, record version 1, with a 64 MiB record limit | Durable apply record replayed after the selected checkpoint or snapshot. Only an incomplete final frame is truncated. |
| `state-machine/snapshot.json` | OpenRaft `SnapshotMeta` plus a JSON snapshot payload | Current snapshot cache and persisted recovery image. Atomic replacement makes the file boundary durable. |

The snapshot payload is version 2 on write and accepts version 1 (including an
omitted legacy version) on read. Its materialized body includes:

- stream IDs, group IDs, lifecycle state, and every retained message's
  timestamp, key, and opaque payload;
- ordinary consumer checkpoints and grouped-consumer state, including
  in-flight ownership, attempts, and delivery tokens;
- the replicated lease-clock floor;
- producer request-ID deduplication offsets; and
- redelivery and dead-letter counters.

OpenRaft metadata separately carries the last applied log ID, membership, and
snapshot ID. The snapshot payload does not carry the applied boundary by
itself; the metadata and payload must be treated as one image.

### Build path

`StateMachineStore::build_snapshot` holds a read lock while it serializes a
borrowed view of the complete `SnapshotState` into one `Vec<u8>`. Borrowed
views avoid cloning each `StoredMessage` before encoding, but they do not make
the operation incremental: serialization, allocation, and traversal are all
proportional to the complete materialized state.

The encoded bytes are then cloned into the cached `StoredSnapshot`, written
through an atomic temporary-file-and-rename operation, and retained as the
OpenRaft snapshot stream. The persisted wrapper is JSON, so its `Vec<u8>` data
is encoded as a JSON array rather than as a raw file. This creates additional
serialization and storage overhead beyond the snapshot payload itself.
Journal compaction then reads the journal and rewrites the suffix after the
snapshot boundary. A build failure before the cache update does not publish a
new current snapshot, but the cost already spent encoding or writing is not
recoverable work.

### Install path

The current receiver starts with an empty in-memory cursor. OpenRaft assembles
the transfer, then `StateMachineStore::install_snapshot` owns the complete
byte vector, validates the version and JSON payload, converts it to a complete
`SnapshotState`, and holds the state write lock while it:

1. atomically persists `snapshot.json`;
2. atomically persists the complete `state-machine.json` checkpoint;
3. compacts the apply journal; and
4. replaces in-memory state and publishes the current-snapshot cache.

State and the current snapshot cache remain unchanged until all durable steps
succeed. A rejected payload does not mutate state, and the focused persistence
failure test preserves the previous image. More generally, a failure after an
earlier atomic write can leave a newer durable snapshot for restart recovery,
so the accepted crash contract is “previous valid image or complete newer
image,” not “every failed install leaves every file untouched.” The trade-off
is that install latency and temporary memory/workspace demand scale with the
complete snapshot, and ordinary group operations wait behind the install's
state write lock.

### Transfer and cadence

The current OpenRaft configuration uses:

- automatic snapshots after 32 committed log entries;
- 4 log entries retained after a snapshot;
- a replication-lag threshold of 64 entries; and
- a maximum snapshot chunk size of 64 KiB.

Peer frames have a 64 MiB limit. Snapshot chunks are carried over the
group-addressed framed peer protocol. A real-process replacement test uses a
256 KiB payload to force multiple chunks, kills the receiver during three
non-final transfer attempts, and verifies recovery after a final retry. The
receiver retries from byte zero rather than persisting partial transfer state.
The metrics expose build/install failures, installed bytes, chunks, final
chunks, received bytes, and installs in progress, but not build duration,
peak memory, transfer duration, retry waste, or retained-state size.

The 32-entry cadence bounds consensus-log growth; it does not bound snapshot
size or retained-history growth. A busy group with large messages can produce
a large snapshot after only 32 entries, while a quiet group may retain a
larger log interval before another snapshot. Cadence and chunk size are
therefore operational defaults, not evidence-backed SLO settings.

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
4. **Installation is replacement-safe.** Validation or durable-write failure
   leaves the previous in-memory state and current snapshot available; a
   restart recovers the previous valid state.
5. **Legacy read-forward is narrow.** Version-1 snapshot payloads and legacy
   stream arrays are converted in memory to current stream identity/lifecycle
   state. Current writers emit version 2. This is not a mixed-version writer
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
streaming validation, bounded decompression/decoding memory, partial snapshot
resume, crash injection at every atomic-write boundary, cross-release mixed
writers, or a supported empty-replica replacement lifecycle. The replacement
experiment remains test-only under [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md).

## Known cost model and evidence gaps

For a group with `M` retained messages, payload/key bytes `B`, consumer and
dedup metadata `S`, and journal suffix `J` after the snapshot boundary, the
current operations have the following qualitative shape:

| Operation | Current work and temporary state | Evidence currently available |
| --- | --- | --- |
| Build | Traverse and JSON-encode `O(B + S)` materialized state while holding a read lock; retain an encoded copy for the snapshot cache; write a second JSON wrapper; then read/rewrite the journal suffix | Correctness tests and snapshot counters only; no controlled build latency, peak-memory, or bytes-written measurement. |
| Transfer | Send the complete encoded snapshot in chunks; the current receiver assembles the complete transfer in an in-memory cursor, and retrying starts at byte zero | Real-process multi-chunk and repeated-interruption test; no retry-waste or concurrent-transfer resource matrix. |
| Install | Decode and materialize the complete state, then write a complete snapshot and checkpoint and compact journal while holding the state write lock | Failure-preservation tests and 256-message install/reopen test; no install latency, workspace, lock-wait, or large-payload matrix. |
| Reopen | Read/validate the checkpoint and snapshot, choose the newer applied boundary, read the full journal, and replay entries after that boundary | Focused restart/recovery tests; no snapshot-size versus cold-start benchmark or memory profile. |

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
recovery work.

## Candidate future directions

These are outcome-level alternatives, not implementation instructions:

| Direction | Potential benefit | Cost or unresolved risk |
| --- | --- | --- |
| Keep complete snapshots and tune cadence | Lowest compatibility and implementation risk; easy recovery oracle | Snapshot build/install/recovery remain proportional to retained state; cadence cannot solve large-history transfer cost. |
| Versioned metadata snapshot plus immutable data extents | Lets recovery transfer a small semantic image and only the missing payload extents; opens independent retention and validation units | Requires extent identity, checksums, manifest generations, cleanup fencing, and agreement between replicated logical floors and physical data. |
| Incremental snapshots/deltas | Avoids rewriting unchanged state between snapshots | Delta chains need bounded depth, compaction, base identity, ordering, crash recovery, and a rule for a missing/corrupt ancestor. |
| Streaming snapshot encode/validate/install | Bounds encoder and receiver buffers and permits progress metrics while bytes move | Streaming cannot by itself avoid rewriting all retained bytes; atomic serving still needs a complete validated image or an equivalent durable cutover. |
| External or remote snapshot storage | Can reduce local replacement transfer pressure and support larger histories | Adds availability, credentials, consistency, garbage collection, and cross-node failure modes; not appropriate as the first response to unmeasured local cost. |

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

No safe runtime refactor is included in this evidence-only change. The current
`StateMachineStore` and OpenRaft adapter boundaries are clear enough to measure
the complete-snapshot baseline, while introducing a snapshot abstraction or a
manifest type now would create a second compatibility surface without evidence
that it solves a current product constraint.

The existing TD-009 entry remains the focused implementation debt. TD-010
continues to track the broader retained-state materialization problem, and
TD-007 tracks storage compatibility/migration; neither should be duplicated by
this note. A future implementation change should update those records together
with its accepted ADR and benchmark artifact.

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
