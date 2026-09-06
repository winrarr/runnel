# TD-010: Clustered retained-state materialization evidence

- Status: exploratory design and evidence note; no implementation authorized
- Last reviewed: 2026-09-06
- Baseline: `2774d69aaab462431b8fac342dba7084b8abb9d7`
- Scope: clustered data-group retained messages, materialized state, journal replay, and resource growth
- Related debt: [TD-010](../tech-debt.md#td-010-clustered-state-materializes-complete-retained-history)
- Related outcome: [Make retained-state growth independent of the hot path](../backlog.md#make-retained-state-growth-independent-of-the-hot-path)
- Separate concern: [TD-009](../tech-debt.md#td-009-snapshots-rewrite-the-complete-materialized-group-state) owns the cost and crash contract of full snapshot creation and installation

This note records what the clustered state machine currently materializes, where
retained payloads are copied or scanned, and what evidence is needed before
retained data is separated from replicated semantic state. It is not an
accepted storage decision, an implementation plan, or a commitment to a
particular database or file layout. Rust code and tests remain authoritative if
this note becomes stale.

## Question and boundary

The clustered engine has two different durable histories:

1. the compactable consensus log, which establishes the order of commands; and
2. the state-machine journal and materialized state, which preserve broker
   semantics after consensus entries are compacted.

TD-010 asks whether the second layer can keep publish, consume, replay,
acknowledgement, and recovery work predictable while retained message payloads
grow. It does not ask whether the current snapshot should be full, staged, or
extent-based. Those are TD-009 questions. A future retained-data design still
has to provide a state image or equivalent snapshot input to the snapshot
contract.

## Observed current representation

### In-memory semantic state

Each data group currently owns a `SnapshotState` containing stream state,
ordinary consumer offsets, grouped-consumer state, lease-clock state,
producer-request deduplication, and counters. A `StreamState` stores every
retained message in a `Vec<StoredMessage>`; each message owns its key, payload,
and publication timestamp. A publish assigns the next offset from the vector
length and moves the command payload into the vector. There is no retention
floor, tombstone, or external payload reference in this model.

This representation makes the first semantic model easy to inspect:

- offsets are contiguous array positions starting at zero;
- replay and ordinary polling locate a message directly by logical offset;
- grouped delivery scans the materialized vector from the consumer's committed
  offset, subject to the scheduler rules tracked separately in TD-016; and
- acknowledged consumer progress, in-flight delivery tokens, attempts, and
  deduplication live beside the retained messages in the same state image.

The authoritative definitions are [`StoredMessage` and `StreamState`](../../crates/runnel-raft/src/state_machine.rs#L109-L136)
and [`SnapshotState`](../../crates/runnel-raft/src/state_machine.rs#L182-L198).
The publish transition and offset assignment are in
[`apply_command`](../../crates/runnel-raft/src/state_machine.rs#L258-L304).

### Apply and journal path

`StateMachineStore::apply` first collects the OpenRaft entry iterator into a
`Vec`. While holding the state write lock, it serializes borrowed entries into
the framed `state-machine.log` and calls `sync_data`. Only after that durable
journal write succeeds does it move commands into the in-memory materialized
state and return command responses. This preserves the important ordering:
the state machine does not report an applied command before its write-ahead
record reaches the current local durability boundary.

The current path therefore has these memory and I/O characteristics:

| Boundary | Current behavior | Growth or copy consequence |
| --- | --- | --- |
| Apply input | The complete apply batch is collected before persistence and application. | Peak transient command memory follows the batch supplied by OpenRaft; there is no retained-history bound on this batch. |
| Journal encoding | Borrowed entries are JSON-serialized into a temporary byte buffer and appended as length-delimited records. | A publish payload exists in the input command and in the encoded journal buffer while the batch is persisted; the optimization avoids an additional clone before the move into state, but not the encoded durable copy. |
| Materialized messages | Every retained payload is owned by a `StoredMessage` in a stream vector. | Resident payload memory grows with retained history, in addition to keys, timestamps, collection capacity, consumer state, and deduplication metadata. |
| Read response | Replay, ordinary poll, and grouped poll clone key and payload bytes into the response. | One response can transiently copy a message payload; this is per-request work, not a second retained copy. Grouped scheduling scans are tracked by TD-016. |
| Journal lifetime | The journal contains applied commands after the latest state image and is compacted only as part of snapshot building or installation. | Recovery and journal compaction work grow with the unapplied-to-checkpoint suffix; a snapshot does not make the materialized retained vector bounded. |

The journal format and replay behavior are implemented in
[`state_machine_journal.rs`](../../crates/runnel-raft/src/state_machine_journal.rs)
and the apply path is in
[`StateMachineStore::apply`](../../crates/runnel-raft/src/state_machine_store.rs#L743-L775).

### Open and recovery path

On open, the store reads the JSON checkpoint if present, selects a newer
persisted snapshot when its applied log is later, then reads and parses the
entire state-machine journal before replaying entries after the selected
`last_applied_log`. The replay guard makes entries at or before the applied
boundary no-ops. An incomplete final journal record is truncated to the last
complete record and synced; a complete malformed or unsupported record fails
closed.

This gives a clear recovery invariant but not bounded retained-history work:

- `fs::read` materializes the journal file before parsing it;
- parsed journal entries own command payloads until replay moves them into the
  state, so recovery temporarily holds both the parsed suffix and the resulting
  materialized messages;
- checkpoint and snapshot deserialization similarly builds owned message
  vectors before they become the active state; and
- ordinary apply does not rewrite `state-machine.json` after every command, so
  the journal is the recovery source for changes after the most recent state
  image.

The startup sequence is visible in
[`StateMachineStore::open`](../../crates/runnel-raft/src/state_machine_store.rs#L386-L435)
and journal parsing/replay in
[`read` and `replay`](../../crates/runnel-raft/src/state_machine_journal.rs#L42-L156).
The current parser's 64 MiB per-record limit bounds one journal record, not the
total journal file, parsed apply batch, or retained state.

### Snapshot and checkpoint boundary

Snapshot building serializes borrowed views of the complete materialized state
under a read lock, then persists the complete JSON snapshot and compacts the
journal through the snapshot's applied log. The borrowed view avoids cloning
each retained message before serialization, but the encoded snapshot is still a
full retained-state representation. Installation validates and deserializes
the complete input, writes the snapshot and checkpoint, compacts the journal,
and swaps the active state only after those durable steps succeed.

These behaviors are useful correctness evidence and are deliberately kept
separate from the TD-010 redesign question. The full-state transfer,
serialization, installation staging, and snapshot cadence remain TD-009 even
when the eventual state store uses external or extent-backed retained data.
See [`build_snapshot`](../../crates/runnel-raft/src/state_machine_store.rs#L687-L729)
and [`install_snapshot`](../../crates/runnel-raft/src/state_machine_store.rs#L787-L845).

## Current correctness and retention invariants

A retained-data representation may change physical ownership but must preserve
the following logical outcomes:

1. **Order and identity.** Logical offsets remain monotonic and contiguous for
   the selected retention policy. Stream identity and data-group identity do
   not become properties of a file name, segment number, or consensus-log
   index.
2. **Durable apply ordering.** An acknowledged publish remains protected by
   the documented quorum and local state-machine durability boundaries. A
   future payload write, metadata/index write, and journal/apply record need a
   defined crash ordering; returning the same command response is not enough
   if the retained bytes are absent after restart.
3. **Replay equality.** Replay, ordinary polling, and grouped delivery return
   the same key, opaque payload, timestamp, and logical offset. A physical
   relocation or compaction must not turn a valid offset into an empty result
   or a duplicate.
4. **Consumer progress.** Ordinary and grouped acknowledgements, out-of-order
   acknowledgement state, in-flight delivery tokens, attempts, lease-clock
   floors, and dead-letter outcomes recover consistently with retained data.
   A retained-data operation cannot advance consumer state merely because an
   index or payload reference was updated.
5. **Request deduplication.** A retried publish with a known request identity
   resolves to the original offset and does not append another record. Any
   external payload map must preserve this relation across restart, recovery,
   and cleanup.
6. **No implicit retention.** The current engine never deletes messages and
   reports an unavailable replay only when the requested offset is outside the
   vector's current range. A future retention floor, replay pin, or consumer
   entitlement must be explicit before physical deletion is introduced.
7. **Recovery failure classification.** Partial active tails may follow the
   current documented recovery rule, while complete corruption, missing
   payload extents, unsupported versions, and contradictory metadata must fail
   closed or follow a separately accepted rebuild rule. Recovery must not
   silently serve a shorter or empty stream.
8. **Consensus separation.** Compaction of the Raft consensus log must never
   delete retained messages merely because their commands are no longer needed
   for consensus replay. The retained-data lifetime is governed by broker
   semantics and the future retention policy.

The current code has no retention implementation, so retention safety is a
future acceptance gate rather than an observed behavior. The local retention
and segmentation invariants in the [TD-002 evidence note](td-002-storage-scalability-evidence.md)
remain relevant to local storage but do not imply that the clustered engine
should share its physical representation.

## Existing evidence

The current tests establish semantic and recovery slices, not scalability
limits:

| Evidence | What it proves | What it does not prove |
| --- | --- | --- |
| `state_machine_journal_replays_a_retained_batch_after_restart` applies and reopens 256 messages | Journal replay reconstructs retained payloads in order after restart. | Recovery time, peak memory, larger payloads, malformed complete records, or bounded replay work. |
| `retained_history_survives_snapshot_install_and_reopen` transfers and reopens 256 messages | A full snapshot can preserve the retained first/last payloads through install and reopen. | Incremental transfer, snapshot cost at scale, concurrent apply, retention cleanup, or replacement safety in the production path. |
| `snapshots_bound_consensus_history_and_recover_state` publishes 40 messages, builds a snapshot, purges consensus history, and reopens | Consensus-log compaction is separate from retained message recovery in the tested path. | That state-machine materialization or snapshot cost is bounded as retained history grows. |
| `cluster_retained_recovery` preloads 2,048 records, restarts one process, replays offset 0, and acknowledges it | A real three-node process probe exercises restart and cold replay beyond the local 1,024-record tail-index threshold. | The probe's one recovery point does not isolate startup from journal replay, measure full retained-state replay, test retention, or establish a memory bound. |
| `retained_hot_path` preloads a selected history and measures later durable publishes | The clustered publish path has a repeatable post-preload hot-path baseline. | Consume/replay cost, state serialization cost, retention, storage amplification, or performance beyond selected history and payload sizes. |

The clustered probes record workload and resource metadata, but the repository
does not yet provide a dedicated measurement of state-machine apply batch size,
payload-copy count, parsed journal bytes, checkpoint/snapshot bytes, or peak
RSS attributable to recovery. The retained-history scenarios are diagnostic
coverage, not optimization evidence. Their workload and interpretation are
documented in the [clustered baseline benchmark guide](../../scripts/benchmarks/README.md#clustered-baseline)
and must be compared only with matching topology, payload, retained count,
durability, runtime, resource limits, and source/build conditions.

## Evidence needed before redesign

Before selecting a new retained-data representation, register a controlled
comparison against the current state machine. At minimum, vary:

- retained records around and well beyond 2,048, including 1,025, 16K, and a
  size representative of the intended operating envelope;
- payload sizes and shapes, key cardinality, request-ID deduplication, and
  grouped-consumer state;
- ordinary publish, replay from the coldest retained position, consume/ack,
  restart, snapshot build/install as a separate TD-009 case, and follower
  recovery; and
- process and container runtimes under the documented CPU, memory, filesystem,
  and durability limits.

Each case should report median and observed range for publish and delivery
latency, restart-to-ready and first-replay time, replay throughput, peak/RSS
memory, CPU, journal/checkpoint/snapshot bytes, total on-disk bytes, and bytes
read or parsed during recovery. If practical, add instrumentation for
allocation/copy counts and apply-batch sizes; otherwise state that they are
not directly observed. Keep each scenario's setup outside the measured
interval when the question is hot-path behavior, and keep full recovery work
inside the recovery interval when the question is retained-state growth.

The result is inconclusive if it changes durability, payload shape, topology,
or measured work between candidates, or if host noise exceeds the project's
benchmark stability policy. A smaller JSON image at one tiny history size is
not evidence that the representation scales.

## Candidate directions and trade-offs

The following are concrete approaches to evaluate, not requirements:

| Direction | Potential benefit | Costs and proof obligations |
| --- | --- | --- |
| Payload extents with a compact semantic index | Keeps replicated command/order and consumer state small while allowing retained bytes to be read or reclaimed independently. | Requires an atomic relation between offset, payload bytes, checksums, index generation, and journal replay; missing or orphaned extents need explicit recovery behavior. |
| Immutable per-stream data extents with bounded metadata | Makes sequential replay and independent retention units measurable without putting every payload in the state vector. | Rollover, manifest publication, offset continuity, compaction, and active-tail crash rules add compatibility surface; this overlaps local TD-002 only at the invariant level. |
| Transaction-backed key/value or LSM state with a value log | Can make applied metadata and payload references crash-atomic and provide storage-native compaction. | Durability modes, write amplification, read latency, memory cache policy, and dependency behavior must be measured against the current append/journal path. No substrate is selected by this note. |
| Bounded materialized cache over durable retained data | Reduces resident payload memory while preserving the current logical model as a short-term step. | It does not by itself bound recovery, journal replay, snapshot size, or storage growth, and may add read amplification; it cannot retire TD-010 alone. |

Regardless of direction, the consensus log, replicated semantic command
ordering, retained payload ownership, and snapshot transfer interfaces should
remain distinct. A design that merely moves the same complete payload vector
behind another type or rewrites the full JSON state less often is an
implementation change, not evidence that retained-state growth is independent
of the hot path.

## Outcome gates

Keep TD-010 open until a candidate representation demonstrates all of the
following on the intended clustered workload envelope:

### Hot path and recovery

- Post-preload publish and delivery latency remain within the documented
  product-fit budget as retained history grows, with no unreported increase in
  lock hold, allocation, or queue pressure.
- Restart and follower recovery report bounded, explainable work and preserve
  retained payloads, offsets, consumer progress, attempts, deduplication, and
  dead-letter semantics.
- Recovery does not require loading or parsing all retained payload bytes when
  the selected representation can safely validate metadata separately; any
  integrity-check trade-off is measured rather than assumed.

### Resource and storage behavior

- Peak resident memory, journal/index metadata, total disk usage, and storage
  amplification are measured across the retained-history matrix and remain
  within documented bounds or produce explicit admission outcomes.
- Apply batches and foreground reads remain bounded by configured policy;
  one large retained stream cannot consume all process memory merely because
  its history is durable.
- Snapshot and install measurements are reported separately under TD-009, so a
  retained-data result does not hide full-state transfer cost.

### Correctness and compatibility

- Fault-injected or real-process tests cover payload write, journal/index
  publication, response loss, restart, partial-tail recovery, corruption,
  compaction, and interrupted cleanup at the selected durability boundary.
- A current retained-history fixture recovers with exact logical equality,
  including opaque payload bytes, key/timestamp, logical offset, request-ID
  deduplication, consumer progress, attempts, and in-flight fencing.
- The public engine and protocol contracts do not expose physical extents,
  materialization, consensus indexes, or storage paths.
- A consequential format or semantics choice is captured in an ADR before a
  production implementation, and migration/replacement behavior is addressed
  separately from an experimental fixture.

Until these gates are met, retain the current inspectable state machine and
treat external payload storage, extent manifests, caches, and embedded
databases as competing hypotheses. Do not mark TD-010 retired because a
benchmark exists or because snapshot serialization has fewer transient clones.

## Refactor and planning assessment

No safe runtime refactor is included in this evidence-only change. The current
`StateMachineStore` and journal boundaries are explicit enough to measure, and
introducing payload-reference abstractions before their crash and retention
contract is accepted would add compatibility surface without retiring TD-010.
The snapshot lifecycle remains intentionally tracked by TD-009; no new
tech-debt item is needed.
