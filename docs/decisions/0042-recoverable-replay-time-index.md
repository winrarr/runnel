# ADR 0042: Select a recoverable replay-time index

- Status: accepted index and recovery contract; runtime implementation remains open
- Date: 2026-10-06
- Baseline: `da6b14e72ce75317ad0fa3fe05f91a28026b67b2`
- Primary evidence class: design/research; secondary: correctness/reliability, storage/recovery
- Related outcome: [Make replay an explicit and safe consumer operation](../backlog.md#make-replay-an-explicit-and-safe-consumer-operation)
- Related decisions: [ADR 0024](0024-explicit-offset-replay-read.md), [ADR 0036](0036-retained-history-and-disk-pressure-contract.md), and [ADR 0038](0038-timestamp-based-replay-selector.md)
- Design: [replay selectors and bounded sessions](../design/replay-sessions.md)
- Research: [replay time-selector semantics](../research/replay-time-selector-semantics.md)

## Context

[ADR 0038](0038-timestamp-based-replay-selector.md) accepts an inclusive
selection of the lowest logical offset whose stored `published_at_ms` is at
least T. Timestamps may be equal or decrease as offsets increase. The selector
must distinguish a complete view with no match from a retained suffix whose
deleted prefix could contain an earlier match, and it must not scan history
for every request.

The local engine appends mixed `RNL1`, `RNL2`, and `RNL3` frames to one file per
stream. Opening a stream already scans every complete frame to rebuild the
recent record cache, bounded sparse offset checkpoints, and request-ID index.
The recent cache retains 1,024 records; sparse offset checkpoints occur every
64 offsets but retain only 1,024 entries. An old logical offset can therefore
require scanning from byte zero. There is no timestamp index.

The clustered engine materializes each stream's retained messages in an
offset-ordered `Vec`. Applied commands are first appended to and synced in the
state-machine journal; the in-memory state is then updated. Checkpoints and
snapshots serialize retained messages, but have no timestamp lookup structure.
Snapshot installation and recovery reconstruct the materialized stream state.

Kafka's accepted [KIP-33 time-index design](https://cwiki.apache.org/confluence/display/KAFKA/KIP-33+-+Add+a+time+based+log+index)
uses sparse timestamp/offset entries recording the maximum timestamp seen so
far, then scans log records. It also describes segment-local timestamp indexes
and permits a search to include some records earlier than T. That design
shows the value of sparse maxima and bounded local scans, but its documented
result allowance is weaker than Runnel's exact lowest-offset rule. PostgreSQL
[BRIN indexes](https://www.postgresql.org/docs/current/brin.html) summarize
adjacent block ranges and recheck candidate tuples; their min/max summaries
are compact, but false-positive ranges can make a query visit many ranges.
Runnel needs a summary ordering that makes the first possible matching block
searchable even when individual timestamps regress.

## Decision

Use an offset-ordered **cumulative prefix-maximum checkpoint index** in both
engines. Divide the retained logical range into consecutive blocks of at most
256 records, beginning at the current retained floor. Each block checkpoint
stores the maximum `published_at_ms` observed from that floor through the
block's final record. The local checkpoint also stores the byte cursor of the
block's first record. The clustered checkpoint derives its logical block
offsets from the retained floor and block number; it does not store physical
coordinates.

These are prefix maxima, not independent per-block maxima. Therefore
checkpoint values are nondecreasing even when record timestamps regress. For
threshold T, binary-search for the first checkpoint whose prefix maximum is
at least T. If found, scan that block in logical-offset order and return its
first record whose timestamp is at least T. Every earlier block's prefix
maximum is below T, so no earlier retained record can match. If no checkpoint
qualifies, the retained view contains no match. The selected record is then
read using the existing one-record replay path. This does not change any
  selector outcome or timestamp meaning accepted by ADR 0038.

### Update and recovery

- The index is derived metadata, not an independent source of truth or a new
  durable format. Do not require a sidecar, index transaction, or additional
  sync boundary.
- On local open, build checkpoints during the existing complete-frame scan.
  Include only complete records after torn-tail recovery. A malformed complete
  frame continues to fail closed as it does today. After a successful local
  append becomes visible in the stream's serialized state transition, update
  its block prefix maximum and cursor under the same stream lock. Batch
  appends update the index in the same order as their appended records.
- In the clustered engine, update the derived checkpoints while applying the
  already-synced journal commands. Do not add index state to the replicated
  command or its durability boundary.
- Rebuild the clustered index from authoritative message timestamps when
  loading checkpoint state or replaying the journal. Exclude derived
  checkpoints from persisted state and snapshots; rebuild them when a snapshot
  is installed. This keeps snapshots free of duplicate index bytes and makes
  the index recoverable from the authoritative state accepted by the current
  recovery path, without a separate index-format migration.
- Any detected mismatch between the derived index and the authoritative
  retained messages/log invalidates the index. Rebuild from that source before
  answering a time selector; if recovery cannot establish completeness, fail
  closed. Never turn an unavailable/corrupt index into `no_match`.

The local open path remains O(N) because it already scans the complete stream
log. Index construction adds one maximum comparison per record and one compact
checkpoint per 256 records. Cluster recovery already materializes retained
messages; it adds one pass over them to build the same summaries. These are
startup costs, not per-request scans.

### Bounds and concurrency

- Search performs at most `ceil(log2(ceil(N / 256) + 1))` checkpoint
  comparisons plus at most 256 timestamp-header checks, where N is the
  captured retained record count. The local scanner reads fixed frame headers
  and seeks over key/request-ID/payload bytes for nonmatching records; it does
  not allocate or checksum-scan 256 candidate payloads for each query. It
  carries the selected frame's byte cursor directly into the one-record read,
  rather than falling back to a cold sparse-offset scan. The no-match case
  searches checkpoint summaries and examines no record block.
- Query scratch space is constant apart from the one selected record. Index
  space is sparse and deterministic: one compact summary per 256 retained
  records, plus one partially filled final block. Local summaries need the
  prefix maximum and block-start byte cursor; clustered summaries need only
  the prefix maximum. Thus metadata grows with retained history at a fixed
  ratio rather than storing one timestamp/offset entry per message. The
  index has no absolute per-stream memory cap; memory and index-space impact
  must be measured over increasing histories and tracked with retained-data
  scalability work.
- The index selects at most one record. Encoded result size remains subject to
  the negotiated response limit in [ADR 0031](0031-protocol-v2-contract.md);
  if a historical record cannot fit, return its bounded `response_too_large`
  outcome before materializing the result payload. Never truncate it. The
  index never materializes a timestamp-filtered result set.
- Local selection, block scan, and one-record read remain serialized under
  the stream's existing lock/lane. Cluster selection uses one committed
  stream-group view and the state machine's existing apply serialization.
  Concurrent publishes are included or excluded at that existing boundary.
  Time replay still leaves consumer checkpoints, acknowledgements, attempts,
  leases, and delivery tokens unchanged.

### Retention boundary

The index covers exactly the retained contiguous range `[earliest, next)`.
When no prefix has been deleted, the view is complete from offset zero. When
ADR 0036 retention is implemented, preserve a complete maximum timestamp D for
the deleted contiguous prefix and its exclusive floor as authoritative
metadata. Before physical cleanup removes any newly eligible prefix, make the
advanced floor and updated D durable locally or committed in the stream's
Raft group. Physical deletion may lag that boundary; it must never precede it.
Compute the new D from every newly removed record (or from a separately proven
complete summary for precisely that range). Include floor and D in clustered
snapshots and rebuild the retained-range index from the installed state.

Time selection checks completeness before the retained index. With floor zero,
the complete history starts at offset zero and no D summary is needed. With a
positive floor, if floor metadata or D is missing, incomplete, or untrusted,
return `history_unavailable`; if D is at least T, return
`history_unavailable`; if D is less than T, search the retained range. Only
then can an absent retained match return `no_match`. On recovery, a physical
prefix that remains after the floor advanced is logically unavailable and
must be excluded from the rebuilt index. A deletion scheme with holes is
outside this contract until it provides an equivalent proof for every deleted
range that could contain an earlier match.

## Alternatives considered

- **Assume timestamps increase and binary-search records by timestamp.**
  Rejected because current local and clustered publishers do not enforce
  monotonic time and old records may regress.
- **Sort a secondary index by timestamp.** Rejected because timestamp order
  does not identify the minimum matching logical offset without additional
  range-minimum metadata, and duplicate timestamps still require offset-order
  resolution.
- **Store each block's independent maximum and scan blocks in order.**
  Rejected because many later blocks may each contain a high timestamp; the
  search can then visit a history-proportional number of blocks. A separate
  range-maximum tree could avoid this, but costs more nodes and update/rebuild
  machinery than needed here.
- **Use sparse segment maxima as in Kafka or BRIN-style range summaries.**
  Rejected as the complete algorithm: local maxima are not monotone across
  blocks, so a left-to-right search can still visit many blocks. Kafka's KIP-33
  also allows a timestamp lookup to include earlier-timestamp records, while
  Runnel must identify the exact lowest matching offset. The chosen cumulative
  maximum preserves the sparse-summary idea while making checkpoints
  monotone and binary-searchable; one block is still rechecked in offset order.
- **Use a full per-record timestamp index or a durable independently updated
  tree.** Rejected for this slice because it adds per-message metadata or a
  second local durability/recovery protocol. The current local open and
  clustered recovery paths already scan their authoritative history, so a
  deterministic derived index meets the first selector's bounds without a
  new on-disk format.
- **Scan all retained records for each request.** Rejected because no-match
  and late-match requests would perform work proportional to history while
  holding the local stream lane or occupying clustered apply work.

## Consequences and boundaries

ADR 0038's public selector semantics remain unchanged. This decision accepts
the index algorithm, block size, derived-state recovery rule, and future
contiguous-prefix completeness boundary. It does not implement time replay,
retention, retention pins, pages, sessions, or a new protocol representation.
The index adds predictable metadata proportional to retained history; it does
not solve the local monolithic-file or clustered materialized-payload growth
tracked by TD-002 and TD-010.

## Runtime acceptance matrix

- Compare index results with a brute-force offset-order oracle for empty
  streams; T below, equal to, within, and above the timestamp range; duplicate
  timestamps; and arbitrary forward/backward timestamp sequences.
- Exercise first matches at offsets 0, 255, 256, 257, and at both ends of a
  partially filled final block. Verify exact offset selection, append-order
  continuation, at most 256 header reads for a match, and zero block reads
  for no-match.
- Verify local checkpoint construction for RNL1, RNL2, and RNL3 records,
  byte-cursor correctness, batch append ordering, torn final tails, and
  restart/rebuild. Complete malformed or corrupt log records must still fail
  recovery rather than produce an index result.
- Verify clustered index equivalence after journal replay, state checkpoint
  reopen, snapshot build/install, follower restart, and leader change. The
  index must be absent from persisted snapshots and reconstructed from their
  messages.
- With future prefix retention, cover floor movement within a block and
  across blocks, D below/equal/above T, a retained match after deleted matching
  history, missing/corrupt D, crash after durable floor advance but before
  physical deletion, and refusal to physically delete before that advance.
  Reopen must preserve the logical `[floor, next)` view whether cleanup has
  finished or still has physical prefix bytes to remove.
- Assert the per-query checkpoint-probe and 256-header bounds for no-match,
  sparse-match, and adversarial-regression workloads. Measure index bytes and
  resident memory, startup/rebuild cost, selector latency, and foreground
  publish/poll latency over increasing histories before making performance
  claims. Verify `response_too_large` for a legacy record that exceeds the
  negotiated response limit without cloning/materializing its payload or
  changing any consumer state.

This is documentation and design evidence only. No runtime or crash-recovery
result is claimed by this ADR.
