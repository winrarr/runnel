# TD-002: Local retained-storage scalability evidence

- Status: exploratory design and evidence note; no implementation authorized
- Last reviewed: 2026-09-06
- Baseline: `d7c390a4962a9f7200df4306447a245f7bc5052a`
- Scope: local stream-log growth, segmentation, indexing, retention, and recovery
- Related debt: [TD-002](../tech-debt.md)
- Related outcomes: [Make retained data operationally scalable](../backlog.md) and [Make retained-state growth independent of the hot path](../backlog.md)
- Related design: [Retention and disk-pressure design](retention-disk-pressure-plan.md)

This note records what the current local log proves, what the existing growth
measurements do not prove, and the evidence gates for a future segmented and
indexed representation. It is not an accepted storage decision or an
implementation plan. The physical unit, index format, migration mechanism, and
retention API remain open.

## Observed baseline

The local engine currently owns one append-only `streams/<stream>.log` file per
stream. It supports three recognized frame families: legacy `RNL1`, checksummed
version-1 `RNL2`, and request-aware checksummed version-1 `RNL3`. The frame
families have fixed headers of 28, 44, and 48 bytes respectively, before key,
request-ID, and payload bytes. The normal broker path uses `RNL1`; versioned
formats are explicit core configuration/test paths.

On `StreamLog::open`, the reader starts at byte zero and parses every complete
frame to recover the next logical offset, sparse checkpoints, the bounded tail
index, and request-ID locations. `RNL1` skips payload bytes after reading the
header and key; `RNL2` and `RNL3` read payload bytes to verify their checksums.
Offsets must be contiguous. A suffix that does not contain a complete frame is
truncated to the last complete cursor; complete malformed, unsupported, or
checksum-invalid data fails recovery.

The current lookup structures deliberately bound only part of the metadata:

| Structure | Current behavior | What it does not guarantee |
| --- | --- | --- |
| Tail record index | Keeps the newest 1,024 record locations and message metadata. | A cold lookup older than the tail can still scan durable history. |
| Sparse offset index | Records every 64th offset and keeps at most 1,024 recent checkpoints. | An old checkpoint can be evicted; a cold scan can then start at byte zero. |
| Request-ID map | Retains one string-to-offset entry for every distinct request-aware ID seen in the file. | The bounded tail and sparse index do not bound request-ID metadata. |
| Durable file | Retains the complete stream history in one file. | There is no independent immutable region to reclaim, compact, or validate in isolation. |

Normal appends call `sync_data` before reporting success; a publish batch
appends its records and syncs once after the batch. Consumer checkpoints and
delivery-attempt journals are separate from the stream log. There is no
retention policy, segment manifest, active-generation selector, or supported
one-file-to-segmented migration today.

These are observations of the baseline, not compatibility promises. The
authoritative implementation is `crates/runnel-core/src/stream_log.rs` and its
recovery and bounded-index tests in `crates/runnel-core/src/lib.rs`.

## Current measured evidence

The Criterion benchmark `streaming_recovery_retained_messages` reopens a
prepared default-format stream and measures startup scanning. On the baseline
host, with 100-byte payloads and 10 samples per case, the reported central
times were:

| Retained records | Reopen time |
| ---: | ---: |
| 100 | 45.5 µs |
| 1,000 | 475 µs |
| 5,000 | 2.37 ms |
| 20,000 | 9.59 ms |

The `retained_history_restart_cold_replay` benchmark includes both reopen and
the first cold poll at offset 1,024. Its reported central times were 27.9 ms
for 65,537 records and 64.2 ms for 131,072 records. The latter cases are
deliberately beyond the 1,024-record tail cache and exercise the current sparse
lookup boundary.

These results are useful evidence of approximately linear work for this
default `RNL1`, 100-byte workload, not a product SLO or a cross-filesystem
prediction. They do not measure resident memory, request-ID-map growth,
retention cleanup, crash-at-rollover behavior, large payloads, versioned-frame
checksum cost, concurrent streams, or cold offsets whose sparse checkpoint has
been evicted. The restart-plus-cold-poll benchmark also combines two costs, so
it cannot identify how much time is spent opening versus locating the first
record. Re-run on controlled resources before making a performance claim; use
the benchmark policy in [benchmarking.md](../benchmarking.md).

The current result is not an argument to rush a format rewrite. At 20,000
records the measured reopen path is small on this host. Segmentation is
primarily justified by the unbounded growth and operational constraints that
one file cannot address: independent retention, bounded startup/recovery
work, isolated corruption or cleanup, and a path to substantially larger
retained streams. Whether it improves hot-path latency must be measured rather
than assumed.

## Invariants a future representation must preserve

Any segmented or indexed design must preserve the current public model and
these durable properties:

1. Logical offsets remain contiguous and monotonic across segment boundaries;
   a segment boundary is not a visible ordering boundary.
2. Every acknowledged publish remains durable at the documented local
   durability point. Segment rollover, metadata publication, and cleanup must
   not report success before their required bytes are durable.
3. Recovery distinguishes an allowed incomplete active-tail rule from complete
   corruption, an unsupported version, a bad checksum, and contradictory
   metadata. It must fail closed rather than silently treating a missing
   segment as an empty stream.
4. Acknowledgement progress, out-of-order acknowledgements, delivery attempts,
   and request-ID deduplication retain their current meaning across restart.
   Segmentation must not turn an acknowledged record into a duplicate or make
   an ambiguous request outcome unknowable.
5. Retention may delete only data that the selected policy says is no longer
   deliverable or replayable. A lagging consumer, active delivery, or replay
   cursor must not be bypassed by physical cleanup. The logical retention
   floor and physical reclamation remain separate concerns, especially for a
   future clustered engine.
6. Indexes and manifests are derived or durable metadata with explicit
   integrity and recovery rules. A missing or corrupt index may be rebuilt or
   must fail closed according to a documented policy; it must never change the
   logical record set silently.
7. The public stream, record, consumer, and acknowledgement model remains
   independent of file names, segment numbers, byte offsets, and index layout.

The existing storage-upgrade proposals add stricter cross-generation
requirements: identity binding, a recorded source boundary, side-by-side
target validation, activation fencing, and retained source evidence. A future
segmentation change must satisfy those requirements or receive a narrower
accepted decision before it becomes a supported format migration.

## Alternatives and trade-offs

These are candidate mechanisms, not commitments:

| Candidate | Benefit | Cost or unresolved risk |
| --- | --- | --- |
| Fixed record-count or byte-sized immutable segments | Simple rollover and independent deletion; bounded units for checksums and recovery. | Byte sizes vary with payloads; record-count sizing can produce very uneven physical units; active-tail and manifest crash rules remain necessary. |
| Time-window segments | Aligns physical units with age-based retention. | Workloads with uneven traffic create very large or tiny units; time alone does not protect lagging consumers or bound disk use. |
| Segment-local sparse indexes | Reduces cold lookup work without retaining one location per record globally. | Index rebuild, corruption, versioning, and atomic publication add recovery paths; a sparse index still has a lookup/scan trade-off. |
| Segment footer or embedded index | Keeps data and its index under one replacement boundary. | The index is unavailable until sealing; footer updates and interrupted sealing need explicit rules. |
| Separate index sidecar | Allows index rebuild or replacement without rewriting data. | Data and sidecar can disagree after a crash unless generation/checksum binding is durable and tested. |
| Full index rebuild on every startup | Smallest persistent format and fewer metadata compatibility concerns. | Reintroduces startup work proportional to retained history, which does not retire TD-002's startup goal. |
| Tiered or remote historical storage | Offers a path beyond local-disk growth. | Adds availability, latency, consistency, credentials, and operational failure modes; it is a later product decision, not a prerequisite for local segmentation. |

The likely first useful direction is immutable bounded segments with a small,
explicitly versioned manifest and segment-local lookup metadata, while keeping
the active segment append-only. That recommendation is conditional: it should
be accepted only if the evidence below shows that the extra crash and
compatibility surface buys material startup, retention, or operational benefit.
It also does not solve the request-ID map by itself. Request identity needs its
own retention, indexing, or bounded-deduplication decision.

## Migration and compatibility boundary

The existing `<stream>.log` representation may contain recognized mixed frame
families, and current recovery truncates an incomplete trailing frame. A
segmented representation must not infer compatibility from a directory listing
or from the first parsable segment. Before a supported migration, it needs an
explicit format/version identity, a deterministic active-generation rule, and
validation of:

- stream identity, frame family, limits, checksums, and contiguous offsets;
- timestamps, keys, opaque payload bytes, and request IDs;
- consumer checkpoints, out-of-order acknowledgements, and delivery attempts;
- source and target record/byte counts and the exact conversion boundary; and
- crash outcomes during copy, segment sealing, index publication, activation,
  cleanup, and restart.

The one-file source must remain authoritative until a complete target has been
validated and activated through the future storage-upgrade contract. A failed
or interrupted conversion must not truncate or rewrite the source merely to
make a target appear complete. Existing consumers must continue to receive
the same logical offsets; physical byte locations and segment identities must
remain internal.

This is an offline conversion/recovery boundary, not a commitment to online
dual writes, downgrade support, local-to-cluster movement, or a public
administration API. Those choices belong in a later ADR after the evidence
gates pass.

## Outcome and evidence gates

Retire or materially revise TD-002 only when a future implementation provides
evidence for all of the following outcomes:

### Growth and lookup

- A controlled matrix varies retained record count, payload size, key
  cardinality, request-ID usage, and cold lookup position. It reports reopen,
  first lookup, replay throughput, p50/p99/p99.9 where meaningful, resident
  memory, physical bytes, and index/metadata bytes.
- The comparison uses the current one-file baseline and the candidate layout
  on the same host, with repeated runs and explicit CPU, memory, filesystem,
  and durability settings. Results include median and observed range; a noisy
  or mismatched run is inconclusive rather than a claimed win.
- Hot-path publish and delivery behavior does not regress beyond the
  documented product-fit budget, including active-segment rollover and a
  stream with substantial retained history.

### Recovery and integrity

- Real restart/process-failure tests cover an append with an incomplete active
  frame, segment rollover, manifest or index publication, and cleanup in
  progress. They preserve offsets, payloads, request-ID deduplication,
  acknowledged consumer progress, attempts, and redelivery behavior.
- Complete corruption, unsupported versions, missing segments, contradictory
  metadata, and corrupt indexes produce explicit failure or the documented
  rebuild behavior without serving partial or empty history.
- Recovery work is bounded and observable; it does not require scanning all
  retained payload bytes when the chosen format can validate metadata without
  doing so, and any checksum trade-off is measured rather than assumed.

### Retention and migration

- Deleting an immutable unit never removes history protected by the selected
  retention and consumer/replay policy. Interrupted cleanup is restart-safe,
  bounded, and observable, and low-space behavior has explicit publish
  outcomes.
- A fixture containing each current frame family converts to the candidate
  representation with exact logical equality and leaves the source recoverable
  until the documented cleanup point. A failed conversion cannot make valid
  state appear empty.
- The public protocol and engine contract require no physical-storage changes;
  a local implementation and a future clustered implementation may use
  different physical layouts while preserving the same logical behavior.

Until these gates are met, retain the current one-file format and treat
segmentation/indexing as an exploratory design choice. Do not mark TD-002
retired merely because a segment type or index exists: the debt covers
scalability, retention, recovery, and compatibility together.

## Refactor and planning assessment

No safe runtime refactor is included in this evidence-only change. The
existing `StreamLog` boundaries are sufficiently explicit for measurement and
future replacement, while introducing segment abstractions before the format,
retention, and migration invariants are accepted would create compatibility
surface without retiring the debt. The focused follow-up remains TD-002 rather
than a speculative second storage-debt item.
