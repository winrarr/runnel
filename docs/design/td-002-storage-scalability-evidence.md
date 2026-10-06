# TD-002: Local retained-storage scalability evidence

- Status: exploratory design and evidence note; no implementation authorized
- Last reviewed: 2026-09-29
- Source and test baseline: `9c173ac295bb32cb55336255baa825959ac34d4d`
  (exact CI run [36607359414](https://github.com/winrarr/runnel/actions/runs/36607359414) passed)
- Historical Criterion baseline: `d7c390a4962a9f7200df4306447a245f7bc5052a`.
  The values below were carried forward, not rerun at the source and test
  baseline above.
- Scope: local stream-log growth, segmentation, indexing, retention, and recovery
- Related debt: [TD-002](../tech-debt.md)
- Related outcomes: [Make retained data operationally scalable](../backlog.md) and [Make retained-state growth independent of the hot path](../backlog.md)
- Related design: [Retention and disk-pressure design](retention-disk-pressure-plan.md)

This note preserves storage-growth observations from its recorded source
baseline and identifies what the historical measurements do not prove about
segmentation, indexing, retention, and recovery. Since that baseline, [ADR
0039](../decisions/0039-rnl1-write-admission-and-legacy-read-compatibility.md)
selected checksummed RNL3 version 2 as the only local stream format; the
former RNL1/RNL2 readers and writer selector are gone. Historical RNL1
measurements below remain evidence about that benchmark revision, not the
current frame path. The physical unit, general-purpose offset index,
migration mechanism, and retention API remain open. The narrower replay-time
index is now selected by [ADR 0042](../decisions/0042-recoverable-replay-time-index.md):
its derived cumulative prefix-maximum checkpoints add one row and byte cursor
per 256 retained records and are rebuilt during the existing startup scan.
That selector-specific decision does not settle generic storage segmentation
or retire this debt; its memory and startup effects still need measurement.

## Observed baseline

At the recorded source baseline, local streams used legacy RNL1, version-1
RNL2, and request-aware version-1 RNL3 frames. That historical implementation
used different fixed headers and selected RNL1 for ordinary writes. Current
local streams instead use only checksummed RNL3 version 2 for every record;
see ADR 0039 and the current [stream log](../../crates/runnel-core/src/stream_log.rs).

At the recorded source baseline, opening processed each stream from byte zero,
rebuilt lookup state, and truncated an incomplete final frame. RNL1 had no
checksum; the versioned frame families checked payloads incrementally. Current
recovery accepts only RNL3 v2, checks every stream before repairing any
incomplete suffix, and refuses old frame versions without mutation.

The current lookup structures bound record locations, but not all retained
metadata:

| Structure | Current behavior | What it does not guarantee |
| --- | --- | --- |
| Tail record index | Keeps the newest 1,024 `RecordIndex` entries with offsets, payload lengths, timestamps, and optional key/request-ID strings. Payload bodies stay on disk. | Entry count is bounded, but bytes depend on retained metadata. Older reads scan the log. |
| Sparse offset index | Keeps at most 1,024 checkpoints spaced 64 logical offsets apart, covering roughly the latest 65,536 offsets. | A read before the oldest checkpoint starts its scan at byte zero. |
| Request-ID map | Keeps one entry per distinct `RNL3` ID per stream, rebuilt on open. Duplicate IDs in a file keep the earliest offset; current retries compare key and payload with that record. | Map size grows with distinct retained IDs, outside the tail/index bounds. |
| Durable file | Retains the complete append-only stream history, including acknowledged records. | Acknowledgement does not reclaim space; there is no independent region to remove or validate. |

The request-ID path is exercised by [stream-scoped retry and restart tests](../../crates/runnel-core/src/lib.rs), and its lookup is visible in [`StreamLog`](../../crates/runnel-core/src/stream_log.rs). Aggregate local materialization and current-format memory costs are tracked separately in [TD-028](../tech-debt.md#td-028-aggregate-local-record-materialization-lacks-a-memory-budget) and [TD-007's local-stream evidence](td-007-storage-compatibility-evidence.md#evidence-matrix); this note concerns aggregate history growth and lookup cost, not a per-record format-limit policy.

Normal appends call `sync_data` before reporting success; a publish batch
appends its records and syncs once after the batch. Consumer checkpoints and
delivery-attempt journals are separate from the stream log. There is no
retention policy, segment manifest, active-generation selector, or supported
one-file-to-segmented migration today.

These are observations of the source and test baseline, not compatibility
promises. The authoritative current implementation is [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs), with recovery and bounded-index coverage in [`lib.rs`](../../crates/runnel-core/src/lib.rs).

### Reopen and cold-lookup work

The historical baseline scanned every complete record and rebuilt the offset,
tail, and request-ID structures. Its RNL1 reopen cost was proportional to
retained records and key bytes while skipping payload bodies; RNL2/RNL3 also
checksummed every retained payload byte. The broker opened stream files in a
loop, so process startup work accumulated across streams.
The resulting tail, sparse index, and request-ID maps remain resident for the
broker lifetime.

For polling, a committed offset within the tail starts with a binary lower
bound and then scans tail entries until it finds a deliverable candidate.
Older offsets start from the nearest retained sparse checkpoint and parse
records forward. If the requested offset predates the oldest retained
checkpoint, the scan starts at byte zero. Direct record/replay reads use the
same tail-or-sparse strategy. Thus the checkpoint bounds scan work only within
its retained window; it does not create a bound for older history or for a
consumer that must skip many acknowledged/in-flight candidates.

## Committed measurements and current benchmark coverage

The values carried forward here are historical Criterion central estimates
recorded against `d7c390a4962a9f7200df4306447a245f7bc5052a`; they have not been
rerun against the source and test baseline at the top of this note. The
repository contains the summary values but no raw Criterion report for these
local cases, so per-sample spread and full host/run provenance are unavailable.

[`streaming_recovery_retained_messages`](../../crates/runnel-core/benches/broker.rs)
prepares one default-format stream outside the timed loop, then repeatedly
opens it. The source configures 10 samples per case, a one-second warm-up, and
a three-second measurement window. The recorded cases use 100-byte payloads
and reported these central reopen times:

| Retained records | Reopen time |
| ---: | ---: |
| 100 | 45.5 µs |
| 1,000 | 475 µs |
| 5,000 | 2.37 ms |
| 20,000 | 9.59 ms |

[`retained_history_restart_cold_replay`](../../crates/runnel-core/benches/broker.rs)
prepares the stream and consumer checkpoint outside the timed loop, then
measures both reopen and the first poll at offset 1,024, using the same
100-byte payload. Its reported central times were 27.9 ms for 65,537 records
and 64.2 ms for 131,072 records. At 65,537 records the requested offset still
has a retained sparse checkpoint; at 131,072 it predates the retained
checkpoint window and the poll scan starts at byte zero. Both times include
the full open scan, so this pair does not separate that extra cold-lookup work
from reopen cost.

These values support approximately linear reopen-time growth for one historical
default-RNL1 stream with 100-byte payloads on that host: 200 times as many records
from 100 to 20,000 corresponded to about 211 times the reported central time.
They are growth evidence for this case, not a product SLO, throughput result,
or cross-filesystem prediction.

The recorded source also defined a separate
[`retained_history_lookup`](../../crates/runnel-core/benches/broker.rs)
case for 5,000 and 20,000 records. It creates and opens a prepared stream in
Criterion's setup closure, then times a poll from the midpoint checkpoint.
This exercises a tail-cache miss with an in-window sparse checkpoint,
isolating the poll path from the open scan. No result values or raw report for
this local case are checked in, so it provides no measured latency claim.

Neither historical result set measures resident memory or request-ID-map
growth; large payloads; current RNL3 v2 checksum cost; many streams; publish or
delivery throughput; tail latency; retention, cleanup, or reclamation; physical
storage bytes or write amplification; or resource behavior at an operational
limit. The cases do not exercise segment rollover or cleanup crashes because
the measured implementation has no segments or reclamation. They also do not
establish cold filesystem-page-cache behavior: the benchmark clears neither
OS caches nor controls cache state. The combined
restart/poll result cannot attribute time to open versus lookup, and the
separate lookup case has no committed result. These limitations leave aggregate current-format materialization and retained-history growth unresolved;
the former is tracked by [TD-028](../tech-debt.md#td-028-aggregate-local-record-materialization-lacks-a-memory-budget),
while this note and its linked backlog outcomes address aggregate growth. Use
the benchmark policy in [benchmarking.md](../benchmarking.md) for controlled
comparisons.

These measurements do not justify a format rewrite by themselves. At 20,000
records the historical reopen estimate is small on its host, while the current
single-file layout still has no independent retention or reclamation unit and
its startup/index rebuild scales with retained history. Segmentation is a
candidate for those operational limits and larger retained streams. Any
claimed benefit to publish or delivery latency, memory, or disk amplification
needs separate controlled measurements.

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
segmentation and general-purpose storage indexing as exploratory design
choices. The narrower replay-time lookup is selected by ADR 0042 but remains
unimplemented and does not settle the physical storage representation. Do not
mark TD-002 retired merely because a segment type or index exists: the debt
covers scalability, retention, recovery, and compatibility together.

## Refactor and planning assessment

This evidence-only change found no narrow runtime cleanup supported by the
review. Introducing segment abstractions before the format, retention, and
migration invariants are accepted would add runtime surface without retiring
the debt. Aggregate retained-history growth and its missing measurements
remain in TD-002 and the linked backlog outcomes; current-format aggregate materialization is tracked by TD-028; TD-007 records
that old local frame versions are refused. No new debt item or backlog change is needed.
