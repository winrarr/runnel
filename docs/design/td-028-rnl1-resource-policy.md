# TD-028: RNL1 allocation and resource policy

- Status: decision study; no numeric RNL1 budget or behavior change is accepted
- Baseline for this study update: `f999c1b9ad5d22408bbbe6c6276a42e825cd62ef`
- Primary evidence class: design or research
- Scope: local RNL1 write admission, recovery/indexing, delivery, replay, response materialization, and non-destructive handling of complete records outside a future policy
- Related debt: [TD-028](../tech-debt.md#td-028-rnl1-materialization-lacks-an-operational-allocation-budget)
- Related evidence: [TD-007 storage compatibility](td-007-storage-compatibility-evidence.md#evidence-matrix), [TD-028 allocation-policy evidence](../research/td-028-rnl1-allocation-policy.md), [TD-002 storage scalability](td-002-storage-scalability-evidence.md)

This study recommends the shape of a future policy and the evidence needed to select values. It does not authorize a limit, a format change, a migration, or runtime behavior changes. Current code and tests remain the behavior contract.

## Recommendation

Do not narrow the historical RNL1 read envelope until representative production-log size evidence and a non-destructive route for every complete out-of-policy record exist. The repository contains no production record-size corpus and no valid large-record boundary fixture; RNL2/RNL3 limits therefore do not establish safe legacy limits.

When implementation is considered, use separate controls for new-write admission, complete-record read eligibility, recovery working memory, retained index memory, and concurrent delivery materialization. Check a complete RNL1 record's declared lengths against policy before allocating its key or payload. Preserve complete out-of-policy bytes and report a resource-policy refusal; never treat them as a crash tail, truncate them, skip them, or dead-letter them automatically. Keep an independent read-only inventory/export or side-by-side conversion route for those records. Do not choose numeric values until corpus and workload measurements support them.

This is a recommendation for a later decision, not an accepted policy. A limit below the encoded RNL1 envelope remains a compatibility decision and requires an ADR before implementation.

## Read-only size audit

[`scripts/rnl1_size_audit.py`](../../scripts/rnl1_size_audit.py) provides a read-only inventory for representative local data directories:

```sh
python3 scripts/rnl1_size_audit.py /path/to/runnel-data > /path/outside/data/rnl1-size-audit.json
```

The argument is the broker data directory; the utility scans its direct `streams/*.log` files. On Linux it requires `O_NOATIME` and `O_NOFOLLOW` for both the stream directory and each regular log, so it neither changes access times nor follows symlinks. Run it as the data owner or with the needed `CAP_FOWNER` capability; if the flags are unavailable or not permitted, the scan fails closed instead of falling back to ordinary reads. It never starts `Broker::open` or repairs a suffix. The JSON contains scanner runtime, per-file record counts, per-format key/request-ID/payload/encoded-record byte totals and maxima, plus fixed logarithmic histograms by format. It emits file basenames and size/validation metadata, but no key, request-ID, or payload contents and no absolute source path. Keep the redirected report outside the source data directory.

The scanner follows the current reader's header/limit/completeness order. It uses one fixed 64 KiB scratch buffer per file scan, validates UTF-8 incrementally without building strings, and checks contiguous record offsets. RNL1 payload bytes are skipped after completeness is established because RNL1 has no checksum and recovery does not inspect those bytes. RNL2/RNL3 keys, request IDs, and payloads pass through the fixed buffer to validate UTF-8 and their CRC32C checksums; this makes scan time proportional to their encoded contents without allocating a field-sized buffer. The utility stops at the first malformed record in each file and does not attempt format resynchronization.

Each file and the streams directory are compared before and after inspection using device, inode, access time, size, modification time, and change time. Symlinked or non-regular `.log` entries are reported unreadable rather than followed. A changed file or directory is marked `changed_during_scan`; rerun against a quiesced store or a consistent copy before using its size evidence. Incomplete headers and records extending past the opened file size are reported as `incomplete_tail`, matching the current recovery classification, but remain byte-for-byte untouched. A malformed or changed file's per-format counts summarize only the verified prefix. Exit status is zero only when all scanned files are complete; a report with an incomplete tail, malformed record, unreadable file, or concurrent change exits nonzero so automation cannot confuse partial coverage with a complete scan.

Use size maxima and histograms as observations about only the inspected corpus. They can show how often records fall into ranges and identify observed outliers, but they cannot establish that unseen stores have no larger record, prove a safe process-memory budget, or select the RNL1 read envelope. The utility cannot infer the broker build, cluster/runtime configuration, or which production population the directory represents; record that provenance alongside the report. A compatibility decision still needs representative production-store coverage, workload/concurrency and memory evidence, an explicit treatment for complete records beyond any proposed envelope, and a tested non-destructive route. No numeric cap or runtime behavior change is accepted by adding this collector.

The deterministic fixtures in [`test_rnl1_size_audit.py`](../../scripts/benchmarks/test_rnl1_size_audit.py) cover valid mixed RNL1/RNL2/RNL3 history, malformed legacy UTF-8, current-format checksum failures, declared-size limits, incomplete headers and bodies, large declared RNL1 lengths with short tails, content redaction, and source-byte preservation. They are parser fixtures, not a production corpus or a valid near-limit broker recovery fixture.

## Current behavior and allocation sites

The source of truth is `stream_log.rs` and its tests. RNL1 has a 28-byte header and independent little-endian `u32` byte lengths for key and payload. Its encoded envelope permits `u32::MAX` bytes in each field (`28 + 2 × u32::MAX` bytes per record); this is not a claim that a process can address or use such a record. The normal RNL1 writer rejects only values that do not fit those fields. RNL2 and RNL3 impose their own 128-byte key and 64 MiB body ceilings, and request-aware RNL3 adds a request-ID ceiling. Those format-specific values do not govern RNL1.

| Path | Current materialization | What can be checked before that allocation | Important boundary |
| --- | --- | --- | --- |
| RNL1 append | Caller supplies an owned key and payload; writer emits header and then those bytes | The writer can check key/payload byte lengths before writing any frame bytes | A storage check happens after the core caller already owns the input. The server's request-frame cap is a separate ingress bound. |
| Recovery scan | Reads fixed header, checks checked length arithmetic and file completeness, allocates the full key bytes, validates UTF-8, then owns a `String`; payload is skipped with a seek | Once the fixed header and file length are known, complete-record key and payload lengths can be policy-checked before the key buffer is created | The completeness check must precede an over-policy decision so an incomplete final frame retains the existing tail-recovery meaning. |
| Tail index | Retains up to 1,024 `RecordIndex` entries, each owning its key; sparse checkpoints are separately bounded by entry count | Account key bytes before admitting/cloning an entry, or stop retaining owned keys and re-read them on delivery | An entry-count cap is not a byte cap. A metadata-only index shifts key materialization to delivery but does not by itself bound it. |
| Poll, replay, redelivery | `read_payload` creates an owned `Vec<u8>` for the full stored payload; `Message`/`ReplayMessage` own it | The indexed payload length is known before `vec!` and file read | The public engine result is a complete message. Streaming payloads would change engine and protocol behavior. |
| Dead-letter movement | Reads a complete source message, then appends a request-aware target record | Source length can be checked before reading; target format limits can be checked before append | A resource refusal must not advance source acknowledgement or turn into an automatic skip. RNL2/RNL3 target limits can make a valid RNL1 source unrepresentable. |
| Server response | Serializes a complete protocol response into a `Vec`; client response-size checking occurs at the receiving client | A known limit can reject after serialization; avoiding that allocation needs bounded/streamed serialization or a precomputed size check | Request-frame admission does not bound response serialization. A delivery can be materialized and attempt state persisted before a response is too large for a client. |

Relevant source: [`StreamLog::append_with_sync`, format limits, and record readers](../../crates/runnel-core/src/stream_log.rs#L13); [`StreamLog::open`](../../crates/runnel-core/src/stream_log.rs#L92); [`read_legacy_record`](../../crates/runnel-core/src/stream_log.rs#L751); [`read_payload`](../../crates/runnel-core/src/stream_log.rs#L552); [`Broker::poll_group`, `replay`, and `dead_letter_record`](../../crates/runnel-core/src/broker.rs#L283); [server framing and response serialization](../../crates/runnel-server/src/protocol.rs); [request and response byte limits](../../crates/runnel-protocol/src/lib.rs); [client response limit](../../crates/runnel-client/src/lib.rs).

The existing RNL1 reader first returns `None` for an incomplete header or a declared record extending beyond the current file, so normal open truncates only a suffix it regards as incomplete. For a complete RNL1 record it allocates the key and rejects malformed UTF-8 without truncating that file. It does not allocate the payload during recovery. The current tests cover a short incomplete tail, a complete invalid UTF-8 key whose bytes remain, and mixed RNL1/RNL2 replay. They do not cover valid large RNL1 records, an allocation boundary, RNL1/RNL3 mixing, or whole-store non-mutation when another stream fails recovery. See the [TD-007 evidence matrix](td-007-storage-compatibility-evidence.md#evidence-matrix) and tests [`incomplete_trailing_frame_is_discarded_on_recovery`](../../crates/runnel-core/src/lib.rs#L1883), [`complete_legacy_record_with_malformed_key_fails_closed_on_recovery`](../../crates/runnel-core/src/lib.rs#L1909), and [`versioned_reader_replays_mixed_legacy_and_versioned_frames`](../../crates/runnel-core/src/lib.rs#L2105).

The core poll path reads the candidate before persisting its next delivery-attempt event. Replay does not change ordinary consumer progress. Dead-letter movement reads and appends the target before persisting source acknowledgement. A future resource refusal can preserve these boundaries by occurring before payload materialization and by returning an error without acknowledging, skipping, or initiating dead-letter completion. The server's response serialization still needs its own outcome analysis because a complete core message may be too large to encode or return to a configured client.

## Policy dimensions and viable approaches

The key distinction is between **what may be written now**, **what complete persisted data the broker accepts**, and **how much memory a particular operation may hold**. One limit cannot answer all three.

| Approach | Before-allocation enforcement | Compatibility outcome | Assessment |
| --- | --- | --- | --- |
| Keep full encoded RNL1 reads and writes; use fallible reservations | Can avoid some infallible allocation paths, but has no application-level byte ceiling | Retains the existing encoded envelope | Insufficient as an operational budget. Rust's fallible reservation reports capacity overflow or allocator-reported failure; it does not make an allowed allocation small or bound aggregate memory. |
| Apply RNL2/RNL3 ceilings to every RNL1 record | Header lengths can be rejected before key/payload allocations once the record is known complete | Complete records outside those ceilings become unreadable through normal broker startup or delivery | Reject. The constants belong to different format contracts, and no corpus proves RNL1 history fits. |
| Bound only new RNL1 appends | Writer checks lengths before frame output | Existing records remain readable under current behavior | A useful admission control if chosen, but does not solve recovery or delivery resource risk. It also cannot prevent allocations already made by direct core callers or request decoding. |
| Add a complete-record read ceiling and refuse outliers without mutation | Recovery checks declared lengths before key allocation; delivery checks stored payload length before allocation | Below-ceiling history remains usable; complete outliers block ordinary open or the specific read, depending on policy scope | Viable only with a defined, non-destructive operator path. A limit without that path silently changes practical data accessibility. |
| Replace full key/payload materialization with streaming or offset-only metadata | Recovery can skip or validate fields in fixed-size chunks; response can stream if its API permits | Could preserve access to large records without a new record-size ceiling | Broad engine, protocol, client, retry, and dead-letter change. It reduces allocation pressure but is not a small RNL1 parser adjustment and still needs concurrency/byte budgets. |
| Convert to a bounded newer format | Source can be scanned and target written separately | Records within target limits can preserve logical fields; oversized records cannot be represented by current RNL2/RNL3 | A possible migration for representable records, not a complete escape route for all historical RNL1 data. Never overwrite the source or truncate/split/drop a record to fit. |

Apache Kafka provides a useful reference for keeping accepted batch size separate from fetch targets: its broker/topic maximum defines accepted record-batch size, while consumer fetch limits can be exceeded for the first batch to guarantee progress. Runnel's framing and semantics differ, so this supports only the policy separation, not a Runnel numeric choice. [Kafka broker configuration](https://kafka.apache.org/42/configuration/broker-configs/#brokerconfigs_message.max.bytes) · [Kafka consumer configuration](https://kafka.apache.org/42/configuration/consumer-configs/#consumerconfigs_fetch.max.bytes).

Rust documents `Vec::try_reserve` as returning capacity-overflow or allocator-reported allocation errors. This is appropriate failure plumbing after a budget is chosen, but it does not define that budget; infallible `vec![0; len]` should not be treated as a safe substitute. [Rust `Vec::try_reserve`](https://doc.rust-lang.org/stable/std/vec/struct.Vec.html#method.try_reserve).

## Recommended policy shape

Adopt the following sequence only after a separate implementation decision. The boundaries below are recommendations; this document selects no values or current behavior.

1. **Measure before choosing values.** Build a read-only inventory over representative local stores. Record per-format record counts, key and payload byte distributions and maxima, total and per-stream counts, valid/incomplete/malformed tail counts, and storage/runtime provenance. Do not collect keys or payload content by default. State which stores were inspected and which populations remain unknown.
2. **Make new-write admission explicit.** Select key, payload, and encoded-record admission limits for each write format. Validate at the earliest boundary that owns the input (request parser/admission for network input and engine/storage entry for in-process callers), then re-check in the writer before any frame bytes are written. Keep write admission separate from legacy read compatibility.
3. **Select a legacy read envelope independently.** For RNL1, inspect the fixed header, perform overflow-safe length arithmetic, and determine whether all declared bytes are present. If incomplete, retain the existing incomplete-tail path. If complete, compare each field and total encoded size with the chosen policy before allocating key bytes, constructing a string, reading a payload, or updating delivery state. Return a distinct resource-policy error with stream, offset, and declared lengths; do not label it corruption.
4. **Bound aggregate memory.** Set separate targets for recovery transient bytes, retained index/key bytes, per-operation message materialization, response serialization, and aggregate concurrent materialization. A per-record ceiling alone does not bound a 1,024-entry key cache, simultaneous requests on different streams, response copies, or dead-letter copies. If resident key bytes are bounded by omitting a key from the cache, ensure the offset remains discoverable and key validation/access happens safely when re-read.
5. **Preserve out-of-policy data.** A complete over-policy frame remains byte-for-byte intact and keeps its logical offset. Do not truncate it as a tail, skip it, split it, rewrite it in place, or move/acknowledge it as a dead letter. A refusal during poll/replay must not advance consumer progress; a dead-letter limit refusal must leave the source unacknowledged. If startup can reject one stream because of a complete over-policy frame, establish whole-store non-mutation: current `BrokerState::open` opens logs sequentially and each successful `StreamLog::open` may truncate its incomplete suffix before a later stream fails. A read-only preflight across all relevant files before any recovery mutation is the clearest route if startup refusal is selected.
6. **Offer a recovery route before enabling a lower read ceiling.** Provide an explicit read-only inventory/export path that can identify and extract complete oversized records without changing active source files, or retain a clearly documented compatibility reader. A side-by-side converter may copy only representable records, preserving key bytes, payload bytes, offset, and timestamp, and activate only after logical-equality checks. Keep the original generation until verification and activation succeed. Current RNL2/RNL3 do not represent every RNL1 envelope; conversion must stop with the first unrepresentable record and leave the original intact. Do not claim conversion alone solves oversized-history access.
7. **Define delivery outcomes and transport limits.** Specify whether a complete record that fits the read envelope but exceeds a per-request or response budget is refused, delivered through a streaming path, or made available only to an export route. A client-side response cap does not protect broker-side materialization. Any refusal must be explicit and repeatable; it must not look like `Empty`, acknowledge, or poison-redeliver through an automatic dead-letter transition.

## Read and conversion behavior to preserve

| Input condition | Recovery/read result a future implementation should preserve or explicitly decide | File and consumer state |
| --- | --- | --- |
| Incomplete final header or declared frame extending beyond EOF | Existing incomplete-tail handling, unless a separately accepted recovery rule changes it | Only the incomplete final suffix may be truncated after successful validation of the file; never reinterpret a complete record as a tail because it exceeds a cap. |
| Complete valid RNL1 within selected envelope | Recover and return identical key, payload, offset, and timestamp | No rewrite or conversion during ordinary open. |
| Complete valid RNL1 above selected envelope | Distinct resource-policy refusal; keep available through the approved compatibility/export route | No truncation, offset compaction, acknowledgement, skip, or automatic dead-letter movement. Startup-wide refusal must leave every source stream unchanged. |
| Complete malformed RNL1 (for example invalid UTF-8 key) | Corruption/invalid-data result, distinct from both a size refusal and incomplete tail | Preserve complete bytes; do not truncate. |
| Complete valid RNL1 not representable in conversion target | Conversion reports stream/offset and field lengths and stops | Source stays authoritative and unchanged; discard/retain only an isolated incomplete target generation per converter policy. |

Normal `Broker::open` is not a read-only preflight: it creates directories and scans/open logs, and `StreamLog::open` truncates an incomplete suffix. TD-007's clustered preflight evidence does not apply to this local path. A compatibility/export tool should open source streams read-only and must not call normal broker startup if its purpose is to guarantee source non-mutation.

## Focused fixture and verification plan

When a concrete policy is selected, add small deterministic fixtures around injectable test limits so boundary behavior can be proved without allocating multi-gigabyte buffers or relying on host OOM. Keep at least one real on-disk record fixture for end-to-end parser behavior.

- **Header decision order:** under a small test policy, verify complete below-limit, exactly-at-limit, and above-limit key, payload, and total-record cases. Verify policy refusal occurs before key/payload allocation or read. Use allocation/read instrumentation or a test seam; do not use memory exhaustion as the assertion.
- **Tail distinction:** pair a complete over-policy record with a short/incomplete frame that declares the same over-policy lengths. Confirm the complete one is a resource refusal and byte-preserving, while the incomplete suffix follows the selected tail-recovery rule without allocating the declared buffers.
- **Whole-store non-mutation:** create two streams so one has a recoverable incomplete tail and a later stream has a complete over-policy record. If startup refuses the store, assert every original file's bytes and length are unchanged; this catches mutation caused by sequential open order.
- **Mixed history:** cover RNL1/RNL2/RNL3 around RNL1 policy boundaries and contiguous offsets. Verify RNL2/RNL3 retain their own validation and that RNL1 limits are not inferred from their constants.
- **Valid key data:** test empty key, multi-byte UTF-8 key at the byte boundary, valid payload at the chosen boundary, invalid complete UTF-8 key, and a boundary split across buffer chunks if recovery becomes streaming.
- **Read paths:** exercise cold sparse-index replay, tail-cache poll, existing in-flight redelivery, ordinary poll, and replay. A refusal must not return `Empty`, mutate acknowledged progress, persist a new delivery attempt, or lose the candidate. Verify retry after operator remediation reaches the same offset and bytes.
- **Dead-letter path:** with attempt limits configured, verify a source record refused by resource policy is not automatically appended/acknowledged; separately verify that an oversized source that cannot fit the target format retains source progress and bytes.
- **Export/conversion:** snapshot original source bytes; export or convert supported history; compare offsets, timestamps, key bytes, payload bytes, order, and request identity where present. Verify an unrepresentable record stops conversion without replacing or modifying source, and document target cleanup/resume semantics.
- **Concurrency evidence:** measure recovery peak RSS/time, retained index bytes, delivery/replay/response peaks, and concurrent reads across streams under the actual supported request and container limits. Include large legacy records and mixed formats. Correctness fixtures establish boundaries but not operational budgets.

## Evidence needed before an ADR

The absent production record-size corpus is the main blocker to a numeric legacy read limit. Synthetic data, RNL2/RNL3 constants, default request frames, or a small sample of development logs cannot establish the maximum valid historical record. The inventory must be run against representative production stores under an approved handling process, and its report must say what it does not cover.

The policy owner must then set values and behavior for at least:

- maximum new-write key, payload, and encoded record bytes at network and engine boundaries;
- maximum accepted complete RNL1 key, payload, and encoded record bytes;
- recovery transient and retained-index byte budgets;
- per-operation read, response serialization, and aggregate concurrent materialization budgets;
- error and retry behavior for complete records outside a policy;
- startup-wide mutation guarantees and when incomplete-tail repair may run;
- operator export, compatibility read, and conversion route, including records outside current RNL2/RNL3 representability.

If evidence shows a cap is viable only by making some complete records unavailable to normal broker operations, that consequence needs explicit product and compatibility acceptance plus a tested, non-destructive retrieval route. Otherwise retain the historical read behavior and defer the read ceiling while improving measurement and failure handling where it can be done without changing access semantics.

## References and planning disposition

### Repository evidence

- [RNL1/RNL2/RNL3 constants, writers, readers, index, and payload materialization](../../crates/runnel-core/src/stream_log.rs)
- [Read-only record-size audit utility](../../scripts/rnl1_size_audit.py) and [deterministic fixtures](../../scripts/benchmarks/test_rnl1_size_audit.py)
- [Broker open, poll, replay, dead-letter, and acknowledgement ordering](../../crates/runnel-core/src/broker.rs)
- [RNL1 tail, malformed-key, mixed-format, and RNL3 recovery tests](../../crates/runnel-core/src/lib.rs)
- [TD-007 compatibility matrix](td-007-storage-compatibility-evidence.md#evidence-matrix)
- [TD-028 allocation-policy evidence](../research/td-028-rnl1-allocation-policy.md)

### Primary external references

- [Rust `Vec::try_reserve`](https://doc.rust-lang.org/stable/std/vec/struct.Vec.html#method.try_reserve) describes a fallible reservation mechanism, not an application memory budget.
- [Apache Kafka broker configuration](https://kafka.apache.org/42/configuration/broker-configs/#brokerconfigs_message.max.bytes) separates the accepted record-batch maximum from fetch limits.
- [Apache Kafka consumer configuration](https://kafka.apache.org/42/configuration/consumer-configs/#consumerconfigs_fetch.max.bytes) documents that fetch targets may be exceeded to return the first batch and permit progress.

Those references inform mechanism comparisons only. The recommendation is an inference from them, current Runnel source/tests, TD-007 evidence, and the missing production corpus. No external format or default value is proposed for Runnel.

No separate backlog item is warranted: TD-028 already captures the goal, compatibility constraint, and acceptance evidence; this study clarifies the policy dimensions and adds whole-store non-mutation and delivery-failure evidence. No ADR is added because no behavior choice is accepted. Runtime tests and benchmarks do not apply to this documentation-only study; validate links, anchors, and Markdown plus `git diff --check`.
