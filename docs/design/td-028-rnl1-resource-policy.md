# TD-028: RNL1 allocation and resource policy

- Status: first compatibility policy accepted by [ADR 0039](../decisions/0039-rnl1-write-admission-and-legacy-read-compatibility.md) and local RNL1 write admission implemented; bounded legacy reads remain open
- Baseline for this policy update: `f6bc65cbe19a5902616aeeaa3ce46dedae583d2a`
- Primary evidence class: design/research; secondary: storage safety
- Scope: local RNL1 write admission, recovery/indexing, delivery, replay, response materialization, and non-destructive handling of complete records outside a future read policy
- Related debt: [TD-028](../tech-debt.md#td-028-rnl1-materialization-lacks-an-operational-allocation-budget)
- Related evidence: [TD-007 storage compatibility](td-007-storage-compatibility-evidence.md#evidence-matrix), [TD-028 allocation-policy evidence](../research/td-028-rnl1-allocation-policy.md), [TD-002 storage scalability](td-002-storage-scalability-evidence.md)

This document records the accepted first policy, its implementation, and the evidence still required for bounded historical reads. The runtime enforces the selected new-write bound; that bound does not make arbitrary historical records safely materializable.

## Accepted policy summary

New RNL1 appends are limited to a 128-byte UTF-8 key and a 64 MiB payload, matching the existing per-field limits for RNL2/RNL3. The storage writer checks before any frame bytes are written and returns an explicit `invalid_record` rejection for scalar and per-record batch publish. A rejected-only batch does not notify delivery waiters; mixed batches retain independent per-record results and notify when at least one item is accepted. The values are existing format limits and a cap on future persisted records, not a measured whole-process memory budget.

No lower cap is imposed on complete historical RNL1 reads. Recovery continues to distinguish an incomplete tail from a complete frame, and must not reject, truncate, skip, rewrite, acknowledge, or automatically dead-letter a complete record only because its declared field lengths exceed the new-write limits. This preserves eligibility under the current reader; it does not promise that every host can allocate, replay, or return an arbitrarily large record. Existing process, client-response, and target-format limits may still constrain delivery, and the current reader does not provide a reliable bounded-memory refusal.

The size audit and the existing RNL2/RNL3 constants do not prove a safe legacy read ceiling or an aggregate memory budget. Before any future lower read ceiling is enabled, provide a read-only, bounded-memory way to inspect and export complete oversized records with their offsets, timestamps, keys, and payload bytes; make normal startup refuse visibly before mutating any stream; and verify source bytes remain unchanged. Current RNL2/RNL3 conversion is insufficient for records outside those formats' representable range. Any read ceiling must be a separate ADR backed by representative corpus and workload evidence.

## Read-only size audit

[`scripts/rnl1_size_audit.py`](../../scripts/rnl1_size_audit.py) provides a read-only inventory for representative local data directories:

```sh
python3 scripts/rnl1_size_audit.py /path/to/runnel-data > /path/outside/data/rnl1-size-audit.json
```

The argument is the broker data directory; the utility scans its direct `streams/*.log` files. On Linux it requires `O_NOATIME` and `O_NOFOLLOW` for both the stream directory and each regular log, so it neither changes access times nor follows symlinks; regular log opens also use `O_NONBLOCK` to avoid waiting if an entry is replaced by a FIFO after inspection. Run it as the data owner or with the needed `CAP_FOWNER` capability; if the flags are unavailable or not permitted, the scan fails closed instead of falling back to ordinary reads. It never starts `Broker::open` or repairs a suffix. The JSON contains scanner runtime, per-file record counts, per-format key/request-ID/payload/encoded-record byte totals and maxima, plus fixed logarithmic histograms by format. It emits file basenames and size/validation metadata, but no key, request-ID, or payload contents and no absolute source path. Keep the redirected report outside the source data directory.

The scanner follows the current reader's header/limit/completeness order. Its per-record scratch buffer is fixed at 64 KiB independent of record size; the file-name list, per-file summaries, and JSON report grow with the number of `.log` files. It validates UTF-8 incrementally without building strings and checks contiguous record offsets. RNL1 payload bytes are skipped after completeness is established because RNL1 has no checksum and recovery does not inspect those bytes. RNL2/RNL3 keys, request IDs, and payloads pass through the fixed buffer to validate UTF-8 and their CRC32C checksums; this makes scan time proportional to their encoded contents without allocating a field-sized buffer. The utility stops at the first malformed record in each file and does not attempt format resynchronization.

Each file and the streams directory are compared before and after inspection using device, inode, access time, size, modification time, and change time. Symlinked or non-regular `.log` entries are reported unreadable rather than followed. A changed file or directory is marked `changed_during_scan`; rerun against a quiesced store or a consistent copy before using its size evidence. Incomplete headers and records extending past the opened file size are reported as `incomplete_tail`, matching the current recovery classification, but remain byte-for-byte untouched. A malformed or changed file's per-format counts summarize only the verified prefix. Exit status is zero only when all scanned files are complete; a report with an incomplete tail, malformed record, unreadable file, or concurrent change exits nonzero so automation cannot confuse partial coverage with a complete scan.

Use size maxima and histograms as observations about only the inspected corpus. They can show how often records fall into ranges and identify observed outliers, but they cannot establish that unseen stores have no larger record, prove a safe process-memory budget, or select a RNL1 read envelope. The utility cannot infer the broker build, cluster/runtime configuration, or which production population the directory represents; record that provenance alongside the report. ADR 0039 selects only new-write limits from existing RNL2/RNL3 behavior. Representative stores and workload/concurrency evidence are still required before selecting any lower legacy read ceiling or claiming a whole-process budget. No read-policy behavior is accepted by adding this collector.

The deterministic fixtures in [`test_rnl1_size_audit.py`](../../scripts/benchmarks/test_rnl1_size_audit.py) cover valid mixed RNL1/RNL2/RNL3 history, malformed legacy UTF-8, current-format checksum failures, declared-size limits, incomplete headers and bodies, large declared RNL1 lengths with short tails, content redaction, unchanged source bytes and metadata, symlink rejection, and prompt FIFO reporting. They are parser fixtures, not a production corpus or a valid near-limit broker recovery fixture.

## Current behavior and allocation sites

The source of truth is `stream_log.rs` and its tests. RNL1 has a 28-byte header and independent little-endian `u32` byte lengths for key and payload. Its encoded envelope permits `u32::MAX` bytes in each field (`28 + 2 × u32::MAX` bytes per record); this is not a claim that a process can address or use such a record. New RNL1 appends now enforce the 128-byte key and 64 MiB payload values selected by ADR 0039. The reader still accepts complete historical RNL1 records beyond these values; RNL2/RNL3's own limits are not retroactive read limits.

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
| Bound only new RNL1 appends | Writer checks lengths before frame output | Existing records remain eligible for recovery and reads under current behavior | **Selected by ADR 0039:** cap key/payload at 128 bytes/64 MiB, matching existing RNL2/RNL3 per-field limits. This reduces the size of future persisted records but does not solve legacy recovery, delivery, response, or aggregate memory risk, nor prevent allocations made before the writer. |
| Add a complete-record read ceiling and refuse outliers without mutation | Recovery checks declared lengths before key allocation; delivery checks stored payload length before allocation | Below-ceiling history remains usable; complete outliers block ordinary open or the specific read, depending on policy scope | Viable only with a defined, non-destructive operator path. A limit without that path silently changes practical data accessibility. |
| Replace full key/payload materialization with streaming or offset-only metadata | Recovery can skip or validate fields in fixed-size chunks; response can stream if its API permits | Could preserve access to large records without a new record-size ceiling | Broad engine, protocol, client, retry, and dead-letter change. It reduces allocation pressure but is not a small RNL1 parser adjustment and still needs concurrency/byte budgets. |
| Convert to a bounded newer format | Source can be scanned and target written separately | Records within target limits can preserve logical fields; oversized records cannot be represented by current RNL2/RNL3 | A possible migration for representable records, not a complete escape route for all historical RNL1 data. Never overwrite the source or truncate/split/drop a record to fit. |

Apache Kafka provides a useful reference for keeping accepted batch size separate from fetch targets: its broker/topic maximum defines accepted record-batch size, while consumer fetch limits can be exceeded for the first batch to guarantee progress. Runnel's framing and semantics differ, so this supports only the policy separation, not a Runnel numeric choice. [Kafka broker configuration](https://kafka.apache.org/42/configuration/broker-configs/#brokerconfigs_message.max.bytes) · [Kafka consumer configuration](https://kafka.apache.org/42/configuration/consumer-configs/#consumerconfigs_fetch.max.bytes).

Rust documents `Vec::try_reserve` as returning capacity-overflow or allocator-reported allocation errors. This is appropriate failure plumbing after a budget is chosen, but it does not define that budget; infallible `vec![0; len]` should not be treated as a safe substitute. [Rust `Vec::try_reserve`](https://doc.rust-lang.org/stable/std/vec/struct.Vec.html#method.try_reserve).

## Implementation and future read-policy gates

The accepted first implementation is complete and intentionally narrower than a complete process-memory budget. Core tests cover the new write limits, non-mutating rejection, independent batch outcomes, rejected-only notification behavior, and replay of historical records above the write limits. The remaining gates are:

1. Preserve every complete historical RNL1 record through the existing recovery and read path, even when its key or payload exceeds the new write ceiling. Do not reuse write limits as read limits.
2. Keep incomplete-tail handling independent: only an actually incomplete suffix may enter the existing repair path. A complete malformed record remains an error and is not truncated.
3. Do not claim that the new write cap bounds recovery, the key cache, delivery materialization, response serialization, or aggregate concurrent memory. These remain explicit follow-up resource outcomes.
4. Before a future read ceiling below the encoded RNL1 envelope, implement read-only bounded-memory inspection/export for complete outliers and a whole-store non-mutating preflight. Refuse with a distinct resource-policy result that names the stream, offset, field lengths, and selected limit; preserve all source bytes and consumer state. Export must preserve logical offset, timestamp, key bytes, and payload bytes and must not require the record to fit RNL2/RNL3.
5. A complete read-budget solution must cover recovery transient bytes, retained key/index bytes, operation materialization, response serialization, and aggregate concurrent work separately. Any streaming or refusal path must preserve poll/replay progress and dead-letter source acknowledgement ordering.

## Read and conversion behavior to preserve

| Input condition | Current policy | File and consumer state |
| --- | --- | --- |
| Incomplete final header or declared frame extending beyond EOF | Existing incomplete-tail handling, unless a separately accepted recovery rule changes it | Only the incomplete final suffix may be truncated after successful validation of the file; never reinterpret a complete record as a tail because it exceeds a cap. |
| New RNL1 append within selected write limits | Accept with unchanged key, payload, and next logical offset | Writer validates before any header or field bytes. |
| New RNL1 append above either selected write limit | Explicit rejected input; publish-batch rejection is per item | No frame bytes, index entry, offset advance, or consumer-state change. Later valid batch items may still append under the existing independent-item contract. |
| Complete valid historical RNL1, including fields above the new write limits | Continue current recovery and read behavior; write limits are not a retroactive read ceiling | Preserve exact file bytes, key/payload, offset, and timestamp during ordinary open. This is eligibility, not a guarantee of successful arbitrary-size materialization on every host. |
| Complete malformed RNL1 (for example invalid UTF-8 key) | Corruption/invalid-data result, distinct from both a size refusal and incomplete tail | Preserve complete bytes; do not truncate. |
| Complete valid RNL1 not representable in conversion target | Conversion reports stream/offset and field lengths and stops | Source stays authoritative and unchanged; discard/retain only an isolated incomplete target generation per converter policy. |

Normal `Broker::open` is not a read-only preflight: it creates directories and scans/open logs, and `StreamLog::open` truncates an incomplete suffix. TD-007's clustered preflight evidence does not apply to this local path. A compatibility/export tool should open source streams read-only and must not call normal broker startup if its purpose is to guarantee source non-mutation.

## Focused implementation and later read-policy verification

For the selected write policy, test exact acceptance at 128 key bytes and 64 MiB payload bytes, and rejection immediately above each value before any durable bytes or in-memory index change. A large payload boundary test may allocate roughly 64 MiB; it is bounded by the selected limit and must not rely on host OOM. Batch coverage must prove per-item rejection and that a following valid item receives the next available offset. A complete on-disk historical record with a key above 128 bytes must still reopen and replay with identical logical fields. Keep these separate from any future read-ceiling fixtures.

- **Writer boundaries:** cover key at/above 128 bytes and payload at/above 64 MiB; rejection occurs before header/field writes, does not advance `next_offset` or indexes, and does not alter consumer state. Use both single and batch publish.
- **Historical read compatibility:** hand-build a complete RNL1 frame with a key beyond 128 bytes, reopen it, and replay it. Preserve a valid RNL1 payload beyond 64 MiB in a separate fixture if the test resource budget permits; otherwise report that specific gap rather than treating a key-only fixture as proof of payload compatibility.
- **Tail distinction:** keep the existing incomplete-tail and malformed-complete-record tests. Any future read limit must additionally prove that completeness is established before resource refusal, so an actually incomplete suffix retains its existing repair meaning.
- **Whole-store non-mutation:** before a future startup read-ceiling refusal, create two streams so one has a recoverable incomplete tail and a later stream has a complete over-policy record. Assert every original file's bytes and length are unchanged if startup refuses.
- **Mixed history:** cover RNL1/RNL2/RNL3 around RNL1 policy boundaries and contiguous offsets. Verify RNL2/RNL3 retain their own validation and that RNL1 limits are not inferred from their constants.
- **Valid key data:** test empty key, multi-byte UTF-8 key at the byte boundary, valid payload at the chosen boundary, invalid complete UTF-8 key, and a boundary split across buffer chunks if recovery becomes streaming.
- **Read paths:** when a future policy refuses materialization, exercise cold sparse-index replay, tail-cache poll, existing in-flight redelivery, ordinary poll, and replay. A refusal must not return `Empty`, mutate acknowledged progress, persist a new delivery attempt, or lose the candidate.
- **Dead-letter path:** a future resource refusal must not append/acknowledge the source; an oversized historical source that cannot fit the target format must retain source progress and bytes.
- **Export/conversion:** snapshot original source bytes; export or convert supported history; compare offsets, timestamps, key bytes, payload bytes, order, and request identity where present. Verify an unrepresentable record stops conversion without replacing or modifying source, and document target cleanup/resume semantics.
- **Concurrency evidence:** measure recovery peak RSS/time, retained index bytes, delivery/replay/response peaks, and concurrent reads across streams under the actual supported request and container limits. Include large legacy records and mixed formats. Correctness fixtures establish boundaries but not operational budgets.

## Evidence and implementation gates after ADR 0039

The absent production record-size corpus is not a blocker to the selected **new-write** admission rule: its values match limits already enforced for RNL2/RNL3 and the change does not narrow reads. It remains a blocker to any numeric legacy read ceiling. Synthetic data, newer-format constants, default request frames, or a small sample of development logs cannot establish the maximum valid historical record. The inventory must be run against representative stores under an approved handling process, and its report must say what it does not cover.

The runtime implementation of ADR 0039 must establish:

- exact 128-byte key and 64 MiB payload checks at local RNL1 storage append, before the first frame byte;
- a domain-level rejected result for scalar and per-item batch publish, with no offset, index, or durable state change for rejected items;
- a compatibility fixture proving complete historical RNL1 records above the new write limit remain readable;
- unchanged incomplete-tail repair and malformed-complete-record refusal behavior.

The remaining TD-028 work needs an explicit aggregate resource policy: recovery transient bytes, retained-index bytes, per-operation materialization and response bytes, and concurrent in-flight materialization. A future read ceiling additionally needs representative corpus/workload evidence, an explicit visible refusal outcome, a bounded-memory read-only inspector/export path that preserves all logical fields for records not representable in RNL2/RNL3, and a whole-store preflight that prevents recovery mutation before refusal. Until that separate decision and route exist, retain the historical read path and do not infer its envelope from the new-write rule.

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

The new-write outcome now has a focused backlog item; TD-028 remains open for the unbounded historical read/materialization paths. No runtime benchmark is claimed: the selected ceilings are compatibility limits copied from existing formats, not a demonstrated whole-process bound. The writer implementation and its correctness tests belong with ADR 0039; the later aggregate memory/read policy requires representative resource-scoped evidence. Documentation links, anchors, and `git diff --check` apply to this record update.
