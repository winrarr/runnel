# TD-028: RNL1 allocation policy evidence

- Status: exploratory evidence; no RNL1 size policy or implementation is accepted
- Last reviewed: 2026-10-02
- Baseline: `a42fbfd207b1a26f4b52fce8110311453d55a009`
- Primary evidence class: storage safety
- Scope: allocation during local `RNL1` recovery, indexing, delivery, replay, and dead-letter movement
- Related debt: [TD-028](../tech-debt.md#td-028-rnl1-materialization-lacks-an-operational-allocation-budget)
- Related evidence: [TD-007 storage compatibility](../design/td-007-storage-compatibility-evidence.md#evidence-matrix), [TD-002 storage scalability](../design/td-002-storage-scalability-evidence.md), and [message encoding and compression](message-encoding-and-compression.md)
- Related proposal: [safe durable storage upgrades](../design/storage-upgrade-safety-plan.md)

This note describes current source behavior, compares practical policy choices, and identifies the evidence needed before implementation. It does not authorize a size limit, change recovery, or make a release compatibility promise.

## Disposition

Do not enforce a smaller `RNL1` bound yet. The source confirms that a complete legacy record can trigger large allocations, but the repository has no production-log corpus, no valid near-limit fixtures, and no evidence that the `RNL2`/`RNL3` bounds cover deployed history. Applying those bounds retroactively could make complete, valid historical bytes fail startup or delivery. The near-term work justified by current evidence is to collect corpus size statistics and choose an explicit read, write, and conversion policy; it is not to select a numeric limit from the newer formats by analogy.

No tracker change is warranted. TD-028 already records the unaccepted policy, the missing production-corpus evidence, the compatibility constraint, and the retirement evidence required. This investigation does not change that scope or identify a separate untracked implementation shortcut.

## Current behavior established by source and tests

`RNL1` has a 28-byte header: magic, little-endian `u64` offset and timestamp, then independent little-endian `u32` key and payload byte lengths. The writer rejects lengths that do not fit `u32`, but defines no smaller key or payload limit. Its encoded length field therefore permits up to `u32::MAX` bytes for each field, or `28 + 2 × u32::MAX` bytes for one record. That arithmetic describes the old format's encoded envelope, not a guarantee that any process can allocate, address, or use such a record. See `StreamLog::append_with_sync` and the constants in [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs).

Recovery now scans the log incrementally rather than loading the entire file. For `RNL1`, it checks length arithmetic and compares the complete record length with the remaining file bytes before allocating the key. For a complete record it then allocates the full key bytes, validates UTF-8, creates an owned `String`, and seeks over the payload without allocating that payload. `StreamLog::open` retains up to 1,024 recent `RecordIndex` entries, each of which owns its key. So recovery has per-record key materialization and a bounded entry count, but neither a per-key byte bound nor a total cached-key byte budget. A complete record's payload is allocated later by `read_payload` for poll, replay, and dead-letter movement. `read_legacy_record`, `StreamLog::open`, `read_payload`, and `Broker::dead_letter_record` show these paths.

The incomplete-tail behavior is separate from complete-record validation. If the header is incomplete, or its declared record extends past the current file length, the parser returns `None`; `StreamLog::open` then truncates the file to the last complete cursor. A complete malformed UTF-8 key returns an error and is not truncated. [`incomplete_trailing_frame_is_discarded_on_recovery`](../../crates/runnel-core/src/lib.rs#L1891) covers a short `RNL1` suffix; [`complete_legacy_record_with_malformed_key_fails_closed_on_recovery`](../../crates/runnel-core/src/lib.rs#L1917) covers one complete invalid record and confirms its bytes remain. Neither test uses a huge declared length or places a valid large key/payload at a boundary. See `StreamLog::open` and `read_legacy_record`.

The newer format limits are not an accepted RNL1 policy. `RNL2` uses 128-byte keys and 64 MiB payloads. `RNL3` uses those bounds plus a 1 KiB request ID; the RNL3 limits selected under the default durable format are still RNL3 limits, not `RNL1` limits. Non-request-aware appends under the default format continue to write RNL1, while request-aware appends use RNL3. The server's request-frame default is 1 MiB and configurable up to 64 MiB encoded; this limits new wire requests only. Direct core publishes and bytes already in the log are outside that frame bound. See the [`stream_log.rs` limits and writers](../../crates/runnel-core/src/stream_log.rs), [`runnel-server` admission](../../crates/runnel-server/src/protocol.rs), and [protocol size constants](../../crates/runnel-protocol/src/lib.rs).

Delivery also has materialization beyond the local file read: core `Message` and replay payloads are owned `Vec<u8>` values; the server serializes a full response into another vector; and the reusable client has a configurable encoded-response limit, defaulting to about 65 MiB. The server does not apply that client response limit before serialization. A sufficiently large legacy record can therefore be readable through an in-process path but exceed a client's response limit. If the first poll read succeeds, the delivery attempt is persisted before the message is returned; an oversized response can leave the client with an unknown operation outcome and the delivery to expire for redelivery. A configured dead-letter attempt limit adds another constraint: automatic dead-letter movement reads the complete source payload and appends a request-aware RNL3 target record, whose payload ceiling is 64 MiB and key ceiling is 128 bytes. [`Broker::poll_group`](../../crates/runnel-core/src/broker.rs#L309), [`send_response`](../../crates/runnel-server/src/protocol.rs#L146), [`ClientConfig::max_response_bytes`](../../crates/runnel-client/src/lib.rs#L35), and [`Broker::dead_letter_record`](../../crates/runnel-core/src/broker.rs#L592) make these distinct limits visible. They are not evidence that all historical RNL1 records are network-deliverable or dead-letterable today.

No ADR accepts a retroactive RNL1 allocation ceiling. Existing RNL2/RNL3 constants define their own on-disk validation and append behavior; the storage upgrade documents are proposals, not accepted policy. The task-specific gap also remains explicit in TD-028 and the [TD-007 evidence matrix](../design/td-007-storage-compatibility-evidence.md#evidence-matrix).

## External reference evidence

Rust's stable [Vec::try_reserve contract](https://doc.rust-lang.org/stable/std/vec/struct.Vec.html#method.try_reserve) reports capacity overflow or an allocation failure reported by the allocator. This is a useful way to turn some oversized or failed reservations into a returned error, but it does not define an application byte budget, and it does not make a large valid record small. Rust's documented default [handle_alloc_error behavior](https://doc.rust-lang.org/stable/std/alloc/fn.handle_alloc_error.html) for a binary linked with `std` is to print a message and abort, with a configurable hook; allocator and operating-system behavior still matter. Fallible reservation is therefore a failure-handling mechanism, not a substitute for an accepted resource envelope or evidence that allocation failure is always recoverable.

Apache Kafka documents a distinct accepted record-batch bound through `message.max.bytes`/topic `max.message.bytes`, alongside consumer fetch budgets. Its fetch documentation says an initial batch larger than the per-partition or whole-fetch target can still be returned so a consumer can make progress. This is a useful reference for separating write admission from delivery/fetch behavior: a fetch target alone is not necessarily a hard per-record ceiling. It does not prescribe Runnel's legacy migration policy, and Kafka's record format and protocol are not equivalent to RNL1. See Kafka's [broker configuration](https://kafka.apache.org/42/configuration/broker-configs/#brokerconfigs_message.max.bytes) and [consumer configuration](https://kafka.apache.org/42/configuration/consumer-configs/#consumerconfigs_fetch.max.bytes).

## Feasible approaches

| Approach | What it addresses | Compatibility and operational consequence |
| --- | --- | --- |
| Keep the historical encoded envelope and use fallible allocation where supported | Avoids imposing an arbitrary smaller format limit; can report some capacity/allocation failures rather than relying on infallible allocation paths | Does not bound memory. The format permits multi-gigabyte fields, retained key strings multiply across the recent-record cache, and multiple delivery operations may materialize payloads concurrently. It is not sufficient retirement evidence for TD-028. |
| Apply the `RNL2`/`RNL3` bounds to every `RNL1` record | Gives a simple known key/payload ceiling and consistent limits across formats | Rejects complete RNL1 bytes that the old writer could produce. No corpus establishes that all existing histories fit, and there is no large-record conversion or alternate read path. This is not safe to implement without an explicit compatibility decision. |
| Define separate legacy read and write ceilings plus an aggregate in-flight byte budget | Makes legacy compatibility and operational working-set limits explicit; can keep future writes within the selected policy | Any finite legacy read ceiling below the historical envelope rejects some previously valid record. A per-record ceiling alone does not bound the 1,024-key cache, concurrent polls, response serialization, or dead-letter copies. Requires measured values and explicit behavior for over-limit history. |
| Stream recovery metadata and payload output | Can avoid allocating complete keys during startup scans and can avoid full payload materialization in a future chunked read/export path | Current `RecordIndex` and engine message models own `String` keys and `Vec<u8>` payloads; server JSON responses also serialize a whole response. Preserving unbounded record delivery requires a broader API/protocol change. Streaming the scanner alone reduces startup allocation but does not retire delivery risk. |
| Copy-convert supported history to a bounded checksummed format | Can keep source bytes authoritative while building and validating a new generation; source can remain untouched if a record cannot be represented | RNL2/RNL3 currently top out at 128-byte keys and 64 MiB payloads. An oversized source record must stop conversion with its offset and sizes, remain retrievable through a documented legacy path, or wait for a format/read path that can represent it. Truncation, omission, or splitting into multiple logical messages would change data semantics. |

The plausible long-term policy is layered: explicitly bound new writes, decide the supported legacy read envelope from evidence, and separately bound total materialized bytes in recovery/indexing and concurrent delivery. Whether to retain unrestricted legacy reading, provide a compatibility mode, or require a non-destructive conversion for out-of-envelope records is unresolved. This is a research recommendation, not an accepted design.

## Evidence and decision required before implementation

The next policy decision should not be made from format constants alone. It needs all of the following evidence and explicit choices:

1. **Corpus inventory.** Scan representative real RNL1 logs without opening
   them through the normal broker path. Record format counts, number of logs
   and records, key and payload byte-length distributions, maximum complete
   record size, malformed/incomplete tails, and storage-engine/runtime
   provenance. Store no message contents or keys unless separately approved.
   State which production stores were inventoried and which were not. A sample
   maximum cannot prove unseen stores have no larger valid record.
2. **Operational target.** Choose separate maximum key bytes, payload bytes,
   total encoded record bytes, recovery transient bytes, resident index bytes,
   and concurrent materialization bytes, or explain why some are intentionally
   unbounded. Measure peak RSS, recovery time, and poll/replay memory under the
   intended host/container limit and supported request concurrency. Do not
   infer a whole-process budget from a single-record limit.
3. **Compatibility decision.** Decide whether limits constrain only new
   RNL1 writes, all complete RNL1 reads, or delivery/export only. Define
   behavior for already persisted complete records outside each limit and the
   versioned format that new writes use. If a lower read cap is selected,
   require a documented non-destructive route before making those records
   inaccessible through ordinary startup or delivery.
4. **Conversion/read route.** Define a read-only inventory or export mode
   that does not mutate the source. Normal `StreamLog::open` is not read-only:
   it opens for append and truncates an incomplete suffix. A converter should
   build a side-by-side target, preserve source key/payload bytes, offset, and
   timestamp, verify logical equality, retain the source until target
   validation and activation succeed, and stop without mutation when a record
   is not representable. Preserve RNL1's lack of checksum as an explicit source
   limitation; a checksum generated during conversion only covers the target
   bytes written by the converter.
5. **Failure outcome.** Distinguish “complete record exceeds configured
   resource policy” from corruption and from an incomplete crash tail. Report
   stream/offset and declared lengths without dumping content. Allocation
   failure must not silently look like a partial frame or mutate the source.
   A poll/replay refusal must not acknowledge or skip the record; test how an
   already in-flight delivery behaves if a later materialization fails.
6. **Boundary and crash tests.** Cover keys and payloads at, below, and above
   the selected limits; complete over-limit frames; every meaningful
   incomplete-header/key/payload suffix; mixed RNL1/RNL2/RNL3 histories; valid
   UTF-8 boundary cases; and retry/reopen after refusal. Assert source bytes and
   length remain unchanged for complete over-limit or malformed records,
   while only the existing incomplete final suffix is truncated. Cover poll,
   replay, and configured dead-letter movement. Inject deterministic
   reservation/reader failures where possible; do not rely on exhausting the
   test host's memory to prove graceful allocation handling.

An implementation that changes current recovery or data accessibility needs focused crash/recovery coverage and an accepted ADR describing the selected compatibility consequence. Any claimed performance or memory improvement also needs measured, resource-scoped evidence for startup and delivery; synthetic boundary tests alone establish correctness, not an operational allocation budget.

## Sources

### Repository sources

- [`stream_log.rs` format constants, writers, recovery, and payload reads](../../crates/runnel-core/src/stream_log.rs)
- [`broker.rs` publish, poll, replay, and dead-letter behavior](../../crates/runnel-core/src/broker.rs)
- [`runnel-server` request admission and response serialization](../../crates/runnel-server/src/protocol.rs)
- [`runnel-protocol` request/response byte limits](../../crates/runnel-protocol/src/lib.rs)
- [`runnel-client` response limit and unknown-outcome behavior](../../crates/runnel-client/src/lib.rs)
- [RNL1 incomplete-tail and complete-malformed-key tests](../../crates/runnel-core/src/lib.rs)
- [TD-007 local-stream compatibility evidence](../design/td-007-storage-compatibility-evidence.md#evidence-matrix)

### Primary external sources

- [Rust `Vec::try_reserve` documentation](https://doc.rust-lang.org/stable/std/vec/struct.Vec.html#method.try_reserve)
- [Rust `Vec::try_reserve_exact` documentation](https://doc.rust-lang.org/stable/std/vec/struct.Vec.html#method.try_reserve_exact)
- [Rust `handle_alloc_error` documentation](https://doc.rust-lang.org/stable/std/alloc/fn.handle_alloc_error.html)
- [Apache Kafka 4.2 broker configuration](https://kafka.apache.org/42/configuration/broker-configs/#brokerconfigs_message.max.bytes)
- [Apache Kafka 4.2 consumer configuration](https://kafka.apache.org/42/configuration/consumer-configs/#consumerconfigs_fetch.max.bytes)

The Rust and Kafka sources support only the mechanism descriptions above. The Runnel policy recommendation is an inference from those sources plus the repository code and its stated compatibility constraint.

## Refactor and planning-record assessment

This research changes no runtime code, so no runtime or end-to-end test applies. It adds a focused research record and an index entry. No concrete adjacent runtime refactor is safe to make before the allocation and compatibility policy is accepted; implementing one here would cross into storage behavior owned by `runnel-core`. No tracker update is needed because TD-028 already captures the goal, rationale, constraints, production-corpus gap, and retirement evidence.
