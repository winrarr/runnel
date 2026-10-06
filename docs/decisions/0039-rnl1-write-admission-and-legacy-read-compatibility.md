# ADR 0039: Bound new RNL1 writes and preserve complete legacy reads

- Status: accepted; local write admission implementation in progress
- Date: 2026-10-06
- Baseline: `f6bc65cbe19a5902616aeeaa3ce46dedae583d2a`
- Primary evidence class: design/research; secondary: storage safety, public outcome
- Related: [TD-028](../tech-debt.md#td-028-rnl1-materialization-lacks-an-operational-allocation-budget), [RNL1 resource-policy study](../design/td-028-rnl1-resource-policy.md), [allocation-policy evidence](../research/td-028-rnl1-allocation-policy.md), [legacy storage compatibility evidence](../design/td-007-storage-compatibility-evidence.md#evidence-matrix), [bounded legacy materialization backlog](../backlog.md#bound-legacy-record-materialization-without-losing-old-data), and [ADR 0037](0037-offline-side-by-side-storage-upgrades.md)

## Context

The default local writer uses RNL1 for publishes without a request ID. RNL1 stores independent `u32` key and payload lengths; before ADR 0039, its writer accepted values up to the representable field size. Recovery verifies that a frame is complete before allocating its key, but it materializes each complete key; poll, replay, and dead-letter movement later materialize the whole payload. An entry-count-bounded tail index does not bound key bytes, and server response serialization adds another full response buffer.

RNL2 and RNL3 already cap keys at 128 bytes and payloads at 64 MiB. RNL3 adds a request-ID field and limit. These are concrete Runnel format behaviors, but they are not measurements of a safe aggregate memory budget or evidence that all persisted RNL1 records fit those limits. The read-only [RNL1 size audit](../design/td-028-rnl1-resource-policy.md#read-only-size-audit) can describe inspected stores; the repository has no representative production corpus from which to infer the largest deployed legacy record.

The compatibility question has two independent parts: the size of records Runnel will create now, and the treatment of complete records already persisted under RNL1's wider envelope. One limit should not silently answer both questions.

## Alternatives considered

| Alternative | Resource and compatibility effect | Decision |
| --- | --- | --- |
| Keep the full RNL1 envelope for all new writes and reads | Preserves the existing writer's accepted input range, but allows new oversized records and retains unbounded per-record and aggregate materialization. Fallible allocation alone does not impose a byte budget. | Rejected for new writes; retained as the current complete-record read eligibility rule until a safe legacy read policy exists. |
| Apply the RNL2/RNL3 key and payload limits to every complete RNL1 read | Bounds those two stored fields before allocation, but can make a previously complete RNL1 record fail startup or become unavailable at delivery. `BrokerState::open` opens logs sequentially and can truncate an incomplete suffix in an earlier stream before a later log fails. A read ceiling therefore also needs whole-store preflight and an outlier retrieval route. | Rejected as a retroactive read rule. No corpus establishes its compatibility, and current RNL2/RNL3 cannot represent every RNL1 record. |
| Bound only new RNL1 appends and keep the existing complete-record read path | Stops newly persisted RNL1 records from exceeding the same per-field limits already used by the newer formats, while preserving existing complete bytes and offsets for recovery. It does not claim that legacy reads or aggregate memory are bounded. | Accepted as the first behavior. |
| Replace complete message materialization with streaming or a new record format immediately | Could address more read and response copies, but crosses engine, storage, protocol, client, dead-letter, and failure-outcome boundaries and still requires aggregate concurrency budgets. It is broader than the first safe policy slice. | Deferred to the bounded legacy materialization outcome in the backlog. |

Kafka provides a relevant policy distinction: accepted record-batch size and consumer fetch targets are separate, and a fetch target can be exceeded for an initial batch to make progress. This supports separating write admission from read/fetch behavior, but Kafka's batch framing does not determine Runnel's legacy compatibility choice. Rust documents `Vec::try_reserve` as reporting capacity overflow or allocator-reported failure; fallible reservation does not itself define a size ceiling or bound aggregate allocations. See [Kafka broker configuration](https://kafka.apache.org/42/configuration/broker-configs/#brokerconfigs_message.max.bytes), [Kafka consumer configuration](https://kafka.apache.org/42/configuration/consumer-configs/#consumerconfigs_fetch.max.bytes), and [Rust `Vec::try_reserve`](https://doc.rust-lang.org/stable/std/vec/struct.Vec.html#method.try_reserve).

## Decision

### New local RNL1 writes

The local RNL1 writer accepts keys of at most 128 UTF-8 bytes and payloads of at most 64 MiB. These limits match the existing RNL2/RNL3 per-field limits. They are byte limits; multibyte UTF-8 keys are measured after encoding. The complete encoded RNL1 frame can be 28 bytes larger than the payload plus the key.

The writer validates both lengths before writing any header or field bytes. Oversized input has a domain-level rejected result (`InvalidRequest` / `Rejected`), exposed through the provisional v1 server as `invalid_record`. It is a confirmed no-effect result: no record bytes, logical offset advance, in-memory record/index entry, consumer-state change, or delivery notification is produced. Publish batches retain their existing independent per-record semantics: an oversized item is rejected at its position, and later valid items may append and receive the next available offset. The same validation occurs in storage so direct core callers cannot bypass it.

This acceptance narrows future no-request-ID publish input. It does not change request-aware RNL3 or explicit RNL2 behavior, wire frame limits, or previously persisted bytes. The values are selected because they already define current bounded durable write formats, not because a corpus or benchmark proves an end-to-end memory budget.

### Complete historical RNL1 records

Do not apply the 128-byte or 64 MiB write limits to complete RNL1 frames during recovery, replay, poll, redelivery, or dead-letter source reads. Keep the current distinction between an incomplete trailing frame and a complete malformed or valid frame. A complete record above the new-write limits must not be truncated, skipped, rewritten, compacted, acknowledged, or automatically moved to a dead-letter stream solely because it exceeds those write values.

This is a compatibility eligibility rule, not an unconditional guarantee that an arbitrary record can be materialized on every machine or returned through every configured client. Current payload allocation, cached keys, response serialization, request/response limits, and dead-letter target limits remain distinct resource and delivery constraints. In particular, a client response limit may be exceeded after a delivery attempt has been persisted. This ADR does not claim those paths provide a clear, bounded refusal today.

No lower complete-record read ceiling is selected. Before a future decision selects one, the implementation must provide a bounded-memory, read-only way to inspect and export complete outliers while preserving stream identity, logical offset, timestamp, key bytes, and payload bytes. The export path must handle records that do not fit RNL2/RNL3; conversion to either format alone is not an escape route. If ordinary startup will refuse an over-limit record, a whole-store read-only preflight must discover that condition before recovery truncates any incomplete suffix in any stream. The refusal must identify the stream, offset, declared field sizes, and configured limit, preserve all source bytes and consumer state, and remain distinct from corruption and incomplete-tail recovery.

## Consequences and implementation gates

- New RNL1 input above the selected write limits is rejected even though the old writer could encode it. The new error result makes the no-effect outcome explicit for scalar and batch publish.
- Complete historical RNL1 records beyond those values remain subject to today's read/materialization behavior. This avoids a corpus-blind read break but leaves the RNL1 resource debt partially open.
- The read-only size audit can inform a future read and memory policy, but observed maxima do not prove that unseen stores fit or establish safe concurrent RSS.
- Runtime verification must cover accepted values at the boundary, rejection immediately above each boundary before any durable byte or index mutation, scalar and batch outcomes, valid later writes after rejection, and reopening/replaying a complete historical RNL1 record whose field exceeds the write limit. Existing incomplete-tail and malformed-complete-record tests must remain valid.
- A later aggregate resource policy must separately account for recovery scratch memory, retained key/index bytes, per-operation payload materialization, response serialization, and concurrent operations. Measure claims under explicit process/container resources; do not infer them from these per-field limits.
- Any future lower read ceiling requires its own ADR, representative corpus/workload evidence, the non-destructive export/preflight behavior above, and crash/recovery tests proving no acknowledgement or source mutation on refusal.

The [bounded legacy record materialization backlog outcome](../backlog.md#bound-legacy-record-materialization-without-losing-old-data) owns that remaining work. ADR 0039 selects no RNL1 read ceiling, streaming public message contract, migration format, or process-wide memory promise.
