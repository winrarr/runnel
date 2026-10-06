# ADR 0039: Use one canonical local stream-log format

- Status: accepted; supersedes the earlier RNL1 write-admission and legacy-read decision
- Date: 2026-10-06
- Related: [TD-028](../tech-debt.md#td-028-aggregate-local-record-materialization-lacks-a-memory-budget), [local materialization study](../design/td-028-rnl1-resource-policy.md), [historical allocation evidence](../research/td-028-rnl1-allocation-policy.md), and [TD-007 storage evidence](../design/td-007-storage-compatibility-evidence.md#evidence-matrix)

## Context

The earlier ADR 0039 capped new `RNL1` writes while preserving reads of complete `RNL1`/`RNL2` history. That split avoided making a corpus-blind decision about persisted data and led to proposals for a legacy audit/export path, format selection, and separate read policy. Runnel has no deployed users or backward-compatibility requirement. Carrying three readers and multiple writers would preserve obsolete state at the cost of more parsing, recovery, test, and operational paths, without serving a current deployment.

The existing request-aware `RNL3` frame already carries offsets, timestamps, keys, payloads, CRC32C, and typed identities with bounded field sizes. Extending that frame to ordinary records provides one local format without changing message or retry semantics.

## Decision

Use `RNL3` version 2 for every local stream record. The 48-byte header and checksum cover the record metadata, key, identity bytes, and payload. The identity flag is `0` for a public request ID, `1` for a dead-letter move identity, and `2` for a record with no identity. Keys are limited to 128 bytes, payloads to 64 MiB, and identity strings to 1 KiB. Ordinary publishes with no request ID store no identity bytes.

Recovery accepts only this frame version. It rejects `RNL1`, `RNL2`, `RNL3` version 1, unknown versions or flags, malformed complete frames, offset gaps, and checksum failures with explicit errors. Startup first scans every stream without mutation, then repairs incomplete trailing frames only after the full store passes validation. Thus unsupported old artifacts fail before any stream tail is truncated.

The existing durability and messaging behavior remains: successful appends cross the current `sync_data` boundary before confirmation; an incomplete final frame is discarded during recovery; public request IDs still return the original offset for equivalent retries and reject changed content; local dead-letter moves keep typed identity and append-before-source-ack ordering. No RNL1/RNL2/RNL3-v1 reader, writer selector, conversion utility, or historical export route is maintained.

## Alternatives

- Keep the legacy readers and selectors: rejected because there is no compatibility obligation, and each supported format creates another recovery and verification path.
- Write ordinary records as `RNL2`: rejected because it would retain multiple frame families and split ordinary records from request-aware records without a current need.
- Use `RNL3` version 2 for all records: accepted because it already provides checksums, explicit versioning, bounded fields, and typed identity; the no-identity flag handles ordinary records directly.

## Consequences

- A pre-existing `RNL1`, `RNL2`, or `RNL3` version-1 stream is refused and left byte-for-byte unchanged. Operators must start with a current-format store; this decision adds no migration or export path.
- Crash-tail recovery, checksums, contiguous offsets, bounded parsing, at-least-once acknowledgement ordering, and request-ID deduplication remain covered by focused recovery tests.
- Per-record bounds do not establish an aggregate process-memory budget. Retained indexes, distinct request identities, response copies, and concurrent reads remain tracked in [TD-028](../tech-debt.md#td-028-aggregate-local-record-materialization-lacks-a-memory-budget).
- This decision governs the local message log. It does not set a Raft log format, application protocol, or cluster-migration contract.
