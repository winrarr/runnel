# ADR 0029: Separate local public and dead-letter move identities

- Status: accepted
- Date: 2026-10-05
- Related: [ADR 0014](0014-local-retry-and-dead-letter-policy.md), [ADR 0034](0034-publish-request-id-content-contract.md), [TD-002](../tech-debt.md#td-002-one-file-and-a-startup-scan-per-local-stream), and [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records)
- Research: [Public request IDs and dead-letter move identity](../research/td-029-dead-letter-identity-contract.md)
- Extends: ADR 0014's local identity and reconciliation details; the append-then-ack order remains unchanged

## Context

The local engine currently persists caller-supplied publish IDs and generated
dead-letter move IDs as the same string identity. A strict key/payload check
prevents a conflicting target record from advancing source progress, but a
public record with matching content can impersonate an internal move. A
mismatching public record can also keep the source delivery blocked on every
retry. The local target append and source acknowledgement remain separate
durable writes, so the move identity must survive restart and let a retry find
only the target record for that move.

## Decision

For new local request-aware records, persist the identity kind as part of the
durable record identity. Public replay and internal move reconciliation use
separate buckets in one per-stream identity index, each keyed by the existing
string value. The move string remains the stable, length-prefixed source
stream, consumer, and source offset value used by the current local
implementation. Keeping one offset value per identity avoids increasing the
per-ID value size; the second map has a fixed per-stream header plus separate
capacity slack, and a cross-kind collision stores the text in both buckets.

Use the RNL3 version-2 flags byte to identify the record variant: zero for
a public request ID, one for a local dead-letter move, and two for an ordinary
record with no identity. The checksum covers the version, flags, metadata,
key, identity bytes, and payload. Reject unknown versions or flag values.
RNL3 version 1 and the former RNL1/RNL2 formats are unsupported under
[ADR 0039](0039-rnl1-write-admission-and-legacy-read-compatibility.md).

Public publishes look up only `PublicRequestId`. The typed lookup separation
does not determine content-equivalence behavior for a reused public ID. ADR
0034 supersedes this ADR's former mismatch-specific rule: its accepted contract
returns the original offset for a content-equivalent retry and confirms
rejection without appending for a difference in representable key bytes or
payload bytes. Request-ID comparison treats absent and empty keys as
equivalent because the current local durable representation cannot distinguish
them; this does not equate their ordering intent. ADR 0034 defines the full
comparison rule, which the local and clustered runtime now enforce. Shared
engine and real-server verification is recorded in the related request-ID
research. A public ID whose text equals a move ID does not
match an internal move record. Move reconciliation looks up only
`DeadLetterMove`, and it still requires the stored key and payload to match
before source progress advances. A public target record with the same text,
whether its content matches or differs, remains a separate public record; the
broker appends the internal move and advances source progress only after that
append is durable.

Recovery indexes current public and move identities in separate buckets;
ordinary records have no identity entry. The current reader accepts only
RNL3 version 2, so it never has to infer the type of an older ambiguous ID.
Within the current format, a durable move append can be reconciled after
restart before source progress advances, preserving the append-then-ack
ordering.

The typed identity variants are part of the canonical local RNL3 version-2
format selected by ADR 0039. Older local frame families are refused, and no
downgrade or historical conversion path is promised. This decision does not
alter the clustered engine, whose current derived record and source progress
remain in one replicated data-group transition under ADR 0016.

## Rationale and alternatives

A durable type discriminator makes equality of caller input distinct from
identity of broker-owned work. Apache Kafka's KIP-98 provides a reference for
persisted producer identity and separate control messages; its session-scoped
producer identity and transaction protocol are not Runnel's caller-supplied,
restart-replayable request ID or local two-file transaction. NATS JetStream
uses a separate message header for caller-defined deduplication IDs and
reserves its protocol header namespace; its configured duplicate window is
shorter than Runnel's current retained-record replay horizon. RabbitMQ's
at-least-once dead-letter path retains source work until a target confirmation
and documents possible target duplicates on retry; its quorum and publisher
confirm boundaries differ from Runnel's local log/checkpoint pair. These
references support keeping operation identity explicit and source progress
behind confirmed target durability, but do not specify Runnel's format or
guarantee.

Rejecting a reserved textual prefix would restrict public IDs, rely on every
public write path applying the convention, and still leave old frames
ambiguous. Comparing content cannot prove origin. A transaction or recovery
journal could improve the two-write failure boundary tracked by TD-017, but it
would not separate public and internal identity unless the target lookup were
also typed. Two namespace buckets keep each retained identity's value at one
offset, which avoids the larger per-ID value of a single string-keyed map
containing two optional offsets. This costs a fixed second map header per
stream and duplicates the key string only when the same text exists in both
namespaces. Separate capacities may also leave more unused buckets than a
shared map. The combined entry count remains unbounded under current retention
and stays covered by TD-002. This is a memory-accounting choice, not a latency
claim.

## Consequences

- Equal public and internal ID text can coexist in one target stream without
  impersonation or a source-blocking content conflict.
- Public and internal move retries remain in separate identity buckets. Public
  content-equivalence behavior is governed by ADR 0034, and both local and
  clustered runtime paths apply its exact-retry and changed-content rejection
  contract.
- The two local identity buckets may contain the same text; combined index
  cardinality remains unbounded and retention is not defined by this change.
- Stream name, consumer name, and offset remain the move key while the current
  stream lifecycle has no delete/recreate operation. A future stream
  incarnation feature must revisit that key.
- The clustered path and public wire schema are unchanged.

## Verification evidence

Core and real-server tests cover same-content and mismatching public
collisions; separate public and internal target offsets and contents; public
request replay after restart; and source acknowledgement failure followed by
reopen/retry without a second typed move. The earlier core tests that asserted
first-use-wins exercise ADR 0034's exact retry and conflict behavior; shared
engine and clustered real-process coverage is recorded in the linked
request-ID research. Parser tests cover ordinary records, both typed identity
flags, unsupported versions or flags failing closed, checksum validation, and
incomplete tails. The target-write, source-event, and restart tests support
at-least-once ordering. ADR 0039 owns the current local frame-version policy.
ADR 0034 owns cross-engine public request-ID mismatch semantics. The current
local and clustered runtime behavior and focused verification are recorded in
the linked request-ID research; this ADR's evidence remains specific to local
typed identity storage and move recovery.
