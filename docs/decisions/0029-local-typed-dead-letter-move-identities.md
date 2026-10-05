# ADR 0029: Separate local public and dead-letter move identities

- Status: proposed
- Date: 2026-10-05
- Related: [ADR 0014](0014-local-retry-and-dead-letter-policy.md), [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records), and [TD-029](../tech-debt.md#td-029-public-request-ids-can-collide-with-local-dead-letter-move-ids)
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

Advance the request-aware `RNL3` frame version to version 2. Keep its current
header layout and use the flags byte to identify the variant: zero for a public
request ID and one for a local dead-letter move. Reject unknown versions or
flag values as malformed records. The existing checksum covers the version
and flags. Version 1 continues to require zero flags and is read as a public
request identity. RNL1 and RNL2 records remain unchanged; new code reads mixed
RNL1, RNL2, and RNL3 version-1/version-2 histories.

Public publishes look up only `PublicRequestId`. Reusing a public ID continues
to return its first public record's offset, including when the retry supplies
a different key or payload. A public ID whose text equals a move ID does not
match an internal move record. Move reconciliation looks up only
`DeadLetterMove`, and it still requires the stored key and payload to match
before source progress advances. A public target record with the same text,
whether its content matches or differs, remains a separate public record; the
broker appends the internal move and advances source progress only after that
append is durable.

Treat every existing RNL3 version-1 ID as public during recovery, including
historical internal move records. The old frame has no trustworthy identity
kind, and neither its prefix nor content can establish provenance. If an old
move's target append is present but its source acknowledgement is not durable
at upgrade, a retry may append one new typed move before advancing source
progress. This possible duplicate at the upgrade boundary is accepted to
preserve at-least-once progress without allowing an ambiguous public record to
stand in for the move. Once a typed move exists, repeated retry/reopen
reconciles that move and does not append another record.

This is a forward-read storage change, not a public wire change. The current
version-1 reader rejects every other RNL3 frame version as invalid data; an
older binary therefore fails closed on a version-2 frame. Downgrade after the
first version-2 append is unsupported. No conversion of old frames is required
or attempted: new code reads the existing RNL1/RNL2/RNL3 version-1 history and
the new version-2 records. This is a deliberate one-way boundary for the
current provisional storage format. ADR 0001 already leaves long-lived format
compatibility undefined, and TD-007 tracks the broader storage compatibility
policy; this local identity fix does not claim a general upgrade or rollback
path. This decision does not alter the clustered engine, whose current derived
record and source progress remain in one replicated data-group transition
under ADR 0016.

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
- A public retry still resolves to the first public record after restart and
  preserves the existing mismatched-content replay behavior.
- One extra dead-letter record may be appended for an interrupted move from
  pre-upgrade RNL3 data; new typed retries remain duplicate-safe under the
  existing append-then-checkpoint ordering.
- The two local identity buckets may contain the same text; combined index
  cardinality remains unbounded and retention is not defined by this change.
- Stream name, consumer name, and offset remain the move key while the current
  stream lifecycle has no delete/recreate operation. A future stream
  incarnation feature must revisit that key.
- The clustered path and public wire schema are unchanged.

## Evidence required

Core and real-server tests must cover both same-content and mismatching-content
public collisions; separate public and internal target offsets and contents;
public request replay after restart; source acknowledgement failure followed
by reopen/retry without a second typed move; and legacy RNL3 version-1 recovery
including the permitted upgrade-boundary duplicate. A completed version-1
public record with the predictable move-ID text remains the public replay
result and cannot satisfy a new move. An interrupted version-1 move is treated
the same way, so retry appends a new typed move once and then advances source
progress. Parser tests cover valid mixed versions, unsupported versions or
identity flags failing closed, and incomplete tails. The baseline reader's
version-1-only guard establishes that it rejects a newly written version-2
frame; the test suite separately verifies that the new reader does not accept
unknown versions or flags. The existing target-write, source-event, and
restart tests for at-least-once ordering remain required. No clustered runtime
change is part of this evidence claim.
