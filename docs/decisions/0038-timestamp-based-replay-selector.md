# ADR 0038: Define timestamp-based replay selection

- Status: selector semantics accepted; [ADR 0042](0042-recoverable-replay-time-index.md) accepts the index/recovery contract; runtime and protocol representation remain open
- Date: 2026-10-06
- Baseline: `5dc76270a46690fce074fcaf61b5a8cda9838cd0`
- Primary evidence class: design/research; secondary: correctness/reliability, storage/recovery
- Related outcome: [Make replay an explicit and safe consumer operation](../backlog.md#make-replay-an-explicit-and-safe-consumer-operation)
- Related decisions: [ADR 0024](0024-explicit-offset-replay-read.md), [ADR 0023](0023-independent-retained-storage-and-placement.md), [ADR 0031](0031-protocol-v2-contract.md), and [ADR 0042](0042-recoverable-replay-time-index.md)
- Design: [replay selectors and bounded sessions](../design/replay-sessions.md)
- Research: [replay time-selector semantics](../research/replay-time-selector-semantics.md)

## Context

ADR 0024 accepts a bounded, read-only replay of one inclusive logical offset.
The time selector in the replay backlog is still unimplemented. The existing
message field `published_at_ms` provides a timestamp source, but it is a
broker-assigned integer millisecond value sampled from process wall time. The
local engine samples it during append; the clustered engine samples it at the
requesting node before a follower may forward the publish. No caller event time
is accepted, timestamps are not clamped to logical order, and clock uncertainty
is not represented. Equal or decreasing timestamps at increasing offsets are
therefore possible.

The local log supports logical-offset lookup through a bounded recent index
and sparse offset checkpoints; a cold lookup may scan retained bytes. The
clustered state machine keeps retained messages in an offset-ordered vector.
Neither engine has a timestamp index. Retention is not implemented, so current
history starts at offset zero. A time selector needs deterministic behavior
under timestamp ties and regressions, and it must not silently report a
retained suffix as complete if a deleted prefix could contain an earlier
match.

[Kafka's `offsetsForTimes`](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
defines a lookup as the earliest offset whose record timestamp is greater
than or equal to a requested timestamp; Kafka separately distinguishes
producer create time from broker append time in its
[`message.timestamp.type`](https://kafka.apache.org/42/configuration/topic-configs/)
setting. [Pulsar Readers](https://pulsar.apache.org/api/client/4.2.x/org/apache/pulsar/client/api/Reader.html)
also accept a timestamp-based starting position, but their reader position
semantics do not define Runnel's independent read-only replay boundary.
[Spanner's TrueTime paper](https://research.google.com/archive/spanner-osdi2012.pdf)
illustrates why a scalar wall-clock value cannot establish physical-time
ordering or a clock-error bound. These primary references inform the selector
shape; their clocks, cursor behavior, and retention contracts do not transfer
to Runnel.

## Decision

A future one-shot time replay selector uses the existing stored
`published_at_ms` value and resolves to one logical record in a single
consistent stream view:

1. The input `T` is a non-negative unsigned Unix-epoch millisecond value.
   Compare it exactly at the stored millisecond precision. Reject values that
   cannot be represented by the protocol's selected integer type. The wire
   field name and encoding remain subject to the protocol contract.
2. In the captured retained view `[earliest, next)`, select the matching record
   with the **lowest logical offset** whose `published_at_ms >= T`. The
   comparison is inclusive. Equal timestamps therefore select the lowest
   matching offset.
3. This is a replay **start selector**, not a timestamp filter. A caller
   continuing replay reads later logical offsets in append order, even when a
   later record's timestamp is less than `T` because of clock regression.
4. Resolve against one local stream-lock view or one committed clustered
   stream-group view. A publish concurrent with selector resolution is either
   included in that view or not; the operation does not wait for future
   records.
5. If the complete captured view contains no match—including an empty stream
   or `T` later than every timestamp in that view—return an explicit
   `no_match` outcome. Do not report ordinary poll `Empty`, and do not keep the
   request open for later publishes.
6. The selector is read-only with respect to ordinary consumer state. It does
   not create a consumer checkpoint, move the committed offset, change
   out-of-order acknowledgements, create delivery attempts or leases, or
   produce an ordinary acknowledgement token. The consumer identity remains
   validated and scopes the replay request as in ADR 0024.
7. If retention has removed a contiguous logical prefix, time selection may
   return a result only when the broker can prove that no deleted record could
   precede the returned match. Maintain complete metadata for the maximum
   `published_at_ms` in the deleted prefix, `D`. When `D >= T`, return
   `history_unavailable`: at least one deleted record matched and the earliest
   matching offset is unknowable. When `D < T`, no deleted record matched and
   lookup may continue in retained history. If the summary is absent,
   incomplete, or corrupt, return `history_unavailable`. The same rule applies
   when there is no retained match: return `no_match` only if the deleted
   prefix is proven not to contain a match. This summary is sufficient only for
   contiguous-prefix deletion; any future retention scheme with holes needs an
   equivalent completeness proof.

Before exposing the selector, implementation must resolve it without an
unbounded history scan. [ADR 0042](0042-recoverable-replay-time-index.md)
selects an offset-ordered cumulative prefix-maximum checkpoint index with
256-record blocks. Monotone prefix summaries remain searchable under timestamp
regressions; the first qualifying block is scanned in logical order. The index
is derived from the authoritative log/state and rebuilt on recovery and
snapshot installation. When prefix retention is introduced, recovery must
also preserve or rebuild the complete deleted-prefix maximum. See ADR 0042
for work, memory, result, and runtime acceptance bounds.

## Alternatives considered

- **Treat timestamps as monotonic and binary-search them.** Rejected: current
  writers do not enforce monotonicity, clustered ingress can use different
  node clocks, and old stored records cannot be assumed monotonic without a
  migration and a new publish-time guarantee.
- **Return the first match in timestamp order.** Rejected: a timestamp-sorted
  index could choose a later offset than an earlier matching record, changing
  the logical stream order seen by replay.
- **Filter all records whose timestamp is at least `T`.** Rejected for this
  selector: matches can be non-contiguous under clock regressions, so this is
  a different query with separate pagination and resource semantics.
- **Use caller event time or leader commit time.** Deferred: neither exists in
  the current message contract. Introducing either requires a distinct field
  and source/clock semantics; it must not silently reinterpret
  `published_at_ms`.
- **Return the retained suffix when an earlier deleted prefix may match.**
  Rejected: callers could mistake incomplete history for a complete replay.
- **Rewind the ordinary consumer checkpoint or create a special consumer.**
  Rejected by ADR 0024: replay remains separate from delivery and durable
  consumer progress.

## Consequences and boundaries

This decision accepts timestamp selection semantics, not runtime support. The
existing offset replay operation and its outcomes remain unchanged. No
protocol field, error code, replay page/session, retention policy, retention
floor, checkpoint reset, timestamp monotonicity promise, or clock-accuracy
promise is added here. The provisional protocol can represent the selector
later under its versioned contract; protocol naming and exact outcome encoding
remain open under ADR 0031.

A future implementation adds only the bounded one-record selector first.
Stateless pages, durable replay sessions, retention pins, acknowledgement of
replay results, and replacement of ordinary consumer progress remain separate
choices. Cursor traversal after the selected record is in logical offset
order; applications that need an event-time filter must apply that filter
explicitly and account for late or clock-skewed records.

## Verification gates

- Deterministic tests inject timestamps rather than relying on wall-clock
  sleeps. Cover inclusive equality, multiple records with equal timestamps,
  regressing timestamps, first-match minimum offset, no match on empty and
  captured-end views, and a concurrent append at the view boundary.
- Verify that a time replay leaves the ordinary checkpoint, grouped
  acknowledgements, retry attempts, leases, and delivery tokens unchanged in
  local and clustered engines, including restart and cluster snapshot/recovery.
- Before retention exists, prove current offset-zero history returns either
  the exact earliest matching logical offset or `no_match`. With prefix
  retention, test `D < T`, `D = T`, `D > T`, absent/corrupt summary, and a
  retained matching record after deleted matching history; only the first case
  may safely continue to retained lookup.
- Establish the ADR 0042 timestamp index with a bounded checkpoint search and
  at most 256 record-header checks, including no-match and adversarial
  regression cases. Test rebuild, crash recovery, snapshot installation, and
  agreement between local and clustered results. Recovery must validate the
  authoritative state and future retention metadata under their current
  contracts; the derived index is rebuilt and has no separate index-format
  migration.
- Exercise real server/process and three-node paths for the eventual public
  operation. Measure selector latency, bytes/records examined, index space,
  recovery cost, and foreground publish/poll latency over increasing histories.
  Do not claim bounded work or acceptable overhead from unit tests alone.

Until these gates pass, the selector remains unavailable at runtime. The
semantic choice itself is settled; implementation and wire outcomes remain
reviewable follow-on work.
