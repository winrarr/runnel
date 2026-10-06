# Replay time-selector semantics

- Status: source research; selector semantics accepted by ADR 0038 and index/recovery contract accepted by ADR 0042; runtime and API remain open
- Last reviewed: 2026-10-06
- Baseline inspected: `da6b14e72ce75317ad0fa3fe05f91a28026b67b2`
- Primary evidence class: design/research; secondary: correctness/reliability, storage/recovery
- Scope: meanings and operational bounds for unfinished time-based replay and durable sessions
- Related: [replay backlog outcome](../backlog.md#make-replay-an-explicit-and-safe-consumer-operation), [ADR 0024](../decisions/0024-explicit-offset-replay-read.md), [ADR 0038](../decisions/0038-timestamp-based-replay-selector.md), [retention and disk-pressure design](../design/retention-disk-pressure-plan.md), and [durable replay sessions](../design/replay-sessions.md)

This note records the current timestamp source, relevant reference behavior,
and lookup constraints for the replay selector. [ADR 0038](../decisions/0038-timestamp-based-replay-selector.md)
accepts the selector semantics, and [ADR 0042](../decisions/0042-recoverable-replay-time-index.md)
selects its derived prefix-maximum index and recovery boundary. Neither adds
runtime behavior or selects an API representation or replay-session contract.
The inspected baseline is code evidence, not a claim that a timestamp selector
is already implemented.

## Current Runnel behavior

At this baseline, replay reads one record at an inclusive **logical message
offset**. Local replay validates stream and consumer names, takes the stream
lock, and reads the selected record without creating delivery state or changing
the consumer checkpoint. The core log returns `history_unavailable` for an
offset outside its current `[0, next)` range. The clustered path submits replay
through the stream data group's Raft command; its state machine reads the
message vector and returns the same kind of unavailable-history result. There
is no retention floor in the current implementations, so the earliest logical
offset is zero. The consumer name is validated but does not otherwise select
state in this read-only operation.

These behaviors are visible in [`Broker::replay`](../../crates/runnel-core/src/broker.rs),
[`StreamLog::read_replay_message`](../../crates/runnel-core/src/stream_log.rs),
[`PersistentEngine::replay`](../../crates/runnel-raft/src/engine.rs), and the
clustered [`apply_replay`](../../crates/runnel-raft/src/state_machine.rs).
The common assertions are in
[`assert_replay_contract`](../../crates/runnel-test-support/src/lib.rs), and
the accepted boundary is recorded in [ADR 0024](../decisions/0024-explicit-offset-replay-read.md).

Runnel already returns a `published_at_ms` field, but the caller cannot supply
an event timestamp. The local log and Raft engine derive it from the process
wall clock as non-negative milliseconds since the Unix epoch. In the
persistent clustered engine, the timestamp is sampled by the node receiving a
publish request before a follower forwards the operation to the leader. The
stored time therefore describes Runnel's current broker-side publish-time
approximation, not necessarily application event time, leader commit time, or
a globally ordered clock. The code does not clamp timestamps to the previous
record's timestamp. A wall-clock step or different node clocks can produce
equal or decreasing timestamps at increasing logical offsets.

## What reference systems establish

These references describe product behavior or a primary research result. They
are evidence for design tradeoffs, not a Runnel compatibility target.

- [Kafka's `offsetsForTimes`](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
  defines the result as the earliest offset whose record timestamp is greater
  than or equal to the requested time. Kafka's topic configuration separately
  distinguishes `CreateTime` from `LogAppendTime` in
  [`message.timestamp.type`](https://kafka.apache.org/42/configuration/topic-configs/).
  This makes the selector boundary and timestamp source explicit, but it does
  not settle Runnel's retention completeness or replay-session behavior.
  Kafka's accepted [KIP-33 time-index proposal](https://cwiki.apache.org/confluence/display/KAFKA/KIP-33+-+Add+a+time+based+log+index)
  records a sparse maximum-timestamp/offset index and scans the log around the
  selected entry. It notes that indexes are monotone only within a segment and
  allows earlier-timestamp records in a lookup result. This is useful evidence
  for sparse summaries and scan bounds, but its result allowance is weaker
  than Runnel's exact lowest-offset selector.
- [PostgreSQL BRIN indexes](https://www.postgresql.org/docs/current/brin.html)
  summarize adjacent physical block ranges and recheck candidate tuples;
  min/max range summaries can be compact, with range size trading index size
  against false-positive scans. Runnel borrows the compact-block idea but
  needs an ordered prefix summary so one binary search identifies the first
  possible matching block under arbitrary timestamp regressions.
- [Pulsar's Reader API](https://pulsar.apache.org/api/client/4.2.x/org/apache/pulsar/client/api/Reader.html)
  accepts a Unix-millisecond timestamp to reposition a reader by message
  publish time. The public API describes a reader position reset; it does not
  provide the independent, read-only replay semantics Runnel accepted in ADR
  0024. Its documentation also leaves tie and missing-history behavior to
  implementation details, illustrating why Runnel should state those cases
  itself.
- [Spanner's OSDI paper](https://research.google.com/archive/spanner-osdi2012.pdf)
  represents `TrueTime` as an interval with bounded uncertainty and explains
  that ordinary time APIs do not expose such uncertainty. This is a useful
  limit on what a timestamp can prove: Runnel's scalar millisecond field does
  not represent clock error bounds or establish externally consistent event
  ordering.
- [RFC 3339](https://www.rfc-editor.org/rfc/rfc3339) specifies fully qualified
  Internet timestamps and uses UTC-oriented representations. It can inform a
  future human-facing timestamp syntax, but it does not define replay boundary,
  clock accuracy, or precision policy. Runnel's current stored field is an
  integer millisecond value.

The session-specific comparisons add three useful contrasts:

- [NATS JetStream durable pull consumers](https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment)
  keep a named consumer position, advance its acknowledgement floor on explicit
  ack, and redeliver an unacknowledged record after its ack-wait interval. Its
  [pull API](https://docs.nats.io/learn/jetstream/pull-consumers) bounds one
  fetch by message count and expiry. This supports an ack-driven, one-record
  replay-session candidate and bounded polls. It does not directly provide an
  independent replay cursor beside an unchanged ordinary cursor: the durable
  consumer itself is the delivery cursor.
- [Pulsar Readers](https://pulsar.apache.org/docs/client-libraries/readers/)
  have a caller-selected start message but no broker-maintained cursor or
  acknowledgement; Pulsar's [consumer model](https://pulsar.apache.org/docs/4.0.x/concepts-clients/)
  uses a subscription cursor advanced by acknowledgements. This illustrates
  the separate stateless-reader and durable-subscription designs. Neither
  alone settles Runnel's durable replay-session lifecycle.
- The [Raft paper](https://raft.github.io/raft.pdf) describes ordered committed
  commands applied by replicated state machines and snapshots that preserve
  state after log compaction. This supports placing clustered session changes
  and retention-floor decisions in one logical command order, with session
  metadata included in recoverable state. Raft does not supply time lookup,
  retention, or lease semantics.

## Selector alternatives and accepted contract

ADR 0038 selects the first interpretation in the table: the lowest logical
offset in one captured view whose stored broker `published_at_ms` is at least
`T`, with append-order traversal thereafter. The selector is inclusive and
selects a replay start, not a timestamp-filtered set. Kafka uses a similar
lower-bound shape for `offsetsForTimes`; the accepted tie, regression, no-match,
and deleted-prefix rules are Runnel decisions.

| Meaning | Boundary and ordering | Benefits | Main risk or ambiguity |
| --- | --- | --- | --- |
| First logical record with timestamp `>= T` | Inclusive timestamp comparison; among matches choose the lowest logical offset. Subsequent records follow append order. | A familiar “start from time” selector and a clear tie rule; preserves stream order. | If timestamps go backwards, later replayed records can have timestamps below `T`. It is a start-position lookup, not a filter returning only records whose timestamp is `>= T`. |
| First logical record with timestamp `> T` | Exclusive timestamp comparison; choose lowest matching logical offset. | Can model “strictly after” a checkpoint time. | Excludes all same-millisecond records at `T`; easy for callers to expect the opposite unless the operation name makes exclusivity explicit. |
| Last logical record with timestamp `<= T` | Inclusive upper boundary; choose the highest matching logical offset and begin there or after it. | Useful for “as of” reads or choosing a preceding anchor. | It is a different question from replaying forward from time; “start at” and “as of” must not share an underspecified selector. |
| Nearest record by absolute time distance | Minimize `abs(record_time - T)` with another rule for ties. | Can find a nearby anchor even when no exact timestamp exists. | May select a record before `T`, is unstable around equal-distance ties, and does not define the replay interval that follows. “Nearest” alone is not a safe replay contract. |
| Return every record whose timestamp is `>= T` | Timestamp is a filter over a bounded view, not a cursor lookup. | Gives a literal time-filtered result. | Results can be non-contiguous in append order; work and response size need explicit bounds, and consumers need a cursor independent of timestamps. |

Other rows remain rejected alternatives for this replay-start operation. A
request for every event whose timestamp is at least T would be a distinct
filter with separate bounded-result and late-arrival semantics.

## Accepted contract and open implementation gates

### Timestamp source and clock

**Observed:** the timestamp is broker-assigned using process wall time, stored
at millisecond precision. The clustered request path can sample it on a
non-leader ingress node and forward it. The protocol has no caller-supplied
event-time field.

**Accepted:** use the persisted `published_at_ms` exactly as stored. It means
Runnel-assigned publish time only; it is not event time, a commit timestamp,
or a globally monotonic timeline. A future caller event-time field would be a
distinct message-contract change, not a reinterpretation of this field.

**Observed:** local append samples process wall time. The clustered request
path samples at ingress before forwarding may occur. ADR 0038 does not move
that sampling point, clamp old or new timestamps, or promise a clock-error
bound. Spanner's uncertainty interval illustrates the additional machinery
needed for stronger clock claims; Runnel currently exposes no such bound.

### Equal, late, and out-of-order timestamps

With millisecond precision, multiple records can share one timestamp. ADR
0035 resolves ties to the lowest matching logical offset. The code does not
enforce nondecreasing timestamps. Later logical records may therefore have
lower timestamps than earlier ones; the selector still chooses the lowest
offset matching `published_at_ms >= T`, and continuation remains in append
order. A timestamp-only sorted index cannot implement that rule. A logical-
order scan can define it, but requires examining a potentially large retained
range, so runtime lookup must use a recoverable bounded structure.

The current field also cannot distinguish delayed publication, clock skew,
wall-clock correction, or an intentionally late application event. Late
events are therefore resolved by their stored publish timestamp and logical
offset, not by inferred causality.

### No match and retained history

An input time later than every record in the captured view has no match.
ADR 0038 assigns this a distinct `no_match` outcome, including an empty stream;
the one-shot read does not wait for future records. This outcome is valid only
when the searched view is complete for the threshold.

With prefix retention, the deleted prefix can contain an earlier match even
when a retained suffix has a match. ADR 0038 accepts a complete maximum
`published_at_ms` summary for the deleted prefix: if it is below T, no deleted
record matched; if it is at least T, return `history_unavailable` because the
earliest matching offset was deleted and is unknowable. Missing or untrusted
summary metadata also fails closed. This summary works only for contiguous
prefix deletion; a future scheme with holes needs an equivalent completeness
proof.

### Precision and input representation

The stored timestamp is integer Unix milliseconds. ADR 0038 selects a
non-negative unsigned millisecond input and rejects values that cannot be
represented by the selected integer type. A textual RFC 3339 field is not
selected; if considered later, it would need UTC-offset and sub-millisecond
rounding rules because the stored source cannot recover finer precision.

### Concurrency, progress, and sessions

Current offset replay is read-only and does not change ordinary progress; the
consumer contract tests that replaying an acknowledged record does not rewind
the next poll. The accepted time selector preserves this separation. Replaying a
record while the ordinary consumer processes it may cause the application to
observe the same logical record through both paths; the replay result is not
an ordinary delivery and has no delivery token or acknowledgement.

A selector is resolved against one captured stream view: its retention
floor and end position at the operation's serialization point. The local
stream lock already gives an individual replay read an append boundary; the
clustered replay command is submitted through the stream Raft group. A
multi-record replay session needs a stable end boundary (or an explicit
live-follow mode), a cursor, and retention pin/failure behavior. Otherwise a
session could see new appends inconsistently or lose a record between selector
resolution and fetch. Ordinary poll/ack activity must not silently reset or
advance that replay cursor, and replay must not rewrite the ordinary
checkpoint. The [session design](../design/replay-sessions.md) develops a
fixed-view, separate-cursor model and records its open choices for ack fencing,
retries, lifecycle, and retention.

The public boundary should remain a stream, consumer, logical record, and
replay scope. Existing logical message offsets are already part of the
provisional protocol; a time selector need not expose file positions, segment
IDs, Raft log IDs, node placement, or other physical storage coordinates.

## Bounds and operational implications

No time lookup operation is implemented. The local log keeps logical-offset
lookup structures, while the clustered state machine holds retained messages
in an offset-ordered vector. The minimum matching offset under timestamp
regressions cannot be found by binary-searching record timestamps. A plain
timestamp-sorted index also does not preserve logical-offset order.

[ADR 0042](../decisions/0042-recoverable-replay-time-index.md) selects an
offset-ordered index with one cumulative prefix maximum per 256-record block.
The checkpoint values are monotone even when record timestamps regress, so a
lower-bound search finds the first block that can contain a match; scanning
that block in logical order finds the exact earliest match. This bounds a
request to logarithmic checkpoint comparisons plus at most 256 record-header
checks. Local byte cursors are stored with checkpoints; clustered block
offsets follow from the retained floor. The index is derived from the local
log or committed clustered state, rebuilt on open/recovery, and reconstructed
after snapshot installation. It adds a deterministic sparse metadata cost of
one compact summary per 256 retained records, which still grows with retained
history. Startup, resident-memory, and foreground publish/poll effects need
measurement; the decision makes no performance claim.

## Disposition

The source evidence supports a deterministic time replay contract using the
existing `published_at_ms` field. [ADR 0038](../decisions/0038-timestamp-based-replay-selector.md)
accepts inclusive `>= T`, the lowest logical matching offset, append-order
continuation, explicit `no_match`, and fail-closed behavior when a deleted
prefix could contain an earlier match. This selects broker-assigned publish
time, not event time or a globally ordered physical clock.

Runtime work is next within the replay backlog, not deferred pending another
semantic decision. The first slice is a bounded one-record time selector
using the index contract in ADR 0042; implementing a history-proportional
request scan would not satisfy the backlog's resource constraint. Pages and
durable sessions remain follow-on decisions
because they add cursor, fencing, failover, and retention-pin lifecycle state.

Evidence needed before implementation includes:

- tests for inclusive boundaries and several records sharing one
  millisecond;
- tests with backward, equal, and forward timestamp values at increasing
  logical offsets;
- tests for a target before, within, and after the retained time range, plus
  prefix deletion where the deleted-prefix maximum is below, equal to, and
  above the selector threshold, including missing/corrupt summary metadata;
- local and clustered tests that capture the same logical view during
  concurrent publish, poll, acknowledgement, and replay;
- restart and leader-change coverage for derived-index rebuild, plus snapshot
  installation and future persisted retention-floor metadata;
- bounded-work and resource evidence over increasing retained-history sizes,
  including impact on foreground poll/publish latency.

## Code and planning assessment

The inspection covered the current local and clustered replay paths, the
shared replay contract, the timestamp creation paths, ADRs 0024, 0036, and
0038, the retention proposal, Kafka KIP-33, and PostgreSQL BRIN behavior.
This documentation run changes no runtime or tests. [ADR 0042](../decisions/0042-recoverable-replay-time-index.md)
selects the routine index and recovery details: cumulative prefix-maximum
checkpoints, a 256-record scan bound, deterministic rebuild, and a
fail-closed contiguous-prefix retention boundary. The existing replay backlog
remains the correct tracker. TD-002 and TD-010 already cover local cold scans
and clustered retained-state growth; their retirement conditions remain open
for implementation, measurement, and failure evidence. No new debt identifier
is warranted.
