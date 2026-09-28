# Replay time-selector semantics

- Status: exploratory research; no API or compatibility decision
- Last reviewed: 2026-09-28
- Baseline: `a5a228e59c2caa19c1a6520cde6a6abdd9e90196`
- Primary evidence class: correctness/reliability
- Scope: meanings and operational bounds for the unfinished replay time selector
- Related: [replay backlog outcome](../backlog.md#make-replay-an-explicit-and-safe-consumer-operation), [ADR 0024](../decisions/0024-explicit-offset-replay-read.md), and [retention and disk-pressure design](../design/retention-disk-pressure-plan.md)

This note examines the open time-selector semantics in the replay backlog. It
does not change runtime behavior, propose a compatibility promise, or accept
the candidate in the retention design. The current contract and code are the
baseline; the existing design's “first retained record at or after” wording is
a useful candidate that still needs edge-case decisions.

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

## Candidate meanings

The retention design currently proposes a **published-time selector resolved
to the first retained record at or after the requested time**. That is closest
to Kafka's lower-bound lookup and is a plausible replay-start meaning. To be
testable, it would need to mean something like “choose the lowest logical
message offset in the selected retained view whose stored `published_at_ms` is
greater than or equal to `T`; begin replay at that record and then follow
logical append order.” This is an inference for discussion, not an accepted
definition.

| Meaning | Boundary and ordering | Benefits | Main risk or ambiguity |
| --- | --- | --- | --- |
| First logical record with timestamp `>= T` | Inclusive timestamp comparison; among matches choose the lowest logical offset. Subsequent records follow append order. | A familiar “start from time” selector and a clear tie rule; preserves stream order. | If timestamps go backwards, later replayed records can have timestamps below `T`. It is a start-position lookup, not a filter returning only records whose timestamp is `>= T`. |
| First logical record with timestamp `> T` | Exclusive timestamp comparison; choose lowest matching logical offset. | Can model “strictly after” a checkpoint time. | Excludes all same-millisecond records at `T`; easy for callers to expect the opposite unless the operation name makes exclusivity explicit. |
| Last logical record with timestamp `<= T` | Inclusive upper boundary; choose the highest matching logical offset and begin there or after it. | Useful for “as of” reads or choosing a preceding anchor. | It is a different question from replaying forward from time; “start at” and “as of” must not share an underspecified selector. |
| Nearest record by absolute time distance | Minimize `abs(record_time - T)` with another rule for ties. | Can find a nearby anchor even when no exact timestamp exists. | May select a record before `T`, is unstable around equal-distance ties, and does not define the replay interval that follows. “Nearest” alone is not a safe replay contract. |
| Return every record whose timestamp is `>= T` | Timestamp is a filter over a bounded view, not a cursor lookup. | Gives a literal time-filtered result. | Results can be non-contiguous in append order; work and response size need explicit bounds, and consumers need a cursor independent of timestamps. |

The first interpretation is a reasonable **candidate** because replay is
forward through an ordered stream and the current retention design already
uses that wording. However, the actual predicate should be on the stored
broker-assigned `published_at_ms`, with the meaning that records after the
selected start position are returned in logical order even if their timestamps
are lower. If product intent instead requires “all events occurring after
time T,” Runnel would need a timestamp filter and a late-arrival policy, not
only a replay start selector.

## Decisions a future contract must make

### Timestamp source and clock

**Observed:** the timestamp is broker-assigned using process wall time, stored
at millisecond precision. The clustered request path can sample it on a
non-leader ingress node and forward it. The protocol has no caller-supplied
event-time field.

**Inference:** using this field is the smallest compatible selector because
the current record already exposes it. It means “Runnel-assigned publish
time” only. It must not be described as event time, a commit timestamp, or a
globally monotonic timeline. A future API could support caller event time, but
that would require a distinct field and validation rules rather than
silently changing `published_at_ms`.

**Open:** should the clustered timestamp continue to come from the request
ingress node, be sampled at the leader, or be assigned from a per-stream
monotonic policy? These choices move the meaning and clock dependency. A
monotonic policy could make timestamp lookup easier but would be a semantic
change and still would not make timestamps represent physical elapsed time.
Spanner's uncertainty interval is an example of the additional machinery
needed to make stronger clock claims; Runnel currently has no such bound.

### Equal, late, and out-of-order timestamps

With millisecond precision, multiple records can share one timestamp. An
inclusive lower-bound selector should therefore choose the lowest matching
logical offset, so a retry of the same selector over an unchanged history
resolves consistently. A rule based on timestamps alone cannot establish
append order when timestamps tie.

The code does not enforce nondecreasing timestamps. A timestamp index sorted
only by time could therefore return a later logical offset while an earlier
logical record also matches. A lower-bound scan in logical order is a precise
definition even with out-of-order timestamps, but it may require inspecting
many records. Enforcing monotonic timestamps or scanning/filtering by time are
different designs with different API consequences; neither follows from the
current code.

The current field also cannot distinguish delayed publication, clock skew,
wall-clock correction, or an intentionally late application event. Late
events are therefore resolved by their stored publish timestamp and logical
offset, not by inferred causality.

### No match and retained history

An input time later than every record in the captured view has no matching
record; that is not necessarily `history_unavailable`. A future API should
distinguish “no match through this view's end” from “the requested scope is
partly or wholly below the retained floor.” Reusing `history_unavailable` for
both cases would make a future time selector indistinguishable from a missing
record at an offset.

Retention makes completeness more difficult when timestamps may be out of
order. The logical retention floor tells the broker which offsets were
deleted, but the timestamp at the first retained record cannot prove that no
deleted record had a timestamp greater than or equal to `T`. Returning only
the first retained match may silently truncate a time scope. Possible
directions to evaluate include conservative unavailable-history reporting,
retaining a summary of timestamp bounds for deleted history, or requiring a
documented timestamp-order invariant before promising complete time ranges.
This note does not select one. The existing retention design's requirement to
report the available logical and time boundary and fail a request that spans
deleted history is a useful safety constraint, but needs an implementable
definition for nonmonotonic timestamps.

If no record matches `T` in the current retained view, a read-only one-shot
lookup should report the view's end as “no current match.” Waiting for future
records would be a follow/live operation with separate timeouts and session
semantics.

### Precision and input representation

The stored timestamp is integer Unix milliseconds. A selector with finer
precision cannot recover information that was never stored. If a future wire
field is numeric milliseconds, negative and out-of-range values need explicit
validation. If a textual field is desired, it should require an unambiguous
UTC offset and define rounding or rejection for sub-millisecond fractions;
RFC 3339 provides a representation vocabulary but not those Runnel policies.

### Concurrency, progress, and sessions

Current offset replay is read-only and does not change ordinary progress; the
consumer contract tests that replaying an acknowledged record does not rewind
the next poll. A time selector should preserve this separation. Replaying a
record while the ordinary consumer processes it may cause the application to
observe the same logical record through both paths; the replay result is not
an ordinary delivery and has no delivery token or acknowledgement.

A one-shot selector should be resolved against one captured stream view: its
retention floor and end (`next`) at the operation's serialization point. The
local stream lock already gives an individual replay read an append boundary;
the clustered replay command is submitted through the stream Raft group. A
multi-record replay session still needs a stable end boundary (or an explicit
live-follow mode), a cursor, and retention pin/failure behavior. Otherwise a
session could see new appends inconsistently or lose a record between selector
resolution and fetch. Ordinary poll/ack activity must not silently reset or
advance that replay cursor, and replay must not rewrite the ordinary
checkpoint.

The public boundary should remain a stream, consumer, logical record, and
replay scope. Existing logical message offsets are already part of the
provisional protocol; a time selector need not expose file positions, segment
IDs, Raft log IDs, node placement, or other physical storage coordinates.

## Bounds and operational implications

There is no time lookup operation or declared timestamp index in the current
replay contract. The local log keeps logical-offset lookup structures, while
the clustered state machine holds retained messages in a vector. A first
matching logical offset under nonmonotonic timestamps may require examining
the retained range. That could turn a nominally one-record replay into
history-proportional work, hold the local per-stream lock, or consume
clustered state-machine work. The exact cost is not measured here.

Any implementation proposal should therefore bound both the selector work
and the replay result/session. It should state whether lookup has a proven
index, a bounded scan with a resource limit, or a potentially long scan that
must be rejected or scheduled away from foreground delivery. It should
measure local and clustered lookup cost over increasing retained histories
and include concurrent publish/poll workloads before claiming acceptable
impact. Adding an index may require additional recovery metadata and consistency
checks; the existing replay budget constraint that replay not starve ordinary
consumers still applies.

## Disposition and evidence needed

**Runnel inference:** time-based replay is reasonably implementable as an
incremental capability because records already carry broker-assigned
millisecond timestamps and both engines have a logical replay boundary. The
smallest candidate is an inclusive lower-bound start selector over stored
publish time, resolved in logical offset order. However, it is not ready to
become an implementation contract until the project decides whether that
lookup remains meaningful with nonmonotonic timestamps, what “complete” means
below a retention floor, and how a multi-record session pins a stable view.

**Disposition:** keep the existing replay backlog outcome open; do not add a
separate backlog item or edit the shared tracker for this note. The current
backlog already names time selectors, and the retention design already records
the first-at-or-after candidate. Treat this note as evidence to refine those
open semantics. Defer runtime work until a design/ADR resolves timestamp
source, tie and no-match outcomes, retention completeness, bounded lookup,
and session behavior. This is near-term design work, while a production
selector with retention-aware sessions depends on the broader replay and
retention capabilities.

Evidence needed before implementation includes:

- tests for inclusive/exclusive boundaries and several records sharing one
  millisecond;
- tests with backward, equal, and forward timestamp values at increasing
  logical offsets;
- tests for a target before, within, and after the retained time range, plus
  prefix deletion where deleted timestamps are not monotonic;
- local and clustered tests that capture the same logical view during
  concurrent publish, poll, acknowledgement, and replay;
- restart and leader-change coverage for any persisted replay session, cursor,
  retention pin, or timestamp index;
- bounded-work and resource evidence over increasing retained-history sizes,
  including impact on foreground poll/publish latency.

## Code and planning assessment

The inspection covered the current local and clustered replay paths, the
shared replay contract, the timestamp creation paths, ADR 0024, and the
retention design proposal. No runtime or test changes are appropriate in this
research-only task. No separate safe refactor was identified: consolidating
timestamp assignment or adding a time index would itself constrain selector
semantics and should follow the design decision and evidence above. The
existing replay backlog remains the correct planning record; no backlog or
tech-debt change is warranted by this exploratory note.
