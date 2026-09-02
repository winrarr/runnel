# ADR 0024: Make the first replay operation an explicit offset read

- Status: accepted for the first replay slice; replay sessions remain open
- Date: 2026-09-02
- Baseline: `origin/main` `51ba190205e66c37a9daf7176e02c58020ae519c`
- Scope: the first local, clustered, and provisional protocol replay operation
- Related: [replay backlog](../backlog.md#make-replay-an-explicit-and-safe-consumer-operation), [retention and disk-pressure plan](../design/retention-disk-pressure-plan.md), [ADR 0004](0004-multi-raft-first-distributed-engine.md), and [ADR 0023](0023-independent-retained-storage-and-placement.md)

## Context

Runnel's ordinary poll follows a durable consumer checkpoint and creates
delivery state. Replay must not silently reset that checkpoint, consume an
acknowledgement, or invent a special consumer name. The retention design
requires explicit unavailable-history outcomes, but retention floors and
bounded replay sessions are not implemented yet.

The first useful slice is therefore a single-record operation with a complete
meaning: read the record at an inclusive logical offset, or return an explicit
unavailable-history error. It does not claim to be a resumable replay
session.

## Evidence and differences that matter

| Reference | Relevant behavior | Consequence for Runnel |
| --- | --- | --- |
| [Apache KafkaConsumer `seek` and `offsetsForTimes`](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) | A consumer can change the next fetch position, look up the first offset at or after a timestamp, and encounter an out-of-range offset subject to reset policy. | Kafka demonstrates useful offset/time selectors, but mutating the fetch position and automatic reset are unsafe defaults for Runnel while ordinary durable progress and retention policy are still being defined. |
| [NATS JetStream pull consumers](https://docs.nats.io/learn/jetstream/pull-consumers) | Pull fetches are explicitly bounded by message count and expiry; delivered messages use the normal explicit acknowledgement path and the consumer cursor advances through acknowledgement. | NATS supports bounded replay-like reads, but tying the read to delivery/ack state would require Runnel to settle replay session lifetime, fencing, and progress replacement first. |
| [The Raft paper](https://raft.github.io/raft.pdf) | A replicated state machine applies an ordered command stream and can compact consensus history with snapshots. | The clustered operation is routed through the stream data-group leader and committed there, while the replayed record remains retained message history rather than a consumer-state mutation. |

These references inform boundaries only. They do not establish Runnel
compatibility or retention policy.

## Decision

Expose an additive `replay` operation:

```json
{"op":"replay","stream":"events","consumer":"worker","offset":7}
```

The selector is inclusive and currently supports only a logical offset. A
successful response is `replay_message`; it has the stream, consumer,
logical offset, key, payload, and publish timestamp, but no delivery token or
attempt. A text response and a binary-safe response are both supported by the
existing protocol representation. The typed client exposes replay messages as
a distinct type that cannot be passed to its ordinary acknowledgement result
without an explicit conversion by the caller.

Replay validates the stream and consumer names, reads at most one record, and
does not create consumer state, in-flight state, delivery attempts, or
ordinary checkpoint progress. A missing offset returns `history_unavailable`
with the current available half-open offset range `[earliest, next)`. The
current engines retain all history from offset zero, so `earliest` is zero;
the same outcome shape is reserved for future retention floors. The outcome
is never represented as ordinary `empty` polling.

The local engine performs one bounded indexed/cold logical-offset lookup. The
clustered engine submits the same read intent through the stream data-group
Raft command and forwards it to the elected leader when a client reaches a
follower. The command does not mutate broker state, although committing the
intent keeps the first clustered read path linearizable with the committed
stream view.

## Alternatives considered

- **Reset the ordinary consumer checkpoint and reuse poll/ack.** Rejected:
  it can discard or reorder durable progress and makes replay acknowledgements
  indistinguishable from normal delivery.
- **Create a special consumer name.** Rejected: it leaks an application-level
  workaround for broker semantics and gives the special consumer an undefined
  lifecycle and retention fence.
- **Add a durable replay session immediately.** Deferred: session identity,
  cursor durability, replay acknowledgement/fencing, lease expiry, retention
  pins, and failover behavior are not sufficiently specified to expose safely
  in this slice.
- **Return ordinary `Empty` when the offset is absent.** Rejected: callers
  could not distinguish end-of-history or deleted history from an empty normal
  poll and might silently replay a different suffix.

## Consequences and compatibility boundary

Existing `poll`, `ack`, grouped delivery, and checkpoint formats retain their
meaning. Replay is additive and has no effect on existing consumer progress,
retry attempts, delivery metrics, or acknowledgements. The operation is
bounded to one record, so it cannot monopolize a stream's ordinary delivery
lane through a large historical scan or response.

This slice does not provide time selectors, earliest selectors, ranges,
durable replay cursors, replay acknowledgements, progress replacement,
retention cleanup, replay pins, replay lag metrics, or replay-specific
backpressure metrics. It also does not claim that an unavailable offset is
recoverable. A client that receives a transport timeout or disconnect must
apply the existing unknown-outcome rule, even though the operation itself is
read-only.

## Verification

- Engine conformance covers local and clustered implementations, replay of an
  acknowledged record, ordinary progress remaining unchanged, and explicit
  unavailable history.
- Protocol and client tests cover the additive wire shape, binary payloads,
  typed replay responses, and rejected `history_unavailable` outcomes.
- Real-server tests cover replay before and after local restart and replay
  through a three-process clustered deployment.

The full replay backlog item remains open until session, selector, retention,
failover, and observability acceptance evidence exists.
