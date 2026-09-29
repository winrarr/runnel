# Replay selectors and bounded sessions

- Status: exploratory design; no API or compatibility decision
- Last reviewed: 2026-09-29
- Baseline: 3d2f2a6a68ef978ed43a0735159f26db332483d9
- Primary evidence class: design/research
- Scope: bounded replay selectors, paging, and optional durable replay sessions
- Related: [replay backlog outcome](../backlog.md#make-replay-an-explicit-and-safe-consumer-operation), [ADR 0024](../decisions/0024-explicit-offset-replay-read.md), [replay time-selector research](../research/replay-time-selector-semantics.md), and [retention and disk-pressure design](retention-disk-pressure-plan.md)

This note explores how to extend the accepted one-record offset read into
bounded replay without coupling replay to a consumer's ordinary checkpoint. It
compares selector reads, stateless bounded pages, and durable sessions. It
does not accept wire fields, retention behavior, a timestamp index, or a
session state format. Any API shapes below are illustrative.

## Current boundary

At this baseline, Rust code and [ADR 0024](../decisions/0024-explicit-offset-replay-read.md)
define one read-only replay operation. It takes a stream, consumer identity,
and inclusive logical offset, reads at most one record, and returns
<code>history_unavailable</code> when the offset is outside the retained range.
Replay does not create delivery state, attempts, tokens, or ordinary checkpoint
progress. The local engine reads under the stream lock; the clustered engine
routes the read through the stream data group's committed state. The
provisional protocol, typed client, CLI, and real-server tests expose this
one-record operation. Current history begins at offset zero because retention
floors are not implemented.

Relevant implementation boundaries are [local replay](../../crates/runnel-core/src/broker.rs),
[the local log lookup](../../crates/runnel-core/src/stream_log.rs),
[cluster replay application](../../crates/runnel-raft/src/state_machine.rs),
[the protocol request](../../crates/runnel-protocol/src/lib.rs), and
[the typed client methods](../../crates/runnel-client/src/lib.rs). The current
architecture and [retention design](retention-disk-pressure-plan.md) remain
the sources for broader storage and consumer behavior.

The stored <code>published_at_ms</code> is a broker-assigned Unix-millisecond
wall-clock value, not caller event time, commit time, or a globally monotonic
clock. In the clustered path it may be sampled by a non-leader ingress node
before forwarding. Equal or decreasing timestamps at increasing offsets are
possible. The [selector research](../research/replay-time-selector-semantics.md)
documents the code paths and these limits.

## Design boundary

The useful public concepts are a stream, consumer identity, logical record,
replay selector, bounded result, and explicit outcome. Replay must not expose
segment names, file positions, Raft indexes, placement, or node identity. It
must leave ordinary poll, acknowledgement, retries, and durable consumer
progress independent unless a future explicit operation intentionally
replaces ordinary progress.

Any multi-record operation should capture a finite view [earliest, next) at
its start. It should not follow later publishes unless a separately named
live-follow operation defines timeouts, backpressure, and session lifetime.
Pages are in logical offset order. A cursor is a logical replay position,
never a storage coordinate.

## Bounded models

| Model | Durable state and bound | Advantages | Costs and failure cases |
| --- | --- | --- | --- |
| One-shot selector read | No replay state; return one record or one selector outcome. Work and payload are bounded by one record and the existing protocol frame limit. | Extends the accepted read with an earliest or time selector while preserving its read-only behavior. No orphan session or cursor recovery. | Repeated calls are chatty. It does not provide a stable multi-record view by itself. A caller that increments offsets must store its own cursor and handle retention between calls. |
| Stateless bounded page | No server-side cursor. Resolve a selector once, capture next, and return at most a configured/requested count and encoded-byte budget, with a continuation cursor containing the logical next offset and captured end. | Supports efficient multi-record scans while avoiding durable session lifecycle, fencing, snapshot, and pin state. A retry can reread a page safely. | The caller stores the cursor. Later pages can become unavailable under expire retention; no server pin guarantees completion. A cursor must preserve the original end and selector resolution to avoid including concurrent appends or resolving a time selector differently on retry. |
| Durable replay session | Persist a session identity/generation, resolved start, captured end, acknowledged cursor, bounded lease/expiry, and any retention pin. Each fetch is bounded; acknowledgement advances only that session. | Can resume a replay independently of client-side cursor storage, pin history under a selected policy, and make session progress visible. Provides an explicit place for replay-specific acknowledgement and lag. | Adds durable and replicated state, lease/fencing rules, idempotent creation and acknowledgement, snapshot/recovery obligations, resource quotas, and possible retention pressure. A protected abandoned session must not pin history forever. |
| Rewind ordinary consumer progress | Reuse the poll checkpoint as replay position. | Superficially reuses existing poll and acknowledgement behavior. | Rejected as the default replay model. It can discard or reorder acknowledged progress, mixes replay acknowledgements with ordinary delivery, and gives concurrent poll/ack operations ambiguous meaning. If ever required, progress replacement needs a separate explicit operation and generation fence. |

The first two models can remain read-only with respect to consumer and replay
state. The durable session model must choose whether fetching advances a cursor
or whether a replay acknowledgement does. Advancing on fetch can lose work
after a crash; acknowledgement-driven progress provides at-least-once replay,
but requires a replay-scoped token or sequence and idempotent acknowledgement.
An initial session, if justified, should permit one unacknowledged page per
session until stronger concurrency has evidence. Replay acknowledgement must
never be accepted as an ordinary consumer acknowledgement.

### Selector semantics

An eventual selector set could include an inclusive offset, the earliest
retained offset, a snapshot of the named consumer's current committed offset,
or broker-published time. Each selector resolves to one logical start offset
against the captured view. A checkpoint selector reads the ordinary committed
offset once at session/page creation; subsequent poll or acknowledgement
activity does not change that replay position. It does not create a second
checkpoint meaning or rewind ordinary progress.

For published time, the most implementable candidate is the lowest logical
offset in the captured retained view whose stored <code>published_at_ms</code>
is greater than or equal to T. Equal timestamps select the lowest matching
offset. After selecting a start, replay follows logical append order; it is not
a filter that excludes later records whose timestamp is less than T. This is
an inclusive lower-bound start selector, not event-time range semantics. The
candidate follows the existing retention design and resembles Kafka's
<code>offsetsForTimes</code>, but it remains an inference, not an accepted
contract.

The candidate has material edge cases:

- **Equal timestamps:** choose the lowest logical offset satisfying the
  predicate, so retries over the same view resolve identically.
- **Backward timestamps:** later logical records may be returned with times
  below T. A time-sorted index alone cannot implement lowest-offset lookup
  when timestamps are nonmonotonic.
- **No match at the captured end:** return a distinct <code>no_match</code> or
  empty-scope outcome, not <code>history_unavailable</code>. A one-shot lookup
  does not wait for future publishes.
- **Before or within deleted history:** return <code>history_unavailable</code>
  if the requested scope cannot be proven complete. Returning only a retained
  suffix would silently truncate the caller's requested replay.
- **Precision and range:** the stored unit is integer milliseconds. A numeric
  selector should define negative and out-of-range validation. A textual
  representation would need a UTC-offset requirement and a rule for
  sub-millisecond precision; RFC 3339 defines timestamp syntax, not these
  replay semantics.
- **Clock source:** the selector means broker-assigned publish time only. It
  must not imply event time, global ordering, or a clock uncertainty bound.

Retention complicates completeness because the current timestamps can move
backwards. The timestamp of the first retained record is not enough to prove
that deleted records did not satisfy <code>published_at_ms &gt;= T</code>.
Candidate directions include rejecting ambiguous time requests, persisting a
prefix summary such as the maximum deleted publish timestamp, or accepting a
monotonic timestamp invariant with its clock and migration costs. A maximum
deleted timestamp could prove that no deleted record matches a threshold
when it is below T; it cannot locate a matching deleted record, so requests
for which it is at least T must still fail as unavailable. This is an
illustrative bound, not a selected retention format.

If a time lookup scans records to find the lowest matching offset, it may
inspect history proportional to retained stream size. A sorted-by-time index
does not preserve lowest-offset semantics under clock regressions; an index
that does preserve that semantic can have memory, recovery, and consistency
costs. Before exposing a time selector, bound the scan or establish an index
whose consistency and recovery are tested. The read itself must also be
limited by the server's serialized response size, not only by logical payload
bytes.

## Fencing and concurrency

For a stateless page, resolve selector, retained floor, and next at one
serialization point. The cursor should carry the resolved offset and original
end boundary (or an opaque integrity-protected equivalent). Replaying the
same page after an unknown transport outcome must be safe. Pages never move
the ordinary checkpoint. Concurrent ordinary poll/ack can still deliver the
same logical record to the application through the normal path; replay does
not provide mutual exclusion with ordinary delivery, so applications needing
exclusive processing must coordinate that themselves.

A durable session needs its own opaque session identity and generation. Fetch,
acknowledgement, expiry, explicit end, and retention invalidation must be
serialized against that generation. An acknowledgement from an expired or
replaced session must not advance a new session. Session acknowledgements
advance only the session cursor; they do not change ordinary committed
progress, attempts, in-flight delivery tokens, or key gates. If a separate
progress-replacement operation is ever accepted, it must explicitly fence
ordinary deliveries and define what happens to out-of-order acknowledgements.

Session creation changes durable state and can time out after commit but before
the response reaches the client. It therefore needs idempotent creation, such
as a caller-supplied session key, or another way for a client to recover the
created identity. Acknowledgements should likewise be retry-safe for a given
session generation and replay sequence. Session limits, maximum lifetime,
renewal policy, maximum concurrent sessions per stream/consumer, and cleanup
work all need hard bounds. A lease by itself is not a total lifetime bound if
unlimited renewal is allowed.

## Retention and unavailable history

The logical retained range is half-open [earliest, next). Offset selection
below earliest is unavailable; an offset at or above captured next is not
present in that view. The current protocol's offset replay uses
<code>history_unavailable</code> for absent offsets and returns the available
offset range. A future time selector also needs to distinguish no current
match from unavailable history. A session whose next required offset falls
below the retention floor must report explicit unavailability or expiry; it
must not advance to a later retained record or report normal end-of-session.

The retention proposal's <code>protect</code> policy lets active replay sessions
pin their earliest unread history, while <code>expire</code> can end replay
eligibility and must expose the new boundary. These are candidate policies,
not current behavior. A bounded session lease and explicit close can limit
pins, but cleanup must wait for the durable expiry/fence to win the race with
a fetch or ack. Physical deletion may lag a committed logical floor without
making deleted records eligible again. Retention design must decide the policy
for a session created before a destructive floor advance and must report that
outcome to its caller.

An offset range or checkpoint selection crossing a known deleted prefix is
unavailable rather than a successful partial replay. For time selection, the
same whole-scope rule applies if deleted timestamps could have matched. The
broker needs enough retained-boundary metadata to report the available offset
range and, if promised, a meaningful time boundary. It must not infer time
completeness from the first retained record while timestamps are nonmonotonic.

## Restart, failover, and observability

| Model | Local process restart | Cluster leader change/recovery |
| --- | --- | --- |
| One-shot read | No replay cursor survives; a repeated request reads current retained history. | The committed stream view is served through the current data-group path; no replay state needs replication. |
| Stateless page | The caller resumes from its cursor if the referenced range remains retained. Captured next prevents restart from adding later records to that replay. | The same logical cursor/end can be used after forwarding to a new leader. A changed retention floor can make it unavailable; it cannot silently lower or reinterpret the end. |
| Durable session | Persist session generation, cursor, captured end, lease, and pin before reporting creation/ack success. Recovery may redeliver work after the last acknowledged cursor. | Session and retention facts must be committed in replicated stream state and included in snapshot/recovery. A new leader continues only committed session state; stale acknowledgements are fenced by generation. |

No model may claim session failover safety until real-process restart and
leader-change tests cover session creation, fetch, acknowledgement, lease
expiry, retention racing with fetch/ack, and snapshot recovery. The current
cluster replacement boundary remains governed by [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md).

For a session implementation, useful bounded signals include active and
expired session counts, oldest session age, sessions pinning retention, replay
records/bytes returned, unavailable/expired outcomes, selector work or scan
limits reached, and storage/cluster failures. Avoid unbounded labels such as
session IDs, consumer names, or offsets in metrics. Existing request and
storage metrics do not describe replay cursor progress or pin pressure.

## Reference designs and differences

| Source | Sourced behavior or result | Relevance and limit for Runnel |
| --- | --- | --- |
| [Kafka KafkaConsumer timestamp lookup and seek](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) and [message.timestamp.type](https://kafka.apache.org/42/configuration/topic-configs/) | Kafka documents a first offset at or after a timestamp and distinguishes producer CreateTime from broker LogAppendTime. | Supports an inclusive lower-bound selector and explicit timestamp provenance. Kafka's consumer-position mutation and timestamp configuration do not define Runnel's read-only progress separation or retention completeness. |
| [Pulsar Reader API](https://pulsar.apache.org/api/client/4.2.x/org/apache/pulsar/client/api/Reader.html) | A reader can start at a Unix-millisecond publish-time position. | Shows a time-positioned reader shape. It is not the same contract as a replay session independent from ordinary consumer progress, and the API reference does not settle Runnel's tie and deleted-prefix outcomes. |
| [NATS JetStream pull consumers](https://docs.nats.io/learn/jetstream/pull-consumers) | Pull fetches expose message-count and expiry bounds; delivery uses explicit acknowledgements. | Illustrates bounded pages and acknowledgement-driven delivery. Runnel should independently specify its replay token, session lifetime, retained-history pin, and behavior under ordinary poll/ack concurrency. |
| [The Raft paper](https://raft.github.io/raft.pdf) and [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md) | Replicated state-machine commands are ordered; snapshots compact consensus history while preserving state needed for recovery. | A durable clustered session belongs in committed stream state and snapshots; Raft log compaction alone does not retain replay history or define session semantics. |
| [Spanner OSDI paper](https://research.google.com/archive/spanner-osdi2012.pdf) and [RFC 3339](https://www.rfc-editor.org/rfc/rfc3339) | Spanner models time with an uncertainty interval; RFC 3339 provides an Internet timestamp syntax. | A scalar millisecond value cannot support strong physical-time ordering claims. A text format does not define replay precision, tie, or retention outcomes. |

These sources are reference evidence, not compatibility requirements. See the
[replay selector research](../research/replay-time-selector-semantics.md) for
the inspected timestamp code and a fuller selector comparison, and
[ADR 0024](../decisions/0024-explicit-offset-replay-read.md) for the accepted
one-record contract.

## Staged recommendation and evidence gates

These are outcome gates, not an accepted implementation sequence.

1. **Accept selector and outcome semantics.** Decide whether earliest and a
   snapshot of the ordinary committed checkpoint are useful first selectors.
   If accepting time, explicitly choose inclusive >= T, lowest logical
   matching offset, subsequent append-order traversal, the broker-publish-time
   meaning, tie/no-match outcomes, and the deleted-prefix completeness rule.
   Keep selectors read-only with respect to ordinary progress.
2. **Prefer bounded stateless pages as the next runtime slice if sufficient.**
   Start with selectors whose lookup is bounded by current state, such as an
   offset, earliest retained position, or captured consumer checkpoint. Keep a
   time selector gated on a bounded lookup strategy and a completeness rule
   for deleted prefixes. Capture selector resolution and [earliest, next)
   once. Bound returned records and serialized bytes, return a resumable
   logical cursor, and make repeated page reads safe. Preserve the current
   single-offset API until an explicit protocol decision replaces or subsumes
   it. A page does not pin history and must surface a retention race as
   unavailable.
3. **Add durable sessions only for a demonstrated resume/pinning requirement.**
   Specify idempotent creation, separate session-generation fencing,
   acknowledgement and retry behavior, lease and maximum lifetime, quotas,
   explicit end/expiry outcomes, and the protect/expire interaction before
   implementing session state. Replicate all eligibility-affecting facts and
   include them in recovery snapshots.
4. **Establish resource and failure evidence.** Test equal/backward/forward
   timestamps; offset/time/checkpoint boundaries; no match versus unavailable
   history; deleted nonmonotonic prefixes; concurrent ordinary poll/ack and
   replay; unknown session-create/ack responses; local restart; and clustered
   leader change and snapshot recovery. Bound timestamp lookup work and page
   size. Measure lookup and foreground poll/publish impact over increasing
   retained histories before claiming the index or scan strategy is
   acceptable. Add replay-specific metrics without high-cardinality labels.

The selector/page slice is reasonably implementable in the near term because
both engines already expose logical offsets, record timestamps, a read-only
replay boundary, and explicit unavailable-history outcomes. A durable session
is a larger follow-on: safe pins depend on retention floors and cleanup, and
cluster failover requires versioned replicated state plus snapshot coverage.
The current one-file local log and materialized clustered message vectors
also mean this design does not select a physical lookup or storage strategy.

## Planning and refactor disposition

The [replay backlog outcome](../backlog.md#make-replay-an-explicit-and-safe-consumer-operation)
already covers time, offset, and checkpoint scopes, deterministic fencing,
restart/failover behavior, and observability. The [retention and disk-pressure
design](retention-disk-pressure-plan.md) already identifies replay sessions,
protected pins, expiry, and unavailable-history behavior as coupled open
questions. This note sharpens those boundaries but does not change the intended
outcome or acceptance criteria, so no backlog or tech-debt edit is warranted.
The current single-offset replay is an accepted first slice, not a newly
identified implementation shortcut. No code refactor is appropriate in a
design-only change; consolidating timestamp assignment or adding an index
would constrain semantics and requires a separate decision and recovery
evidence.

No runtime performance change is expected. This document neither implements
an API nor claims lookup or throughput improvement. Runtime tests and
benchmarks do not apply to this documentation-only outcome; design acceptance
and later implementation evidence remain open.

## Unresolved decisions

- Is a stateless bounded page enough for the initial multi-record replay
  capability, or does a user-facing first release require durable resumability?
- Does the ordinary consumer checkpoint selector snapshot the first
  uncommitted offset only, including grouped consumers with out-of-order
  acknowledgements?
- Can the broker maintain enough bounded timestamp summary/index state to
  prove time-selector completeness after prefix deletion without assuming
  monotonic timestamps?
- What exact offset outcome distinguishes an offset at captured next from
  history below earliest, and which result fields expose a retained time
  boundary without implying clock certainty?
- If a session can pin history, what duration, renewal limit, administrative
  expiry, and quota prevent abandoned or high-cardinality sessions from
  exhausting retention or disk capacity?
- What client-provided identity makes session creation safe to retry after an
  unknown response, and how long is that idempotency record retained?
- Does the first durable session permit only one in-flight page, and what
  token/sequence makes its acknowledgement idempotent and separate from
  ordinary acknowledgements?
- How does an explicit future operation that replaces ordinary progress fence
  active and out-of-order acknowledgements? It is outside the replay design
  until separately specified.
