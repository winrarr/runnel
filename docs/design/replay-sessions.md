# Replay selectors and bounded sessions

- Status: exploratory design for pages and sessions; ADRs 0038 and 0042 accept one-shot time-selector semantics and its index/recovery contract; runtime and wire shape remain open
- Last reviewed: 2026-10-06
- Baseline inspected for this update: `da6b14e72ce75317ad0fa3fe05f91a28026b67b2`
- Primary evidence class: design/research; secondary: correctness/reliability, storage/recovery
- Scope: bounded replay selectors, paging, and optional durable replay sessions
- Related: [replay backlog outcome](../backlog.md#make-replay-an-explicit-and-safe-consumer-operation), [ADR 0024](../decisions/0024-explicit-offset-replay-read.md), [ADR 0038](../decisions/0038-timestamp-based-replay-selector.md), [ADR 0042](../decisions/0042-recoverable-replay-time-index.md), [replay time-selector research](../research/replay-time-selector-semantics.md), and [retention and disk-pressure design](retention-disk-pressure-plan.md)

This note explores how to extend the accepted one-record offset read into
bounded replay without coupling replay to a consumer's ordinary checkpoint. It
compares selector reads, stateless bounded pages, and durable sessions. It
does not accept wire fields, retention policy, or a session state format. Any
API shapes below are illustrative. ADR 0042 accepts the derived index for the
one-shot timestamp selector; this design keeps pages and sessions open.

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

The current inclusive offset selector is accepted in ADR 0024, and the
one-shot broker-publish-time selector is accepted in ADR 0038. Earliest
retained offset and a snapshot of the named consumer's current committed
checkpoint remain possible future selectors. Each selector resolves to one
logical start offset against the captured view. A checkpoint selector would
read ordinary committed offset once at session/page creation; subsequent poll
or acknowledgement activity would not change that replay position or rewind
ordinary progress.

[ADR 0038](../decisions/0038-timestamp-based-replay-selector.md) accepts a
one-shot timestamp selector for the lowest logical offset in the captured
retained view whose stored <code>published_at_ms</code> is greater than or
equal to T. The comparison is inclusive; ties resolve to the lowest offset.
After selection, traversal follows logical append order, so a later record may
have a timestamp below T. This is a replay start selector, not an event-time
filter. The source is the existing broker-assigned Unix-millisecond field, not
caller event time, commit time, or a globally ordered clock.

The accepted outcomes distinguish a complete view with no matching record
(including an empty stream or T later than all visible records) from a view
whose deleted prefix could contain an earlier match. The first is
<code>no_match</code>; the latter is <code>history_unavailable</code>. For
contiguous-prefix deletion, complete metadata for the maximum timestamp in the
deleted prefix proves completeness when that maximum is less than T. If the
maximum is at least T, or the summary is missing or untrustworthy, the earliest
match cannot be established. A retention design with deletion holes needs an
equivalent completeness proof.

The semantic contract and derived-index design are accepted, but runtime
lookup remains gated. [ADR 0042](../decisions/0042-recoverable-replay-time-index.md)
selects cumulative prefix-maximum checkpoints at 256-record boundaries. The
monotone summaries identify the first block that can match despite timestamp
regressions; the engine scans at most that block in logical order. Local and
clustered engines rebuild the index from authoritative log/state, and
clustered snapshot installation rebuilds from retained messages. Index
metadata is one compact summary per block and therefore still grows with
retained history; its memory, startup, and foreground impact requires runtime
measurement. See ADR 0042 for the exact update, bounds, retention, and
acceptance contract.

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
progress, ordinary attempts or retry state, in-flight delivery tokens, or key
gates. If a separate progress-replacement operation is ever accepted, it must
explicitly fence ordinary deliveries and define what happens to out-of-order
acknowledgements.

For the initial one-unacknowledged-page model, a useful candidate transition
is to reserve the current page and a session-scoped delivery token durably
before returning it. A concurrent poll, or a retry after a lost response,
returns that same unacknowledged page and token while its lease is valid; it
must not reserve a second page. An acknowledgement identifies the session
generation and reserved page/token. Only a successful durable acknowledgement
clears the reservation and advances that session's cursor. Repeating that
acknowledgement is idempotent, while an expired, replaced, or already-fenced
token cannot advance progress. This keeps uncertain poll and ack outcomes
retryable without involving ordinary consumer delivery state.

Any reset or seek operation would also need a new generation. Its race with
acknowledgement is resolved by the same session serialization point: ack first
means that ack applies before reset; reset first fences the old receipt. The
smallest initial API can omit reset and require close plus new-session
creation. Close, expiry, and retention invalidation likewise fence outstanding
receipts before releasing a pin. These are candidate semantics, not selected
API operations.

Session creation changes durable state and can time out after commit but before
the response reaches the client. It therefore needs idempotent creation, such
as a caller-supplied session key, or another way for a client to recover the
created identity. Acknowledgements should likewise be retry-safe for a given
session generation and replay sequence. Session limits, maximum lifetime,
renewal policy, maximum concurrent sessions per stream/consumer, and cleanup
work all need hard bounds. Persisted session inventory must be complete for
retention decisions; a bounded in-memory cache alone cannot prove which
sessions pin history. Bound per-stream session count and state bytes, returned
page bytes, and selector work as well as lifetime and concurrent sessions. A
lease by itself is not a total lifetime bound if unlimited renewal is allowed.

## Retention and unavailable history

The logical retained range is half-open [earliest, next). Offset selection
below earliest is unavailable; an offset at or above captured next is not
present in that view. The current protocol's offset replay uses
<code>history_unavailable</code> for absent offsets and returns the available
offset range. The time-selector outcomes for no current match and incomplete
deleted-prefix history are accepted by ADR 0038. A session whose next required
offset falls below the retention floor must report explicit unavailability or
expiry; it must not advance to a later retained record or report normal
end-of-session.

The retention proposal's <code>protect</code> policy lets active replay sessions
pin their earliest unread history, while <code>expire</code> can end replay
eligibility and must expose the new boundary. These are candidate policies,
not current behavior. The pin follows the earliest unacknowledged offset, not
the highest delivered or acknowledged offset. Creation and floor advancement
must share one logical order: under protect, creation first establishes its
pin before a later floor transition; if the floor transition wins first, a
session whose start is now unavailable fails explicitly. Under expire, the
floor transition must durably invalidate the affected session and fence its
outstanding receipt before reporting expiry. A bounded session lease and
explicit close can limit pins, but cleanup must wait for the durable
expiry/fence to win the race with a fetch or ack. Physical deletion may lag a
committed logical floor without making deleted records eligible again. The
selected retention policy must report the outcome to the session caller.

For local and clustered engines, these races need the same logical outcome.
The local engine would serialize transitions with the stream and persist each
state change before success. The clustered engine would order equivalent
transitions through the stream's replicated state and include session and
retention eligibility in snapshots. This is a semantic requirement, not a
choice of journal layout, command shape, or physical pin representation.

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

The timestamp selector semantics are accepted by ADR 0038. These are
implementation and later session gates, not open selector choices.

1. **Implement the accepted one-shot time selector.** Preserve the current
   offset operation. Resolve one time-selected record against a single
   `[earliest, next)` view, distinguish `no_match` from
   `history_unavailable`, and never touch the ordinary checkpoint. Implement
   the cumulative prefix-maximum index, rebuild it from each engine's
   authoritative state, and meet the query and retained-prefix gates in ADR
   0042. Add complete deleted-prefix metadata when retention is implemented;
   until its completeness is known, fail closed.
2. **Add stateless bounded pages only after selector resolution is stable.**
   Capture selector resolution and `[earliest, next)` once. Bound returned
   records and serialized bytes, return a resumable logical cursor, and make
   repeated page reads safe. A page does not pin history and must surface a
   retention race as unavailable.
3. **Add durable sessions only for a demonstrated resume/pinning requirement.**
   Specify idempotent creation, separate session-generation fencing,
   acknowledgement and retry behavior, lease and maximum lifetime, quotas,
   explicit end/expiry outcomes, and the protect/expire interaction before
   implementing session state. Replicate all eligibility-affecting facts and
   include them in recovery snapshots.
4. **Establish resource and failure evidence.** Test equal/backward/forward
   timestamps; selector boundaries; no match versus unavailable history;
   deleted nonmonotonic prefixes; concurrent ordinary poll/ack and replay;
   local restart; and clustered leader change and snapshot recovery. Verify
   the index's worst-case work for sparse matches and no match. Measure lookup,
   index space/rebuild, and foreground poll/publish impact over increasing
   histories before deployment. Add replay-specific metrics without
   high-cardinality labels only when operationally justified.

The selected one-shot timestamp behavior is a near-term outcome, but not a
scrape-time scan: bounded lookup requires an offset-order-preserving timestamp
index and recovery evidence in each engine. A durable session remains a
larger follow-on because safe pins depend on retention floors and cleanup,
and cluster failover requires versioned replicated state plus snapshot
coverage. The current one-file local log and materialized clustered message
vectors remain relevant scalability constraints under TD-002 and TD-010.

## Planning and refactor disposition

The [replay backlog outcome](../backlog.md#make-replay-an-explicit-and-safe-consumer-operation)
now links the accepted selector semantics and carries their bounded lookup
and recovery gates. [ADR 0038](../decisions/0038-timestamp-based-replay-selector.md)
settles the previously open timestamp source, comparison, tie, regression,
no-match, and deleted-prefix behavior; [ADR 0042](../decisions/0042-recoverable-replay-time-index.md)
settles the first selector's index and recovery contract. Replay sessions,
protected pins, expiry, and page semantics remain open in the [retention and
disk-pressure design](retention-disk-pressure-plan.md). Existing
[TD-002](../tech-debt.md#td-002-one-file-and-a-startup-scan-per-local-stream)
and [TD-010](../tech-debt.md#td-010-clustered-state-materializes-complete-retained-history)
cover local cold scans and clustered full-history traversal; both remain open
for runtime and resource evidence. No runtime refactor belongs in this
documentation-only change; storage growth and session decisions remain separate.

No runtime performance change is expected. This document neither implements
an API nor claims lookup or throughput improvement. Runtime tests and
benchmarks do not apply to this documentation-only outcome; design acceptance
and later implementation evidence remain open.

## Unresolved decisions

- Is a stateless bounded page enough for multi-record replay, or does a
  user-facing release require durable resumability?
- Should a future consumer-checkpoint selector snapshot the first
  uncommitted offset only, including grouped consumers with out-of-order
  acknowledgements?
- Is the selected sparse index's update, memory, and rebuild cost acceptable
  over supported retained-history sizes? Runtime evidence is still required.
- How will the selected deleted-prefix timestamp maximum and retained floor be
  persisted and included in snapshots when retention is implemented?
- What exact offset outcome should distinguish an offset at captured `next`
  from history below `earliest` for future multi-record pages?
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
