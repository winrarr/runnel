# Delivery bookkeeping and bounded durability batching

- Status: exploratory evidence note; no accepted storage or durability change
- Last reviewed: 2026-09-06
- Baseline: `2569e12013533696b5632f719b381608a9d6096a`
- Related debt: [TD-019](../tech-debt.md#td-019-delivery-bookkeeping-synchronizes-durable-state-per-delivery)
- Related policy boundary: [Durability and delivery policy](durability-delivery-policy.md)

This note records the current durability boundary for delivery bookkeeping and
the evidence needed before batching or group commit is considered. It is not an
implementation plan, a public durability contract, or a decision to weaken the
current synchronous write points. Rust code and tests remain authoritative if
this note becomes stale.

## Question

Polling and acknowledgement update small pieces of consumer state, but both
operations currently pay a durable write and sync. A future implementation may
amortize that cost across several operations, provided that an operation is not
reported as successful before the state it relies on is durable. The useful
comparison is therefore not “one syscall versus several”; it is the complete
cost of safe delivery, acknowledgement, recovery, and ambiguous outcomes.

## Observed baseline

### Local engine

The local engine uses one bounded JSON-lines journal per `(stream, consumer)`.
The current state is a checkpoint plus replayable delivery and acknowledgement
events. The important boundaries are:

| Operation | Durable record | When memory and the caller advance | Current physical work |
| --- | --- | --- | --- |
| Poll or grouped poll | `DeliveryAttempt` | Only after the journal append and sync succeed; the message is then returned and the in-flight lease is installed. | Open/append one JSON line and call `sync_all` for every delivery attempt. |
| Acknowledgement | `Acknowledge` | Only after the append and sync succeed; then progress and in-flight ownership are changed. | Open/append one JSON line and call `sync_all` for every acknowledgement. |
| Terminal dead-letter movement | Durable target append, then `Acknowledge` event | Source progress advances only after the target record and source acknowledgement event succeed. | Two separate stream/state writes; the local outcome remains at least once and can duplicate across an unresolved failure boundary. |
| Journal compaction | Full checkpoint, then journal truncation | The checkpoint is published before the old journal is truncated. | Atomic checkpoint replacement, followed by truncation and sync; this is triggered by the 64 KiB journal bound. |

The event-before-response ordering is visible in the local poll and
acknowledgement paths ([poll](../../crates/runnel-core/src/broker.rs#L233-L334),
[ack](../../crates/runnel-core/src/broker.rs#L338-L408)) and in the journal
writer ([consumer state](../../crates/runnel-core/src/consumer_state.rs#L91-L130)).
The bounded recovery and compaction behavior is implemented in
[journal replay](../../crates/runnel-core/src/consumer_state.rs#L146-L221) and
covered by `consumer_delivery_journal_recovers_committed_events_and_discards_partial_tail`,
`consumer_delivery_journal_stays_within_its_checkpoint_bound`, and the
restart acknowledgement tests in `runnel-core`.

The state cache is only a fast path. Eviction does not change the durable
source of truth, and active delivery ownership, tokens, and deadlines remain
in memory. The async engine adapter sends these synchronous operations through
the bounded per-stream storage executor; that bounds admitted blocking work but
does not amortize the filesystem sync.

### Clustered engine

Clustered delivery bookkeeping is not a second consumer journal. Poll and
acknowledgement are replicated commands in the stream data group. The current
state-machine storage has two relevant levels of batching:

1. The Raft log storage receives an iterator of entries, updates its in-memory
   map, and atomically rewrites and syncs the complete persisted log for that
   append invocation. OpenRaft determines how many entries arrive together; the
   public `publish_batch` operation must not be interpreted as proof of one
   consensus append or one sync.
2. The state-machine apply path receives an entry iterator, appends all entries
   to its framed journal, performs one `sync_data`, and only then applies the
   entries to materialized state. The number of entries per apply call is an
   implementation detail, not a caller-visible durability guarantee.

The relevant boundaries are the [Raft log append](../../crates/runnel-raft/src/log_store.rs#L312-L335),
[state-machine journal persist](../../crates/runnel-raft/src/state_machine_store.rs#L438-L458),
and [state-machine apply](../../crates/runnel-raft/src/state_machine_store.rs#L743-L775)
paths. Snapshots and checkpoint compaction are separate full-state work and do
not turn ordinary delivery acknowledgements into a full materialized-state
replacement. The accepted clustered durability boundary remains quorum commit
and durable state-machine application; see [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md)
and [the clustered outcome design](clustered-outcome-contract.md).

## Correctness invariants to preserve

Any batching or group-commit experiment must preserve these outcomes for both
engines where the operation exists:

- A delivery attempt is durable before a message is returned. Restart must not
  reset the attempt count or make a previously durable attempt disappear.
- Acknowledgement state is durable before progress or in-flight ownership is
  advanced in memory. A failed acknowledgement must leave the old durable
  state authoritative.
- Out-of-order acknowledgements remain monotonic: a higher offset may be
  remembered, but cannot cause an unacknowledged prefix to be skipped.
- Repeated acknowledgement events and replayed journal entries are harmless.
  A partial trailing record is recoverable only under the current documented
  rules; complete corruption must still fail closed.
- Compaction cannot lose the newest event. A crash between publishing the
  checkpoint and truncating the journal may replay old events, but replay must
  not rewind progress, revive an acknowledged attempt, or create an invalid
  state.
- Expired or reassigned grouped deliveries remain fenced. Reducing sync count
  must not make an old delivery token capable of acknowledging a later delivery.
- A dead-letter move keeps its existing local at-least-once ordering and its
  explicit duplicate caveat. A clustered terminal transition remains one
  replicated state-machine outcome.
- A bounded pending batch has an explicit shutdown, timeout, and response-loss
  behavior. “Buffered in memory” is not a durable success point.

## Cost evidence and gaps

The first local journal change replaced a complete consumer-state replacement
on every delivery with compact events and bounded compaction. The repository
records an observed roughly 8% improvement in the focused benchmark; this is
historical evidence, not a current cross-filesystem SLO. The current Criterion
cases measure 100-byte local `publish_poll_ack`, shared-consumer poll/ack, and
keyed shared-consumer paths over 100 messages with 20 samples. Their setup
publishes the messages outside the measured poll/ack interval. They are useful
regression signals, but do not isolate poll from acknowledgement, sync from
serialization, compaction from ordinary events, or filesystem behavior.

The following evidence is still missing:

- repeated, resource-scoped comparisons of per-event sync versus a bounded
  batching candidate, with the batch size, maximum wait, and durability mode
  recorded in the result;
- separate poll, acknowledgement, and end-to-end latency distributions,
  including p99 and p99.9, under one, two, four, and eight workers and both
  independent and shared consumers;
- measured journal compaction frequency and cost as consumer count,
  out-of-order acknowledgement depth, and attempt history grow;
- clustered evidence that records Raft append batch sizes, state-machine apply
  batch sizes, journal sync counts, quorum latency, and recovery replay work;
- controlled comparisons across the filesystems and resource boundaries that
  the supported deployment is expected to use; and
- fault evidence at write, partial-write, sync, response-loss, shutdown, and
  compaction boundaries. The existing tests prove important recovery outcomes,
  but they do not inject a failure inside an OS write or sync and do not by
  themselves establish hardware-level durability.

The rejected `sync_all`-to-`sync_data` experiment is useful negative evidence:
its replay checks passed, but controlled local comparisons did not show a
repeatable improvement. A narrower flush primitive should therefore not be
treated as retirement of TD-019 without new workload evidence.

## Illustrative options, not requirements

The following mechanisms make the trade-offs concrete without selecting an
API or storage layout:

- **Bounded group commit:** collect events up to a byte/count limit or a short
  maximum wait, then write and sync them together. Delivery and acknowledgement
  responses must wait for the batch's durable boundary, and the pending queue
  must remain bounded.
- **Durable event batches:** encode several events in one framed record with a
  recoverable prefix. This may reduce write overhead, but requires explicit
  handling for partial frames, event ordering, and recovery of a batch whose
  response was lost.
- **Transaction-backed state:** atomically persist the state transition and its
  operation identity using a storage primitive that has an appropriate crash
  contract. This could simplify reconciliation, but it does not remove the
  need to measure contention, checkpoint growth, and recovery cost.
- **No change:** retain one synchronous event boundary while the workload is
  small or the measured cost is not material. Correctness and predictable
  recovery are valid reasons to defer optimization.

Changing only `sync_all` to `sync_data`, exposing a flush primitive publicly,
or relying on the current clustered apply iterator as a guaranteed group
commit would not establish a complete solution. Each would leave unanswered
which caller-visible operations are durable and how unknown outcomes are
resolved.

## Outcome and evidence gates

Before changing the local or clustered bookkeeping strategy, pre-register a
comparison against the current baseline with matching message shape, topology,
durability semantics, worker counts, resource limits, and source/build
conditions. The candidate should demonstrate a repeatable material reduction
in delivery overhead on the target workload, with no unacceptable p99/p99.9,
memory, storage-growth, or recovery regression. The materiality threshold must
be stated before the comparison rather than chosen after seeing the result.

The candidate also needs focused fault and restart coverage that compares the
logical state before and after interruption at each durable boundary. For the
cluster, this includes follower restart, leader change, and a lost response to
an acknowledgement or delivery command. For local storage, it includes
checkpoint/journal interruption and bounded shutdown while a batch is pending.
The tests must prove both safe rejection/retry behavior and the absence of
silent progress loss.

If those gates are not met, retain the current synchronous boundaries and keep
TD-019 open. If they are met, record the accepted durability and ambiguity
semantics in an ADR before treating the optimization as a supported behavior.

## Refactor and planning assessment

No safe code refactor is included: the relevant local persistence, delivery
state, and clustered state-machine boundaries are already separated and are
covered by focused tests. A broader refactor toward shared local/clustered
bookkeeping would couple distinct durability models and should remain out of
scope until the workload and failure evidence above justify it. This note is
the focused planning record for that future work; no new tech-debt item is
needed.
