# Delivery bookkeeping and bounded durability batching

- Status: exploratory evidence note; no accepted storage or durability change
- Last reviewed: 2026-09-06
- Baseline: `ff987fe19b28c3a3640615d742d4ea7c5df8824c` (`origin/main` at review)
- Related debt: [TD-019](../tech-debt.md#td-019-delivery-bookkeeping-synchronizes-durable-state-per-delivery)
- Related policy boundary: [Durability and delivery policy](durability-delivery-policy.md)
- Related decisions: [ADR 0013](../decisions/0013-local-shared-consumer-delivery.md), [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md), [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md), [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md), and [ADR 0026](../decisions/0026-semantic-engine-error-classification.md)

This note records the current durability boundary for delivery bookkeeping and
the evidence needed before batching or group commit is considered. It is not an
implementation plan, a public durability contract, or a decision to weaken the
current synchronous write points. Rust code and tests remain authoritative if
this note becomes stale.

## Question

Successful delivery attempts and newly accepted acknowledgements update small
pieces of consumer state, but both operations currently pay a durable write and
sync. In the local consumer journal path, empty polls and
already-acknowledged results short-circuit without adding an event. The
clustered path still replicates a `PollGroup` or `AckGroup` command even when
its result is empty or already acknowledged, so command/journal overhead must
be measured separately from state-changing delivery events. A future
implementation may amortize the durable cost across several operations,
provided that an operation is not reported as successful before the state it
relies on is durable. The useful comparison is therefore not “one syscall
versus several”; it is the complete cost of safe delivery, acknowledgement,
recovery, and ambiguous outcomes.

## Observed baseline

### Local engine

The local engine uses one bounded JSON-lines journal per `(stream, consumer)`.
The current state is a checkpoint plus replayable delivery and acknowledgement
events. The important boundaries are:

| Operation | Durable record | When memory and the caller advance | Current physical work |
| --- | --- | --- | --- |
| Poll or grouped poll | `DeliveryAttempt` | Only after the journal append and sync succeed; the message is then returned and the in-flight lease is installed. | Append one JSON line and call `sync_all` for each successful delivery attempt; a threshold crossing may also compact the prior state first. |
| Acknowledgement | `Acknowledge` | Only after the append and sync succeed; then progress and in-flight ownership are changed. | Append one JSON line and call `sync_all` for each newly accepted acknowledgement. Already-acknowledged results do not append an event. |
| Terminal dead-letter movement | Durable target append carrying an internal move identity, then `Acknowledge` event | Source progress advances only after the target record or same-content reconciliation and source acknowledgement event succeed. | The target stream uses the request-aware durable append (`sync_data`); the source consumer journal uses `sync_all`. These remain separate local writes, but retries after a completed target append do not append a second record for the same move identity. |
| Journal compaction | Checkpoint of the state before the triggering event, then journal truncation and the triggering event append | The checkpoint is written and renamed before the old journal is truncated; the triggering event is not considered successful until its later append and sync succeed. | A 64 KiB journal bound triggers a temporary checkpoint write and rename, journal truncation plus sync, and then the normal append. The checkpoint file is synced before rename, but this path does not sync the parent directory, so directory-entry durability after a crash is not established. |

The event-before-response ordering is visible in the local poll and
acknowledgement paths ([poll](../../crates/runnel-core/src/broker.rs#L233-L334),
[ack](../../crates/runnel-core/src/broker.rs#L338-L408)) and in the journal
writer ([consumer state](../../crates/runnel-core/src/consumer_state.rs#L91-L130)).
The target-before-source ordering and internal move identity are visible in
the [dead-letter path](../../crates/runnel-core/src/broker.rs#L505-L549) and
[request-aware stream append](../../crates/runnel-core/src/stream_log.rs#L314-L333).
The bounded recovery and compaction behavior is implemented in
[journal replay](../../crates/runnel-core/src/consumer_state.rs#L146-L221) and
covered by `consumer_delivery_journal_recovers_committed_events_and_discards_partial_tail`,
`consumer_delivery_journal_stays_within_its_checkpoint_bound`,
`oversized_consumer_delivery_journal_is_rejected_on_recovery`, and the
`acknowledged_group_progress_and_retry_state_survive_restart` restart test in
`runnel-core`. The local move-identity and source-ack recovery slice is covered
by `dead_letter_move_reconciles_after_source_ack_persistence_failure_and_restart`
and its same-content and mismatch checks. These tests exercise complete writes,
partial-tail handling, and an injected pre-ack failure; they do not establish
directory-entry durability or inject a failure inside an OS write or sync.

The state cache is only a fast path. Eviction does not change the durable
source of truth, and active delivery ownership, tokens, and deadlines remain
in memory. The async engine adapter sends these synchronous operations through
the bounded per-stream storage executor; that bounds admitted blocking work but
does not amortize the filesystem sync. A restart therefore reconstructs
attempts and acknowledged progress, but it does not preserve an in-flight
delivery token or its `Instant` deadline.

### Clustered engine

Clustered delivery bookkeeping is not a second consumer journal. Group poll and
acknowledgement are replicated commands in the stream data group; the ordinary
clustered acknowledgement path is also represented by the grouped state
machine command with the consumer acting as its member. The current
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
replacement. A successful `client_write` response is the current engine's
confirmed applied result, but v1 still has no stage-aware wire response; a lost
response remains ambiguous even when the command may have committed or
applied. The accepted clustered durability boundary remains quorum commit and
durable state-machine application; see [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md),
[ADR 0026](../decisions/0026-semantic-engine-error-classification.md), and [the
clustered outcome design](clustered-outcome-contract.md).

Clustered journal and recovery coverage includes
`state_machine_journal_replays_and_discards_a_partial_tail`,
`state_machine_journal_replays_a_retained_batch_after_restart`,
`grouped_lease_survives_journal_restart_and_leader_change`,
`persistent_raft_recovers_group_delivery_after_restart`, and
`persistent_raft_dead_letters_after_the_configured_attempt_limit` in
`runnel-raft`. The real-process `cluster_smoke` coverage additionally checks
group delivery through replica restart and reassignment after node failure.
These tests establish replay, attempt persistence, token fencing, and
same-data-group terminal movement; they do not measure apply batch sizes or
inject a failure between quorum commit, state-machine journal sync, apply, and
response delivery.

## Correctness invariants to preserve

Any batching or group-commit experiment must preserve these outcomes for both
engines where the operation exists:

- A delivery attempt is durable before a message is returned. Restart must not
  reset the attempt count or make a previously durable attempt disappear.
- Acknowledgement state is durable before progress or in-flight ownership is
  advanced in memory. A failed acknowledgement must leave the old durable
  state authoritative. A post-write or post-apply response loss remains an
  unknown attempt, not evidence that the acknowledgement was rejected.
- Out-of-order acknowledgements remain monotonic: a higher offset may be
  remembered, but cannot cause an unacknowledged prefix to be skipped.
- Repeated acknowledgement events and replayed journal entries are harmless.
  A partial trailing record is recoverable only under the current documented
  rules; complete corruption must still fail closed.
- Compaction cannot make a triggering event appear successful before its own
  durable append. A crash after checkpoint publication and before journal
  truncation may replay old events; a crash after truncation and before the
  triggering event append must leave that event unapplied. Replay must not
  rewind progress, revive an acknowledged attempt, or create an invalid state.
- Expired or reassigned grouped deliveries remain fenced. Reducing sync count
  must not make an old delivery token capable of acknowledging a later delivery.
- A dead-letter move keeps its existing local at-least-once ordering and its
  explicit duplicate caveat. A clustered terminal transition remains one
  replicated state-machine outcome.
- A bounded pending batch has an explicit shutdown, timeout, and response-loss
  behavior. “Buffered in memory” is not a durable success point.

## Cost evidence and gaps

The first local journal change replaced a complete consumer-state replacement
on every delivery with compact events and bounded compaction. The register
records an observed roughly 8% improvement in the focused benchmark, but no raw
machine-readable artifact or complete workload/resource record for that result
is committed in this repository. Treat it as historical directional evidence,
not a current cross-filesystem SLO. The current Criterion suite measures
100-byte local `publish_poll_ack`, two-member shared-consumer poll/ack,
four-key/four-member shared-consumer poll/ack, and a 64-member unacknowledged
shared-consumer delivery case, all with 20 samples. Setup publishes messages
outside the measured delivery interval. Separate local benchmarks cover durable
publish, publish batches, async-engine publish, independent/same-stream
concurrent publish, and retained-history recovery; they are useful surrounding
regression signals but not direct delivery-bookkeeping evidence. None of these
benchmarks isolates poll from acknowledgement, sync from serialization,
compaction from ordinary events, or filesystem behavior.

The following evidence is still missing:

- repeated, resource-scoped comparisons of per-event sync versus a bounded
  batching candidate, with the batch size, maximum wait, and durability mode
  recorded in the result;
- separate poll, acknowledgement, and end-to-end latency distributions,
  including p99 and p99.9, under one, two, four, and eight workers and both
  independent and shared consumers;
- measured journal compaction frequency and cost as consumer count,
  out-of-order acknowledgement depth, and attempt history grow, including
  whether checkpoint rename and parent-directory durability behave as required
  on supported filesystems;
- clustered evidence that records Raft append batch sizes, state-machine apply
  batch sizes, journal sync counts, quorum latency, and recovery replay work;
- controlled comparisons across the filesystems and resource boundaries that
  the supported deployment is expected to use; and
- fault evidence at write, partial-write, sync, response-loss, shutdown, and
  compaction boundaries. The existing tests prove important recovery outcomes,
  including local move-identity reconciliation and clustered journal replay,
  but they do not inject a failure inside an OS write or sync, exercise a
  killed process at each compaction step, or by themselves establish
  hardware-level durability.

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
the focused planning record for that future work. The local checkpoint
parent-directory-sync gap is recorded above as a durability evidence gate
within TD-019; it does not warrant a separate debt identifier or a runtime
change in this documentation-only audit.
