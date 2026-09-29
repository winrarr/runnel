# Delivery bookkeeping and bounded durability batching

- Status: exploratory evidence note; no accepted storage or durability change
- Last reviewed: 2026-09-29
- Baseline: `c1d0766cd0ac3e844e9763e261cebc03d539cd3a` (`origin/main` at review)
- Related debt: [TD-019](../tech-debt.md#td-019-delivery-bookkeeping-synchronizes-durable-state-per-delivery)
- Related policy boundary: [Durability and delivery policy](durability-delivery-policy.md)
- Related research: [Systems performance research for Runnel](../research/systems-performance-research.md)
- Related decisions: [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md), [ADR 0013](../decisions/0013-local-shared-consumer-delivery.md), [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md), [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md), [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md), [ADR 0026](../decisions/0026-semantic-engine-error-classification.md), and [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md)

This note records the current durability boundary for delivery bookkeeping and
the evidence needed before batching or group commit is considered. It is not an
implementation plan, a public durability contract, or a decision to weaken the
current synchronous write points. Rust code and tests remain authoritative if
this note becomes stale.

## Question

Successful delivery attempts, newly accepted acknowledgements, and changed
consumer policies update small pieces of consumer state. Local changes append
and synchronously sync journal events. Empty polls, returns of an already
in-flight delivery, unchanged policy configuration, and already-acknowledged
results do not add a local event. The clustered engine sends poll,
acknowledgement, and policy commands through Raft even when the applied result
is empty, already acknowledged, or an idempotent policy configuration;
consensus-log and state-machine-journal work must therefore be measured
separately from state-changing delivery events.

A future implementation may amortize durable cost across several operations,
provided that an operation is not reported as successful before the state it
relies on is durable. No alternate durability mode, delivery-batch API, or
group-commit policy has been accepted. The useful comparison is the complete
cost of safe delivery, acknowledgement, recovery, and ambiguous outcomes,
including the lock and queueing behavior around each durability boundary.

## Observed baseline

### Local engine

The local engine uses one bounded JSON-lines journal per `(stream, consumer)`.
The current state is a checkpoint plus replayable delivery-attempt,
acknowledgement, and configured-policy events. A first delivery also records
the selected policy snapshot so retries keep that budget after a policy change
or restart. The important boundaries are:

| Operation | Durable record | When memory and the caller advance | Current physical work |
| --- | --- | --- | --- |
| Poll or grouped poll | `DeliveryAttempt { offset, attempt, policy }` | Only after the journal append and `sync_all` succeed; the attempt is then cached, the in-flight lease is installed, and the message is returned. | Append one JSON line and sync per new delivery attempt. A repeated poll for the same active member returns the already durable delivery; an empty poll appends nothing. A threshold crossing may compact before the event append. |
| Acknowledgement | `Acknowledge { offset }` | Only after the append and `sync_all` succeed; then progress and in-flight ownership are changed. | Append one JSON line and sync for each newly accepted acknowledgement. Already-acknowledged results do not append an event. |
| Consumer policy change | `PolicyConfigured { policy }` | Only after the append and `sync_all` succeed; then the new version becomes active. | A changed policy adds one journal event and sync; repeating the same values returns the current version without writing. First assignment persists the selected policy with its delivery-attempt event. |
| Terminal dead-letter movement | Durable target append carrying an internal move identity, then `Acknowledge` event | Source progress advances only after the target record or same-content reconciliation and source acknowledgement event succeed. | The target stream uses the request-aware durable append (`sync_data`); the source consumer journal uses `sync_all`. These remain separate local writes, but retries after a completed target append do not append a second record for the same move identity. |
| Journal compaction | Checkpoint of the current state before the triggering event, then journal truncation and the triggering event append | The checkpoint is written and renamed before the old journal is truncated; the triggering event is not considered successful until its later append and sync both succeed. | A 64 KiB journal bound triggers a temporary checkpoint write and rename, journal truncation plus `sync_all`, and then the normal append and `sync_all`. The checkpoint file is synced before rename, but this path does not sync the parent directory, so directory-entry durability after a crash is not established. |

The event-before-response ordering is visible in the local
[poll and acknowledgement paths](../../crates/runnel-core/src/broker.rs) and
[consumer-state journal writer and replay](../../crates/runnel-core/src/consumer_state.rs).
The target-before-source ordering and internal move identity are visible in
the [dead-letter path](../../crates/runnel-core/src/broker.rs) and
[request-aware stream append](../../crates/runnel-core/src/stream_log.rs).
The local tests in [`runnel-core`](../../crates/runnel-core/src/lib.rs) cover
committed-event replay and a partial tail
(`consumer_delivery_journal_recovers_committed_events_and_discards_partial_tail`),
the 64 KiB checkpoint bound
(`consumer_delivery_journal_stays_within_its_checkpoint_bound`), refusal of an
oversized journal (`oversized_consumer_delivery_journal_is_rejected_on_recovery`),
and durable, isolated, per-delivery policy pinning
(`consumer_policy_is_isolated_durable_and_pinned_per_delivery`).
`acknowledgement_journal_open_failure_preserves_progress_across_restart`
exercises an append-open failure before writing. The
`dead_letter_move_reconciles_after_source_ack_persistence_failure_and_restart`,
`dead_letter_move_recovers_after_partial_target_write_and_restart`,
`dead_letter_move_reconciles_complete_target_write_reported_as_failure`,
`dead_letter_move_retries_after_source_event_sync_failure_and_restart`, and
`dead_letter_move_content_mismatch_is_storage_error_without_acknowledgement`
tests cover partial target frames, a complete target write reported as failed,
source acknowledgement persistence/sync failures, restart reconciliation, and
content mismatch. Their failure hooks model selected boundaries; they do not
inject an actual OS write or sync failure. The tests also do not establish
checkpoint directory-entry durability or kill a process at every compaction
step.

The state cache is only a fast path. Eviction does not change the durable
source of truth, and active delivery ownership, tokens, and deadlines remain
in memory. Local poll and acknowledgement keep the per-stream mutex while
appending and syncing, so operations for one stream serialize across the
filesystem barrier; separate streams can use the bounded storage executor's
independent lanes. The async adapter bounds admitted blocking work but does not
amortize syncs. A restart reconstructs attempts, pinned policies, and
acknowledged progress, but it does not preserve a local in-flight delivery
token or its `Instant` deadline.

### Clustered engine

Clustered delivery bookkeeping is not a second consumer journal. Poll,
acknowledgement, and policy configuration are replicated commands in the
stream data group. Ordinary poll and acknowledgement delegate to the grouped
commands with the consumer acting as their member. These `client_write` calls
append a Raft entry even when the applied result makes no state change, such as
an empty poll, an already-acknowledged result, or an idempotent policy
configuration. The current state-machine storage has two relevant levels of
batching:

1. The Raft log storage receives an iterator of entries, updates its in-memory
   map, and atomically rewrites the complete persisted log file for that append
   invocation. The temporary file and parent directory are synced. OpenRaft
   determines how many entries arrive together; the public `publish_batch`
   operation must not be interpreted as proof of one consensus append or one
   sync.
2. The state-machine apply path receives an entry iterator, appends all entries
   to its framed journal, performs one `sync_data` for a non-empty iterator, and
   only then applies the entries to materialized state. The number of entries
   per apply call is an implementation detail, not a caller-visible durability
   guarantee. A unit test directly applies and recovers a 256-entry iterator;
   it does not establish the batch sizes OpenRaft supplies in a running cluster.

The relevant boundaries are the [Raft log append and atomic persistence](../../crates/runnel-raft/src/log_store.rs),
[cluster command submission](../../crates/runnel-raft/src/engine.rs),
[state-machine journal persistence and apply](../../crates/runnel-raft/src/state_machine_store.rs),
and [command outcomes](../../crates/runnel-raft/src/state_machine.rs). Snapshots
and checkpoint compaction are separate full-state work and do not turn
ordinary delivery acknowledgements into a full materialized-state replacement.
A successful `client_write` response is the current engine's confirmed applied
result, but v1 still has no stage-aware wire response; a lost response remains
ambiguous even when the command may have committed or applied. The accepted
clustered durability boundary remains quorum commit and durable state-machine
application; see [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md),
[ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md),
[ADR 0026](../decisions/0026-semantic-engine-error-classification.md), and [the
clustered outcome design](clustered-outcome-contract.md).

Clustered journal and recovery coverage in
[`runnel-raft`](../../crates/runnel-raft/src/lib.rs) includes
`state_machine_journal_replays_and_discards_a_partial_tail`,
`state_machine_journal_replays_a_retained_batch_after_restart` (a direct
256-entry `apply` call followed by reopen),
`grouped_lease_survives_journal_restart_and_leader_change`,
`persistent_raft_recovers_group_delivery_after_restart`,
`persistent_raft_consumer_policy_is_durable_and_pins_attempts`, and
`persistent_raft_dead_letters_after_the_configured_attempt_limit`. The
three-process [cluster smoke tests](../../crates/runnel-server/tests/cluster_smoke.rs)
also check group delivery through replica restart and reassignment after node
failure. These tests establish replay, policy/attempt persistence, token
fencing, and same-data-group terminal movement; they do not record production
apply batch-size distributions or inject failure between quorum commit,
state-machine journal sync, apply, and response delivery.

## Correctness invariants to preserve

Any batching or group-commit experiment must preserve these outcomes for both
engines where the operation exists:

- A delivery attempt is durable before a message is returned. Restart must not
  reset the attempt count or make a previously durable attempt disappear.
- The first delivery durably pins the selected consumer policy to that offset;
  a later configuration update must not change the retry budget of an existing
  delivery after restart or replay.
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
on every delivery with compact events and bounded compaction. TD-019 records an
observed roughly 8% improvement in its focused benchmark, but this baseline has
no committed machine-readable result or complete workload/resource record for
that comparison. Treat it as historical directional evidence, not a current
cross-filesystem SLO. The September 5, 2026 `sync_all`-to-`sync_data` experiment
passed its replay checks but produced mixed or neutral results across repeated
local acknowledgement and shared-delivery comparisons; the proposed change
was not merged. Its summary is recorded in the [backlog](../backlog.md), but no
raw result artifact is tracked here. Neither result justifies changing the current
durability boundary.

The current [local Criterion definitions](../../crates/runnel-core/benches/broker.rs)
measure 100-byte `publish_poll_ack`, two-member shared-consumer poll/ack,
four-key/four-member shared-consumer poll/ack, and a 64-member unacknowledged
shared-consumer delivery case, each with 20 samples. Setup publishes messages
outside the measured delivery interval. These direct cases measure poll and
acknowledgement together; the 64-member case does not acknowledge. A separate
tail-candidate case measures 100 grouped poll/ack pairs from offset 1,900 in a
2,048-message history. Other local cases cover durable publish, publish
batches, async-engine publish, independent/same-stream concurrent publish,
and retained-history recovery. They are useful surrounding regression signals,
but none isolates poll from acknowledgement, sync from serialization,
compaction from ordinary events, or filesystem behavior.

The real three-node
[cluster benchmark suite](../../scripts/benchmarks/README.md#clustered-baseline)
includes sequential two-member and parallel shared-consumer delivery. It
reports combined poll-and-ack request latency and throughput plus per-node CPU,
memory, and on-disk size samples; the preload is outside the measured interval.
Its opt-in `raft_log_growth` case measures single-message durable publishes,
samples Raft entry counts and persisted file sizes, then exercises an actual
snapshot/purge cycle and follower recovery. Those sampled file sizes are not
bytes-written or sync-count attribution, and that scenario does not exercise
consumer bookkeeping. The opt-in `publish_batch` case batches publishes, not
delivery or acknowledgement. These workloads cannot establish that public
publish batches become one consensus append or one state-machine sync. No raw
benchmark result JSON for these comparisons is tracked in this baseline.

The following evidence is still missing:

- repeated, resource-scoped comparisons of per-event sync versus a bounded
  batching candidate, with the batch size, maximum wait, offered-load model
  (closed-loop or open-loop, including rate-shift scenarios), and durability
  mode recorded in the result. These arrival models can favor different
  batching policies, as discussed in the [systems performance
  research](../research/systems-performance-research.md#durable-batching-amortize-barriers-without-moving-the-success-boundary);
- separate poll, acknowledgement, and end-to-end latency distributions,
  including p99 and p99.9, under one, two, four, and eight workers and both
  independent and shared consumers;
- measured journal compaction frequency and cost as consumer count,
  out-of-order acknowledgement depth, and attempt history grow, including
  whether checkpoint rename and parent-directory durability behave as required
  on supported filesystems;
- clustered evidence that records Raft append batch sizes, state-machine apply
  batch sizes for delivery commands, journal sync counts, quorum latency, and
  recovery replay work;
- controlled comparisons across the filesystems and resource boundaries that
  the supported deployment is expected to use; and
- fault evidence at write, partial-write, sync, response-loss, shutdown, and
  compaction boundaries. The existing tests prove important recovery outcomes,
  including local move-identity reconciliation and clustered journal replay,
  but they do not inject a failure inside an OS write or sync, exercise a
  killed process at each compaction step, or by themselves establish
  hardware-level durability.

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
the focused planning record for that future work. Existing TD-019 and the
[batching backlog outcome](../backlog.md#make-batching-preserve-per-record-outcomes)
already cover measurable bookkeeping and delivery-batching goals, so this
evidence refresh does not warrant another planning item. The local checkpoint
parent-directory-sync gap remains a TD-019 evidence gate; it does not warrant a
separate debt identifier or runtime change in this documentation-only audit.
