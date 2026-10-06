# Application-aware retry and dead-letter provenance

- Status: ADR 0027's consumer-scoped policy is implemented; ADR 0033 accepts a fixed retry-delay contract that is not implemented; other extensions remain proposals
- Last reviewed: 2026-10-06
- Baseline reviewed for this decision: `c3a894b6d88a40245c1116e2c5006b94f5573aee`
- Reading guide: [design-note conventions](README.md)
- Related debt: [TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse)
- Related outcome: [Make retry policy application-aware](../backlog.md#make-retry-policy-application-aware)
- Related decisions: [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md), [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md), [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md), [ADR 0026](../decisions/0026-semantic-engine-error-classification.md), [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md), and [ADR 0033](../decisions/0033-fixed-consumer-retry-delay.md)
- Related boundaries: [clustered outcomes](clustered-outcome-contract.md), [durability and delivery policy](durability-delivery-policy.md), and [delivery bookkeeping](td-019-delivery-bookkeeping.md)
- Companion design: [Dead-letter recovery across durable boundaries](dead-letter-recovery.md)

This note records the implemented consumer-scoped policy and the accepted
fixed retry-delay contract, then explores other follow-on outcomes tracked by
TD-018. ADR 0027 and current source define the implemented timeout and attempt
limit. ADR 0033 accepts a fixed delay as the next policy extension, but no
runtime behavior implements it yet. Exponential/jittered backoff, failure
dispositions, provenance, redrive, and cross-boundary movement remain
proposals with separate design and evidence needs.

For current behavior, Rust code and tests are authoritative. The accepted
consumer-policy decision is [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md),
and [the architecture note](../architecture.md) summarizes implemented
delivery behavior. ADR 0033 defines the accepted `retry_delay_ms` policy
extension and engine-specific time behavior. Other policy shapes, state
transitions, field names, and stage names below are illustrative options and
candidate evidence gates; they are not accepted APIs or state formats.

Retry is one policy axis among durable-write, delivery, retention, and overload
policy. The [durability and delivery policy boundary](durability-delivery-policy.md)
owns that separation; this note focuses on delivery attempts, terminal movement,
and provenance. The [clustered outcome contract](clustered-outcome-contract.md)
also separates a safe retry classification from evidence about how far an
operation progressed. A transport error or a delivery token must not be used as
an implicit policy or stage signal.

## Accepted and implemented first slice

ADR 0027 accepts a durable, consumer-scoped timeout and attempt-limit policy.
The implementation has this bounded surface:

1. The provisional v1 protocol exposes `configure_consumer(stream, consumer,
   ack_timeout_ms, max_delivery_attempts)` and `inspect_consumer(stream,
   consumer)`. Configuration applies to an existing stream. Polling still
   lazily creates an unconfigured consumer; inspection reports that consumer's
   broker-wide fallback with `configured = false` and version zero.
2. A configured policy contains `ack_timeout_ms` from zero through seven days
   and an optional positive `max_delivery_attempts`. A zero attempt limit is
   rejected. The broker-wide values remain the fallback for consumers that
   have not been configured.
3. Repeating the same values is idempotent. Changed values advance the
   monotonic policy version. Each source record pins the complete policy
   snapshot on its first persisted assignment, so a later configuration
   change does not alter an existing attempt budget.
4. The local engine journals policy and per-offset snapshots with consumer
   state. The clustered engine stores them with replicated per-stream
   group-consumer state; ordinary polling uses the same grouped state path.
5. `ack_timeout_ms` controls the active acknowledgement lease; after expiry,
   a later poll may redeliver the record. A local restart also makes an
   unacknowledged record eligible because its lease is process-local. Attempt
   exhaustion retains the existing derived dead-letter behavior. It does not
   add a separately scheduled backoff or richer terminal action.

This accepted slice does not include exponential or jittered backoff, explicit
retry/dead-letter dispositions, `hold`, named or cross-group targets,
provenance-bearing dead-letter records, or redrive. Those remain proposals
with separate design and evidence gates.

ADR 0033 accepts a fixed `retry_delay_ms` from zero through seven days,
separate from the acknowledgement lease and pinned in each offset's first
assignment policy snapshot. Zero is the default, preserving current behavior.
The delay begins when a committed poll or stale-acknowledgement operation
first observes lease expiry; `retry_not_before` is that observation time plus
the pinned delay. Late observation starts a fresh full delay in both engines.
The contract is not implemented yet. A deadline already persisted locally or
in replicated clustered state survives restart; when restart precedes durable
expiry observation, the first later observation starts the delay. See
[ADR 0033](../decisions/0033-fixed-consumer-retry-delay.md) for
terminal-attempt, stale-ack, ordering, and clock semantics.

ADR 0027's two operations are additive to the current v1 protocol and were
added without runtime capability discovery. The
[protocol compatibility design](protocol-compatibility.md) records that an
older v1 server may return generic `invalid_request` for an operation it does
not recognize; a v1 declaration alone does not guarantee support. That note
owns the general v1/v2 evolution questions. Authorization remains open.

## Broader future outcome and non-goals

The outcome in this section describes the remaining extension of TD-018.
Per-consumer timeout and attempt-limit selection is implemented, and the fixed
retry-delay contract is accepted but not implemented. Exponential/jittered
backoff, richer terminal handling, dead-letter provenance, and redrive remain
unaccepted goals; examples here do not commit to implementing them.

An application should be able to give each durable consumer a policy that
matches its failure mode. A short-lived interactive worker may want a small
bounded retry budget and quick isolation of poison messages. A dependency
worker may need exponentially spaced retries. An event fan-out consumer may
choose unlimited retry while its stream retention policy protects history.
These choices must not require knowledge of local files, Raft groups, nodes,
leaders, or ownership placement.

When a record is dead-lettered, an operator or recovery tool should be able to
answer, from the record and normal broker inspection APIs:

- which stream incarnation, consumer, and logical source offset produced it;
- how many source delivery attempts occurred and why the terminal transition
  happened;
- which policy version made that decision;
- whether this is an original dead letter or a redrive of an earlier one; and
- which stable identity can be used to retry an uncertain move without
  appending another logical copy.

The guarantee remains at least once. A source delivery, a dead-letter record,
and a redriven record can each be observed more than once after a process,
leader, network, or client failure. The broker may make durable broker-side
moves duplicate-safe, but it cannot make an arbitrary application side effect
exactly once. Applications must use the source identity or another idempotency
key when an external effect cannot tolerate duplicates.

This design does not attempt to provide:

- exactly-once application processing or a transaction with an arbitrary
  external database, HTTP service, or side effect;
- a push consumer, a general scheduler, or a second public topology model;
- per-message policy blobs large enough to contain unbounded failure history;
- a destructive drop policy hidden behind a retry limit;
- consumer-specific routing that silently bypasses normal stream fan-out; or
- a final answer to the clustered lease-clock problem tracked by
  [TD-020](../tech-debt.md#td-020-clustered-delivery-leases-use-absolute-wall-clock-deadlines).

## Baseline and design boundary

The [durability and delivery policy note](durability-delivery-policy.md)
provides the cross-cutting current-state overview. This table narrows it to
retry and terminal delivery behavior; its open policy axes remain unaccepted.

The current behavior is a useful compatibility baseline:

| Area | Current behavior | Unaccepted TD-018 questions |
| --- | --- | --- |
| Policy selection | Consumers are created implicitly by polling. Broker-wide `--ack-timeout-ms` and optional `--max-delivery-attempts` remain the fallback; configured consumers have durable per-consumer values. | Whether future policy dimensions can be added without exposing placement or making policy state unbounded. |
| Attempts | The first assignment is attempt 1. Repeating a poll while the delivery is still in flight does not increment it. An expired delivery is assigned again with a persisted, higher attempt. | Any extension should preserve per-stream, per-consumer, per-offset accounting rather than count polls, connections, or transient members. |
| Delay | The acknowledgement timeout sets the active lease. In the current implementation, expiry is observed on a later poll and has no additional delay. An unacknowledged local record is immediately eligible after restart because the lease is process-local. Clustered deadlines are leader-sampled absolute timestamps, and each data group persists an observation floor that prevents backward expiry evaluation but not clock skew, forward jumps, or idle/no-command expiry. | ADR 0033 accepts a separate fixed delay beginning at the first durable observation of lease expiry. Both engines persist `retry_not_before = observation_time + pinned_delay`; late observation starts a full delay. A persisted deadline survives restart/leadership change; if restart precedes durable expiry observation, the first later observation starts it. TD-020 and local wall-clock behavior limit timing guarantees. |
| Local dead letter | `runnel-core` appends to a derived dead-letter stream, then persists source progress. The internal move identity and strict same-content reconciliation can reuse a completed target append during retry or reopen. A target/source crash boundary remains at least once; active ownership and deadlines are not durable. | Whether bounded public provenance or other recovery metadata is needed beyond the internal move identity described in [dead-letter recovery](dead-letter-recovery.md). |
| Clustered dead letter | `runnel-raft` commits the original key/payload copy and source progress in one stream data-group state-machine transition. Group ownership, attempts, deadlines, the lease-clock floor, and fencing state are replicated, but the automatic clustered copy has no provenance-bearing move identity. | Whether future named or cross-group targets need a transaction or reconciliation design; current same-group behavior does not decide that boundary. |
| Delivery API | The provisional protocol has `poll`, `poll_group`, `ack`, `ack_group`, and the additive `configure_consumer` / `inspect_consumer` operations, with an attempt number and opaque grouped-delivery token. It has no explicit negative acknowledgement, provenance field, or redrive operation. | Any future operations or optional metadata need a compatibility decision. ADR 0027's operations do not require capability negotiation. |
| Observability | Health exposes process-lifetime redelivery and dead-letter counters. `/metrics` additionally exposes request, traffic, admission, storage, uptime, health, and clustered snapshot activity, but no retry cause, schedule, provenance, or consumer catalogue. | Which bounded retry, terminal-move, redrive, or blocked-target signals would be useful and supportable without unbounded stream/consumer labels. |

The local attempt and move paths are visible in [`Broker::poll_group`](../../crates/runnel-core/src/broker.rs#L233), [`Broker::dead_letter_record`](../../crates/runnel-core/src/broker.rs#L505), [`dead_letter_move_id`](../../crates/runnel-core/src/lib.rs#L198), and the durable [`ConsumerState`](../../crates/runnel-core/src/consumer_state.rs#L13). Active ownership is maintained by the process-local [`DeliveryState`](../../crates/runnel-core/src/delivery_state.rs#L72), including a due-deadline index and a bounded 1,024-entry state cache. The clustered state shape and grouped transition are visible in [`SnapshotState`](../../crates/runnel-raft/src/state_machine.rs#L182), [`apply_group_poll`](../../crates/runnel-raft/src/delivery.rs#L49), and the persisted [lease-clock floor](../../crates/runnel-raft/src/delivery.rs#L330). The current public message has only `delivery_attempt` and no provenance object ([engine contract](../../crates/runnel-engine/src/lib.rs#L73), [protocol response](../../crates/runnel-protocol/src/lib.rs#L247)).

At the engine boundary, [ADR 0026](../decisions/0026-semantic-engine-error-classification.md)
now provides backend-independent `BrokerError::kind()` and
`BrokerError::outcome()` classifications. This is an accepted source-level
failure boundary, not operation-stage evidence: the provisional v1 server and
wire response still do not tell an application whether a mutation crossed a
proposal, commit, apply, or response boundary.

### Current evidence snapshot

The accepted slice is implemented and has focused engine and server coverage:

- The reusable engine contract covers first delivery, redelivery after lease
  expiry, stale acknowledgement fencing, scoped key exclusion, and restart
  recovery in [`crates/runnel-core/tests/engine_contract.rs`](../../crates/runnel-core/tests/engine_contract.rs).
- Local [`consumer_policy_is_isolated_durable_and_pinned_per_delivery`](../../crates/runnel-core/src/lib.rs)
  covers configure/inspect, independent consumers, an in-flight policy update,
  the legacy fallback, and configured-policy persistence across reopen. Other
  local tests cover attempt limits, derived dead-letter movement, reconciliation
  after restart, and journal recovery. The journal append and sync boundaries
  are recorded in [delivery bookkeeping](td-019-delivery-bookkeeping.md).
- Clustered [`persistent_raft_consumer_policy_is_durable_and_pins_attempts`](../../crates/runnel-raft/src/lib.rs)
  covers replicated consumer policy, shared member behavior, version pinning,
  and persistence after reopening the persistent engine. It is a persistent
  single-node Raft test, not a multi-process leader-transfer test.
- The real-server [`typed_client_configures_consumer_retry_policy_and_dead_letters`](../../crates/runnel-server/tests/client_path.rs)
  test exercises the public configure/inspect path, attempt limit, and derived
  dead-letter behavior through a broker process. Three-process
  [cluster smoke tests](../../crates/runnel-server/tests/cluster_smoke.rs)
  cover shared-consumer reassignment and stale-token fencing after node
  failure. The focused
  [`three_process_cluster_transfers_consumer_policy_and_delivery_snapshot_after_leader_failure`](../../crates/runnel-server/tests/cluster_smoke.rs#L1243)
  test configures and pins a policy on an unacknowledged delivery, changes the
  current policy, stops the observed group leader, and verifies that a
  different leader inherits the current policy while the delivery retains its
  pinned attempt limit. The test identifies exactly one leader before and
  after failure through read-only peer `InspectConsumer` probes.

At the earlier baseline
`2a8cae863e4970d1c8ba00271af0d45a80206dcd`, source code stored the configured
policy and per-offset snapshot in replicated group-consumer state, but the
public multi-process test gap remained. The leader-transfer test above is now
part of baseline `eaf93da579a86db01ceb452a2514d426934f4775`, closing that
evidence gap. The existing
[retry-policy backlog criterion](../backlog.md#make-retry-policy-application-aware)
already requires policy state to transfer when ownership moves between nodes,
so this review does not create another outcome for the same goal.

One adjacent implementation detail is relevant to the accepted slice's
non-recursive dead-letter behavior: local and clustered polling keep
separate predicates for recognizing generated targets. The current boundary
tests show equivalent behavior for ordinary suffix targets, maximum-length
hashed targets, and user streams that merely share the hash prefix
([local tests](../../crates/runnel-core/src/lib.rs#L954) and
[cluster tests](../../crates/runnel-raft/src/lib.rs#L319)). A shared helper could
reduce future implementation drift, but the earlier local recursion mismatch
is no longer an observed runtime gap.

## Remaining candidate future design (nonbinding)

Everything in this section and the following future-design sections is an
exploration except for the fixed-delay contract explicitly accepted in ADR
0033. This material is not an implementation plan or execution order.
Imperative wording describes candidate properties to evaluate, not
requirements for current code. Types and field names outside the accepted
`retry_delay_ms` value are illustrative and do not reserve a public API or
storage format.

### Policy scope and selection

Retry policy belongs to the durable consumer, not the stream, member, node, or
delivery token. All members of one shared consumer inherit the same policy;
independent consumer names may select different policies for the same stream.
This preserves the existing distinction between fan-out consumers and workers
sharing one consumer.

The accepted `configure_consumer` and `inspect_consumer` operations are
defined by ADR 0027. ADR 0033 extends their policy with `retry_delay_ms`; the
remaining fields in this sketch are only vocabulary for future dimensions:

```text
RetryPolicy {
    version: opaque durable policy version,
    ack_timeout: bounded duration,
    max_attempts: optional positive u32,
    retry_delay_ms: bounded duration, // accepted by ADR 0033; not implemented
    on_exhausted: hold | dead_letter,
    dead_letter_target: derived | named stream,
    explicit_retry: disabled | enabled,
    explicit_dead_letter: disabled | enabled,
}
```

Possible constraints for later policy extensions, subject to design review,
include:

1. `ack_timeout` is the maximum time a consumer has to acknowledge an active
   assignment. ADR 0033 accepts `retry_delay_ms` as an independent fixed gap
   beginning when an operation first durably observes that lease has expired.
2. `max_attempts` counts assignments, inclusively. `None` means unlimited
   retry, preserving the current default. Zero is invalid.
3. `on_exhausted = hold` keeps the record pending at the source and makes the
   policy terminal state visible; it never advances source progress by
   dropping the record. `on_exhausted = dead_letter` requires a valid target.
4. The default target remains `<source-stream>.dead-letter`, so current
   tooling and stream naming continue to work. A named target is an explicit
   choice and uses normal stream validation. It must not be inferred from a
   filesystem path or a physical group.
5. Automatically generated dead-letter streams are not recursively
   dead-lettered. A dead-letter record may be consumed, acknowledged, and
   explicitly redriven, but a failed dead-letter inspection consumer does not
   create an unbounded `.dead-letter.dead-letter` chain.
6. `explicit_retry` and `explicit_dead_letter` are future additive
   dispositions. A current client can still rely on lease expiry, while a
   capable application can classify a known transient or permanent failure.

The policy is selected when a consumer is created or explicitly configured.
The policy definition and its version are durable. A poll request must not
carry an arbitrary policy that a follower or a new leader can interpret
differently. In the cluster, policy state should be part of the replicated
consumer state or a referenced replicated metadata record; process startup
flags remain only a legacy fallback.

The current local consumer state is created lazily and its in-memory cache is
only a bounded fast path; there is no complete durable consumer catalogue.
ADR 0027 already defines configuration and inspection for existing streams:
configuration persists policy metadata, while inspection of an unconfigured
consumer returns the broker-wide fallback. Any future complete consumer
catalogue or alternate absent-consumer semantics would need separate review.

ADR 0027 accepts a version fence: the full policy snapshot is pinned when a
source record receives its first persisted assignment and remains selected
through retry or terminal movement. ADR 0033 includes the fixed delay in that
snapshot. A later configuration applies to records without attempt state. An
administrative reset is one possible later operation, but no reset command or
semantics are accepted.

Existing consumers with no explicit policy continue using the broker-wide
settings and are marked as legacy in inspection output. Existing dead-letter
records continue to expose their original key and payload. They have no
retroactively recoverable provenance, so a new field must report provenance as
absent or incomplete rather than guessing from the target offset.

### Attempts and state transitions

The state sketch below includes the accepted fixed-delay transition. It is a
target contract from ADR 0033, not current runtime state; `held` and other
terminal actions remain unaccepted:

```text
eligible
   -> in_flight(attempt = 1)
   -> acknowledged
   -> retry_scheduled(attempt = 1, retry_not_before = t)
   -> in_flight(attempt = 2)
   -> ...
   -> exhausted -> held | dead_lettered
```

The following rules make the state observable and portable between engines:

- An attempt is a committed assignment, not a poll call. Returning the same
  unexpired delivery to the same member does not create an attempt.
- Attempt state is keyed by `(stream incarnation, consumer, source offset)`.
  A shared-consumer member is transient ownership and is not part of the
  retry budget.
- The attempt number is persisted before a local message is returned and is
  committed with the grouped Raft command before a clustered response is
  considered successful.
- A timeout leaves the record pending until a committed poll or stale
  acknowledgement first observes lease expiry. That operation durably records
  `retry_not_before` as its effective observation time plus the pinned delay;
  late observation starts a fresh full delay. A schedule already recorded
  survives local or clustered restart and clustered leadership transfer. If
  restart occurs before expiry is durably observed, the first later operation
  starts the delay in either engine. Explicit retry dispositions remain future
  work. The next assignment increments the count exactly once.
- A delivery token remains a fencing token, not provenance. An acknowledgement
  with an expired or superseded token is stale even when the source offset is
  otherwise valid.
- A retry-scheduled record continues to reserve its requested ordering key so
  later records with that key cannot overtake it. Unrelated work follows the
  existing candidate-ordering rules.
- A redrive is a new source record at a new destination offset. Its delivery
  attempt starts at 1 under the destination consumer's policy; the original
  attempt count remains in provenance.

An attempt limit is evaluated before assigning another delivery. If the next
attempt would exceed `max_attempts`, the broker performs the terminal action
instead. This keeps a limit of 1 equivalent to "one source assignment, then
hold or dead-letter" and matches the current inclusive behavior.

### Accepted fixed retry delay and deferred backoff choices

ADR 0033 selects one constant `retry_delay_ms` per durable consumer policy,
bounded from zero through seven days. The delay follows an expired
acknowledgement lease and begins when expiry is first durably observed:

```text
retry_not_before = first_durable_expiry_observation + pinned_retry_delay
```

A zero delay makes the not-before deadline equal the observation time and
preserves immediate retry eligibility under the existing poll lifecycle.
The delay is not backdated to the lease deadline. A late poll or stale
acknowledgement starts a full delay from that operation's durable observation.
An acknowledgement is stale at the lease deadline, not at the end of the
retry delay. If the attempt limit is exhausted, the existing terminal
transition is eligible at the first durable expiry observation without an
unused redelivery delay. If no operation observes expiry, no retry delay or
terminal transition has started.
The delay is a single fixed value on every retry; there is no attempt-based
growth, jitter, per-message override, or explicit NAK behavior. Complete local
restart, clustered persistence, ordering, and clock rules are recorded in
[ADR 0033](../decisions/0033-fixed-consumer-retry-delay.md).

This decision differs from reference systems. Pulsar documents its
acknowledgement-timeout redelivery delay separately and illustrates an
acknowledgement timeout plus the selected retry delay. NATS JetStream instead
documents `BackOff` as replacing `AckWait`; the first list entry becomes the
initial acknowledgement deadline. Runnel keeps its existing acknowledgement
lease and adds a gap afterward, so healthy-work timeout tuning is unchanged.

Exponential backoff and jitter are deferred. AWS's contention analysis shows
that jitter can spread correlated retries, but Runnel has no measured
representative retry workload yet, and a fixed delay is the smallest
deterministic policy extension. Correlated failures can still synchronize
messages, so this first contract does not claim to prevent retry herds.

An explicit `retry` disposition may carry a bounded reason code and, in a
future protocol version, an optional delay request. The broker clamps an
application delay to the policy's bounds and records the effective delay. An
application must not be able to turn one message into an unbounded timer or
use an override to bypass `max_attempts`. A reason code should be a small
allow-listed value such as `timeout`, `dependency`, `invalid`, or `unknown`;
unbounded diagnostic text belongs in application logs or a separate bounded
operator event, not in every retained message.

The local engine uses volatile monotonic `Instant` deadlines for active
leases. The first poll or stale acknowledgement that durably observes expiry
persists `retry_not_before = local_wall_time_at_observation + pinned_delay` as
part of its durable transition; that deadline survives later restart. If
restart occurs before expiry is durably observed, the first later operation
starts a full delay. Local not-before deadlines are absolute wall-clock
timestamps evaluated on that clock both before and after restart, so clock
jumps can make them early or late. The
cluster uses leader-selected absolute lease deadlines and persists the
not-before deadline when a committed command first observes expiry. Its
persisted observation floor prevents backward evaluation after restart or
leader change, but forward jumps can make a retry due early and no committed
command means no lazy expiry. Both engines therefore apply the same late
observation rule: the delay begins at observation, not at the expired lease
timestamp. These residual constraints remain in
[TD-020](../tech-debt.md#td-020-clustered-delivery-leases-use-absolute-wall-clock-deadlines).

### Poison handling and terminal outcomes

Poison handling is a policy decision, not an implicit storage error:

- `max_attempts = None` keeps retrying until the application acknowledges, an
  explicit retention policy makes history unavailable, or an operator changes
  the policy through a documented operation.
- A bounded `max_attempts` with `hold` parks the record without acknowledging
  it. Later records subject to the same ordering gate remain blocked, while
  unrelated keyed work may progress. Health and metrics identify the held
  record and policy version.
- A bounded `max_attempts` with `dead_letter` creates one logical derived
  record, then advances source progress only after the target outcome is
  durable or reconciled by the stable move identity. If the target is
  unavailable, source progress does not advance; the pending transition is
  observable and consumes bounded retry/reconciliation resources.
- An explicit permanent-failure disposition may request the terminal action
  before the attempt limit only when the policy enables it. It still cannot
  skip the source record without either a durable dead-letter target or an
  explicit hold outcome.
- Dropping after exhaustion is outside this first contract. If a future
  deployment needs loss-tolerant disposal, it must be a separately named,
  explicitly destructive policy with its own compatibility and retention
  decision.

The broker should distinguish `attempts` from `delivery_attempt` on the
dead-letter consumer. A dead-letter inspector receiving a copied record sees
attempt 1 for its own consumer, while the provenance says that the source
consumer reached, for example, attempt 5 before the move.

### Dead-letter provenance

The dead-letter record should retain the original key and binary payload and
carry a bounded, versioned metadata sidecar. It should not wrap arbitrary
application bytes in JSON or require consumers to decode a broker-owned
payload envelope. A conceptual provenance object is:

```text
DeadLetterProvenance {
    schema: 1,
    dead_letter_id: opaque stable move identity,
    source_stream_id: opaque stream-incarnation identity,
    source_stream: validated logical stream name,
    source_consumer: validated logical consumer name,
    source_offset: logical offset,
    original_published_at_ms: timestamp,
    first_delivery_at_ms: optional timestamp,
    last_delivery_at_ms: optional timestamp,
    source_attempts: positive u32,
    terminal_reason: timeout | explicit_retry | explicit_dead_letter |
        retention | policy_exhausted | unknown,
    policy_version: opaque durable version,
    dead_lettered_at_ms: timestamp,
    root_id: opaque bounded lineage identity,
    redrive_count: u32,
    previous_dead_letter_id: optional opaque identity,
}
```

`source_stream_id` distinguishes a stream incarnation from a later stream
with the same name. The current clustered metadata already has a durable
stream identity; the local engine needs an equivalent durable incarnation
identity before stream deletion and recreation can be supported safely. Until
then, an existing stream name and logical offset are useful inspection data
but must be marked incomplete as a globally stable identity.

`dead_letter_id` is derived from the source stream incarnation, source
consumer, and source offset. The consumer is essential: two independent
consumers may legitimately dead-letter the same source record, and they must
produce distinct logical moves. The identity is bounded and encoded as an
opaque value; it is never a filesystem path. The existing local move identity
is the starting point for this invariant ([TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records)).

The metadata intentionally records an aggregate rather than an unbounded
attempt history. A bounded terminal reason and first/last timestamps support
operations without allowing a repeatedly redriven message to grow its record
forever. If detailed failure history becomes a product requirement, it should
be a separate bounded audit stream or metric, not an ever-growing message
header.

For legacy dead-letter records written before provenance, the fields are
absent and `provenance_complete = false` (or an equivalent explicit status).
The broker must not infer `source_offset` from the dead-letter stream offset:
multiple source consumers and crash retries make that inference invalid.

### Redrive

Redrive is an explicit recovery operation over a dead-letter record. It does
not rewind a source consumer checkpoint. The default safe shape is:

1. Read a dead-letter record and its provenance.
2. Choose a destination stream explicitly. A tool may suggest the recorded
   source stream, but publishing there intentionally fans the new record out
   to every consumer of that stream, including consumers that successfully
   processed the original. A first redrive API must not pretend this is a
   targeted consumer-only operation.
3. Supply a stable `redrive_id` for the intended redrive. A retry of the same
   request after a lost response returns or reconciles the same destination
   record. A distinct intentional redrive uses a distinct ID.
4. Append the original key and payload as a new record, preserving a bounded
   lineage (`root_id`, the immediate prior `dead_letter_id`, and an incremented
   `redrive_count`). The destination consumer applies its own policy from
   attempt 1.
5. Retain the dead-letter source record until the destination append is known
   to be durable. An implementation may leave source acknowledgement to the
   caller, but it must never acknowledge a move whose destination result is
   unknown.

The redrive identity is scoped to the destination stream and must reject a
same-ID request whose destination or key/payload differs. A successful target
append with a lost response is therefore safe to retry. The source dead-letter
record can still be delivered more than once until its inspector acknowledges
it; redrive deduplication does not make inspector processing exactly once.

The first implementation should support the derived dead-letter target while
it shares the source data-group ownership boundary. A named destination in the
same durable boundary can use the same move/reconciliation invariant. A
source dead-letter record and destination in different Raft groups require a
transaction or a durable cross-group workflow before the public API describes
the move as atomic. The current same-group clustered behavior must not be
generalized to that future layout.

## Retention and deduplication interaction

Retry policy and retention are separate policy layers, but they cannot make
contradictory promises. The source record, its attempt state, and any pending
dead-letter move form a retention fence:

- Under a `protect` retention policy, the source record remains eligible until
  it is acknowledged, held, or moved to a durable dead-letter record. A
  pending retry deadline and an unresolved local move pin the required source
  history. A target outage can therefore consume source capacity; the broker
  must expose pressure and apply admission rather than silently skip the
  record.
- Under an explicit `expire` policy, retention may end retry eligibility after
  the active lease has expired. The consumer receives an explicit
  `history_unavailable` or `retry_expired` outcome, never `Empty` and never an
  implicit checkpoint advance. If operators require every expired poison
  record to be inspected, they must choose `protect` or a separately defined
  retention-to-dead-letter transition.
- The dead-letter stream has its own retention policy. It should not silently
  inherit a shorter source retention period. Operators should normally retain
  it longer than the source because provenance and manual recovery are its
  purpose, while still setting a finite bound when disk safety requires one.
- Retention must preserve the move identity for as long as the source move is
  unresolved. Deleting the target record or its deduplication index while the
  source can still retry would reopen the duplicate window. If the target
  history has expired, the broker must fail closed with an explicit storage or
  retention outcome rather than append an indistinguishable second record.
- Physical cleanup may lag a logical retention floor. Retry and redrive
  admission must use bounded reserved capacity and must not depend on an
  unbounded in-memory list of pending bodies.

The proposed rules extend the [retention design](retention-disk-pressure-plan.md#lagging-consumers-and-replay-eligibility): a delivery lease, retry schedule, and unresolved move are additional durable fences, while the stream's chosen `protect`/`expire` behavior remains authoritative.

### Broker-side deduplication boundaries

There are three identities with different scopes:

| Identity | Scope | Purpose | Must not be confused with |
| --- | --- | --- | --- |
| Source identity | Stream incarnation + source offset | Locate the original logical record | A target stream offset |
| Dead-letter move ID | Source identity + source consumer | Deduplicate the automatic source-to-DLQ move | A delivery token or application request ID |
| Redrive ID | Dead-letter record + destination + caller-selected intent | Deduplicate one explicit redrive | A new source message identity |

Local movement remains append-before-checkpoint and reconciles by move ID, as
described in the [companion recovery note](dead-letter-recovery.md). Clustered
movement remains one replicated state transition while source and target share
the same data group. A future provenance-bearing implementation in either
engine must treat a same-ID key/payload or destination mismatch as an explicit
corruption/conflict outcome, not a compatibility-style "already accepted"
result; the current clustered automatic copy has no such move ID.

## Cluster ownership transfer

In the accepted implementation, the configured policy, per-offset policy
snapshot, attempt count, clustered lease deadline, and fencing state are stored
in replicated per-stream group-consumer state. Ordinary clustered polling uses
the grouped path with the consumer name as its member. The local engine also
persists policy snapshots and attempt counts, while its active lease remains
process-local. Delivery members and tokens do not define the durable policy
scope.

The fixed-delay deadline rules below are accepted in ADR 0033 but are not
implemented. The remaining disposition, provenance, and redrive rules are
candidate invariants:

On leader or member transfer:

1. An unexpired assignment remains owned by its current member until its
   replicated lease expires. A repeated poll may return the same delivery and
   token; it does not consume another attempt.
2. The first committed operation after expiry durably observes the expired
   lease. If another attempt is allowed, it stores
   `retry_not_before = effective_observation_time + pinned_retry_delay`; if the
   attempt limit is exhausted, it performs the terminal transition without
   waiting through an unused delay. A new member receives a new token and the
   next attempt number only after the recorded deadline is due and assignment
   commits. A stale acknowledgement from the old member is rejected as soon as
   the lease expires, before the retry delay ends.
3. A pending `retry_not_before` is copied as durable state. A successor does
   not recalculate a shorter deadline from its local policy or reset it because
   it became leader.
4. If the terminal transition commits, source progress and the derived record
   recover together under the current same-group state-machine guarantee. If
   the client loses the response, a repeated request or later poll observes
   the committed state rather than appending another logical move.
5. If the command was never committed, no attempt or terminal move was
   consumed. The next leader may retry the operation. The client must treat a
   lost response as ambiguous until it observes state, not as proof of failure.

The clustered poll path selects configured policy from replicated consumer
state before proposing a poll command. It carries the selected version,
leader-calculated deadline, and legacy fallback values through the replicated
transition ([clustered poll path](../../crates/runnel-raft/src/engine.rs),
[grouped state-machine request](../../crates/runnel-raft/src/delivery.rs)).
Startup flags are still used for consumers without an explicit policy; they do
not replace configured replicated policy. Mixed-version leadership and
rollback rules are not part of ADR 0027; future wire and rollout choices
belong in the [protocol compatibility design](protocol-compatibility.md).

The accepted scheduled-retry state is not implemented at the reviewed
baseline. There is no separate `retry_not_before` or retry-delay index.
Clustered expiry is evaluated when a valid grouped command is applied; ADR
0033 requires the future clustered not-before deadline to remain in replicated
state. The [lease-clock evidence](td-020-lease-clock-floor.md) records current
clock assumptions; the accepted schedule inherits its real-time error and
no-quorum limitations, which require focused tests in addition to fencing
tests.

## Ambiguous outcomes and client responsibilities

The public contract should classify outcomes rather than silently retrying
operations. The table is a future contract: current v1 has no stable poll,
retry, or redrive identity, and no inspection operation that resolves an
unknown poll.

| Operation/outcome | Possible durable state when the response is lost | Safe client behavior |
| --- | --- | --- |
| Poll assignment | Assignment committed, response lost; the message may already have been processed. | Retry the poll or inspect the consumer. Treat delivery as at least once and use the source identity/token for application deduplication. |
| Acknowledge | Source progress committed, response lost. | Retry the same acknowledgement; return `acknowledged` or `already_acknowledged`. If it was not committed, expect a later redelivery. |
| Explicit retry/dead-letter | Retry state or target move may be committed, response lost. | Retry with the current delivery token and an idempotent command ID where supported. Never assume a failed response means no broker state changed. |
| Automatic local dead letter | Target append may be durable while source progress is not. | Reconcile by `dead_letter_id`; leave source progress eligible until the target is known durable. A duplicate physical write is allowed only for legacy records or an unreconciled old format. |
| Clustered same-group move | Raft command may be committed before the client observes its response. | Query/repoll state; committed source progress and provenance record are one logical transition. Do not issue a distinct redrive ID unless a second recovery action is intended. |
| Redrive | Destination append may be durable while the source DLQ acknowledgement is not. | Retry the same `redrive_id`. A target record with the same ID and content is the prior success; the source remains available until acknowledged. |

An application must not use a delivery token as a long-lived business key:
local tokens are process-epoch values and clustered tokens are assignment
fences. The stable source identity and provenance are the broker correlation
keys. Neither identity suppresses external side effects without cooperation
from the application.

## Observability and inspection

The existing process-lifetime counters should remain compatible, but they do
not explain why records are waiting or which terminal action was selected.
The implementation should add low-cardinality metrics with bounded label sets,
for example:

- `runnel_retry_attempts_total{cause}` for timeout, explicit retry,
  ownership recovery, and other bounded causes;
- `runnel_retry_scheduled_total{kind}` and
  `runnel_retry_exhausted_total{action}` for hold versus dead-letter;
- `runnel_dead_letter_moves_total{outcome}` and
  `runnel_dead_letter_move_failures_total{cause}`;
- `runnel_redrives_total{outcome}` and
  `runnel_redrive_conflicts_total`;
- gauges for pending retries, held records, unresolved dead-letter moves,
  dead-letter records awaiting inspection, and oldest pending retry/dead-letter
  age; and
- histograms for selected backoff delay and time from first delivery to a
  terminal move, if their collection can remain bounded.

Stream and consumer names should not become mandatory metric labels: a large
or untrusted name set would create an unbounded time-series resource. A
diagnostic inspection operation may return policy, counts, earliest pending
offset, next retry deadline, and provenance for one requested record. Structured
logs may include stream, consumer, offset, policy version, attempt, reason,
and move ID, but payload bytes and unbounded application error strings should
not be logged by default.

Health/readiness should report a target-blocked or held state only when it
affects the broker's declared availability policy. A single held poison
message must not make an unrelated stream unready. Disk pressure caused by a
blocked dead-letter target must remain visible even if ordinary poll requests
continue to succeed.

## Compatibility and migration

The accepted consumer-policy operations are already additive v1 behavior
under ADR 0027. The following are possible compatibility concerns for future
extensions; they are not migration rules accepted by this note:

- Existing clients and implicit consumers keep the current broker-wide
  fallback. The existing `Message` shape keeps its payload, key, offset, and
  delivery attempt semantics.
- New message responses expose an optional provenance object only for
  dead-letter records that carry it. Older clients still receive the original
  payload and can continue acknowledging through the existing contract.
- Future requests for explicit retry/dead-letter disposition or redrive, and
  future provenance response metadata, need a compatibility decision. The
  current `configure_consumer` and `inspect_consumer` operations do not require
  capability negotiation. General v1/v2 evolution belongs in the
  [protocol compatibility design](protocol-compatibility.md).
- New durable records use a versioned metadata-capable frame or an equivalent
  bounded sidecar. Existing `RNL1`/versioned records and payload-only
  dead-letter records remain readable. A failed metadata migration must fail
  closed before a source checkpoint advances.
- The default derived stream name remains `<source>.dead-letter`. A future
  per-consumer destination or named target is opt-in and must not change where
  existing consumers find current dead letters.
- A rolling clustered upgrade must gate leadership and snapshot installation
  on the policy/provenance format version. A node must not apply a command it
  cannot deserialize or interpret deterministically. Rollback is allowed only
  before policy-bearing state is created, or after a documented state upgrade
  and downgrade boundary.
- Provenance absence is an explicit legacy state. The broker must not invent
  source offsets, attempts, or policy versions for records that predate the
  metadata format.

The wire protocol is still provisional ([protocol compatibility design](protocol-compatibility.md)).
ADR 0033 selects `retry_delay_ms` for the accepted extension, but does not
define mixed-version rollout or rollback. Later retry-policy extensions still
need field names, feature-negotiation behavior, and durable frame migration
decisions where applicable.

## Bounded resource behavior

Retry must reduce failure amplification rather than create a broker-managed
retry storm. An implementation should enforce bounds at configuration and
execution time:

- Validate `retry_delay_ms` from zero through seven days and use checked or
  saturating deadline arithmetic. Future exponential/jitter bounds, attempt
  limits, reason codes, provenance size, redrive request size, and policy
  catalogue limits need their own constraints.
- Keep at most one compact retry not-before entry per pending source offset.
  Local active lease deadlines remain process-local monotonic values, while a
  retry schedule is persisted as an absolute local wall-clock deadline;
  clustered schedules remain replicated until reassignment or terminal
  handling. Do not retain every attempt's payload or failure text in memory.
- Use a deadline index or equivalent bounded scheduler work so a poll does
  not scan every active delivery or wake one timer per message. The current
  decision adds no separate timer thread, background reclaim loop, scheduler
  capacity limit, or new backpressure outcome.
- Keep provenance to one root and one immediate predecessor plus aggregate
  counters. Cap `redrive_count` and treat saturation as an explicit conflict
  or terminal outcome.
- Bound redrive concurrency, target buffering, and reconciliation retries.
  A blocked target keeps the source record safe but must not accumulate
  unbounded copied bodies, worker tasks, or metric labels.
- Apply normal stream admission, retention, and ordering gates to automatic
  dead-letter moves and redrive. A redrive cannot bypass a target's byte limit,
  reserved capacity, or replication durability boundary.
- Preserve per-key exclusion while a retry is delayed. A policy that lets
  unrelated keys progress must not silently allow two assignments of the same
  key or let a later same-key record pass a pending earlier record.

The current storage executor and per-stream lanes already establish useful
local isolation boundaries ([TD-022](../tech-debt.md#td-022-local-durable-io-has-bounded-async-isolation-but-incomplete-evidence)). Retry scheduling should use those boundaries rather than introducing an unbounded background task per consumer. The clustered state vector and snapshot representation are currently a vertical-slice limitation; any larger-scale scheduler needs separate resource evidence.

## Reference designs and primary research

These references solve related problems with different delivery and topology
models. They are evidence for tradeoffs, not compatibility targets.

| Reference | Relevant design | What matters for Runnel |
| --- | --- | --- |
| [Amazon SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html), [message attributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_Message.html), and [redrive policy](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html) | A source queue's redrive policy bounds receives with `maxReceiveCount`; receive attributes include an approximate receive count and the source queue ARN, and SQS exposes an explicit redrive task. Standard-queue retention keeps the original enqueue timestamp, so DLQ retention must account for source age. Redrive is rate-limited and creates a new destination message identity. | Keep source consumer, source offset/incarnation, attempts, and redrive lineage as first-class bounded metadata. Do not equate a DLQ target offset with the source offset. Make redrive explicit, rate-bounded, and retention-aware. Runnel's consumer/stream model needs consumer identity in addition to SQS's queue source. |
| [NATS JetStream consumer configuration](https://docs.nats.io/nats-concepts/jetstream/consumers) | Durable consumers own acknowledgement state. `MaxDeliver` limits attempts but leaves exhausted messages in the stream; `BackOff` is a consumer setting that overrides `AckWait`, and explicit negative acknowledgement is immediate unless a delay is supplied. | This is the closest policy-scope reference: put policy on the durable consumer and let shared members inherit it. Persist Runnel's count and deadline rather than treating a transient member or poll as the policy owner. A future `hold` outcome should be explicit because leaving a message in the stream alone does not prevent another delivery. |
| [RabbitMQ dead-letter exchanges](https://www.rabbitmq.com/docs/dlx) and [quorum-queue dead lettering](https://www.rabbitmq.com/docs/quorum-queues) | Ordinary DLX republishing removes the source without publisher confirms and can lose a message. Quorum at-least-once dead lettering retains the source until the target confirms, but pending messages consume source resources and retries can duplicate target deliveries. RabbitMQ records compressed `x-death` history. | Retain Runnel's append-before-source-progress rule and its explicit duplicate caveat. Add move identity instead of an unbounded death header, expose blocked-target pressure, and make the current same-group clustered atomicity a layout-specific fact. |
| [Apache Pulsar retry and dead-letter topics](https://pulsar.apache.org/docs/next/concepts-messaging/) | Retry topics make delay and retry count persistent message properties such as `REAL_TOPIC`, `ORIGIN_MESSAGE_ID`, `RECONSUMETIMES`, and `DELAY_TIME`. Pulsar documents that a negative-ack redelivery counter can reset on restart or ownership changes, while retry-topic state is the reliable path to a maximum redelivery count. | Persist Runnel's retry state in the durable consumer/record model regardless of whether a future implementation uses a delay stream. Keep provenance separate from payload and distinguish broker assignment attempts from the DLQ inspector's own attempts. Avoid requiring an application to build a second topic topology just to get reliable retry accounting. |
| [Apache Kafka Connect error handling](https://kafka.apache.org/26/kafka-connect/user-guide/) and [KIP-98 transactions](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) | Kafka Connect makes retry duration, delay cap, tolerance, and an error/DLQ topic configurable per connector, while Kafka transactions atomically commit produced records and consumed offsets across participating partitions. These are framework/topology or transaction-coordinator features, not a generic per-message broker retry policy. | Keep policy scope and retry decisions in Runnel's durable consumer model. Treat a retry/DLQ stream as an escape hatch or later implementation, and reserve cross-group atomicity for a separate transaction/reconciliation decision. Stable operation identity is useful; Kafka's exactly-once terminology must not be applied to arbitrary application side effects. |
| [AWS exponential backoff and jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) and [retry guidance](https://docs.aws.amazon.com/wellarchitected/latest/reliability-pillar/rel_mitigate_interaction_failure_limit_retries.html) | Exponential backoff reduces synchronized retry pressure, jitter spreads clients across the interval, and retry count/delay need caps. Retry should not be applied blindly to non-idempotent operations. | Keep the accepted first delay fixed and bounded. Jitter may reduce synchronized retry pressure, but is deferred until representative workload evidence justifies a deterministic replicated distribution contract. |

The primary research points to the same boundary:

- Saltzer, Reed, and Clark's [End-to-end arguments in system design](https://web.mit.edu/6.033/2002/wwwdocs/papers/endtoend.pdf) explains why duplicate suppression and recovery that depend on application semantics belong at the end points; lower-level mechanisms can help but cannot infer whether an external effect has already happened. This supports broker-side move deduplication while leaving application-effect idempotency to the consumer.
- Garcia-Molina and Salem's original [Sagas](https://www.cs.princeton.edu/techreports/1987/070.pdf) describes long operations as local transactions with compensating actions. A compensating delete is not a safe first choice for a visible dead-letter copy because the copy may already have been consumed; this supports durable move identity and no-loss ordering instead.
- Helland's [Life beyond Distributed Transactions](https://www.cidrdb.org/cidr2007/papers/cidr07p15.pdf) argues for independent durable entities, unique message identities, and workflows that make uncertain outcomes harmless. That supports an idempotent `redrive_id` and durable reconciliation, while not proving exactly-once processing across Runnel and an external system.

## Alternatives considered

| Alternative | Benefit | Cost or reason not selected as the starting contract |
| --- | --- | --- |
| Keep broker-wide timeout and attempt flags | No new configuration model and fully compatible with the current slice. | Event fan-out, interactive work, and long-running jobs cannot select different failure budgets; provenance remains absent. Retain it only as the legacy fallback. |
| Application-managed retry streams | Maximum application control over delay, reason, and routing. | Every application must implement durable attempts, provenance, move deduplication, and safe source acknowledgement. It also makes retry topology and fan-out mistakes easy. It can remain an escape hatch for policies Runnel does not support. |
| Dedicated retry/delay stream per consumer, modeled after Pulsar | Delay becomes a visible durable record and can be scheduled by ordinary stream machinery. | It multiplies stream state, creates a second delivery identity, complicates ordering and retention, and can move retry correctness into a client helper. Consider it as an implementation technique only after the compact in-place schedule is resource-tested. |
| Immediate negative acknowledgement without broker scheduling | Small protocol addition and explicit application failure signal. | Without a durable delay and cap it can create retry storms and lose attempt limits across restart or ownership transfer. A disposition can be layered onto the proposed durable schedule. |
| Per-consumer dead-letter stream by default | Isolates operators and avoids mixing copies from independent consumers. | It changes the current `<source>.dead-letter` discovery rule and creates streams with potentially unbounded consumer churn. Keep one derived stream plus provenance by default; allow an explicit target later. |
| Full transaction across source, target, and acknowledgement | Strongest broker-side atomicity and simpler recovery reasoning. | A local cross-log transaction or cross-Raft-group coordinator needs prepare/commit state, timeout/recovery rules, format migration, and bounded resource policy. The current clustered same-group transition is sufficient for the first slice; local reconciliation is the smaller repair. |
| Drop after the attempt limit | Gives the strongest disk/backlog bound. | Violates the current no-loss default and can turn a configuration mistake into silent data loss. Any future destructive policy needs a separate explicit decision. |

## Outcome and evidence gates

This section separates the accepted implementation, the accepted but
unimplemented fixed-delay contract, and candidate questions for later
extensions. These sections are not an implementation task list or execution
sequence. Any runtime change must preserve the current at-least-once behavior
and pass the class-specific policy in [testing.md](../testing.md).

### Accepted implementation: durable consumer-scoped attempt policy

ADR 0027 and the implementation define this accepted boundary:

- **Policy operations:** `configure_consumer` and `inspect_consumer` operate
  on an existing stream and a named consumer. Poll requests do not carry
  policy values.
- **Durable state outcome:** the implemented versioned policy contains only bounded
  `ack_timeout_ms`, optional positive `max_delivery_attempts`, and the
  existing derived dead-letter action. Broker-wide settings remain the
  fallback for an unconfigured consumer.
- **Lease outcome:** in current code, `ack_timeout_ms` is the active lease and
  expiry is the only timeout-driven retry gate. There is no implemented
  separate retry delay.
- **Placement outcome:** local consumer state and replicated clustered
  group-consumer state persist policy and per-offset snapshots. A shared
  member cannot override the consumer's policy.
- **Versioning outcome:** configuration is idempotent for unchanged values
  and advances the version when values change. The first persisted assignment
  pins the full policy snapshot; later updates do not silently change that
  record's attempt budget.

An unacknowledged local assignment keeps its policy and attempt count across
restart, while its process-local lease is immediately eligible after recovery.
The next assignment starts a fresh interval using the pinned policy. Clustered
assignments retain their existing replicated absolute deadline behavior.

The tests listed in the [current evidence snapshot](#current-evidence-snapshot)
exercise this implemented slice, including real-process configured-policy
leader transfer. The existing backlog criterion already includes policy
transfer; no additional first-slice outcome is introduced here.

### Accepted contract: fixed per-consumer retry delay (not implemented)

ADR 0033 adds bounded `retry_delay_ms` to the durable policy. The value
defaults to zero and is pinned on the first assignment. For an attempt with
another delivery allowed, the delay starts when a committed poll or stale
acknowledgement first durably observes lease expiry. The persisted deadline is
that observation time plus the pinned delay; a late observation starts a full
delay. The acknowledgement token becomes stale at lease expiry, before the
delay ends. Attempt exhaustion does not wait through a delay for an attempt
that will not be made. The delay is fixed across attempts, with no jitter,
exponential growth, per-message override, or NAK operation.

The local engine uses monotonic deadlines for active leases while running.
When expiry is first durably observed, it persists a local absolute wall-clock
retry deadline, which survives restart. If restart occurs before expiry is
durably observed, the first later operation starts the delay. The cluster
retains its absolute lease deadline and persists the retry not-before value
through replay, snapshots, and leader changes. Both engines follow the same
late-observation rule; local wall-clock jumps and the clustered clock floor
retain their documented early/late and no-command limitations. The complete
contract and implementation gates are in
[ADR 0033](../decisions/0033-fixed-consumer-retry-delay.md).

The tests in the evidence snapshot do not cover this new behavior. Runtime
work still needs local and clustered boundary, restart, ordering, and stale
token tests plus a real multi-process clustered transfer test. `hold`,
exponential or jittered delay, explicit dispositions, named or cross-group
targets, provenance metadata, and redrive remain unaccepted extensions.

### Candidate evidence: define later-extension fixtures

Define versioned policy and provenance fixtures before changing runtime code:

- validate future policy combinations, duration/attempt bounds, default
  mapping from legacy flags, and policy-version pinning; fixed retry-delay
  bounds and pinning are accepted in ADR 0033;
- define `attempt`, `retry_not_before`, `dead_letter_id`, and `redrive_id`
  examples for both independent and shared consumers;
- define explicit outcomes for `held`, `history_unavailable`, target blocked,
  conflict, stale delivery, confirmed success, and unknown result; and
- document whether an explicit retry/dead-letter disposition is in the first
  later additive protocol stage.

Gate: reviewable contract fixtures and compatibility examples; no runtime or
performance claim.

### Measurement rule for recovery gates

The repository has correctness fixtures and broad retained-history recovery
probes, but it does not yet define a retry-policy recovery SLO. A future
implementation PR must therefore publish its measurement envelope before
running it rather than choosing a favorable threshold afterward. At minimum,
the envelope should record:

- local and clustered stream/consumer counts, retained message shape, pending
  attempt count, and policy-state cardinality;
- time and bytes for local journal replay and clustered checkpoint/journal or
  snapshot recovery, with the exact failure point and whether a leader change
  or cache eviction occurred; and
- peak resident memory, bounded scheduler/index work, and any response or
  admission latency while the retry state is recovered.

The pass condition must include exact logical equality for payload, key,
offset, attempts, acknowledgement state, fencing, terminal movement, and
provenance where supported. Timing and resource limits must be selected in the
compatibility or implementation ADR from representative workload evidence;
this design note does not invent absolute limits. The existing [benchmark
policy](../benchmarking.md) still applies when a runtime change affects a hot
path, while design-only gate evidence remains semantic and reviewable.

### Accepted implementation gate: fixed retry delay

Runtime work for ADR 0033 must establish the exact accepted semantics:

- validate zero, the seven-day maximum, and rejection above it; read old
  policy/request values without the field as zero;
- prove policy-version idempotence and per-offset pinning across a later
  consumer configuration change;
- exercise exact lease-expiry observation and retry-not-before boundaries,
  late poll and stale-ack observations, stale acknowledgements during the
  delay, attempt exhaustion without an unused delay, and attempt increments
  only on committed reassignment;
- preserve same-key exclusion during a scheduled delay while allowing
  unrelated eligible work under current candidate-ordering rules;
- prove that restart or leader transfer before durable expiry observation
  starts a full delay at the first later observation in both engines, and
  that a previously persisted deadline survives restart or leader transfer;
- prove that clustered state-machine replay and snapshots preserve the
  absolute not-before deadline; and
- bound scheduled-state/index work without one background task per record.

Gate: focused local and clustered failure, recovery, ordering, timeout, and
resource tests plus a real three-node process test for ownership transfer.
The delay schedule remains demand-driven. A benchmark is not required for this
design-only change, but runtime changes to delivery or persistence hot paths
must assess and run applicable benchmarks under
[benchmarking.md](../benchmarking.md), or explain a concrete evidence gap.

Exponential or jittered schedules, explicit-retry attempt accounting, and
`hold` versus dead-letter outcomes remain future design questions.

### Candidate evidence: provenance and duplicate-safe automatic movement

Evidence must establish a versioned metadata-capable durable record and
populate the provenance object at the automatic move, covering:

- binary payload/key preservation and bounded metadata validation;
- stable identity across local restart and incomplete/torn-tail recovery;
- same-ID same-content reconciliation and same-ID mismatch conflict;
- two independent source consumers dead-lettering the same source offset;
- legacy payload-only dead-letter records;
- source/target retention and dedup-index expiry; and
- clustered atomic movement, snapshot recovery, follower restart, and leader
  change within the same data group.

Gate: fault injection at target append and source-progress durability points,
including ambiguous I/O results, plus real local-process and three-node
protocol tests. No atomicity claim is allowed for a target in another durable
group.

### Candidate evidence: explicit dispositions and redrive

Evidence must establish explicit retry/dead-letter and redrive operations
with a separately decided compatibility path, covering:

- idempotent repeated redrive with the same ID and explicit conflict on
  destination/content mismatch;
- lost response after target append, source acknowledgement, Raft commit, and
  client reconnect;
- new destination offset and attempt 1 after redrive;
- bounded lineage across repeated redrives;
- explicit source-stream fan-out semantics and no accidental consumer-only
  claim; and
- target unavailable, retention-expired, admission-rejected, and
  cross-group outcomes.

Gate: interoperability fixtures, real-server tests, cluster ownership tests,
and documented rollback/upgrade behavior. A future cross-group implementation
requires its own transaction or reconciliation ADR and failure matrix.

### Candidate evidence: operational acceptance

Evidence must establish metrics, inspection, alert guidance, and bounded
resource behavior for large pending-retry and dead-letter populations. It must
show that metric labels, logs, metadata, and scheduler memory remain bounded,
and re-evaluate whether the materialized clustered representation is
sufficient before claiming scale.

## Unresolved risks and hypotheses

- **Policy update semantics:** pinning a version is predictable but may delay
  an urgent change to an already-poison record. Reset semantics, authorization,
  and operator visibility need a separate contract.
- **Clock behavior:** a persisted local retry deadline uses wall time before
  and after restart, so a clock jump can make it due early or late. A restart
  before expiry is durably observed leaves no retry schedule; the first later
  observation starts the delay. Clustered retry deadlines inherit TD-020's
  wall-clock assumptions: forward jumps can make them due early, backward
  jumps can delay them, and no command or quorum means no progress. The
  contract does not promise hard real-time timing.
- **Local stream incarnation:** provenance needs an identity that survives
  rename/recreate distinctions. The current repository has no stream deletion
  API, but future deletion cannot reuse name-plus-offset as a globally stable
  source identity.
- **Cross-group movement:** a named redrive target can be physically separate
  even when the public model is topology-free. The implementation must detect
  that boundary without exposing placement and return an explicit unsupported
  or reconciled outcome.
- **Shared dead-letter destination:** a common derived stream is compatible and
  resource-efficient, but operators must use provenance to distinguish
  independent consumers. Per-consumer streams may be needed for isolation in a
  later product tier.
- **Derived-name predicate drift:** local and clustered implementations retain
  separate predicates for identifying generated dead-letter streams. Current
  tests cover equivalent suffix, maximum-length hashed-target, and hash-prefix
  collision behavior, so no runtime mismatch is known. A shared helper remains
  an optional maintainability improvement; any future policy slice must keep
  those boundary tests when changing either predicate.
- **Retention versus retry:** `protect` can pin storage when a target is down;
  `expire` can end delivery eligibility. The policy and operator UI must make
  that trade-off explicit rather than hiding it in a cleanup loop.
- **Provenance privacy and size:** source consumer names and failure reasons
  may be sensitive, and metadata still consumes storage. The schema needs
  bounded fields, redaction guidance, and a deliberate exposure policy.
- **Snapshot and journal growth:** durable attempts, schedules, identities, and
  provenance add state to the existing vertical slice. Compaction and snapshot
  behavior need evidence before a large number of pending records is claimed.
- **Application semantics:** a broker can identify and reconcile its own
  movement, but it cannot know whether a consumer completed an external side
  effect before losing its acknowledgement. This is a permanent at-least-once
  boundary, not a missing retry flag.

## Evidence boundary

This decision and design update has primary evidence class `design/research`,
with secondary tags `public-contract`, `storage/recovery`, `clustered`, and
`resource-safety`. It makes no runtime, correctness, durability, latency,
throughput, or memory-performance claim. No benchmark is required for this
design-only change. The implementation gate above names the real-process,
crash/recovery, compatibility, and bounded-resource evidence required before
the runtime extension is recommended for merge.

ADR 0033, the backlog retry subsection, and TD-018 are updated together to
record the accepted-but-unimplemented fixed delay. TD-020 remains open because
the clustered wall-clock limitations still apply. The existing code around
the changed design is not refactored because this PR does not change runtime
behavior.
