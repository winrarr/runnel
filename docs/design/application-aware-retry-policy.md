# Application-aware retry and dead-letter provenance

- Status: exploratory design note; decision-ready for a scoped first slice; no runtime semantics are changed here
- Date: 2026-09-04
- Related debt: [TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse)
- Related outcome: [Make retry policy application-aware](../backlog.md#make-retry-policy-application-aware)
- Related decisions: [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md), [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md), and [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md)
- Companion design: [Dead-letter recovery across durable boundaries](dead-letter-recovery.md)

This note proposes the application-facing retry and dead-letter contract for
TD-018. It is a design input, not an accepted decision and not an API or
storage-format change. It recommends a narrow first implementation slice, but
the full outcome remains unresolved because consumer lifecycle/versioning,
clock behavior, provenance encoding, and cross-boundary movement still need
explicit choices. An implementation should first turn the parts that change
compatibility, recovery, or data lifecycle into a dated ADR.

## Decision-ready recommendation (not an accepted decision)

The first runtime slice should establish durable, consumer-scoped attempt
policy without simultaneously solving every retry and recovery feature:

1. Add an explicit durable consumer policy operation and inspection result.
   A policy is selected by a named consumer and is not carried as arbitrary
   data in each poll request. Polling an existing implicit consumer continues
   to use the broker-wide legacy fallback until it is explicitly configured.
2. Persist `policy_version`, `ack_timeout`, and an optional positive
   `max_attempts` with the consumer state. For this slice, exhaustion retains
   the current derived dead-letter action and target; `ack_timeout` remains
   the retry delay. This makes the policy application-aware while isolating
   consumer-state placement and compatibility from new timer arithmetic.
3. Pin the policy version on the first assignment of a source record. A
   policy update affects records with no attempt state; an in-flight or
   scheduled record keeps its pinned version until it is acknowledged or
   terminally moved.
4. Store the policy with local `ConsumerState` and with both ordinary and
   grouped clustered consumer state. Group members therefore inherit one
   replicated policy, and ownership transfer cannot silently select a
   member's process-local settings.

The first slice deliberately excludes exponential or jittered backoff,
explicit retry/dead-letter dispositions, `hold`, named or cross-group targets,
provenance-bearing record frames, and redrive. Those are follow-on slices with
separate failure and compatibility gates. The target contract below remains
the direction to evaluate after this boundary is accepted.

This cut is recommended rather than accepted. The current protocol creates
consumers implicitly on first poll and has no consumer configuration or
inspection operation, so an ADR must still choose the operation names,
capability/version negotiation, policy-update authorization, and migration
shape. No ADR is added in this change because those choices affect the
provisional wire contract and durable formats without enough implementation
evidence to select one safely.

## User outcome and non-goals

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

The current behavior is a useful compatibility baseline:

| Area | Current behavior | Boundary for TD-018 |
| --- | --- | --- |
| Policy selection | `--ack-timeout-ms` and optional `--max-delivery-attempts` are broker-wide. Consumers are created implicitly by polling. | Add a durable policy for a named consumer; retain the broker-wide settings as the fallback for legacy consumers. |
| Attempts | The first assignment is attempt 1. Repeating a poll while the delivery is still in flight does not increment it. An expired delivery is assigned again with a persisted, higher attempt. | Keep the count per source stream, consumer, and logical offset, not per transient member or connection. Persist the count before returning a new delivery or commit it in the cluster command. |
| Delay | The acknowledgement timeout is both the active lease and the redelivery delay. Local active leases are volatile; clustered deadlines are absolute timestamps selected by the leader. | Separate the processing lease from the delay before the next attempt. Persist the selected retry deadline so restart and ownership transfer do not reset policy state. |
| Local dead letter | `runnel-core` appends to `<source>.dead-letter`, then persists source progress. The existing internal move identity and strict same-content reconciliation prevent a completed move from appending a second target record during a retry. A target/source crash boundary remains at least once. | Preserve append-before-source-progress and the [dead-letter recovery](dead-letter-recovery.md) identity. Add bounded provenance to the derived record. |
| Clustered dead letter | `runnel-raft` currently commits the derived record and source progress in one stream data-group state-machine transition. Group ownership, attempts, deadlines, and fencing state are replicated. | Replicate policy and retry schedule with the consumer state. Keep same-group atomic movement; require a separate transaction or reconciliation design before claiming atomicity across groups. |
| Delivery API | The provisional protocol has `poll`, `poll_group`, `ack`, `ack_group`, an attempt number, and an opaque grouped-delivery token. It has no negative acknowledgement, consumer configuration, provenance field, or redrive operation. | Additive capability-gated operations and optional response metadata are required. Existing payloads and legacy dead-letter records must remain readable. |
| Observability | Health exposes process-lifetime redelivery and dead-letter counters; `/metrics` exposes those counters and general request/storage metrics. | Add retry schedule, terminal reason, move, redrive, and blocked-target signals without unbounded stream/consumer label cardinality. |

The local attempt and move paths are visible in the [local poll and attempt handling](../../crates/runnel-core/src/lib.rs#L388), [local dead-letter append](../../crates/runnel-core/src/lib.rs#L655), and [request-aware move identity](../../crates/runnel-core/src/lib.rs#L845). The clustered state shape and grouped transition are visible in the [clustered consumer state](../../crates/runnel-raft/src/lib.rs#L240) and [grouped poll state machine](../../crates/runnel-raft/src/lib.rs#L572). The current public message has only `delivery_attempt` and no provenance object ([engine contract](../../crates/runnel-engine/src/lib.rs#L73), [protocol response](../../crates/runnel-protocol/src/lib.rs#L145)).

### Current evidence snapshot

The existing tests establish the compatibility baseline that the first slice
must preserve:

- The reusable engine contract covers first delivery, redelivery after lease
  expiry, stale acknowledgement fencing, scoped key exclusion, and restart
  recovery in [`crates/runnel-core/tests/engine_contract.rs`](../../crates/runnel-core/tests/engine_contract.rs).
- Local unit tests cover durable attempt counting and the existing derived
  dead-letter transition ([attempt limit](../../crates/runnel-core/src/lib.rs#L1565),
  [stable move identity](../../crates/runnel-core/src/lib.rs#L1646),
  [restart reconciliation](../../crates/runnel-core/src/lib.rs#L1692), and
  [content mismatch](../../crates/runnel-core/src/lib.rs#L1800)).
- Cluster tests cover the same broker-wide policy through restart, stale
  delivery fencing, leader/clock recovery, and a replicated derived
  dead-letter transition ([cluster retry tests](../../crates/runnel-raft/src/lib.rs#L3579)
  and [cluster dead-letter test](../../crates/runnel-raft/src/lib.rs#L3765)).
- Real-server tests cover the provisional wire shape, restart recovery, and
  attempt-limit/dead-letter behavior ([local protocol](../../crates/runnel-server/tests/server_smoke.rs#L612)
  and [cluster process recovery](../../crates/runnel-server/tests/cluster_smoke.rs#L600)).

There is currently no test evidence for consumer creation/configuration,
different policies on two consumers of one stream, policy-version pinning,
durable retry deadlines, explicit failure dispositions, provenance, or
redrive. The first slice's acceptance tests below are therefore future gates,
not claims about the current implementation.

One adjacent implementation gap is also relevant to the first slice's
non-recursive dead-letter requirement: local polling excludes only stream names
ending in `.dead-letter`, while clustered polling also recognizes the hashed
fallback used when a derived name would exceed the 128-byte name limit. A
maximum-length source name can therefore receive one extra local dead-letter
hop through its hashed target. This design-only change does not alter that
runtime behavior; a future implementation must centralize the derived-target
predicate and add a boundary test before claiming the rule for all valid names.

## Proposed contract

### Policy scope and selection

Retry policy belongs to the durable consumer, not the stream, member, node, or
delivery token. All members of one shared consumer inherit the same policy;
independent consumer names may select different policies for the same stream.
This preserves the existing distinction between fan-out consumers and workers
sharing one consumer.

The eventual administrative vocabulary should be intent-oriented. The exact
request names are intentionally left for a compatibility ADR, but its semantic
model should be equivalent to:

```text
RetryPolicy {
    version: opaque durable policy version,
    ack_timeout: bounded duration,
    max_attempts: optional positive u32,
    backoff: BackoffPolicy,
    on_exhausted: hold | dead_letter,
    dead_letter_target: derived | named stream,
    explicit_retry: disabled | enabled,
    explicit_dead_letter: disabled | enabled,
}

BackoffPolicy {
    kind: fixed | exponential,
    initial_delay: bounded duration,
    max_delay: bounded duration,
    jitter: none | full,
}
```

Recommended rules for this first model are:

1. `ack_timeout` is the maximum time a consumer has to acknowledge an active
   assignment. It is not used as the backoff once a policy explicitly sets a
   different delay.
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

Policy updates need a version fence. A safe starting rule is that a policy
version is pinned when a source record receives its first delivery. That
record keeps the version through acknowledgement, exhaustion, or dead-letter
movement. A policy update applies to records with no attempt state. An
explicit administrative reset can re-evaluate pending records later, but it
must name the old and new version and report which records were affected.
This avoids silently changing a poison decision while a deployment is being
rolled out. A policy update must not shorten an already persisted retry
deadline without an explicit reset operation.

Existing consumers with no explicit policy continue using the broker-wide
settings and are marked as legacy in inspection output. Existing dead-letter
records continue to expose their original key and payload. They have no
retroactively recoverable provenance, so a new field must report provenance as
absent or incomplete rather than guessing from the target offset.

### Attempts and state transitions

The logical state for one source record and consumer is:

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
- A timeout, explicit retry disposition, consumer disconnect, or ownership
  transfer after lease expiry makes the record eligible for a later attempt.
  The next assignment increments the count exactly once.
- A delivery token remains a fencing token, not provenance. An acknowledgement
  with an expired or superseded token is stale even when the source offset is
  otherwise valid.
- A retry-scheduled record does not make later records with the same ordering
  key eligible. For an unkeyed ordered consumer, the first pending record also
  remains the frontier. Unrelated keys in a shared consumer may continue.
- A redrive is a new source record at a new destination offset. Its delivery
  attempt starts at 1 under the destination consumer's policy; the original
  attempt count remains in provenance.

An attempt limit is evaluated before assigning another delivery. If the next
attempt would exceed `max_attempts`, the broker performs the terminal action
instead. This keeps a limit of 1 equivalent to "one source assignment, then
hold or dead-letter" and matches the current inclusive behavior.

### Backoff and jitter

Backoff begins after a failed attempt becomes retryable. Lease duration and
retry delay are separate so a worker that normally needs 10 seconds does not
have to wait 10 seconds after every fast, explicit failure. The policy stores
the resulting `retry_not_before` timestamp, not just the formula inputs.

For a failure after attempt `n`, define the uncapped delay as:

```text
fixed:       initial_delay
exponential: initial_delay * 2^(n - 1)
cap:         min(uncapped delay, max_delay)
```

`max_delay` and all intermediate arithmetic are bounded and saturating. With
`jitter = none`, the selected delay is `cap`. With `jitter = full`, the
selected delay is a deterministic value in `[0, cap]` derived from the stable
source identity and failed attempt number. The actual selected deadline is
persisted with the retry state. Deterministic jitter is necessary because a
Raft state machine cannot independently sample random values on every replica;
deriving it from message identity still spreads distinct messages across the
interval. A later implementation may use a committed random seed, but it must
not make follower application nondeterministic.

An explicit `retry` disposition may carry a bounded reason code and, in a
future protocol version, an optional delay request. The broker clamps an
application delay to the policy's bounds and records the effective delay. An
application must not be able to turn one message into an unbounded timer or
use an override to bypass `max_attempts`. A reason code should be a small
allow-listed value such as `timeout`, `dependency`, `invalid`, or `unknown`;
unbounded diagnostic text belongs in application logs or a separate bounded
operator event, not in every retained message.

The current local engine uses volatile `Instant` leases, and the cluster uses
leader-selected absolute deadlines. A future durable schedule will need a
conservative local wall-clock floor and the existing clustered clock-floor
invariant so a backward clock step cannot make a retry earlier or indefinitely
late. The exact timing error and failover behavior remain part of TD-020, so
this note does not claim deadline precision across nodes.

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
the same data group. In both engines, a same-ID key/payload or destination
mismatch is an explicit corruption/conflict outcome, not a compatibility-style
"already accepted" result.

## Cluster ownership transfer

Consumer policy version, attempt count, retry deadline, terminal state,
provenance fields, and automatic move identity must be part of the durable
consumer/data-group state that moves with ownership. A member name and
delivery token remain transient ownership details.

On leader or member transfer:

1. An unexpired assignment remains owned by its current member until its
   replicated lease expires. A repeated poll may return the same delivery and
   token; it does not consume another attempt.
2. After expiry, a new member receives a new token and the next attempt number
   only when that assignment is committed. A stale acknowledgement from the
   old member is rejected.
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

The current cluster passes `max_delivery_attempts` in each grouped poll
command and expects configuration consistency across nodes
([clustered poll path](../../crates/runnel-raft/src/lib.rs#L1362)). The
application-aware contract should replace that per-request configuration with
replicated consumer policy state. A node that cannot interpret the policy
version must not become leader for that state; mixed-version behavior and
rollback belong in the compatibility ADR.

## Ambiguous outcomes and client responsibilities

The public contract should classify outcomes rather than silently retrying
operations:

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

This is an additive design. The compatibility ADR should require the
following behavior:

- Existing clients and implicit consumers keep the current broker-wide
  fallback. The existing `Message` shape keeps its payload, key, offset, and
  delivery attempt semantics.
- New message responses expose an optional provenance object only for
  dead-letter records that carry it. Older clients still receive the original
  payload and can continue acknowledging through the existing contract.
- New requests for consumer policy, explicit retry/dead-letter disposition,
  inspection, and redrive require capability negotiation or a protocol version
  recognized by the server. An older server must return an explicit unsupported
  operation, not treat policy data as an ordinary publish or silently drop it.
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

The wire protocol is still provisional ([protocol compatibility design](protocol-compatibility.md)); this note intentionally does not choose field
names, feature-negotiation messages, or a durable frame migration version.

## Bounded resource behavior

Retry must reduce failure amplification rather than create a broker-managed
retry storm. An implementation should enforce bounds at configuration and
execution time:

- Validate positive, finite durations, maximum delay, attempt count, reason
  code length, provenance size, redrive request size, and any policy count per
  broker. Reject overflow and `max_delay < initial_delay` explicitly.
- Store one compact retry state per pending source record: attempt, policy
  version, next deadline, and terminal/move identity. Do not retain every
  attempt's payload or failure text in memory.
- Use a deadline index, bounded scheduler work, or an equivalent structure so
  a poll does not scan every active delivery or wake one timer per message.
  Work that cannot be scheduled within the configured bound must produce an
  explicit backpressure/retryable outcome.
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
| [AWS exponential backoff and jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) and [retry guidance](https://docs.aws.amazon.com/wellarchitected/latest/reliability-pillar/rel_mitigate_interaction_failure_limit_retries.html) | Exponential backoff reduces synchronized retry pressure, jitter spreads clients across the interval, and retry count/delay need caps. Retry should not be applied blindly to non-idempotent operations. | Separate lease from backoff, cap both delay and attempts, and persist the selected deadline. Use deterministic jitter derived from stable identity in the replicated state machine rather than nondeterministic replica-local randomness. |

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

## Staged implementation and verification gates

The stages below are sequencing guidance, not acceptance of runtime behavior.
Every stage must preserve the current at-least-once wording and pass the
class-specific gate in [testing.md](../testing.md).

### First implementation slice: durable consumer-scoped attempt policy

This is the recommended first runtime slice for a future implementation PR.
It is intentionally smaller than the full policy model above:

- Add one explicit create/configure/inspect contract for a durable consumer;
  do not make policy-bearing poll requests authoritative.
- Persist a versioned policy containing only bounded `ack_timeout`, optional
  positive `max_attempts`, and the existing derived dead-letter action. Keep
  the broker-wide settings as the legacy fallback for implicit consumers.
- Apply `ack_timeout` as both the active lease and retry delay for this slice.
  Do not add a second scheduler, explicit negative acknowledgement, or
  backoff formula yet.
- Place the policy and its version with local consumer checkpoint state and
  with both ordinary and grouped clustered consumer state. A grouped member
  must not override it, and a new leader must not recalculate it locally.
- Pin the policy version at first assignment and preserve it through the
  current attempt-limit/dead-letter transition. Updating a policy must not
  silently change an in-flight record.

The slice must define how an unacknowledged local assignment is recovered
before claiming a durable delay. The safe options are to persist a bounded
lease deadline with the assignment or to state explicitly that restart makes
the volatile lease immediately eligible and begins the next policy interval
from recovery. The implementation must choose one, test it, and keep the
choice consistent with the clustered absolute-deadline model; this note does
not silently choose between them.

The slice does not include `hold`, exponential/jittered backoff, explicit
retry/dead-letter dispositions, named or cross-group targets, provenance
metadata, or redrive. Those omissions are deliberate: each introduces a
separate public, storage, or recovery contract. The first slice is useful even
without them because it lets two consumers of the same stream select different
attempt budgets while preserving the existing payload-only dead-letter shape.

Verifiable acceptance tests for this slice are:

1. A protocol fixture can create and inspect a policy, rejects zero attempts,
   invalid durations, and unsupported policy versions explicitly, and proves
   that an unconfigured legacy consumer still uses the broker-wide fallback.
2. A local engine test gives two consumers on one stream different attempt
   limits and shows that each receives its own policy; another test gives one
   grouped consumer two members and shows that both share one policy and
   attempt budget.
3. Local restart tests show that policy version, attempt count, and the chosen
   lease-recovery behavior survive restart; updating the policy leaves an
   already-assigned record on its pinned version and applies the new version
   only to a record with no attempt state.
4. Cluster state-machine and real three-node process tests show that policy,
   attempt limits, and version pinning survive snapshot/restart, follower
   forwarding, leader change, and grouped member replacement. A node with an
   unsupported policy version returns an explicit compatibility failure and
   cannot serve that state.
5. Existing local and clustered dead-letter tests continue to show original
   key/payload preservation, no recursive dead-lettering, stale-token
   fencing, and the current local-versus-clustered movement boundaries. No
   provenance or redrive behavior is inferred from these legacy tests.

This slice is a design gate, not evidence that any of these tests currently
exist. The subsequent stages add the missing schedule, provenance, and
redrive behavior only after this state/compatibility boundary is accepted.

### Stage 0: freeze the semantic fixtures

Define versioned policy and provenance fixtures before changing runtime code:

- validate policy combinations, duration/attempt bounds, default mapping from
  legacy flags, policy-version pinning, and deterministic backoff vectors;
- define `attempt`, `retry_not_before`, `dead_letter_id`, and `redrive_id`
  examples for both independent and shared consumers;
- define explicit outcomes for `held`, `history_unavailable`, target blocked,
  conflict, stale delivery, confirmed success, and unknown result; and
- document whether an explicit retry/dead-letter disposition is in the first
  protocol version or a later additive stage.

Gate: reviewable contract fixtures and compatibility examples; no runtime or
performance claim.

### Stage 1: durable consumer-scoped policy

This stage is the first implementation slice above. Keep the existing
acknowledgement timeout as the retry delay and derived dead-letter behavior;
do not add the later schedule, provenance, or redrive features here.

Implement policy creation/inspection and durable selection for local and
clustered consumers while keeping legacy fallback. Cover:

- independent policies for two consumers on one stream;
- one policy shared by multiple grouped members;
- restart and snapshot recovery of policy versions;
- policy updates, in-flight pinning, and explicit reset behavior; and
- invalid policy and mixed-version responses.

Gate: local engine tests, clustered state-machine tests, real-server protocol
fixtures for capability/compatibility behavior, and no change to existing
payload-only dead-letter records.

### Stage 2: durable schedule and poison handling

Add persistent retry deadlines and policy-selected backoff. Cover:

- fixed, exponential, capped, and deterministic jitter schedules;
- timeout and explicit-retry attempt accounting;
- restart before and after a retry deadline;
- local process restart and cluster leader/ownership transfer;
- stale token rejection and per-key/non-key ordering while a retry is delayed;
- hold versus dead-letter when the attempt limit is reached; and
- bounded scheduler work, blocked-target backpressure, and retention fences.

Gate: focused failure, recovery, ordering, timeout, and resource tests in both
engines; a real three-node process test for ownership transfer. A benchmark is
not required for this design-only note, but an implementation changing the
delivery hot path must assess and run the applicable benchmark or document a
concrete targeted-coverage gap.

### Stage 3: provenance and duplicate-safe automatic movement

Introduce a versioned metadata-capable durable record and populate the
provenance object at the automatic move. Cover:

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

### Stage 4: explicit dispositions and redrive

Add the capability-gated explicit retry/dead-letter and redrive operations.
Cover:

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

### Stage 5: operational acceptance

Add metrics, inspection, alert guidance, and bounded resource tests for large
pending-retry and dead-letter populations. Test that metric labels, logs,
metadata, and scheduler memory remain bounded. Re-evaluate whether the
materialized clustered representation is sufficient before claiming scale.

## Unresolved risks and hypotheses

- **Policy update semantics:** pinning a version is predictable but may delay
  an urgent change to an already-poison record. Reset semantics, authorization,
  and operator visibility need a separate contract.
- **Clock behavior:** durable local and clustered retry deadlines inherit the
  unresolved clock assumptions in TD-020. A schedule must not promise exact
  wall-clock timing across failover until those assumptions are bounded.
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
- **Derived-name recursion boundary:** the local and clustered implementations
  do not currently share the same predicate for identifying hashed derived
  dead-letter streams. A maximum-length source name can expose the local
  mismatch described above; normalize and test this before a policy slice
  claims non-recursive dead-lettering for every valid stream name.
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

This document is a `design or research` change. Its primary evidence class is
design/research, with secondary tags `public-contract`, `storage/recovery`,
`clustered`, `retention`, and `resource-safety`. It makes no runtime,
correctness, durability, latency, throughput, or memory-performance claim.
No benchmark is required for this design-only change. The implementation gates
above name the real-process, crash/recovery, compatibility, and bounded-resource
evidence required before any future runtime change is recommended for merge.

No backlog or ADR is changed here: the backlog outcome remains open and no
wire/storage choice is accepted. TD-018 is updated only to record the newly
verified hashed-derived-target predicate gap and its concrete retirement test;
accepting the policy contract and retiring the debt remain future work after
implementation evidence.
