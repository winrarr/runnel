# ADR 0030: Define per-record semantics for consume batches

- Status: accepted
- Date: 2026-10-06
- Revalidated against baseline: `c3a894b6d88a40245c1116e2c5006b94f5573aee`
- Primary evidence class: design/research

## Context

At the revalidation baseline, the current local and clustered engines return
one delivery per poll. A member
has one in-flight delivery, every grouped delivery has a lease and receipt
fence, same-key records do not overlap within a shared consumer, and
acknowledgements may advance out of order. Local attempt and acknowledged state
is journaled before confirmation, while active leases are volatile. Clustered
assignment, lease, attempt, policy, and acknowledgement state is committed in
the stream's Raft data group. There is no consume-batch protocol, engine
operation, or typed-client method at this decision's baseline.

The [consume-batch research](../research/consume-batch-semantics.md) compares
official Kafka, Pulsar, SQS, and RabbitMQ documentation with primary systems
research. Those references support separating delivery limits from storage
fetch, returning per-entry results, and making timeout and cumulative-ack
tradeoffs explicit. Their partition, subscription, receipt, and storage
semantics do not transfer to Runnel. The references and current code support a
semantic contract, but establish no Runnel performance gain.

## Decision

Accept a bounded pull batch with independently fenced, per-record
acknowledgements under the following contract:

- A new pull returns messages in increasing offset order, bounded by a request
  count of `1..=1024` and by the fully serialized response size. The byte bound
  includes JSON/base64 expansion, metadata, and the line delimiter, and is
  capped by both the protocol hard limit and the client's configured response
  limit. A first eligible message that cannot fit produces an explicit
  oversized-record error before assignment; if a later message would exceed
  the limit, the broker returns the smaller prefix.
- A member owns at most one active set. A repeated pull returns its remaining
  active deliveries without topping up the set. After partial acknowledgements,
  only still-active, unacknowledged deliveries are returned. If the request's
  count or byte limits cannot hold the live set, reject without creating new
  assignments and let the caller retry with the original or larger limits.
  Normal demand-driven expiry is observed before checking the live set and may
  release leases. Once the set is empty, the member may receive a new set.
  Concurrent pulls for one member serialize.
- A zero collection wait returns currently eligible records immediately. A
  positive wait collects until the count cap is reached, the next eligible
  record would exceed the byte cap, or the deadline expires; it then assigns
  the collected prefix, or returns empty if none became eligible. The wait is
  bounded by the server request deadline and completes before assignment, so a
  timeout while waiting creates no new assignment. Existing active sets return
  immediately after limit validation. Expiry and attempt-limit transitions
  retain their existing demand-driven behavior. Waiting must not hold the local stream
  lock or run inside Raft state-machine apply, and wakeups must be registered
  before availability is rechecked.
- Every record returned by a batch operation carries its own opaque receipt.
  Acknowledgement is never cumulative and never acknowledges an omitted item
  implicitly. A request must carry `1..=1024` receipts; empty vectors,
  duplicate offsets, invalid names, or over-limit lists are rejected before
  state changes. Other receipt checks are independent; completed results
  preserve input order and can mix `confirmed`, `already_confirmed`, and
  `rejected` outcomes.
- Acknowledgements that are valid at the request's state and time sample form
  one successful subset. Persist that subset as one complete local journal
  event or one replicated data-group command. Acknowledgement of the subset is
  atomic at that engine boundary; the input vector, application work, another
  request, and downstream side effects are not transactional. A lost response
  after submission makes every submitted receipt unknown to the client. Exact
  retry resolves committed offsets as already confirmed and leaves stale or
  unapplied receipts with their applicable result. Only failures proven to
  precede application are retryable; timeouts, disconnects, and generic
  post-submission failures are unknown. [ADR 0026](0026-semantic-engine-error-classification.md)
  classifies error kinds and safe retry outcomes but does not identify operation
  stage, so a runtime needs explicit stage evidence to report a local
  pre-append failure as retryable; a generic storage error remains unknown.
- Local assignment and acknowledgement are confirmed only after the
  corresponding consumer-journal sync. If a local append or sync outcome is
  uncertain, the broker must reconcile or reload journal state and align its
  in-flight index before another poll, acknowledgement, or compaction. Active
  leases remain volatile across local restart, so unacknowledged records may
  redeliver with new tokens; attempt counts, per-offset policy snapshots, and
  acknowledged progress remain durable. Clustered assignment and
  acknowledgement are confirmed only after the corresponding data-group Raft
  command commits. A submitted command with a lost response is unknown until
  the exact receipts are retried.
- Preserve the current same-key exclusion rule across the complete consumer:
  at most one in-flight record for any non-empty key. Different keys can be
  acknowledged out of order. Response order does not promise application
  execution order. Each offset continues to use its pinned timeout and attempt
  limit across retry, even when a batch contains offsets with different policy
  snapshots; policy changes do not rewrite an existing offset's policy.
- Existing attempt-limit terminal movement remains per offset and follows the
  current engine boundary. It is not an acknowledgement-vector item. Local
  dead-letter movement remains at least once across its separate target-log
  append and source checkpoint. Its typed move identity is separate from public
  request IDs under [ADR 0029](0029-local-typed-dead-letter-move-identities.md);
  that identity distinction does not make the two writes transactional.
  Clustered dead-letter movement remains in the source data group's replicated
  transition. Batch assignment does not strengthen either recovery claim.
- The protocol operation and field names in the linked design are candidates,
  not accepted compatibility promises. The provisional protocol has no
  backward-compatibility requirement. No exactly-once application guarantee,
  cumulative progress, hidden prefetch, or performance improvement is implied.

## Rationale

One active set per member lets a caller recover a lost pull response without
persisting a separate unbounded response cache or introducing a stable
cross-restart request identity. Per-record receipts preserve Runnel's current
out-of-order progress and key-scoped work while allowing an acknowledgement
vector to report mixed outcomes. A single durable transition for the valid
acknowledgement subset can amortize persistence or replication work without
claiming transactionality across invalid entries or application side effects.

The selected count and encoded-byte bounds constrain a request and response;
the optional wait makes the latency tradeoff explicit. They do not bound all
consumer state or establish a good default batch size. The 1,024-entry ceiling
reuses the current publish-batch protocol ceiling as an initial hard limit and
must be reviewed against consume workloads and state growth before release.

Kafka documents a client poll-record limit separately from underlying fetch
and caching. Pulsar documents batch receive bounded by count, bytes, and wait,
and distinguishes individual from cumulative acknowledgement. SQS reports
batch-delete results per entry and offers a bounded receive-attempt identity
for FIFO response recovery. RabbitMQ's multi-ack is a contiguous prefix. Runnel
adopts none of their queue, partition, receipt, or offset contracts: same-member
active-set replay and per-receipt outcomes follow its current leases, tokens,
and per-key concurrency model. Aether and SEDA explain possible logging and
queueing bottlenecks; The Tail at Scale motivates measuring latency tails.
These papers support measuring the tradeoff, not predicting Runnel's result.

## Expected consequences

- The one-in-flight-per-member rule becomes one-active-set-per-member for the
  new pull operation. A slow unacknowledged record holds its place in that set
  and prevents top-up; same-key work remains serial.
- The local consumer journal and clustered state machine need new bounded
  multi-record transitions and focused recovery coverage. Local uncertain
  appends require cache reconciliation before later state changes.
- Encoded response bytes, collection wait, active delivery state, snapshots,
  journal growth, and attempt-limited candidate processing must be measured.
  No default batch size or speedup is selected from current benchmark evidence.
- The [batching backlog item](../backlog.md#make-batching-preserve-per-record-outcomes)
  remains open until the runtime behavior, failure matrix, and representative
  local and three-node workload evidence are complete.

## Alternatives considered

- **Loop over scalar polls and acknowledgements:** preserves the current
  operation boundaries but keeps per-record protocol and durability costs and
  cannot return one consistent active set after a lost response.
- **All-or-nothing acknowledgement vector:** couples independent work and
  forces a valid receipt to redeliver because a sibling is stale.
- **Cumulative or prefix acknowledgement:** can acknowledge an unprocessed gap
  when different keys complete out of order.
- **Auto-ack on fetch:** moves the crash window before application processing
  and weakens at-least-once processing.
- **Background storage prefetch:** hides buffers and scheduling state before
  Runnel has workload evidence that this architecture is needed.

## Verification gates for implementation

Before a runtime change is ready for review, add reusable engine-contract and
focused local and clustered tests for ordered partial batches; empty, count,
and byte truncation; oversized first messages; lower limits against existing
sets; same-key exclusion within and across sets; out-of-order acknowledgements;
duplicate, stale, expired, already-confirmed, retryable, and unknown receipt
results; exact retry after lost pull and acknowledgement responses; and
response disconnects. Local crash coverage must inject failures before append,
during partial append, and after a complete event write, then reopen, retry,
poll, and compact to prove replay and cached state remain aligned. Cluster
coverage must distinguish pre-submit routing rejection from a committed
command whose response is lost through leader change.

Tests must include multiple pinned policy snapshots in one set, policy updates
across local restart and clustered leadership transfer, attempt-limit
dead-letter movement before and among returned records, and the existing
engine-specific dead-letter recovery boundaries. Test collection wakeups after
publish, acknowledgement, and observed expiry, and prove waits remain outside
the local stream lock and Raft apply path. Real broker processes and the typed
client must cover wire mapping, response loss, timeouts, restart, and cluster
leader change. The single-record expiry-before-reassignment engine assertion
remains relevant but does not replace vector tests.

The implementation must not claim a performance improvement until controlled
local and three-node comparisons vary message size, requested record count,
encoded response bytes, wait, member count, key distribution, and ack pattern.
Report throughput, p50/p99/p99.9 latency, response size, memory, local syncs,
Raft commands and bytes, in-flight state, redelivery, and timeout behavior.
Follow [benchmarking policy](../benchmarking.md) for authoritative evidence.

## References

- [Consume-batch semantics research](../research/consume-batch-semantics.md)
- [Consume-batch contract design](../design/consume-batches.md)
- [Local shared-consumer delivery, ADR 0013](0013-local-shared-consumer-delivery.md)
- [Local retry and dead-letter policy, ADR 0014](0014-local-retry-and-dead-letter-policy.md)
- [Clustered shared-consumer ownership, ADR 0015](0015-clustered-shared-consumer-ownership.md)
- [Clustered retry and dead-letter policy, ADR 0016](0016-clustered-retry-and-dead-letter-policy.md)
- [Semantic engine error classification, ADR 0026](0026-semantic-engine-error-classification.md)
- [Consumer-scoped retry policy, ADR 0027](0027-consumer-scoped-retry-policy.md)
- [Local typed dead-letter move identities, ADR 0029](0029-local-typed-dead-letter-move-identities.md)
