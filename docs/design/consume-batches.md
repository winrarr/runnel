# Consume batches: proposed semantic contract

- Status: proposed for acceptance by [ADR 0030](../decisions/0030-consume-batch-contract.md); runtime API and behavior are not implemented
- Implementation review baseline: `7a20de5f03f6976c3d411f5bdf5f878824c99fc5`
- Linked research reviewed at baseline: `7a20de5f03f6976c3d411f5bdf5f878824c99fc5`
- Primary evidence class: design/research
- Related outcome: [Make batching preserve per-record outcomes](../backlog.md#make-batching-preserve-per-record-outcomes)
- Source study: [Consume-batch semantics](../research/consume-batch-semantics.md)
- Current shared-consumer decisions: [ADR 0013](../decisions/0013-local-shared-consumer-delivery.md), [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md)

## Recommendation

Add a pull operation that assigns a bounded set of records to one consumer
member, and an acknowledgement operation that accepts independent delivery
receipts and returns one outcome per receipt. Keep one active set per member:
a repeated pull returns its still-in-flight records and does not top up the
set. A member may acknowledge any subset; it must resolve the remainder before
requesting another set. This gives a lost pull response a recovery path without
persisting an unbounded response cache or inventing cumulative offsets.

Use the existing per-record receipt fence, per-key exclusion, and contiguous
consumer progress model. The local engine confirms assignment after its
consumer journal sync and acknowledgement after its journal sync. The cluster
confirms both only after the corresponding data-group Raft command commits.
Neither boundary makes application work exactly once.

ADR 0030 proposes these semantic invariants, including the initial
1,024-record request ceiling, but does not freeze the names or fields below as
a wire API. No performance gain is established. Implementation remains gated
on the correctness and performance evidence listed at the end of this design.

## Proposed protocol shape

The candidate wire shape is `poll_batch` and `poll_group_batch`, each with
`stream`, `consumer`, `max_records`, `max_bytes`, and `max_wait_ms`; the grouped
form also carries `member`. Add `ack_batch` and `ack_group_batch`, each with
the relevant stream, consumer, member identity, and a list of
`{offset, delivery_token}` receipts.
The ordinary form uses the consumer name as its member, matching scalar poll.
Every returned message, including an ordinary-consumer message, carries its
opaque receipt token. The typed client exposes the same per-entry results as
the protocol. Each offset retains its pinned consumer policy and attempt
history across retries; a batch may therefore contain receipts with different
policy snapshots. A terminal attempt-limit move continues to follow the
existing engine-specific dead-letter boundary and is not represented as an
acknowledgement-vector entry.

Reject an empty ack list, duplicate offsets, invalid names, and over-limit
lists before changing state. The first implementation caps both a pull set and
an acknowledgement vector at 1,024 entries. These request-shape errors reject
the whole request; delivery-specific receipt failures remain per-entry.

Polling returns messages in increasing offset order, or an empty list. If a
member already has an active set, the poll returns its remaining active
deliveries and does not assign more. Retrying the same member before lease
expiry therefore recovers an uncertain response. After partial acknowledgement,
only the still-active, unacknowledged subset is returned; expired receipts
follow ordinary reassignment rules. A request whose lower limits cannot
hold the live set is rejected without creating new assignments; retry with the
original or larger limits. Expiry is observed using the existing
demand-driven rules before validating the live set; that normal expiry may
release leases, but a limit rejection creates no new assignments. Once the set
is empty, a new poll may assign a new set. Concurrent polls for one member
serialize through the same ledger.

For a new set, `max_wait_ms = 0` returns currently eligible work immediately.
With a positive wait, the broker collects until the record cap is reached,
the next eligible record would exceed the encoded-byte cap, or the deadline is
reached; it then assigns the collected prefix, or returns empty if none became
eligible. An existing active set is returned immediately after bound
validation and is never topped up. The wait is bounded by the request deadline
and happens before assignment, so a timeout while waiting creates no new
assignments. Existing expiry and attempt-limit transitions still follow their
current demand-driven rules. A byte limit too small for the first eligible
record returns an explicit oversized-record error before assignment.

This expands the accepted scalar rule of one outstanding delivery per member
to one outstanding *set* per member. ADR 0030 proposes this semantic change;
no compatibility obligation requires preserving the scalar wire shape.

Each ack response preserves input order and reports one of:

| Outcome | Meaning and client action |
|---|---|
| `confirmed` | This receipt was durably recorded as acknowledged at the engine's boundary; the contiguous checkpoint may still wait for earlier offsets. |
| `already_confirmed` | The offset was already durably acknowledged; treat progress as confirmed, even if another valid receipt advanced it. |
| `rejected` | This receipt is invalid, stale, expired, or not owned by the supplied member; unchanged retry will not help. |
| `retryable` | The engine can prove this request did not apply, such as a pre-commit routing rejection; retry the same receipts after the condition changes. |
| `unknown` | The operation may have crossed its durable boundary; retry the same receipts to resolve each item. |

A lost response after a submitted ack is unknown for every submitted receipt.
Repeating the exact ack request returns `already_confirmed` for committed items
and the applicable result for the rest. A request rejected before dispatch
applies one request-level outcome to all entries. Do not translate a timeout,
disconnect, or generic cluster failure into a rejection without evidence that
the engine did not apply it. This follows the engine outcome distinction in
[ADR 0026](../decisions/0026-semantic-engine-error-classification.md).

Poll results contain only confirmed assignments. A poll rejected before
assignment has no per-message result. If the client loses the response after
submission, it retries for the same member to recover its active set. If that
member's lease expires, or the local broker restarts, another poll may redeliver
the logical records with new receipt tokens and higher attempts. That is an
at-least-once duplicate, not a lost record. After a cluster leader change, a
committed set remains available from replicated state until its lease expires;
an uncommitted or timed-out write is resolved by retrying the same member poll.

## Bounds and scheduling

- Require `1..=1024` records per request, using the existing publish-batch
  count ceiling as an initial protocol cap. Keep this cap subject to workload
  evidence because a consume set also expands per-member in-flight state.
- Bound the fully serialized response, including JSON/base64 expansion,
  metadata, and the line delimiter, by the client's configured response limit
  and the protocol hard limit (`MAX_RESPONSE_BYTES`). The typed client must
  reject or cap `max_bytes` against its own response-buffer setting.
  `max_bytes` counts this same encoded form. If the first eligible record does
  not fit, reject the request before assignment with an explicit
  oversized-record result; do not silently exceed the caller's bound or strand
  it as an empty poll.
- Stop collecting when the record cap, byte cap, or `max_wait_ms` deadline is
  reached. `max_wait_ms = 0` returns immediately; otherwise it must be less
  than the remaining server request deadline so the broker can serialize and
  write a response. The batch wait happens before assignment, outside the
  local stream lock and outside any Raft state-machine apply; lease deadlines
  begin only when the assignment commits. Local and clustered engines need a
  wake-and-recheck path for publish, acknowledgement, and observed expiry;
  register the wakeup before rechecking availability to avoid a missed signal.
  Bound all waits by the request deadline.
- Keep the existing connection, request, response, and in-flight request
  limits as aggregate admission bounds. Do not queue full payload batches
  outside the active request. If limits or a record is invalid, reject before
  creating any delivery state.

If a later record would cross the byte limit, return the smaller prefix
already selected. The scheduler may skip offsets that are already acknowledged
or in flight, so the returned offsets are ordered but need not be contiguous.

## Ordering, acknowledgement, and atomicity

Keep at most one in-flight record with a given non-empty ordering key across
the shared consumer, including within one returned batch. Reserve selected keys
while building the set so it contains no duplicate key. This preserves the
current key exclusion rule: the next same-key record is not assignable until
the prior receipt is acknowledged or expires. Unkeyed records may share a
batch. Messages are returned in offset order, but applications may process and
acknowledge different keys out of order; no execution order is implied by the
array order. Fan-out consumers retain independent progress.

An ack is always per receipt. There is no “ack all through this offset”,
cumulative ack, or implicit ack of omitted records. Each token fences the
assignment for one offset and member. Expired or reassigned tokens are rejected
even before another member polls. The contiguous committed offset advances
only across the acknowledged prefix; later successful receipts remain in the
existing out-of-order set.

Use each offset's pinned policy snapshot for its attempt limit and lease
duration. A policy update affects first assignments made under the new policy;
it does not change the timeout or attempt limit already pinned to an offset.
If the scheduler reaches an offset whose attempt limit is exhausted, retain
the current terminal dead-letter behavior. The move is not a returned batch
receipt and is not included in the ack-vector atomicity guarantee. Local
dead-letter movement remains at least once across its separate log and
consumer-state writes; clustered movement remains within the stream data
group's replicated transition.

The batch is not a transaction with application work, another consumer, a
publish, a different ack request, or a downstream database. Mixed batches may
contain confirmed/already-confirmed entries alongside rejected or unknown
entries. The engine may encode the successful subset as one local journal event
or one replicated command to amortize durability work; if that transition
commits, its successful entries share that engine boundary. This does not
promise all-or-nothing behavior for the batch input or exactly-once application
processing. A process can fail after performing an external side effect and
before its ack commits, so consumers must remain idempotent or reconcile their
own side effects.

## Local and clustered durability boundaries

| Engine | Assignment confirmation | Ack confirmation | Failure/recovery boundary |
|---|---|---|---|
| Local | Persist selected delivery attempts and their pinned policy snapshots as one bounded consumer-journal event and sync it, then install tokened leases in memory and return messages. | Validate receipts independently against one monotonic-time sample, persist accepted offsets as one journal event, sync, then advance progress and remove those leases. | A write or sync error after bytes may have reached storage is `unknown`. Reconcile/reload journal state before another mutation. Active lease ownership and tokens are volatile today, so restart may redeliver with new tokens; attempt counts, policy snapshots, and acknowledged progress remain durable. Dead-letter movement retains its existing at-least-once boundary. |
| Clustered | Submit one `PollBatch` state transition. Return messages only after its Raft entry commits; derive each opaque receipt from the committed entry plus its item identity. Preserve each offset's policy snapshot and derive its deadline from the pinned timeout. | Submit one `AckBatch` state transition that validates each receipt at one replicated lease-clock value and applies the successful subset. Return its ordered outcomes only after commit. | A known pre-submit `NotLeader` is retryable; a timeout or connection loss after submission is unknown. Committed assignment, per-offset deadline and policy, attempts, and ack progress survive restart and leader change through the stream data group. A new leader applies the stored deadlines and token fences. |

The local implementation must not update its cache before journal persistence
succeeds. If a write or sync fails after an append could be visible, it must
reconcile/reload that event or prove that retry and replay remain idempotent
before accepting later polls or acks; a retry must not report state that
conflicts with restart recovery. Cluster results must be computed
deterministically from the committed command and its leader-sampled lease
time. Local lease expiry uses a monotonic clock; clustered
expiry uses the replicated lease-clock floor and absolute deadline described
in [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md). At this
baseline, both engines reject an ack that observes expiry before reassignment;
the shared-engine contract test exercises that case. The refreshed
[consume-batch research note](../research/consume-batch-semantics.md) records
the aligned result and the matching test coverage.

## Ack-vector mixed outcomes and failures

Do not fail the whole ack vector when one receipt is expired or stale. Evaluate
the list at one engine state and one time sample, then return outcomes in input
order. An expired receipt maps to the existing stale-delivery rejection; a
receipt already acknowledged maps to `already_confirmed`; each currently valid
receipt is independently eligible for acknowledgement. This matches the
current scalar behavior: the local engine removes expired leases before
checking a receipt, then returns `StaleDelivery` for a missing or mismatched
group receipt ([local ack path](../../crates/runnel-core/src/broker.rs));
the clustered state machine observes its lease-clock floor, removes expired
leases, and returns `GroupStaleDelivery` for that entry
([cluster ack path](../../crates/runnel-raft/src/delivery.rs)). The
existing per-receipt results must therefore remain meaningful when siblings
are accepted. SQS provides a useful API precedent: batch delete reports
success and failure per entry even when the HTTP request itself succeeds
([AWS `DeleteMessageBatch`](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessageBatch.html)).
Runnel must keep its own durable receipt and lease semantics; the SQS result
shape does not define them.

Persist the eligible subset as one bounded local journal transition or one
replicated data-group command. A mixed completed response can then say, for
example, that an expired receipt was rejected while valid siblings were
confirmed. This atomicity applies only to the accepted acknowledgement subset;
it does not make the entire input vector or application work transactional.
Reject duplicate offsets during whole-request validation before mutation, so
the result for a given offset cannot depend on input order. Preserve input
order in the output even though consumer progress can advance out of order.

For the local journal, a write failure known to occur before any acknowledgement
event bytes are appended can report valid receipts as retryable, while expired
receipts remain rejected. A write or sync failure after append begins is
unknown for every otherwise-valid receipt because the event may be replayable
after restart; it must not be reported as a confirmed prefix. Use one complete
replayable event for the successful subset, rather than several acknowledgement
lines in one append, so recovery can apply the whole subset or none of it. On
any uncertain write, invalidate or reconcile the cached consumer state before
another mutation or journal compaction, and align the in-flight index with
whatever acknowledgements replay as applied. This matters because a member poll
can return its in-flight delivery before consulting cached consumer progress.
Today the local ack path returns directly when `persist_consumer_event` fails,
before updating cached progress or removing the in-flight receipt
([local acknowledgement path](../../crates/runnel-core/src/broker.rs));
the journal loader replays complete newline-terminated events and truncates an
incomplete tail ([journal persistence and replay](../../crates/runnel-core/src/consumer_state.rs)).
If the vector was evaluated before the failure, return those per-item statuses
(`rejected` for expired entries plus `retryable` or `unknown` for the otherwise
valid entries); reserve a whole-request error for failures that prevent item
evaluation. That makes stale-cache handling and complete-event recovery
explicit acceptance requirements for a future vector path.

For the cluster, compute every item result in one committed state-machine
transition using one observed lease-clock value. A committed transition applies
all valid siblings and retains the rejected expired entries as per-item
results. A failure known to precede submission is retryable; after submission,
a timeout, connection loss, or leader change leaves the entire vector unknown
to a client that did not receive its response. Retrying the exact receipt list
resolves committed entries as already confirmed and leaves expired receipts
rejected. A complete server response can preserve mixed item outcomes, but a
client that loses that response must conservatively classify every submitted
entry as unknown until it retries.

| Ack-vector model | Tradeoff |
|---|---|
| Fail the whole vector when any receipt is stale | Avoids mixed state changes, but an expired receipt blocks valid independent work and forces clients to split and resubmit the vector. |
| Run the current scalar ack once per receipt | Reuses proven single-ack behavior, but keeps one local sync or quorum command per item and can stop after a confirmed prefix. The result must then distinguish confirmed, unknown, and not-attempted suffix entries. |
| Return `unknown` for the whole vector on every backend failure | Safest when no response arrives or cluster submission is ambiguous, but discards known stale outcomes after a local vector was evaluated. |
| Return per-item validation results and commit the valid subset in one transition | Recommended: preserves independent outcomes and amortizes the durability boundary. Local event recovery is all-or-none for that subset; a submitted clustered command is reconciled by replaying the exact receipts. |

| Scenario | Completed vector result | Recovery assertion |
|---|---|---|
| One expired receipt before reassignment plus one valid receipt, in either input order | Expired entry is rejected as stale; valid entry is confirmed. | Valid progress is durable; expired work remains eligible for redelivery with a new token. |
| Already-confirmed, valid out-of-order, and expired receipts in one vector | Each result maps to its corresponding input item; out-of-order progress is retained without skipping an unacknowledged gap. | Retrying the same list reports already confirmed for committed work and stale for the expired token. |
| Duplicate offsets or malformed vector | One request-level rejection before state mutation. | No receipt is acknowledged and no lease is removed. |
| Local failure before append; failure after complete event write but before successful sync; incomplete-tail recovery | With a complete vector response, expired entries are rejected and eligible entries are retryable before append or unknown after append. A lost response makes every submitted entry unknown to the client. | Reopen may recover the complete successful subset or none, never a prefix. Exact retry resolves each receipt. Follow an uncertain write with another poll/ack and compaction boundary to prove stale cached state cannot erase the recovered event. |
| Cluster command committed but response lost during leader change | No complete vector result reaches the caller, so all submitted entries are unknown. | Retrying on the new leader returns already confirmed for accepted entries and stale for expired entries; durable progress is not lost or applied twice. |
| Client disconnect or malformed response while reading the vector result | All entries are unknown to the client. | Exact retry returns per-receipt resolution; no automatic retry changes receipt identity. |

The first, second, and fourth rows are required local and state-machine tests;
the leader-change row requires the real three-process cluster test; the final
row requires the real server and typed-client path. Inject local failures both
before append, during a partial event append, and after a complete event write,
then test restart/replay plus subsequent journal compaction rather than
inferring recovery from a returned error alone. Test a committed cluster
command whose reply is lost separately from a known pre-submit routing
rejection.

## Reference designs and alternatives

| Reference | Relevant fact | Runnel consequence and boundary |
|---|---|---|
| [Kafka consumer configuration](https://kafka.apache.org/41/generated/consumer_config.html) | `max.poll.records` limits records returned by one client poll independently of records fetched and cached by the client; fetch bytes and wait are separate controls. | Keep API delivery count/bytes/wait distinct from any future storage prefetch. Kafka's partition assignment and offset commits do not transfer to Runnel receipts or shared keyed work. |
| [Pulsar batch receive and acknowledgement](https://pulsar.apache.org/docs/client-libraries/consumers/) and [4.1 messaging semantics](https://pulsar.apache.org/docs/4.1.x/concepts-messaging/) | Batch receive is bounded by count, bytes, or timeout; individual and cumulative ack are different, and cumulative ack is unavailable for Shared and Key_Shared. | Adopt independent bounds and individual ack for shared work. Do not inherit producer-batch storage or Pulsar's subscription model. |
| [Amazon SQS ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) and [DeleteMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessageBatch.html) | Receive can return fewer than requested; FIFO offers a bounded receive-attempt identity for recovering the same set after response loss. Batch delete reports mixed per-entry results even with HTTP success. | Same-member active-set replay is the first Runnel resolution rule; return per-receipt ack outcomes. SQS's FIFO-only five-minute identity and receipt/visibility model are not Runnel guarantees. |
| [RabbitMQ consumer acknowledgements](https://www.rabbitmq.com/docs/confirms) | A multi-ack acknowledges every outstanding delivery tag through a tag on one channel. | Do not adopt prefix ack: Runnel allows out-of-order completion across keys, so every input receipt is checked separately. |

Alternatives considered: a client-side loop of scalar polls preserves today's
one-in-flight contract but still pays per-record protocol, local sync, and Raft
command cost; prefix/cumulative ack hides gaps and can acknowledge unfinished
work; an all-or-nothing transaction across batch receipts is unnecessary for
independent application records and cannot include consumer side effects; and
background prefetch would decouple application batch size from storage fetch,
but adds hidden buffers and scheduling state before Runnel has evidence for
that architecture. The proposed pull batch is the smallest design that can
amortize the existing assignment and ack boundaries while retaining receipt
semantics.

Unresolved risks are the tail-latency cost of `max_wait_ms`, state and snapshot
growth from outstanding receipts, the work performed when a batch poll crosses
multiple attempt-limited records, and throughput loss when one slow record
keeps a member's active set from being replenished. A hot ordering key remains
intentionally serial. Cluster waiters also need a bounded wake-and-recheck
mechanism that behaves correctly during leadership changes; polling the Raft
state machine while holding apply or stream locks is not acceptable.

## Verification and disposition

Before implementation, add reusable engine-contract and focused local/cluster
tests for ordered partial batches, empty and byte/count truncation, oversized
first records, lower limits against an existing active set, same-key exclusion
within/across batches, out-of-order per-key ack, duplicate and stale receipts,
the mixed-vector resolution matrix above, lost ack response and same-member
poll retry after response loss, disconnect during response, local journal
failure before/after append and restart redelivery, clustered restart and
leader change around commit, and request timeout during collection. Cover
multiple pinned policy snapshots in one set, policy updates across local
restart and clustered leadership transfer, and attempt-limit dead-letter
movement before and among returned records. Verify each engine's existing
dead-letter crash boundary is preserved. Cover collection wakeups after
publish, acknowledgement, and lease expiry, while proving waits stay outside
the local stream lock and Raft apply path. The existing shared-engine contract
already covers single-receipt expiry before reassignment. Real-server tests
must cover wire and typed-client outcome mapping; the local protocol/restart
test and three-process cluster test remain required end-to-end gates.

No performance claim is made by this design. Before recommending an
optimization implementation, compare the scalar path with count, encoded-byte,
wait, payload-size, consumer-count, key-distribution, and ack-pattern variants
on local and three-node engines. Measure throughput, p50/p99/p99.9 latency,
response bytes, memory, local sync count, Raft commands/bytes, in-flight state,
redelivery, and timeout behavior under controlled resources. A concurrent or
microbenchmark-only result is exploratory; use the canonical authoritative
comparison after commit when it meaningfully covers the changed path, otherwise
record the targeted benchmark and coverage gap as required by
[benchmarking policy](../benchmarking.md).

**Recommendation:** accept the semantic contract proposed in ADR 0030. It is
implementable as a protocol/engine vertical slice within the existing local
and single-group replicated design. Runtime work remains gated on the tests
above, including ack journal reconciliation and the preserved dead-letter
boundary.
The existing [batching backlog item](../backlog.md#make-batching-preserve-per-record-outcomes)
already tracks the intended outcome and broad evidence gate, so this decision
does not change its goal or acceptance criteria; keep it open. No separate
tech-debt item is warranted: the journal ambiguity case is a required design
and test gate for this future behavior, not a newly discovered independent
current shortcut. No runtime or performance effect is expected from this
documentation-only decision and research refresh.
