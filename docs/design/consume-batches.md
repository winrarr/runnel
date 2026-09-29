# Consume batches: proposed contract

- Status: implementation-ready design proposal; no runtime API or behavior is accepted yet
- Reviewed against repository baseline: `3d2f2a6a68ef978ed43a0735159f26db332483d9`
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

This proposal selects the initial semantics below; it does not accept a wire
API or claim a performance gain. The names and fields are a concrete protocol
shape for implementation review, not compatibility promises.

## Proposed protocol shape

Add `poll_batch` and `poll_group_batch`, each with `stream`, `consumer`,
`max_records`, `max_bytes`, and `max_wait_ms`; the grouped form also carries
`member`. Add `ack_batch` and `ack_group_batch`, each with the relevant stream,
consumer, member identity, and a list of `{offset, delivery_token}` receipts.
The ordinary form uses the consumer name as its member, matching scalar poll.
Every returned message, including an ordinary-consumer message, carries its
opaque receipt token. The typed client exposes the same per-entry results as
the protocol.

Reject an empty ack list, duplicate offsets, invalid names, and over-limit
lists before changing state. These request-shape errors reject the whole
request; delivery-specific receipt failures remain per-entry.

Polling returns messages in increasing offset order, or an empty list. If a
member already has an active set, the poll returns its remaining active
deliveries and does not assign more. Retrying the same member before lease
expiry therefore recovers an uncertain response. After partial acknowledgement,
only the unacknowledged subset is returned. A request whose lower limits cannot
hold the existing set is rejected without changing delivery state; retry with
the original or larger limits. Once the set is empty, a new poll may assign a
new set. Concurrent polls for one member serialize through the same ledger.

This expands the accepted scalar rule of one outstanding delivery per member
to one outstanding *set* per member. It is a semantic change that requires
review and an ADR update before implementation; no compatibility obligation
requires preserving the scalar wire shape.

Each ack response preserves input order and reports one of:

| Outcome | Meaning and client action |
|---|---|
| `confirmed` | This receipt advanced durable consumer state at the engine's boundary. |
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
| Local | Persist the selected delivery attempts as one bounded consumer-journal event and sync it, then install the tokened leases in memory and return messages. | Validate receipts independently, persist the accepted offsets as one journal event, sync, then advance progress and remove those leases. | A write or sync error after bytes may have reached storage is `unknown`. Reconcile/reload journal state before another mutation. Active lease ownership and tokens are volatile today, so restart may redeliver with new tokens; attempt counts and acknowledged progress remain durable. |
| Clustered | Submit one `PollBatch` state transition. Return messages only after its Raft entry commits; derive each opaque receipt from the committed entry plus its item identity. | Submit one `AckBatch` state transition that validates each receipt and applies the successful subset. Return its ordered outcomes only after commit. | A known pre-submit `NotLeader` is retryable; a timeout or connection loss after submission is unknown. Committed assignment, lease deadline, attempts, and ack progress survive restart and leader change through the stream data group. A new leader applies the stored deadline and token fence. |

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
the shared-engine contract test exercises that case. The older research note
predates the aligned test and should be refreshed before runtime work relies on
its earlier boundary-gap discussion.

## Reference designs and alternatives

| Reference | Relevant fact | Runnel consequence and boundary |
|---|---|---|
| [Kafka consumer configuration](https://kafka.apache.org/41/generated/consumer_config.html) | `max.poll.records` limits records returned by one client poll independently of records fetched and cached by the client; fetch bytes and wait are separate controls. | Keep API delivery count/bytes/wait distinct from any future storage prefetch. Kafka's partition assignment and offset commits do not transfer to Runnel receipts or shared keyed work. |
| [Pulsar batch receive and acknowledgement](https://pulsar.apache.org/docs/client-libraries/consumers/) and [messaging semantics](https://pulsar.apache.org/docs/next/concepts-messaging/) | Batch receive is bounded by count, bytes, or timeout; individual and cumulative ack are different, and cumulative ack is unavailable for Shared and Key_Shared. | Adopt independent bounds and individual ack for shared work. Do not inherit producer-batch storage or Pulsar's subscription model. |
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
growth from many outstanding receipts, and throughput loss when one slow
record keeps a member's active set from being replenished. A hot ordering key
remains intentionally serial. Cluster waiters also need a bounded wake-and-
recheck mechanism that behaves correctly during leadership changes; polling
the Raft state machine while holding apply or stream locks is not acceptable.

## Verification and disposition

Before implementation, add engine-contract and local/cluster tests for ordered
partial batches, empty and byte/count truncation, oversized first record,
same-key exclusion within/across batches, out-of-order per-key ack, duplicate
and stale receipts, a mixed ack result, lost ack response and same-member poll
retry after response loss, disconnect during response, local journal failure
before/after append and restart redelivery, clustered restart and leader
change around commit, deadline expiry before reassignment, and request timeout
during collection. Real-server tests must cover wire and client outcome mapping. The
relevant end-to-end gate is the protocol/restart test and, for clustered
behavior, the three-process cluster test.

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

**Near-term disposition:** implementable as a protocol/engine vertical slice
within the existing local and single-group replicated design, after this
contract receives review. Defer runtime work until the design is accepted, the
scalar/batch member state transition is recorded in an ADR, and the ack journal
reconciliation path is specified in its focused crash tests.
The existing [batching backlog item](../backlog.md#make-batching-preserve-per-record-outcomes)
already tracks the intended outcome and broad evidence gate, so this proposal
does not change its goal or acceptance criteria; keep it open. No separate
tech-debt item is warranted: the journal ambiguity case is a required design
and test gate for this future behavior, not a newly discovered independent
current shortcut. No runtime or performance effect is expected from this
documentation-only proposal.
