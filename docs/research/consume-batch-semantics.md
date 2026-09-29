# Consume-batch semantics study

- Status: source-backed exploratory research; no runtime change or accepted API decision
- Last reviewed: 2026-09-28
- Repository baseline: `a5a228e59c2caa19c1a6520cde6a6abdd9e90196`
- Primary evidence class: research/design
- Scope: bounded consume delivery and acknowledgement contracts for the local and early clustered engines
- Related outcome: [Make batching preserve per-record outcomes](../backlog.md#make-batching-preserve-per-record-outcomes)
- Related decisions: [ADR 0013](../decisions/0013-local-shared-consumer-delivery.md), [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md)
- Related performance evidence: [Systems performance research for Runnel](systems-performance-research.md)

This note compares consume-batch contracts against the behavior in the code at
the recorded baseline. It is evidence for later design work, not an API
proposal or a compatibility promise. It does not select batch acknowledgement,
delivery limits, or atomicity semantics.

## Assessment

The safest candidate for an initial Runnel consume batch is bounded delivery
with a distinct receipt and acknowledgement outcome for each record. If
acknowledgement round trips are a measured bottleneck, one acknowledgement
request could carry multiple receipts while preserving per-record validation
and outcomes. That transport optimization must not silently mean “ack all
through this offset” or make a partially processed application batch
all-or-nothing.

The current shared-consumer contract makes batching a semantic extension, not
just a response wrapper: each member has one outstanding delivery, each
delivery has a lease and stale-token fence, and an in-flight key blocks another
delivery with that key. A batch must define which records are assigned together,
when each lease starts, how an uncertain receive is repeated, and whether a
member can own multiple simultaneous deliveries. The clustered path also needs
an explicit replicated transition for assigning or acknowledging several
records.

The backlog outcome already covers this work and calls out partial outcomes,
ordering, bounds, failures, recovery, and latency/resource evidence. Keep that
item open. The near-term next step is a focused design and test matrix; defer a
runtime batch API until that contract and representative measurements are
reviewed. The present evidence does not establish a throughput gain for Runnel.

## Evidence labels

- **Observed** describes code and tests at the recorded repository baseline.
- **Sourced fact** describes behavior in a linked official product document,
  protocol specification, or primary research source.
- **Inference** applies observed and sourced facts to Runnel; it is not an
  accepted decision.
- **Hypothesis** is a possible effect that requires measurement.

## Current Runnel behavior

The provisional wire protocol has `poll` and `poll_group` requests. Each returns
one `message` or `empty` response. A separate `ack` or `ack_group` request
acknowledges one offset. The typed client exposes one-message poll methods and
does not retry a poll after an uncertain transport result. Publish has a
bounded batch shape already: at most 1,024 records and 64 MiB of encoded
request, with ordered per-record results. Its existence does not imply that
consume batches use the same limits or durability behavior. See the
[protocol types](../../crates/runnel-protocol/src/lib.rs),
[typed client](../../crates/runnel-client/src/lib.rs),
[server framing](../../crates/runnel-server/src/protocol.rs), and
[dispatch](../../crates/runnel-server/src/dispatch.rs).

| Boundary | Observed behavior | Batch implication |
|---|---|---|
| Local ordinary poll | `Broker::poll` delegates to grouped delivery with the consumer name as its member. The poll holds the stream lock, selects one candidate, persists the delivery attempt before returning it, and records one in-flight delivery for that member. Repeating the poll returns that member's same active message. | Returning several messages to one member requires an intentional change to its one-outstanding-delivery rule, or a different client/member model. A vector of messages cannot be assumed to be only a serialization change. |
| Local shared delivery | The delivery index tracks in-flight offsets, members, deadlines, and keys. A same-key candidate is blocked while its key is in flight. Ack persistence precedes progress mutation and successful response; acknowledgements can advance out of order. Active leases are in-memory and restart can cause redelivery. `Broker::ack_group` expires overdue entries before looking up the requested delivery, so a token-bearing ack after expiry returns `StaleDelivery` even before a replacement poll. The reusable engine assertion covers that ordering; the local unit test also checks the old token after reassignment. | Each batched delivery needs a separately fenced receipt and bounded lease state. Same-key records cannot be concurrently processed without weakening the current scoped-order guarantee. The single-record deadline-before-reassignment result is already aligned across engines; batch-specific partial outcomes still need definition and tests. |
| Clustered delivery | One grouped poll submits a `PollGroup` Raft write and carries the leader-selected absolute lease deadline. One grouped ack submits an `AckGroup` write. The replicated state keeps in-flight deliveries, attempts, policy, and out-of-order ack state; tokens fence reassignment. `apply_group_ack` observes the replicated lease clock and removes expired entries before checking the token, so the old token is rejected as stale even before a replacement poll. The reusable engine assertion covers this, and a state-machine unit test also checks the deadline and backward-clock floor. | One batch command could reduce protocol and consensus operations per record, but changes the replicated state transition and error surface. A commit/response timeout can make all or part of the assigned set uncertain. Batch acknowledgement still needs explicit per-item results and retry behavior. |
| Progress | Local and clustered consumers track a contiguous committed offset plus out-of-order acknowledged offsets. Grouped acks identify member, offset, and delivery token. The compatibility ordinary ack omits the token. | Prefix acknowledgement is not equivalent to Runnel's existing per-record acknowledgement, especially for shared work that can complete out of order. The ordinary tokenless path also needs an explicit decision if batched deliveries are independently reassigned. |
| Size and timing | The wire listener has a configurable request-frame limit capped at 64 MiB. The typed client has a configurable response-buffer bound, with a 65 MiB default. Consume currently returns one record; it has no batch count, aggregate response-byte, or collection-wait policy. | Future delivery bounds must apply to encoded response bytes as well as record count and decoded payload bytes. A maximum wait adds queueing latency and must fit within request and lease timeouts. |

Relevant code and current semantic decisions are in
[local poll and ack](../../crates/runnel-core/src/broker.rs),
[local consumer state](../../crates/runnel-core/src/consumer_state.rs),
[local candidate selection](../../crates/runnel-core/src/stream_log.rs),
[clustered poll and ack](../../crates/runnel-raft/src/engine.rs),
[replicated delivery transitions](../../crates/runnel-raft/src/delivery.rs),
[ADR 0013](../decisions/0013-local-shared-consumer-delivery.md), and
[ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md).

## Reference behavior

| Source | Sourced fact | What transfers to Runnel, and what does not |
|---|---|---|
| [Apache Kafka consumer configuration](https://kafka.apache.org/41/configuration/consumer-configs/) | `max.poll.records` caps records returned by one `poll`, while the client may have fetched and cached records separately. Fetch bytes, per-partition fetch bytes, and fetch wait are separate controls. | A client-visible delivery batch can differ from a storage or network fetch batch. Runnel should keep those units distinct in both claims and future measurements; Kafka's partition and group-offset contracts do not transfer directly. |
| [Apache Pulsar consumer API](https://pulsar.apache.org/docs/client-libraries/consumers/) and [4.1 messaging semantics](https://pulsar.apache.org/docs/4.1.x/concepts-messaging/) | Batch receive can stop at a message-count limit, byte limit, or timeout. Individual and cumulative acknowledgement are distinct. Shared and Key_Shared subscriptions require individual acknowledgements. Producer batches may be tracked as a single unit; absent batch-index acknowledgement, a partially acknowledged stored batch may be redelivered as a whole. Batch-index acknowledgement has additional memory cost. | Count/bytes/time are useful independent bounds, while batch storage, receive buffering, and ack granularity are separate choices. Shared/keyed work is a strong reason not to infer prefix or all-batch acknowledgement from a batched response. Pulsar's stored producer-batch behavior is not Runnel's current one-record-per-log-record model. |
| [Amazon SQS ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html), [DeleteMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessageBatch.html), and [visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html) | Receive returns up to a count and may return fewer. FIFO message groups prevent later same-group records from being returned while an earlier record is invisible. For FIFO, a stable receive attempt ID can retrieve the same set and receipt handles after response loss within a bounded interval. Delete batch reports per-entry success/failure; HTTP 200 can contain a mixed result. Visibility expiry may cause duplicate processing, and longer timeouts delay retry. | An uncertain batch receive needs a defined replay rule, and a batch ack should report per-record results. Lease/visibility time is part of the batch-size and processing-time contract. SQS's managed queue and FIFO-group rules are not Runnel's offset/checkpoint model. |
| [AMQP 0-9-1 specification](https://www.rabbitmq.com/resources/specs/amqp-xml-doc0-9-1.pdf), section 1.8.3.13, and [RabbitMQ acknowledgement guide](https://www.rabbitmq.com/docs/confirms) | AMQP `basic.ack` can target one delivery or all outstanding delivery tags up to and including a tag (`multiple=true`). RabbitMQ describes this as a way to reduce acknowledgement traffic. | Cumulative ack is a compact prefix contract, not “ack any subset in this response.” Runnel's out-of-order shared acknowledgements make adopting it as the default unsafe without a separately constrained ordered-consumer mode. |
| [Aether logging study](https://www.vldb.org/pvldb/vol3/R61.pdf), [SEDA](https://cs.uwaterloo.ca/~brecht/servers/readings-new/seda-sosp01.pdf), and [The Tail at Scale](https://research.google/pubs/the-tail-at-scale/) | These primary systems studies analyze batching/queue stages and the effect of shared-resource delay on end-to-end tails. They do not report a Runnel-like workload or establish a batch size for this broker. | **Inference:** amortizing request and replicated/durable transitions may improve small-record throughput, while batch collection, larger response buffers, longer lock/transition work, and slow record processing can worsen tail latency and resource pressure. These effects need a Runnel-specific controlled comparison, not a transferred speedup estimate. See the more detailed [Runnel performance research](systems-performance-research.md). |

## Contract alternatives

These are alternatives for further design work, not selected requirements.

| Alternative | Result and failure model | Ordering and fencing | Tradeoff and fit |
|---|---|---|---|
| Bounded batch receive, per-record ack | Return up to configured count and encoded/decoded byte limits, possibly after a maximum collection wait. Each returned message has its own delivery result. Ack remains one record per operation. On response loss, the broker/client needs a repeat rule for the assigned set; unacknowledged entries redeliver after expiry. | Preserve member, offset, token, attempt, and lease per record. Same-key gating stays in force; a batch must not expose multiple same-key records for concurrent processing unless the contract defines ordered serial processing. | Simplest mapping to current per-record failure semantics. Reduces receive round trips but does not reduce ack round trips or clustered ack commits. Several active leases can expire while messages wait in the application batch. |
| Bounded batch receive plus per-item acknowledgement vector | One ack request carries multiple delivery receipts and returns a result for every item, such as accepted, already acknowledged, stale/expired, or rejected. Request/response loss leaves the client uncertain about the vector and needs idempotent resolution. | Every item remains independently fenced. No item is acknowledged just because an earlier or later item succeeded. Shared ordering remains per key and can advance out of order as today. | Can amortize network request cost. A local implementation might persist one event containing the vector; a cluster implementation might apply one Raft command with per-item validation/results. Either makes a new durability/atomicity boundary that needs explicit tests. It does not inherently reduce local disk syncs or consensus log entries unless implemented to do so. |
| Batch-level atomic acknowledgement | The broker acknowledges all batch entries only if the full set validates; otherwise none are acknowledged. A lost response makes the batch result unknown unless retry returns a stable resolution. | Requires the exact batch identity and current ownership for every receipt. Partial application must be impossible or explicitly reported. It cannot make application side effects atomic with broker checkpointing. | May be easy to explain as an all-or-none broker transition, but can cause already-processed records to redeliver when one receipt fails. It broadens atomicity and failure coupling. Not a good default for independent work. |
| Cumulative contiguous-prefix acknowledgement | An acknowledgement of the highest processed offset advances through every earlier delivery in the prefix. An unprocessed gap makes acknowledging a later record unsafe. | Requires one strictly ordered lane and proof that all earlier records in the prefix completed. It conflicts with current shared-consumer out-of-order acknowledgements and unrelated-key parallel progress. | Fewer ack messages and compact state, as in AMQP/RabbitMQ and some stream APIs. Could be explored only as a separately explicit mode with a clear prefix cursor; it must not be inferred from delivery batch order. |
| Auto-ack on fetch | Receipt itself advances durable progress. Processing failure after response cannot be recovered by normal redelivery. | No per-record stale-ack race because no ack remains, but the crash window moves to between delivery and application processing. | Lower broker acknowledgement work but violates the backlog's at-least-once delivery goal for processing. Exclude from the initial durable-consume contract. |

## Cross-cutting requirements for a future design

### Partial and unknown outcomes

An ordered batch response must carry one stable receipt per record, not only a
first/last offset. A transport timeout after assignments commit can leave the
whole returned set unknown; a disconnect while writing a large response may
leave the client with only a prefix. A retry must either recover the same
assignment set using a bounded stable receive-attempt identity, or define that
the member's current in-flight set is returned until resolved. Choosing a new
set on every retry could strand work behind leases or give the caller no way to
identify which deliveries were assigned.

An acknowledgement vector should validate receipts independently and return a
result for each record. If the engine instead makes one all-or-none state
transition, that atomicity must be explicit and retry-safe. A lost ack response
must not lead a caller to assume no progress: applied entries can be resolved
as already acknowledged, while unapplied or expired entries need their own
unambiguous result. In every design, successful ack means durable progress has
reached the engine's existing local or replicated durability boundary.

### Ordering, membership, and expiry

For shared consumers, each delivery must retain its member and generation/token
fence. Acking a batch must reject expired or reassigned tokens item by item, so
an old member cannot commit a later assignment. Member removal or process
restart can redeliver any unacknowledged portion; the previously acknowledged
subset must remain acknowledged.

The current scheduler blocks another in-flight record with the same key. A
batch may contain different keys, but it must say whether records are processed
in response order and whether more than one same-key record can be returned to
one member. Returning same-key records together is only safe if the API
guarantees sequential processing and ack behavior that preserves the key's
order; the broker cannot observe application work that continues after lease
expiry. A conservative first contract would keep at most one unacknowledged
record per key for each consumer, even within a response.

### Bounds, time, and resources

Count, encoded response bytes, decoded payload bytes, and collection wait are
separate bounds. The contract must define an oversized first record so an
otherwise valid record cannot become permanently undeliverable. It should
return fewer than requested when the byte limit or wait expires, with empty
remaining distinguishable from a transport failure. The server's request
timeout and the client's response limit also need to accept the batch without
allocating an unbounded response.

Leases begin when assignment commits, before all consumers necessarily start
processing the returned records. Large batches or slow first items can therefore
expire later items in the same response. Increasing the lease reduces premature
redelivery but delays recovery after a crashed consumer. Any future heartbeat
or lease-extension operation is a separate contract and should not be assumed
by batch receive.

**Hypothesis:** a broker-side batch may amortize JSON framing, state lookup,
local consumer-journal syncs (if acknowledgement persistence is combined), and
cluster `client_write` overhead. It also adds batch response serialization,
payload copies, more in-flight state, and larger per-request work. The local
engine holds a per-stream lock while selecting and persisting delivery state;
filling a large batch could lengthen that critical section. The clustered path
currently commits one grouped poll or ack operation at a time; a multi-record
command could reduce Raft commands but enlarge each state transition and its
apply/response work. No existing measurement isolates these alternatives.

### Local and clustered consistency

The local engine persists delivery attempts before returning each delivery and
persists acknowledgement state before reporting success. Its active lease map is
volatile, so restart may redeliver. The cluster replicates assignment and
acknowledgement state in the stream data group. A future shared contract should
keep these guarantee differences explicit while requiring both engines to
agree on per-record outcomes, same-key exclusion, stale-token rejection,
out-of-order progress, and partial batch behavior. A single batch response
must not imply a stronger all-record durability guarantee than the engine
actually provides.

The pre-reassignment expiry case now has matching single-record behavior in
both engines. Local `Broker::ack_group` expires overdue state before looking up
the supplied token; clustered `apply_group_ack` observes the replicated lease
clock and removes expired state before token validation. In either engine, a
token-bearing grouped ack that first observes the expired lease returns
`StaleDelivery` without requiring a replacement poll. The reusable
`assert_expired_delivery_is_fenced` sleeps through the deadline, immediately
acks the old token and expects that stale result, then polls a replacement and
checks that its new token succeeds while the old token remains stale. The
assertion runs in the [local engine contract test](../../crates/runnel-core/tests/engine_contract.rs)
and [persistent clustered test](../../crates/runnel-raft/src/lib.rs). The local
unit test `expired_group_delivery_rejects_stale_acknowledgement` only checks
the old token after a replacement poll; the cluster unit test
`grouped_ack_preserves_backward_clock_safety_and_fences_expired_tokens` also
checks rejection at the exact deadline and preservation of the lease-clock
floor after a backward time sample. The former local-unit-test ordering is not
evidence of a current engine difference. For batching, the remaining question
is how a vector reports one expired receipt alongside other valid receipts,
not which single-record rule the engines use. See [local delivery tests](../../crates/runnel-core/src/lib.rs),
[shared engine assertion](../../crates/runnel-test-support/src/lib.rs), and
[cluster delivery tests](../../crates/runnel-raft/src/lib.rs).

## Evidence needed before selecting an implementation

The existing Criterion suite measures local one-record `publish/poll/ack`,
shared-consumer polling, keyed delivery, and many in-flight members; it has no
networked consume-batch or three-node replicated consume-batch case. A
performance claim therefore requires a focused, controlled comparison against
the current one-record path after semantics are specified. The standard
benchmark suite may not represent the protocol, response encoding, partial
acks, or clustered state-transition cost.

The minimum comparison should keep durability semantics constant and vary
message size, requested count, aggregate response bytes, collection wait,
member count, key distribution, and acknowledgement pattern. Record throughput,
p50/p95/p99/p99.9 latency, response bytes and allocations where practical,
process memory, local journal sync count, clustered Raft commands/bytes and
apply work, in-flight count, redeliveries, and timeouts. Compare local and
three-node workloads separately; do not use overlapping host measurements as
authoritative latency evidence. The backlog's full tradeoff matrix remains the
acceptance gate.

The existing reusable engine assertion already covers a single-record grouped
ack immediately after lease expiry and before any replacement poll for both
the local and clustered engines. Batch-specific correctness coverage should
include empty/partial batches, count and byte limits, one record at the maximum
supported payload, response loss after assignment, retry of the same receive
identity, disconnect during response, an ack vector with an expired or
reassigned token among valid tokens and an explicit result for each item,
response loss after ack, restart, leader change, lease expiry while later batch
entries wait, member replacement, same-key records, and out-of-order ack
across different keys.
Network and failure tests should start real broker processes. Reusable engine
assertions should preserve topology-free semantics where practical.

## Disposition and gaps

- **Backlog:** consume batching remains unfinished under the existing outcome,
  which names per-record outcomes, partial failures, ordering, bounded
  count/bytes/time, restart and leader-change tests, and latency/resource
  evidence. The existing shared-consumer acceptance criterion for matching
  local and clustered ack results after expiry but before reassignment is now
  covered by the reusable engine assertion; vector outcomes and the other
  batch-specific cases remain open.
- **Near-term vs deferred:** a design note or ADR proposal is reasonable
  near-term work because the outcome is already committed to product fit and
  both engines expose the relevant delivery and durability boundaries. Runtime
  implementation should wait for the receive-retry identity, per-member
  concurrency, key-ordering, acknowledgement-vector, and lease rules to be
  resolved and tested.
- **Performance:** no direct performance change is expected from this research
  note. Throughput improvement is a hypothesis; no magnitude is estimated.
  Batching may improve small-message throughput while increasing tail latency,
  memory, lease expiries, or queueing. The current benchmarks do not quantify
  that tradeoff.
- **Refactor/planning assessment:** the local and clustered acknowledgement
  implementations, reusable engine assertion, focused tests, ADRs 0013 and
  0015, and shared-consumer backlog criterion were inspected. No code refactor
  is warranted: both implementations already follow their accepted expiry
  decisions, and this change corrects the research record. No backlog, debt, or
  ADR update is needed: the existing backlog criterion already records this
  expected result and the test evidence now covers it; the broader batch
  outcome and its unresolved batch-specific rules remain open. No separate
  actionable refactor or tech-debt item was identified.
- **Unresolved evidence:** no workload has established whether network round
  trips, per-record local sync, consensus round trips, JSON/base64 work, or
  client-side processing dominates; no batch size or ack model is selected.

## References

- [Apache Kafka consumer configuration](https://kafka.apache.org/41/configuration/consumer-configs/)
- [Apache Pulsar consumer API](https://pulsar.apache.org/docs/client-libraries/consumers/)
- [Apache Pulsar 4.1 messaging and acknowledgement semantics](https://pulsar.apache.org/docs/4.1.x/concepts-messaging/)
- [Amazon SQS ReceiveMessage API](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html)
- [Amazon SQS DeleteMessageBatch API](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessageBatch.html)
- [Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [AMQP 0-9-1 specification](https://www.rabbitmq.com/resources/specs/amqp-xml-doc0-9-1.pdf)
- [RabbitMQ consumer acknowledgements](https://www.rabbitmq.com/docs/confirms)
- [Aether: A Scalable Approach to Logging](https://www.vldb.org/pvldb/vol3/R61.pdf)
- [SEDA: An Architecture for Well-Conditioned, Scalable Internet Services](https://cs.uwaterloo.ca/~brecht/servers/readings-new/seda-sosp01.pdf)
- [The Tail at Scale](https://research.google/pubs/the-tail-at-scale/)
