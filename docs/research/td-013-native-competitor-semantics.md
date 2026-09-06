# TD-013 native competitor benchmark semantics

Status: scoped research note; no compatibility or ranking decision

Reviewed: 2026-09-06

Baseline revision: `da31af5d55e40e85d491f1f8c5de42f27257f78d`

Scope: review the current native-tool comparison and define a concrete,
implementation-independent path toward comparable broker workloads. This note
does not authorize a new client, protocol, benchmark runner, or product claim.

## Question

The native comparison is useful for learning how Runnel behaves relative to
familiar systems, but it cannot answer "which broker is faster" today. The
tools do not expose the same confirmation, delivery, batching, or latency
boundary. The useful question is therefore narrower: which semantic envelope
must a future workload share before results may be compared, and which results
should remain separate even when the command-line options look similar?

## Primary sources

The following primary product sources were reviewed alongside the repository
at the baseline revision:

- [Apache Kafka 4.3 producer configuration](https://kafka.apache.org/43/configuration/producer-configs/)
  defines `acks` as the producer completion boundary, describes producer
  buffering and batching through `batch.size` and `linger.ms`, and bounds the
  time a record may spend waiting, retrying, or awaiting acknowledgement with
  `delivery.timeout.ms`.
- [Redpanda 26.2 producer configuration](https://docs.redpanda.com/streaming/current/develop/produce-data/configure-producers/)
  documents that `acks=all` normally includes a filesystem flush, while write
  caching changes that boundary. It also describes the interaction between
  batching, in-flight requests, retries, and idempotence. The same option name
  therefore does not establish the same durability semantics as Kafka.
- The [NATS CLI benchmark documentation](https://github.com/nats-io/natscli/blob/main/README.md#benchmarking-and-latency-testing)
  separates Core NATS from JetStream benchmarks and exposes synchronous,
  asynchronous, and batch publish modes plus consumer acknowledgement modes.
  The [JetStream pull-consumer guide](https://docs.nats.io/learn/jetstream/pull-consumers)
  describes fetch batches, continuous consumption, and explicit acknowledgement
  as distinct application choices.
- The [OpenMessaging Benchmark framework](https://github.com/openmessaging/benchmark)
  demonstrates a common workload model with broker-specific drivers. It is a
  useful reference for separating workload intent from adapter mechanics, but
  its supported driver set and Java-oriented framework do not prove that its
  workloads are semantically equivalent for Runnel.

These sources are evidence about the products and reference tooling. They are
not a Runnel compatibility promise, and they do not make internal persistence
mechanisms identical.

## Observed baseline

The current comparison is intentionally a native-tool baseline:

- `compare.py` runs the Runnel host Python socket client, Kafka and Redpanda's
  Kafka performance clients, and NATS's `nats bench js` client in isolated
  containers. The default run uses one broker, one stream or topic, one
  partition where applicable, 10,000 records, 100-byte and 1 KiB payloads, and
  explicit per-container CPU and memory limits.
- Runnel runs one in-flight durable publish or one poll-and-ack sequence at a
  time. Its latency is the client request-to-response path for those operations.
- Kafka and Redpanda publish with `acks=all`, idempotence enabled,
  `linger.ms=0`, and the native producer's default batch size. A native
  consumer performance command measures fetch throughput and does not perform
  an application-level acknowledgement.
- NATS publishes synchronously to a file-backed JetStream stream and consumes
  through a durable pull consumer with explicit acknowledgement, a batch limit
  of one, and synchronous double acknowledgement. Its publish and consume
  paths are therefore not equivalent to the Kafka-family consumer path.
- The three-node mode measures only durable publish for Kafka, Redpanda, and
  NATS JetStream. It uses one partition or stream with replication factor
  three, but it does not include Runnel or any consume/recovery scenario.
- Results carry operation-specific semantic metadata and are validated as
  `apples_to_apples: false`, `ranking_eligible: false`, and `experimental: true`.
  This is the correct disposition for the current evidence.

The current script split and result validation are already clear enough to
support this baseline. The material gap is not missing labels; it is the lack
of a shared, externally verifiable workload contract for a later comparison.

## Comparability envelope

A result should be comparable only when all dimensions in the following key
are equal or the difference is explicitly declared as a separate scenario. A
shared name such as `durable` or `acks=all` is not sufficient.

| Dimension | What must be fixed or declared | Why it changes the result |
| --- | --- | --- |
| Operation | Publish confirmation, consume delivery, application acknowledgement, or recovery | A fetch completion and a durable consumer checkpoint are different outcomes. |
| Confirmation | What the client has learned when the operation returns, including unknown outcomes after a timeout | A fast return may mean only socket acceptance, leader append, quorum replication, or durable consumer progress. |
| Persistence | The broker's documented persistence boundary and any flush or caching mode | Replication and filesystem persistence are separate claims; one may complete before the other. |
| Topology | Broker count, replica count, partition or stream count, placement, and endpoint used | Single-node and quorum paths exercise different work and failure budgets. |
| Delivery | Fan-out versus shared work, pull versus push, redelivery, ordering key, and duplicate policy | Delivery throughput without application progress does not represent a worker workload. |
| Batching | Record and byte limits, linger or wait time, in-flight requests, acknowledgement granularity, and whether partial outcomes exist | Hidden client batching changes both throughput and per-record tail latency. |
| Client | Client implementation and version, connection reuse, serialization, startup and warm-up treatment, retries, and idempotence | Client work can dominate small-message measurements and retries can change duplicates and ordering. |
| Workload | Payload bytes on the wire, key distribution, record count or duration, concurrency, offered rate, and retention state | Different key or concurrency distributions exercise different partitions, locks, and storage paths. |
| Measurement | Clock and start/stop boundaries, warm-up, timeout policy, percentile definition, and resource scope | A broker-only timer cannot be directly compared with an end-to-end client timer. |
| Failure state | Failure injection point, process or node scope, recovery readiness condition, and allowed duplicate/redelivery outcomes | A post-ack crash and a response-loss retry are different correctness and recovery tests. |

The current result schema covers many of these dimensions and correctly
declares the remaining mismatch. A future schema may add fields, but adding a
field alone should not make a comparison ranking-eligible; the adapter must
demonstrate the declared boundary.

## Candidate common workload

The common workload should be a workload intent translated by each broker
adapter, not a forced universal wire protocol. The adapter may use a native
client, but it must expose the same observable outcome and report any weaker
or stronger guarantee.

### Confirmed publish

Publish a fixed ledger of binary payloads with a stable record identity. Use a
single in-flight mode first, then a separately named bounded-pipeline mode.
For each mode, declare whether the confirmation covers local persistence,
replica acknowledgement, or another product-specific boundary. Measure
throughput and end-to-end p50, p99, and p99.9 latency, while recording rejected,
retryable, and unknown outcomes. Keep setup, connection establishment, and
warm-up outside the steady-state result but report them separately.

This gives Kafka, Redpanda, NATS, and Runnel a comparable *client-visible*
publish outcome without pretending their storage engines flush in the same
way. A comparison that requires identical filesystem persistence should remain
unsupported until every adapter can establish that condition.

### Consume with application acknowledgement

Publish the same ledger, create one durable consumer, deliver each record to a
no-op application handler, and acknowledge only after the handler returns. Run
serial and bounded-concurrency variants as separate scenarios. The ledger must
reconcile records delivered, records acknowledged, duplicates or redeliveries,
and final durable progress.

Kafka-family adapters may need a client that controls offset commits rather
than the native fetch performance command. If the adapter cannot establish an
application acknowledgement boundary, it should report a fetch-only scenario
instead of silently entering this class. JetStream's explicit and double-ack
options and Runnel's consumer acknowledgement should be recorded as their
actual confirmation boundaries, including any difference in synchronous
confirmation.

### Bounded batching

Repeat both operations with explicit record-count and byte-count limits. The
workload must report the requested and observed batch size, maximum in-flight
work, acknowledgement granularity, and partial-failure behavior. A batch
operation is not equivalent to a sequence of single-record operations merely
because it contains the same number of records.

Batch scenarios should remain separate from serial scenarios and should not be
collapsed into a composite score. This makes the throughput/latency tradeoff
visible instead of rewarding an unreported buffering delay.

### Response-loss recovery

Deliberately create an ambiguous client outcome after a broker operation may
have committed, then restart or fail the relevant broker process or node. The
driver reconciles the ledger after recovery and classifies each record as
confirmed, rejected, retryable, unknown, duplicate, or redelivered according
to the broker's documented contract. The acceptance condition is no loss of a
confirmed record and no unreported duplicate; exactly-once processing is not
implied.

This scenario should be introduced only after each adapter can run the normal
confirmed-publish and consume-with-ack paths. A native benchmark command that
cannot inject or reconcile this failure state remains a baseline, not a
recovery comparison.

## Evidence and ranking policy

The following gates keep the eventual comparison useful without turning it
into a misleading leaderboard:

1. Compare only within an exact scenario identity consisting of the envelope
   above. If one backend lacks a dimension, show it as unsupported or
   experimental rather than dropping the dimension from the label.
2. Report throughput, latency percentiles, CPU, memory, storage, startup, and
   failure outcomes independently. Do not create a weighted cross-product
   score; its weights would hide product and workload assumptions.
3. Preserve raw output, tool and image versions, configuration, resource
   limits, workload ledger, and environment provenance. A summary without the
   ledger cannot establish loss, duplicates, or acknowledgement coverage.
4. Require repeated runs under controlled resources for performance claims.
   Use the Runnel current-versus-default workflow for Runnel optimization
   claims; competitor comparisons remain positioning and engineering evidence
   as defined by [ADR 0017](../decisions/0017-benchmark-cadence-and-evidence.md)
   and [ADR 0020](../decisions/0020-stable-optimization-evidence.md).
5. Keep native-tool and common-workload histories in separate series until the
   workload identity and semantic evidence are stable. Do not splice the
   current native points into a later common-client trend line.

## Alternatives

- **Keep the native suite only.** This is the lowest-maintenance option and is
  appropriate for exploratory engineering baselines, but it cannot retire
  TD-013 or support a product ranking.
- **Adopt a general framework such as OpenMessaging Benchmark.** This provides
  reusable workload and driver conventions and may reduce adapter plumbing,
  but its execution model, supported drivers, and semantics would still need
  review against Runnel's binary payloads, explicit outcomes, recovery tests,
  and cluster model.
- **Build a project-specific common workload driver.** This gives the best
  control over the ledger, outcome classes, and failure injection, at the cost
  of maintaining adapters and a second benchmark client. It is the strongest
  candidate once Runnel's client and clustered acknowledgement contracts are
  stable enough to serve as a comparison target.
- **Compare broker protocols directly.** This removes some client variance,
  but it measures protocol mechanics rather than the application outcomes that
  motivated Runnel. It is complementary evidence, not a replacement for the
  common workload.

## Recommendation and open questions

**Recommendation:** retain the native comparison exactly as a non-ranking
baseline. Treat the envelope and four workload classes above as outcome gates
for future work. Do not spend implementation effort on a common client until
the Runnel client compatibility contract, clustered application
acknowledgement behavior, and response-loss recovery path are stable enough
that adapters can be tested against durable outcomes rather than moving
semantics.

The following questions remain unresolved:

- Can Kafka offset commits be adapted to the desired per-record application
  acknowledgement and recovery ledger without overstating their granularity?
- What product-level durability label should be used when a broker confirms a
  replicated write but its filesystem flush behavior is configurable or not
  externally observable?
- Should a common workload compare one partition/stream first, then add a
  separately identified sharding scenario, or should the first common target
  include multiple shards from the start?
- Which client startup, warm-up, retry, and connection-reuse costs belong in
  the product-fit view even when they are excluded from steady-state latency?
- What minimum repetition count and tail-latency coverage are affordable for
  the full cross-broker matrix without making it too slow to rerun?

Evidence needed to retire TD-013 is a repeatable common workload or rigorously
equivalent adapter set that answers these questions for confirmed publish,
application-acknowledged consume, batching, and response-loss recovery. Until
then, the current machine-readable guardrails are the right result.

Related records: [TD-013](../tech-debt.md#td-013-native-competitor-benchmark-semantics-are-not-equivalent),
[cross-broker benchmark backlog](../backlog.md#make-cross-broker-benchmark-comparisons-reproducible),
[ADR 0009](../decisions/0009-native-broker-comparison-baseline.md), and
[benchmarking policy](../benchmarking.md).
