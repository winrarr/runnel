# Adaptive handling of hot ordering domains

- Status: exploratory design proposal
- Scope: detecting and scheduling overloaded ordering domains in shared consumers
- Related outcome: [Explore adaptive handling of hot ordering domains](../backlog.md)

This note explores how Runnel could recognize a hot ordering domain and protect
unrelated work without quietly weakening ordering. It is not an ADR, does not
change the protocol or runtime, and makes no performance claim about the
current implementation or any candidate design. The current demand-driven
grouped-delivery path remains the supported behavior until an implementation
and failure-test sequence establishes otherwise.

The working recommendation is to start with observation, bounded scheduler
work, and explicit backpressure while retaining one active delivery per key.
Fixed internal lanes may then be evaluated as a bounded placement mechanism.
Splitting one logical key across concurrent workers should remain an explicit,
separately reviewed contract change; it is not an adaptive optimization that a
broker should infer from traffic.

## Current contract and implementation boundary

The current grouped operation supplies a stream, durable consumer, and
transient member. A successful response contains a record, an opaque delivery
token, and an attempt number; no key owner, partition, or scheduler lane is
public. The local path is [Broker::poll_group](../../crates/runnel-core/src/lib.rs#L791-L909) and its candidate search is [StreamLog::find_candidate](../../crates/runnel-core/src/lib.rs#L1591-L1625). The clustered path applies the analogous committed operation in [apply_group_poll](../../crates/runnel-raft/src/lib.rs#L1564-L1739).

Both engines begin at the committed consumer position and skip acknowledged
offsets, all in-flight offsets, and a keyed record whose key is already in
flight for that shared consumer. Different keys can be delivered concurrently,
but the same key cannot be delivered concurrently. The current grouped path
also allows only one outstanding delivery per member. Selection is demand
driven: a member polls, the broker scans for the first eligible record, and an
empty result means that no record was selected for that request. There is no
durable member registry, assignment response, graceful-leave operation, or
public placement unit.

Delivery attempts are persisted before a message is returned. Expiry makes an
unacknowledged delivery eligible for redelivery, and opaque tokens fence stale
acknowledgements. The maximum attempt count and dead-letter behavior are
broker-wide settings today. Local dead-letter movement crosses two durable
operations and may duplicate a dead-letter record after a crash; clustered
dead-letter movement is committed in the stream data group. Local and
clustered expiry evaluation also have an implementation difference that a
future scheduler must not widen accidentally. See the [current architecture](../architecture.md#delivery-behavior) and the [stable internal work placement exploration](stable-work-placement.md).

These facts impose the design boundary:

- A scheduler can change which eligible record is considered first, but it
  cannot remove the acknowledged, in-flight, or same-key gate.
- A key can have at most one active owner or delivery at a time in a strict
  design. Handoff must wait for completion/expiry or fence the old delivery and
  permit the resulting redelivery; it cannot expose old and new ownership
  concurrently.
- Any scheduler queue, lane map, member state, timer, and hot-domain index must
  have a fixed bound or be rebuildable from authoritative retained state.
- Placement and hotness are internal facts. The public protocol should not
  expose a lane ID, hash range, owner node, or physical offset layout.
- The current in-process operation lock and one-delivery-per-member limit are
  vertical-slice constraints. A scheduler experiment must not attribute a
  benefit to hot-domain handling if it also changes concurrency, batching,
  storage execution, or the acknowledgement boundary.

## What is a hot ordering domain?

For this note, an ordering domain is (stream, shared consumer, ordering key).
The keyless domain is special: current keyless records pass the key gate and do
not receive a per-key ordering guarantee, so they must not be used to claim
strict-key behavior. A hot domain is not merely a popular key. It is a domain
whose admitted arrival, service time, outstanding work, or retry behavior
consumes a disproportionate and sustained share of a bounded scheduling
resource.

The detector should reason over a time window rather than make a permanent
decision from one large batch. For domain k and window W, record:

| Quantity | Meaning | Why it matters |
| --- | --- | --- |
| in_k(W) | Records admitted for k | Offered load; publish rate alone is insufficient if admission rejects work. |
| ack_k(W) | Records durably acknowledged for k | Delivered service, including the application’s processing and acknowledgement path. |
| backlog_k(t) | Admitted but not yet acknowledged records for k | The direct signal of accumulating work. |
| age_k(t) | Age of the oldest unacknowledged record for k | User-visible lag; useful when payload rates differ. |
| busy_k(t) | Time or count for which k is blocked by its active delivery, retry, or handoff | Shows serial service and poison-message effects. |
| candidate_k(W) | Candidate records examined or rejected while seeking work for k | Separates scheduler waste from application or storage time. |
| retry_k(W) | Expiries, redeliveries, stale acknowledgements, and dead letters | Distinguishes a hot workload from a failing or poison workload. |

An implementation may classify a domain as hot when all of the following are
true for a configurable number of consecutive windows: in_k(W) exceeds ack_k(W)
by a configured margin, backlog_k or age_k is rising, and the domain exceeds a
minimum volume. A separate scheduler-hot classification is useful when
candidate_k(W) consumes a large fraction of the selection budget even though
the domain is not application-throughput hot. Thresholds, window length,
minimum volume, decay, and hysteresis are experiment inputs, not defaults
selected by this note.

The detector should be bounded and lossy rather than maintain an unbounded
durable map. A top-K heavy-hitter sketch, sampled key hashes, or a bounded
window index can identify candidates. Exact state is justified only for
currently active deliveries and handoffs. A key that falls out of the sketch
must be allowed to re-enter through normal candidate discovery; losing an
observation must never lose a message or alter an acknowledgement.

## Goals, fairness, and isolation

The goals below are testable properties, not claims about the current system.

### Strict mode goals

- Preserve the current per-key ordering and no-concurrent-delivery guarantee.
- Preserve at-least-once delivery, persisted attempt counts, explicit stale
  token outcomes, out-of-order acknowledgement behavior, and current local vs.
  clustered recovery boundaries.
- Keep a hot key’s serial limit visible. No scheduler should imply that more
  members can increase the processing rate of one strictly ordered key.
- Keep unrelated keys work-conserving: an in-flight or overloaded key should
  not make an eligible cold key wait solely because the selector repeatedly
  revisits the hot key.
- Bound scheduler work, memory, ready queues, timers, and member state even
  when the stream has very high key cardinality or an adversarial key
  distribution.

### Fairness and isolation measures

Fairness must be weighted by offered demand. A cold key that has no backlog
should not receive an artificial share, and a hot key should not be declared
unfair merely because it is inherently serial. For each measurement interval,
report service rate, backlog age, and candidate work for:

- the hottest domain;
- the next 10 or 100 domains, using a fixed bounded top-K set;
- the remaining cold-domain aggregate; and
- each member and internal lane, when those units exist.

Useful acceptance signals are the cold-domain p50/p99/p99.9 delivery age and
poll latency under a hot mix, the ratio of cold service with and without the
hot mix, and a demand-normalized Jain fairness index over domains with positive
offered load. The index is diagnostic, not a universal target: a scheduler
that gives equal service to a domain with one message and a domain with a
continuous backlog is not necessarily fair. Report the max/min normalized
service share and the oldest cold-domain age as companion measures. For active
domains, define normalized service as acknowledged rate divided by offered
rate, then calculate J = (sum of normalized service)^2 / (n times the sum of
squared normalized service), retaining n and the domain-selection rule in the
artifact.

Isolation means that:

- the candidate budget spent on one hot domain is bounded;
- one hot lane cannot consume every member credit or ready-queue slot;
- a slow member can delay its owned work, but does not globally block unrelated
  lanes or members;
- retry and dead-letter activity is visible separately from fresh work; and
- disabling an adaptive policy falls back to a known scheduler state without
  dropping, reordering, or silently acknowledging records.

The design should not promise a numeric isolation threshold before a baseline
has established variance. An implementation experiment can propose a gate such
as “no correctness failure, no unbounded resource growth, and no material
regression in cold-domain p99/p99.9 under the uniform and slow-member controls,”
then record the calibrated threshold in its implementation ADR.

## Bounded scheduling choices

These choices can be combined only when their effects are measured separately.
In particular, placement, candidate indexing, batching, and extra delivery
credits must not arrive in one experiment.

### 1. Demand-driven selection with a bounded candidate budget

Keep the current scan and eligibility predicate, but give each poll or
scheduling turn a bounded candidate-inspection budget. Advance a cursor or
round-robin position after a blocked candidate, and do not repeatedly rescan
the same hot prefix in one turn. A lossy ready hint may reduce work, but the
authoritative log/index must remain able to rediscover work when a hint is
stale.

This is the smallest strict-ordering experiment. It does not assign owners or
move keys, and it preserves the current demand-driven member behavior. Its
tradeoff is that a budget can return empty or defer an eligible record even
when more work exists; the protocol must either retain the current empty
meaning or introduce an explicit, separately reviewed “try again” outcome.
Candidate-budget exhaustion must be observable and bounded, not confused with
consumer lag.

### 2. Fixed virtual lanes with fair lane scheduling

Map each keyed domain to one of a fixed number L of internal lanes. Schedule
lanes with round-robin, deficit, or another bounded fair policy. A key remains
behind one lane until an explicit placement epoch changes, while the lane can
contain many keys. Keyless records need a separate rule because
hashing an offset must not create a new global ordering promise.

Fixed lanes can reduce repeated scan work and make lane-level load and
ownership measurable. They cannot increase the service rate of one hot key;
they can also make matters worse when a hot key collides with many cold keys or
when a slow member owns the lane. Lane count is a resource bound, not a
parallelism guarantee. An internal lane must not be confused with the existing
per-stream storage lane described in [stable-work-placement.md](stable-work-placement.md).

If lanes are assigned to members, use cooperative handoff: stop admitting new
work to a changing lane, let current deliveries acknowledge or expire, advance
an epoch/generation, and only then permit the new owner to claim it. Because
the current API has no assignment response or member join protocol, strict
owner-only polling can return empty to a member that cannot know which member
has work. A first experiment should retain demand-driven claiming or define a
bounded internal router and test the meaning of empty explicitly.

### 3. Direct key affinity

Map each key hash directly to a member using rendezvous or consistent hashing.
This avoids lane collisions and can improve application-state locality, but it
requires more difficult membership and handoff semantics. A direct map also
needs a policy for keyless records and either a bounded heavy-hitter structure
or derived mapping; an unbounded exact key-to-member map would turn key
cardinality into recovery and snapshot cost.

Direct affinity is worth considering only if measurements show that fixed-lane
collisions, rather than candidate scan or request overhead, dominate. It still
cannot parallelize one strict key and can pin unrelated keys to a slow member.

### 4. Bounded credits and backpressure

A scheduler needs a hierarchy of bounds. The current one-outstanding-delivery
per-member rule is the first credit. Future experiments may add bounded
per-member credits, a global scheduler budget, a lane-ready queue cap, and an
optional hot-domain hint budget. Each bound must have an explicit behavior when
full: stop selecting, return the current empty result, or use a documented
admission error. It must not silently drop or acknowledge work.

Increasing credits can improve utilization and batching but raises memory,
redelivery, duplicate side-effect, and tail-latency risk for slow consumers.
Credits must not permit two unacknowledged deliveries for one key unless the
contract explicitly changes. A server-side ready queue is safe only if it is
bounded and rebuildable; a queue entry is a scheduling hint, not durable
consumer progress.

The first comparison should keep the current credit and protocol boundary. A
later fetch window or batch operation is its own design: it must define partial
acknowledgement, per-record retry, timeout, memory, and crash semantics.

## Strict ordering, key splitting, and contract boundaries

There are three different meanings of “split” and they must not be conflated.

| Mechanism | Same original key remains strictly ordered? | Contract effect |
| --- | --- | --- |
| Split a fixed lane or hash range while keeping each key in exactly one child | Yes, if handoff is fenced and the key is not concurrently served by parent and child | Internal placement only; requires durable/committed epoch handling. |
| Let a key choose one of two lanes for each message and allow both lanes to process it | No; messages from one original key can overlap or complete out of order | Changes the ordering contract. It resembles Partial Key Grouping and requires application acceptance. |
| Have the producer publish semantic subkeys such as account/0 and account/1 | Only per subkey; not for the original account sequence | An explicit application-level partitioning choice; the broker cannot infer that the subkeys commute. |

If messages for one logical key have a sequence number and an independent,
serial merge stage can enforce that sequence before side effects, parallel
processing may be possible. The merge stage is then the ordering domain and can
become the same hot bottleneck; it is not evidence that the broker preserved
the original contract.

The Partial Key Grouping research is relevant precisely because it relaxes key
grouping: it uses two candidate workers, local load estimation, and key
splitting to improve skewed stream balance. Its reported gains are evidence
for a different semantic point, not a prediction for Runnel. An opt-in
“parallel subkey” mode would need a new protocol/configuration contract,
producer guidance, sequence or merge semantics, duplicate handling, and an
ADR. It must be benchmarked and tested separately from strict mode.

## Retry, redelivery, and dead-letter interaction

Under strict ordering, a failed first delivery of a hot key blocks later
records for that key. No fair scheduler can legally skip the blocker and
process later same-key records concurrently. This makes retry policy part of
hot-domain behavior:

- **Current expiry and redelivery:** retain the key gate. A later poll can
  redeliver the expired record, with its persisted attempt count and a new
  opaque token as applicable. Redelivery may move between members only after
  the old delivery is expired/fenced according to the engine’s existing
  boundary.
- **Backoff:** a future per-domain backoff can reduce retry pressure but
  increases that domain’s lag and requires bounded timer/state. It must not
  cause fresh later records for the same strict key to bypass the blocker.
- **Dead-letter after a limit:** moving a poison record to the configured
  dead-letter stream can unblock subsequent records, but it is an explicit
  policy decision and changes the source sequence by skipping the failed
  record. Preserve current local at-least-once duplicate risk and clustered
  atomicity until a separate policy change is accepted.
- **Retry priority:** prioritizing retries limits disorder and bounds time to
  recovery, but a constantly failing hot domain can consume scheduler turns.
  Bounded retry work per turn and separate retry counters make this visible.
- **Early operator action:** manually dead-lettering, pausing, or rerouting a
  hot domain can protect the rest of the consumer, but it changes processing
  semantics and needs explicit authorization, auditability, and recovery
  behavior. It must not be hidden behind an automatic detector.

The scheduler must report fresh delivery, redelivery, expiry, stale-ack, and
dead-letter work separately. Otherwise an apparent throughput improvement can
simply be more duplicate processing or earlier poison-message removal.

## Metrics and cardinality policy

These are proposed measurements for a future implementation, not statements
that the current metrics endpoint already exposes them. Metric names are
illustrative; the existing aggregate delivery, retry, dead-letter, admission,
storage, and health metrics remain the compatibility baseline.

| Dimension | Proposed measurements | Bound and interpretation |
| --- | --- | --- |
| Selection work | Candidate records examined; rejected by acknowledged, in-flight, same-key, lane, and budget gates; empty polls; existing-delivery hits | Counters plus histograms for work per poll; report successful and empty polls separately. |
| Hotness | Offered/admitted rate, acknowledged rate, backlog records, oldest backlog age, hot-domain transitions, time in hot state | Use aggregate gauges and bounded top-K/sketch data; never a raw key label for every domain. |
| Fairness/isolation | Cold-domain delivery age and poll latency; hot/cold service shares; lane/member shares; lane collisions; blocked-domain count | Export quantiles from bounded samples or report them in benchmark artifacts; include population and sample count. |
| Scheduling | Ready-hint depth, lane load, owner changes, drain duration, handoff delay, epoch changes, work-steal/claim attempts | Fixed lane labels only if L is bounded; otherwise aggregate and sample. |
| Backpressure | Per-member credit use, global admission, lane queue cap hits, scheduler budget exhaustion, storage-lane wait/rejection | Correlate with CPU, RSS, storage bytes/read work, lock wait, and network queue pressure. |
| Delivery policy | Delivery attempts, expiries, redeliveries, stale acknowledgements, dead letters, retry delay, duplicate dead-letter reconciliation | Keep fresh and retry paths distinct; report local and clustered recovery separately. |
| Recovery | Time to first eligible redelivery, time to drain after restart/handoff, recovered placement epoch, rebuilt hints, records replayed, acknowledged progress before/after failure | Assert that volatile hints can be lost without losing durable progress or attempt counts. |

Do not attach raw key, offset, member name, or payload labels to an unbounded
Prometheus series. A bounded top-K view may use a stable redacted/hash identity
in a diagnostic artifact, but cardinality, retention, and privacy must be
explicit. Metrics must not make every key durable merely to explain a hot key.

## Reference systems and research

These references solve adjacent problems with different contracts. Their
usefulness is in making the tradeoffs explicit, not in making Runnel
compatible with their public topology.

| Reference | Relevant evidence | Difference that matters to Runnel |
| --- | --- | --- |
| [Apache Kafka design documentation](https://kafka.apache.org/41/design/design/) and [Kafka consumer rebalance protocol](https://kafka.apache.org/42/operations/consumer-rebalance-protocol/) | Records with the same key are placed in one partition; a consumer group assigns each partition to one member, preserving partition order while parallelizing other partitions. | Partitions are explicit, provisioned units. A hot key or hot partition remains serial, and changing the partitioning model is visible to producers and consumers. Runnel’s lanes must stay internal and cannot be treated as a public partition count. |
| [Apache Pulsar Key_Shared subscriptions](https://pulsar.apache.org/docs/next/concepts-messaging/) | Messages with the same key or ordering key are sent to one consumer. Sticky, auto-split hash range, and auto-split consistent hashing adjust mapping as consumers join or leave. | Pulsar supplies an explicit consumer-affinity model and discusses mapping changes. Runnel can borrow bounded range movement and drain/fence ideas, but its current transient member API does not define membership or assignment. |
| [Google Cloud Pub/Sub ordered delivery](https://docs.cloud.google.com/pubsub/docs/ordering) | Ordered delivery is per key, different keys are independent, and a hot key is limited by subscriber processing. Pull delivery permits only one outstanding batch per ordering key; the documentation recommends finer-grained keys for hot-key mitigation. | This is a close semantic warning: one key’s serial processing speed is the ceiling in strict mode. Finer-grained keys change application meaning, and ordered delivery adds coordination and latency. |
| [Amazon SQS FIFO message-group delivery](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html) | A MessageGroupId is strictly ordered and processed one at a time; different groups can run concurrently, and a batch can include multiple groups. Additional messages for a group wait for deletion or visibility expiry. | SQS makes ordered groups explicit and does not let a consumer request a specific group. It supports the case for bounded group-level work and shows why a poison/visibility blocker affects one group. Runnel must retain its current per-record token and ack semantics. |
| [RabbitMQ consumer acknowledgements and prefetch](https://www.rabbitmq.com/docs/confirms) | A bounded prefetch window stops delivery when unacknowledged work reaches the limit; increasing prefetch can improve rate but increases client memory and outstanding work. | Prefetch is a useful backpressure control independent of key placement. Runnel should measure credits and batching separately from lanes because more in-flight work changes retry and memory behavior. |
| [Apache Flink key groups](https://flink.apache.org/2017/07/04/a-deep-dive-into-rescalable-state-in-apache-flink/) | Key groups are bounded atomic units of keyed-state assignment, trading rescaling flexibility against indexing and restore overhead. | Fixed lanes are a plausible internal analogue, but Runnel’s durable consumer progress and delivery fencing are not Flink state assignment. A lane can contain colliding keys and cannot split one strict key. |
| [The Power of Both Choices](https://arxiv.org/abs/1504.00788) and [The Power of Two Choices in Randomized Load Balancing](https://doi.org/10.1109/71.963420) | Choosing among a small set of less-loaded workers can reduce imbalance; Partial Key Grouping obtains this by allowing a key to be handled by either of two workers, i.e. key splitting. | The load-balancing result does not preserve Runnel’s current one-worker-at-a-time per-key contract. It is evidence for an opt-in relaxed mode, not for automatic strict-mode scheduling. |
| [The Tail at Scale](https://research.google/pubs/the-tail-at-scale/) | Tail latency becomes dominant as utilization and system size increase; queueing, constrained concurrency, and scheduling interference can affect the tail. | p50 alone cannot validate hot-domain isolation. Runnel’s plan must retain p99 and p99.9, identify queueing and candidate work, and avoid using speculative tail improvements as proof of correctness. |

## Alternatives and tradeoff summary

| Alternative | Ordering contract | Isolation and fairness | State/resource risk | Use as first experiment? |
| --- | --- | --- | --- | --- |
| Keep demand-driven scan | Preserves current strict behavior | Naturally lets fast members request more work; repeated scans may waste turns on a hot prefix | Low new state; scan work can grow with blocked candidates | Yes, as the control and observation baseline |
| Bounded candidate budget/cursor | Preserves strict key ordering | Bounds one domain’s selector work; empty/defer semantics need care | Small bounded cursor/hints; no owner map required | Yes, before placement |
| Fixed virtual lanes | Preserves strict key ordering if each key has one lane and handoff is fenced | Unrelated lanes can progress; collisions and slow owners create head-of-line blocking | Bounded L, but queues, member lifecycle, and epoch recovery add state | Yes, opt-in after baseline |
| Direct key affinity | Preserves strict key ordering with one active owner | Best distinct-key locality; can pin work to a slow member | Key map/handoff can grow with cardinality unless derived or top-K | Only if lane collisions are measured as the bottleneck |
| Larger credits or server batching | Can preserve strict ordering only with per-key serialization and new ack rules | More utilization, but slow/failed consumers hold more work | Memory, duplicates, partial-ack, and timeout pressure | Separate experiment |
| Producer-chosen subkeys | Per-subkey only | Can turn one logical stream into parallel domains | Moves partitioning and merge burden to application | Explicit application contract, not automatic mode |
| Partial Key Grouping / automatic key split | Changes original-key concurrency/order | Strong skew balancing potential | Requires key-choice tracking, sequence/merge semantics, and duplicate policy | No; separate contract and ADR |

The safe progression is: measure the current path; bound candidate work; test
fixed lanes only if scan or locality evidence justifies them; and treat any
parallelism for one logical key as a new product contract. Stable lanes are not
a remedy for a key whose application semantics genuinely require one serial
sequence.

## Staged benchmark and test plan

No benchmark is required for this design-only change because it adds no runtime
path. The following plan applies when an implementation exists. The current
Criterion suite and clustered runner establish small grouped baselines, but
they do not cover hot-key skew, lane movement, grouped slow members, or
failure during handoff. Any authoritative comparison must run sequentially
under the repository’s benchmark lock against the recorded origin/main
baseline; concurrent diagnostics are exploratory only. Follow the
[benchmarking evidence policy](../benchmarking.md).

### Workload matrix

Use deterministic seeds, the current public protocol, the same durability
boundary, and separate scenarios for local and three-node clustered engines.
Start with 100-byte and 1-KiB payloads and 100,000 records; scale to 1 million
records when resource behavior is stable. Keep member population separate from
simultaneous request concurrency.

| Scenario | Controlled workload | Hot-domain question |
| --- | --- | --- |
| Uniform control | 10,000 or 100,000 keys with uniform arrivals; 2, 4, 16, and 64 members; one outstanding delivery per member | Does an adaptive scheduler add overhead without a hot domain? |
| One hot key | 50%, 80%, 95%, and 99% of records use one key; remainder uses 10,000 cold keys | Does the hot key stay serial while cold keys continue to receive service? |
| Many heavy keys | Zipf-like key popularity with exponents 0.8 and 1.2; 10, 100, and 10,000 keys; repeat with fixed lane counts | Are lane collisions or heavy-hitter state the bottleneck? |
| Arrival/service boundary | For the hottest key, set admitted arrival/service ratios around 0.5, 0.9, 1.1, and 2.0; use 0, 1, 10, and 100 ms application delays | Does the detector distinguish sustainable load from a growing backlog and slow worker? |
| Bursty hot key | 10x the steady rate for 1 second every 30 seconds, with hotness decay enabled | How quickly does detection react, and how much churn does hysteresis cause? |
| Keyless mix | 50% keyless records and 50% keyed records under the same member pool | Does keyless scheduling remain independent from key-ordering claims? |
| Slow member | One member delays acknowledgement by 10 or 100 ms while other members remain fast; place the slow member on hot and cold work where possible | Can a slow owner or lane avoid global head-of-line blocking? |
| Poison/retry | A hot key’s first record expires repeatedly, then reaches the configured attempt limit; repeat with an unrelated cold backlog | Are retry, dead-letter, duplicate, and cold-service effects distinguishable? |
| Churn/failure | Join/leave events and process or leader failure while a hot key and cold keys are in flight | Are epochs, handoff, redelivery, and recovery bounded and fenced? |
| High-cardinality/adversarial | 1 million distinct keys, repeated short-lived keys, and deterministic hash-collision candidates for any lane hash | Does state, memory, and metrics cardinality remain bounded? |

### Stages and evidence

1. **Semantic and instrumentation control.** Exercise current local and
   clustered grouped delivery with fixed key traces. Verify same-key overlap is
   zero, key order is preserved, out-of-order acknowledgements remain valid,
   stale tokens fail explicitly, and attempts survive restart. Add only
   aggregate/bounded counters in this stage. Record poll/ack p50, p99, and
   p99.9 separately from end-to-end delivery age.
2. **Selector-only comparison.** Run the existing scan and the candidate
   budget/lane selector over the same preloaded deterministic index without
   payload I/O or persistence in the measured loop. Measure candidate records
   examined, each rejection reason, budget exhaustion, ready-hint rebuilds,
   and selected-record distribution. This isolates scheduler work from storage
   and network effects.
3. **Local real-process behavior.** Use just smoke plus a focused real broker
   process workload with the hot-key matrix. Test slow members, bounded
   credits, queue caps, restart, expiry, stale acknowledgements, and local
   dead-letter duplicate reconciliation. Report backlog count and oldest age
   p50/p99/p99.9, cold-domain delivery age, CPU, RSS, storage bytes/read work,
   lock wait, and admission/storage queue pressure.
4. **Clustered placement and handoff.** After local semantics pass, run the
   same public workload through three real broker processes with just
   cluster-test and a focused workload. Test follower forwarding, leader
   change, owner/member failure, epoch recovery, and node restart with
   acknowledged and unacknowledged deliveries. Placement state must be
   replicated or deterministically rebuilt before measuring its runtime cost.
5. **Fault matrix and soak.** Use the clustered matrix for repeated hot-key,
   slow-member, retry/dead-letter, churn, and recovery cases. Run long enough
   to observe detector hysteresis, lane movement, queue bounds, compaction or
   snapshot behavior, and metric cardinality. Preserve raw artifacts and
   classify every result as stable, noisy/inconclusive, blocked, or regressed.
6. **Optional relaxed-contract experiment.** If key splitting remains
   attractive, benchmark it in a distinct mode with an explicit sequence,
   merge, duplicate, and application contract. Never combine its throughput
   with strict-mode results or describe it as preserving per-key ordering.

Every benchmark artifact should include:

- exact revision and origin/main baseline;
- payload size, key distribution, seed, member population, concurrency,
  service delay, ack timeout, attempt limit, lane/credit settings, and node
  count;
- poll and acknowledgement latency p50/p99/p99.9 with sample counts and
  boundary definitions;
- backlog records and oldest age p50/p99/p99.9 for hot, top-K cold, and
  aggregate cold populations;
- candidate work, rejection reasons, queue depth, lane/member load, and
  detector transition counts;
- CPU, RSS, storage bytes and I/O, lock/storage wait, network traffic, and
  admission rejection/resource pressure; and
- delivery, redelivery, expiry, dead-letter, duplicate, stale-ack, handoff,
  first-redelivery, and full-drain recovery results.

The minimum strict-mode acceptance set is zero same-key concurrency and no
acknowledged-progress regression, with explicit stale-token behavior and
bounded scheduler state under high cardinality. A candidate must also show a
repeatable isolation or scheduler-work benefit in a target workload without a
material uniform, slow-member, retry, or recovery regression. The exact
benefit and regression thresholds belong in the implementation ADR after
baseline variance is known.

## Operator controls

Operator controls should expose safety bounds and a reversible mode, not force
operators to know key hashes or lane ownership. A possible future control set
is:

- demand-driven (the current default), observe-only, and an opt-in bounded
  scheduler mode;
- maximum internal lane count and maximum member/lane queue depth;
- candidate-inspection budget, per-member credit, and global scheduler budget;
- hotness window, minimum volume, threshold, decay, and hysteresis;
- existing acknowledgement timeout and maximum delivery attempts, with any
  future per-consumer or per-domain policy treated as a separate contract;
- a bounded diagnostic top-K view with redacted domain identity; and
- an emergency disable that stops new adaptive assignments, fences or drains
  safely, and returns to a known demand-driven state.

Defaults should remain conservative and invalid bounds should fail at startup
or an explicit configuration boundary. A live control change must define
whether it affects only new assignments or also drains existing lanes. A
configuration change must not reset attempts, shorten a lease without fencing,
or make a previously acknowledged record eligible again.

An operator should be able to answer “is this a hot application domain, a slow
member, a retry storm, a scan problem, or resource admission pressure?” from
aggregate metrics and bounded diagnostics. Automatic key-specific pause,
dead-letter, or key splitting should not be introduced as an unreviewed
operational shortcut.

## Unresolved risks and questions

1. Can the current poll-only member API express stable ownership without making
   an empty response ambiguous or requiring a public assignment operation?
2. Should a hotness decision be local to a broker, replicated in the clustered
   stream data group, or derived independently with a bounded epoch?
3. How can a detector observe high-cardinality keys without making key labels,
   memory, snapshots, or recovery work unbounded?
4. What hysteresis prevents a bursty key from repeatedly entering and leaving a
   mode, causing more handoff and duplicate work than it saves?
5. Does a fixed lane count isolate a hot key, or merely move the bottleneck to
   a lane that also contains cold keys? What collision rate is acceptable?
6. How should lanes be weighted for heterogeneous members without turning
   capacity into a public placement contract or starving slower members?
7. What does a bounded candidate budget return when eligible work exists but
   the budget is exhausted, and can clients distinguish that from true empty?
8. Can a future batch or credit operation preserve per-key serialization,
   partial acknowledgement, attempt persistence, memory bounds, and crash
   recovery together?
9. Should retrying a hot key receive priority over cold fresh work, and how is a
   retry storm bounded without changing at-least-once semantics?
10. Does dead-lettering a blocker provide sufficient isolation, or does the
    application need an explicit pause/quarantine contract?
11. How are local and clustered expiry, token fencing, leader failover, and
    placement epochs aligned without widening their existing semantic
    difference?
12. Can adaptive state be recovered from a snapshot/journal without replaying
    every historical key or making a volatile hint look durable?
13. What adversarial key distributions can force lane collisions, detector
    churn, or expensive top-K updates, and what resource limit fails closed?
14. If relaxed key splitting is ever allowed, what sequence/merge guarantee is
    actually useful to applications, and who owns duplicate side effects?

Until these questions have focused implementation and recovery evidence,
demand-driven strict delivery remains the reference behavior. A hot key should
be made measurable and isolated from unrelated work, not declared parallelizable
by the scheduler.

## References

- [Current architecture](../architecture.md)
- [Stable internal work placement exploration](stable-work-placement.md)
- [Local shared-consumer delivery, ADR 0013](../decisions/0013-local-shared-consumer-delivery.md)
- [Clustered shared-consumer ownership, ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md)
- [Repository testing and evidence gates](../testing.md)
- [Repository benchmarking policy](../benchmarking.md)
