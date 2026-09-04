# Initial product-fit validation

- Status: repository validation recorded; product-fit claim remains unknown
- Last reviewed: 2026-09-04
- Scope: validate the audience, workloads, and product promise in
  [product-fit.md](../product-fit.md) against the current single-node and early
  three-node slices.

This note turns the open outcome in [backlog.md](../backlog.md) into a
repeatable validation plan and records the bounded repository checks completed
against the current slices. It does not accept a product decision or claim
that the current implementation is production-ready.

## Evidence vocabulary and budget registration

Keep these categories separate in every result:

- **Current repository evidence** is behavior demonstrated by code, tests,
  scripts, and a run artifact at a named revision. It is not evidence that an
  intended user can operate the system successfully.
- **Hypotheses** are propositions about user value, usability, or an operating
  envelope that the exercises below must test.
- **Unresolved questions** are gaps that must remain visible when a workload
  cannot be evaluated honestly.

Before each run, create a validation manifest containing the revision, binary
and configuration, message size and encoding, message volume, key distribution,
consumer concurrency, retained history, failure injection, hardware, and
numeric budgets. The following names make the required budgets explicit without
inventing measurements or choosing thresholds after seeing results:

| Dimension | Pre-registered budget and pass condition |
| --- | --- |
| Delivery and durability | `B_safety` requires zero loss of a confirmed durable publish, zero successful stale acknowledgements, and zero concurrent delivery of the same requested ordering key. Redelivery of work that was not durably acknowledged is allowed and must be counted. |
| End-to-end latency | Record numeric `B_p95` and `B_p99` limits for publish-to-confirm, publish-to-consume, and consume-to-ack in the manifest. Each observed percentile must be at or below its workload limit under the stated durability mode. |
| Throughput | Record a numeric sustained `B_rate`, workload volume, duration, and allowed error count. The run must sustain the rate without violating `B_safety`, the memory limit, or the storage limit. |
| Memory and in-flight work | Record a numeric `B_rss` peak and the permitted in-flight count. Peak resident memory must stay within the ceiling, and a steady workload or slow consumer must not show unbounded growth. |
| Storage growth | Record the retained logical bytes and numeric `B_disk` ceiling. Report physical bytes, bytes per logical record, and recovery headroom; do not treat an unbounded append-only log as a retention guarantee. |
| Recovery | Record a numeric `B_rto` from failure injection to ready service and a workload-specific replay/recovery window. Confirmed durable data has zero recovery-point loss; allowed duplicates and redeliveries are classified rather than hidden. |
| Operator effort | Record numeric `B_onboard` and `B_recover` time limits plus the critical task list. An intended participant must complete every critical task without editing broker files or learning internal storage/topology concepts. |

The numeric values are part of the signed-off workload manifest and must come
from the intended application or an explicitly documented representative
workload. This note deliberately contains no measured performance numbers.

## Repository validation at the baseline

On 2026-09-04, three bounded checks were run from revision
`3e114d68cb5dab989f33fb5bb5453b0f072fcbf1`. The worktree was clean before the
run, and `origin/main` resolved to the same revision. The latest `ci.yml` run
for that revision was still in progress at the time of the run, so it is not
counted as completed evidence here.

The host was Linux 7.0.0-30-generic on x86_64 with 20 logical CPUs, 31 GiB of
RAM, and 589 GiB free on the 934 GiB filesystem. The process checks used the
repository isolation runner's unique temporary directories, ports, and Cargo
targets, but they did not impose a CPU or memory cgroup. These host details are
provenance, not a resource budget.

No tracked `benchmark-results/` artifacts were present at the baseline. The
repository's benchmark harnesses document how to produce machine-readable
measurements, but no retained run was available to supply product-fit budgets
for this validation.

| Workload slice | Manifest shape and recovery boundary | Exact command and result |
| --- | --- | --- |
| Single-node durable background work | Real broker and CLI; five small UTF-8 source records across `events`, `jobs`, and `poison`, two members of one shared consumer, 50 ms acknowledgement timeout, two delivery attempts, one generated dead-letter record, broker restart, readiness and metrics checks. Messages were unkeyed, so this run did not exercise keyed ordering. | `just isolated smoke` — pass; `Runnel smoke test passed`. |
| Single-node events and offset replay | Real server process with default test configuration; two small UTF-8 records, one acknowledged ordinary consumer, replay of offset 0, unavailable offset 2, broker restart, replay and ordinary poll after restart. | `CARGO_TARGET_DIR=/tmp/runnel-fit-replay.6r2AzN cargo test --locked -p runnel-server --test server_smoke network_protocol_persists_acknowledgements_across_restart -- --exact --nocapture --test-threads=1` — pass; 1 passed, 12 filtered out, test body 0.07 s. |
| Three-node failover and rejoin | Three real static Raft-backed broker processes per scenario, low-volume UTF-8 records, default 50 ms lease for the replication scenario and 5 s lease for reassignment fencing, follower restart, leader/process failure, reassignment, stale-token rejection, quorum continuation, retry and dead-letter recovery. No migration or sustained-load exercise. | `just isolated cluster-test` — pass; all 3 cluster tests passed, 0 failed, test body 22.86 s. |

The replay command used a unique temporary Cargo target for concurrent-run
safety; the isolation runner removed successful temporary state after the
other checks. The commands above are validation workflows, not application
benchmarks: they do not report latency distributions, sustained throughput,
resident-memory samples, storage growth, or participant task times.

### Claim and budget disposition

| Criterion | Disposition from this run |
| --- | --- |
| `B_safety` and semantic delivery behavior | **Pass for exercised assertions:** confirmed messages remained available, acknowledged progress survived restart, shared members received distinct records, retry/dead-letter transitions completed, and stale delivery state was rejected or reported as already acknowledged where the scenario required it. Keyed ordering was not part of the smoke manifest; existing contract and engine tests cover that separate predicate. |
| `B_p95`, `B_p99`, and `B_rate` | **Unknown:** no numeric application budget was registered and these checks do not collect end-to-end distributions or sustained-rate measurements. |
| `B_rss`, in-flight, and `B_disk` | **Unknown:** the host was not resource-limited and these checks did not sample peak resident memory, in-flight work, logical/physical storage growth, or recovery headroom. |
| `B_rto` and replay/recovery window | **Semantic recovery passed within the test harness bounds**, including restart and one-node failover/rejoin. A numeric workload recovery budget was not registered; the cluster harness allows up to 120 s for recovery requests, which is a test timeout rather than a product SLO. |
| `B_onboard` and `B_recover` | **Unknown:** maintainers ran the commands; no intended-user onboarding or recovery exercise was performed, and no operator-effort times or help requests were recorded. |

The three slices therefore provide current repository evidence for the
acceptance surface, but none meets the full product-fit acceptance criterion
that requires numeric operating budgets and intended-user evidence. A green
test is not evidence that an engineer understands consumer groups, replay,
redelivery, stale acknowledgements, or cluster recovery.

### Evidence-supported operating point and explicit boundaries

The following is the current evidence boundary, not a production support
promise:

- A single local broker process can be exercised through the development CLI
  and provisional protocol for small durable background-work and event/replay
  flows. The tested semantics include at-least-once acknowledgement progress,
  independent consumers, shared-member delivery, expiry-based redelivery,
  dead-letter handling, restart recovery, and one-record offset replay with an
  explicit unavailable-history outcome.
- A static three-node development cluster can preserve the tested public model
  across one broker process failure, follower restart, leader election, group
  reassignment, stale-token fencing, and quorum-backed continuation. The
  tests do not establish a supported deployment scale or availability SLO.
- The documented recovery entry points are `just smoke` for the local
  broker/CLI path and `just cluster-test` for the three-process development
  cluster. The current operator must still understand the test/development
  topology and inspect readiness, logs, and metrics; operator effort has not
  been validated with an intended user.

The current evidence does not establish support for a stable production client
compatibility promise, numeric message/connection/in-flight/lag limits,
retention or disk-pressure behavior, sustained resource bounds, time-based or
durable replay sessions, one-node-to-three-node migration, dynamic membership,
network partitions, storage loss, simultaneous failures, multi-region
operation, exactly-once application processing, or large-cluster operation.
Teams requiring those properties should defer adoption or use an established
system until Runnel has the corresponding design, implementation, and
evidence.

## Representative end-to-end workloads

### 1. Single-node durable background work

**Scenario and exercise.** Start with an empty temporary data directory. Create a
`jobs` stream, publish keyed and unkeyed work, and run two members of one
durable consumer. A worker acknowledges successful jobs; another worker is
stopped or allowed to time out; a replacement receives the redelivery. Submit
the old delivery token and record the explicit stale outcome. Send a poison
message through the configured attempt limit, inspect and acknowledge its
dead-letter record, then restart the broker and verify both the acknowledged
checkpoint and the unacknowledged/redelivered work. The participant must use the
documented client or CLI path and inspect readiness and metrics during the run.

**Pass evidence.** The event ledger shows disjoint normal work distribution,
no same-key overlap, durable acknowledgement before progress advances, stale
acknowledgements rejected, and poison work isolated according to the selected
policy. The final inventory accounts for every published record, attempt,
redelivery, dead-letter record, and permitted duplicate. `B_p95`, `B_p99`,
`B_rate`, `B_rss`, `B_disk`, `B_rto`, and operator budgets pass for the
declared workload.

**Current repository evidence.** [`just smoke`](../testing.md) runs a real
broker and CLI through publish, consume, acknowledgement, shared members,
dead-letter handling, restart, readiness, and metrics. The reusable shared
delivery assertions cover disjoint members and out-of-order acknowledgements;
the local and persistent clustered tests cover expiry, token fencing, durable
attempts, and dead-letter recovery ([runnel-test-support](../../crates/runnel-test-support/src/lib.rs),
[`runnel-core` tests](../../crates/runnel-core/src/lib.rs), and
[`runnel-raft` tests](../../crates/runnel-raft/src/lib.rs)).

**Insufficient evidence and backlog mapping.** This validates the acceptance
surface of [Validate the initial product fit](../backlog.md), [Make shared
consumer delivery dependable](../backlog.md), and [Make retry policy
application-aware](../backlog.md), but it does not yet prove user
comprehension, a supported production client, application-selected retry
policy, dead-letter provenance/redrive, or sustained resource-pressure
recovery. Those gaps map to the client, retry-policy, overload, and single-node
deployment outcomes in the backlog.

### 2. Single-node durable application events and replay

**Scenario and exercise.** Publish a retained event sequence once. Run two
independent consumers representing a projection and an audit or integration
feed; allow them to advance at different rates, stop and restart the lagging
consumer, and catch it up. Deliberately replay an already processed offset and
verify that replay does not move the ordinary durable consumer position.
Request history outside the available range and record the explicit outcome.
Repeat after a broker restart. Register the retained-history size, lag, replay
scope, and resource budgets before the run.

**Pass evidence.** Each consumer receives its own durable copy and can recover
from its own checkpoint. Replay is bounded, does not alter ordinary progress,
and reports unavailable history explicitly. The event ledger reconciles source
publishes, each consumer checkpoint, replay reads, and any duplicate application
work. The run meets the declared latency, throughput, memory, storage, recovery,
and operator budgets without claiming retention beyond the selected policy.

**Current repository evidence.** The shared engine contract checks independent
consumer behavior and the additive replay operation; persistent local and
clustered tests check replay and ordinary progress independently across restart
([`runnel-test-support`](../../crates/runnel-test-support/src/lib.rs) and
[`runnel-raft` tests](../../crates/runnel-raft/src/lib.rs)).

**Insufficient evidence and backlog mapping.** This maps to [Make replay an
explicit and safe consumer operation](../backlog.md), [Make retained data
operationally scalable](../backlog.md), and the initial product-fit outcome.
Current evidence covers a bounded offset replay, not a complete retention
policy: time selectors, durable replay sessions, retention floors or pins,
replay-specific observability, and replay-induced resource pressure remain
open. The current tests therefore cannot establish a supported retained-data
operating envelope or prove that a participant understands replay versus
ordinary consumption.

### 3. Three-node failover and the growth path

**Scenario and exercise.** Start the documented static three-node deployment
from clean state. Create a stream, publish durable records, and consume through
more than one public endpoint so follower forwarding is exercised. Inject one
node failure, wait for readiness and elected-service recovery, continue
publishing and consuming on a survivor, allow an unacknowledged delivery to be
reassigned, and check stale-token fencing. Restart the failed node and verify
that the survivor's acknowledgement remains terminal after rejoin. Capture
health, metrics, process exit status, broker logs, and all request outcomes.
Then run the documented one-node-to-three-node migration exercise when that
procedure exists; until it does, record migration as `unknown`, not pass.

**Pass evidence.** Every confirmed record remains available under the selected
cluster durability mode; no acknowledged consumer progress is lost; permitted
redelivery is visible; stale ownership cannot commit; readiness, failure, and
rejoin complete within `B_rto`; and the participant can explain the failure and
recovery path without inspecting Raft state or storage files. The same public
stream, record, consumer, acknowledgement, retry identity, and ordering-key
intent is used before and after the failure.

**Current repository evidence.** [`just cluster-test`](../testing.md) starts
three real Raft-backed broker processes and currently verifies quorum
replication, follower forwarding, grouped and non-grouped delivery,
reassignment after node failure, retry limits, dead-letter recovery, follower
restart, election, post-failure recovery, and recovery metrics. The focused
cluster scenario explicitly checks replacement delivery, stale-token rejection,
terminal acknowledgement after the failed node rejoins, and clustered
dead-letter handling ([`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs)).

**Insufficient evidence and backlog mapping.** This maps to [Run a reliable
three-node development deployment](../backlog.md), [Make growth from one node
to a cluster non-disruptive](../backlog.md), [Make membership and failover
behavior safe](../backlog.md), and the initial product-fit outcome. The current
static cluster is not evidence of a supported migration procedure, dynamic
membership, empty-replica replacement, leader-identity detection beyond the
current test assumptions, broad network or disk fault coverage, or a larger
supported scale. The opt-in replacement experiment remains a separate recovery
boundary.

## Onboarding, operation, and recovery study

Run each workload with at least one engineer who matches the intended small-team
audience and who has not been given broker-internals knowledge. Provide only the
current getting-started and operation instructions. Ask the participant to:

1. choose the stream/consumer model and explain why;
2. start from clean state and complete the workload within `B_onboard`;
3. identify readiness and the relevant metrics while work is in flight;
4. perform the documented stop, restart, replacement, or replay exercise within
   `B_recover`;
5. explain what was confirmed, what may be redelivered, what a stale or
   ambiguous outcome means, and when Runnel is not a fit.

Record elapsed time, commands, errors, requests for help, misunderstood
guarantees, runbook changes, and whether the participant edited files or used
undocumented internals. A workload is not validated by a green automated test
alone: usability is a separate result, and a participant failure should produce
an actionable documentation or product gap.

## Required evidence package

Store one immutable, reviewable package per workload and failure variant:

- a manifest with revision, environment, workload shape, selected durability
  mode, all `B_*` budgets, and the exact command or harness version;
- raw client transcript, broker logs, process exit codes, readiness transitions,
  and failure/rejoin timestamps;
- a message ledger keyed by request/message identity with publish response,
  offset, key, consumer/member, delivery attempt, delivery token, ack outcome,
  replay outcome, dead-letter identity, and final disposition;
- Prometheus snapshots and resource samples for CPU, RSS, in-flight work,
  storage bytes, connections, errors, and consumer lag where available;
- latency and throughput distributions with workload parameters, not just a
  single average;
- the participant worksheet or interview record, including task times,
  confusion points, and reasons to choose or reject Runnel;
- a claim matrix marking each product-fit and backlog criterion `pass`, `fail`,
  or `unknown`, with the supporting artifact and the next action.

The final validation report must state the good-fit workload, the reject or
defer conditions, supported limits, unresolved risks, and the alternative a
team should use when a non-goal is a requirement. Passing one workload must not
be generalized to the others.

## Hypotheses and unresolved questions

### Hypotheses to test

- A small engineering team can get useful durable background work from one
  broker without learning partitions, Raft, storage layout, or manual ownership
  transfer.
- The streams/consumers/acknowledgement model makes at-least-once delivery,
  scoped ordering, redelivery, and dead-letter outcomes understandable enough
  for safe application behavior.
- Independent event consumers can stop, restart, catch up, and replay without
  changing producer behavior or corrupting ordinary consumer progress.
- The early three-node path retires meaningful availability risk while keeping
  the application intent unchanged and the recovery procedure operable by a
  small team.

### Questions that block a product-fit claim

- Which concrete users, payload sizes, arrival rates, retention periods, lag
  limits, failure objectives, and resource ceilings define the initial audience?
- Is the current CLI/provisional protocol only a test harness, or is a supported
  client path available for participant evaluation?
- Which durability modes and ambiguous publish/ack outcomes may be promised to
  an application, especially across a local restart versus a cluster quorum?
- What are the supported message-size, connection, in-flight, retained-history,
  and consumer-lag limits, and what happens under disk pressure?
- What retry, backoff, provenance, redrive, and duplicate policy does an
  application need for each workload?
- Can a one-node deployment be migrated to three nodes with a rollback and
  compatibility procedure that preserves offsets, replay eligibility, and
  consumer progress? What operator state must be transferred?
- Which leader, follower, network, disk, process, and replacement failures must
  be in the supported fault matrix before the cluster is more than a
  development operating point?

## Explicit non-goals for this validation

This plan does not validate or imply support for Kafka compatibility or its
connector/analytics ecosystem, multi-region active-active messaging, hosted
multi-tenant isolation, exactly-once application processing, transactional
database or workflow-engine behavior, unlimited retention/tiering/compaction,
or very large clusters. It also does not rank Runnel against Kafka, RabbitMQ,
NATS, or another broker. Competitor and reference documentation below informs
which semantics and operator questions to test; it is not a performance or
market claim.

## Reference designs and implications for the test shape

These sources are used as reference points, not compatibility targets:

- [RabbitMQ consumer acknowledgements and publisher
  confirms](https://www.rabbitmq.com/docs/confirms) makes the acknowledgement
  boundary, requeue/dead-letter choice, publisher confirmation, and the
  difference between receipt and completed work explicit. The corresponding
  Runnel test must classify stale acknowledgements and dead-letter duplicates,
  rather than assume a generic “delivered” response means processed.
- [Apache Kafka design](https://kafka.apache.org/42/design/design/) describes
  consumer groups for load-balanced work, separate groups for fan-out, key-based
  partition ordering, retained logs, and failure-tolerant replication. Runnel's
  hypothesis is similar at the application-intent level while hiding physical
  partitions and leaders, so the validation must test the same work/fan-out/key
  cases without claiming Kafka compatibility.
- [NATS pull consumers in depth](https://docs.nats.io/learn/jetstream/pull-consumers)
  treats explicit acknowledgement, bounded fetch batches, and fetch expiry as
  part of a usable worker and resource model. This supports recording batch,
  timeout, in-flight, and slow-consumer behavior; Runnel's current poll slice
  does not establish the broader client and batching contract.
- [The Raft paper](https://raft.github.io/raft.pdf), [OpenRaft state-machine
  storage](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html),
  [snapshot replication](https://docs.rs/openraft/latest/openraft/docs/protocol/replication/snapshot_replication/index.html),
  and [dynamic membership](https://docs.rs/openraft/latest/openraft/docs/cluster_control/dynamic_membership/index.html)
  establish why quorum commit, persisted applied state, snapshot installation,
  and controlled membership transitions must be tested as separate recovery
  facts. They do not make Runnel's current static cluster or one-node migration
  supported by implication.

## Benchmark applicability and current conclusion

This is a design/research change with no runtime path change. No benchmark is
required for this note, and no latency, throughput, memory, storage, or product
fit claim is made. Future representative end-to-end performance runs should
follow [benchmarking.md](../benchmarking.md); synthetic broker results alone
cannot satisfy the product-fit outcome.

Current conclusion: the three repository checks provide credible automated
semantic evidence for the selected slices, but initial product fit remains
`unknown` until intended-user exercises, pre-registered operating budgets,
resource/fault coverage, and a documented single-node-to-cluster migration
result are available. The backlog progress note records this partial result;
the evidence does not warrant adoption claims or closure of the outcome.
