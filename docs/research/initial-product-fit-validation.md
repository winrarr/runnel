# Initial product-fit validation

- Status: proposed validation plan; not a product-fit claim
- Last reviewed: 2026-09-03
- Scope: validate the audience, workloads, and product promise in
  [product-fit.md](../product-fit.md) against the current single-node and early
  three-node slices.

This note turns the open outcome in [backlog.md](../backlog.md) into a
repeatable validation plan. It does not change the backlog, accept a product
decision, or claim that the current implementation is production-ready.

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
fit claim is made. When the plan is executed, use representative end-to-end
workloads and follow [benchmarking.md](../benchmarking.md); synthetic broker
results alone cannot satisfy the product-fit outcome.

Current conclusion: the repository has credible automated starting evidence for
the three slices, but initial product fit remains `unknown` until intended-user
exercises, pre-registered operating budgets, resource/fault coverage, and a
documented single-node-to-cluster migration result are available. The evidence
package and claim matrix should then drive a backlog update or an explicit
decision to defer adoption claims; this note itself does not warrant either.
