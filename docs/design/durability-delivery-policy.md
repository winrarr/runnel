# Durability and delivery policy boundary

- Status: exploratory design note; not an accepted policy or compatibility decision
- Last reviewed: 2026-09-06
- Baseline: `2a917aeaf442a8970519206309852e12a20ca3c4`
- Scope: separate durable-write, delivery, retention, and overload policy choices
- Related debt: [TD-005](../tech-debt.md#td-005-durability-and-delivery-policies-are-hard-coded), [TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse), and [TD-023](../tech-debt.md#td-023-external-protocol-admission-remains-incomplete)
- Related outcomes: [make message processing complete](../backlog.md#make-message-processing-complete), [make retained data operationally scalable](../backlog.md#make-retained-data-operationally-scalable), and [make clustered durability and outcomes explicit](../backlog.md#make-clustered-durability-and-outcomes-explicit)

This note narrows the meaning of “policy.” The current broker has several
deliberately conservative defaults, but they are not one configurable feature:

1. durable-write policy says when a publish or acknowledgement may be reported
   as committed;
2. delivery policy says when an assignment expires, retries, or reaches a
   terminal outcome;
3. retention policy says when committed history is still available; and
4. overload policy says whether work waits, is rejected, or becomes ambiguous
   when bounded resources are exhausted.

These policies interact, but one must not silently stand in for another. In
particular, a retry limit is not a retention policy, a bounded request queue is
not a disk budget, and a successful local `sync_data` call is not quorum
durability. The note is an evidence map and a set of design boundaries. It
does not add runtime configuration, protocol fields, or an ADR.

## Observed baseline

The following is behavior of the implementation at the baseline above. Rust
code and tests remain authoritative if this note becomes stale.

| Policy axis | Local engine | Clustered engine | What is not established |
| --- | --- | --- | --- |
| Durable publish | A stream append writes the complete frame and calls `File::sync_data` before the operation returns success. Request-aware appends use the same durable write point. | A publish is a replicated `client_write`; the persisted Raft log uses an atomic, fully synced replacement and the state-machine journal is synced before applying committed entries. | No user-selectable durability mode, hardware/filesystem failure model, or public stage-aware outcome contract. |
| Durable acknowledgement | Consumer events append to a bounded JSON-lines journal and call `sync_all` before the in-memory checkpoint advances. Checkpoint compaction is an atomic replacement. | Acknowledgements are replicated state-machine commands. The state-machine journal is synced before the applied state is exposed through the command response. | No configurable acknowledgement durability or measured guarantee across storage devices. |
| Processing lease | The broker-wide `ack_timeout` is both the active in-flight lease and the redelivery delay. Active leases are volatile and restart can redeliver an unacknowledged record. | The broker-wide timeout becomes a leader-selected absolute deadline in the replicated command. Ownership, attempts, and fencing state are replicated; clock quality and configuration consistency remain assumptions. | No independent retry backoff, durable retry schedule, or final clock/fencing policy. |
| Retry and terminal handling | Delivery attempts are persisted before returning a message. An optional broker-wide `max_delivery_attempts` moves an exhausted record to a derived dead-letter stream before source progress advances. The two local writes are at-least-once, with a documented duplicate caveat. | The same broker-wide limit is carried in the poll command. Source progress and the derived dead-letter record commit in one stream data-group transition. | No consumer-scoped policy, explicit failure disposition, provenance, or redrive operation. |
| Retention and replay | All committed stream history is retained; there is no automatic time/size deletion. The first replay operation reads one offset without changing ordinary consumer progress. | Retained messages remain in materialized stream state and the same bounded replay intent is handled through the data-group leader. | No retention floor, replay session, expiry policy, or disk-pressure admission policy. |
| Overload and backpressure | The storage executor has fixed bounded execution, global queue, and per-stream lane limits. Exhaustion returns an I/O `WouldBlock` error. | Network and consensus paths have their own bounded behavior; there is no unified retained-storage or cluster-capacity policy. | No public distinction between queue saturation, disk pressure, retryable failure, and an operation whose response was lost after a possible commit. |

The implementation evidence for these observations is concentrated in the
[local append path](../../crates/runnel-core/src/stream_log.rs),
[local consumer journal](../../crates/runnel-core/src/consumer_state.rs),
[local broker delivery path](../../crates/runnel-core/src/broker.rs),
[bounded local storage executor](../../crates/runnel-core/src/storage.rs),
[clustered publish and delivery calls](../../crates/runnel-raft/src/engine.rs),
[clustered state-machine journal](../../crates/runnel-raft/src/state_machine_store.rs),
and [clustered log persistence](../../crates/runnel-raft/src/log_store.rs).

The accepted semantic boundaries are recorded in [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md),
[ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md),
[ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md), and
[ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md).
The focused tests include the [local restart acknowledgement contract](../../crates/runnel-core/tests/engine_contract.rs),
[local delivery and dead-letter tests](../../crates/runnel-core/src/broker.rs),
and [clustered delivery recovery tests](../../crates/runnel-raft/src/lib.rs).

## Design boundaries

### Durable-write policy

The first public durability contract should name an observable success point,
not expose `sync_data`, `sync_all`, Raft, or a particular file. At minimum it
must answer:

- whether a confirmed publish survives a broker-process restart;
- for a cluster, how many failure domains must retain the committed operation;
- whether an acknowledgement has crossed the same boundary as the progress it
  advances; and
- what the client receives when the connection fails after the broker may have
  crossed that boundary.

An operation-level mode and a deployment-level mode are both plausible. The
choice should be made using representative workloads and failure evidence,
not by exposing every storage flush primitive. A weaker mode, if ever needed,
must be explicitly named and must not be the silent default for existing
durable operations. The [clustered outcome contract](clustered-outcome-contract.md)
already defines the useful distinction between confirmed, rejected, retryable,
and unknown attempts; this note does not redefine it.

### Delivery policy

The processing lease, retry delay, attempt budget, and terminal action are
separate concepts even though the current slice uses one timeout for two of
them. A future policy should be durable and scoped to a named consumer, so all
members of one shared consumer see the same policy while independent fan-out
consumers can differ. Policy changes need a version boundary so an in-flight
record cannot silently acquire a different attempt budget during failover.

The concrete retry and dead-letter proposal, including backoff, terminal
outcomes, provenance, and redrive tradeoffs, belongs in
[application-aware retry policy](application-aware-retry-policy.md). Its
illustrative state and field names are not requirements. The minimum evidence
is semantic: restart and ownership-transfer tests must show that attempts are
counted once, stale acknowledgements remain fenced, terminal movement cannot
skip source progress, and bounded retry state remains recoverable.

### Retention policy

Retention must be an explicit data-lifecycle choice. The current absence of
automatic deletion is an unlimited-retention behavior, not a durable promise
that storage is unbounded. A future policy must state whether time, size,
consumer lag, and replay sessions protect history or make it explicitly
unavailable. It must also distinguish the logical retained-history boundary
from physical cleanup and from consensus-log compaction.

The [retention and disk-pressure design](retention-disk-pressure-plan.md)
contains the current alternatives, competitor evidence, and outcome gates. It
should own the eventual retention and storage-admission decision rather than
adding retention fields to a retry or delivery policy.

### Overload policy

Current bounded request and storage queues are useful safety mechanisms, but
they only bound work already admitted to the process. They do not reserve
filesystem capacity, account for snapshot or cleanup workspace, or by
themselves define whether a timed-out publish may have committed.

A future overload policy should make the following states distinguishable in
the operational and client surfaces:

- rejected before durable work begins;
- waiting within a bounded queue;
- retryable because no durable boundary was crossed;
- unknown because the durable boundary may have been crossed; and
- confirmed after the selected boundary.

The policy must remain bounded under slow consumers and storage stalls. It
must not turn a queue timeout into an implicit data-loss decision. The
[clustered outcome contract](clustered-outcome-contract.md) and
[overload backlog outcome](../backlog.md#make-overload-and-abusive-client-behavior-bounded)
are the appropriate homes for the public outcome and admission work.

## Evidence gates before exposing policy

No policy should be promoted from this note to public configuration until the
following evidence exists for both engines where both engines support the
operation:

1. A contract test states the selected success point and every failure outcome
   that a caller can observe.
2. A process restart test covers a write, acknowledgement, retry, or terminal
   transition at each relevant boundary.
3. Cluster tests cover leader change, follower restart, and response loss when
   the policy is replicated or leader-authorized.
4. Resource tests measure queue depth, retained bytes, retry state, latency,
   and recovery work under a bounded workload. A benchmark must name the
   durability mode, message shape, topology, and failure state as required by
   [the benchmark policy](../benchmarking.md).
5. Inspection and metrics expose the selected policy, its version or
   generation, and enough counters to distinguish protected, delayed,
   exhausted, rejected, and unknown work without unbounded labels.
6. An ADR records compatibility, migration, and rollback consequences before
   the policy becomes a supported public guarantee.

These gates are deliberately outcome-oriented. They do not require a
particular command, module, file layout, timer implementation, or storage
engine.

## Open questions

- Is durability selected per deployment, stream, operation, or some bounded
  combination, and which combinations are worth the operational cost?
- Should a consumer be allowed to change retry policy while records are
  pending, or must a policy version remain pinned until acknowledgement or a
  named administrative reset?
- Should an exhausted record be held, dead-lettered, or made unavailable under
  each supported retention and delivery policy, and how is that choice
  surfaced to an application?
- Which capacity source and reserve are safe across local filesystems,
  snapshots, and clustered replicas without treating a volume claim as free
  broker capacity?
- What operation identity and retention window are sufficient to resolve an
  unknown publish without retaining unbounded producer state?

Until these questions have accepted answers and the evidence gates pass,
Runnel should keep the current conservative defaults and describe them as
implementation behavior, not as selectable product policy.

## Refactor and planning assessment

This review found no safe code refactor that belongs with a documentation-only
policy boundary. The current policy values are deliberately passed through
different engine constructors and state-machine commands; combining them into
a shared configuration type before a public policy contract exists would add
coupling without proving a useful abstraction. The existing [TD-005](../tech-debt.md#td-005-durability-and-delivery-policies-are-hard-coded),
[TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse),
and [TD-023](../tech-debt.md#td-023-external-protocol-admission-remains-incomplete)
records are sufficient; no new debt item is warranted by this review.

## References

- [Durability and outcomes for clustered operations](clustered-outcome-contract.md)
- [Application-aware retry policy](application-aware-retry-policy.md)
- [Retention and disk-pressure design](retention-disk-pressure-plan.md)
- [ADR 0004: first distributed engine](../decisions/0004-multi-raft-first-distributed-engine.md)
- [ADR 0014: local retry and dead-letter policy](../decisions/0014-local-retry-and-dead-letter-policy.md)
- [ADR 0016: clustered retry and dead-letter policy](../decisions/0016-clustered-retry-and-dead-letter-policy.md)
