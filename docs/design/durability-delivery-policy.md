# Durability and delivery policy boundary

- Status: exploratory design note; not an accepted policy or compatibility decision
- Last reviewed: 2026-09-29
- Baseline: `c1d0766cd0ac3e844e9763e261cebc03d539cd3a`
- Scope: separate durable-write, delivery, retention, and overload policy choices
- Related debt: [TD-005](../tech-debt.md#td-005-durability-and-delivery-policies-are-hard-coded), [TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse), and [TD-023](../tech-debt.md#td-023-external-protocol-admission-remains-incomplete)
- Related outcomes: [make message processing complete](../backlog.md#make-message-processing-complete), [make retry policy application-aware](../backlog.md#make-retry-policy-application-aware), [make retained data operationally scalable](../backlog.md#make-retained-data-operationally-scalable), and [make clustered durability and outcomes explicit](../backlog.md#make-clustered-durability-and-outcomes-explicit)

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
particular, a retry limit is not a retention policy, bounded protocol
admission is not a disk budget, and a successful local `sync_data` call is not
quorum durability. This note records current behavior and separates accepted
decisions from open design boundaries; it does not add runtime configuration,
protocol fields, or an ADR of its own.

## Observed baseline

The following is behavior of the implementation at the baseline above. Rust
code and tests remain authoritative if this note becomes stale.

| Policy axis | Local engine | Clustered engine | What is not established |
| --- | --- | --- | --- |
| Durable publish | A stream append writes the complete frame and calls `File::sync_data` before the operation returns success. Request-aware appends use the same durable write point. A publish batch preserves ordered per-record outcomes and does not imply batch atomicity. | A publish is a replicated `client_write`; the persisted Raft log uses an atomic, fully synced replacement and the state-machine journal is synced before applying committed entries. A successful mutation is returned from the applied leader result; followers may apply it later. | No user-selectable durability mode, hardware/filesystem failure model, or public stage-aware outcome contract. |
| Durable acknowledgement | Consumer events append to a bounded JSON-lines journal and call `sync_all` before the in-memory checkpoint advances. Checkpoint compaction is an atomic replacement. | Acknowledgements are replicated state-machine commands. The state-machine journal is synced before the applied state is exposed through the command response. | No configurable acknowledgement durability or measured guarantee across storage devices. |
| Processing lease | A configured consumer's `ack_timeout_ms`, or the broker-wide legacy fallback, determines its in-flight lease. Local leases use process-local monotonic deadlines; expiration is observed on a later poll, and restart can redeliver an unacknowledged record. | The selected consumer policy or legacy fallback determines a leader-selected absolute deadline carried by the replicated poll command. Assignment, deadline, attempt, and fencing state are replicated. Expiration is acted on by a later poll command; the persisted lease-clock floor prevents backward evaluation but does not remove wall-clock skew or forward-jump assumptions. | No separate retry schedule or backoff; clustered real-time expiry without a subsequent committed command and final clock/fencing semantics are unresolved. |
| Retry and terminal handling | Delivery attempts and the policy snapshot selected on first assignment are persisted before returning a message. A consumer may configure an optional positive attempt limit (`None` means unlimited); unconfigured consumers use the broker-wide legacy fallback. At exhaustion, a stable source-stream/consumer/offset move identity deduplicates a retried target append when its key and payload match, then source progress is persisted. The target append and source checkpoint remain separate durable writes, so the local path is not atomic and does not have a blanket duplicate-free guarantee. | Consumer policy, per-offset policy snapshots, attempts, and assignment state live in replicated group-consumer state. An optional consumer-scoped attempt limit (`None` means unlimited) overrides the broker-wide fallback. On exhaustion, the derived record and source progress are applied together in the source stream's Raft data-group transition. | No independent retry backoff, explicit failure disposition, provenance, named dead-letter target, or redrive operation. |
| Retention and replay | All committed stream history is retained; there is no automatic time/size deletion. The first replay operation reads one offset without changing ordinary consumer progress. | Retained messages remain in materialized stream state and the same bounded replay intent is handled through the data-group leader. | No retention floor, replay session, expiry policy, or disk-pressure admission policy. |
| Overload and backpressure | The server bounds connections, request-frame bytes, in-flight request/response work, and request duration, rejecting connection or request saturation rather than queueing protocol work. The storage executor has fixed bounded execution admission and per-stream FIFO lanes; exhaustion returns an I/O `WouldBlock` error. | Consensus and peer forwarding use bounded per-peer connection pools and fallback permits with a reserved control lane and request TTLs; these are transport bounds, not a cluster-wide capacity policy. | No public distinction between queue saturation, disk pressure, retryable failure, and an operation whose response was lost after a possible commit. |

The implementation evidence for these observations is concentrated in the
[local append path](../../crates/runnel-core/src/stream_log.rs),
[local consumer journal](../../crates/runnel-core/src/consumer_state.rs),
[local broker delivery path](../../crates/runnel-core/src/broker.rs),
[bounded local storage executor](../../crates/runnel-core/src/storage.rs),
[clustered publish and delivery calls](../../crates/runnel-raft/src/engine.rs),
[clustered state-machine journal](../../crates/runnel-raft/src/state_machine_store.rs),
and [clustered log persistence](../../crates/runnel-raft/src/log_store.rs).
The shared [`ConsumerPolicy`](../../crates/runnel-engine/src/lib.rs#L80)
validates configured timeouts from zero through seven days and rejects a zero
attempt limit. The provisional protocol exposes configure and inspect
operations for an existing stream. Local configuration is journaled per
consumer, while the policy used for an offset is pinned with its first
persisted delivery attempt
([local configuration and polling](../../crates/runnel-core/src/broker.rs#L220),
[consumer state and pinning](../../crates/runnel-core/src/consumer_state.rs#L13)).
The clustered equivalent is part of replicated group-consumer state and the
group poll transition ([configuration command](../../crates/runnel-raft/src/state_machine.rs#L365),
[policy selection and delivery](../../crates/runnel-raft/src/delivery.rs#L166),
[leader-selected lease deadline](../../crates/runnel-raft/src/engine.rs#L610)).
Protocol admission and peer-transport bounds are covered by the
[server admission tests](../../crates/runnel-server/tests/admission.rs),
[storage executor tests](../../crates/runnel-core/src/storage.rs), and
[peer transport tests](../../crates/runnel-raft/src/network/outbound.rs).

At the engine boundary, [ADR 0026](../decisions/0026-semantic-engine-error-classification.md)
accepts `BrokerError::kind()` and `BrokerError::outcome()` as the stable
semantic failure boundary: a successful result is confirmed; resource-not-ready
and routing failures are retryable; validation, configuration, missing-resource,
history, and delivery rejections are rejected; and generic storage, state,
internal, corruption, and cluster failures are unknown. The classification is
conservative because the engine cannot prove non-application for those generic
failures ([implementation](../../crates/runnel-engine/src/lib.rs#L285)). The
provisional protocol still uses its v1 error codes and does not expose engine
outcomes or operation stages. The reusable client does not automatically replay
an operation; failures after request writing may have begun are treated as
unknown, while known pre-request failures and the explicit saturation response
can be retryable. This is accepted source-level behavior, not a selectable
durability policy.

The accepted semantic boundaries are recorded in [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md),
[ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md),
[ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md),
[ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md),
[ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md),
[ADR 0019](../decisions/0019-clustered-storage-identity.md),
[ADR 0026](../decisions/0026-semantic-engine-error-classification.md), and
[ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md).
The focused tests include the [local restart acknowledgement contract](../../crates/runnel-core/tests/engine_contract.rs),
[local attempt and dead-letter tests](../../crates/runnel-core/src/lib.rs#L903),
and [clustered delivery and recovery tests](../../crates/runnel-raft/src/lib.rs#L1135).
Consumer-policy tests cover local consumer isolation, policy version pinning,
restart persistence, and dead-letter exhaustion
([local engine test](../../crates/runnel-core/src/lib.rs#L983),
[persistent Raft engine test](../../crates/runnel-raft/src/lib.rs#L1442)).
Real-process protocol coverage includes [client outcome and stable-identity
retry tests](../../crates/runnel-server/tests/client_retry.rs), [local server
restart and dead-letter tests](../../crates/runnel-server/tests/server_smoke.rs),
the [typed-client consumer-policy test](../../crates/runnel-server/tests/client_path.rs#L132),
and [three-process cluster failure tests](../../crates/runnel-server/tests/cluster_smoke.rs).
In this baseline I found no real multi-process test that configures a
consumer-scoped retry policy and then exercises it across leader transfer; the
policy-specific clustered test uses a persistent single-node Raft engine.
The source stores this policy and each offset's selected snapshot in replicated
group-consumer state, but the general cluster failure tests do not establish
that particular end-to-end combination. The existing backlog criterion under
[make retry policy application-aware](../backlog.md#make-retry-policy-application-aware)
already requires policy state to transfer when ownership moves between nodes;
a real-process leader-transfer test is a suitable way to close this evidence
gap, not a new product-policy decision.

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

The accepted first slice in [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md)
exposes a durable policy for a named consumer: an acknowledgement timeout and
optional positive attempt limit. Unconfigured consumers retain the broker-wide
fallback; unchanged configuration is idempotent, changed values advance the
policy version, and a record keeps the snapshot selected on its first
assignment through retry and terminal movement. This gives members of one
shared consumer the same replicated policy while independent consumers can
differ. The timeout currently supplies both the lease boundary and the time
before a later poll can redeliver; it is not a separate scheduled retry
backoff. Independent backoff, explicit retry/dead-letter dispositions,
provenance, named targets, and redrive remain open choices.

The concrete retry and dead-letter proposal, including backoff, terminal
outcomes, provenance, and redrive tradeoffs, belongs in
[application-aware retry policy](application-aware-retry-policy.md). Its
illustrative state and field names are not requirements beyond the accepted
surface in ADR 0027. Any extension needs evidence that restart and ownership
transfer preserve attempt counts, pinned policy, stale-ack fencing, terminal
movement, and recoverable bounded state.

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

Current protocol admission and storage queues are useful safety mechanisms, but
they are separate ingress and execution bounds rather than one overload
policy. Protocol saturation is rejected before request execution; storage
work may wait within bounded executor/lane capacity and then return
`WouldBlock`. Neither path reserves filesystem capacity, accounts for snapshot
or cleanup workspace, or by itself defines whether a timed-out publish may
have committed. Cluster peer-pool limits similarly protect transport control
traffic but do not establish a cluster-wide retained-storage budget.

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

No additional policy surface or guarantee should be promoted from this note
beyond the consumer-scoped timeout and attempt limit accepted by ADR 0027 until
the following evidence exists for both engines where both engines support the
operation:

1. A contract test states the selected success point and every failure outcome
   that a caller can observe.
2. A process restart test covers a write, acknowledgement, retry, or terminal
   transition at each relevant boundary.
3. Cluster tests cover leader change, follower restart, and response loss when
   the policy is replicated or leader-authorized.
4. Resource and failure tests measure protocol admission (connections, frame
   bytes, in-flight work, timeouts, slow readers/writers), storage executor
   and per-stream queue bounds under stalls, retained bytes, retry state,
   latency, and recovery work under bounded workloads. Peer contention tests
   must preserve the reserved control lane. A benchmark must name the
   durability mode, message shape, topology, resource limits, and failure
   state as required by [the benchmark policy](../benchmarking.md); a
   correctness or reliability change need not claim a performance result.
5. Inspection exposes effective policy selection and version, while metrics
   distinguish relevant bounded outcomes without unbounded labels. The
   accepted consumer inspection returns its configured flag and policy
   version. Current aggregate metrics expose admission limits and rejection or
   timeout counters, storage bytes, in-flight deliveries, redeliveries, dead
   letters, and clustered snapshot activity, but do not expose a consumer
   policy version or stage-aware protocol outcome.
6. An ADR records compatibility, migration, and rollback consequences before
   each new policy choice becomes a supported public guarantee; ADR 0027
   records those consequences for the existing consumer-scoped retry slice.

These gates are deliberately outcome-oriented. They do not require a
particular command, module, file layout, timer implementation, or storage
engine.

## Open questions

- Is durability selected per deployment, stream, operation, or some bounded
  combination, and which combinations are worth the operational cost?
- How should future policy fields, such as backoff or terminal disposition,
  interact with the policy version already pinned to a pending record?
- Beyond the current derived dead-letter action at attempt exhaustion, should
  future policy allow a record to be held or made unavailable, and how should
  those choices interact with retention and be surfaced to an application?
- Which capacity source and reserve are safe across local filesystems,
  snapshots, and clustered replicas without treating a volume claim as free
  broker capacity?
- What operation identity and retention window are sufficient to resolve an
  unknown publish without retaining unbounded producer state?

Until these questions have accepted answers and their evidence gates pass,
Runnel should describe the relevant current behavior as implementation
behavior. The consumer-scoped timeout and attempt limit are already accepted
selectable policy in ADR 0027; unresolved choices should not be presented as
part of that contract.

## Refactor and planning assessment

This review found no code refactor warranted by a documentation-only policy
boundary refresh. The shared `ConsumerPolicy` now owns the accepted timeout,
attempt limit, and version values; local journal persistence and replicated
group-consumer state still have distinct durability lifecycles, so combining
them behind another storage abstraction would add coupling without removing a
useful difference. The existing [TD-005](../tech-debt.md#td-005-durability-and-delivery-policies-are-hard-coded),
[TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse),
and [TD-023](../tech-debt.md#td-023-external-protocol-admission-remains-incomplete)
records remain aligned with the unresolved policy work. The policy-specific
multi-process failover coverage gap above is recorded as a limitation, not a
confirmed runtime shortcut. The existing [retry-policy backlog outcome](../backlog.md#make-retry-policy-application-aware)
already requires policy transfer on ownership change, so no separate backlog
or debt entry is needed; this evidence refresh does not create one.

## References

- [Durability and outcomes for clustered operations](clustered-outcome-contract.md)
- [Application-aware retry policy](application-aware-retry-policy.md)
- [Retention and disk-pressure design](retention-disk-pressure-plan.md)
- [ADR 0004: first distributed engine](../decisions/0004-multi-raft-first-distributed-engine.md)
- [ADR 0014: local retry and dead-letter policy](../decisions/0014-local-retry-and-dead-letter-policy.md)
- [ADR 0016: clustered retry and dead-letter policy](../decisions/0016-clustered-retry-and-dead-letter-policy.md)
- [ADR 0026: semantic engine error classification](../decisions/0026-semantic-engine-error-classification.md)
- [ADR 0027: consumer-scoped retry policy](../decisions/0027-consumer-scoped-retry-policy.md)
