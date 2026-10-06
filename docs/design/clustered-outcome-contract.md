# Clustered durability and outcome contract

- Status: partially accepted; ADR 0031 accepts the v2 public outcome/stage
  vocabulary and protocol boundary below. Clustered identity, forwarding, and
  durability implementation details remain target design. ADR 0027 separately
  accepts consumer retry policy.
- Date: 2026-09-03
- Last reviewed: 2026-10-06
- Baseline reviewed: `c3a894b6d88a40245c1116e2c5006b94f5573aee`
- Scope: clustered writes, leader forwarding, client retry boundaries, and the evidence required to make those behaviors public
- Related work: [clustered durability and outcomes backlog item](../backlog.md#make-clustered-durability-and-outcomes-explicit), [current architecture](../architecture.md), [distributed architecture research](../research/distributed-architecture-options.md), [Multi-Raft implementation plan](multi-raft-implementation-plan.md), [durability and delivery policy](durability-delivery-policy.md), [application-aware retry policy](application-aware-retry-policy.md), [Raft recovery research](../research/raft-recovery-and-replacement.md), [protocol compatibility design](protocol-compatibility.md), [ADR 0026](../decisions/0026-semantic-engine-error-classification.md), [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md), and [TD-025](../tech-debt.md#td-025-shared-engine-errors-expose-implementation-specific-failure-details)

This note records observed clustered behavior and a target for remaining
durability work. It does not change runtime behavior. The public v2
outcome/stage names and compatibility boundary are accepted by [ADR 0031](../decisions/0031-protocol-v2-contract.md); no such fields exist on provisional
v1 today. The remaining forwarding, identity-storage, recovery, and
observability requirements below are proposed implementation targets, not an
accepted storage format or implementation sequence.

## Contract boundary

Runnel should expose a confirmed write only when it has crossed a durable replicated boundary that the cluster can explain. A client-visible transport result is not itself evidence that the command was accepted: a connection can fail after the leader has committed and applied a command but before the response reaches the client.

The contract has four attempt outcomes:

| Outcome | Broker-side meaning | Client action |
| --- | --- | --- |
| `confirmed` | The operation definitely reached its advertised success point and its result is known. | Use the result; do not replay it. |
| `rejected` | The operation definitely was not applied, and the request or intent is invalid or conflicts with durable state. | Fix the request or surface the rejection; do not blind-retry. |
| `retryable` | The operation definitely was not applied, but a later attempt with the same intent may succeed. | Retry within a bounded, caller-visible policy. |
| `unknown` | The request may have crossed an effect boundary or the client cannot establish which result was produced. | Resolve with a supported stable publish identity when present; otherwise inspect state or make an explicit duplicate-versus-loss decision. |

`unknown` describes the result of one attempt, not a permanent broker state. Reusing a supported stable publish identity must return the original result or a definitive mismatch rejection while the identified record is retained. A client must not infer “not applied” from a timeout, EOF, or lost response.

## Outcome and stage are separate dimensions

An outcome answers what the caller may safely do next. A stage answers the
furthest broker-side progress point that the broker can establish for that
attempt. They must not be collapsed into one enum or inferred from a transport
error:

| Dimension | Meaning | Topology-neutral vocabulary | What it must not imply |
| --- | --- | --- | --- |
| Outcome | Whether the intent is confirmed, definitely not applied, safe to retry, or unresolved. | `confirmed`, `rejected`, `retryable`, `unknown` | A particular storage primitive, Raft term, node, or response path. |
| Stage | The furthest authoritative processing point known to have been reached. | `received`, `validated`, `execution_started`, `durable`, `completed`, or `unknown` | That later stages were not reached, especially after a disconnect or cancellation. |

The public v2 stages are fixed by ADR 0031 as `received`, `validated`,
`execution_started`, `durable`, `completed`, and `unknown`; they deliberately
do not expose proposed/committed/applied Raft sub-stages. A client-observed
successful reply is confirmed. A durable effect with a lost reply remains
unknown unless the supported publish request ID resolves it. A retryable result
requires authoritative proof of no effect; if engine execution began, the stage
must report it and the engine must prove no proposal or effect occurred. Stage
alone never establishes non-application.

The engine classification intentionally exposes only the outcome boundary:
`BrokerError::kind()` is a semantic reason and `BrokerError::outcome()` is a
safe retry classification. It does not claim to know a stage for every
failure, and it must not be extended with backend stages merely to make a
future wire response convenient. Stage evidence belongs at the layer that can
observe it, while the public contract should describe the evidence without
leaking topology or persistence layout.

## Current behavior and the target durability point

The following is the observed baseline at the revision recorded above. The
current layers provide useful mechanics for this contract, but they do not by
themselves establish the proposed public guarantee:

| Layer | Current behavior | Contract consequence |
| --- | --- | --- |
| `runnel-engine` | `BrokerError::kind()` and `BrokerError::outcome()` provide a backend-independent reason and `Rejected`/`Retryable`/`Unknown` classification; a successful `Result` is the confirmed engine result. `NotLeader` and `StreamNotReady` classify as retryable, while generic cluster failures remain unknown. | This boundary does not report proposal, commit, apply, or response stage. The classification is not serialized on the wire; variants and error text are not stage evidence. |
| `runnel-raft` | `PersistentEngine` routes operations to each metadata or stream data-group leader. Mutations call OpenRaft `client_write`; client forwarding preserves the operation fields, including publish `request_id`. Membership is initialized from the configured peer map; there is no dynamic membership lifecycle. | A successful `client_write` result includes the state-machine response, but the engine does not return a stage record. Three-process tests exercise three configured voters; that profile does not establish a universal membership or availability guarantee. |
| Durable storage | The Raft log is atomically replaced with file and parent-directory syncs. State-machine apply appends and `sync_data`s the journal entry before applying it in memory. Checkpoints and snapshots are atomically replaced; snapshots contain retained messages, consumer state, and publish deduplication state. Snapshot install/reopen and process-restart paths have coverage, but there is no crash injection at each commit/apply/response boundary. | Recovery must cover the consensus record and materialized state. The flush guarantee still depends on the filesystem and device honoring sync operations. |
| Server/protocol | The provisional JSON-lines protocol returns successful operation responses or `{code, message}` errors. `NotLeader` and generic cluster errors map to `cluster_error`. Consumer-policy operations are current v1 operations under ADR 0027; the canonical Rust request fixtures pin their JSON fields and optional-field behavior. | The wire carries no authoritative outcome class or commit/apply stage. The client treats `cluster_error` as unknown, even when the underlying engine error is `NotLeader`. The fixtures do not establish cross-language compatibility or clustered outcome behavior. |
| Client | `AttemptOutcome` classifies non-error responses as confirmed and local encoding errors as rejected. Pre-request connection failures are retryable. The static server-code map also treats `connection_limit`, `request_saturated`, and `stream_not_ready` as retryable; `cluster_error`, `request_timeout`, and connection failures after request work may have started are unknown. The client does not automatically replay requests. | v1 exposes a conservative attempt classification to the caller, not a negotiated broker outcome. Retryability for those named codes is a client mapping, not proof that all cluster failures are safe to retry. |

The accepted v2 public `durable` stage for a mutating operation in one data group corresponds to this target success point:

1. The current leader accepts a valid command.
2. The selected replication engine appends it and establishes quorum commit for the configured membership. In the current three-process evidence profile, a three-voter group has a two-voter majority; this is not a universal setting or guarantee.
3. The leader’s state machine durably records and applies the command, including the retained record, consumer checkpoint, deduplication result, or other materialized state.
4. The response is produced from that applied result.

Followers need not have applied the command before the response. The guarantee is that a committed command is replicated to the required quorum and can be recovered by the surviving cluster; replicas may apply it asynchronously. For a documented three-voter durability profile, a confirmed write should survive restart or loss of one voter, with the two remaining voters able to recover and serve. The current configuration does not expose selectable durability modes or a general failure-tolerance declaration, so this remains a target contract rather than a promise for arbitrary peer sets. Runnel must not promise availability, recovery, or data preservation after loss of the quorum required by the selected profile, nor permit an unclean continuation to manufacture confirmation.

The guarantee concerns the retained broker state, not only the Raft log. A record is not confirmed merely because it is in a leader’s memory or because a follower accepted a forwarded frame. Conversely, a committed log entry whose state-machine application failed is not a rejection: it is a recovery/reconciliation condition that must remain visible until the state machine catches up or the group reports an operational fault.

## Identity, forwarding, and deduplication

### Identity boundary

The v2 protocol uses response order on its sequential connection and does not
add a per-attempt correlation ID. It retains the stable publish request ID
across client retries, leader changes, and internal forwarding. This ID is
per-stream and publish-only; it is not a generic operation ID. Any future
identity for poll, acknowledgement, or another operation requires a separate
semantic and lifecycle decision before it can resolve unknown outcomes.

The current optional v1 request ID is stored as a raw per-stream key and is
not echoed. Reusing an ID today returns the prior offset without comparing the
new key or payload; the ID has no producer namespace or retention policy.
`three_process_cluster_replicates_and_recovers_after_failures` sends the same
ID, key, and payload through different nodes and observes offset zero, but does
not test conflicting reuse. This is observed v1 behavior, not the v2 target.
For negotiated v2, ADR 0031 defines the fingerprint as stream, key presence and
exact key bytes, and exact logical payload bytes; server-assigned publish time
and the ID itself are excluded. An identical retry returns the original
receipt. Reuse with changed intent returns `request_id_conflict`, rejected with
definitive no-effect evidence, preserving the original record. Local and
clustered runtime paths must prove this before v2 is supported.

The v2 ID remains valid while the original record is retained. There is no
current message-retention policy or independent ID expiry window. Any future
retention policy must not expire the ID while retaining its record. This does
not promise bounded identity-index memory; that remains a storage concern.

### Leader routing

Any public node may accept a request. It resolves the relevant metadata or data-group leader and forwards internally; topology, node IDs, and storage placement remain outside the public model. The receiving node must not execute a mutation locally after learning that it is not leader. A stale leader hint may cause another bounded routing attempt, but it must never change a publish request ID or turn a transport timeout into a safe-to-retry claim.

A forwarded publish preserves its stable request ID and the exact logical
intent. Forwarding may also carry an internal per-hop correlation value,
bounded hop/origin metadata, a deadline or remaining budget, and internal
group context for diagnostics. Those routing details are not public protocol
fields and must not turn a transport timeout into a safe-to-retry result.

The current `ClientForwarder` makes up to three rounds. In each round it tries
the known leader first, then every other configured peer except the receiving
node; each peer call has a two-second timeout. The total calls can therefore
exceed three, and there is no single end-to-end forwarding deadline. A peer's
`NotLeader` response refreshes the preferred leader for the next attempt. This
is liveness behavior, not a safety boundary: a timeout or exhausted forwarding
attempts can occur after the target leader has committed, and the client sees
a generic `cluster_error`. Retrying the same mutation is safe only when durable
identity makes it idempotent. A future forwarding layer should return the
leader’s definitive response when possible and otherwise preserve `unknown`
through the public boundary.

## Retry boundaries

The boundary must be based on what the broker can prove, not on which socket exception happened to be observed.

| Point of failure | What can be proven | Target outcome | Safe client behavior |
| --- | --- | --- | --- |
| Local encoding, invalid local options, or malformed batch | No request was sent. | `rejected` | Correct the request. |
| Connect refused/timeout before a request is written | No request reached this broker connection. | `retryable` | Reconnect and retry the same intent if the caller policy allows it. |
| Admission rejects before dispatch, such as a saturated or unavailable group | No command was proposed. | `retryable` when the error says to wait; otherwise `rejected`. | Honor the documented retry class and backoff. |
| Validation or semantic conflict at the broker | The command was not applied. | `rejected` | Do not replay unchanged. |
| The broker proves no leader/quorum existed before proposal | No command was proposed. | `retryable` | Retry after readiness returns, subject to policy. |
| Partial write, write timeout, cancellation after writing starts, response timeout, EOF, or response write failure | The command may have crossed a proposal, durability, application, or response boundary. | `unknown` | Reuse a supported stable publish identity when present; otherwise inspect state or make an explicit duplicate-versus-loss decision. |
| Leader or peer dies after proposal, including a forwarding timeout | The command may be committed on the old or new leader. | `unknown` unless the broker proves non-application. | Resolve with a supported stable identity or surface the ambiguity; do not infer that every operation has an operation ID. |
| Server returns an explicit stage-aware error | The broker supplies authoritative evidence. | The encoded class (`rejected`, `retryable`, or `unknown`) | Follow that class; do not reinterpret a generic transport code. |

The current server uses `request_timeout` for incomplete frames and timed-out engine work, and maps consensus failures to `cluster_error`. Its client maps `connection_limit`, `request_saturated`, and `stream_not_ready` to retryable, maps `cluster_error` and `request_timeout` to unknown, and does not automatically replay. Until a versioned protocol carries authoritative outcomes, a generic `cluster_error` must not become an automatic retry instruction.

Retries are for the same intent, not merely the same payload. V2 has no public correlation ID while request/response stays sequential. Reuse the same publish request ID for an identical retry. If no ID was supplied for a publish, the client must choose between possible duplication and possible loss after an unknown result; the library must not hide that choice.

## Operation-specific semantics

### Publish

A confirmed publish returns one durable receipt, including its stream and offset. Replaying the same request ID and identical fingerprint returns that receipt without appending another record. A publish without an ID remains at-least-once: a retry after `unknown` can append a second record and consume a new offset. The broker must not claim exactly-once processing from publish deduplication.

The request ID must remain stable across follower forwarding and leader replacement; a broker must not regenerate it. V2 rejects the same ID with a different key or payload as a definitive conflict, rather than returning the old receipt silently.

### Create and stream activation

Create is semantically idempotent by validated stream name, but the current operation spans metadata and a stream data group. A response must not claim the stream is ready until the advertised activation/reconciliation point is complete. Cross-group work is not a transaction: an unknown create can leave durable metadata that a later retry must reconcile, not duplicate or silently overwrite. The public result must distinguish an existing ready stream from an unresolved group-creation condition without exposing physical placement.

### Poll and acknowledgement

Non-grouped poll is a read of committed records, but current clustered polling is routed through the leader and the group-poll path mutates durable consumer state. Group poll creates ownership, delivery attempts, deadlines, and fenced tokens; a lost response can therefore leave a real in-flight delivery. Retrying a poll with only the member name is not a general resolution protocol. Before automatic retry is permitted, the operation needs a stable poll identity or another documented way to retrieve the same delivery result. The existing behavior of returning the member’s in-flight delivery while its lease is valid is a compatibility aid, not proof that every unknown poll was resolved.

Acknowledgements advance durable consumer state only after the state update succeeds. Repeating the exact acknowledgement tuple (stream, consumer, member, offset, and delivery token where applicable) must be safe and return a terminal acknowledgement result. A stale token must remain an explicit rejection; an old token must not acknowledge a redelivery after a leader change or lease expiry. Confirming an acknowledgement confirms broker progress, not application processing exactly once.

### Batches

The current batch contract is ordered per-record processing with individual outcomes and no implicit atomicity. A transport failure can leave a committed prefix and an unobserved suffix. Each publish-batch record therefore needs its own stable request ID if the client is expected to resolve unknown publish outcomes. A batch ID alone must not imply all-or-nothing behavior. Any future atomic batch or transaction is a separate compatibility and design decision.

## Ordering and durability implications

Within one stream data group, committed Raft command order defines record order and offsets. A deduplicated retry does not consume an ordering slot. A retry without identity can append a duplicate at a later offset, so it can alter per-key ordering and downstream delivery. The contract should state ordering only for committed records in one group; it should not promise a global order across streams or groups.

Consumer delivery remains at least once. Group ownership, attempts, deadlines, tokens, acknowledgements, and dead-letter transitions are replicated state-machine decisions. Leader replacement may redeliver unacknowledged work, while the fence prevents an old token from committing progress. Out-of-order acknowledgements may be accepted according to the current consumer contract, but durable progress must remain monotonic. A confirmed publish says nothing about whether any consumer has received or acknowledged the record.

## Observability

The current server metrics cover request totals, failures, durations, bytes, health, stream operations, snapshot activity, request timeouts, and response-write timeouts. They do not distinguish quorum commit, state-machine apply, forwarding, deduplication, or client-visible ambiguity. The client exposes `AttemptOutcome` to callers but does not publish outcome metrics. An implementation of this contract should add low-cardinality evidence at both server and client boundaries:

- `operations_total{operation,outcome,reason}` for confirmed, rejected, retryable, and broker-observed unknown results;
- proposal, quorum-commit, state-apply, response-write, and response-loss counters, with histograms for proposal-to-commit, commit-to-apply, and end-to-end confirmed latency;
- forwarding attempts and failures by reason, leader changes, no-quorum rejections, and deduplication hits/conflicts;
- group health/readiness and aggregate committed/applied indexes or replication lag, without putting stream names, request IDs, payload hashes, or delivery tokens in metric labels;
- structured logs or traces carrying correlation ID, a redacted operation hash, group identity, and internal term/index only where access is appropriate.

The server cannot know whether a disconnected client classified an attempt as unknown. It currently counts response-write timeouts, but does not report a general response-loss outcome. It should report observable response-write failures without pretending to count client outcomes. The client should record unknown attempts, resolution attempts, retry decisions, and whether a deduplication receipt was returned. Readiness should eventually expose whether the groups required for a durable workload have a leader and quorum; metadata readiness alone is insufficient evidence for every data stream.

## Required implementation and test gates

The following gates are required before this becomes a public guarantee. Tests must exercise the real protocol and broker processes where the behavior crosses a network or restart boundary; an in-process mock is not a substitute.

### State-machine and storage gates

- Applying the same operation twice returns the same result and does not advance offsets twice.
- Reusing an identity with a changed fingerprint returns a deterministic conflict; the original receipt remains intact.
- Deduplication, checkpoints, delivery tokens, and terminal outcomes survive journal reopen, snapshot creation/install, and replacement recovery.
- Kill/reopen coverage exists after local log persistence, after quorum commit but before state-machine apply, after state-machine apply but before response, and during snapshot replacement. Committed retained data must recover; uncommitted data must not become visible.
- A state-machine apply failure after consensus commit is surfaced as an operational/recovery condition and cannot be reported as an ordinary rejected request.

### Forwarding and fault-injection gates

- Delay, duplicate, reorder, and drop forwarded frames; prove bounded hops and that a leader change does not create a second publish for one publish request ID.
- Distinguish no-quorum-before-proposal (`retryable`) from a timeout after proposal (`unknown`).
- Exercise stale leader hints, leader failure, follower failure, partition, reconnect, and deadline exhaustion.
- Verify that a follower never locally applies a mutation after forwarding it, and that internal node identity does not leak through the public outcome.

### Required real-process public tests

The baseline already covers parts of the first and fifth scenarios; those
tests do not establish the unknown-outcome resolution guarantees in the other
scenarios. Extend the real-process coverage to include:

1. Publish through a follower, stop one node, restart it, and read the confirmed record through the new leader.
2. Lose quorum before proposal and assert `retryable` with no record; restore quorum and retry.
3. Drop the response after quorum commit/state-machine apply. The client must see `unknown`; retrying the same publish request ID through another node must return the original receipt and leave exactly one record.
4. Kill the leader between accepted write and response, then resolve through the replacement leader with no duplicate.
5. Drop acknowledgement and poll responses; verify durable progress, delivery-token fencing, and documented redelivery/resolution behavior without assuming those operations have a stable public identity.
6. Verify request-ID conflict, per-record batch ambiguity, restart recovery, and absence of topology/storage paths from public responses.
7. Assert outcome, response-loss, forwarding, commit/apply, dedup, and quorum-health metrics for the corresponding scenarios.

`three_process_cluster_replicates_and_recovers_after_failures` covers public
follower forwarding, a follower restart, leader failure, and replicated
post-failure publishing in the three-process setup. The
`three_process_cluster_preserves_group_delivery_through_replica_restart` and
`three_process_cluster_reassigns_group_delivery_after_node_failure` tests
cover grouped delivery state, fencing, and recovery. The
`three_process_cluster_transfers_consumer_policy_and_delivery_snapshot_after_leader_failure`
test verifies consumer-policy inspection and a pinned delivery policy after
leader replacement. Its helper identifies the data-group leader with a
read-only `InspectConsumer` probe over the peer listener because the public
request path forwards to the leader; the test assertions use public requests.
None intentionally drops a clustered write, poll, or acknowledgement
response after commit and resolves it through another node.

The real-server [request-ID response-loss test](../../crates/runnel-server/tests/client_retry.rs)
demonstrates conservative `unknown` classification and deduplication only for
a single-node publish. The cluster tests do not yet establish post-commit
response-loss resolution, no-quorum-before-proposal classification, or conflicting
request-ID rejection. A generic operation identity is not part of the accepted v2 contract. Existing server metrics
cover request behavior and response-write timeouts, but outcome, forwarding,
deduplication, commit/apply, and quorum-health metrics remain unestablished.
`just verify` owns the real-process cluster smoke test in the normal
verification path; `just integration` covers the separate process/container
integration sequence. These remain the canonical gates as coverage is added.

The protocol [wire tests](../../crates/runnel-protocol/tests/wire.rs) now
include `consumer_policy_request_fixtures_pin_serialization_and_deserialization`,
which pins the exact `configure_consumer` and `inspect_consumer` request JSON
and the current `max_delivery_attempts` omission-to-`null` Serde behavior.
This is evidence for the Rust wire shape only; it does not establish
cross-language interoperability, a stage-aware outcome, or clustered
response-loss resolution.

## Compatibility and rollout boundary

The current v1 line protocol remains unchanged by this design. In v1:

- `request_id` is an optional publish field, not a generic operation identity;
- responses have no correlation ID or outcome class;
- the client does not automatically replay requests;
- `cluster_error`, timeout, EOF, and response loss must be treated conservatively as ambiguous once request work may have begun.

The accepted engine classification in [ADR 0026](../decisions/0026-semantic-engine-error-classification.md)
does not change these wire rules. In particular, a `NotLeader` engine error is
retryable to an engine caller, while v1 still maps it to `cluster_error` and
the client treats that response as unknown. The client's retryable mappings
for `connection_limit`, `request_saturated`, and `stream_not_ready` are also
static response-code rules rather than a general wire outcome field. Only a negotiated v2 response can carry the accepted authoritative outcome
class and processing stage; runtime support remains unimplemented.

ADR 0031 accepts v2 outcome and stage fields, the publish-only request-ID fingerprint/mismatch rule, and the connection-scoped compatibility boundary. It does not add a correlation ID while requests remain sequential or a generic identity for other operations. The accepted names and rules apply only to negotiated v2; they do not silently change current v1. No storage path, offset layout, Raft term, or node placement becomes public.

## Alternatives and reference comparison

The design follows the leader-and-quorum shape already selected for the clustered vertical slice, while narrowing what a client may infer from a response.

| Reference or alternative | Relevant behavior | Difference that matters for Runnel |
| --- | --- | --- |
| [Raft paper, client interaction and commitment](https://raft.github.io/raft.pdf) | A leader replicates a command, commits it after a majority, then applies it and returns the result. A response lost after commit can cause duplicate execution unless clients use unique serials and the state machine stores the latest result. | This supports quorum confirmation plus durable result deduplication. Runnel applies that pattern only to its accepted publish request ID; it does not infer a generic operation identity. |
| [OpenRaft `client_write`](https://docs.rs/openraft/0.9.25/openraft/raft/struct.Raft.html#method.client_write) | The mutating client call is documented as append, commit, apply, and return; its client guidance also calls out duplicate execution after a lost response and serial-number deduplication. | Runnel already uses this path; the engine has no stage result and provisional v1 has no authoritative outcome/stage fields. |
| [Kafka design](https://kafka.apache.org/42/design/design/) and [producer protocol](https://kafka.apache.org/42/design/protocol/) | Producer acknowledgements vary by `acks` and in-sync replicas; idempotent producers use producer identity and sequence numbers; a network error after publish is unknown. | Runnel should begin with one explicit configured-membership safety point rather than expose `acks` choices, unclean leader behavior, transactions, or Kafka producer sessions. Its publish-only application-supplied request ID has the narrower scope and retention rule accepted in ADR 0031. |
| [RabbitMQ publisher confirms](https://www.rabbitmq.com/docs/confirms) and [quorum queues](https://www.rabbitmq.com/docs/quorum-queues) | Confirm/nack is an explicit publisher contract; quorum queues confirm after quorum replication. Confirms are asynchronous and may arrive out of order. | Runnel’s current request/response path is synchronous and serial per connection. It should add correlation before considering asynchronous confirms and must not assume response order beyond the current protocol behavior. Consumer acknowledgements remain distinct from publisher confirmation. |
| [NATS JetStream stream configuration and deduplication](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md) | Streams can be replicated and use a client message ID for duplicate suppression within a configurable duplicate window. | Runnel should make the identity retention window and post-expiry behavior explicit. Its current persisted per-stream map is stronger in duration but unbounded; that is a storage risk, not a finished contract. |

Alternatives considered:

- Always replaying a publish is simple but converts unknown outcomes into possible duplicate records and changed ordering. It is rejected as a library default.
- Broker-generated IDs do not help when the response carrying the ID is the part that is lost. A caller-supplied stable identity is required for resolution.
- A two-phase transaction across metadata and stream groups would complicate the current static cluster without being required for one-stream publish durability. Cross-group atomicity is deferred.
- Follower reads or clock-based leader leases could reduce forwarding, but committed leader reads are easier to reason about while the failure contract is being established. Any linearizable-read optimization needs its own timing and partition evidence.
- Reusing the current member/in-flight behavior to resolve every unknown group poll works only while the lease and member state remain unchanged. A stable poll identity or explicit resolution is not accepted for v2 and requires a separate decision before it can offer a general guarantee.

## Hypotheses and unresolved risks

The implementation should validate these hypotheses rather than silently convert them into promises:

- A stable, caller-supplied publish request ID plus its durable original result is sufficient to resolve an ambiguous publish without exposing topology; other operations need separate identity decisions.
- The current `sync_data`/`sync_all` ordering is sufficient for the intended process-crash tests, but the guarantee depends on the filesystem and storage device honoring those operations; crash-injection evidence must define the supported failure model.
- The distinction between quorum commit and state-machine apply is operationally important. Recovery must handle committed log entries whose materialization was interrupted, including after a leader change.
- Identity storage, conflict fingerprints, expiry, snapshot compaction, and migration need bounded resource rules. After expiry, a replay may no longer be safe, and the client-facing behavior must be explicit.
- Group-poll result retention, lease expiry, and response loss can conflict. The contract must say whether a retry returns the original token, a terminal result, or a new redelivery after the resolution window.
- Cancellation of a client future and cancellation of a server-side `client_write` are not the same event. A cancelled request may still commit; tests must cover this boundary.
- There is currently no authenticated producer namespace or TLS-level identity contract. Collision resistance and malicious reuse of public publish request IDs remain unresolved until authentication is designed.

ADR 0031 accepts the public v2 outcome/stage protocol, while its runtime,
clustered identity comparison, and real-process ambiguity gates remain
unimplemented. The clustered-outcomes backlog item and TD-025 continue to track
that work. ADR 0026 still defines engine classification and ADR 0027 consumer
policy; neither provides the v2 wire implementation.

## Evidence and recommendation

Primary evidence class: Design/research. Secondary tag: clustered outcomes.

Source evidence at the baseline is in [`BrokerError::outcome`](../../crates/runnel-engine/src/lib.rs),
the [`ClientForwarder`](../../crates/runnel-raft/src/forwarding.rs) and Raft
write paths ([`engine.rs`](../../crates/runnel-raft/src/engine.rs)), and the
state-machine [journal](../../crates/runnel-raft/src/state_machine_journal.rs)
and [snapshot store](../../crates/runnel-raft/src/state_machine_store.rs).
Server evidence is in [error mapping](../../crates/runnel-server/src/dispatch.rs)
and [metrics](../../crates/runnel-server/src/observability.rs); client
classification is in the [`AttemptOutcome` mapping](../../crates/runnel-client/src/lib.rs).

Unit coverage classifies engine errors; the real-process tests named above
cover follower forwarding, replica/leader failures, grouped delivery, and
policy transfer. The local real-server retry test covers a dropped publish
response and request-ID resolution only in a single-node engine. Clustered
ambiguity resolution, no-quorum classification, identity conflicts, and
stage/outcome metrics remain gaps. Recommendation: treat the v2 outcome and
stage vocabulary as accepted by ADR 0031, and retain the remaining clustered
recovery, forwarding, and observability gates as proposed implementation work.
No runtime refactor is warranted by this documentation update. The clustered
outcomes backlog item and TD-025 should remain open until real-process evidence
covers those boundaries.
