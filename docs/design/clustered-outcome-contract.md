# Clustered durability and outcome contract

- Status: proposed design note; not an accepted wire or compatibility decision
- Date: 2026-09-03
- Scope: clustered writes, leader forwarding, client retry boundaries, and the evidence required to make those behaviors public
- Related work: [clustered durability and outcomes backlog item](../backlog.md#make-clustered-durability-and-outcomes-explicit), [current architecture](../architecture.md), [Multi-Raft implementation plan](multi-raft-implementation-plan.md), and [protocol compatibility design](protocol-compatibility.md)

This note turns the current clustered implementation and the remaining durability backlog into an implementation-ready semantic target. It does not change the runtime, protocol, backlog, or compatibility policy. In particular, the line-delimited JSON protocol remains provisional v1, and no field or error code proposed below is accepted for v1 without a separate compatibility decision.

## Contract boundary

Runnel should expose a confirmed write only when it has crossed a durable replicated boundary that the cluster can explain. A client-visible transport result is not itself evidence that the command was accepted: a connection can fail after the leader has committed and applied a command but before the response reaches the client.

The contract has four attempt outcomes:

| Outcome | Broker-side meaning | Client action |
| --- | --- | --- |
| `confirmed` | The operation definitely reached its advertised success point and its result is known. | Use the result; do not replay it. |
| `rejected` | The operation definitely was not applied, and the request or intent is invalid or conflicts with durable state. | Fix the request or surface the rejection; do not blind-retry. |
| `retryable` | The operation definitely was not applied, but a later attempt with the same intent may succeed. | Retry within a bounded, caller-visible policy. |
| `unknown` | The request may have been proposed, committed, applied, or only partially transmitted; the client cannot establish which point was reached. | Resolve with the same stable operation identity, or make an explicit duplicate-versus-loss decision. |

`unknown` describes the result of one attempt, not a permanent broker state. Reusing the stable operation identity must eventually return the original result, a definitive rejection, or an explicitly documented unresolved result while the identity is retained. A client must not infer “not applied” from a timeout, EOF, or lost response.

## Current behavior and the target durability point

The current layers already provide most of the mechanical boundary needed for this contract:

| Layer | Current behavior | Contract consequence |
| --- | --- | --- |
| `runnel-engine` | Mutations return `Result` and have no public outcome or durability type. | The semantic contract belongs above the current `Result` boundary; adding a type is a compatibility change to review. |
| `runnel-raft` | Stream and consumer mutations use OpenRaft `client_write`; the static cluster has three voters and forwards requests to a group leader. | A successful mutation is intended to mean committed and applied, not merely accepted by a follower. |
| Durable storage | Consensus log entries are persisted with `sync_all`; the state-machine journal is synced before in-memory application; snapshots include broker state and dedup/checkpoint state. | Recovery must test both the consensus record and the materialized broker state. Filesystem and hardware flush semantics remain an explicit assumption. |
| Server/protocol | v1 returns `Published` or an error with `code` and `message`; `not_leader` is mapped to `cluster_error`. | v1 does not expose authoritative outcome classes or a commit/apply stage. Generic `cluster_error` cannot safely drive automatic retry. |
| Client | A successful response is `Confirmed`; local encoding errors are `Rejected`; pre-connect failures are `Retryable`; write/read/EOF/timeout failures after request work begins are `Unknown`. No automatic replay is performed. | This is a useful v1 client safety baseline, but the broker must eventually provide the evidence needed to resolve unknowns. |

The target success point for a mutating operation in one data group is:

1. The current leader accepts a valid command.
2. OpenRaft appends it and establishes quorum commit. In the current three-voter deployment, quorum is two voters.
3. The leader’s state machine durably records and applies the command, including the retained record, consumer checkpoint, deduplication result, or other materialized state.
4. The response is produced from that applied result.

Followers need not have applied the command before the response. The guarantee is that a committed command is replicated to a quorum and can be recovered by the surviving cluster; replicas may apply it asynchronously. A confirmed write in the current static deployment must survive restart or loss of one voter, with the two remaining voters able to recover and serve. Runnel must not promise availability, recovery, or data preservation after loss of two voters, nor permit an unclean single-voter continuation to manufacture confirmation.

The guarantee concerns the retained broker state, not only the Raft log. A record is not confirmed merely because it is in a leader’s memory or because a follower accepted a forwarded frame. Conversely, a committed log entry whose state-machine application failed is not a rejection: it is a recovery/reconciliation condition that must remain visible until the state machine catches up or the group reports an operational fault.

## Identity, forwarding, and deduplication

### Two identities

The future protocol should distinguish two concepts:

- `correlation_id` identifies one wire attempt and response exchange. A new connection, retry, or forwarding hop may use a new correlation ID. It is not a deduplication key.
- A stable `operation_id` identifies one application intent across client retries, leader changes, and internal forwarding. Every hop carries the same operation ID. Its scope must include an authenticated or otherwise collision-resistant producer namespace, operation kind, and target stream/group.

The current optional v1 `request_id` exists only on publish requests, is not echoed, and is currently stored as a raw per-stream key. The current clustered state returns the old offset when the same ID is reused, even if the new key or payload differs; it has no producer namespace or retention policy. That behavior is observed compatibility, not the target contract. The future contract must persist the original request fingerprint and return a definitive `request_id_conflict`-style rejection for a reused identity with different intent. The exact name, scope, retention, and authentication of the identity are unresolved and require the compatibility/ADR process.

The deduplication record must contain enough durable information to reproduce the original result, such as the operation fingerprint, terminal outcome, offset or delivery result, and expiry/retention metadata. It must be included in snapshots and replacement recovery. Once the record expires or is compacted, the safe replay guarantee expires with it and the client must be told how to handle that boundary. Unbounded per-stream identity maps are not an acceptable long-term storage design.

### Leader routing

Any public node may accept a request. It resolves the relevant metadata or data-group leader and forwards internally; topology, node IDs, and storage placement remain outside the public model. The receiving node must not execute a mutation locally after learning that it is not leader. A stale leader hint may cause another bounded routing attempt, but it must never change the operation identity or turn a transport timeout into a safe-to-retry claim.

Each forwarded request should carry:

- the stable operation ID and request fingerprint;
- a per-hop correlation ID;
- a bounded hop count or forwarding origin marker, so forwarding cannot loop;
- an absolute deadline or remaining budget;
- enough group/term context for diagnostics, without exposing topology in the public response.

The current implementation makes up to three forwarding attempts with a two-second per-attempt timeout. That is liveness behavior, not a safety boundary: a timeout can occur after the target leader has committed, and retrying the same mutation is safe only when durable identity makes it idempotent. A future forwarding layer should return the leader’s definitive response when possible, and otherwise preserve `unknown` through the public boundary. `NotLeader` and peer transport errors should not be exposed as a promise that the command was not applied.

## Retry boundaries

The boundary must be based on what the broker can prove, not on which socket exception happened to be observed.

| Point of failure | What can be proven | Target outcome | Safe client behavior |
| --- | --- | --- | --- |
| Local encoding, invalid local options, or malformed batch | No request was sent. | `rejected` | Correct the request. |
| Connect refused/timeout before a request is written | No request reached this broker connection. | `retryable` | Reconnect and retry the same intent if the caller policy allows it. |
| Admission rejects before dispatch, such as a saturated or unavailable group | No command was proposed. | `retryable` when the error says to wait; otherwise `rejected`. | Honor the documented retry class and backoff. |
| Validation or semantic conflict at the broker | The command was not applied. | `rejected` | Do not replay unchanged. |
| The broker proves no leader/quorum existed before proposal | No command was proposed. | `retryable` | Retry after readiness returns, subject to policy. |
| Partial write, write timeout, cancellation after writing starts, response timeout, EOF, or response write failure | The command may have crossed any proposal, commit, apply, or response boundary. | `unknown` | Reuse the same operation ID to resolve; never assume loss. |
| Leader or peer dies after proposal, including a forwarding timeout | The command may be committed on the old or new leader. | `unknown` unless the broker proves non-application. | Retry only with the same operation ID, or surface the ambiguity. |
| Server returns an explicit stage-aware error | The broker supplies authoritative evidence. | The encoded class (`rejected`, `retryable`, or `unknown`) | Follow that class; do not reinterpret a generic transport code. |

The current server uses `request_timeout` for both incomplete frame and engine timeout paths, and maps consensus failures to `cluster_error`. Until v2 can carry an outcome class, the client’s existing classification is the conservative compatibility behavior: only clearly pre-request failures are retryable; server execution and connection failures are unknown. A generic `cluster_error` must not become an automatic retry instruction.

Retries are for the same intent, not merely the same payload. A client should use a new correlation ID for each attempt and reuse the stable operation ID. If no stable identity was supplied for a non-idempotent publish, the client must choose between possible duplication and possible loss; the library must not hide that choice.

## Operation-specific semantics

### Publish

A confirmed publish returns one durable receipt, including its stream and offset. Replaying the same operation ID and identical fingerprint returns that receipt without appending another record. A publish without an operation ID remains at-least-once: a retry after `unknown` can append a second record and consume a new offset. The broker must not claim exactly-once processing from publish deduplication.

The operation ID must be stable across follower forwarding and leader replacement. It must not be regenerated by a broker node. If the operation ID is reused with a different key, payload, or other fingerprinted intent, the result is a definitive conflict rather than the old receipt being silently returned.

### Create and stream activation

Create is semantically idempotent by validated stream name, but the current operation spans metadata and a stream data group. A response must not claim the stream is ready until the advertised activation/reconciliation point is complete. Cross-group work is not a transaction: an unknown create can leave durable metadata that a later retry must reconcile, not duplicate or silently overwrite. The public result must distinguish an existing ready stream from an unresolved group-creation condition without exposing physical placement.

### Poll and acknowledgement

Non-grouped poll is a read of committed records, but current clustered polling is routed through the leader and the group-poll path mutates durable consumer state. Group poll creates ownership, delivery attempts, deadlines, and fenced tokens; a lost response can therefore leave a real in-flight delivery. Retrying a poll with only the member name is not a general resolution protocol. Before automatic retry is permitted, the operation needs a stable poll identity or another documented way to retrieve the same delivery result. The existing behavior of returning the member’s in-flight delivery while its lease is valid is a compatibility aid, not proof that every unknown poll was resolved.

Acknowledgements advance durable consumer state only after the state update succeeds. Repeating the exact acknowledgement tuple (stream, consumer, member, offset, and delivery token where applicable) must be safe and return a terminal acknowledgement result. A stale token must remain an explicit rejection; an old token must not acknowledge a redelivery after a leader change or lease expiry. Confirming an acknowledgement confirms broker progress, not application processing exactly once.

### Batches

The current batch contract is ordered per-record processing with individual outcomes and no implicit atomicity. A transport failure can leave a committed prefix and an unobserved suffix. Each record therefore needs its own stable operation identity if the client is expected to resolve unknown records. A batch ID alone must not imply all-or-nothing behavior. Any future atomic batch or transaction is a separate compatibility and design decision.

## Ordering and durability implications

Within one stream data group, committed Raft command order defines record order and offsets. A deduplicated retry does not consume an ordering slot. A retry without identity can append a duplicate at a later offset, so it can alter per-key ordering and downstream delivery. The contract should state ordering only for committed records in one group; it should not promise a global order across streams or groups.

Consumer delivery remains at least once. Group ownership, attempts, deadlines, tokens, acknowledgements, and dead-letter transitions are replicated state-machine decisions. Leader replacement may redeliver unacknowledged work, while the fence prevents an old token from committing progress. Out-of-order acknowledgements may be accepted according to the current consumer contract, but durable progress must remain monotonic. A confirmed publish says nothing about whether any consumer has received or acknowledged the record.

## Observability

The current server metrics cover request totals, failures, durations, bytes, health, stream operations, and snapshot activity. They do not distinguish quorum commit, state-machine apply, forwarding, deduplication, or client-visible ambiguity. An implementation of this contract should add low-cardinality evidence at both server and client boundaries:

- `operations_total{operation,outcome,reason}` for confirmed, rejected, retryable, and broker-observed unknown results;
- proposal, quorum-commit, state-apply, response-write, and response-loss counters, with histograms for proposal-to-commit, commit-to-apply, and end-to-end confirmed latency;
- forwarding attempts and failures by reason, leader changes, no-quorum rejections, and deduplication hits/conflicts;
- group health/readiness and aggregate committed/applied indexes or replication lag, without putting stream names, request IDs, payload hashes, or delivery tokens in metric labels;
- structured logs or traces carrying correlation ID, a redacted operation hash, group identity, and internal term/index only where access is appropriate.

The server cannot know whether a disconnected client classified an attempt as unknown. It should report `response_lost` or `response_write_timeout`, not pretend to count client outcomes. The client should record unknown attempts, resolution attempts, retry decisions, and whether a deduplication receipt was returned. Readiness should eventually expose whether the groups required for a durable workload have a leader and quorum; metadata readiness alone is insufficient evidence for every data stream.

## Required implementation and test gates

The following gates are required before this becomes a public guarantee. Tests must exercise the real protocol and broker processes where the behavior crosses a network or restart boundary; an in-process mock is not a substitute.

### State-machine and storage gates

- Applying the same operation twice returns the same result and does not advance offsets twice.
- Reusing an identity with a changed fingerprint returns a deterministic conflict; the original receipt remains intact.
- Deduplication, checkpoints, delivery tokens, and terminal outcomes survive journal reopen, snapshot creation/install, and replacement recovery.
- Kill/reopen coverage exists after local log persistence, after quorum commit but before state-machine apply, after state-machine apply but before response, and during snapshot replacement. Committed retained data must recover; uncommitted data must not become visible.
- A state-machine apply failure after consensus commit is surfaced as an operational/recovery condition and cannot be reported as an ordinary rejected request.

### Forwarding and fault-injection gates

- Delay, duplicate, reorder, and drop forwarded frames; prove bounded hops and that a leader change does not create a second publish for one operation ID.
- Distinguish no-quorum-before-proposal (`retryable`) from a timeout after proposal (`unknown`).
- Exercise stale leader hints, leader failure, follower failure, partition, reconnect, and deadline exhaustion.
- Verify that a follower never locally applies a mutation after forwarding it, and that internal node identity does not leak through the public outcome.

### Required real-process public tests

Extend the existing three-process cluster coverage to include:

1. Publish through a follower, stop one node, restart it, and read the confirmed record through the new leader.
2. Lose quorum before proposal and assert `retryable` with no record; restore quorum and retry.
3. Drop the response after quorum commit/state-machine apply. The client must see `unknown`; retrying the same operation ID through another node must return one original receipt and exactly one record.
4. Kill the leader between accepted write and response, then resolve through the replacement leader with no duplicate.
5. Drop acknowledgement and poll responses, retry with their stable identities, and verify durable progress, delivery-token fencing, and the documented redelivery behavior.
6. Verify request-ID conflict, per-record batch ambiguity, restart recovery, and absence of topology/storage paths from public responses.
7. Assert outcome, response-loss, forwarding, commit/apply, dedup, and quorum-health metrics for the corresponding scenarios.

`just cluster-test` already covers follower forwarding, quorum replication, leader failure, restart, grouped delivery, stale tokens, and dead-letter behavior. It does not yet establish the public four-outcome contract, post-commit response loss, pre-proposal no-quorum classification, generic operation identity, or outcome metrics. `just verify` owns the real-process cluster smoke test in the normal verification path; `just integration` covers the separate process/container integration sequence. These commands should remain the canonical gates as the tests are added.

## Compatibility and rollout boundary

The current v1 line protocol remains unchanged by this design. In v1:

- `request_id` is an optional publish field, not a generic operation identity;
- responses have no correlation ID or outcome class;
- the client does not automatically replay requests;
- `cluster_error`, timeout, EOF, and response loss must be treated conservatively as ambiguous once request work may have begun.

A future negotiated protocol version may add a response outcome class, per-attempt correlation ID, stable operation identity, fingerprint conflict, and explicit retry/resolution metadata. The wire names, identity scope, retention behavior, batch semantics, and error-code vocabulary require a compatibility decision and interoperability fixtures. Adding fields that old v1 clients ignore is not sufficient if the meaning of an existing response changes. No storage-path, offset-layout, Raft term, or node-placement concept should become public as part of this work.

## Alternatives and reference comparison

The design follows the leader-and-quorum shape already selected for the clustered vertical slice, while narrowing what a client may infer from a response.

| Reference or alternative | Relevant behavior | Difference that matters for Runnel |
| --- | --- | --- |
| [Raft paper, client interaction and commitment](https://raft.github.io/raft.pdf) | A leader replicates a command, commits it after a majority, then applies it and returns the result. A response lost after commit can cause duplicate execution unless clients use unique serials and the state machine stores the latest result. | This directly supports quorum confirmation plus durable operation-result deduplication. Runnel must implement the serial/result part rather than treating retry as a transport concern. |
| [OpenRaft `client_write`](https://docs.rs/openraft/0.9.25/openraft/raft/struct.Raft.html#method.client_write) | The mutating client call is documented as append, commit, apply, and return; its client guidance also calls out duplicate execution after a lost response and serial-number deduplication. | Runnel already uses this path but does not expose the application stage or generic identity in its public engine/protocol. |
| [Kafka design](https://kafka.apache.org/41/design/design/) and [producer protocol](https://kafka.apache.org/41/design/protocol/) | Producer acknowledgements vary by `acks` and in-sync replicas; idempotent producers use producer identity and sequence numbers; a network error after publish is unknown. | Runnel should begin with one explicit static-cluster safety point rather than expose `acks` choices, unclean leader behavior, transactions, or Kafka producer sessions. Its application-supplied identity is smaller and must document scope and retention. |
| [RabbitMQ publisher confirms](https://www.rabbitmq.com/docs/confirms) and [quorum queues](https://www.rabbitmq.com/docs/quorum-queues) | Confirm/nack is an explicit publisher contract; quorum queues confirm after quorum replication. Confirms are asynchronous and may arrive out of order. | Runnel’s current request/response path is synchronous and serial per connection. It should add correlation before considering asynchronous confirms and must not assume response order beyond the current protocol behavior. Consumer acknowledgements remain distinct from publisher confirmation. |
| [NATS JetStream stream configuration and deduplication](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md) | Streams can be replicated and use a client message ID for duplicate suppression within a configurable duplicate window. | Runnel should make the identity retention window and post-expiry behavior explicit. Its current persisted per-stream map is stronger in duration but unbounded; that is a storage risk, not a finished contract. |

Alternatives considered:

- Always replaying a publish is simple but converts unknown outcomes into possible duplicate records and changed ordering. It is rejected as a library default.
- Broker-generated IDs do not help when the response carrying the ID is the part that is lost. A caller-supplied stable identity is required for resolution.
- A two-phase transaction across metadata and stream groups would complicate the current static cluster without being required for one-stream publish durability. Cross-group atomicity is deferred.
- Follower reads or clock-based leader leases could reduce forwarding, but committed leader reads are easier to reason about while the failure contract is being established. Any linearizable-read optimization needs its own timing and partition evidence.
- Reusing the current member/in-flight behavior to resolve every unknown group poll works only while the lease and member state remain unchanged. A stable poll identity or explicit resolution is required for a general guarantee.

## Hypotheses and unresolved risks

The implementation should validate these hypotheses rather than silently convert them into promises:

- A stable, caller-supplied operation identity plus a durable original result is the smallest extension that resolves committed-but-unacknowledged publish attempts without exposing topology.
- The current `sync_data`/`sync_all` ordering is sufficient for the intended process-crash tests, but the guarantee depends on the filesystem and storage device honoring those operations; crash-injection evidence must define the supported failure model.
- The distinction between quorum commit and state-machine apply is operationally important. Recovery must handle committed log entries whose materialization was interrupted, including after a leader change.
- Identity storage, conflict fingerprints, expiry, snapshot compaction, and migration need bounded resource rules. After expiry, a replay may no longer be safe, and the client-facing behavior must be explicit.
- Group-poll result retention, lease expiry, and response loss can conflict. The contract must say whether a retry returns the original token, a terminal result, or a new redelivery after the resolution window.
- Cancellation of a client future and cancellation of a server-side `client_write` are not the same event. A cancelled request may still commit; tests must cover this boundary.
- There is currently no authenticated producer namespace or TLS-level identity contract. Collision resistance and malicious reuse of operation IDs remain unresolved until authentication is designed.

This note intentionally does not update the backlog: the outcome remains unimplemented, so the backlog item is neither retired nor materially changed.

## Evidence and recommendation

Primary evidence class: Design/research. Secondary tags: public-contract, storage/recovery, compatibility.

This is a design-only change. It has no runtime, wire, storage, or performance effect, and no benchmark is applicable. The required implementation gates above remain coverage gaps; existing `just cluster-test` and `just verify` coverage is useful but insufficient for the new public outcome claims. Recommendation: merge the design note as an implementation baseline, then implement the contract behind an explicit compatibility/ADR decision and rerun the real-process/restart gates before making any guarantee public.
