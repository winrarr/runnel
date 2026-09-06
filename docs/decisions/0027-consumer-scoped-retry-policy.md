# ADR 0027: Consumer-scoped retry policy

- Status: accepted
- Date: 2026-09-06

## Decision

Add two additive operations to the provisional JSON-lines protocol:

- `configure_consumer(stream, consumer, ack_timeout_ms, max_delivery_attempts)`
- `inspect_consumer(stream, consumer)`

The operation returns the durable policy version, whether it is explicitly
configured, the bounded acknowledgement timeout, and the optional positive
attempt limit. Configuration applies to an existing stream and validates names
and values before changing state. Repeating the same values is idempotent;
changing values advances a monotonic version.

The policy is stored with durable consumer state in both engines. Broker-wide
acknowledgement timeout and attempt limits remain the fallback for consumers
that have not been configured. A record pins the policy snapshot on its first
assignment; retries, expiry, and terminal movement continue to use that
snapshot even if a later configuration update changes the consumer.
Configured acknowledgement timeouts are bounded to seven days and zero is
allowed for deterministic immediate-expiry workflows. Zero attempt limits are
rejected. Exhaustion retains the existing derived `<stream>.dead-letter`
transition and same-content move identity. The first slice does not add
backoff, explicit retry/dead-letter dispositions, named targets, provenance,
redrive, or a new wire-version negotiation mechanism.

## Rationale

The prior broker-wide policy could not express different failure budgets for
independent consumers of one stream. Persisting a small policy snapshot at the
consumer and delivery boundaries makes policy selection application-aware
without exposing files, nodes, or Raft groups. Pinning avoids silently changing
the outcome for an already in-flight poison record. The additive v1 operations
preserve existing clients and leave capability negotiation for the future
protocol compatibility decision.

The scope follows the evidence and alternatives in the [application-aware retry
design](../design/application-aware-retry-policy.md): Kafka, NATS JetStream,
and Redpanda provide useful durable consumer or delivery-policy precedents, but
their partition, acknowledgement, and dead-letter models do not transfer
directly to Runnel's stream/consumer contract. A broader policy with exponential
backoff, provenance, and cross-group redrive was rejected for this slice because
it would require new clock, storage, and compatibility decisions before the
consumer lifecycle was durable.

## Consequences

- Local and clustered consumers can select independent bounded attempt budgets.
- Consumer policy survives local restart, Raft replay, snapshots, and ownership
  transfer because it is part of existing consumer state.
- Legacy consumers retain current broker-wide behavior and old persisted state
  remains readable through serde defaults.
- Dead-letter records still contain only their existing key and payload; safe
  provenance and redrive remain tracked follow-up work in TD-018.
- The provisional protocol has no authorization or capability negotiation, so
  deployments must treat these operations as part of the current trusted
  administrative surface until that boundary is accepted.

## Evidence and follow-up gates

Focused local, clustered, protocol, and real-server tests cover policy
validation, idempotent inspection, per-consumer isolation, version pinning,
restart persistence, and existing derived dead-letter behavior. Remaining
backlog acceptance requires a separate decision and tests for backoff,
provenance, redrive, and richer terminal dispositions.
