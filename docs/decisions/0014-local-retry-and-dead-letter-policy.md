# ADR 0014: Start retry and dead-letter handling in the local engine

- Status: accepted
- Date: 2026-08-20

## Decision

The local engine uses the acknowledgement timeout as the initial retry delay. It persists a delivery-attempt count with each durable consumer before returning the message. A local deployment may configure a maximum number of attempts; the default remains unlimited redelivery.

When a message reaches the configured limit, the broker appends its original key and payload to an automatically created stream named <source-stream>.dead-letter, with a bounded hashed fallback for long source names, then advances the source consumer past the message. New moves persist an internal source-stream/source-consumer/source-offset identity in the target log. Retrying or reopening can reconcile a completed target append with the same key and payload before persisting source progress; mismatched content fails instead of acknowledging the source. The source consumer state is persisted only after the durable target append or reconciliation succeeds.

Delivery responses expose the attempt number. Local health metrics expose process-lifetime redelivery and dead-letter counters. This decision originally scoped the retry policy to the local engine. Clustered delivery now has an extension of this policy recorded in [ADR 0016](0016-clustered-retry-and-dead-letter-policy.md).

## Rationale

The existing acknowledgement timeout already provides a simple redelivery boundary. Persisting attempts makes retry behavior survive restart without introducing a separate scheduler or a second public consumer model. A bounded attempt policy gives small deployments a way to isolate poison messages while preserving the original payload for inspection.

The append-then-checkpoint order favors no loss. Stable move identity adds reconciliation without an atomic cross-log transaction. The focused pre-acknowledgement failure and reopen test establishes reuse of a completed target append, but legacy records and remaining write/sync failure boundaries prevent a blanket duplicate-free guarantee. Those limits remain explicit in [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records).

## Consequences

- retry configuration is currently broker-wide and has no exponential backoff, jitter, per-consumer override, or redrive operation;
- dead-letter streams preserve the original key and payload but do not yet include source consumer, source offset, or attempt provenance;
- dead-letter streams are not recursively dead-lettered;
- a dead-letter stream counts as a normal stream and can be consumed and acknowledged through the existing protocol;
- retry attempt state is durable in the local consumer journal/checkpoint, while active leases remain volatile and may redeliver after restart;
- the policy is represented in clustered consumer state by [ADR 0016](0016-clustered-retry-and-dead-letter-policy.md); richer policy remains future work.

## Scope history

ADR 0016 extends the initial local policy to clustered delivery. Local movement originally had no reconciliation identity; legacy dead-letter records remain opaque. This record describes the current append-or-reconcile-then-checkpoint behavior without claiming atomic local movement or complete I/O failure coverage.

## References

- [Product backlog](../backlog.md)
- [Current architecture](../architecture.md)
- [Local shared-consumer delivery](0013-local-shared-consumer-delivery.md)
