# ADR 0014: Start retry and dead-letter handling in the local engine

- Status: accepted
- Date: 2026-08-20

## Decision

The local engine uses the acknowledgement timeout as the initial retry delay. It persists a delivery-attempt count with each durable consumer before returning the message. A local deployment may configure a maximum number of attempts; the default remains unlimited redelivery.

When a message reaches the configured limit, the broker appends its original key and payload to an automatically created stream named <source-stream>.dead-letter, then advances the source consumer past the message. The source consumer state is persisted after the dead-letter append. This preserves at-least-once behavior across the two local durable records: a crash can leave a duplicate dead-letter record, but it cannot turn the source checkpoint into a committed skip before the dead-letter append succeeds.

Delivery responses expose the attempt number. Local health metrics expose process-lifetime redelivery and dead-letter counters. This decision originally scoped the retry policy to the local engine. Clustered delivery now has an extension of this policy recorded in [ADR 0016](0016-clustered-retry-and-dead-letter-policy.md).

## Rationale

The existing acknowledgement timeout already provides a simple redelivery boundary. Persisting attempts makes retry behavior survive restart without introducing a separate scheduler or a second public consumer model. A bounded attempt policy gives small deployments a way to isolate poison messages while preserving the original payload for inspection.

The append-then-checkpoint order favors no loss over duplicate dead-letter output. An atomic cross-log move would require a transaction or reconciliation protocol that is not justified for this first local slice, but the ambiguity is explicit and recorded as technical debt.

## Consequences

- retry configuration is currently broker-wide and has no exponential backoff, jitter, per-consumer override, or redrive operation;
- dead-letter streams preserve the original key and payload but do not yet include source consumer, source offset, or attempt provenance;
- dead-letter streams are not recursively dead-lettered;
- a dead-letter stream counts as a normal stream and can be consumed and acknowledged through the existing protocol;
- retry attempt state is durable in the local consumer checkpoint, while active leases remain volatile and may redeliver after restart;
- the policy is represented in clustered consumer state by [ADR 0016](0016-clustered-retry-and-dead-letter-policy.md); richer policy remains future work.

## Scope history

ADR 0016 extends the initial local policy to clustered delivery. This record remains the source of the local append-then-checkpoint guarantee and its duplicate dead-letter caveat.

## References

- [Product backlog](../backlog.md)
- [Current architecture](../architecture.md)
- [Local shared-consumer delivery](0013-local-shared-consumer-delivery.md)
