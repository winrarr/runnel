# ADR 0016: Add broker-wide retry limits and dead-letter outcomes to clustered delivery

- Status: accepted
- Date: 2026-08-20

## Decision

The clustered engine uses the acknowledgement timeout as the retry delay and accepts the same broker-wide optional maximum-attempt setting as the local engine. An absent limit means unlimited redelivery; zero is invalid; an attempt number starts at one and increases only when an expired delivery is assigned again.

When a source delivery reaches the configured limit, the source consumer is advanced and the original key and payload are appended to a derived `<source-stream>.dead-letter` stream. Clustered grouped delivery commits both outcomes in the source stream's Raft data group, so a committed dead-letter transition cannot be separated from the source progress by a process crash or leader change. The derived stream is resolved to that source data group when it is addressed through the public protocol.

Non-grouped `poll` and `ack` operations use the same replicated consumer state and policy. Their acknowledgement remains tokenless for compatibility; the grouped path continues to require its delivery token for fencing.

Dead-letter streams are not recursively dead-lettered. They use the normal consumer and acknowledgement model, including delivery tokens and their own retry state.

## Rationale

The initial clustered broker already makes assignment, lease expiry, and acknowledgement authoritative in the stream data group. Keeping the first retry policy broker-wide preserves the small public model and avoids introducing a second policy-coordination system before consumer-scoped configuration exists.

An atomic state-machine transition is possible because the first clustered layout keeps the derived dead-letter record in the source data group. This provides a stronger clustered outcome than the local append-then-checkpoint path without claiming exactly-once processing of the dead-letter consumer.

## Guarantees

- A message is delivered at most the configured number of attempts before the source consumer progresses past it.
- A message reaching the limit is available on the derived dead-letter stream with its original key and payload.
- A committed clustered dead-letter transition survives replica restart and leader change under the data group's Raft durability guarantee.
- Repeating a poll after a committed transition does not create another dead-letter record for the same source offset.
- Dead-letter consumers remain at least once and may redeliver if they do not acknowledge.

## Consequences

- The setting must be consistent across nodes; the value is carried in the replicated poll command so state-machine application is deterministic for both delivery modes.
- Retry backoff, jitter, consumer-scoped policies, redrive, and source offset/attempt provenance remain future work.
- The local engine retains its separate at-least-once append-then-checkpoint behavior and may expose a duplicate dead-letter record after a crash between those durable operations.
- Derived dead-letter names and their data-group resolution are implementation boundaries, not public topology concepts.

## References

- [Local retry and dead-letter policy](0014-local-retry-and-dead-letter-policy.md)
- [Clustered shared-consumer ownership](0015-clustered-shared-consumer-ownership.md)
- [Current architecture](../architecture.md)
- [Product backlog](../backlog.md)
- [Technical debt](../tech-debt.md)
