# ADR 0013: Start shared consumer delivery in the local engine

- Status: accepted
- Date: 2026-08-20

## Decision

The first shared-consumer implementation will live in `runnel-core` behind the topology-free engine contract. A named consumer owns durable progress, and multiple transient members can request work from that consumer. Different consumer names remain independent fan-out consumers.

Grouped deliveries use opaque delivery tokens. Acknowledgements may complete out of order, and records with the same requested key are not delivered concurrently within one consumer. Expired deliveries can be assigned again, while acknowledgements from the previous delivery are rejected. The first slice keeps one outstanding delivery per member and uses the existing local persistence model.

The public development protocol adds explicit grouped poll and acknowledgement operations. The legacy poll and acknowledgement operations remain supported for the existing single-member behavior.

This decision established the local semantic baseline. The initial clustered extension now applies the same public contract with replicated ownership, lease expiry, and stale-delivery fencing; its narrower guarantees and remaining policy gaps are recorded in [ADR 0015](0015-clustered-shared-consumer-ownership.md).

## Rationale

Runnel's target audience needs both durable fan-out and a simple way to scale a worker pool. A shared durable consumer provides that capability without exposing partitions, physical shards, or rebalancing protocols. Demand-driven work selection is easier to reason about for a small deployment than permanent worker ownership and naturally handles workers with different capacity.

Per-key delivery gates preserve the required ordering scope while allowing unrelated keys to progress concurrently. Delivery tokens make the failure boundary explicit and provide a path to fencing stale workers when the distributed engine later owns consumer state authoritatively.

The clustered implementation was intentionally deferred while the local conformance baseline was established. That baseline is now reused by the distributed adapter rather than creating a second interpretation of consumers and acknowledgements.

## Consequences

- Local applications can share work between members without configuring partitions or worker assignments.
- Grouped acknowledgements require a delivery token; this is an intentional development-protocol boundary for stale-delivery safety.
- The first local dispatcher scans the in-memory record index and limits each member to one outstanding delivery. This is a correctness baseline, not the target performance architecture.
- Grouped progress is durable, but active delivery leases are reconstructible volatile state and may redeliver after restart.
- Batching, cross-engine retry policy, dead-letter handling, and final clustered lease policy remain unfinished.
- Stable virtual-shard or key-affine ownership is an exploratory optimization and must be benchmarked against this demand-driven baseline before adoption.

## References

- [Product backlog](../backlog.md)
- [Current architecture](../architecture.md)
- [Distributed architecture alternatives](../design/distributed-architecture-options.md)
