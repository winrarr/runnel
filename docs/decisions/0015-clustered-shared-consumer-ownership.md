# ADR 0015: Replicate initial shared-consumer ownership in the stream data group

- Status: accepted
- Date: 2026-08-20

## Decision

The initial clustered shared-consumer implementation keeps a consumer's committed progress, out-of-order acknowledgements, delivery attempts, in-flight member assignments, lease deadlines, and delivery tokens in the stream's replicated data-group state.

Grouped poll and acknowledgement requests are authoritative writes handled by the elected data-group leader. A client connected to another node is forwarded to that leader through the existing internal peer protocol. The public protocol continues to expose only stream, consumer, member, record, acknowledgement, and delivery-token concepts.

Each new assignment receives a token derived from the committed Raft log identity of the assignment command. Acknowledgement requires the member and token that currently own the delivery. When a lease has expired, the next grouped poll may assign the record again with a new token and incremented attempt number. The old acknowledgement is rejected as stale.

Lease deadlines are absolute millisecond timestamps selected by the leader and included in the replicated command. This makes state-machine application deterministic across replicas and provides a simple restart and leader-failover baseline. The configured acknowledgement timeout is expected to be consistent across nodes; a final lease and fencing model remains future work.

The broker-wide maximum attempt setting applies to clustered grouped delivery. When a message reaches that limit, the source consumer's progress and a derived `.dead-letter` record are committed in the same stream data group. The derived stream is resolved back to that data group when addressed through the public protocol, and dead-letter streams are not recursively dead-lettered.

## Rationale

Replicating ownership in the same data group as the records gives the first cluster a single ordering point for publishing, assignment, and acknowledgement without exposing physical partitions or requiring an external coordinator. It also means a replacement leader can recover the consumer state from the same durable replication and snapshot path as the stream.

The design deliberately keeps the first scheduler demand-driven and bounded: one outstanding delivery per member, a simple record-index scan, and scoped key exclusion. This preserves the local contract and leaves room for later virtual-shard, key-affine, or batched scheduling work without making those choices part of the public model.

## Guarantees

- An acknowledgement accepted by the data-group leader is replicated under that group's Raft durability guarantee before the client receives success.
- A message is delivered at least once unless the configured clustered grouped-delivery attempt limit moves it to the derived dead-letter stream.
- A member may receive the same delivery again before acknowledgement, but a member's active lease is returned consistently until it expires.
- Expired or superseded delivery tokens cannot acknowledge a later assignment.
- Records with the same key are not assigned concurrently within one shared consumer, while unrelated keys may progress concurrently.
- A process or leader failure may cause redelivery after the lease boundary; the system does not claim exactly-once processing.
- A message reaching the configured clustered attempt limit is not delivered again to the source consumer and is available through its derived dead-letter stream.

## Consequences

- The clustered backend now implements the reusable shared-delivery contract and process-level failure tests.
- Consumer delivery state increases the replicated and snapshot state for each stream; the current materialized representation is not the long-term large-stream design.
- Clustered backoff, richer dead-letter provenance, policy selection per consumer, and final fencing semantics are not enabled by this decision.
- Lease behavior depends on a consistent wall-clock configuration across nodes. The command carries the leader's chosen deadline, but clock quality and configuration drift remain operational concerns.
- A future scheduler may replace the scan and one-delivery-per-member policy behind the same engine contract, subject to benchmarks and the ordering and fencing invariants.

## References

- [Local shared-consumer delivery](0013-local-shared-consumer-delivery.md)
- [Current architecture](../architecture.md)
- [Product backlog](../backlog.md)
- [Technical debt](../tech-debt.md)
