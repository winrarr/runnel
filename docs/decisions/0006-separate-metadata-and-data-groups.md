# ADR 0006: Separate metadata and stream data groups

- Status: accepted
- Date: 2026-08-19

## Decision

The clustered engine uses one replicated metadata Raft group and one replicated data group for each stream. The public protocol continues to address streams by name; group identities and node placement remain internal.

Stream creation is a durable lifecycle transition. The metadata group first records `Creating`, the configured nodes prepare the stream's data group, the data group establishes its initial durable stream state, and the metadata group records `Active`. Publish, consume, and acknowledgement requests are rejected until the stream is active. Retried creation resumes from the recorded state and is idempotent.

The peer protocol carries a group identity on every Raft RPC. A per-process group manager opens, restores, and routes metadata and stream data groups. The initial deployment keeps static membership and assigns the same configured three voters to every group.

## Rationale

Separating metadata from stream data makes the future placement and movement model explicit without exposing physical shards to applications. It also prevents the metadata log from becoming the retained message history and gives each stream an independent consensus and storage lifecycle.

The lifecycle is intentionally reconciled rather than pretending that two Raft groups can commit one atomic transaction. A crash between metadata creation, data-group preparation, data initialization, and activation leaves a state that can be resumed or rejected deterministically.

## Consequences

- Stream creation currently requires the configured nodes to prepare the data-group runtime before activation.
- Every initial data group uses the same static membership; dynamic placement, balancing, fencing, and membership changes remain future work.
- A new clustered storage layout is not silently interpreted as the old single-group layout. Starting against legacy clustered files returns an explicit migration error until a safe migration path exists.
- Consumer checkpoints are currently stored with their stream data group; transferable consumer ownership and consumer-group coordination are not yet implemented.
- The group manager and group-addressed transport provide the adapter boundary for later distributed engines, but engine selection remains process-wide.

## References

- [Cluster architecture](../architecture.md)
- [Multi-Raft implementation plan](../design/multi-raft-implementation-plan.md)
- [Replicated stream metadata](0005-replicated-stream-metadata.md)
