# ADR 0004: Use Multi-Raft as the first distributed engine

- Status: accepted
- Date: 2026-08-19

## Decision

Runnel's first multi-node implementation will use Multi-Raft with OpenRaft 0.9 behind a Runnel-owned engine boundary.

The initial clustered topology will use three statically configured voters, one metadata group, and one data group per stream. Each first-version data group will be replicated to all three nodes. Public requests may reach any node; the receiving node forwards operations to the elected leader over the internal peer protocol, and leader-routed reads reject stale participants. Stable publish request IDs are deduplicated in replicated state so a safe retry returns the original offset. The public model will remain streams, records, consumers, acknowledgements, and related delivery intent rather than exposing Raft groups, leaders, partitions, or replica placement.

The common engine boundary will express Runnel messaging semantics and outcomes. It will not expose consensus-specific types. The local engine remains supported and will implement the same boundary. Engine selection is made at process startup; mixed engines and live engine migration are deferred.

An acknowledged durable publish completes only after quorum commit and durable state-machine application. The compactable consensus log and retained message history are separate storage concepts. Durable consumer checkpoints and producer request identity belong to replicated broker state; delivery leases may remain reconstructible volatile state in the initial implementation.

Runnel's MSRV is raised to Rust 1.88 to support the selected dependency graph without relying on a fragile transitive compatibility pin.

## Rationale

Multi-Raft is the smallest established design that provides a credible path to quorum durability, leader fencing, recovery, and future membership changes while allowing independent ordering groups to progress concurrently. It gives Runnel a correctness baseline against which sequenced-quorum/copyset, chain-replication, and other specialized engines can later be measured.

OpenRaft provides the runtime integration, pluggable storage and networking, snapshots, membership, linearizable reads, metrics, and testing support needed for the first implementation. Its pre-1.0 API and on-disk type evolution require exact pinning, adapter isolation, and focused upgrade tests. TiKV's `raft` crate remains a fallback if direct control of the consensus event loop becomes necessary for measured performance work, but it would require Runnel to own more of the consensus integration immediately.

The three-node static topology intentionally limits the first implementation's scope. Dynamic placement, virtual shard movement, copysets, and chain topology are valuable future options, but implementing them before the semantic and failure-test baseline would combine too many independent correctness problems.

## Consequences

- The first cluster will not yet provide automatic balancing, dynamic membership, virtual-shard splitting, follower reads, mixed replication engines, or live engine migration.
- Every Raft group adds runtime, storage, snapshot, and observability overhead; the initial implementation must keep group density modest and benchmark it.
- Quorum durability adds network and storage latency relative to the local engine. The acknowledgement point must remain visible in metrics and documentation.
- OpenRaft is an internal implementation dependency, not a public protocol or storage format. Its types must not cross the semantic engine boundary.
- The MSRV change affects local setup and CI, but the pinned development toolchain remains independent from the supported minimum.
- A future engine must pass the same semantic, failure, recovery, and benchmark suites before it can be offered as a selectable implementation.

## References

- [Distributed architecture alternatives](../design/distributed-architecture-options.md)
- [Proposed Multi-Raft implementation plan](../design/multi-raft-implementation-plan.md)
- [OpenRaft project](https://github.com/databendlabs/openraft)
