# ADR 0007: Snapshot-based replica recovery

- Status: accepted
- Date: 2026-08-19

## Decision

The first clustered backend uses OpenRaft state-machine snapshots to compact consensus history and recover a missing or lagging stream replica. Snapshots contain versioned materialized stream state, durable consumer checkpoints, and producer request deduplication state; retained broker history remains conceptually separate from the compactable Raft log.

Snapshot creation is automatic after a bounded number of committed log entries and can also be triggered by the engine for deterministic recovery tests and administrative workflows. OpenRaft retains a small suffix of entries after a snapshot so normal replication can bridge short lag without a transfer. A replacement node starts with an empty local data directory, learns stream identity from replicated metadata, materializes the group runtime on demand, and receives the snapshot through the normal group-addressed peer protocol.

Snapshot peer RPCs use bounded chunks, and the broker reports received chunk counts and bytes as well as final-chunk and installation outcomes through clustered metrics.

Persisted snapshots are validated before the state machine accepts them or exposes them after restart. State-machine replacement is persisted before the installed snapshot record is published locally, so a process failure between those writes leaves either the previous recoverable snapshot or the newer durable materialized state rather than an acknowledged state that is only present in memory.

## Rationale

Consensus entries are a recovery mechanism, not retained message history. Requiring a replacement to replay every historical command would make recovery depend on unbounded log retention and would couple consensus maintenance to replay semantics. A versioned snapshot gives the cluster a bounded recovery path while preserving the public stream model.

Lazy group materialization is required for replacement recovery: a node whose local group directory was lost cannot receive a group-addressed snapshot if the peer listener rejects the group before it exists. Metadata is the authority for resolving the group identity, and the normal group manager remains responsible for its local manifest and storage boundaries.

## Consequences

- The initial snapshot implementation rewrites the complete materialized state of a group; its recovery and performance limits are tracked as technical debt.
- Snapshot transfer and installation are not yet exposed as public broker operations and do not change the client programming model.
- Snapshot lifecycle and chunk counters are exposed through the clustered metrics endpoint. An interrupted transfer is currently restarted from byte zero; repeated interruption cost and durable partial-transfer resumption remain future work.
- Snapshot cadence and retained-log suffix are conservative initial defaults and require workload benchmarks before being treated as production tuning.
- Future extent or copyset storage can preserve the snapshot contract while transferring immutable data manifests and payload extents separately.

## References

- [Cluster architecture](../architecture.md)
- [Multi-Raft implementation plan](../design/multi-raft-implementation-plan.md)
- [Replica recovery backlog](../backlog.md)
