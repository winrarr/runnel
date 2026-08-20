# Technical debt register

This register records known implementation shortcuts in the current vertical slice. Product gaps belong in [backlog.md](backlog.md); an entry may link to the backlog item that will retire it.

## TD-001: One process-wide broker lock

- Status: open
- Impact: all publishes, polls, acknowledgements, recovery-related state access, and health reads serialize behind one mutex. This limits throughput and makes tail-latency work impossible to evaluate against the intended architecture.
- Context: the lock keeps the first crash/recovery path easy to inspect and correct.
- Retirement condition: replace it with measured ownership boundaries and concurrency tests without weakening acknowledgement or recovery invariants. This is part of the storage and clustered-core backlog work.

## TD-002: One file and an in-memory index per stream

- Status: open
- Impact: startup scans the complete file and the record index grows with retained data. This prevents bounded recovery and memory usage for large streams and makes retention awkward.
- Context: the representation is intentionally the smallest durable log that can be tested end to end.
- Retirement condition: segmented, indexed storage with explicit format/version metadata, retention tests, and recovery benchmarks.

## TD-003: Provisional JSON-lines protocol and text-only server payload mapping

- Status: open
- Impact: the current wire format is not a compatibility contract, opens one connection per CLI operation, and maps payloads through UTF-8 strings at the server boundary.
- Context: JSON makes the first vertical slice inspectable and easy to exercise from shell tools.
- Retirement condition: a versioned protocol preserves binary payloads, explicit outcome classes, compatibility policy, and interoperability tests.

## TD-004: Consumer checkpoints are local files without ownership metadata

- Status: open
- Impact: consumer state cannot yet be safely moved between brokers, fenced, or coordinated for consumer groups and failover.
- Context: local durable checkpoints are sufficient for independent consumers in a single process.
- Retirement condition: consumer state and ownership are represented independently from local files and have crash, fencing, and membership tests.

## TD-005: Durability and delivery policies are hard-coded

- Status: open
- Impact: direct `sync_data` publishing and a fixed acknowledgement timeout are useful defaults but do not yet expose a documented durability mode, retry policy, retention policy, or backpressure budget.
- Context: the current implementation intentionally chooses one conservative path while semantics are being established.
- Retirement condition: each configurable policy has an explicit guarantee, bounded-resource behavior, and focused failure tests before it is exposed publicly.

## TD-006: Operational telemetry remains incomplete

- Status: open
- Impact: current metrics expose streams, storage bytes, and snapshot lifecycle counters, but cannot yet explain latency, throughput, consumer lag, redelivery, resource pressure, or most failure behavior under load.
- Context: the metrics endpoint exists early so deployment checks have a real surface.
- Retirement condition: the deployment-grade operations backlog item is implemented and its metrics are exercised by integration or benchmark tests.

## TD-007: Storage format compatibility is not yet defined

- Status: open
- Impact: the current Raft/state-machine formats have version checks and limited legacy recovery, but the new metadata/data-group directory layout has no migration path from the earlier single-group clustered layout. Long-lived rolling upgrades and in-place layout changes are not supported.
- Context: the clustered storage model is still changing, so startup refuses the old layout rather than silently ignoring acknowledged data.
- Retirement condition: storage metadata has an explicit upgrade and downgrade policy, a safe migration path for supported layout changes, and compatibility tests before durable format changes are relied upon.

## TD-008: Distributed Raft backend is an early static-cluster implementation

- Status: open
- Impact: `runnel-raft` now supports versioned local persistence, group-addressed framed TCP peer RPCs, a metadata group, one static data group per stream, reconciled stream activation, topology-free forwarding, stable stream metadata, durable publish request deduplication, follower restart, leader failure, recovery of an empty replacement replica from a compacted snapshot, interrupted-transfer retry from byte zero, and snapshot lifecycle telemetry. It still lacks dynamic membership, scalable placement and balancing, efficient large-stream representation, fencing policy, repeated-interruption cost controls, authentication, and production-grade operational policy. Replicating every stream to the same static voters is a useful three-node baseline but is not a scalable placement model.
- Context: the first process-level backend establishes a correctness baseline without committing the public model to Raft topology. All first-version groups use the same static voters, and consumer ownership remains local to a stream data group.
- Retirement condition: the distributed-engine backlog outcomes are complete, including topology-free access from any node, replicated metadata, failure and upgrade policy, security, observability, and documented production guarantees.

## TD-009: Snapshots rewrite the complete materialized group state

- Status: open
- Impact: snapshot creation and installation currently serialize or replace the complete retained state for a group. This keeps the first recovery path understandable, but snapshot cost grows with retained data and the default cadence is not yet tuned against realistic workloads.
- Context: the initial snapshot is deliberately independent from the compactable consensus log and is sufficient to recover a replacement replica without exposing storage details publicly.
- Retirement condition: measured recovery and hot-path benchmarks justify a staged snapshot or extent-manifest design with bounded transfer work, compatibility tests, and no loss of retained messages or consumer progress.

## TD-010: Clustered state materializes complete retained history

- Status: open
- Impact: the clustered state machine keeps retained messages in materialized state and persists that state after committed apply batches. Serialization, copying, memory use, and recovery cost grow with retained history, which can make hot-path latency and resource use unpredictable.
- Context: this is the smallest state-machine representation that makes the first replicated vertical slice and snapshot recovery easy to inspect.
- Retirement condition: retained-data growth, recovery, and resource benchmarks justify a durable representation whose append, read, and recovery work remains bounded without weakening ordering, replay, or acknowledgement guarantees.

## TD-011: End-to-end benchmark coverage is incomplete

- Status: open
- Impact: the current Criterion suite measures only local in-process paths. It does not yet establish containerized server, concurrent, clustered, resource-limited, competitor, or tail-latency baselines.
- Context: microbenchmarks were added first to catch local regressions while the broker semantics and cluster recovery behavior were still changing.
- Retirement condition: the performance backlog outcomes provide repeatable machine-readable local, container, clustered, and comparable-broker measurements with explicit workload and durability semantics.

## TD-012: Peer RPC connections are short-lived

- Status: open
- Impact: the current distributed transport opens a new TCP connection for each internal RPC. Connection setup and teardown can dominate small-message coordination latency and add avoidable scheduling and allocation work under load.
- Context: short-lived framed connections keep the first group-addressed transport simple and make request boundaries easy to inspect.
- Retirement condition: transport benchmarks demonstrate whether connection reuse, multiplexing, or another bounded communication strategy improves throughput and p99/p99.9 latency without changing failure or fencing behavior.

## TD-013: Native competitor benchmark semantics are not equivalent

- Status: open
- Impact: the first comparison baseline uses Runnel's host-side protocol client, Kafka/Redpanda's native Kafka performance clients, and NATS's native JetStream benchmark client. Publish batching, consumer acknowledgement behavior, client startup, and latency visibility differ, so the numbers cannot yet support a definitive product ranking.
- Context: native tools provide an immediately reproducible baseline while Runnel's public protocol and common benchmark client are still provisional. The result artifacts record each measurement boundary and configuration.
- Retirement condition: a common workload client or rigorously equivalent adapters measure durable publish, consume with application acknowledgement, batching, recovery, resource usage, and tail latency across all supported brokers while preserving each broker's explicitly stated guarantee.

## TD-014: Security audit has a documented optional-dependency exception

- Status: open
- Impact: the required dependency audit currently ignores `RUSTSEC-2026-0235`, which is reported for `rkyv 0.7.46` retained in the lockfile through an optional/dev-only `rust_decimal` dependency. The exception keeps the security workflow useful for actionable production dependencies, but it could hide the advisory if that feature becomes active.
- Context: the pinned stable security toolchain can parse current advisories, while the current dependency graph does not expose this optional serialization path in Runnel's runtime dependency tree.
- Retirement condition: remove the audit exception after the lockfile no longer contains the affected optional dependency or the dependency is upgraded to a fixed release, then verify the complete audit workflow without ignores.
