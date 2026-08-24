# Technical debt register

This register records known implementation shortcuts in the current vertical slice. Product gaps belong in [backlog.md](backlog.md); an entry may link to the backlog item that will retire it.

## TD-001: One process-wide broker lock

- Status: open
- Impact: all publishes, polls, acknowledgements, recovery-related state access, and health reads serialize behind one mutex. This limits throughput and makes tail-latency work impossible to evaluate against the intended architecture.
- Context: the lock keeps the first crash/recovery path easy to inspect and correct. A bounded Criterion baseline now measures same-stream durable publishing with 1, 2, 4, and 8 workers; the observed median throughput stayed around 347–373 thousand elements per second while batch time increased from 179 microseconds to 1.45 milliseconds. This is an end-to-end persistence and scheduling measurement, not an isolated mutex-wait measurement.
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
- Context: the metrics endpoint exists early so deployment checks have a real surface. An opt-in Rust timing feature and clustered profiling workflow now provide investigation data without adding timing calls to the default build, but these timings are not yet deployment-grade metrics.
- Retirement condition: the deployment-grade operations backlog item is implemented and its metrics are exercised by integration or benchmark tests.

## TD-007: Storage format compatibility is not yet defined

- Status: open
- Impact: the current Raft/state-machine formats have version checks and limited legacy recovery, but the new metadata/data-group directory layout has no migration path from the earlier single-group clustered layout. Long-lived rolling upgrades and in-place layout changes are not supported.
- Context: the clustered storage model is still changing, so startup refuses the old layout rather than silently ignoring acknowledged data.
- Retirement condition: storage metadata has an explicit upgrade and downgrade policy, a safe migration path for supported layout changes, and compatibility tests before durable format changes are relied upon.

## TD-008: Distributed Raft backend is an early static-cluster implementation

- Status: open
- Impact: `runnel-raft` now supports versioned local persistence, group-addressed framed TCP peer RPCs, a metadata group, one static data group per stream, reconciled stream activation, topology-free forwarding, stable stream metadata, durable publish request deduplication, follower restart, leader failure, recovery of an empty replacement replica from a compacted snapshot, interrupted-transfer retry from byte zero, and snapshot lifecycle telemetry. It still lacks dynamic membership, scalable placement and balancing, efficient large-stream representation, fencing policy, repeated-interruption cost controls, authentication, and production-grade operational policy. Replicating every stream to the same static voters is a useful three-node baseline but is not a scalable placement model.
- Context: the first process-level backend establishes a correctness baseline without committing the public model to Raft topology. All first-version groups use the same static voters, and shared-consumer ownership is replicated within each stream data group even though placement and policy remain simple.
- Retirement condition: the distributed-engine backlog outcomes are complete, including topology-free access from any node, replicated metadata, failure and upgrade policy, security, observability, and documented production guarantees.

## TD-009: Snapshots rewrite the complete materialized group state

- Status: open
- Impact: snapshot creation and installation currently serialize or replace the complete retained state for a group. This keeps the first recovery path understandable, but snapshot cost grows with retained data and the default cadence is not yet tuned against realistic workloads.
- Context: the initial snapshot is deliberately independent from the compactable consensus log and is sufficient to recover a replacement replica without exposing storage details publicly.
- Retirement condition: measured recovery and hot-path benchmarks justify a staged snapshot or extent-manifest design with bounded transfer work, compatibility tests, and no loss of retained messages or consumer progress.

## TD-010: Clustered state materializes complete retained history

- Status: open
- Impact: the clustered state machine keeps retained messages in materialized state and appends JSON journal records for committed apply entries. The journal avoids a complete state-file replacement on every apply, but serialization, copying, memory use, and recovery replay still grow with retained history.
- Context: the incremental journal and checkpoint path is the first durable step toward separating hot-path appends from materialized recovery state. It deliberately keeps the existing JSON checkpoint and snapshot model while compatibility and crash behavior are established.
- Retirement condition: retained-data growth, recovery, and resource benchmarks justify a durable representation whose append, read, and recovery work remains bounded without weakening ordering, replay, or acknowledgement guarantees.

## TD-011: End-to-end benchmark coverage is incomplete

- Status: open
- Impact: the Criterion suite remains focused on local in-process paths, while the end-to-end benchmark harness does not yet establish clustered, statistically stable, equivalent competitor, or complete tail-latency baselines.
- Context: microbenchmarks were added first to catch local regressions while the broker semantics and cluster recovery behavior were still changing. Containerized, clustered Runnel, single-node native competitor, and first RF=3 competitor-publish measurements now exist, including scenario-scoped resource efficiency and optional Linux profiles. Pull requests now get a short Runnel-only report, daily and `main` runs provide the primary longer Runnel history, and competitor comparisons are kept in separate weekly/manual suites; their semantic and statistical limitations remain.
- Retirement condition: the performance backlog outcomes provide repeatable machine-readable local, container, clustered, and comparable-broker measurements with explicit workload and durability semantics, plus enough repeated samples and profiling evidence to distinguish regressions from host noise.

## TD-012: Peer RPC connections are short-lived

- Status: open
- Impact: the current distributed transport opens a new TCP connection for each internal RPC. Connection setup and teardown can dominate small-message coordination latency and add avoidable scheduling and allocation work under load.
- Context: short-lived framed connections keep the first group-addressed transport simple and make request boundaries easy to inspect.
- Retirement condition: transport benchmarks demonstrate whether connection reuse, multiplexing, or another bounded communication strategy improves throughput and p99/p99.9 latency without changing failure or fencing behavior.

## TD-013: Native competitor benchmark semantics are not equivalent

- Status: open
- Impact: the first comparison baseline uses Runnel's host-side protocol client, Kafka/Redpanda's native Kafka performance clients, and NATS's native JetStream benchmark client. Publish batching, consumer acknowledgement behavior, client startup, and latency visibility differ, so the numbers cannot yet support a definitive product ranking.
- Context: native tools provide an immediately reproducible single-node and RF=3 publish baseline while Runnel's public protocol and common benchmark client are still provisional. The result artifacts record each measurement boundary, topology, and configuration.
- Retirement condition: a common workload client or rigorously equivalent adapters measure durable publish, consume with application acknowledgement, batching, recovery, resource usage, and tail latency across all supported brokers while preserving each broker's explicitly stated guarantee.

## TD-014: Security audit has a documented optional-dependency exception

- Status: open
- Impact: the required dependency audit currently ignores `RUSTSEC-2026-0235`, which is reported for `rkyv 0.7.46` retained in the lockfile through an optional/dev-only `rust_decimal` dependency. The exception keeps the security workflow useful for actionable production dependencies, but it could hide the advisory if that feature becomes active.
- Context: the pinned stable security toolchain can parse current advisories, while the current dependency graph does not expose this optional serialization path in Runnel's runtime dependency tree.
- Retirement condition: remove the audit exception after the lockfile no longer contains the affected optional dependency or the dependency is upgraded to a fixed release, then verify the complete audit workflow without ignores.

## TD-016: Initial shared dispatcher uses a simple scan and one delivery per member

- Status: open
- Impact: grouped polling scans the in-memory record index and allows only one outstanding delivery per member. This bounds the first implementation and keeps its behavior inspectable, but it limits throughput, batching, and scalability for large or highly concurrent worker pools.
- Context: the first implementation is a semantic baseline for demand-driven delivery, not a target storage or scheduling architecture.
- Retirement condition: workload benchmarks justify bounded scheduling, batching, or stable internal placement improvements while preserving scoped ordering, backpressure, and stale-delivery fencing.

## TD-017: Dead-letter movement spans separate durable records

- Status: open
- Impact: a crash after a dead-letter append but before the source consumer checkpoint is persisted can produce a duplicate dead-letter record when the source message is retried. The ordering prevents acknowledged progress from being silently skipped, but dead-letter consumers must tolerate at-least-once duplicates.
- Context: the first local policy uses the existing append-only stream logs and atomic consumer checkpoints without adding a cross-log transaction or reconciliation journal.
- Retirement condition: a durable transaction or recovery reconciliation protocol provides duplicate-safe dead-letter movement while preserving bounded recovery and at-least-once guarantees.

## TD-018: Retry policy and dead-letter provenance are coarse

- Status: open
- Impact: retry configuration is broker-wide, backoff is limited to the acknowledgement timeout, and dead-letter records preserve only the original key and payload. Applications cannot yet select policy per consumer or reliably identify the source offset and attempt history from the dead-letter record alone.
- Context: the initial policy establishes durable attempt counting and usable local and clustered grouped-delivery dead-letter streams before the public consumer configuration model is finalized.
- Retirement condition: consumer-scoped policy, documented backoff and redrive behavior, and durable dead-letter provenance are available and covered by restart, retry, and clustered ownership tests.

## TD-019: Delivery bookkeeping synchronizes consumer state per delivery

- Status: open
- Impact: the current durable attempt and acknowledgement bookkeeping replaces and syncs a consumer state file for each delivery path. The current 100-byte local benchmark run measured roughly 31 thousand shared-consumer messages per second in this configuration, but this cost will limit throughput and tail-latency headroom as concurrency and membership grow.
- Context: persisting the attempt before returning a message makes retry behavior survive restart and keeps the first correctness model straightforward.
- Retirement condition: benchmarked batching, group commit, or another bounded bookkeeping strategy reduces delivery overhead without losing durable retry state, acknowledgement safety, or predictable recovery.

## TD-020: Clustered shared-consumer policy is only a semantic baseline

- Status: open
- Impact: the clustered engine now replicates shared progress, in-flight ownership, lease expiry, stale-delivery fencing, broker-wide attempt limits, and atomic dead-letter transitions for grouped and non-grouped delivery. Backoff, provenance, consumer-scoped configuration, and final lease/fencing policy remain unfinished. Delivery state also remains part of the complete materialized stream-group state.
- Context: the first multi-node implementation establishes the contract and failure boundary before policy and placement work are expanded.
- Retirement condition: clustered consumers have a documented policy for backoff, provenance, consumer-scoped configuration, and stronger failover fencing semantics, with representative recovery and performance tests that preserve at-least-once delivery and scoped ordering.
