# Technical debt register

This register records known implementation shortcuts in the current vertical slice. Product gaps belong in [backlog.md](backlog.md); an entry may link to the backlog item that will retire it. Remove entries when the shortcut is retired, and do not reuse their identifiers.

## TD-002: One file and a startup scan per local stream

- Status: open
- Impact: startup scans each complete stream file, older replay falls back to a linear scan, and retention cannot reclaim immutable regions independently. The bounded tail index prevents retained history from consuming unbounded index memory, but recovery and cold reads still grow with total retained data.
- Context: the one-file representation is intentionally the smallest durable log that can be tested end to end. The local engine now keeps only the newest 1,024 record locations in memory and has recovery coverage beyond that cache.
- Retirement condition: segmented, indexed storage with explicit format/version metadata, retention tests, and recovery benchmarks.

## TD-003: Provisional JSON-lines protocol and text-only server payload mapping

- Status: open
- Impact: the current wire format is not a compatibility contract, the development CLI still opens one connection per invocation, and payloads are mapped through UTF-8 strings at the server boundary. The reusable client now supports persistent sequential connections but does not change those protocol limitations.
- Context: JSON makes the first vertical slice inspectable and easy to exercise from shell tools. A bounded reusable client transport now centralizes connection and request timeout behavior for applications that use the provisional protocol.
- Retirement condition: a versioned protocol preserves binary payloads, explicit outcome classes, compatibility policy, and interoperability tests.

## TD-004: Local and clustered durable state have no supported migration path

- Status: open
- Impact: applications can keep the same public messaging intent when selecting the clustered engine, but retained local records, consumer progress, delivery attempts, and producer retry identity cannot be moved through a supported cutover. Growth from one node to three currently requires starting with empty clustered state or inventing an operational migration.
- Context: local checkpoints and stream logs remain appropriate for the single-node engine, while clustered consumer ownership is already replicated and fenced inside each stream data group. Their durable representations were developed as separate vertical slices.
- Retirement condition: the growth-from-one-node backlog outcome provides a versioned, validated, interruptible migration with explicit writer fencing, rollback boundaries, and end-to-end delivery tests.

## TD-005: Durability and delivery policies are hard-coded

- Status: open
- Impact: direct `sync_data` publishing and broker-wide acknowledgement timeout and attempt-limit settings are useful defaults but do not yet expose a documented durability mode, consumer-scoped retry policy, retention policy, or backpressure budget.
- Context: the current implementation intentionally chooses one conservative path while semantics are being established.
- Retirement condition: each configurable policy has an explicit guarantee, bounded-resource behavior, and focused failure tests before it is exposed publicly.

## TD-006: Operational telemetry remains incomplete

- Status: open
- Impact: current metrics expose request rates and latency buckets, connections, traffic bytes, publish/delivery/acknowledgement totals, redelivery, dead letters, storage bytes, health failures, and snapshot and peer-transport activity. They still cannot explain consumer lag, retained and reclaimable storage, admission rejection, queue saturation, replication progress, or resource pressure under load.
- Context: the metrics endpoint now covers the basic broker and transport path. An opt-in Rust timing feature and clustered profiling workflow provide deeper investigation data without adding timing calls to the default build, but these timings are not deployment-grade metrics.
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
- Impact: the Criterion suite remains focused on local in-process paths. The end-to-end harness provides statistically screened same-host clustered comparisons and longer history runs, but it does not yet cover the complete fault, slow-consumer, batching, p99.9, or semantically equivalent competitor workload matrix.
- Context: microbenchmarks were added first to catch local regressions while the broker semantics and cluster recovery behavior were still changing. Containerized, clustered Runnel, single-node native competitor, and first RF=3 competitor-publish measurements now exist, including scenario-scoped resource efficiency and optional Linux profiles. Performance-sensitive pull requests use a same-host current-versus-main clustered comparison as their primary evidence and retain a single-node diagnostic; hosted daily and `main` runs provide the longer Runnel history, and competitor comparisons are kept in separate weekly/manual suites. Repeated summaries retain median, observed range, and relative range evidence; workload equivalence and complete tail-latency and fault coverage remain incomplete.
- Retirement condition: the performance backlog outcomes provide repeatable machine-readable local, container, clustered, and comparable-broker measurements with explicit workload and durability semantics, plus enough repeated samples and profiling evidence to distinguish regressions from host noise.

## TD-012: Peer RPC connection strategy remains incomplete

- Status: open
- Impact: peer RPCs reuse connections, but ownership is split between streams retained by OpenRaft network clients and a process-wide compatibility pool for forwarding, data-group setup, and stateless first control requests. The global pool is capped at 64 peer addresses with four connections each; traffic beyond that bridge falls back to an unpooled connection, and there is no multiplexing or cluster-scoped lifecycle. Connection contention, head-of-line blocking, and snapshot interference therefore remain incompletely characterized.
- Context: the initial short-lived framed transport kept request boundaries easy to inspect. Reuse now covers append traffic, heartbeats, votes, snapshots, forwarding, and data-group setup, discards failed or timed-out connections, and has focused reuse, replacement, timeout, and concurrency tests. The optimization is merged, but transport ownership and end-to-end latency evidence are not yet complete.
- Retirement condition: transport benchmarks demonstrate whether connection reuse, multiplexing, or another bounded communication strategy improves throughput and p99/p99.9 latency without changing failure or fencing behavior.

## TD-013: Native competitor benchmark semantics are not equivalent

- Status: open
- Impact: the first comparison baseline uses Runnel's host-side protocol client, Kafka/Redpanda's native Kafka performance clients, and NATS's native JetStream benchmark client. Publish batching, consumer acknowledgement behavior, client startup, and latency visibility differ, so the numbers cannot yet support a definitive product ranking.
- Context: native tools provide an immediately reproducible single-node and RF=3 publish baseline while Runnel's public protocol and common benchmark client are still provisional. The result artifacts record each measurement boundary, topology, and configuration.
- Retirement condition: a common workload client or rigorously equivalent adapters measure durable publish, consume with application acknowledgement, batching, recovery, resource usage, and tail latency across all supported brokers while preserving each broker's explicitly stated guarantee.

## TD-014: Security audit has a documented optional-dependency exception

- Status: open; blocked on the transitive dependency graph
- Impact: the required dependency audit ignores `RUSTSEC-2026-0235`, which is reported for `rkyv 0.7.46`. The advisory is not in Runnel's active feature graph, but `cargo audit` scans the lockfile and reports optional package entries that are not compiled. The exception must remain until the affected lockfile entry can be removed or upgraded without weakening the OpenRaft dependency.
- Context: OpenRaft 0.9.25 depends on `byte-unit` with its default `byte` feature, which activates `rust_decimal 1.42.1`. `rust_decimal` declares `rkyv 0.7.46` as an optional feature dependency, but neither the default workspace features nor `--all-features --all-targets` activate that feature; `cargo tree --all-features --all-targets --invert rkyv@0.7.46` produces no dependency path. The advisory states that the fixed release is `rkyv >=0.8.17`, which is outside the `rust_decimal` 1.42.1 dependency requirement of `^0.7.46`. Downgrading or locally patching this indirect dependency would be speculative and could weaken compatibility or reintroduce unrelated defects.
- Retirement condition: remove the ignore only after a compatible upstream release or dependency path change removes `rkyv 0.7.46` from `Cargo.lock`, or after a reviewed upgrade of the affected serialization path makes the fixed `rkyv >=0.8.17` release available. Before retiring the exception, verify both `cargo tree --all-features --all-targets --invert rkyv@0.7.46` has no result and `cargo audit` passes without `--ignore`; until then, the security workflow must fail if the advisory appears in the active feature graph.

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
- Impact: the current durable attempt and acknowledgement bookkeeping still replaces and synchronously syncs a consumer state file on each delivery path. An in-memory cache now avoids repeated reads and JSON parsing while a consumer has active deliveries, but the durable write cost remains a throughput and tail-latency limit as concurrency and membership grow.
- Context: persisting the attempt before returning a message makes retry behavior survive restart and keeps the first correctness model straightforward. The active-state cache preserves that durability behavior, adds restart coverage for out-of-order acknowledgements and retry state, and produced an observed roughly 8% improvement in the focused benchmark; it does not yet remove per-delivery durable persistence.
- Retirement condition: benchmarked batching, group commit, or another bounded bookkeeping strategy reduces delivery overhead without losing durable retry state, acknowledgement safety, or predictable recovery.

## TD-020: Clustered delivery leases use absolute wall-clock deadlines

- Status: open
- Impact: a leader chooses an absolute expiry timestamp and replicates it with each delivery. Clock jumps or skew between successive leaders can make redelivery materially earlier or later than the configured acknowledgement timeout; delivery tokens prevent a stale acknowledgement from committing, but they do not make lease timing predictable.
- Context: replicated absolute deadlines establish a simple failover baseline without adding a separate lease service. The state machine now persists a per-group maximum command-time observation and evaluates expiry against that floor, so a backward wall-clock step cannot delay expiry after recovery or a leader change. Forward jumps, inter-node clock offsets, delayed evaluation, and no-leader intervals remain outside the invariant; a full monotonic-timer/reclaim or bounded-clock policy is still unresolved. Retry configuration and dead-letter provenance are tracked independently in TD-018, while retained delivery-state growth is covered by TD-010.
- Retirement condition: the supported clock assumptions and maximum timing error are documented and tested under leader changes, clock skew, and clock jumps, or delivery eligibility moves to a mechanism that does not depend on comparable node wall clocks while preserving stale-delivery fencing and at-least-once behavior.

## TD-022: Local durable I/O blocks asynchronous server workers

- Status: open
- Impact: local engine futures execute synchronous log reads, writes, file replacement, and `sync_data` operations directly when polled by the Tokio connection task. A slow or stalled filesystem can therefore occupy runtime workers, inflate unrelated request and health latency, and undermine the intended predictable tail behavior.
- Context: the local engine began as a synchronous library and is exposed through the shared asynchronous engine contract without a separate blocking-I/O boundary. Per-stream locking limits logical contention but does not isolate runtime scheduling from storage latency.
- Retirement condition: measured storage execution boundaries keep filesystem stalls from starving unrelated network, health, and shutdown work while preserving per-stream ordering, durable acknowledgement semantics, bounded queues, and useful backpressure.

## TD-023: External protocol admission remains incomplete

- Status: open
- Impact: the server now bounds connection count, request frame size, in-flight request work, and request duration, with explicit rejection responses and metrics. Slow-writer behavior, storage-stall isolation, and the full resource-pressure test matrix remain incomplete.
- Context: the development protocol favors a minimal persistent-connection loop. The admission layer now uses bounded frame reads, connection/request permits, and request deadlines while retaining graceful drain behavior for already-used connections.
- Retirement condition: the overload backlog outcome establishes configurable safe defaults and tests for connection count, request size, request duration, in-flight work, slow readers and writers, and rejection metrics without weakening graceful shutdown or normal client ergonomics.
