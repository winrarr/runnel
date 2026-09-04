# Technical debt register

This register records known implementation shortcuts in the current vertical slice. Product gaps belong in [backlog.md](backlog.md); an entry may link to the backlog item that will retire it. Remove entries when the shortcut is retired, and do not reuse their identifiers.

## TD-002: One file and a startup scan per local stream

- Status: open
- Impact: startup scans each complete stream file, older replay falls back to a linear scan, and retention cannot reclaim immutable regions independently. The bounded tail index prevents retained history from consuming unbounded index memory, but recovery and cold reads still grow with total retained data.
- Context: the one-file representation is intentionally the smallest durable log that can be tested end to end. The local engine now keeps only the newest 1,024 record locations in memory, and diagnostic replay benchmarks cross that bound at 65,537 and 131,072 retained records. Those measurements expose cold-replay growth but do not provide segmented storage, retention, or storage-amplification evidence.
- Retirement condition: segmented, indexed storage with explicit format/version metadata, retention tests, and recovery benchmarks.

## TD-003: Provisional JSON-lines protocol and limited payload compatibility

- Status: open
- Impact: the current wire format is not a compatibility contract, the development CLI still opens one connection per invocation, and the legacy text path still maps payloads through UTF-8 strings at the server boundary. Binary-safe client and server methods now carry opaque payloads as padded base64 inside JSON, preserving bytes at the cost of representation overhead.
- Context: JSON makes the first vertical slice inspectable and easy to exercise from shell tools. The reusable client now supports persistent sequential connections, bounded timeouts, and binary payload methods, but protocol versioning, negotiation, and compatibility policy remain undefined.
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
- Impact: current metrics expose request rates and latency buckets, connections, traffic bytes, publish/delivery/acknowledgement totals, in-flight deliveries, redelivery, dead letters, storage bytes, configured admission limits, admission rejection counters, health failures, and snapshot and peer-transport activity. They still cannot explain consumer lag, retained and reclaimable storage, queue saturation, replication progress, or resource pressure under load.
- Context: the metrics endpoint now covers the basic broker and transport path, reports the shared engine's currently tracked in-flight deliveries for both local and clustered delivery, and exposes the configured admission boundaries and their rejection counters. Real-server tests cover in-flight saturation, slow readers, and slow-writer recovery. An opt-in Rust timing feature and clustered profiling workflow provide deeper investigation data without adding timing calls to the default build, but these timings are not deployment-grade metrics.
- Retirement condition: the deployment-grade operations backlog item is implemented and its metrics are exercised by integration or benchmark tests.

## TD-007: Storage format compatibility is not yet defined

- Status: open
- Impact: the current Raft/state-machine formats have version checks and limited legacy recovery, but the new metadata/data-group directory layout has no migration path from the earlier single-group clustered layout. Long-lived rolling upgrades and in-place layout changes are not supported.
- Context: startup now performs read-only preflight validation for clustered logs, checkpoints, journals, snapshots, manifests, and legacy layouts and fails closed before opening or mutating unsupported state. This is a safety boundary, not a migration or downgrade path.
- Retirement condition: storage metadata has an explicit upgrade and downgrade policy, a safe migration path for supported layout changes, and compatibility tests before durable format changes are relied upon.

## TD-008: Distributed Raft backend is an early static-cluster implementation

- Status: open
- Impact: `runnel-raft` now supports versioned local persistence, group-addressed framed TCP peer RPCs, a metadata group, one static data group per stream, reconciled stream activation, topology-free forwarding, stable stream metadata, durable publish request deduplication, follower restart, leader failure, recovery of an empty replacement replica from a compacted snapshot, interrupted-transfer retry from byte zero, and snapshot lifecycle telemetry. It still lacks dynamic membership, scalable placement and balancing, efficient large-stream representation, fencing policy, repeated-interruption cost controls, authentication, and production-grade operational policy. Replicating every stream to the same static voters is a useful three-node baseline but is not a scalable placement model.
- Context: the first process-level backend establishes a correctness baseline without committing the public model to Raft topology. All first-version groups use the same static voters, and shared-consumer ownership is replicated within each stream data group even though placement and policy remain simple.
- Retirement condition: the distributed-engine backlog outcomes are complete, including topology-free access from any node, replicated metadata, failure and upgrade policy, security, observability, and documented production guarantees.

## TD-009: Snapshots rewrite the complete materialized group state

- Status: open
- Impact: snapshot creation and installation currently serialize or replace the complete retained state for a group. This keeps the first recovery path understandable, but snapshot cost grows with retained data and the default cadence is not yet tuned against realistic workloads.
- Context: the initial snapshot is deliberately independent from the compactable consensus log and is sufficient to recover a replacement replica without exposing storage details publicly. Snapshot and checkpoint serialization now uses borrowed retained-message views rather than cloning every message, with focused retained-state recovery coverage; the complete materialized state is still serialized and replaced.
- Retirement condition: measured recovery and hot-path benchmarks justify a staged snapshot or extent-manifest design with bounded transfer work, compatibility tests, and no loss of retained messages or consumer progress.

## TD-010: Clustered state materializes complete retained history

- Status: open
- Impact: the clustered state machine keeps retained messages in materialized state and appends JSON journal records for committed apply entries. The journal avoids a complete state-file replacement on every apply, but serialization, copying, memory use, and recovery replay still grow with retained history.
- Context: the incremental journal and checkpoint path is the first durable step toward separating hot-path appends from materialized recovery state. Journal apply and replay now avoid redundant retained-payload clones and persist the journal batch before materializing in-memory state, while deliberately keeping the existing JSON checkpoint, snapshot, and journal formats. Serialization, copying, memory use, and recovery replay still grow with retained history.
- Retirement condition: retained-data growth, recovery, and resource benchmarks justify a durable representation whose append, read, and recovery work remains bounded without weakening ordering, replay, or acknowledgement guarantees.

## TD-011: End-to-end benchmark coverage is incomplete

- Status: open
- Impact: the Criterion suite remains focused on local in-process paths. The end-to-end harness provides statistically screened same-host clustered comparisons and longer history runs, but it does not yet cover the complete fault, slow-consumer, batching, p99.9, or semantically equivalent competitor workload matrix.
- Context: microbenchmarks were added first to catch local regressions while the broker semantics and cluster recovery behavior were still changing. Containerized, clustered Runnel, retained-history restart/replay, peer-forwarding saturation, publish-batch, single-node native competitor, and first RF=3 competitor-publish measurements now exist, with scenario-scoped resource efficiency, aggregate/per-node CPU, resident-memory and storage-byte samples, p99.9 metadata where applicable, optional Linux profiles, and explicit semantic guardrails. Cluster probes now cover bootstrap-leader and non-bootstrap-follower process stop/restart sequences, and a sequential matrix runner preserves independent case artifacts across payload, delay, concurrency, runtime, retained-history, and repetition dimensions. Performance-sensitive pull requests use a same-host current-versus-main clustered comparison as their primary evidence and retain a single-node diagnostic; hosted daily runs provide the longer Runnel history, and competitor comparisons are kept in separate weekly/manual suites. Repeated summaries retain median, observed range, and relative range evidence; workload equivalence, complete tail-latency and fault coverage, slow-consumer backpressure proof, and stable optimization evidence remain incomplete.
- Retirement condition: the performance backlog outcomes provide repeatable machine-readable local, container, clustered, and comparable-broker measurements with explicit workload and durability semantics, plus enough repeated samples and profiling evidence to distinguish regressions from host noise.

## TD-012: Peer RPC connection strategy remains incomplete

- Status: open
- Impact: peer RPCs reuse connections, but ownership is split between streams retained by OpenRaft network clients and a `GroupManager`-scoped compatibility pool for forwarding, data-group setup, and stateless first control requests. Each pooled peer has five bounded connections: one reserved for Raft control traffic and four shared by forwarding and setup traffic. Each transport is capped at 64 peer addresses, traffic beyond that bridge falls back to an unpooled connection, and there is no multiplexing or cross-group pooling. Connection contention, head-of-line blocking, snapshot interference, and end-to-end latency therefore remain incompletely characterized.
- Context: the initial short-lived framed transport kept request boundaries easy to inspect. Reuse now covers append traffic, heartbeats, votes, snapshots, forwarding, and data-group setup, reserves control capacity, bounds idle-pool waits, lazily expires idle compatibility sockets, discards failed or timed-out connections, and has focused reuse, replacement, timeout, concurrency, and transport-lifecycle tests. The compatibility pool now closes with its owning engine, but persistent per-group streams and stable high-contention latency evidence are not yet complete.
- Retirement condition: transport benchmarks demonstrate whether connection reuse, multiplexing, or another bounded communication strategy improves throughput and p99/p99.9 latency without changing failure or fencing behavior.

## TD-013: Native competitor benchmark semantics are not equivalent

- Status: open
- Impact: the first comparison baseline uses Runnel's host-side protocol client, Kafka/Redpanda's native Kafka performance clients, and NATS's native JetStream benchmark client. Publish batching, consumer acknowledgement behavior, client startup, and latency visibility differ, so the numbers cannot yet support a definitive product ranking.
- Context: native tools provide an immediately reproducible single-node and RF=3 publish baseline while Runnel's public protocol and common benchmark client are still provisional. Results now record operation-specific acknowledgement, durability, replication, delivery, batching, client, latency, topology, and resource boundaries, reject inconsistent declarations, and mark mismatched comparisons as experimental and non-ranking. A common equivalent workload client and fully comparable consume/recovery measurements remain absent.
- Retirement condition: a common workload client or rigorously equivalent adapters measure durable publish, consume with application acknowledgement, batching, recovery, resource usage, and tail latency across all supported brokers while preserving each broker's explicitly stated guarantee.

## TD-014: Security audit has a documented optional-dependency exception

- Status: open; blocked on the transitive dependency graph
- Impact: the required dependency audit ignores `RUSTSEC-2026-0235`, which is reported for `rkyv 0.7.46`. The advisory is not in Runnel's active feature graph, but `cargo audit` scans the lockfile and reports optional package entries that are not compiled. The exception must remain until the affected lockfile entry can be removed or upgraded without weakening the OpenRaft dependency.
- Context: OpenRaft 0.9.25 depends on `byte-unit` with its default `byte` feature, which activates `rust_decimal 1.42.1`. `rust_decimal` declares `rkyv 0.7.46` as an optional feature dependency, but neither the default workspace features nor `--all-features --all-targets` activate that feature; `cargo tree --all-features --all-targets --invert rkyv@0.7.46` produces no dependency path. The advisory states that the fixed release is `rkyv >=0.8.17`, which is outside the `rust_decimal` 1.42.1 dependency requirement of `^0.7.46`. Downgrading or locally patching this indirect dependency would be speculative and could weaken compatibility or reintroduce unrelated defects.
- Retirement condition: remove the ignore only after a compatible upstream release or dependency path change removes `rkyv 0.7.46` from `Cargo.lock`, or after a reviewed upgrade of the affected serialization path makes the fixed `rkyv >=0.8.17` release available. Before retiring the exception, verify both `cargo tree --all-features --all-targets --invert rkyv@0.7.46` has no result and `cargo audit` passes without `--ignore`; until then, the security workflow must fail if the advisory appears in the active feature graph.

## TD-016: Initial shared dispatcher uses a simple scan and one delivery per member

- Status: open
- Impact: grouped polling scans the in-memory record index and allows only one outstanding delivery per member. This bounds the first implementation and keeps its behavior inspectable, but it limits throughput, batching, and scalability for large or highly concurrent worker pools.
- Context: the first implementation is a semantic baseline for demand-driven delivery, not a target storage or scheduling architecture. Due-delivery polling now uses a bounded deadline index, so it no longer scans every active delivery to find expired leases. Candidate selection and the one-outstanding-delivery-per-member limit remain; the stable-placement design records a possible future virtual-lane direction without changing the default scheduler.
- Retirement condition: workload benchmarks justify bounded scheduling, batching, or stable internal placement improvements while preserving scoped ordering, backpressure, and stale-delivery fencing.

## TD-017: Dead-letter movement spans separate durable records

- Status: open; the first local identity/reconciliation slice is implemented, but the runtime retirement gates remain open
- Impact: new local moves use a stable source-stream/source-consumer/source-offset identity and do not append a second target record when a completed target append is retried or recovered. Legacy dead-letter records written before this identity was introduced remain opaque and cannot be retroactively reconciled by this slice.
- Context: local dead-letter movement still uses separate append-only stream logs and consumer checkpoints, but the target append now uses the existing durable request-aware frame/index with an internal move identity and strict key/payload matching. The source acknowledgement remains after the durable target append or same-content reconciliation. Deterministic retry and broker reopen tests cover the identity path; no fault-injection seam exists in the local core, and the clustered path remains unchanged because it already keeps the derived record and source progress in one replicated data-group transition.
- Retirement condition: a durable transaction or recovery reconciliation protocol provides duplicate-safe dead-letter movement while preserving bounded recovery and at-least-once guarantees. The remaining evidence requires fault-injected proof at target-write and source-event sync boundaries, ambiguous-I/O eligibility, corruption and retention behavior, and a real-process restart through the public protocol. The item remains open until those runtime gates and legacy-record handling are implemented and verified.

## TD-018: Retry policy and dead-letter provenance are coarse

- Status: open
- Impact: retry configuration is broker-wide, backoff is limited to the acknowledgement timeout, and dead-letter records preserve only the original key and payload. Applications cannot yet select policy per consumer or reliably identify the source offset and attempt history from the dead-letter record alone.
- Context: the initial policy establishes durable attempt counting and usable local and clustered grouped-delivery dead-letter streams before the public consumer configuration model is finalized.
- Retirement condition: consumer-scoped policy, documented backoff and redrive behavior, and durable dead-letter provenance are available and covered by restart, retry, and clustered ownership tests.

## TD-019: Delivery bookkeeping synchronizes durable state per delivery

- Status: open
- Impact: the current durable attempt and acknowledgement bookkeeping now appends compact delivery and acknowledgement events and periodically compacts them into the checkpoint instead of replacing the full consumer state file on every delivery. Each event still crosses a synchronous durability boundary, so write and sync cost remains a throughput and tail-latency limit as concurrency and membership grow.
- Context: persisting the attempt before returning a message makes retry behavior survive restart and keeps the first correctness model straightforward. The bounded append-only journal, replay, partial-tail recovery, compaction, active-state cache, stale-token fencing, and out-of-order acknowledgement coverage preserve those guarantees and produced an observed roughly 8% improvement in the focused benchmark; the durable write per delivery remains.
- Retirement condition: benchmarked batching, group commit, or another bounded bookkeeping strategy reduces delivery overhead without losing durable retry state, acknowledgement safety, or predictable recovery.

## TD-020: Clustered delivery leases use absolute wall-clock deadlines

- Status: open
- Impact: a leader chooses an absolute expiry timestamp and replicates it with each delivery. Clock jumps or skew between successive leaders can make redelivery materially earlier or later than the configured acknowledgement timeout; delivery tokens prevent a stale acknowledgement from committing, but they do not make lease timing predictable.
- Context: replicated absolute deadlines establish a simple failover baseline without adding a separate lease service. Deterministic tests now cover forward jumps, fixed leader offsets, deadline boundaries, restart and snapshot recovery, leader changes, no-command expiry, stale-token fencing, and the regression floor for a successor clock moving backward. The state machine persists a per-group maximum command-time observation and evaluates expiry against that floor, so a backward wall-clock step cannot delay expiry after recovery or a leader change. Inter-node clock offsets, delayed evaluation, and no-leader intervals remain outside the invariant; a full monotonic-timer/reclaim or bounded-clock policy is still unresolved. Retry configuration and dead-letter provenance are tracked independently in TD-018, while retained delivery-state growth is covered by TD-010.
- Retirement condition: the supported clock assumptions and maximum timing error are documented and tested under leader changes, clock skew, and clock jumps, or delivery eligibility moves to a mechanism that does not depend on comparable node wall clocks while preserving stale-delivery fencing and at-least-once behavior.

## TD-022: Local durable I/O has bounded async isolation but incomplete evidence

- Status: open
- Impact: local durable filesystem work is now dispatched through a bounded `StorageExecutor` on Tokio's blocking pool, and same-stream operations are queued behind a weakly tracked per-stream lane so they cannot consume all global storage slots. Storage pressure can still reject new work or inflate tail latency, and the remaining resource behavior needs dedicated slow-I/O evidence.
- Context: the first async boundary bound storage to 32 active and 32 queued operations with explicit `WouldBlock` admission, then centralized the boundary and added per-stream ordering and isolation. Focused tests cover ordering, cancellation, bounded admission, unrelated-stream progress, and storage-stall behavior, including bounded readiness failure and a scrapeable metrics fallback that omits unavailable engine samples; dedicated measured slow-I/O, shutdown, and tail-latency evidence remains incomplete.
- Retirement condition: measured storage execution boundaries keep filesystem stalls from starving unrelated network, health, and shutdown work while preserving per-stream ordering, durable acknowledgement semantics, bounded queues, and useful backpressure.

## TD-023: External protocol admission remains incomplete

- Status: open
- Impact: the server now bounds connection count, request frame size, in-flight request work, and request duration, with explicit rejection responses and metrics. Real-process coverage now exercises connection floods, oversized requests, in-flight saturation, slow writers, incomplete slow readers, sustained in-flight recovery, and scrapeable metrics during a stalled storage health dependency. Sustained storage pressure and the full sustained resource-pressure matrix remain incomplete.
- Context: the development protocol favors a minimal persistent-connection loop. The admission layer now exposes configured limits and resource-specific rejection counters, uses bounded frame reads, connection/request permits, and request deadlines, and retains graceful drain behavior for already-used connections. Focused tests demonstrate that slow or malformed clients do not consume unrelated request capacity, and sustained pressure verifies active-work, rejection, saturation, timeout, and response-write-timeout metrics across recovery. FIFO storage probes now confirm that a bounded `200` metrics fallback retains process and admission samples, signals unavailable engine health, restores the full metric set after recovery, and prevents a timed-out same-stream waiter from poisoning the next durable request after release; broader storage-pressure and operational-resource evidence remains open.
- Retirement condition: the overload backlog outcome establishes configurable safe defaults and tests for connection count, request size, request duration, in-flight work, slow readers and writers, and rejection metrics without weakening graceful shutdown or normal client ergonomics.

## TD-024: Local engine responsibilities are concentrated in one module

- Status: open
- Goal: separate local broker orchestration, storage execution, stream-log encoding, delivery state, and consumer-state persistence into private modules with explicit ownership boundaries.
- Rationale: `crates/runnel-core/src/lib.rs` is a 3,669-line module containing several independently evolving domains. Changes to record formats, storage admission, delivery semantics, and consumer recovery must navigate the same file, increasing review coupling and making focused test seams harder to maintain. This is a code-organization debt distinct from TD-002's one-file stream representation and TD-022's storage-execution behavior.
- Progress: the bounded storage-dispatch slice now lives in private `crates/runnel-core/src/storage.rs`. `StorageExecutor`, per-stream lanes, lane permits/waiters, admission and execution permits, and panic/cancellation mapping have a dependency direction from the storage module to engine outcomes and the existing name validator; focused admission, FIFO ordering, cancellation, and runtime-isolation tests are attached to that module. Consumer checkpoint state, event replay, journal compaction, and checkpoint persistence now live in private `crates/runnel-core/src/consumer_state.rs`, which depends only on engine outcomes and its own filesystem/serialization concerns; focused state-transition tests remain with that boundary. Stream-log append/recovery orchestration, `RNL1`/`RNL2`/`RNL3` record codecs, checksums, request-aware deduplication, and bounded tail/sparse indexes now live in private `crates/runnel-core/src/stream_log.rs`, which depends on the durable-format choice and engine message/outcome types while receiving only generic delivery-index views for candidate selection; focused index coverage remains with that boundary. Per-stream consumer caches, in-flight deliveries, expiry/member/key indexes, and delivery-token generation now live in private `crates/runnel-core/src/delivery_state.rs`; focused index-expiry coverage is attached to that boundary. Broker orchestration remains in `lib.rs`, so this debt stays open.
- Constraints: preserve the `runnel-engine::Engine` contract and all durability, acknowledgement, redelivery, and recovery behavior; keep local file I/O and consumer-state persistence inside `runnel-core`; do not expose storage layout or introduce abstractions without a second concrete use.
- Retirement condition: private modules have clear dependency direction for broker state, storage execution, stream-log codecs, delivery state, and consumer persistence; focused tests remain attached to each boundary; `just verify` and the real-process recovery tests pass without semantic or public-API changes.

## TD-025: Clustered adapter responsibilities are concentrated in one module

- Status: open
- Goal: establish private module boundaries for clustered command/state models, durable state-machine persistence, delivery transitions, group lifecycle, client forwarding, and engine construction.
- Rationale: `crates/runnel-raft/src/lib.rs` combines persisted representations and snapshot/journal recovery with grouped delivery, Raft group management, forwarding, and the public engine adapter. These concerns evolve at different rates and make recovery or consensus changes unnecessarily coupled to delivery and lifecycle code. This is a structural debt distinct from TD-008's early static-cluster architecture and TD-009/TD-010's retained-state representation.
- Constraints: keep consensus-specific behavior behind the distributed-engine adapter; preserve persisted formats, Raft trait behavior, public engine semantics, fencing, and recovery guarantees; do not use extraction to hide or weaken topology, durability, or failure-policy decisions.
- Progress: state-machine journal framing, replay ordering, persisted representations, durable store lifecycle, snapshot persistence, and OpenRaft state-machine traits now have private ownership in `crates/runnel-raft/src/state_machine_journal.rs` and `crates/runnel-raft/src/state_machine_store.rs` with focused recovery and validation coverage. Group manifests, Raft group construction, group lookup and restoration, data-group preparation, stream activation reconciliation, clustered-layout validation, and manager-scoped group access now have private ownership in `crates/runnel-raft/src/group_manager.rs`; existing lifecycle and recovery tests continue through the same public engine behavior. Materialized state, delivery transitions, client forwarding, and engine adaptation remain in `lib.rs`, so this debt stays open.
- Retirement condition: state-machine persistence, delivery transitions, group management, forwarding, and engine adaptation have explicit private module ownership; focused unit and cluster recovery tests cover the boundaries; `just verify` and `just cluster-test` pass with unchanged public behavior and storage compatibility.

## TD-027: Server entrypoint combines lifecycle, protocol, admission, and observability

- Status: open
- Goal: separate server bootstrap and shutdown orchestration, TCP framing and admission, request dispatch and response mapping, and HTTP health/metrics handling into private modules.
- Rationale: `crates/runnel-server/src/main.rs` is a 1,845-line entrypoint that owns CLI configuration, engine startup, listener lifecycle, persistent-connection framing, admission limits, request execution, response serialization, health, metrics, and shutdown. These responsibilities have different failure and test boundaries, so continued feature work will make the entrypoint harder to review and change safely. This is a structural debt distinct from TD-023's admission behavior.
- Progress: HTTP router state, liveness/readiness handlers, bounded health checks, metric state, active-resource guards, delivery accounting, and Prometheus formatting have private ownership in `crates/runnel-server/src/observability.rs`. TCP admission validation, bounded framing, request-line/deadline helpers, response serialization, connection rejection, and stable admission responses have private ownership in `crates/runnel-server/src/protocol.rs`. Request dispatch and response/error mapping have private ownership in `crates/runnel-server/src/dispatch.rs`. TCP listener admission, persistent connection handling, and connection-task joining now have private ownership in `crates/runnel-server/src/connection.rs`; shutdown signaling, peer/HTTP task supervision, graceful drain, and bounded shutdown escalation now have private ownership in `crates/runnel-server/src/lifecycle.rs`. `main.rs` retains CLI, engine, and listener bootstrap and delegates runtime ownership to the private modules; the broader item stays open for any remaining bootstrap/configuration boundary work.
- Constraints: keep transport concerns in `runnel-server`; preserve the provisional protocol, explicit outcome mapping, bounded-resource behavior, graceful shutdown, and real-process network tests; do not broaden the public protocol while extracting modules.
- Retirement condition: bootstrap/lifecycle, TCP protocol/admission, request mapping, and HTTP observability have clear private module ownership; network, health, metrics, and shutdown tests remain green under `just verify` and `just integration`.

## TD-028: Cluster benchmark scenarios and runtime are concentrated in one script

- Status: open
- Goal: separate clustered process/container lifecycle, resource observation, workload scenarios, fault/recovery operations, result metadata, and CLI dispatch into focused benchmark modules.
- Rationale: `scripts/benchmarks/cluster.py` is a 2,622-line script containing peer proxies, node lifecycle, process sampling, publish/consume/batch workloads, hot-ordering observations, failure probes, result shaping, argument parsing, and command dispatch. Adding a scenario currently expands an already coupled harness and makes cleanup, resource isolation, and semantic evidence harder to audit. The same boundary should guide the adjacent `test_cluster.py` tests without forcing a speculative framework rewrite.
- Progress: native/container resource reads and scenario sampling now live in `scripts/benchmarks/cluster_resources.py`, with the existing process/container probes, throttled storage scans, aggregate fields, per-node samples, and interval summaries preserved. The run-scoped framed peer-response delay fault injector now lives in `scripts/benchmarks/cluster_faults.py`, with its lifecycle, bounded frame handling, delay behavior, and diagnostic counters preserved. Workload scenario implementations, hot-ordering observations, shared-consumer helpers, and restart/failure-recovery orchestration now live in the private `scripts/benchmarks/cluster_scenarios.py`; the existing result fields, bounded timeouts, public protocol checks, and setup exclusions remain attached to those scenarios. `cluster.py` retains cluster lifecycle, resource/fault wiring, CLI validation/dispatch, and the top-level machine-readable result envelope, so this item stays open.
- Constraints: preserve command names and defaults, machine-readable result fields, resource limits, unique-resource cleanup, scenario semantics, and the distinction between diagnostic and authoritative benchmark evidence; update benchmark documentation if an interface changes.
- Retirement condition: lifecycle/resource primitives, scenario implementations, observations, and CLI dispatch have focused ownership and tests; existing benchmark smoke workflows and `just bench-test` pass with equivalent outputs and cleanup guarantees.

## TD-029: Competitor comparison harness mixes adapters, orchestration, and policy validation

- Status: open
- Goal: isolate backend-specific service and output adapters from comparison lifecycle, semantic guardrails, result validation, and CLI orchestration.
- Rationale: `scripts/benchmarks/compare.py` is a 1,563-line script combining Docker service lifecycle, Kafka/Redpanda/JetStream command construction and parsing, Runnel execution, equivalence metadata validation, result aggregation, and CLI dispatch. Backend-specific changes can therefore affect common comparison policy, while the harness remains difficult to extend with a new adapter or failure mode. This is a structural debt distinct from TD-013's unresolved semantic equivalence.
- Progress: native-tool images, command construction, readiness-output validation, and Kafka/NATS output parsing now live in `scripts/benchmarks/compare_adapters.py`; shared service lifecycle, measurement, policy validation, result handling, and CLI dispatch remain in `compare.py`, so this item stays open.
- Constraints: preserve operation-specific durability and acknowledgement declarations, experimental/non-ranking guardrails, result schemas, bounded cleanup, and documented comparison commands; do not make comparisons appear equivalent through extraction alone.
- Retirement condition: backend adapters, service lifecycle, policy validation, result handling, and CLI dispatch have separate focused modules and tests; comparison smoke tests pass and continue to reject mismatched semantics.
