# Multi-Raft implementation plan

- Status: accepted direction; staged implementation plan
- Last reviewed: 2026-09-06
- Baseline: `origin/main` `16b3c63cd4478eb49b960a70565a718db8d235ac`
- Scope: the smallest credible three-node implementation, designed to leave room for alternative distributed engines

ADR 0004 accepts the distributed-engine direction and its initial defaults. ADRs 0006, 0007, 0015, 0016, 0018, 0019, 0023, 0024, and 0026 refine the current lifecycle, recovery, delivery, placement, replay, identity, and engine-outcome boundaries. This document records the staged implementation plan and the evidence gates that still apply before production clustering is enabled.

## Current implementation status

At this baseline, the repository has a metadata Raft group, one group-addressed data Raft runtime per stream, reconciled `Creating` to `Active` stream creation, framed TCP peer transport, `--engine raft` server selection, hostname-capable peer addresses, durable publish request deduplication, replicated shared-consumer ownership and retry/dead-letter outcomes, automatic consensus-log snapshots and purge, bounded snapshot chunks, lazy group materialization from committed metadata, clustered storage identity validation, snapshot lifecycle metrics, real three-process failure tests, a separate test-only interrupted-transfer retry experiment, and a three-replica Kubernetes development manifest. The shared engine boundary now includes bounded per-record publish batches, explicit offset replay, health, and stable error kind/outcome classification; the clustered engine uses the default per-record batch implementation and does not promise batch atomicity. The three-node process test is the current development profile, but the runtime accepts any non-empty configured peer set; three voters and one-failure quorum durability are not enforced by configuration validation. The current clustered state machine still owns retained messages in memory and persists a JSON write-ahead journal plus JSON checkpoint and complete-snapshot files. The peer protocol is length-bounded framed JSON without runtime version negotiation. Repeated interruption cost, broader observability, dynamic membership, placement, replica/replacement fencing, and the later stages remain necessary before treating clustered operation as production-ready. The [TD-008 static-cluster evidence](td-008-static-cluster-evidence.md), [TD-009 snapshot evidence](td-009-snapshot-evidence.md), and [TD-010 retained-state evidence](td-010-retained-state-evidence.md) record the corresponding limits.

The current implementation evidence is intentionally narrower than the target invariants and stage exit criteria below. The persistent engine validates clustered storage identity and layout before opening groups, while `GroupManager` opens metadata and materializes data groups from committed stream metadata. The process-level cluster path covers publish, replay, follower forwarding, grouped delivery, node failure, restart, and the test-only empty replacement experiment; it does not establish a production replacement, rolling-upgrade, or dynamic-membership contract. The current grouped path also replicates ownership, attempts, lease deadlines, and delivery tokens, while member registration and final clock/fencing policy remain unresolved. See the [persistent engine startup path](../../crates/runnel-raft/src/engine.rs), [group manager](../../crates/runnel-raft/src/group_manager.rs), [state-machine store](../../crates/runnel-raft/src/state_machine_store.rs), and [cluster smoke tests](../../crates/runnel-server/tests/cluster_smoke.rs).

The configured three-node topology is a deployment convention rather than an engine invariant: `PersistentEngine` and the server parser do not reject one, two, or more configured peers. `InMemoryCluster` likewise supports arbitrary non-empty node sets for focused tests. Any future claim about tolerated node failures must therefore name the configured membership and quorum rather than relying on the plan's three-node wording.

## Recommendation

Build Multi-Raft as Runnel's first distributed engine. Start with three statically configured nodes, one replicated metadata group, and one replicated data group per stream. Let clients contact any node; the receiving node resolves or forwards the operation without exposing group or leader topology.

Keep the existing local engine. Introduce a narrow broker-engine contract expressed in messaging outcomes such as publish, poll, acknowledge, stream lookup, and durability status. Do not make Raft, leaders, quorums, terms, or physical groups part of that contract. A later sequenced-quorum, copyset, chain-replication, or partially ordered engine should be able to implement the same semantics without pretending to be Raft.

The first implementation should be deliberately small:

- one cluster-wide engine selection at process startup;
- three known nodes and one-failure quorum durability in the initial development profile;
- a modest number of streams and groups;
- static replica placement on all three nodes;
- leader-routed reads and writes;
- no automatic balancing, virtual-shard splitting, dynamic membership, or mixed engines;
- no Kubernetes dependency in discovery, elections, fencing, or recovery.

This is the right first direction because it gives Runnel an understandable correctness baseline for replication, fencing, failover, and recovery. It also creates a real comparison target for the more specialized endgame designs. Trying to build the sequenced-quorum design first would combine too many new protocols—sequencer epochs, holes, committed watermarks, copyset placement, repair, and reconfiguration—before the product has a tested distributed semantic contract.

## Invariants to establish before code

The first implementation must make these properties executable in tests:

- a publish reported as durably committed has been persisted by a quorum and applied to durable broker state;
- consumers observe only committed records;
- records in one stream group receive one deterministic order, while different streams can progress independently;
- a stale leader cannot commit after losing authority;
- durable consumer progress survives the promised single-node failure and restart;
- applying a committed command more than once is harmless;
- producer retries have stable request identity, allowing duplicates and unknown outcomes to be resolved;
- success, rejection, retryable failure, and unknown outcome remain distinct at the public boundary;
- queues, outstanding requests, batches, and replication work are bounded;
- retained broker history is not deleted merely because the Raft consensus log is compacted;
- cluster identity, node identity, group identity, storage format version, and protocol version survive restart and cannot be accidentally reused for a different cluster.

## Rust library assessment

### Consensus: use OpenRaft 0.9

The leading candidates are OpenRaft and TiKV's `raft` crate.

OpenRaft is the better first fit. It supplies the asynchronous Raft runtime, replication tasks, membership changes, snapshots, linearizable-read support, metrics, and explicit storage and network interfaces. Runnel still owns transport, durable storage, state-machine semantics, process supervision, and testing, but it does not need to reproduce the full `RawNode` drive loop. The repository pins OpenRaft 0.9.25 exactly; keep all OpenRaft types inside a dedicated adapter crate because its pre-1.0 API and stored types may change. Do not treat the pin as evidence that rolling compatibility or stored-format compatibility has been established.

TiKV's `raft` crate is a strong production-proven consensus core and remains the fallback if OpenRaft's runtime model becomes a measured limitation. It intentionally supplies only the consensus module: Runnel would have to implement and correctly order ticking, `Ready` processing, stable log writes, state-machine application, outbound messages, snapshots, and advancement. That control may eventually suit a thread-per-core runtime, but it creates substantially more integration and proof work for the first cluster. Its Prost build also expects `protoc` in the development environment unless the dependency is wrapped with a vendored toolchain, which conflicts with the preference for minimal setup.

Do not implement Raft from scratch or upgrade to a newer OpenRaft line without a fresh compatibility and recovery evaluation. Keep a small library-evaluation test fixture so the decision can be revisited against a representative Runnel batch and storage adapter instead of synthetic consensus-only results.

Relevant primary sources:

- [OpenRaft project status and features](https://github.com/databendlabs/openraft)
- [OpenRaft storage interfaces](https://docs.rs/openraft/0.9.25/openraft/storage/)
- [OpenRaft integration guide](https://docs.rs/openraft/0.9.25/openraft/docs/getting_started/)
- [TiKV raft-rs integration boundary](https://github.com/tikv/raft-rs)
- [TiKV `Ready` contract](https://docs.rs/raft/0.7.0/raft/raw_node/struct.Ready.html)

### Rust toolchain policy

Runnel is currently distributed as a broker binary or container and does not promise source-build compatibility with an older compiler. Development, CI, and container builds use the pinned Rust 1.97.1 toolchain.

The earlier OpenRaft compatibility probe and the Rust 1.88 decision are historical context. They no longer impose a formal MSRV or require a separate compatibility job. Revisit this policy if Runnel begins publishing libraries or supporting downstream source builds.

### Initial durable store: evaluate redb, keep payload storage replaceable

The Raft log and the broker's retained message log have different lifecycles. Raft entries may be purged after a state-machine snapshot; records must remain replayable until Runnel's retention policy permits deletion. They must not be the same conceptual log.

For the first clustered engine, evaluate `redb` as the default durable adapter. It is pure Rust, stable and maintained, crash-safe by default, and provides ACID transactions with a single writer and concurrent readers. A transaction can atomically materialize an applied record or consumer checkpoint together with the group's last-applied command identity. This makes crash replay idempotent and avoids a bespoke transactional format in the first distributed milestone.

Use it as an implementation substrate, not as Runnel's permanent data architecture. Place consensus entries, materialized stream state, deduplication state, and applied metadata behind Runnel-owned storage interfaces and versioned encodings. Do not expose redb keys or transactions above the storage adapter. Before accepting it, compare a representative group-commit workload with `fjall` and the existing append-only log. Fjall is a credible pure-Rust LSM alternative with bounded block-cache configuration and explicit durability modes, but its durability must be deliberately configured and its newer design increases the initial validation burden.

The evaluation must measure durable batched append, point lookup, sequential replay, deletion/retention, restart, disk growth, and p99 fsync latency. RocksDB remains a fallback baseline, not the default: it is mature and fast but adds a C++ dependency, heavier builds, and durability settings that are easy to misconfigure.

Relevant primary sources:

- [redb status, transaction model, and crash guarantees](https://github.com/cberner/redb)
- [Fjall architecture and durability controls](https://github.com/fjall-rs/fjall)
- [OpenRaft guidance on logs, state machines, and snapshots](https://docs.rs/openraft/0.9.25/openraft/docs/getting_started/)

## Proposed component boundaries

The exact crate names can be adjusted during the first structural change, but ownership should be clear:

| Boundary | Responsibility |
|---|---|
| Broker semantics | Stable stream, publish, consume, acknowledge, replay, error, and durability outcomes shared by local and distributed engines |
| Local engine | Existing single-node behavior adapted to the common semantic contract |
| Raft engine | Group lifecycle, command proposal, committed application, snapshots, leader resolution, and mapping Raft outcomes to broker outcomes |
| Cluster metadata | Stable stream IDs, group IDs, replica assignments, lifecycle state, cluster identity, node descriptors, and format/protocol versions |
| Group manager | Hosts many group runtimes in one process, dispatches internal messages by group ID, and bounds per-group work |
| Internal transport | Length-bounded framed peer protocol and bounded connection ownership per peer, carrying group-addressed Raft and forwarding traffic; protocol version negotiation remains future work |
| Durable storage | Runnel-owned encodings and atomic apply contract over an interchangeable local storage implementation |
| Public protocol | Topology-free client requests and explicit success, rejected, retryable, and unknown outcomes; physical redirects remain hidden |

Only create an abstraction when the local and Raft implementations give it two real users. The first step should extract the smallest contract needed by both, not design a universal plugin framework. Engine selection should initially occur once at startup. Hot swapping, per-stream engine choice, and mixed-engine clusters require migration and compatibility protocols and remain future work.

## Initial topology and lifecycle

### Identities and configuration

Each process has a persisted cluster identity (currently the configured `cluster_name`) and `NodeId`, an external client address, an internal peer address, and a data directory. Development configuration lists the same three node descriptors on every process. Node identity must come from persisted state plus explicit configuration, never from pod ordinal alone.

Reserve a well-known group ID for the metadata group. Every stream receives an opaque stable `StreamId` and `GroupId`. In the initial topology, every data group has the same three voters, but placement is still represented explicitly so future copysets or larger clusters do not require a format rewrite.

Bootstrap must be idempotent. Exactly one configured bootstrap action initializes the metadata group; restarted or duplicate initialization attempts verify the persisted cluster identity rather than replacing state.

### Stream creation

Stream creation is a reconciled state transition, not an assumed atomic operation across two Raft groups:

1. the metadata leader records a stable stream and group identity in `Creating` state;
2. every assigned node idempotently creates or opens the data-group storage and runtime;
3. the data group establishes its initial membership;
4. the metadata leader marks the stream `Active` after the group is usable;
5. retries resume from the persisted state after any crash.

Normal publishes reject or retry while a stream is not active. This state model later extends to moving, splitting, migrating, and deleting without changing stream identity.

### Request routing

Clients may connect to any node. The receiving node looks up the stream in its locally applied metadata and data-group state, then forwards internally when needed. The current implementation obtains the leader view per operation and uses bounded forwarding attempts across configured peers; it has no separate leader cache, hop metadata, or recursive peer forwarding. A stale leader view is retried through that bounded candidate loop. The public client never receives a physical group or node assignment as application state.

Writes complete only after OpenRaft reports the command committed and durably applied by the leader's state machine. Poll, replay, and acknowledgement paths currently use committed data-group commands; metadata lookup used to resolve a stream is a local applied-state read and has no separate read-index proof. Follower reads are deferred until their staleness and fencing semantics are explicitly defined.

### State-machine commands

Commands must contain all nondeterministic choices made by the leader. Followers must not generate timestamps, offsets, retry deadlines, or identities from their local clock or random source while applying a command.

The initial data-group command family now covers:

- publish a record with stable producer/request identity, payload, optional ordering key, assigned logical position, and leader-selected timestamp;
- read a committed record for ordinary poll or explicit offset replay without exposing uncommitted state;
- advance a durable consumer checkpoint after validating the acknowledgement against the delivered record;
- assign shared-consumer ownership with a leader-selected lease deadline and token, record attempts, and apply stale-delivery fencing;
- commit source progress and the derived dead-letter record together when the configured attempt limit is reached; and
- create any stream-local durable metadata required for those operations.

Application produces a stable request ID before retrying. The current clustered state stores a durable map from stream/request identity to the committed offset; the map is not bounded by the implementation, and conflicting payload or key reuse is not currently reported as a distinct outcome. If the client loses its connection after submission and cannot prove whether commit occurred, the response is `unknown`; retrying the same identity resolves to the original offset when the identity is retained.

The initial clustered implementation replicates in-flight delivery ownership, attempts, absolute lease deadlines, and fencing tokens alongside the durable checkpoint. This is a failover baseline, not a final clock or lease policy: the configured timeout must be consistent across nodes, the lease-clock floor only advances when commands observe time, and there is no durable member registry or incremental rebalance. Consumer identity and state must not be encoded as local file ownership.

## Storage, snapshots, and recovery

Maintain three explicit layers:

1. the compactable consensus log used to establish command order;
2. the durable materialized broker state used for retention, replay, consumer progress, and deduplication;
3. a versioned snapshot that lets a replacement replica reconstruct the state machine independently of purged consensus entries.

Applying a committed command first appends a Runnel-owned journal record and syncs it before moving the command into the in-memory materialized state. On reopen, journal replay skips entries at or before the recovered applied-log boundary and reconstructs the state from the durable record; stable publish request IDs provide application-level duplicate suppression, but this is not a guarantee that an arbitrary command is safe to apply twice. A publish is not acknowledged until that durable apply has completed locally after quorum commit. The current implementation provides this write-ahead/replay boundary with JSON journal, checkpoint, and snapshot files; it is not an atomic embedded-database transaction and has not introduced the proposed interchangeable transactional storage adapter.

The first snapshot may be a consistent snapshot of the embedded state store. The interface should describe a snapshot manifest and byte stream, not a database-specific file path. A future extent engine can snapshot metadata and immutable extent manifests while transferring payload extents separately. Snapshot creation must coexist safely with continued applies, and installation must use a staged, validated, atomic cutover.

Do not implement retention until the replicated apply and snapshot model is stable, but keep record position, logical stream identity, and data encoding independent of Raft log indices. They may coincide in an early fixture, but code and stored metadata must not depend on that coincidence.

## Staged implementation

Each stage should leave the repository runnable and verified. Stop at a stage if its correctness evidence is incomplete.

### Stage 0: accept the design and dependency baseline (complete for the current slice)

- Review this plan and resolve the open decisions below.
- Write an ADR for the first distributed engine, topology, durability acknowledgement point, library choice, engine boundary, and explicitly deferred behavior.
- Keep dependency-license, advisory, and exact-version checks in the normal verification and security workflows before treating the library as a production dependency.

Exit evidence: accepted ADR, clean pinned-toolchain build of the selected libraries, and no change to current broker behavior.

### Stage 1: establish the semantic engine seam (implemented; broader outcome work remains)

- The current seam defines topology-free broker command, query, outcome, durability, and error types from current behavior.
- The existing local implementation is adapted as the first engine without changing its public behavior.
- Shared conformance cases now cover publish, independent and shared poll, acknowledge, redelivery, replay, publish batches, restart, key ordering, and error classification.
- Keep stable producer/request identity in the provisional protocol before clustered retries depend on it; its retention and conflict policy remain open.

Exit evidence: the local broker and the persistent clustered engine pass the applicable shared contract assertions, the batch contract remains explicitly non-atomic, and no Raft type crosses the engine boundary. Stage 1 does not imply that the public wire protocol exposes stage-aware outcomes.

### Stage 2: prove one durable replicated stream (implemented as a development slice; evidence gaps remain)

- The development slice uses an OpenRaft adapter, length-bounded framed internal transport, group manager, and versioned Runnel-owned persistence boundary; an interchangeable transactional storage adapter remains future work.
- Run one statically identified data group across three independent local processes.
- Support durable publish, leader-routed poll, and durable acknowledgement for that stream.
- Make forwarding, duplicate requests, retryable failures, and unknown outcomes explicit; protocol-level outcome and stage reporting remains future work.
- Add process-kill and restart tests at proposal, persistence, commit, apply, and response boundaries.

Exit evidence: a three-process test demonstrates quorum commit, leader loss, redelivery, duplicate suppression, restart, and no observation of uncommitted records.

This intentionally fixed-stream milestone is a test fixture, not the public clustered product.

### Stage 3: add replicated metadata and stream lifecycle

The initial slice of this stage is implemented. Metadata and data groups are separate, stream creation records `Creating`, prepares the data group on the configured nodes, initializes its durable state, and then records `Active`. Group-addressed peer RPCs and restart restoration keep per-stream runtimes independent. Shared-consumer ownership and retry/dead-letter outcomes are now replicated in the data group; the remaining work is stronger reconciliation and failure-boundary coverage, including process failure at each creation boundary and cases where configured membership or future placement changes during creation. Durable member registration and placement-aware ownership remain later work.

- Add the metadata group and stable cluster, node, stream, and group identities.
- Implement idempotent bootstrap and reconciled `Creating` to `Active` stream lifecycle.
- Route create, publish, poll, and acknowledge through committed metadata from any node.
- Recover partially completed creation after each process-failure boundary.

Exit evidence: streams can be created through the existing public intent on any node, survive full-cluster restart, and resume or reject partial creation deterministically.

### Stage 4: make the three-node development cluster operable (partially implemented)

- Add cluster health, per-group leadership and replication lag metrics, quorum/readiness semantics, storage pressure, and forwarding metrics. Aggregate health and snapshot lifecycle metrics exist; per-group leadership/lag, storage pressure, and forwarding-specific metrics remain incomplete.
- Add bounded admission, queue, batch, connection, and timeout configuration with strong defaults. Public request connections, frames, in-flight work, and request duration are bounded; per-group Raft queues, batching policy, and internal resource budgets remain incomplete.
- Add graceful shutdown that stops admission, drains only within a deadline, transfers leadership when practical, and never weakens acknowledged durability. The server now stops new work, cancels frame reads, and drains listener tasks under a fixed deadline; leadership transfer and cluster-wide drain semantics are not implemented.
- Provide one local `just` workflow that starts three real processes and drives the CLI through creation, publish, consume, acknowledgement, failover, and restart.
- Add a three-replica Kubernetes development manifest with independent persistent volumes and broker-owned bootstrap semantics.

Exit evidence: the process-level cluster smoke test and the documented Kubernetes development scenario satisfy the three-node backlog acceptance criteria.

### Stage 5: establish performance and failure baselines

- Benchmark local versus Multi-Raft with 100-byte and 1-KiB records, single and batched publishing, producer-to-consumer latency, sustained load, slow consumers, restart, and recovery.
- Record p50, p99, p99.9, throughput per core, allocation/memory bounds, disk amplification, and the exact durability guarantee.
- Add deterministic network-fault tests and repeated process-level partition, pause, disk-full, slow-disk, and corruption tests.
- Use the evidence to decide whether storage, transport, batching, scheduling, or group density needs redesign before dynamic placement.

Exit evidence: reproducible benchmark reports and failure tests establish a trustworthy baseline for comparing the endgame engines.

## Verification strategy

Use several complementary layers:

- pure state-machine tests for determinism, idempotent apply, deduplication, acknowledgement validation, and snapshots;
- engine conformance tests shared by local and Raft engines;
- OpenRaft storage-contract tests and restart tests for every persisted transition;
- deterministic simulated-network tests for elections, partitions, delayed messages, duplicate messages, and stale leaders (not yet present; the current in-memory cluster exercises OpenRaft with an in-memory transport but does not provide fault injection);
- real three-process tests using temporary directories and dynamically allocated ports;
- fault-injection tests that kill or interrupt a process after durable write, before response, during snapshot, and during stream creation (the current suite covers selected leader/follower process failures, preserved-state restart, and test-only snapshot-transfer interruption, but not response-loss or every persistence boundary);
- model or property tests for monotonic committed positions, no conflicting committed leaders, deduplication, and checkpoint monotonicity (not yet present as a model/property suite);
- local cluster smoke tests driven through `runnelctl`, not through internal test hooks;
- benchmark profiles that state hardware, topology, storage, fsync policy, batch size, and failure state.

No Kubernetes test substitutes for process-level fault tests. Kubernetes tests verify packaging, persistent identity, readiness, disruption, and restart behavior after the broker protocol is already proven.

## Explicitly deferred

- dynamic membership and adding or removing brokers;
- automatic replica placement and leader balancing;
- more than three replicas or heterogeneous failure domains;
- hidden virtual shards within a stream;
- incremental consumer-group rebalancing;
- follower reads;
- cross-stream atomic operations;
- live engine migration, per-stream engine selection, or mixed-engine clusters;
- sequenced-quorum, copyset, chain-replication, and object-storage data paths;
- a custom thread-per-core runtime or custom durable extent format.

These remain design constraints. The initial identities, lifecycle states, semantic engine boundary, versioned encodings, snapshot manifests, and placement representation are intended to support them without implementing them now.

## Risks and controls

| Risk | Initial control |
|---|---|
| A consensus library bug or breaking API change | Pin a reviewed 0.9 patch, isolate it, run fault tests, and keep Runnel-owned stored encodings |
| Raft log incorrectly becomes retained message history | Separate consensus, materialized state, and snapshot contracts and test retention-independent compaction |
| Crash between commit and broker-state materialization | Durable state-machine write-ahead journal, idempotent command identity, and restart replay tests; replace with a transactional applied-state adapter only after the storage evidence gate |
| Partial stream creation leaks unusable groups | Persist lifecycle state and reconcile idempotently |
| Forwarding creates loops or duplicate writes | Origin-side bounded forwarding attempts, stable request IDs, and durable deduplication; recursive peer forwarding is not part of the current path |
| One process accumulates too many tasks and timers | Start with few groups; measure idle memory, task count, timers, file descriptors, and group density |
| Embedded-store behavior dominates tail latency | Benchmark representative durable batches early and preserve the storage adapter boundary |
| Snapshot transfer blocks the hot path | Concurrent snapshot contract, bounded transfer, metrics, and slow-snapshot fault tests |
| Kubernetes identity or control plane becomes part of correctness | Persist broker identities and use static broker-owned membership in the first cluster |
| The engine abstraction becomes a lowest-common-denominator plugin API | Keep it semantic, cluster-wide, and limited to two concrete implementations until migration is designed |

## Accepted direction and provisional implementation defaults

ADR 0004 accepts the following defaults:

1. OpenRaft 0.9.25, exactly pinned and isolated; TiKV `raft` remains the fallback.
2. Use the pinned development toolchain without publishing a formal source-build compatibility floor.
3. Three static voters in the initial development profile, one metadata group, and one data group per stream, with all first-version groups replicated to all three configured nodes. The runtime does not currently enforce this membership shape.
4. Any-node client access with internal forwarding; committed leader-authorized commands for the initial read and write paths. A separate linearizable read-index contract is not yet established.
5. Publish success only after quorum commit and durable state-machine apply.
6. Replicate durable consumer checkpoints and the initial shared-consumer ownership, attempt, lease-deadline, and fencing state with stream data; keep the final clock, member-lifecycle, and fencing policy open.
7. Evaluate redb as a possible first transactional durable adapter, with a short evidence gate against Fjall and the current append log; no clustered storage engine has been accepted yet.
8. Keep local and Multi-Raft engines selectable only at process startup; defer mixed engines and live migration.

The current implementation has completed the semantic seam (including publish batches, replay, shared-delivery assertions, and engine error classification), versioned durable Raft/state-machine files, framed TCP peer transport, topology-free client forwarding, committed data-group command handling for reads and writes, durable publish request deduplication, replicated shared-consumer ownership, clustered retry and dead-letter outcomes, server engine selection, graceful listener drain, and real three-process failure tests. The remaining Stage 2 evidence includes explicit stale-participant behavior, response-loss/unknown resolution, and broader storage/transport fault coverage; Stage 3 still needs stronger lifecycle reconciliation and durable member semantics; Stage 4 still needs per-group operational signals, internal resource budgets, leadership handoff semantics, and production failure/upgrade policy. These gaps are tracked separately in the [clustered outcome contract](clustered-outcome-contract.md), [durability and delivery policy](durability-delivery-policy.md), [recovery research](../research/raft-recovery-and-replacement.md), and [static-cluster evidence](td-008-static-cluster-evidence.md).
