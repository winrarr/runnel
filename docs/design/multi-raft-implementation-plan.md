# Multi-Raft implementation plan

- Status: accepted direction; staged implementation plan
- Last reviewed: 2026-08-19
- Scope: the smallest credible three-node implementation, designed to leave room for alternative distributed engines

ADR 0004 accepts the distributed-engine direction and its initial defaults. This document records the staged implementation plan and the evidence gates that still apply before production clustering is enabled.

## Current implementation status

The repository now has a metadata Raft group, one group-addressed data Raft runtime per stream, versioned durable Raft and state-machine files, reconciled `Creating` to `Active` stream creation, framed TCP peer transport, `--engine raft` server selection, hostname-capable peer addresses, automatic consensus-log snapshots and purge, bounded snapshot chunks, lazy group materialization for replacement nodes, snapshot lifecycle metrics, a real three-process failure test including interrupted-transfer retry, and a three-replica Kubernetes development manifest. All first-version groups use the same statically configured three-node membership. Repeated interruption cost, broader observability, dynamic membership, placement, fencing, and the later stages remain necessary before treating clustered operation as production-ready.

## Recommendation

Build Multi-Raft as Runnel's first distributed engine. Start with three statically configured nodes, one replicated metadata group, and one replicated data group per stream. Let clients contact any node; the receiving node resolves or forwards the operation without exposing group or leader topology.

Keep the existing local engine. Introduce a narrow broker-engine contract expressed in messaging outcomes such as publish, poll, acknowledge, stream lookup, and durability status. Do not make Raft, leaders, quorums, terms, or physical groups part of that contract. A later sequenced-quorum, copyset, chain-replication, or partially ordered engine should be able to implement the same semantics without pretending to be Raft.

The first implementation should be deliberately small:

- one cluster-wide engine selection at process startup;
- exactly three known nodes and one-failure quorum durability;
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

OpenRaft is the better first fit. It supplies the asynchronous Raft runtime, replication tasks, membership changes, snapshots, linearizable-read support, metrics, and explicit storage and network interfaces. Runnel still owns transport, durable storage, state-machine semantics, process supervision, and testing, but it does not need to reproduce the full `RawNode` drive loop. Its 0.9 branch receives bug fixes while 0.10 remains alpha. Pin the latest reviewed 0.9 patch release exactly and keep all OpenRaft types inside a dedicated adapter crate because its pre-1.0 API and stored types may change.

TiKV's `raft` crate is a strong production-proven consensus core and remains the fallback if OpenRaft's runtime model becomes a measured limitation. It intentionally supplies only the consensus module: Runnel would have to implement and correctly order ticking, `Ready` processing, stable log writes, state-machine application, outbound messages, snapshots, and advancement. That control may eventually suit a thread-per-core runtime, but it creates substantially more integration and proof work for the first cluster. Its Prost build also expects `protoc` in the development environment unless the dependency is wrapped with a vendored toolchain, which conflicts with the preference for minimal setup.

Do not implement Raft from scratch and do not start on OpenRaft 0.10 alpha. Keep a small library-evaluation test fixture so the decision can be revisited against a representative Runnel batch and storage adapter instead of synthetic consensus-only results.

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
| Internal transport | One versioned peer protocol and shared connection pool per peer, carrying group-addressed Raft and forwarding traffic |
| Durable storage | Runnel-owned encodings and atomic apply contract over an interchangeable local storage implementation |
| Public protocol | Topology-free client requests and explicit success, rejection, retryable, redirect-hidden, and unknown outcomes |

Only create an abstraction when the local and Raft implementations give it two real users. The first step should extract the smallest contract needed by both, not design a universal plugin framework. Engine selection should initially occur once at startup. Hot swapping, per-stream engine choice, and mixed-engine clusters require migration and compatibility protocols and remain future work.

## Initial topology and lifecycle

### Identities and configuration

Each process has a persisted `ClusterId` and `NodeId`, an external client address, an internal peer address, and a data directory. Development configuration lists the same three node descriptors on every process. Node identity must come from persisted state plus explicit configuration, never from pod ordinal alone.

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

Clients may connect to any node. The receiving node looks up the stream in committed metadata, uses a bounded leader cache, and forwards internally when needed. A stale leader hint causes one bounded refresh/retry; forwarding loops are prevented with request metadata and hop limits. The public client never receives a physical group or node assignment as application state.

Writes complete only after OpenRaft reports the command committed and durably applied by the leader's state machine. Polls initially route to the leader and establish a linearizable applied point before reading. Follower reads are deferred until their staleness and fencing semantics are explicitly defined.

### State-machine commands

Commands must contain all nondeterministic choices made by the leader. Followers must not generate timestamps, offsets, retry deadlines, or identities from their local clock or random source while applying a command.

The first data-group command family should cover:

- publish a record with stable producer/request identity, payload, optional ordering key, assigned logical position, and leader-selected timestamp;
- advance a durable consumer checkpoint after validating the acknowledgement against the delivered record;
- create any stream-local durable metadata required for those operations.

Application produces a stable request ID before retrying. A bounded durable deduplication record maps producer/request identity to its committed outcome. If the client loses its connection after submission and cannot prove whether commit occurred, the response is `unknown`; retrying the same identity resolves to the original result.

In-flight delivery leases and acknowledgement deadlines may remain reconstructible volatile state in the first version. The durable checkpoint is replicated. Consumer groups, ownership epochs, and incremental rebalance are later commands, but consumer identity and state must not be encoded as local file ownership.

## Storage, snapshots, and recovery

Maintain three explicit layers:

1. the compactable consensus log used to establish command order;
2. the durable materialized broker state used for retention, replay, consumer progress, and deduplication;
3. a versioned snapshot that lets a replacement replica reconstruct the state machine independently of purged consensus entries.

Applying a committed command atomically writes its broker-state changes and last-applied log identity. Reapplication after a crash is a no-op or returns the same outcome. A publish is not acknowledged until that durable apply has completed locally after quorum commit.

The first snapshot may be a consistent snapshot of the embedded state store. The interface should describe a snapshot manifest and byte stream, not a database-specific file path. A future extent engine can snapshot metadata and immutable extent manifests while transferring payload extents separately. Snapshot creation must coexist safely with continued applies, and installation must use a staged, validated, atomic cutover.

Do not implement retention until the replicated apply and snapshot model is stable, but keep record position, logical stream identity, and data encoding independent of Raft log indices. They may coincide in an early fixture, but code and stored metadata must not depend on that coincidence.

## Staged implementation

Each stage should leave the repository runnable and verified. Stop at a stage if its correctness evidence is incomplete.

### Stage 0: accept the design and dependency baseline

- Review this plan and resolve the open decisions below.
- Write an ADR for the first distributed engine, topology, durability acknowledgement point, library choice, engine boundary, and explicitly deferred behavior.
- Add dependency-license, advisory, and exact-version checks before production code depends on the library.

Exit evidence: accepted ADR, clean pinned-toolchain build of the selected libraries, and no change to current broker behavior.

### Stage 1: establish the semantic engine seam

- Define topology-free broker command, query, outcome, durability, and error types from current behavior.
- Adapt the existing local implementation as the first engine without changing its public behavior.
- Move shared conformance cases for publish, poll, acknowledge, redelivery, restart, and ambiguous outcomes to an engine test suite.
- Add stable producer/request identity to the provisional protocol before clustered retries depend on it.

Exit evidence: the local broker passes all existing tests plus the engine conformance suite, and no Raft type crosses the engine boundary.

### Stage 2: prove one durable replicated stream

- Add the OpenRaft adapter, versioned internal transport, group manager, and durable storage adapter.
- Run one statically identified data group across three independent local processes.
- Support durable publish, leader-routed poll, and durable acknowledgement for that stream.
- Make forwarding, duplicate requests, retryable failures, and unknown outcomes explicit.
- Add process-kill and restart tests at proposal, persistence, commit, apply, and response boundaries.

Exit evidence: a three-process test demonstrates quorum commit, leader loss, redelivery, duplicate suppression, restart, and no observation of uncommitted records.

This intentionally fixed-stream milestone is a test fixture, not the public clustered product.

### Stage 3: add replicated metadata and stream lifecycle

The initial slice of this stage is implemented. Metadata and data groups are separate, stream creation records `Creating`, prepares the data group on the configured nodes, initializes its durable state, and then records `Active`. Group-addressed peer RPCs and restart restoration keep per-stream runtimes independent. The remaining work in this stage is stronger reconciliation and failure-boundary coverage, including durable consumer ownership and cases where membership or placement changes during creation.

- Add the metadata group and stable cluster, node, stream, and group identities.
- Implement idempotent bootstrap and reconciled `Creating` to `Active` stream lifecycle.
- Route create, publish, poll, and acknowledge through committed metadata from any node.
- Recover partially completed creation after each process-failure boundary.

Exit evidence: streams can be created through the existing public intent on any node, survive full-cluster restart, and resume or reject partial creation deterministically.

### Stage 4: make the three-node development cluster operable

- Add cluster health, per-group leadership and replication lag metrics, quorum/readiness semantics, storage pressure, and forwarding metrics.
- Add bounded admission, queue, batch, connection, and timeout configuration with strong defaults.
- Add graceful shutdown that stops admission, drains only within a deadline, transfers leadership when practical, and never weakens acknowledged durability.
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
- deterministic simulated-network tests for elections, partitions, delayed messages, duplicate messages, and stale leaders;
- real three-process tests using temporary directories and dynamically allocated ports;
- fault-injection tests that kill a process after durable write, before response, during snapshot, and during stream creation;
- model or property tests for monotonic committed positions, no conflicting committed leaders, deduplication, and checkpoint monotonicity;
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
| Crash between commit and broker-state materialization | Atomic applied-state transaction plus idempotent command identity and restart replay tests |
| Partial stream creation leaks unusable groups | Persist lifecycle state and reconcile idempotently |
| Forwarding creates loops or duplicate writes | Hop limits, stable request IDs, bounded retry, and durable deduplication |
| One process accumulates too many tasks and timers | Start with few groups; measure idle memory, task count, timers, file descriptors, and group density |
| Embedded-store behavior dominates tail latency | Benchmark representative durable batches early and preserve the storage adapter boundary |
| Snapshot transfer blocks the hot path | Concurrent snapshot contract, bounded transfer, metrics, and slow-snapshot fault tests |
| Kubernetes identity or control plane becomes part of correctness | Persist broker identities and use static broker-owned membership in the first cluster |
| The engine abstraction becomes a lowest-common-denominator plugin API | Keep it semantic, cluster-wide, and limited to two concrete implementations until migration is designed |

## Accepted implementation defaults

ADR 0004 accepts the following defaults:

1. OpenRaft latest reviewed 0.9 patch, exactly pinned and isolated; TiKV `raft` remains the fallback.
2. Use the pinned development toolchain without publishing a formal source-build compatibility floor.
3. Three static voters, one metadata group, and one data group per stream, with all first-version groups replicated to all three nodes.
4. Any-node client access with internal forwarding; leader-routed linearizable reads initially.
5. Publish success only after quorum commit and durable state-machine apply.
6. Replicate durable consumer checkpoints with stream data; keep delivery deadlines reconstructible and volatile initially.
7. Evaluate redb as the first transactional durable adapter, with a short evidence gate against Fjall and the current append log before accepting it.
8. Keep local and Multi-Raft engines selectable only at process startup; defer mixed engines and live migration.

The current implementation has completed the semantic seam, versioned durable Raft/state-machine files, framed TCP peer transport, topology-free client forwarding, leader-routed reads, durable publish request deduplication, server engine selection, and a real three-process failure test. The remaining Stage 2 evidence includes explicit stale-participant behavior and broader storage/transport fault coverage.
