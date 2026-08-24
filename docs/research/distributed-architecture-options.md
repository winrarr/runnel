# Distributed architecture exploration

- Status: exploratory
- Last reviewed: 2026-08-19

This document records candidate architectures for Runnel's multi-node future. It is not an accepted decision and does not change the current single-node architecture. The product backlog describes the desired outcomes; this document explores ways to reach them. An ADR should record the first selected design before implementation is treated as the distributed architecture.

## Working recommendation

Use Multi-Raft for the first three-node implementation, behind a narrow distributed-log engine boundary shared with the existing local engine. Multi-Raft offers a well-understood path to quorum durability, leader fencing, recovery, and membership changes, making it a useful correctness baseline.

The reviewable implementation proposal is recorded in [multi-raft-implementation-plan.md](../design/multi-raft-implementation-plan.md). It remains a proposal until an ADR accepts the consequential choices.

Do not model the boundary as a generic consensus strategy. The broker should depend on durable messaging semantics, not on terms, leaders, quorums, sequencers, or copysets. Those concepts differ across candidate architectures and belong inside an engine.

Treat alternative engines as experiments until they pass the same conformance, failure, recovery, and benchmark suites. The strongest specialized end-state candidate is currently a sequenced-quorum engine with dynamic virtual ordering shards, epoch-fenced sequencers, batched sequence allocation, quorum/copyset replication, extent-based placement, and a small Raft-backed metadata plane. Chain replication is a first-class throughput-oriented candidate.

## Product invariants every engine must preserve

Architecture selection may change performance and operational tradeoffs, but it must not silently change Runnel's fundamental meaning:

- a message reported as durably committed must survive the failures promised by its selected durability mode;
- an application must be able to distinguish success, rejection, retryable failure, and unknown outcome;
- producer retries must have a path to idempotent resolution;
- consumers normally receive committed messages at least once;
- durable consumer progress must survive the failures promised by its selected durability mode;
- records in the same requested ordering domain are observed in FIFO order;
- unrelated ordering domains may progress independently;
- consumers never observe an uncommitted record merely because it was assigned an internal position;
- stale owners, writers, sequencers, and coordinators cannot make conflicting committed progress;
- backpressure is explicit and memory use remains bounded;
- streams, records, producers, consumers, consumer groups, acknowledgements, replay, and retention remain the public model;
- clients do not need to understand nodes, leaders, physical shards, copysets, extents, or consensus groups;
- Kubernetes may help deploy and discover nodes but is not part of the correctness protocol.

An engine may offer stronger guarantees or different durability profiles, but the selected guarantees must be queryable, documented, and reflected in errors and metrics.

## Decision drivers

Candidate engines should be compared on evidence rather than familiarity:

- p50, p99, and p99.9 publish-to-commit and publish-to-consume latency;
- sustained and burst throughput per core and per storage device;
- batching efficiency for 100-byte and 1-KiB messages;
- memory and allocation behavior at idle, under load, and with slow consumers;
- availability and recovery under process, node, disk, and network failures;
- behavior during replica lag, disk-full conditions, and ambiguous client timeouts;
- time to elect or activate a replacement writer and make the previous writer harmless;
- time and bandwidth required to repair or move retained data;
- operational complexity for three nodes and for larger clusters;
- compatibility with thread-per-core execution and local persistent storage;
- ability to add virtual shards, retention, compaction, and historical object storage without changing the client model;
- implementation complexity, proof burden, and quality of available libraries.

## Candidate engines

### Multi-Raft replicated logs

Each hidden ordering shard is a Raft group with one elected leader and a replica set. The leader assigns log order, replicates entries, and commits after quorum agreement. A separate Raft group may hold cluster metadata, or metadata may be represented through dedicated internal groups.

Why it is the recommended first implementation:

- ordering, replication, fencing, commit, and recovery are integrated in one established model;
- three replicas naturally express one-failure quorum durability;
- mature implementations and extensive literature reduce the amount of novel protocol work;
- the architecture provides a strong baseline against which specialized engines can be measured;
- multiple groups distribute leadership and avoid a single cluster-wide write leader.

Costs and questions:

- every committed append follows the consensus write path;
- many small groups create scheduling, heartbeat, snapshot, and metadata overhead;
- hot ordering shards remain limited by one leader until the stream is split internally;
- group reconfiguration and placement must be coordinated without exposing groups to clients;
- consumer progress and producer deduplication state need deliberate placement rather than being pushed through one global metadata group.

The first version should favor a small number of groups and explicit failure tests over premature automatic sharding.

### Sequencer with quorum and copyset replication

A sequencer assigns positions for one ordering domain while storage replicas persist records independently. A record becomes visible only after the selected acknowledgement quorum has made it durable and the committed watermark covers its position. Replica placement can use copysets and failure-domain rules.

Strengths:

- ordering and storage can scale independently;
- the data path can be specialized for append-only messaging, batching, and sequential I/O;
- sequencers can be lightweight and distributed across many virtual ordering shards;
- copyset placement can control correlated-failure exposure and repair behavior;
- storage nodes need not run a general replicated state machine for each ordering shard.

Costs and questions:

- quorum replication and copyset placement do not by themselves prevent concurrent sequencers;
- sequence allocation creates assigned but potentially uncommitted positions that recovery must resolve;
- epoch activation, fencing, committed-watermark recovery, holes, repair, and replica-set changes form a substantial correctness protocol;
- readers need enough placement history to find records after reconfiguration;
- a global sequencer would become a bottleneck, so sequencing must be scoped to virtual ordering shards;
- copyset width trades correlated-loss frequency against repair parallelism and failure blast radius.

The specialized version worth prototyping combines dynamic virtual ordering shards, epoch-fenced sequencers, batched sequence allocation, extent-level placement, and a Raft-backed metadata plane. Raft would hold low-rate facts such as membership, ownership epochs, placement, and migrations; it would not carry each message.

Sequence assignment and commitment must remain distinct. Batched allocation may reserve ranges, but consumers can only observe a contiguous released prefix or another precisely defined committed view. Producer request identities must allow an uncertain append to be resolved after recovery.

### Chain replication

Each ordering shard or extent has an ordered chain of replicas. Writes enter at the head, flow through every replica, and are acknowledged after reaching the tail. The pipeline can sustain high throughput even though one write experiences the latency of traversing the chain.

Why it deserves a first-class experiment:

- the pipeline can use every replica concurrently and amortize network and storage work across many in-flight batches;
- the model is a natural fit for append-only immutable records;
- it offers a useful operating point for workloads that prioritize sustained throughput over individual-message latency;
- replica responsibilities are simple during steady-state operation.

Costs and questions:

- write latency grows with chain length and slow replicas can constrain the pipeline;
- head, tail, and middle failures require safe chain reconfiguration and fencing;
- acknowledgement and committed-prefix recovery must remain correct when a write is present on only part of the chain;
- reads from the tail simplify consistency but may concentrate read load; alternative read policies need explicit semantics;
- dynamic placement and repair must avoid draining the entire pipeline through one bottleneck;
- consumer progress and metadata still need a separate durable coordination design.

Chain replication should be measured with deep pipelines, large batches, slow replicas, chain repair, and sustained disk pressure. It may be an excellent selectable engine for throughput-oriented deployments even if it is not the default.

### Primary/backup and asynchronous replication

A single owner orders and stores records while replicas follow, optionally acknowledging before remote durability. This is the smallest multi-node extension of a single-node log.

It may be useful as an explicitly weaker durability profile or as a test baseline, but automatic failover still requires epochs, fencing, and a safe view-change protocol. Acknowledging before a failure-surviving copy exists cannot satisfy Runnel's strong durability mode. It should not become an accidental default merely because it benchmarks well.

### Hierarchical or distributed sequencing

Local sequencers can order independent batches while another layer establishes a wider order. Designs such as Scalog show how ordering work can be distributed while retaining a total shared log.

This is relevant if Runnel eventually needs stream-wide ordering at a scale that one sequencer cannot provide. It is less compelling for the current product model because key-scoped ordering can scale through independent ordering domains without manufacturing a global order. The additional ordering layer should only be considered when a measured workload requires it.

### Partially ordered shared logs

A partially ordered log records order only where operations conflict or share an ordering domain. FuzzyLog demonstrates that a shared log need not impose one total order over independent work.

This matches Runnel's preference for FIFO by ordering key while unrelated keys progress concurrently. It could eventually provide a more direct model than hidden conventional partitions. The protocol and replay model are less familiar, however, and cross-domain consumer-group behavior would need careful definition. Treat this as a research candidate rather than the first production engine.

### Leaderless or generalized consensus

Protocols such as EPaxos allow different replicas to coordinate non-conflicting commands without a fixed leader. They can improve locality and distribute coordination load, particularly across regions.

Runnel's append workload conflicts within an ordering domain, reducing the benefit of leaderless fast paths for hot keys or FIFO streams. Dependency tracking, recovery, and implementation complexity are also substantial. This family is worth studying for geographically distributed or highly independent keyed workloads, but it is a poor first implementation.

### Virtualized shared-log composition

Delos separates a stable virtual shared-log API from replaceable underlying log implementations and stitches log generations together during reconfiguration. This is relevant both as an engine architecture and as a way to evolve Runnel without freezing its first replication protocol forever.

The useful lesson is a generation-oriented log boundary: an engine generation has immutable identity and guarantees, and a higher layer can move to a new generation at a defined cutover. This is safer than pretending arbitrary engines are interchangeable at every operation.

Hot-swapping engines is not an initial requirement. Supporting it later would require an explicit protocol for fencing the old generation, establishing a final committed position, importing or referencing historical data, activating the new generation, and teaching readers how to traverse the boundary.

### Erasure-coded and disaggregated historical storage

Erasure coding and object storage can reduce the cost of old immutable data. They add encoding, repair, and read-amplification costs that work against the hot path. Treat them as historical-extent policies behind the active replication engine, not as the first commit protocol.

## Scenario-oriented recommendations

There is no architecture that is best solely because a workload is described as high-throughput or low-latency. The recommendation also depends on durability, ordering scope, workload skew, failure domains, retained history, and operational budget. The following entries identify the strongest candidates to implement and measure under stated assumptions.

Unless stated otherwise, the matrix assumes one datacenter or region, three nodes, tolerance of one node failure, durable local storage, at-least-once delivery, and FIFO only within a requested ordering domain.

### Throughput and latency matrix

| Workload | Best first candidate | Specialized candidate | Reason and caveat |
|---|---|---|---|
| Low throughput, low latency | A small Multi-Raft deployment with direct leader routing and little or adaptive batching | Sequenced quorum with co-located sequencer and storage | Operational simplicity matters more than maximal parallelism. Quorum durability still costs a network and storage round trip. If node-failure durability is not required, the local engine is faster and simpler. |
| Low throughput, latency tolerant | Multi-Raft with conservative durability and minimal shard count | Primary/backup with an explicitly selected weaker mode | Optimize for understandable recovery and low idle overhead. Complex placement and sequencing machinery is unlikely to repay its operational cost. |
| High throughput, low latency | Sharded Multi-Raft as the correctness baseline | Dynamic virtual shards with epoch-fenced sequencers, quorum replication, thread-per-core ownership, and adaptive micro-batching | Parallel ordering domains and single-owner execution are more important than one global protocol. Batches must be small or deadline-driven so queueing does not dominate tail latency. This is the most important head-to-head benchmark. |
| High throughput, latency tolerant | Chain replication with deep pipelining and large batches | Sequenced quorum with sticky copysets and large extents | Pipeline depth, compression, group commit, and sequential I/O can maximize sustained throughput. Chain traversal and large batches increase individual-message latency and make slow-replica behavior important. |

### Ordering scenarios

| Requirement | Best candidate | Notes |
|---|---|---|
| FIFO for one entire stream | One ordering shard using Multi-Raft or one epoch-fenced sequencer with quorum replication | A single ordering authority is the scalability boundary. Horizontal scale requires relaxing global FIFO or introducing hierarchical ordering with additional latency and complexity. |
| FIFO by message key | Dynamic virtual ordering shards | Keep each key in one ordering domain while distributing unrelated keys. Splits and moves require epoch-fenced cutovers so a key never has two active owners. |
| No ordering between independent messages | Many independent shards or a partially ordered log | This exposes the most parallelism. The public contract must still define replay and consumer-group behavior deterministically enough for applications. |
| Atomic order across several keys | Partially ordered shared log or generalized consensus experiment | Use only when the product outcome justifies cross-domain coordination. It can erase the scalability benefit of keyed ordering when conflicts are frequent. |
| Cluster-wide total order | Hierarchical/distributed sequencing or one global sequencer | Expect a coordination bottleneck or additional ordering stage. Runnel should not pay this cost unless an explicit use case requires it. |

### Durability and availability scenarios

| Requirement | Best candidate | Notes |
|---|---|---|
| Lowest acknowledged latency with process-crash durability only | Local engine with durable local append | Must be presented as a local durability mode, not as node-failure-safe replication. |
| Survive one node failure with strong committed-prefix semantics | Multi-Raft or sequenced quorum with intersecting quorums and epoch fencing | Multi-Raft is the simpler first proof. The specialized engine must prove committed-watermark recovery independently. |
| Continue safely through a minority partition | Majority-based Multi-Raft or quorum replication | The minority must stop committing. Availability cannot override fencing and acknowledged-message safety. |
| Continue independent keyed work across a partition | Partially ordered log or carefully scoped regional ownership | Only independent ordering domains may progress. Reconciliation semantics must be explicit before claiming availability. |
| Fast automatic failover | Multi-Raft with tuned failure detection | Sequencer activation can also be fast if epochs and recovery are cheap, but no engine should trade false-positive fencing or split-brain risk for benchmark failover time. |
| Maximum write availability despite slow replicas | Sequenced quorum with a wider eligible nodeset and responsive copyset selection | Placement history, failure-domain constraints, and repair load become more complex. Chain replication is less attractive when one slow member throttles the active chain. |

### Workload-shape scenarios

| Workload | Best candidate | Notes |
|---|---|---|
| One very hot keyed stream | Dynamic virtual shards with per-key ordering | Detect skew, isolate hot keys, and allow shard split or movement without changing the stream API. A single hot key remains one ordering bottleneck. |
| Many mostly idle streams | Shared scheduling over a modest number of Multi-Raft groups, or lightweight sequencer objects backed by shared storage nodes | One heavyweight consensus group or dedicated thread per idle stream wastes resources. Measure idle memory, timers, heartbeats, and file descriptors. |
| Many uniformly busy streams | Multi-Raft with balanced leaders or sequenced quorum with distributed sequencers | Both can spread ordering ownership. Compare coordination overhead, placement quality, and per-core throughput. |
| Large publish batches | Chain replication or sequenced quorum with large sequence allocations and extents | Preserve per-record outcomes and idempotency even when transport and persistence are batched. |
| Tiny messages at very high rate | Thread-per-core sequenced quorum or heavily batched sharded Multi-Raft | Per-message allocation, framing, checksums, system calls, and acknowledgements can dominate. Benchmark end-to-end semantics, not only append throughput. |
| Large messages | Extent-oriented quorum storage with bounded streaming and admission control | Avoid copying payloads through the control plane or keeping full batches in unbounded memory. Ordering metadata and payload transfer may need separate paths. |
| Read-heavy replay | Replicated extent storage with read distribution and optional historical object storage | Reads may use non-leader replicas only under semantics that preserve the committed view. Sequential prefetch and decompression behavior matter. |
| Write-heavy with little immediate consumption | Chain replication or sequenced quorum with deep batching | Storage bandwidth, group commit, compression, and repair reserve determine sustained performance. |
| Slow consumers | Any conforming engine with bounded broker queues and durable cursors | This is primarily a broker semantic requirement, not a reason to select a replication protocol. Retained data must not turn into unbounded memory. |

### Deployment scenarios

| Deployment | Best candidate | Notes |
|---|---|---|
| Hobby or development on one machine | Local engine | Preserve the same client model and conformance tests so moving to a cluster does not require application changes. |
| Small three-node production cluster | Multi-Raft | It provides the clearest first operational and correctness story. Keep shard count and configuration small. |
| CPU-constrained edge deployment | Local engine or small Multi-Raft configuration | Favor low idle overhead and predictable recovery over dynamic placement machinery. Explicitly state whether a node failure can lose acknowledged data. |
| Storage-dense throughput cluster | Chain replication or thread-per-core sequenced quorum | Use many independent pipelines or ordering shards, failure-domain-aware placement, and capacity-aware repair. |
| Multi-availability-zone regional cluster | Multi-Raft or sequenced quorum with placement across zones | Quorum placement and leader/sequencer locality dominate latency. Durability claims must identify the number and type of zone failures tolerated. |
| Geo-distributed active writes | Leaderless/generalized consensus or regional sequencers with partial ordering | This is a research direction, not an initial scope. Global FIFO and low local latency cannot both be assumed under partitions. |
| Kubernetes | Any self-contained engine with persisted identity and broker-owned correctness | StatefulSets, services, and volumes aid deployment. Existing owners may continue only according to engine guarantees when either Kubernetes or Runnel's own metadata quorum is unavailable. |

### Retention and storage-cost scenarios

| Requirement | Best candidate | Notes |
|---|---|---|
| Short retention and low volume | Local segments replicated by the selected engine | Prefer simple deletion and recovery over object-storage machinery. |
| Long retention with frequent replay | Immutable extents on replicas with optional object-storage history | Preserve indexes and enough local cache to avoid pathological replay latency. |
| Long retention with rare replay | Replicated hot extents followed by object storage and optional erasure coding | Move only immutable committed history. Historical durability and restore time need separate guarantees. |
| Compaction by key | Extent-oriented storage with background compaction | Ordering positions, tombstones, consumer replay, and engine migration must remain coherent after physical records are rewritten. |
| Expensive storage footprint | Compression first, then erasure coding for cold extents | Erasure coding on the active commit path is unlikely to fit the initial latency and simplicity goals. |

### Tail-latency and predictability scenarios

For consistently low p99 and p99.9 latency, protocol choice is only part of the answer. The strongest candidate combines bounded queues, admission control, single-owner state, CPU-aware scheduling, preallocated buffers, asynchronous durable I/O, small deadline-driven batches, and isolation between replication, repair, replay, and consumer work.

A thread-per-core model is promising for the specialized sequenced-quorum engine because it can eliminate shared hot locks and make ownership explicit. It remains a hypothesis until compared with a simpler asynchronous design under slow disks, repair traffic, CPU throttling, and container scheduling. The architecture should require single-owner and bounded-work properties without prematurely requiring one runtime library.

### How to use these recommendations

These profiles should become reproducible benchmark and failure-test configurations, not permanent marketing claims. When an engine wins a scenario, record the workload, topology, durability guarantee, failure state, hardware, software version, and confidence interval. Re-run the matrix as engines and storage formats evolve.

## The engine boundary

The common boundary should describe what the broker needs, not how an engine achieves it. It should cover these semantic capabilities:

- initialize or open durable cluster state and report the engine identity and guarantees;
- append one or more records with stream identity, ordering intent, producer identity, and request identity;
- return committed, rejected, retryable, or unknown outcomes;
- read committed records from a logical cursor without exposing physical placement;
- durably advance consumer or group progress with fencing against stale owners;
- support replay and retention decisions against committed history;
- expose readiness, health, lag, durability, and repair state;
- perform engine-specific administrative changes through a stable broker-level intent such as adding or draining a node.

The boundary should not expose Raft terms, sequencer epochs, replica indexes, chain positions, physical shards, copysets, or extents to normal broker code. Engine-specific diagnostics and administration may expose them through explicitly internal or advanced surfaces.

Avoid one large trait that embeds the entire broker. Streams, retries, dead-letter policy, group behavior, authentication, and public protocol semantics should remain common where their correctness does not depend on the engine. Conversely, do not split the engine into tiny consensus-flavored traits before two implementations prove that the seams are real.

The current local engine and the first Multi-Raft engine provide two concrete implementations from which to derive the initial boundary. That satisfies the need for a real second use rather than designing an interface entirely from speculation.

## Packaging and selection

Three packaging approaches are plausible:

### One binary with statically linked engines

The cluster selects an engine when it is initialized. The choice and format version are persisted in cluster metadata, and every joining node must support them. This gives users one operational artifact while avoiding runtime plugin ABI and supply-chain complexity.

This is the preferred user-facing direction if the engines can share a compatible process runtime and dependency footprint.

### Separate engine-specific binaries

Experimental binaries can share the public protocol, broker semantics, conformance suite, and observability conventions while using different runtimes or process layouts. This is attractive when, for example, a thread-per-core sequenced engine and an asynchronous Multi-Raft engine cannot coexist cleanly in one executable.

Separate binaries are appropriate for research and benchmarking. They should not become separate products with subtly different client APIs.

### Runtime-loaded plugins

Dynamic plugins would permit installation without rebuilding the broker, but they introduce ABI stability, trust, deployment, crash-isolation, and upgrade problems. They are not justified for the first implementations. Static composition or separate binaries preserve experimentation without creating a plugin platform.

Engine choice should initially be cluster-scoped and immutable after initialization. Per-stream engine selection and in-place migration are future possibilities that require explicit mixed-engine routing, recovery, and upgrade semantics. They must not be implied by the first adapter.

## Conformance and evaluation

Every engine must run the same black-box scenarios through the public or broker-semantic boundary:

- durable append, consume, acknowledgement, and restart recovery;
- independent consumers and coordinated consumer groups;
- duplicate producer requests before and after failover;
- timeout after an append may have committed;
- stale leader, sequencer, chain head, or consumer coordinator after ownership changes;
- process crash at each persistence and acknowledgement boundary;
- one-node failure and recovery in a three-node deployment;
- network partition with both majority and minority observations;
- slow, unavailable, and full disks;
- lagging replicas and repair while serving traffic;
- node addition, drain, restart, and compatible rolling upgrade;
- slow consumers, bounded queues, and explicit backpressure;
- ordering-key FIFO while unrelated keys make progress;
- retention and replay across placement or ownership changes.

Protocol-specific state machines should also have deterministic simulation or model-based tests. Critical fencing, quorum, recovery, and reconfiguration protocols should be specified independently enough to explore crash points and message reorderings that ordinary integration tests rarely reach.

Benchmark reports must identify the engine, durability guarantee, replica topology, message size, batch policy, storage medium, failure state, and whether data is merely assigned, locally persisted, quorum committed, or visible to consumers.

## Suggested implementation sequence

1. Extract a narrow semantic engine boundary from the current local implementation while preserving its behavior and tests.
2. Add the Multi-Raft engine for one hidden ordering shard per stream and make a three-node process-level test pass before depending on Kubernetes.
3. Add failure injection, conformance tests, and comparable benchmarks around both engines.
4. Prototype a sequenced-quorum engine with one fixed ordering shard and replica set; prove epoch fencing and recovery before dynamic placement.
5. Prototype chain replication under the same contract and compare deep-batch throughput, latency, replica lag, and reconfiguration.
6. Introduce dynamic virtual shards, extent placement, thread-per-core ownership, or alternate sequencing only when measurements identify the bottleneck they address.
7. Decide whether alternative engines belong in one binary or separate engine-specific binaries after their runtime and dependency requirements are concrete.

This sequence is a recommendation, not an accepted architecture decision. Each consequential selection should be captured in an ADR when made.

## Open questions

- Is engine selection permanently cluster-wide, or is per-stream selection worth the operational complexity?
- Is a public record cursor a stable logical sequence, an opaque token, or both?
- Which consumer and producer state belongs in the common broker layer, and which must be committed atomically with an engine log?
- What is the minimum durability guarantee common to all selectable engines?
- May an engine continue serving existing ownership while the Runnel metadata quorum is unavailable?
- How are internal ordering domains split and merged without violating keyed FIFO?
- Are reserved but uncommitted positions represented as permanent holes, omitted from the public sequence, or resolved during recovery?
- Can historical extents remain in their original engine format after a migration?
- Which execution model best delivers predictable tail latency on Linux without coupling the architecture to one Rust runtime?
- What evidence would justify maintaining more than one production engine rather than keeping alternatives as research implementations?

## References

- [Raft consensus paper](https://raft.github.io/raft.pdf)
- [Redpanda partition replication architecture](https://docs.redpanda.com/streaming/24.2/get-started/architecture/)
- [LogDevice architecture](https://logdevice.io/docs/Concepts.html), [write path](https://logdevice.io/docs/Writepath.html), [replication](https://logdevice.io/docs/Replication.html), and [recovery](https://logdevice.io/docs/Recovery.html)
- [Apache BookKeeper protocol](https://bookkeeper.apache.org/docs/development/protocol/)
- [CORFU shared-log paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/04/corfumain-final.pdf)
- [Copyset Replication paper](https://web.stanford.edu/~skatti/pubs/usenix13-copysets.pdf)
- [Chain Replication paper](https://www.usenix.org/conference/osdi-04/chain-replication-supporting-high-throughput-and-availability)
- [Virtual Consensus in Delos](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)
- [Scalog shared-log paper](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- [FuzzyLog partial-order paper](https://www.cs.yale.edu/~aspnes/papers/osdi2018-proceedings.pdf)
- [EPaxos implementation and paper links](https://github.com/efficient/epaxos)
