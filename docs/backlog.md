# Product backlog

These are unfinished product outcomes derived from the project brief. They describe goals, rationale, constraints, and observable acceptance criteria; they intentionally do not prescribe an implementation. Agents should inspect the current system, evaluate alternatives, benchmark material assumptions, and record consequential choices in ADRs.

The recommended sequence is to complete the dependable single-node product first, then establish the design and verification needed for a three-node development deployment. Distributed work must preserve the single-node crash and durability guarantees rather than introduce a separate application model.

The `##` sections are parent outcomes. When a parent becomes large enough to require coordinated work, add unfinished child outcomes as nested `###` sections beneath it. Keep each child goal-oriented with its own rationale, constraints, and verifiable acceptance criteria. Use descriptive headings instead of identifiers; refer to another child by its heading when a dependency matters. Remove a child once its outcome is complete, leaving durable rationale in the relevant ADR and implementation history in the repository. Do not turn children into implementation checklists.

## Make client interactions dependable and evolvable

Goal: provide a stable client-facing contract that lets applications publish and consume messages while understanding the result of each operation.

Rationale: applications need to handle success, rejection, retryable failure, and uncertain outcomes safely. The contract is also the boundary that future language clients and clustered deployments must preserve.

Constraints:

- keep the public vocabulary small and intent-oriented;
- do not expose storage layout, physical placement, or broker topology as normal application concepts;
- support message payloads without requiring them to be text;
- do not claim compatibility until compatibility and upgrade behavior are defined.

Acceptance criteria:

- clients can distinguish confirmed success, confirmed rejection, retryable failure, and unknown outcome;
- a producer can safely retry according to documented semantics without creating an unintended duplicate when deduplication is requested;
- the contract has a documented compatibility policy;
- behavior is covered by interoperability and compatibility tests.

## Make message processing complete

Goal: support independent consumers, coordinated work distribution, acknowledgements, retries, replay, dead-letter handling, batching, and scoped ordering as coherent delivery behavior.

Rationale: the broker should support both event distribution and work processing without requiring applications to understand internal ownership or storage details.

Constraints:

- normal delivery remains at least once;
- ordering applies only where the application requests it, allowing unrelated work to progress concurrently;
- slow consumers must encounter explicit backpressure or rejection rather than unbounded resource use or silent loss;
- consumer state must remain durable and transferable as the system evolves beyond one process.

Acceptance criteria:

- multiple consumers can share work without concurrently processing the same ordered item;
- independent consumers can each process the same stream without affecting one another;
- consumer crashes, membership changes, retries, and acknowledgements do not lose committed progress;
- failed messages can follow a documented retry policy and eventually be isolated for inspection or recovery;
- consumers can request a documented replay scope;
- batching has documented acknowledgement and failure semantics;
- messages with the same requested ordering key are delivered in order while unrelated keys can progress concurrently;
- slow-consumer tests demonstrate bounded memory and explicit backpressure.

## Make retained data operationally scalable

Goal: allow streams to grow from small local workloads to substantially larger retained data while keeping startup, memory use, recovery, and retention behavior predictable.

Rationale: durable messaging is only useful when data remains operable over time. The storage design must support growth without changing the public stream and consumer model.

Constraints:

- streams must not be permanently tied to a particular local file or processing unit;
- time- and size-based retention must respect active consumers and documented replay guarantees;
- durability choices and their crash guarantees must be explicit;
- memory and disk work must remain bounded by configured policy rather than unbounded by retained history.

### Make durable storage upgrades safe

Goal: let supported broker upgrades preserve or deliberately transform acknowledged data and consumer progress without silent loss.

Rationale: the storage layout now has separate metadata and stream data groups, so future format and placement changes need an explicit recovery and migration contract.

Constraints:

- incompatible layouts must fail clearly rather than appear empty or partially recovered;
- migration must preserve the documented durability, ordering, replay, and acknowledgement guarantees;
- upgrade work must remain bounded and observable for large retained streams.

Acceptance criteria:

- supported upgrade paths and unsupported downgrade paths are documented;
- representative old and new layouts have automated recovery or migration tests;
- interrupted migration resumes safely or leaves the original data usable;
- storage version, format, and migration outcomes are visible in diagnostics.

### Make message encoding and compression evolvable

Goal: reduce storage, network, and CPU overhead with efficient message representations and optional compression while preserving the public stream and delivery model.

Rationale: small messages and long-lived streams make framing, copying, encoding, and compression costs significant parts of Runnel's performance and storage profile.

Constraints:

- the wire and durable formats must be versioned and recoverable across compatible upgrades;
- compression must be selectable according to workload and must not silently weaken durability, ordering, replay, or corruption detection;
- bounded memory and predictable tail latency take priority over compression ratio alone;
- format changes must remain independent from any one storage engine or replication architecture.

Acceptance criteria:

- representative message sizes have documented encoding, compression, CPU, memory, storage, and latency measurements;
- the broker can identify, validate, and recover supported format versions after restart;
- mixed-format retained data has documented read, replay, upgrade, and retention behavior;
- benchmarks demonstrate when compression improves total resource usage and when it should be disabled.

### Make retained-state growth independent of the hot path

Goal: keep publish, consume, recovery, and resource behavior predictable as a stream's retained history grows.

Rationale: a broker cannot meet its throughput, latency, and bounded-memory goals if each new message requires work proportional to all retained state.

Constraints:

- retained history must remain durable and replayable under the documented guarantees;
- recovery and retention work must be bounded, observable, and interruptible;
- the public stream, record, consumer, and acknowledgement model must not depend on one local materialization shape.

Acceptance criteria:

- hot-path latency and throughput remain within documented bounds across representative retained-data sizes;
- recovery cost, memory use, and storage amplification are measured as retained data grows;
- interrupted recovery and retention operations preserve acknowledged messages and consumer progress;
- the storage design leaves a credible path to segmented data, historical storage, and future replication engines.

### Make concurrent broker work scale predictably

Goal: allow independent publishing, consuming, acknowledgement, health, and recovery work to make progress concurrently without violating delivery or durability guarantees.

Rationale: a single serialized execution path can hide the performance characteristics needed for low tail latency and high throughput.

Constraints:

- ordering is serialized only within the requested ordering domain;
- backpressure and memory bounds remain explicit under contention and slow consumers;
- crash recovery, acknowledgement ordering, and durable outcomes remain unchanged.

Acceptance criteria:

- representative concurrent workloads demonstrate predictable p50, p99, and p99.9 behavior;
- unrelated streams, consumers, and ordering keys can progress independently;
- contention, queueing, and resource-pressure behavior are visible in metrics and repeatable tests;
- improvements are supported by benchmarks rather than assumptions about scheduling or locking.

Acceptance criteria:

- restart and recovery behavior remains predictable as retained data grows;
- memory use remains within a documented bound for a documented workload;
- retention does not remove data that an active consumer is still entitled to receive;
- compression, when enabled, preserves the documented delivery and recovery guarantees;
- benchmarks report workload, message size, durability choice, throughput, latency, recovery behavior, memory, and storage usage.

## Make the single-node deployment ready for real use

Goal: provide the security, resource, health, and observability behavior needed to operate one broker responsibly in a local, container, or small production environment.

Rationale: low operational complexity is a core product promise, so safe defaults and useful signals matter as much as message throughput.

Constraints:

- the broker remains correct without Kubernetes;
- credentials, keys, and certificates must be supplied at runtime rather than committed or embedded in images;
- health signals must distinguish process liveness from the ability to serve durable traffic;
- resource limits must produce explicit behavior under pressure.

Acceptance criteria:

- authentication and authorization can protect client operations when enabled;
- client connections can use TLS with documented configuration and failure behavior;
- readiness and liveness have documented meanings and are suitable for stateful deployment;
- metrics expose throughput, latency, consumer lag, redelivery, storage, resource pressure, and broker health;
- graceful shutdown, full or slow storage, and restart behavior are covered by repeatable tests;
- the container and single-node Kubernetes deployment document their persistence and resource assumptions.

## Run a reliable three-node development deployment

Goal: deploy three Runnel nodes with persistent storage and exercise a coherent clustered broker while applications continue to use streams and consumers without learning the node layout.

Rationale: the long-term product needs a credible path to availability and larger workloads. A small repeatable deployment is the right boundary for validating distributed assumptions before expanding operational scope.

Constraints:

- write and acknowledgement guarantees must be stated for node failures and network partitions;
- the cluster must remain safe if the Kubernetes control plane is temporarily unavailable;
- ownership, membership, failover, and stale-state behavior must be correct under crashes and restarts;
- the first clustered deployment is a development milestone, not an automatic promise of production-grade operations.

### Make stream identity and lifecycle authoritative

Goal: ensure every node agrees on stream identity, lifecycle, and placement intent while applications continue to address streams normally.

Rationale: a distributed broker cannot safely recover, move, or balance data if stream and consumer state exists only as an incidental property of one node.

Constraints:

- preserve the public stream and consumer model without exposing physical placement;
- recover partial creation and restart transitions deterministically;
- keep metadata independent from any one local file, process, or current replica layout.

Acceptance criteria:

- stream metadata survives full-cluster restart with one authoritative result;
- partial lifecycle operations either complete or reject deterministically after recovery;
- future placement and movement can use the same identities without changing application requests.

### Make placement scale independently of stream identity

Goal: support many streams and uneven workloads without requiring one permanent distributed processing unit or replica layout per public stream.

Rationale: a small static cluster is useful for correctness testing, but its initial placement shape must not become the scalability boundary for larger deployments.

Constraints:

- applications continue to address streams without learning node, shard, or replica placement;
- movement, splitting, and balancing must preserve ordering, acknowledged durability, replay, and consumer progress;
- the cluster must remain safe and resource-bounded while placement changes are in progress.

Acceptance criteria:

- placement can distribute many streams and hot ordering domains across available capacity;
- placement changes have explicit recovery, fencing, and observability behavior;
- a node or storage failure does not require application-level remapping;
- placement and balancing behavior is measured for idle, uniform, and skewed workloads.

### Make consumer ownership authoritative

Goal: make consumer progress and ownership durable, transferable, and safe under concurrent consumers, crashes, and membership changes.

Rationale: stream placement alone does not make work distribution reliable; consumer state must be recoverable without depending on one process's volatile ownership.

Constraints:

- preserve at-least-once delivery and durable acknowledgement semantics;
- stale consumers must not continue acknowledging work after ownership changes;
- normal consumers must not need to understand internal group placement or consensus terms.

Acceptance criteria:

- consumer progress survives node restart and the documented failure scenarios;
- ownership changes are fenced and cannot produce conflicting committed progress;
- independent consumers and consumer groups have repeatable crash, retry, and rebalancing tests.

### Make membership and failover behavior safe

Goal: keep the cluster correct while nodes restart, become unavailable, rejoin, or change membership.

Rationale: availability is only useful when stale participants cannot make conflicting progress or acknowledge state that the cluster has not durably accepted.

Constraints:

- state the failure and partition assumptions for each supported durability mode;
- correctness must not depend on the Kubernetes control plane remaining available;
- membership changes must preserve fencing and recovery invariants.

Acceptance criteria:

- stale participants cannot commit conflicting writes after losing authority;
- the documented node-failure and restart scenarios have repeatable integration tests;
- adding or removing a member has deterministic recovery and rejection behavior.

### Make repeated interrupted replica recovery operationally complete

Goal: make repeated snapshot-based recovery interruptions safe when transfer or installation is interrupted and retried.

Rationale: a replacement replica can now recover without the full consensus history, and a single interrupted transfer safely restarts from the beginning, but recovery is not operationally complete until repeated interruption has explicit behavior and bounded operational cost.

Constraints:

- recovery must validate state before it becomes serving state;
- snapshot and transfer work must remain bounded and observable through operational signals;
- retained message history and consensus recovery state must remain separate concerns.

Acceptance criteria:

- repeated interrupted snapshot transfers restart without corrupting committed broker state;
- recovery does not cause acknowledged messages or durable consumer progress to disappear under repeated interruption and restart tests.

### Make clustered durability and outcomes explicit

Goal: give applications an unambiguous contract for acknowledged writes, retryable failures, and outcomes that cannot yet be known.

Rationale: applications must be able to choose safe retry behavior without learning which node or internal group handled a request.

Constraints:

- acknowledged durable data must have a documented quorum and storage guarantee;
- ambiguous outcomes must remain visible rather than being silently retried;
- producer request identity and deduplication must not weaken ordering or durability semantics.

Acceptance criteria:

- documentation states what acknowledged data survives for each supported node-failure scenario;
- clients can distinguish confirmed success, confirmed rejection, retryable failure, and unknown outcome;
- safe retries do not create unintended duplicate messages when deduplication is requested.

### Make the clustered deployment operable

Goal: provide the health, security, observability, persistence, and upgrade behavior required to operate the development cluster responsibly.

Rationale: a cluster that is correct only during normal traffic is not a dependable deployment.

Constraints:

- readiness must represent the ability to serve the documented durable workload;
- authentication, TLS, and credentials must be supplied and rotated through deployment configuration;
- resource pressure, disruption, and upgrade behavior must be explicit.

Acceptance criteria:

- readiness, liveness, disruption, persistence, and upgrade assumptions are documented beside the deployment;
- metrics expose cluster health, leadership, replication progress, forwarding, storage, and resource pressure;
- security and graceful shutdown behavior are covered by repeatable deployment tests.

### Make broker and peer communication efficient and evolvable

Goal: support efficient production data and peer communication without changing application intent or weakening outcome semantics.

Rationale: framing, payload representation, connection management, copying, and batching can dominate small-message latency and throughput.

Constraints:

- binary payloads, version negotiation, and compatibility behavior must be explicit;
- success, rejection, retryable failure, and unknown outcomes must remain distinguishable;
- batching and connection reuse must preserve ordering, backpressure, and bounded resource use.

Acceptance criteria:

- representative client and peer workloads measure encoding, copying, connection, batching, and scheduling costs;
- supported payload and protocol versions have compatibility and recovery tests;
- communication failures and ambiguous outcomes are observable and safely recoverable;
- the selected communication behavior is documented before it becomes a compatibility promise.

### Establish clustered performance and fault baselines

Goal: measure whether the selected clustered design meets Runnel's latency, throughput, resource, and recovery goals.

Rationale: the first distributed implementation is a baseline for evaluating later copyset, sequencer, chain, and other engines.

Constraints:

- every result must state topology, durability, storage, batching, message size, and failure state;
- tail latency and resource usage matter alongside throughput;
- failure tests must exercise real process and storage boundaries where practical.

Acceptance criteria:

- benchmarks cover durable publish, publish-to-consume latency, sustained throughput, batching, slow consumers, restart, and recovery;
- results include p50, p99, and p99.9 latency, memory, CPU, and storage usage where applicable;
- the baseline can be rerun to compare future distributed engines without changing the public workload model.

### Make cross-broker benchmark comparisons reproducible

Goal: rerun representative Runnel and competing-broker workloads under controlled, documented conditions and compare their results over time.

Rationale: performance leadership is meaningful only when message semantics, durability, resource limits, workload shape, and measurement boundaries are equivalent.

Constraints:

- each broker adapter must state the guarantees it actually measures rather than implying semantic equivalence;
- broker images, client tools, workload definitions, CPU and memory limits, storage assumptions, and host information must be recorded;
- noisy or unsuitable measurements must remain visible as experimental and must not silently gate changes;
- benchmark artifacts must be machine-readable and suitable for later trend reporting.

Acceptance criteria:

- a single documented command can run the supported broker adapters in containers with explicit resource limits;
- the common workload matrix includes message sizes, concurrency, batching, durable publish, consume and acknowledgement, recovery, and resource usage;
- results report throughput, p50, p99, p99.9, CPU, memory, storage, configuration, and failure state where applicable;
- Kafka, Redpanda, and NATS JetStream comparisons document their acknowledgement, replication, and delivery semantics;
- repeated runs can be compared without manually transcribing results.

### Make performance history visible without making noisy benchmarks a gate

Goal: retain benchmark results from selected pull requests and scheduled runs so meaningful performance changes can be investigated over time.

Rationale: a benchmark suite becomes more valuable when regressions and improvements can be related to code changes, while noisy shared runners should not block unrelated development.

Constraints:

- trend data must retain the workload, environment, and broker configuration needed to interpret it;
- pull-request reporting should begin with informative artifacts and summaries before becoming a hard performance gate;
- benchmark jobs must be isolated from correctness CI and have explicit time and resource budgets.

Acceptance criteria:

- selected benchmark runs publish machine-readable artifacts and a human-readable summary linked to the change;
- scheduled runs provide a stable comparison history for supported workloads;
- the project can identify genuine regressions separately from runner noise;
- enabling or changing a performance gate requires an explicit decision based on observed variance.

Acceptance criteria:

- the selected distributed model, failure assumptions, and durability choices are recorded in an ADR before implementation is treated as complete;
- three nodes start with independent persistent storage and can form the documented deployment without application-level topology configuration;
- acknowledged durable data survives the node failures promised by the selected durability mode;
- stale participants cannot make conflicting progress after ownership changes;
- node restart, membership change, and the promised node-failure scenario have repeatable integration tests;
- existing stream, producer, consumer, group, acknowledgement, and replay intent remains usable without exposing physical placement;
- Kubernetes readiness, disruption, persistence, upgrade, and control-plane assumptions are documented beside the deployment artifact.

## Extend the platform after the clustered core is sound

Goal: add larger-scale and ecosystem capabilities when the clustered storage, protocol, and operational foundations can support them without fragmenting the product model.

Candidate outcomes include historical data beyond local storage, compaction and tombstones, transactions and cross-stream atomic publishing, namespaces and multi-tenancy, cross-cluster replication and disaster recovery, schema metadata, connectors, and additional language clients.

Constraints:

- each capability must justify its operational and conceptual complexity;
- capabilities must preserve documented failure and compatibility semantics;
- normal application code should not need to understand internal topology.

Acceptance criteria:

- each capability has a clear user outcome and documented boundaries before implementation;
- consequential design choices have ADRs;
- correctness tests, operational documentation, and workload benchmarks exist where the capability affects reliability or performance.
