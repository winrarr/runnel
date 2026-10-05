# Product backlog

These are unfinished product outcomes derived from the project brief. They describe goals, rationale, constraints, and observable acceptance criteria; they intentionally do not prescribe an implementation. Implementation work should inspect the current system, evaluate alternatives, benchmark material assumptions, and record consequential choices in ADRs.

This is an inventory, not a prioritized execution queue; section order does not determine what happens next. Select work according to current goals, dependencies, correctness risks, and learning value. Early correctness work and reusable tests and benchmarks establish a foundation for later product features. Research may explore larger deployments before their implementation is scheduled. Production readiness still requires the storage, overload, compatibility, and usability foundations of the initial workloads, and distributed work must preserve the single-node crash and durability guarantees rather than introduce a separate application model.

The `##` sections are parent outcomes. When a parent becomes large enough to require coordinated work, add unfinished child outcomes as nested `###` sections beneath it. Keep each child goal-oriented with its own rationale, constraints, and verifiable acceptance criteria. Use descriptive headings instead of identifiers; refer to another child by its heading when a dependency matters. Remove a child once its outcome is complete, leaving durable rationale in the relevant ADR and implementation history in the repository. Do not turn children into implementation checklists.

## Validate the initial product fit and operating envelope

Goal: validate that the audience and workloads in [product-fit.md](product-fit.md) experience Runnel as a dependable, focused broker and establish the limits or alternatives they should understand before adoption.

Rationale: the initial product thesis is specific enough to guide development, but its workload budgets, usability, and operating envelope are not yet supported by intended-user evidence. Without that validation, infrastructure work can expand faster than evidence of user value.

Progress: the repeatable harness passed the two pre-registered local reference
workloads against representative latency, throughput, RSS, storage-growth,
and restart budgets on 2026-09-06. On 2026-10-03, the background-work
workload also recorded a passing point observation of
`runnel_in_flight_deliveries=2` while two distinct deliveries were held. These
are repository measurements, not application SLOs or supported limits.
Intended-user exercises, consumer lag, steady slow-consumer and sustained
memory bounds, broader fault coverage, and one-node-to-cluster migration
evidence remain open. See the [validation
record](research/initial-product-fit-validation.md).

Constraints:

- validate real application workflows rather than optimizing only synthetic broker operations;
- state unsupported workloads and guarantees as clearly as supported ones;
- keep the initial operating model viable for one developer or a small team without dedicated broker expertise;
- treat performance comparisons as engineering evidence, not as a substitute for adoption and operability evidence.

Acceptance criteria:

- two or three representative end-to-end workloads validate their documented durability, ordering, replay, scale, and operational needs;
- each representative workload meets an explicit latency, throughput, memory, storage-growth, recovery, and operator-effort budget;
- onboarding and failure-recovery exercises with intended users identify where the public model or operational workflow is unclear;
- documentation states when Runnel is a good fit, when it is not, and what product evidence supports those boundaries.

## Improve the development feedback loop

Goal: determine whether a newer hosted CI/CD platform can provide a materially better pull-request development experience than the current GitHub Actions workflow.

Rationale: GitHub Actions is deeply integrated with the repository and is currently adequate, but its workflow model and operational experience have known rough edges. A bounded trial of a credible alternative could reveal a simpler or more effective way to validate changes without committing the project to a migration on intuition alone.

Constraints:

- do not assume dedicated workers, specialised hardware, or operator-managed caching and queues are required;
- preserve the existing verification, integration, security, benchmark, artifact, status-check, and release coverage during the evaluation;
- compare equivalent workloads and DAGs, including cold and warm runs, rather than comparing unrelated default configurations;
- keep any trial isolated from required repository checks until the result and rollback path are understood;
- retain GitHub as the source-control and pull-request system unless a broader change is explicitly justified.

Acceptance criteria:

- at least one credible hosted alternative is evaluated against the GitHub Actions baseline using the same representative pull-request DAG and workloads;
- the comparison reports wall-clock time, variability, cache behavior, setup and maintenance effort, failure diagnostics, retry behavior, artifact and status-check integration, contributor ergonomics, and expected cost or usage limits;
- the evaluation records which GitHub Actions shortcomings the candidate addresses, which it does not, and any new constraints it introduces;
- a decision record recommends keeping GitHub Actions, piloting the alternative, or migrating, with an evidence-backed rollback plan and no loss of required coverage;
- if the alternative is not materially better, the result is still recorded so the question does not need to be rediscovered.

Current status: the desk evaluation is recorded in
[ci-feedback-loop-evaluation.md](research/ci-feedback-loop-evaluation.md) and
[ADR 0025](decisions/0025-retain-github-actions-pending-hosted-ci-trial.md).
The outcome remains open: no hosted CircleCI trial was run in this environment,
so CircleCI-versus-GitHub comparative wall-clock, variability, cache,
contributor, diagnostics, status-check, and cost evidence remain to be
collected before acceptance.

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

### Provide a production-usable client path

Goal: let the initial audience integrate Runnel without implementing protocol framing, connection management, retries, and error classification themselves.

Rationale: a development CLI and inspectable JSON protocol are effective for the current vertical slice, but they are not yet a safe or ergonomic application integration surface.

Constraints:

- client behavior must preserve explicit confirmed, rejected, retryable, and unknown outcomes;
- timeouts, cancellation, reconnects, backpressure, and retry identity must have bounded and documented behavior;
- the first supported client should validate the protocol without prematurely committing the project to many language SDKs.

Acceptance criteria:

- at least one supported client library exercises persistent connections, binary payloads, timeouts, cancellation, and safe publish retries;
- a representative application uses the supported client in an end-to-end restart and ambiguous-outcome test;
- client and broker compatibility ranges are documented and checked automatically;
- connection and retry defaults are suitable for the documented initial workloads without requiring broker-internals knowledge.

Current progress: the Rust client now provides persistent sequential
connections, binary payloads, bounded connect/write/read/response-size
timeouts, cancellation-safe connection invalidation, explicit confirmed versus
rejected/retryable/unknown outcomes, and stable-identity publish retry. Real
server tests cover those boundaries, including an application-shaped
publish/consume/ack flow that redelivers an unacknowledged binary message after
restart. Protocol, client, and server-facing code now automatically check that
their provisional v1 version range and UTF-8 text/base64 payload declarations
remain aligned. A version-negotiated compatibility contract and evidence from
an intended external application remain open. The source-backed
[publish request-ID reuse review](research/publish-request-id-content-mismatch.md)
confirms that both engines currently return the first offset when a retry
changes key or payload; the clustered mismatch case lacks focused process
coverage, and the public contract decision remains open.

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

### Make shared consumer delivery dependable

Goal: let multiple worker instances share one durable consumer while preserving independent fan-out consumers, at-least-once delivery, scoped ordering, and safe progress.

Rationale: small applications need a single durable worker without extra coordination, while growing applications should be able to add workers without learning about partitions or triggering application-managed rebalancing.

Current progress: local and clustered grouped delivery now cover durable attempts, out-of-order acknowledgements, expiry, stale-delivery fencing, bounded expiry lookup, and real-process restart/failure paths. The shared `assert_expired_delivery_is_fenced` contract runs against the local broker (100 ms acknowledgement timeout; 250 ms wait) and the persistent engine in a one-node configuration (2 s timeout; 2.1 s wait). Both runs require an acknowledgement after expiry but before any replacement poll to return `StaleDelivery`, then verify same-offset redelivery with a fresh token, continued rejection of the old token, and successful acknowledgement with the replacement token. This establishes matching local and persistent-engine behavior before reassignment; the one-node persistent run does not establish multi-node failover behavior. A separate real three-process public-protocol test verifies that an in-flight key blocks its successor while another key progresses, preserves the held member receipt across a repeated poll, and releases the successor after acknowledgement. Lease expiry remains demand-driven, with no background timer. The shared engine also reports currently tracked in-flight deliveries. Broader failover, replay, retry-policy, dead-letter, and scalable ownership behavior remain incomplete.

Constraints:

- the public model must remain streams, consumers, records, acknowledgements, and ordering intent;
- a worker failure may cause redelivery but must not silently lose committed progress;
- acknowledgements may arrive out of order when work is shared;
- stale workers must not be able to acknowledge a later delivery of the same record;
- delivery and retry state must remain bounded and transferable beyond one process.

Acceptance criteria:

- multiple members of one consumer receive disjoint available work during normal operation;
- different consumer names continue to receive independent copies of the stream;
- a message whose delivery expires can be processed by another member, while an acknowledgement that observes an expired lease is rejected as stale even before reassignment;
- local and clustered conformance tests assert the same acknowledgement result after the lease deadline but before any replacement poll;
- messages with the same requested ordering key are not concurrently delivered to different members;
- durable progress, replay, retry, and dead-letter behavior remain correct after restart and membership changes;
- local and clustered engines share conformance tests for these outcomes.

#### Make retry policy application-aware

Goal: let applications choose documented retry, backoff, dead-letter, and recovery behavior appropriate to each durable consumer without exposing storage or cluster topology.

Rationale: a single broker-wide attempt limit is a useful local default, but event fan-out, interactive work, and long-running jobs have different failure and recovery needs.

Current progress: local and clustered engines now expose durable configure and
inspect operations for bounded per-consumer acknowledgement timeouts and
attempt limits. Policies use the broker-wide settings as a legacy fallback,
pin on first delivery, survive restart and clustered state replay, and retain
the existing derived dead-letter transition. Backoff, provenance, redrive, and
richer terminal dispositions remain open.

Constraints:

- policy changes must not weaken at-least-once delivery or ordering guarantees;
- retry and dead-letter outcomes must remain durable, observable, and bounded;
- dead-letter records must retain enough provenance for safe inspection and redrive;
- policy selection must remain independent from physical placement and future clustered ownership.

Acceptance criteria:

- consumers can select and inspect a documented retry and dead-letter policy;
- backoff, attempt limits, redrive, and poison-message behavior have repeatable failure and restart tests;
- dead-letter provenance and duplicate behavior are explicit;
- policy state can be transferred when consumer ownership moves between nodes.

### Make replay an explicit and safe consumer operation

Goal: let an application deliberately reprocess a documented portion of retained history without editing broker files, inventing consumer names, or confusing replay progress with the consumer’s current durable position.

Rationale: replay is part of the stated public model and a core reason to use durable streams, but the current poll contract only follows one forward checkpoint.

Current progress: local and clustered engines now expose an additive,
bounded, read-only replay operation for one inclusive logical offset. The
protocol, typed client, CLI, and real-server tests preserve ordinary consumer
progress and return explicit `history_unavailable` outcomes. Time selectors,
durable replay sessions, retention floors and pins, replay acknowledgements,
failover/replay-session behavior, and replay-specific observability remain
open.

The exploratory [time-selector research](research/replay-time-selector-semantics.md)
records timestamp ordering, retention-completeness, and bounded-lookup risks.
The [durable replay-session design](design/replay-sessions.md) compares
session models and proposes separate cursor, acknowledgement, fencing,
snapshot, and retention-pin semantics for further review. Neither note accepts
a runtime or API decision.

Constraints:

- replay eligibility must follow the selected retention policy;
- reset and concurrent delivery behavior must not silently discard acknowledged progress;
- local and clustered engines must expose the same intent without revealing storage or placement;
- replay work must remain bounded and must not starve foreground consumers.

Acceptance criteria:

- a consumer can request replay from supported time, offset, or checkpoint scopes with explicit validation and outcome semantics;
- concurrent polls, acknowledgements, retries, and replay changes have deterministic fencing behavior;
- restart and failover tests preserve the selected replay position and original durable progress as documented;
- lag, replay progress, unavailable history, and replay-induced resource pressure are observable.

### Make batching preserve per-record outcomes

Goal: amortize protocol and durability overhead for publish and consume workloads while keeping ordering, retry, acknowledgement, and ambiguous outcomes safe for every record.

Rationale: batching is necessary for efficient small-message workloads, but an underspecified batch can hide partial success or force unsafe retries.

Current progress: bounded binary-safe publish batches now return ordered per-record outcomes, preserve request-ID deduplication, and use explicit local and clustered durability boundaries. A real-process typed-client check verifies ordered mixed record results, binary payload round-trip, and request-ID replay without duplicates across a local restart. A dropping-proxy batch check observes an accepted batch whose response was discarded, asserts unknown outcomes for every record, and confirms stable-ID retry leaves the records present exactly once. A three-process typed-client test withholds a successful batch response from the probed data-group leader, stops that leader, confirms a different survivor reports leadership, then reconnects and retries with stable IDs and confirms the records are present exactly once. A real-server typed-client response-timeout test captures the complete successful broker response while the proxy withholds it, verifies `ResponseTimeout` and unknown outcomes for every record, then reconnects and retries stable IDs to confirm the original offsets and exactly-once public consumption. An opt-in clustered publish-batch baseline records per-record throughput and batch round-trip latency. [ADR 0030](decisions/0030-consume-batch-contract.md) proposes the consume-batch semantic contract for review, and the [design](design/consume-batches.md) records implementation gates for per-offset policy snapshots, mixed ack outcomes, dead-letter behavior, journal reconciliation, and the replicated-command boundary. No consume-batch runtime behavior is implemented; implementation tests and the broader batch-size, failure, recovery, and resource tradeoff matrix remain open.

Constraints:

- clients must be able to determine or resolve each record’s confirmed, rejected, retryable, or unknown outcome;
- batch size and buffering must be bounded by bytes, records, and time;
- batching must not broaden an ordering or atomicity guarantee implicitly;
- local and clustered durability points must remain explicit.

Acceptance criteria:

- publish and delivery batches have documented partial-failure, ordering, acknowledgement, and retry semantics;
- stable request identity resolves ambiguous publish outcomes without duplicating records when deduplication is requested;
- timeout, disconnect, restart, leader-change, and oversized-batch tests cover partial outcomes;
- representative workloads show the throughput, p99/p99.9 latency, memory, and durability tradeoffs across batch sizes.

## Make retained data operationally scalable

Goal: allow streams to grow from small local workloads to substantially larger retained data while keeping startup, memory use, recovery, and retention behavior predictable.

Rationale: durable messaging is only useful when data remains operable over time. The storage design must support growth without changing the public stream and consumer model.

Constraints:

- streams must not be permanently tied to a particular local file or processing unit;
- time- and size-based retention must respect active consumers and documented replay guarantees;
- durability choices and their crash guarantees must be explicit;
- memory and disk work must remain bounded by configured policy rather than unbounded by retained history.

Acceptance criteria:

- restart and recovery behavior remains predictable as retained data grows;
- memory use remains within a documented bound for a documented workload;
- retention does not remove data that a consumer is still entitled to replay under the selected policy;
- compression, when enabled, preserves the documented delivery and recovery guarantees;
- benchmarks report workload, message size, durability choice, throughput, latency, recovery behavior, memory, and storage usage.

### Make retention and disk-pressure behavior safe

Goal: bound retained storage and define what happens as consumers lag or usable disk capacity approaches its limit.

Rationale: an append-only broker without enforceable retention and admission policy eventually turns ordinary consumer lag into an availability or data-loss incident.

Current progress: a real-server test now repeats synthetic same-stream storage-executor saturation and verifies bounded rejection, health/readiness/metrics behavior, recovery, and subsequent durable traffic. The [source-backed retention and disk-pressure review](research/retention-disk-pressure-semantics.md) separates history eligibility, consumer/replay entitlement, and physical write admission; it compares time/size limits, acknowledgement-driven retention, filesystem preflight and `ENOSPC`, cleanup ordering, and bounded observability without selecting a policy or API. It identifies that the local consumer cache is not a complete retention inventory and that existing `storage_bytes` is not filesystem capacity. It does not exercise filesystem-capacity admission, `ENOSPC`, retention cleanup, or interrupted deletion; those outcomes remain unimplemented and the acceptance criteria remain open.

Constraints:

- retention and admission decisions must preserve the selected replay and acknowledged-durability guarantees;
- the broker must never silently discard committed data or accept a write it cannot durably complete;
- time, size, consumer-lag, and reserved-capacity policies must have explicit precedence and observable effects;
- cleanup work must remain interruptible and must not monopolize foreground latency.

Acceptance criteria:

- operators can configure and inspect time- and size-based retention and disk-usage limits;
- documentation defines whether lagging consumers block deletion, lose replay eligibility, or cause new publishes to be rejected for each supported policy;
- low-space, full-disk, deletion, restart, and interrupted-cleanup tests preserve the documented outcomes;
- metrics and diagnostics expose retained bytes, reclaimable bytes, consumer lag that constrains retention, rejected writes, and cleanup progress;
- sustained workloads demonstrate bounded disk and memory use with predictable foreground tail latency.

### Make durable storage upgrades safe

Goal: let supported broker upgrades preserve or deliberately transform acknowledged data and consumer progress without silent loss.

Rationale: the storage layout now has separate metadata and stream data groups, so future format and placement changes need an explicit recovery and migration contract.

Current progress: unsupported versions, identities, and layouts in existing clustered storage now fail closed before recovery opens groups or mutates authoritative state; empty-directory startup may still initialize `storage.json` by design. The [TD-007 storage compatibility evidence note](design/td-007-storage-compatibility-evidence.md) records the exact read-forward fixtures, refusal tests, and compatibility gates. The [durable storage upgrade policy proposal](design/storage-upgrade-policy.md) records the observed compatibility boundary, while the [storage-upgrade safety plan](design/storage-upgrade-safety-plan.md) provides the proposed per-artifact compatibility matrix, validation rules, bounded side-by-side transfer, activation and writer-fencing invariants, rollback boundary, observability requirements, and end-to-end acceptance matrix. This is a design milestone only: no migration command, generation selector, writer fence, supported downgrade, interrupted-transfer recovery, or rolling-upgrade path is implemented.

Constraints:

- incompatible layouts must fail clearly rather than appear empty or partially recovered;
- migration must preserve the documented durability, ordering, replay, and acknowledgement guarantees;
- upgrade work must remain bounded and observable for large retained streams.

Acceptance criteria:

- the current reopen/read-forward behavior and the proposed supported
  side-by-side migration and unsupported downgrade boundary are documented per
  artifact;
- representative old and new layouts have automated recovery or migration
  tests that preserve logical records, consumer progress, attempts, and
  producer request identity;
- interrupted transfer, activation, restart, and cleanup tests either resume
  from verified bounded progress or leave the source generation usable;
- writer/activation fencing prevents stale publish and acknowledgement
  mutations across cutover;
- storage version, format, identity, migration phase/outcome, progress,
  rollback eligibility, validation failure, and cleanup/orphan state are
  visible through bounded diagnostics and metrics; and
- real-process local and three-node clustered upgrade tests cover mixed
  compatibility, leader/follower recovery, snapshot/replacement boundaries,
  and old-binary refusal after target-only state becomes active.

### Make message encoding and compression evolvable

Goal: reduce storage, network, and CPU overhead with efficient message representations and optional compression while preserving the public stream and delivery model.

Rationale: small messages and long-lived streams make framing, copying, encoding, and compression costs significant parts of Runnel's performance and storage profile.

Current progress: the provisional protocol and reusable client support validated binary-safe payloads through padded base64 while retaining the legacy text path. Current review confirms that peer commands and clustered persistence encode payload `Vec<u8>` values as JSON integer arrays, local `RNL1`/`RNL2`/`RNL3` records remain uncompressed, and public/peer codecs are not negotiated. The [message encoding and compression study](research/message-encoding-and-compression.md) compares compression placement across client batches, public and peer frames, retained blocks, and separate Raft/state-machine artifacts; the [day-one plan](design/encoding-compression-day1-plan.md) keeps the candidate frame and codec work exploratory. Version negotiation, mixed-format recovery, compression, and representative resource measurements remain open.

Constraints:

- the wire and durable formats must be versioned and recoverable across compatible upgrades;
- compression placement and codec must be independently measurable for public wire, peer wire, retained data, and clustered persistence; compression must not silently weaken durability, ordering, replay, or corruption detection;
- bounded memory and predictable tail latency take priority over compression ratio alone;
- format changes must remain independent from any one storage engine or replication architecture.

Acceptance criteria:

- encoding and compression are compared separately across 100 B, 1 KiB, 16 KiB, and 1 MiB payloads that include random, repeated-text, JSON-like, and already-compressed data;
- measurements record logical, envelope, stored, and wire bytes; codec CPU; allocations/copies; peak and concurrent buffer memory; throughput; batch wait; publish/replay p50, p99, and p99.9; and recovery cost under stated local and three-node resource limits;
- each tested codec/placement identifies its version and size/window limits, checksum coverage, and the exact writer, reader, persistence, and forwarding boundaries;
- restart and fault evidence covers old-format plus candidate-format data, incomplete tails, complete corruption, unsupported required metadata, and preservation of logical offsets, request identities, replay, acknowledgement, redelivery, and dead-letter results;
- controlled results document where compression reduces total framed bytes without exceeding the workload's CPU, memory, recovery, and tail-latency budgets, and where an uncompressed path should be selected.

### Make retained-state growth independent of the hot path

Goal: keep publish, consume, recovery, and resource behavior predictable as a stream's retained history grows.

Rationale: a broker cannot meet its throughput, latency, and bounded-memory goals if each new message requires work proportional to all retained state.

Current progress: retained-history benchmarks now cross the bounded local tail index, clustered recovery replays retained data after a real node restart, the opt-in clustered hot-path probe measures durable publishes after a controlled retained-history preload, and clustered snapshot/journal paths avoid several redundant retained-payload copies. The [TD-002 scalability evidence note](design/td-002-storage-scalability-evidence.md) records current local reopen and cold-replay measurements, the one-file invariants, and candidate segmentation/indexing gates. The [TD-009 snapshot evidence note](design/td-009-snapshot-evidence.md) records the clustered snapshot correctness boundary and the missing build/install resource and incremental-transfer evidence. The [TD-010 retained-state evidence note](design/td-010-retained-state-evidence.md) records the clustered materialization, copy, scan, correctness, and measurement boundaries and keeps them distinct from the full-snapshot cost tracked by TD-009. Segmented storage, bounded storage amplification, retention behavior, and a complete growth benchmark matrix remain open.

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

Current progress: local storage work now has bounded asynchronous admission, a blocking-I/O executor, and per-stream lanes that preserve order while allowing unrelated streams to progress. A real-process synthetic-FIFO test demonstrates global admission across distinct streams: 32 operations execute, 32 queue, and excess polls receive prompt `storage_error` responses. Fairness, real device stalls, and latency percentiles remain unmeasured. The [TD-022 storage-executor evidence note](design/td-022-storage-executor-evidence.md) records the current limits and distinguishes this synthetic admission-boundary evidence and released FIFO-stall evidence from missing real-device slow-filesystem and uninterruptible-call measurements. Grouped expiry lookup also avoids scanning all active deliveries. A 2026-09-05 targeted comparison of replacing `sync_all` with `sync_data` for existing local consumer-journal appends passed its replay checks but produced mixed or neutral results across repeated acknowledgement and shared-delivery workloads, so the proposed optimization was rejected in closed PR #222. The broker still has broader serialized state and scheduling boundaries, and predictable concurrent p50/p99/p99.9 evidence remains incomplete.

Constraints:

- ordering is serialized only within the requested ordering domain;
- backpressure and memory bounds remain explicit under contention and slow consumers;
- crash recovery, acknowledgement ordering, and durable outcomes remain unchanged.

Acceptance criteria:

- representative concurrent workloads demonstrate predictable p50, p99, and p99.9 behavior;
- unrelated streams, consumers, and ordering keys can progress independently;
- contention, queueing, and resource-pressure behavior are visible in metrics and repeatable tests;
- improvements are supported by benchmarks rather than assumptions about scheduling or locking.

## Make the single-node deployment ready for real use

Goal: provide the security, resource, health, and observability behavior needed to operate one broker responsibly in a local, container, or small production environment.

Rationale: low operational complexity is a core product promise, so safe defaults and useful signals matter as much as message throughput.

Current progress: the HTTP metrics endpoint exposes bounded broker and
transport telemetry, including request rates and latency buckets, traffic,
admission, health failures, logical storage, delivery outcomes, and a
label-free per-process uptime gauge. Real-server tests verify that uptime
progresses and that this process-level signal remains scrapeable while the
engine health dependency is stalled. A real-process characterization also
shows that when SIGTERM follows a durable publish response reaching a proxy
but precedes delivery to the caller, the caller observes an unknown outcome;
after restart on the same data directory, retrying the stable request ID
returns the original offset and the stream contains one record. This does not
cover shutdown during engine execution or a hard process stop. Consumer lag,
reclaimable storage, resource pressure, and the remaining security and
capacity controls are still open.

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

### Make overload and abusive-client behavior bounded

Goal: keep the broker responsive and explicit when clients create more connections, requests, payload bytes, or outstanding work than the configured deployment can safely serve.

Rationale: predictable resource usage requires admission limits before authentication or ordinary application mistakes can turn unbounded network input into memory exhaustion, runtime starvation, or storage failure.

Current progress: connection, request-size, in-flight-request, and request-timeout limits are configurable and exposed as metrics. Real-server tests cover connection floods, oversized requests, in-flight saturation, slow writers, and incomplete slow readers. A sustained in-flight pressure sequence now verifies explicit active-work, rejection, saturation, and timeout metrics, distinguishes response-write expiry from request execution timeout, and recovers health and durable traffic on persistent connections. The FIFO-backed storage-stall probes bound protocol and readiness work, confirm unrelated durable traffic continues, keep metrics scrapeable while engine health is stalled, and verify that releasing the stall does not let a timed-out same-stream waiter poison the next durable request. A real-process pressure test blocks one operation on its stream lane, fills the bounded per-stream waiter queue, and verifies prompt `storage_error` responses and failure metrics. While the blocked poll holds its stream mutex, readiness and engine health time out because health inspection locks that same stream; process liveness stays healthy and the metrics fallback remains scrapeable. Repeated same-stream requests are rejected promptly, a different stream continues durable publish and poll traffic, and queued polls complete after FIFO release. This exercises per-stream queue exhaustion with a synthetic FIFO stall; global executor saturation, actual slow or full-device pressure, bounded-memory measurements, full resource-pressure recovery, and the complete operational matrix remain open.

Constraints:

- limits must apply before unbounded allocation or task creation;
- rejection and timeout behavior must be visible to clients and operators;
- one slow or malformed connection must not prevent unrelated health checks, shutdown, or durable traffic from progressing;
- defaults must remain convenient for the documented initial workloads.

Acceptance criteria:

- request size, connection count, in-flight work, and relevant queue limits are configurable with safe defaults;
- overload produces documented rejection or backpressure responses rather than silent loss or unbounded growth;
- slow-reader, slow-writer, oversized-request, connection-flood, and storage-stall tests demonstrate bounded memory and recovery;
- metrics distinguish active work, rejected admission, timeouts, and saturation by limiting resource.

## Run a reliable three-node development deployment

Goal: deploy three Runnel nodes with persistent storage and exercise a coherent clustered broker while applications continue to use streams and consumers without learning the node layout.

Rationale: the long-term product needs a credible path to availability and larger workloads. A small repeatable deployment is the right boundary for validating distributed assumptions before expanding operational scope.

Constraints:

- write and acknowledgement guarantees must be stated for node failures and network partitions;
- the cluster must remain safe if the Kubernetes control plane is temporarily unavailable;
- ownership, membership, failover, and stale-state behavior must be correct under crashes and restarts;
- the first clustered deployment is a development milestone, not an automatic promise of production-grade operations.

Acceptance criteria:

- the selected distributed model, failure assumptions, and durability choices are recorded in an ADR before implementation is treated as complete;
- three nodes start with independent persistent storage and can form the documented deployment without application-level topology configuration;
- acknowledged durable data survives the node failures promised by the selected durability mode;
- stale participants cannot make conflicting progress after ownership changes;
- node restart, membership change, and the promised node-failure scenario have repeatable integration tests;
- existing stream, producer, consumer, group, acknowledgement, and replay intent remains usable without exposing physical placement;
- Kubernetes readiness, disruption, persistence, upgrade, and control-plane assumptions are documented beside the deployment artifact.

### Make growth from one node to a cluster non-disruptive

Goal: let an application move its retained streams and durable consumer progress from a supported single-node deployment to a supported cluster without changing its messaging model or silently losing acknowledged state.

Rationale: the promise of a credible path from one node to a distributed system is incomplete if only source compatibility exists and operators must invent a risky data migration.

Current progress: no supported local-to-cluster migration exists. Local stream
logs and consumer state use durable representations that the clustered engine
cannot import. Clustered identity checks reject unsupported or ambiguous
layouts instead of converting them. The [migration boundary design](design/single-node-to-cluster-migration.md)
explores a candidate side-by-side logical export/import. The [storage upgrade safety plan](design/storage-upgrade-safety-plan.md)
records related validation, fencing, rollback, and interruption requirements.

Constraints:

- migration must preserve documented offsets, ordering, replay eligibility, producer retry identity, and consumer progress;
- cutover must have explicit writer fencing and rollback boundaries;
- migration work and additional storage must remain bounded and observable for large retained streams;
- applications must not need to learn Raft groups, replica placement, or storage paths.

Acceptance criteria:

- a documented procedure migrates representative retained data and active consumer state from the local engine to the clustered engine;
- interrupted transfer, failed validation, process restart, and cutover races leave one clearly authoritative serving deployment;
- post-migration conformance tests demonstrate the same public delivery behavior and resolve pre-cutover publish retry identities correctly;
- diagnostics report migration progress, validation failures, fencing state, and rollback availability.

### Make placement scale independently of stream identity

Goal: support many streams and uneven workloads without requiring one permanent distributed processing unit or replica layout per public stream.

Rationale: a small static cluster is useful for correctness testing, but its initial placement shape must not become the scalability boundary for larger deployments.

Current progress: the retained-storage and placement identities are separated in the accepted architecture, while the current implementation still uses a static data group per stream and static voters. Placement movement, splitting, balancing, and failure-safe recovery remain unimplemented.

Constraints:

- applications continue to address streams without learning node, shard, or replica placement;
- movement, splitting, and balancing must preserve ordering, acknowledged durability, replay, and consumer progress;
- the cluster must remain safe and resource-bounded while placement changes are in progress.

Acceptance criteria:

- placement can distribute many streams and hot ordering domains across available capacity;
- placement changes have explicit recovery, fencing, and observability behavior;
- a node or storage failure does not require application-level remapping;
- placement and balancing behavior is measured for idle, uniform, and skewed workloads.

### Explore stable internal work placement

Goal: determine whether stable internal work lanes, virtual shards, or key-affine ownership can improve throughput, tail latency, batching, or cache locality for large consumer pools without becoming public topology.

Rationale: demand-driven delivery is the simplest first model, but larger deployments may benefit from moving a small, stable fraction of work when workers join or leave rather than recalculating all ownership.

The current evidence and alternatives are recorded in [the stable work placement design](design/stable-work-placement.md). It favors retaining demand-driven delivery as the default while evaluating bounded virtual lanes with cooperative, epoch-fenced handoff; no runtime or performance conclusion has been established.

Constraints:

- the public stream and consumer model must not expose lanes, shards, ranges, or worker assignments;
- any approach must preserve at-least-once delivery, scoped ordering, bounded state, and stale-owner fencing;
- ownership changes must remain safe during worker, node, and leader failures;
- the design must be compared with demand-driven delivery using representative skewed and uniform workloads.

Acceptance criteria:

- a documented comparison identifies the workloads where stable placement provides a material benefit or is not worthwhile;
- membership changes have measured movement, recovery, and tail-latency behavior;
- hot keys, uneven worker capacity, and slow consumers have explicit behavior;
- a future optimization can be introduced without changing client programming intent.

### Explore adaptive handling of hot ordering domains

Goal: determine how Runnel should respond when a small number of ordering keys dominate traffic or processing time.

Rationale: per-key ordering permits broad concurrency, but one hot key can still become a throughput or latency bottleneck and can make stable placement decisions misleading.

Constraints:

- ordering guarantees must remain explicit rather than being weakened for performance;
- unrelated keys must continue to make progress when one key is slow or repeatedly failing;
- resource and scheduling behavior must remain observable and bounded.

Acceptance criteria:

- representative hot-key workloads quantify backlog, latency, fairness, and resource usage;
- the project documents which improvements preserve strict key ordering and which require an application-visible tradeoff;
- any selected policy has repeatable failure, retry, and recovery tests.

### Make consumer ownership authoritative

Goal: make shared-consumer progress and ownership durable, transferable, and safe across nodes, concurrent members, crashes, and membership changes.

Rationale: stream placement alone does not make work distribution reliable; consumer state must be recoverable without depending on one process's volatile ownership. The current static cluster now provides an initial replicated baseline, which should be extended and hardened before the cluster gains more flexible placement or membership.

Constraints:

- preserve at-least-once delivery and durable acknowledgement semantics;
- applications must continue to address streams and consumers without managing node placement or rebalancing;
- acknowledged progress must remain durable under the selected replication guarantee;
- stale consumers and members must not continue acknowledging work after ownership changes;
- node, leader, member, and network failures must not permit stale work to commit later progress;
- ordering, retries, replay, dead-letter handling, backpressure, and uncertain outcomes must retain their documented meanings;
- normal consumers must not need to understand internal group placement or consensus terms.

Acceptance criteria:

- grouped delivery, acknowledgement, expiry, and redelivery work through a multi-node deployment;
- consumer progress and ownership survive node restart and the documented failure scenarios;
- ownership changes are fenced and cannot produce conflicting committed progress;
- independent consumers remain independent while members of one consumer share work;
- conformance and failure tests demonstrate no loss of acknowledged progress and document permissible redelivery;
- independent consumers and consumer groups have repeatable crash, retry, and rebalancing tests;
- the public protocol remains free of physical partitions, node assignments, and internal placement concepts.

#### Harden the initial clustered shared-consumer contract

Goal: make shared consumers dependable across the supported static-cluster failure scenarios while keeping their behavior consistent with the local engine.

Rationale: the first replicated implementation provides durable ownership, lease expiry, and stale-delivery fencing, but it is intentionally a narrow semantic baseline rather than the final clustered consumer system.

Constraints:

- acknowledged progress must remain durable under the selected replication guarantee;
- redelivery, acknowledgement races, and uncertain client outcomes must remain explicit;
- independent consumers and shared members must retain their separate meanings;
- the public model must not expose consensus terms, node ownership, or physical placement;
- performance work must preserve bounded state and scoped ordering.

Acceptance criteria:

- the shared-delivery contract runs against both local and clustered engines;
- member, leader, process, and replica-restart scenarios have repeatable tests;
- expiry and stale-ack behavior remains correct after leadership changes;
- clustered retry limits and dead-letter outcomes have a documented cross-engine contract and failure tests;
- remaining policy differences such as backoff, provenance, consumer-scoped configuration, and observability are explicit before expansion;
- a repeatable clustered benchmark and profiling workflow identifies throughput, tail latency, CPU, memory, and recovery behavior under documented workloads;
- representative clustered workloads establish throughput and tail-latency baselines before the delivery scheduler is expanded.

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

Current progress: ADRs 0004 and 0006 establish the initial static Multi-Raft topology, while current real-process tests cover preserved-state follower restart and leader failure. The pinned OpenRaft version includes learner and joint-membership APIs, but Runnel does not use them and has no durable coordinator for membership changes across its metadata and per-stream groups. The [cluster membership evolution research](research/cluster-membership-evolution.md) compares the relevant consensus and broker mechanisms, confirms that a restricted group-level experiment is feasible, and records the cross-group, identity, fencing, and recovery evidence still needed. No membership API or policy is accepted; TD-008 remains open for dynamic membership and production fencing.

### Make missing-replica replacement safe

Goal: recover a node whose local replica state is missing or inconsistent without allowing stale or under-specified state to participate in serving or quorum decisions.

Rationale: snapshot transfer is useful evidence for recovery, but an empty process with a reused voter identity is not yet a defined production lifecycle and can interact badly with Raft log invariants.

Current progress: the default real-process path covers a stopped follower rejoining from its original data directory while configured peers commit, followed by a leader failure. Startup rejects mismatched storage identity and malformed or unsupported persisted state. A separate test-only experiment uses a new empty directory under the same voter ID and permissive recovery; a peer-frame gate holds the response to an accepted non-final `events` data-group snapshot chunk, then the test kills the active leader, restarts the replacement, and verifies selected records and consumer progress after successor-led recovery. It retains repeated transfer interruptions and a later leader failure. This does not establish learner promotion, serving or quorum gates before catch-up, or stale same-ID process fencing. The current evidence and reference comparison are in [Raft follower recovery and replacement](research/raft-recovery-and-replacement.md).

Constraints:

- acknowledged messages and durable consumer progress must remain protected by the selected quorum guarantee;
- replacement must have explicit identity, progress, serving, fencing, and membership semantics;
- the Kubernetes control plane must not be required to preserve correctness;
- recovery cost and failure behavior must be observable and bounded;
- the public streams, records, consumers, and acknowledgement model must not expose replica placement.

Acceptance criteria:

- a documented replacement scenario distinguishes preserved-state restart, temporary outage while peers continue, missing or inconsistent local state, configured identity mismatch, and an old or duplicate process returning with the same node ID;
- storage-identity tests separately reject a configured cluster-name mismatch and a configured node-ID mismatch against the persisted marker; the node-ID regression rejects the mismatch before inspecting group state or rewriting the marker;
- a replacement cannot serve or affect quorum decisions before the cluster has validated its recovered state;
- repeated interruption, restart, and leader failure during replacement preserve acknowledged data and consumer progress;
- process, storage, transport, and consensus failures are distinguishable in tests and diagnostics;
- the behavior is supported by competitor/reference research, an ADR, process-level tests, and recovery benchmarks.

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

Current progress: clustered snapshot lifecycle, peer transport, forwarding, storage, health, and in-flight delivery signals are now visible through existing diagnostics and metrics. Cluster replication progress is exposed as a bounded per-broker aggregate: maximum sampled Raft log-entry lag by numeric peer ID across groups this broker leads, with sampled/total group counts and an explicit unavailable signal when no sampled group is locally led. This is not message or byte lag. The illustrative three-node Kubernetes deployment now has a two-Ready-pod disruption budget for voluntary Eviction API requests, aligned with the static cluster's two-member quorum requirement; Ready status does not itself prove quorum health, and the budget does not cover direct deletion or controller updates and cannot prevent involuntary failures. The [cluster peer transport security research](research/cluster-peer-transport-security.md) records that the current Raft, forwarding, and data-group setup listener uses unauthenticated plain TCP and compares static mutual TLS, workload identity, and network filtering boundaries. This is a research milestone only: peer identity, TLS configuration, credential rotation, version compatibility, failure behavior, and deployment tests remain undecided or unimplemented. Broader cluster leadership, resource pressure, security, upgrade, and deployment-level operational behavior remain incomplete.

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

Current progress: binary-safe payloads, bounded peer control/data capacity, lazy idle-socket expiry, payload-copy reductions, peer-forwarding saturation scenarios, a bounded fixed-total-work peer-forwarding sweep across stream/data-group counts, and publish-batch workload coverage now exist. The internal peer RPC is still unversioned; its JSON schema includes Serde-serialized OpenRaft types, and the development deployment does not support mixed-binary rolling upgrades. The [peer protocol versioning research](research/peer-protocol-versioning.md) records why a wire marker alone would not establish compatibility and the policy, mismatch handling, and real-process upgrade evidence needed before making that promise. Protocol versioning, multiplexing or cluster-scoped transport ownership, transport-strategy comparisons, equivalent end-to-end measurements, and broader failure semantics remain open.

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

Current progress: repeatable clustered workloads now cover ordinary durable publish, retained-history restart/replay, peer-forwarding saturation, opt-in publish batches, and opt-in bootstrap-assumed-leader and non-bootstrap-follower process-stop/public-endpoint-service/restart probes, with aggregate/per-node CPU, resident-memory, storage-byte samples and machine-readable results. The failure results identify public request endpoints and preserve the bootstrap assumption; they do not directly observe an election, local request handling, or replacement leader identity. A sequential matrix runner can repeat relevant payload, concurrency, slow-consumer-delay, retained-history, runtime, and fault cases with run-scoped outputs and bounded case timeouts. The opt-in slow-consumer backpressure probe now verifies the current one-in-flight delivery window and duplicate-pull suppression while recording bounded latency and resource samples; publisher throttling/rejection, complete fault coverage, stable tail-latency evidence, repeated recovery/resource matrices at authoritative scale, and a supported signal for leader identity remain open.

Constraints:

- every result must state topology, durability, storage, batching, message size, and failure state;
- tail latency and resource usage matter alongside throughput;
- failure tests must exercise real process and storage boundaries where practical.

Acceptance criteria:

- benchmarks cover durable publish, publish-to-consume latency, sustained throughput, batching, slow consumers, restart, and recovery;
- results include p50, p99, and p99.9 latency, memory, CPU, and storage usage where applicable;
- an opt-in leader-failure probe exercises real process stop, survivor publish/consume/ack, and same-node restart through the public protocol while recording its leader-selection assumption and recovery evidence;
- a repeatable profiling workflow can produce actionable per-process hot-path evidence for clustered workloads;
- the baseline can be rerun to compare future distributed engines without changing the public workload model.

### Keep clustered commit cost predictable as consensus history grows

Goal: keep the latency and resource cost of durable clustered commits predictable as the unpurged Raft log grows.

Rationale: the current log store serializes and atomically rewrites the retained Raft entry map during log persistence. This can make append work depend on the number and size of entries since the last purge. An isolated measurement now confirms local rewrite-byte and latency growth for larger entries, but does not establish the end-to-end effect under clustered workloads.

Current progress: the [isolated TD-026 persistence baseline](research/td-026-log-store-persistence-baseline.md) exercises the real `LogStore` append-flush and committed-index persistence paths. At 4,096 retained entries, a 1 KiB single-entry append/commit pair took 75.8 ms median and rewrote 35.2 MB of serialized Raft-log JSON, versus 32.6 ms and 8.8 KiB with no retained entries. An opt-in real-process scenario now observes per-node Raft-log entry counts, snapshot builds, purge-index advances, selected persistent file footprints, durable publish throughput/tail latency, and follower restart replay/ack through actual snapshot/purge cycles. Initial single runs at 100 B and 1 KiB payloads each used 256 publishes; sampled peaks were 32 and 40 entries, with 8 retained at completion. These peaks apply only to those workloads and sampling cadence, not as general retention bounds. The observer consumed 16.9% and 32.3% of the measured intervals, and file-size deltas are not bytes-written attribution. Repeated workload and batch-size coverage, less intrusive per-path write attribution, and an accepted supported-workload bound remain open. See the [live and isolated TD-026 evidence](research/td-026-log-store-persistence-baseline.md), [TD-026](tech-debt.md#td-026-raft-log-persistence-rewrites-retained-entries), and the [systems performance research](research/systems-performance-research.md#replicated-logs-and-payload-movement-inspect-cross-layer-writes-before-redesigning-locks).

Constraints:

- preserve Raft append, vote, commit, truncation, purge, and snapshot-recovery guarantees;
- keep consensus history separate from retained broker-message history;
- compare the current path and any candidate with the same topology, workload, resource limits, and durability semantics.

Acceptance criteria:

- clustered benchmarks vary retained Raft-log length, appended batch size, payload size, and purge/snapshot cycle;
- results separate Raft-log serialization and physical bytes written from state-machine journal and snapshot work, and report throughput, p50/p99/p99.9 latency, CPU, and recovery cost;
- measurements establish whether append cost grows with the unpurged log and define the supported workload bound or justify a bounded-cost candidate;
- any selected change preserves acknowledged outcomes and passes truncation, purge, restart, follower-recovery, and snapshot compatibility tests.

### Make cross-broker benchmark comparisons reproducible

Goal: rerun representative Runnel and competing-broker workloads under controlled, documented conditions and compare their results over time.

Rationale: performance leadership is meaningful only when message semantics, durability, resource limits, workload shape, and measurement boundaries are equivalent.

Current progress: comparison results now declare operation-specific acknowledgement, durability, replication, delivery, batching, client, latency, topology, and resource boundaries, reject inconsistent metadata, and mark mismatched comparisons as experimental and non-ranking. The [TD-013 semantics note](research/td-013-native-competitor-semantics.md) records the current native boundaries and a candidate common workload envelope without making a client or protocol decision. A common equivalent client and fully comparable consume, recovery, and resource workloads remain open.

Constraints:

- each broker adapter must state the guarantees it actually measures rather than implying semantic equivalence;
- broker images, client tools, workload definitions, CPU and memory limits, storage assumptions, and host information must be recorded;
- noisy or unsuitable measurements must remain visible as experimental and must not silently gate changes;
- benchmark artifacts must be machine-readable and suitable for later trend reporting.

Acceptance criteria:

- a single documented command can run the supported broker adapters in containers with explicit resource limits;
- the common workload matrix includes message sizes, concurrency, batching, durable publish, consume and acknowledgement, recovery, and resource usage;
- results report throughput, p50, p99, p99.9, CPU efficiency, CPU and memory usage, storage, configuration, and failure state where applicable;
- Kafka, Redpanda, and NATS JetStream comparisons document their acknowledgement, replication, and delivery semantics;
- repeated runs can be compared without manually transcribing results.

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
