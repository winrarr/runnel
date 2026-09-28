# Systems performance research for Runnel

- Status: source-backed assessment; no runtime change or architecture decision accepted
- Last reviewed: 2026-09-28
- Repository baseline: `4f10777d155d529c6a93f855b4f36c6c58519cc8` (`origin/main`)
- Scope: durable-write batching, queueing and tail latency, replicated-log write amplification, cache-line contention, storage and network I/O

This note extends the [distributed architecture](distributed-architecture-options.md), [encoding and compression](message-encoding-and-compression.md), and existing TD-002, TD-010, TD-011, TD-019, and TD-022 evidence. It compares established systems work with recent research and maps the useful mechanisms to Runnel's current code. It records research and measurement hypotheses only; it does not select a new executor, storage format, durability mode, or replication engine.

## Assessment

Runnel could benefit substantially from more performance work, but the strongest candidates are currently about reducing repeated durable work and controlling queues, not replacing every mutex with lock-free code. The local path synchronizes each successful publish, delivery attempt, and acknowledgement. The clustered path has separate consensus-log and state-machine-journal writes, while the public engine's default batch implementation calls single-record publish sequentially. These are concrete costs with direct paths to throughput and latency. Their benefit is workload-dependent and not yet isolated by the existing measurements.

The [Go pipeline article](https://dev.to/deepkpat/from-mutex-to-lock-free-how-a-few-cache-line-decisions-made-a-go-pipeline-4x-faster-5b11) that prompted this exploration is useful as a checklist of possible costs—lock scope, scheduling, copies, encoding, batching, and cache lines. Its reported speedup is not an estimate for Runnel: the example changes several things together and does not exercise Runnel's durable local sync or replicated commit-and-apply boundary. Runnel's per-stream serialization also protects offset assignment and FIFO append order. Replacing that mutex alone would not remove the required ordering or persistence work.

## Current Runnel paths checked at this baseline

- **Local publish:** `Broker::publish_with_request_id` takes the stream mutex and appends a frame. A single append calls `sync_data` before success. `append_batch` writes records in order and calls `sync_data` once after all valid appends; outcomes remain per record and the batch is not atomic. See [`broker.rs`](../../crates/runnel-core/src/broker.rs) and [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs).
- **Local consumer state:** successful delivery-attempt and acknowledgement events append to a consumer journal and call `sync_all` before the message or progress transition is returned. The bounded async executor keeps the synchronous operation off Tokio's core threads, but it does not combine syncs or make a started filesystem call cancellable. See [delivery bookkeeping evidence](../design/td-019-delivery-bookkeeping.md) and [storage executor evidence](../design/td-022-storage-executor-evidence.md).
- **Clustered batching:** `Engine::publish_batch` defaults to sequential calls to `publish`; `PersistentEngine` does not override it. A client batch therefore does not itself mean one Runnel `client_write`, one Raft append, or one apply batch. OpenRaft may batch internal work independently; its actual batch sizes and durable cost need measurement.
- **Clustered persistence:** each `LogStore::append` adds entries to an in-memory map and atomically persists the full currently retained Raft-log map. `StateMachineStore::apply` then serializes the same committed entries into a framed state-machine journal and syncs it before applying them. A publish payload also enters the retained `StoredMessage` state and later full snapshots. Raft-log retention is compacted, so this is not a claim of unbounded growth; it is a candidate for repeated payload encoding and writes across durability layers. See [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs) and [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs). The [TD-019 clustered evidence](../design/td-019-delivery-bookkeeping.md#clustered-engine) already distinguishes Raft-log append grouping from state-machine apply grouping.
- **Copies and encoding:** the clustered `PersistentEngine::publish` builds a forwarding operation with a cloned payload before trying the local publish path, including when this node is already leader. The reusable client's batch path serializes the JSON/base64 request once to check its encoded size and serializes it again when writing the request. These are code-inspection findings, not measured bottlenecks; their likely importance rises with payload size and rate. See [`engine.rs`](../../crates/runnel-raft/src/engine.rs) and [`client.rs`](../../crates/runnel-client/src/lib.rs).
- **Bounded execution:** local async storage uses a global 32-operation execution limit, a 32-operation queue, FIFO per-stream lanes, and a separate bounded pool for stream waiters. Same-stream work is serialized; unrelated streams can progress when execution permits are available. Existing overload and slow-storage evidence covers important safety behavior, but queue wait, per-stream lane wait, and service-time contributions are not yet a complete deployment-grade latency breakdown.

## Research findings and what transfers

### Durable batching: amortize barriers without moving the success boundary

The classic [Aether logging study](https://www.vldb.org/pvldb/vol3/R61.pdf) treats small I/O, lock hold time while waiting for flush, scheduler overhead, and contention on log structures as separate bottlenecks. Its central lesson is that logging performance is a whole-path problem: no single lock or syscall explains every workload. PostgreSQL's [WAL configuration guide](https://www.postgresql.org/docs/18/wal-configuration.html) documents group commit and its latency trade-off: a short commit delay can let more transactions share a flush, but an excessive delay can reduce throughput, and the useful setting depends on measured flush cost and concurrent arrivals.

A new June 2026 [group-commit preprint](https://arxiv.org/abs/2606.18187) adds a useful distinction between closed-loop clients, which issue their next operation after a response, and open-loop arrivals that continue offering work independently. It reports that in its closed-loop model a pipelined flush can self-clock near the device limit, while waiting policies can matter more under low or shifting open-loop load. The authors explicitly say one theoretical bound is conjectured and the work is a characterization, not a new logger; treat its result as a hypothesis rather than a rule for Runnel.

**Runnel inference:** a record-count batch, a client publish batch, a queue of concurrent single-record publishes, and a consensus append batch are distinct things. The local publish batch already amortizes one data sync. The next useful comparison is whether a bounded server-side queue can combine concurrent publishes or delivery events while retaining each record's ordered result and withholding every success until the corresponding durable boundary completes. Batch count/bytes and a maximum wait must both be bounded. Tests and reports need a response-loss case because a failed sync or lost response can leave an unknown result even when several records were written together. This maps to [TD-019](../design/td-019-delivery-bookkeeping.md), [TD-022](../design/td-022-storage-executor-evidence.md), and the existing [message batching backlog](../backlog.md#make-message-processing-complete).

### Queueing and tail latency: bounded is necessary, then measure where time waits

[SEDA](https://cs.uwaterloo.ca/~brecht/servers/readings-new/seda-sosp01.pdf) introduced explicit queues between service stages, with admission, batching, and load-shedding controls to keep stages inside their resource limits. [The Tail at Scale](https://research.google/pubs/the-tail-at-scale/) explains why rare delays in shared resources and background work become visible in end-to-end tails. These ideas support Runnel's existing bounded admission and per-stream FIFO lanes; they do not imply that a particular queue depth or scheduler is optimal.

The more specialized [Shenango](https://www.usenix.org/system/files/nsdi19-ousterhout.pdf) and [Caladan](https://www.usenix.org/system/files/osdi20-fried.pdf) studies show that dynamic CPU allocation and interference-aware placement can improve utilization and tail latency in highly controlled, latency-sensitive datacenter workloads. Their dedicated scheduler cores, kernel support, and microsecond-scale assumptions are not an obvious fit for Runnel's current deployment promise. They are useful evidence for a future CPU-pressure experiment, not justification to adopt their runtime architecture.

**Runnel inference:** current queue bounds prevent unlimited work growth, but a bounded queue can still have poor p99 if it sits near saturation or if one hot stream consumes shared waiter capacity. Future end-to-end evidence should distinguish connection/request admission, global execution wait, per-stream lane wait, stream-lock hold, encoding, disk write/sync, consensus replication, state-machine journal sync, and response transmission where instrumentation permits. Include both same-stream and many-stream workloads, slow consumers, and resource pressure. This supports the existing work in [TD-022](../design/td-022-storage-executor-evidence.md), [TD-023](../tech-debt.md#td-023-external-protocol-admission-remains-incomplete), and [TD-011](../tech-debt.md#td-011-end-to-end-benchmark-coverage-is-incomplete).

### Replicated logs and payload movement: inspect cross-layer writes before redesigning locks

The 2026 NSDI paper [XLL: Cross-Layer Logging for Data Deduplication in Consensus-Based Storage](https://www.usenix.org/conference/nsdi26/presentation/shawger) identifies duplicate payload persistence between consensus logging and a local state store. In its TiKV prototype, a shared append log and value references reduced write amplification and increased write throughput. The reported gains belong to that TiKV workload and design; the paper also requires a recovery protocol that coordinates the shared log with the consensus state machine.

The recent [BtrLog preprint](https://arxiv.org/abs/2606.27051) explores a different cloud setting: quorum-replicated SSD log nodes for low-latency durable append, followed by asynchronous large-segment archival to object storage. It assumes a common single-writer architecture and ephemeral local cloud storage. This is a useful frontier reference for Runnel's longer-term storage and placement explorations, not a drop-in alternative to current Multi-Raft semantics.

**Runnel inference:** the current clustered log entry and state-machine apply journal both encode the committed command, including a publish payload, before the payload is retained in state. This makes cross-layer write amplification a concrete measurement question that is more specific than the existing retained-vector and snapshot concerns. First quantify encoded bytes, sync calls, bytes rewritten in the retained Raft log, and payload copies per committed record across payload sizes and snapshot/purge cycles. Only then consider references, key/value separation, or log sharing; any such change would need to preserve quorum commit, replay, deduplication, crash recovery, and snapshot replacement. This belongs alongside [TD-010](../design/td-010-retained-state-evidence.md), [TD-009](../design/td-009-snapshot-evidence.md), and [TD-019](../design/td-019-delivery-bookkeeping.md), not in a new accepted design yet.

### Background storage work: keep foreground tails visible during compaction and cleanup

The [SILK study](https://www.usenix.org/conference/atc19/presentation/balmau) found that LSM compaction and flush work can interfere with client operations and worsen p99 even when throughput-focused changes look favorable. Its exact LSM mechanisms do not describe Runnel's current append-only files. The lesson applies to Runnel's future cleanup, compaction, segment migration, snapshotting, and repair work: a storage-capacity or throughput result is incomplete without concurrent foreground-tail and recovery measurements.

Runnel's [retention and disk-pressure plan](../design/retention-disk-pressure-plan.md) already treats cleanup as bounded work and separates logical expiry from physical deletion. Add concurrent publish, replay, and consumer tests to the evidence gate when cleanup or compaction is implemented; do not infer that deleting immutable segments is free because it does not rewrite their contents.

### Cache lines and lock-free code: profile the hardware symptom

False sharing is a real multicore cost, but it is different from lock contention. The Linux kernel's [false-sharing guide](https://docs.kernel.org/kernel-hacking/false-sharing.html) recommends finding a CPU hotspot first, then using `perf c2c` and structure-layout tools to identify contended cache lines. The [`perf c2c` manual](https://man7.org/linux/man-pages/man1/perf-c2c.1.html) documents the hardware support and sampling limits. Padding can help when independent hot fields actually share a line, but it increases memory use and the appropriate cache-line assumption varies across architectures; the [`CachePadded` documentation](https://docs.rs/crossbeam/latest/crossbeam/utils/struct.CachePadded.html) makes that variability explicit.

**Runnel inference:** no evidence found in the current project docs or benchmark artifacts shows that a Runnel mutex, atomic metric, or cache line is a leading bottleneck. Preserve the per-stream lock until profiling separates wait time from required sync time. If multicore CPU profiles show a plausible shared-data hotspot, add a focused `perf lock` or `perf c2c` run on the same controlled workload before padding counters, sharding state, or adopting a lock-free queue. The existing optional [`perf` workflow](../testing.md) is a starting point for CPU hotspots, not proof of cache-line contention.

### Linux I/O and zero-copy: selective mechanisms beat blanket rewrites

The 2026 PVLDB study [io_uring for High-Performance DBMSs: When and How to Use It](https://www.informatik.tu-darmstadt.de/media/systems/pdf_publications/iouring_vldb.pdf) reports that replacing an existing API with `io_uring` alone produced modest results in its test cases, while designs that used its batching, asynchronous overlap, and registered-buffer features did substantially better. The same paper reports no gain for its synchronous one-I/O-at-a-time storage path because device latency remained dominant. A September 2026 [Oracle Database preprint](https://arxiv.org/abs/2609.22781) independently describes a hybrid choice: use `io_uring` for asynchronous batched paths, retain traditional calls for synchronous paths, and fall back when needed. It is a new preprint, not a settled Linux rule.

The Linux kernel's current [`io_uring` zero-copy receive guide](https://docs.kernel.org/networking/iou-zcrx.html) also requires NIC features and out-of-band queue/flow configuration. It is not a generic switch for every TCP connection. Likewise, [Fast ACS](https://research.google/pubs/fast-acs-low-latency-file-based-ordered-message-delivery-at-scale/), a 2025 production message-delivery system, uses RPC across clusters and RDMA inside clusters to scale consumer traffic; its hardware, product, and scale differ materially from Runnel's current three-process development profile.

**Runnel inference:** Runnel currently sends synchronous filesystem work through a bounded `spawn_blocking` executor, and an acknowledged local publish waits for `sync_data`. Swapping the call to `io_uring` will not by itself eliminate the storage barrier or reduce quorum round trips. Evaluate it only if profiling shows avoidable submission overhead, I/O-thread starvation, or a workload with enough independent in-flight I/O to benefit from batching. Large-message copy reduction and network zero-copy should be separate experiments because small messages may be dominated by encoding, consensus, and sync latency instead.

## Opportunity assessment and evidence order

| Opportunity | Potential relevance to Runnel | Main risk or unknown | Current record |
| --- | --- | --- | --- |
| Amortize local publish, delivery, and acknowledgement syncs with bounded server-side batching | High when many operations share a stream and durability sync dominates | Added queue wait, memory bounds, partial outcomes, and unknown results after response loss | TD-019, TD-022, batching backlog |
| Measure clustered cross-layer payload writes and full retained-log rewrites | Potentially high for larger payloads or frequent commits; no Runnel attribution exists yet | Shared log/value references change recovery, compaction, deduplication, and snapshot invariants | TD-009, TD-010, TD-019, TD-026; clustered commit-cost backlog outcome |
| Improve queue visibility and fairness under hot-stream and resource pressure | High for predictable p99 and explicit overload behavior | More metrics do not themselves improve capacity; global bounds may still couple unrelated work | TD-022, TD-023, TD-011 |
| Remove avoidable payload/encoding copies | Plausible for large payloads and high message rates | Copies may be small beside JSON parsing, fsync, or quorum latency | Encoding/compression study; code findings above |
| Change mutexes, pad atomics, or use lock-free structures | Conditional on measured CPU or cache-line contention | More complex memory ordering, ownership, and memory footprint without fixing durable serialization | No current bottleneck evidence; profile first |
| Adopt `io_uring`, RDMA, or network zero-copy | Possible for high-throughput, large-payload, hardware-specific workloads | Platform dependence and integration cost; synchronous durability remains | Defer until profiling identifies the matching bottleneck |

The first comparison should separate serial and concurrent producers, one hot stream and many independent streams, and local versus three-node durability. Record message bytes, queue depth, batch count/bytes/wait, storage syncs, Raft entries and bytes, state-machine apply sizes, recovery time, throughput, and p50/p95/p99/p99.9 latency. Include slow consumers and maintenance pressure in separate named cases. Follow the [benchmarking evidence policy](../benchmarking.md): keep durability semantics fixed, use controlled resources, preserve raw results, and label results inconclusive when the comparison is unstable.

## Planning disposition

Most findings reinforce existing outcomes and technical-debt items; they do not justify an ADR or separate work items for cache-line, lock-free, `io_uring`, RDMA, or zero-copy experiments. The full retained Raft-log rewrite is a distinct current persistence gap, so [TD-026](../tech-debt.md#td-026-raft-log-persistence-rewrites-retained-entries) and a focused [clustered commit-cost backlog outcome](../backlog.md#keep-clustered-commit-cost-predictable-as-consensus-history-grows) now track measurement and bounded-cost criteria. Prioritize evidence for local durable batching, clustered cross-layer writes, and queue-stage latency. Any implementation that changes which operations are durable, their ordering, or what a timeout means must first update the relevant design and accepted decision record.

## Sources

### Established systems work and operational references

- [SEDA: An Architecture for Well-Conditioned, Scalable Internet Services (SOSP 2001)](https://cs.uwaterloo.ca/~brecht/servers/readings-new/seda-sosp01.pdf)
- [Aether: A Scalable Approach to Logging (PVLDB 2010)](https://www.vldb.org/pvldb/vol3/R61.pdf)
- [The Tail at Scale (Dean and Barroso, CACM 2013)](https://research.google/pubs/the-tail-at-scale/)
- [PostgreSQL WAL configuration and group commit](https://www.postgresql.org/docs/18/wal-configuration.html)
- [Shenango (NSDI 2019)](https://www.usenix.org/system/files/nsdi19-ousterhout.pdf)
- [SILK (USENIX ATC 2019)](https://www.usenix.org/conference/atc19/presentation/balmau)
- [Caladan (OSDI 2020)](https://www.usenix.org/system/files/osdi20-fried.pdf)
- [Linux kernel false-sharing guide](https://docs.kernel.org/kernel-hacking/false-sharing.html)
- [`perf c2c` manual](https://man7.org/linux/man-pages/man1/perf-c2c.1.html)
- [Crossbeam `CachePadded` documentation](https://docs.rs/crossbeam/latest/crossbeam/utils/struct.CachePadded.html)

### Recent and emerging systems research

- [XLL: Cross-Layer Logging for Data Deduplication in Consensus-Based Storage (NSDI 2026)](https://www.usenix.org/conference/nsdi26/presentation/shawger)
- [io_uring for High-Performance DBMSs: When and How to Use It (PVLDB 2026)](https://www.informatik.tu-darmstadt.de/media/systems/pdf_publications/iouring_vldb.pdf)
- [Fast ACS: Low-Latency File-Based Ordered Message Delivery at Scale (USENIX ATC 2025)](https://research.google/pubs/fast-acs-low-latency-file-based-ordered-message-delivery-at-scale/)
- [Group Commit Self-Clocks (June 2026 preprint)](https://arxiv.org/abs/2606.18187) — preliminary analysis; one stated bound is conjectured.
- [BtrLog: Low-Latency Logging for Cloud Database Systems (June 2026 preprint)](https://arxiv.org/abs/2606.27051) — single-writer cloud WAL assumptions differ from Runnel's current engine.
- [io_uring in Oracle Database: A Hybrid Storage I/O Architecture at Production Scale (September 2026 preprint)](https://arxiv.org/abs/2609.22781) — new preprint, not a settled implementation recommendation.
- [Linux `io_uring` zero-copy receive documentation](https://docs.kernel.org/networking/iou-zcrx.html)

### Runnel records connected to this assessment

- [Delivery bookkeeping and bounded durability batching](../design/td-019-delivery-bookkeeping.md)
- [Clustered retained-state materialization evidence](../design/td-010-retained-state-evidence.md)
- [Local durable-I/O isolation evidence](../design/td-022-storage-executor-evidence.md)
- [Retention and disk-pressure plan](../design/retention-disk-pressure-plan.md)
- [Clustered commit-cost backlog outcome](../backlog.md#keep-clustered-commit-cost-predictable-as-consensus-history-grows)
- [TD-026 Raft log persistence evidence](../tech-debt.md#td-026-raft-log-persistence-rewrites-retained-entries)
- [Message encoding and compression study](message-encoding-and-compression.md)
- [Benchmarking and performance evidence](../benchmarking.md)
