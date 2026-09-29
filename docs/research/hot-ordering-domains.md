# Adaptive handling of hot ordering domains

- Status: source-backed exploration; no runtime policy selected
- Last reviewed: 2026-09-29
- Baseline: `f2e7d8ce56c282411148e5519b138cc40b65ca5a`
- Related outcome: [Explore adaptive handling of hot ordering domains](../backlog.md#explore-adaptive-handling-of-hot-ordering-domains)
- Related proposal: [Stable internal work placement](../design/stable-work-placement.md)
- Scope: current shared-consumer ordering, hot-key bottlenecks, and strict-order-preserving options

This note separates the cost of preserving one key's order from the cost of finding work for other keys. It describes the current local and clustered schedulers and proposes evidence needed before selecting an adaptation. It is not an ADR, runtime specification, or performance result.

## Findings at a glance

**Observed:** for one shared consumer, both engines permit at most one unacknowledged delivery per member and per non-null key. A later record with that key stays ineligible until the earlier delivery is acknowledged or expires. Polls can skip that blocked key and deliver unrelated keys, so Runnel does not promise global FIFO across a shared consumer. Keyless records do not use the per-key gate.

**Inference:** if arrivals for one key exceed the rate at which its handler finishes and acknowledges, its backlog grows. More members cannot raise that key's strict serial service rate under the current one-delivery gate. They can still process unrelated keys, subject to the local stream lock, storage path, cluster command path, and available capacity. A hot key can therefore produce two different problems: unavoidable serial work for that key, and avoidable scheduler work repeatedly stepping over its blocked records.

**Recommendation:** first measure these costs separately. Keep one active delivery per key as the baseline. A bounded candidate index or ready-key-head structure is a plausible selector experiment if measurements show scan work is material; it would reduce selection work, not parallelize the key. Stable lanes and reassignment likewise cannot split one strict ordering domain. Increasing per-key handler parallelism requires an application-visible change to the ordering domain; ordered batching could instead amortize per-message overhead while keeping processing serial.

## Current guarantees and implementation

The sources of truth are the engine code and contract tests. The following describes the supplied baseline, not a promise that expired application work cannot overlap after redelivery.

| Concern | Local engine | Static clustered engine |
| --- | --- | --- |
| Delivery and ordering | [`Broker::poll_group`](../../crates/runnel-core/src/broker.rs#L309-L420) returns the member's current delivery first, then asks the log for the earliest eligible record at or after the committed offset. [`record_is_candidate`](../../crates/runnel-core/src/stream_log.rs#L1109-L1123) skips acknowledged or in-flight offsets and keys already in flight for that consumer. The volatile index keeps in-flight offsets and keys in hash sets. | [`apply_group_poll`](../../crates/runnel-raft/src/delivery.rs#L55-L165) returns the requesting member's current delivery first, then walks the materialized message vector from the committed offset. It skips acknowledged/in-flight offsets and any key found in another in-flight delivery for that consumer. |
| Concurrency boundary | Each member has at most one active delivery, indexed by member in [`DeliveryState`](../../crates/runnel-core/src/delivery_state.rs#L64-L83). The key set prevents another member from receiving that same key concurrently. Different keys can be in flight at once. | The replicated consumer state stores in-flight deliveries by offset; polling finds an existing member delivery and applies the same one-active-key rule. State changes occur through the stream data group's committed state-machine commands. |
| Acknowledgements | [`ack_group`](../../crates/runnel-core/src/broker.rs#L432-L495) persists the acknowledgement event before advancing materialized consumer progress. Out-of-order acknowledgements are retained while the committed offset waits for earlier gaps. | [`apply_group_ack`](../../crates/runnel-raft/src/delivery.rs#L308-L392) updates acknowledged offsets and in-flight state in the replicated consumer state; the committed offset waits for earlier gaps. |
| Expiry and retry | Active delivery ownership is process-local. A later poll or acknowledgement expires due entries; attempts and progress are journaled. An expired token is rejected after the delivery is replaced. | Poll and acknowledgement commands advance the replicated lease-clock floor and remove expired in-flight entries before proceeding. Attempts and in-flight ownership are in replicated state; old tokens are fenced after replacement. |
| Selection cost | [`StreamLog::find_candidate`](../../crates/runnel-core/src/stream_log.rs#L563-L601) scans the retained tail from the committed offset, or scans the log from a sparse checkpoint when that offset predates the tail index. The per-consumer in-flight key test is a hash-set lookup. A local poll holds the per-stream mutex across selection, record read, and attempt persistence. | Candidate selection walks `StreamState::messages`; the key gate calls `.any()` over the consumer's in-flight delivery values for each keyed candidate examined. The selected delivery is recorded by the replicated command path. |

The reusable [key-ordering contract](../../crates/runnel-test-support/src/lib.rs#L255-L340) and [local grouped-delivery tests](../../crates/runnel-core/src/lib.rs#L776-L900) cover delivering a different key while the first key is in flight, redelivery after expiry, and ordered same-key progress after acknowledgement. These establish delivery and acknowledgement behavior, not exactly-once application effects. An acknowledgement is the broker's signal that work may advance. If a delivery expires while its old handler is still running, a replacement can start; a token-bearing grouped acknowledgement fences the old delivery from committing later progress, but cannot cancel an external side effect already in progress. The legacy tokenless acknowledgement path is a separate compatibility case and does not provide that token fence.

The clustered scheduler's scan and key lookup costs are code-path observations, not measured CPU profiles. The synchronous broker methods take a per-stream mutex; production `Engine` calls also enter the local [`StorageExecutor`'s per-stream FIFO lane](../../crates/runnel-core/src/lib.rs#L156-L199) before calling them. Direct synchronous broker benchmarks do not include that lane. Clustered grouped polls and acknowledgements are applied through the stream data group's committed command path. These shared execution costs may dominate a particular run, so a hot-key experiment must separate them from time waiting for the previous same-key acknowledgement.

## Bottlenecks to distinguish

1. **Serial service ceiling.** While one delivery for a key is outstanding, no second record for that key is eligible. For a single hot key, the maximum useful rate is therefore bounded by the complete poll, processing, and acknowledgement cycle for that key. This is a consequence of the current contract shape, not a benchmark result. Adding workers only raises capacity for other eligible keys.
2. **Repeated candidate scanning.** The local selector may step past many records for a busy key on each poll. If the committed offset is old, the scan also reads and parses log records from its sparse checkpoint. The clustered selector walks its materialized vector and, for each keyed candidate examined, may search the full active in-flight map for a matching key. A long blocked prefix can thus consume work even when the poll returns a cold-key record or `empty`.
3. **Shared execution serialization.** Local polls and acknowledgements for a stream use the same broker mutex; the production `Engine` path also queues them through the same FIFO storage lane, while direct synchronous broker calls bypass that executor. Clustered polls and acknowledgements pass through the stream data group's replicated command path. These serialize independent-key scheduler operations to some degree; the size of that effect is not established by the current ordering tests.
4. **Retry and lease effects.** A slow or repeatedly failing hot record keeps later same-key records behind it until acknowledgement, expiry, or the configured terminal dead-letter policy. Expiry can increase redelivery and duplicate application work. Any fairness result must include those outcomes and the acknowledgement timeout.

## Strict-order-preserving choices

| Option | What it can improve | Ordering and cost | Runnel fit |
| --- | --- | --- | --- |
| Keep demand-driven per-key gating | No ownership map or rebalance; unrelated keys are eligible around a blocked key. | Preserves current semantics. A hot key remains serial, and repeated scans can be costly. | Current baseline and control case. |
| Bounded admission or producer feedback | Limits durable backlog and resource pressure when a key's offered rate exceeds its serial service rate. | Preserves ordering, but a per-key cap changes publish latency or rejection behavior and may itself require high-cardinality state; a stream-wide cap is simpler but couples unrelated keys. | Possible overload policy only after deciding whether publish waits, rejects, or returns an explicit capacity outcome. It does not raise processing capacity. |
| Bounded ready-key heads or candidate index | Avoid repeatedly scanning records known to be blocked by an active key; expose selector work. | Preserve one active delivery per key and the existing durable ack model. A full per-key index may grow with distinct-key cardinality; a bounded index needs an explicit overflow/fallback scan and recovery rebuild. | Plausible near-term experiment if scan counters show material cost. Do not claim higher hot-key service rate. |
| Fixed virtual lanes with a per-key gate | Bound work-placement state and give unrelated lanes independent queues or owners. | A hot key still occupies one lane; hash collisions can couple cold keys to it. Handoff requires drain/fencing and may redeliver. | Already explored separately in [stable work placement](../design/stable-work-placement.md). It addresses ownership/locality, not same-key parallelism. |
| Dynamically isolate or reassign a hot key | Reduce interference between the hot key and unrelated keys; make capacity and lag visible. | Does not increase serial service rate. An owner change must wait for acknowledgement or expiry and fence stale work. Per-key durable ownership risks high-cardinality state; hash-level ownership has collision spillover. | Defer until workload evidence shows other-key interference and member lifecycle semantics are settled. |
| Ordered same-key batching | Amortize poll/ack and protocol overhead while keeping the batch's records in sequence. | Does not parallelize handler work. Batch failure and partial acknowledgements change redelivery granularity and must prevent later same-key completion from overtaking earlier work. | A future protocol/client experiment, after a measured request-overhead case. The current API returns one record per poll. |
| Split one logical key into subkeys or partitions | Adds parallel execution capacity if the domain can be partitioned. | Weakens ordering across the original key unless an application or downstream sequencer merges results in order; adds application-visible state and failure behavior. | Not an automatic broker optimization. Treat as an explicit application tradeoff, not as strict-order preservation. |

The first index experiment should leave placement and delivery credits unchanged: compare the current scan with a bounded per-key-head candidate structure under the same one-delivery-per-member rule. Before implementation, specify how it handles old replay offsets, out-of-order acknowledgements, keyless records, expiry, dead-lettering, restart/rebuild, and memory limits. A hot-key detector alone is not an adaptation: it only becomes useful when the scheduler can isolate work or enforce a chosen admission policy.

## Reference designs

| Reference | Primary-source behavior | Relevance and difference |
| --- | --- | --- |
| [Apache Pulsar `Key_Shared` docs](https://pulsar.apache.org/docs/4.0.x/concepts-messaging/#preserving-order-of-message-delivery-by-key) and [PIP-379](https://github.com/apache/pulsar/blob/v4.0.0/pip/pip-379.md) | Keys or ordering keys map to consumers using hash ranges or consistent hashing. Pulsar 4.0's `AUTO_SPLIT` mode drains a key/hash before reassignment and exposes draining-hash counts, pending messages, and blocked attempts. PIP-379 explains how coarse blocking during ownership changes could stall unrelated keys and why transition state is bounded by hashes instead of tracking each key. | This is the closest reference for key affinity, safe movement, and observability. Runnel currently has no consumer join/leave protocol or stable key owner; its in-flight key gate already serializes same-key deliveries without such a map. Pulsar's draining logic informs future handoff work, not the steady-state hot-key service ceiling. |
| [Amazon SQS FIFO delivery logic](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html) | `MessageGroupId` defines an explicit ordered group. A group can be delivered in an ordered batch; later requests for that group wait until the batch is deleted or becomes visible again, while other groups progress concurrently. | It makes the serial-group throughput tradeoff concrete and shows batching as overhead amortization rather than parallel execution. Unlike Runnel's optional key on an append-only stream, group IDs are required for FIFO messages and delete/visibility semantics govern the group. |

These systems support the inference that, when a client acknowledges only after processing completes, preserving serial completion order requires one active execution lane per ordering domain or an equivalent sequencing barrier. They do not show that Runnel's current scan or lock costs are significant; those need Runnel-specific evidence.

## Measurements and acceptance gates

The repository already has an opt-in bounded [`hot_ordering` cluster probe](../../scripts/benchmarks/cluster_scenarios.py#L1463-L1625). It preloads an interleaved hot key and cold keys, runs concurrent grouped poll, processing, and acknowledgement workers, verifies per-key delivery and completion order with no same-key processing overlap, and records hot backlog, cold-key progress, request latency, and broker process resource samples. The configured processing delay applies to the hot key. Its preloaded trace does not measure live-ingress admission behavior; it is not a local-versus-cluster comparison, does not count candidate records scanned, and does not test expiry, retries, membership changes, or a selector candidate. The first controlled baseline for this probe is recorded below; it does not claim a performance improvement.

Use the existing probe first, then extend the evidence only where it leaves a decision open:

1. **Separate offered work from service.** Run a single key with arrival rates below, near, and above its acknowledgement-cycle capacity; repeat with interleaved cold keys and uniform, 80/20, and one-key-heavy distributions. Include a burst that changes the hot key over time. Report configured worker count and simultaneously outstanding polls separately.
2. **Attribute selector work.** Record candidates examined and rejected by offset, acknowledgement, or key gates per successful or empty poll. Compare local warm-tail and cold-replay paths with the clustered materialized-vector path. Report blocked-poll attempts, bytes read, and scan CPU separately from message payload I/O, journal syncs, network time, and replicated command time.
3. **Measure impact on other keys.** Record hot-key backlog depth and oldest age; hot-key delivery and acknowledgement rate; unrelated-key completion rate and p50/p99/p99.9 wait; and total throughput, CPU, RSS, storage I/O, and queue bounds. Evaluate fairness against offered demand and per-key lag, not equal throughput per member. Bound top-K key attribution and aggregate the remainder; do not create an unbounded metric label per key.
4. **Compare a candidate fairly.** Keep the public operations, payloads, processing delay, timeout, member count, poll concurrency, storage/cluster resources, and acknowledgement policy equal. Compare candidate scans and the bounded head index first, then the real local engine and three-process cluster. Separate cold-key progress from any same-key throughput claim.
5. **Exercise failure semantics.** With the same-key service deliberately slower than its ack timeout, test redelivery while an old handler is still running, stale acknowledgement rejection, process restart, clustered leader failure, and max-attempt/dead-letter behavior. Assert durable acknowledgments do not regress and per-key processing overlap is either prevented by the harness or explicitly measured as the permitted at-least-once expiry case.

An option is useful only if it preserves the current per-key delivery order and at-least-once recovery semantics, demonstrates unrelated-key progress under a hot or poison key, and stays within an explicit memory/queue budget. If it adds admission control, state the publish-side wait or rejection behavior and measure the backlog it bounds. Report throughput gains only when the same workload shows them; selector improvements alone are evidence about CPU/scan cost, not the hot key's serial processing rate. Follow the sequential, controlled comparison policy in [benchmarking guidance](../benchmarking.md).

## Initial controlled baseline

**Evidence class:** performance. This is an exploratory baseline on the existing probe, not a before/after optimization comparison.

**Source and binary:** the broker source revision was `f2e7d8ce56c282411148e5519b138cc40b65ca5a`. The release binary was built from that clean revision with `cargo build --locked -p runnel-server --release` in the exclusive benchmark lock and a 2 CPU / 2 GiB user scope (`CARGO_BUILD_JOBS=2`); its SHA-256 was `0675f7d8ea35f9e3ba71d5f7464cb54800d992c84bcedc31890be100fcb82123`. Rust and Cargo were `1.97.1` and Python was `3.14.4`.

**Host and isolation:** Linux `rkth-precision5570`, kernel `7.0.0-34-generic`, Intel Core i9-12900H, 20 logical CPUs, 31 GiB RAM. Each run used the native-process three-node cluster and line-delimited JSON public protocol. A single systemd user scope enclosed the benchmark client and all three brokers with `CPUQuota=200%` and `MemoryMax=2G`; the harness result also records these limits. The exclusive benchmark lock was acquired without waiting for each run. No other tests, builds, benchmarks, brokers, or resource-heavy workflows were running during the measurement window.

**Workload and measurement:** three sequential repetitions of `--scenarios hot_ordering`, using its defaults: 64 preloaded hot-key records, four cold keys with eight records each, four grouped workers, 5 ms processing delay for hot-key records, 30 s acknowledgement timeout, and a 60 s scenario deadline. The default payload sizes were 100 B and 1 KiB, so each repetition produced one case at each size. The 96 records were interleaved and preloaded before measurement; preload time is excluded. Throughput and elapsed time cover the concurrent grouped drain, including the configured processing delay. Hot drain is measured from probe-operation start until the last hot-key acknowledgement and overlaps cold-key work; it is not an isolated per-key service-cycle measurement. The injected delay alone means at least 320 ms across the serial 64-message hot key, a nominal 200-message/s client processing ceiling before request and scheduling costs. The measured hot drain also includes polling, acknowledgement, quorum, and runtime scheduling; cold progress is a separate observation. The probe does not apportion any additional time to candidate scans or a specific scheduler cost. Request latency is poll plus acknowledgement time and excludes the configured processing delay. The three-node quorum and normal durable storage remained enabled.

The binary build used the same lock and systemd limits:

```sh
python3 scripts/benchmarks/lock.py --mode exclusive --no-wait -- \
  systemd-run --user --scope --collect --unit=<unique-build-unit> \
  --property=CPUQuota=200% --property=MemoryMax=2G \
  --setenv=CARGO_BUILD_JOBS=2 -- \
  cargo build --locked -p runnel-server --release
```

The run command used the shared lock and one bounded scope per repetition. Set `RUN` to `1`, `2`, then `3` before each invocation; the artifact paths and scope unit are unique for each run:

```sh
python3 scripts/benchmarks/lock.py --mode exclusive --no-wait -- \
  systemd-run --user --scope --collect --unit=runnel-hot-ordering-$RUN-20260929 \
  --property=CPUQuota=200% --property=MemoryMax=2G \
  --setenv=RUNNEL_BENCHMARK_CPU_LIMIT=2 \
  --setenv=RUNNEL_BENCHMARK_MEMORY_LIMIT=2G -- \
  python3 scripts/benchmarks/cluster.py \
  --binary target/release/runnel --runtime process \
  --scenarios hot_ordering \
  --output docs/research/artifacts/hot-ordering-baseline-2026-09-29/run-$RUN.json \
  --log-dir docs/research/artifacts/hot-ordering-baseline-2026-09-29/run-$RUN-logs
```

The committed schema-v2 result copies preserve the measurements, workload, provenance, and broker metric samples. Their generated `command` and backend binary paths are normalized to the repository-relative artifact and binary paths shown above; no measurement fields were changed. The unmodified runner outputs and full driver stdout were also retained locally for review. Driver stdout repeats the complete JSON result, so only the result JSON and distinct broker logs are included here.

| Repetition | Result JSON | Broker logs | Probe cases |
| --- | --- | --- | --- |
| 1 | [run 1](artifacts/hot-ordering-baseline-2026-09-29/run-1.json), 155,523 B | [node logs](artifacts/hot-ordering-baseline-2026-09-29/run-1-logs/), 855 B | 100 B and 1 KiB |
| 2 | [run 2](artifacts/hot-ordering-baseline-2026-09-29/run-2.json), 155,560 B | [node logs](artifacts/hot-ordering-baseline-2026-09-29/run-2-logs/), 855 B | 100 B and 1 KiB |
| 3 | [run 3](artifacts/hot-ordering-baseline-2026-09-29/run-3.json), 155,569 B | [node logs](artifacts/hot-ordering-baseline-2026-09-29/run-3-logs/), 855 B | 100 B and 1 KiB |

**Per-run performance and progress:** overall poll-plus-ack percentiles are milliseconds.

| Payload | Run | Total throughput (msg/s) | Overall poll+ack p50 / p99 / p99.9 (ms) | Hot-key p50 / p99 (ms) | Hot drain (ms) | Hot backlog at first / last cold completion | Cold completed before hot drain | Ordering invariant |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100 B | 1 | 206.45 | 1.546 / 10.656 / 11.736 | 1.691 / 11.060 | 462.379 | 64 / 62 | 32 of 32; 4 of 4 keys | pass |
| 100 B | 2 | 205.46 | 1.680 / 19.218 / 22.898 | 1.734 / 20.596 | 465.317 | 64 / 62 | 32 of 32; 4 of 4 keys | pass |
| 100 B | 3 | 191.12 | 1.627 / 16.424 / 20.705 | 1.979 / 18.026 | 497.530 | 64 / 62 | 32 of 32; 4 of 4 keys | pass |
| 1 KiB | 1 | 191.86 | 1.448 / 21.611 / 25.248 | 1.520 / 22.972 | 499.194 | 64 / 62 | 32 of 32; 4 of 4 keys | pass |
| 1 KiB | 2 | 178.17 | 1.774 / 31.422 / 36.191 | 1.695 / 33.207 | 536.688 | 64 / 62 | 32 of 32; 4 of 4 keys | pass |
| 1 KiB | 3 | 178.10 | 2.033 / 31.494 / 34.689 | 1.825 / 32.690 | 537.308 | 64 / 61 | 32 of 32; 4 of 4 keys | pass |

The overall p99.9 values come from the runner's `scenarios[].latency_microseconds.p999` over 96 poll-plus-ack samples per case. This quantile is especially coarse and unstable with 96 samples and only three repetitions; it is descriptive and has no probe-specific stability threshold. The per-key metadata reports p99 only, so no per-key p99.9 is inferred or reported.

**Per-run cold-key latency and completion spread:** latency ranges are the four cold keys' per-key poll-plus-ack percentiles. Each cold key has only eight samples. First/last spread is the difference between cold keys' first/last acknowledgement-completion times.

| Payload | Run | Cold per-key p50 range (ms) | Cold per-key p99 range (ms) | First / last completion spread (ms) |
| --- | ---: | ---: | ---: | ---: |
| 100 B | 1 | 0.990–1.173 | 1.525–2.077 | 1.880 / 0.852 |
| 100 B | 2 | 1.277–1.657 | 1.890–2.858 | 1.821 / 0.645 |
| 100 B | 3 | 0.845–1.186 | 1.480–1.855 | 0.987 / 0.964 |
| 1 KiB | 1 | 1.238–1.438 | 1.748–2.080 | 1.164 / 1.119 |
| 1 KiB | 2 | 1.638–2.131 | 2.838–3.538 | 2.267 / 1.782 |
| 1 KiB | 3 | 2.013–2.868 | 4.516–6.286 | 2.113 / 1.787 |

**Per-run resource samples:** broker CPU is summed across the three server processes during the measured drain. RSS and data-directory size are aggregate across those same broker processes and directories; the sampler does not include benchmark-client CPU or memory. The enclosing cgroup does include the client. Memory and storage summaries are sampled, not I/O bandwidth or bytes written.

| Payload | Run | Broker CPU (s) | RSS average / maximum (MiB) | Data directories average / maximum (MiB) |
| --- | ---: | ---: | ---: | ---: |
| 100 B | 1 | 0.83 | 33.93 / 34.54 | 0.407 / 0.414 |
| 100 B | 2 | 0.83 | 33.81 / 34.36 | 0.415 / 0.455 |
| 100 B | 3 | 0.89 | 33.66 / 34.27 | 0.416 / 0.467 |
| 1 KiB | 1 | 0.93 | 49.24 / 50.64 | 3.879 / 3.904 |
| 1 KiB | 2 | 1.00 | 49.73 / 50.45 | 3.767 / 4.198 |
| 1 KiB | 3 | 1.03 | 48.66 / 50.37 | 3.929 / 3.941 |

Across the three repetitions, the 100 B case had median throughput **205.46 msg/s** (observed range 191.12–206.45), combined p50/p99 request latency **1.627 / 16.424 ms** (p50 range 1.546–1.680; p99 range 10.656–19.218), overall p99.9 **20.705 ms** (11.736–22.898), and hot-key drain time **465.317 ms** (462.379–497.530). The 1 KiB case had median throughput **178.17 msg/s** (178.10–191.86), combined p50/p99 **1.774 / 31.422 ms** (p50 range 1.448–2.033; p99 range 21.611–31.494), overall p99.9 **34.689 ms** (25.248–36.191), and hot-key drain time **536.688 ms** (499.194–537.308). The hot backlog peaked at 64 in every case and remained 64 at the first cold completion; it was 62 at the last cold completion in five cases and 61 in one. All 32 cold records across all four cold keys completed before the hot backlog drained in all six cases.

Hot-key request p50/p99 medians (observed range) were 1.734 ms (1.691–1.979) / 18.026 ms (11.060–20.596) for 100 B, and 1.695 ms (1.520–1.825) / 32.690 ms (22.972–33.207) for 1 KiB. Per-key cold request p50 ranged from 0.845–1.657 ms for 100 B and 1.238–2.868 ms for 1 KiB across the four keys and three repetitions; corresponding per-key p99 values ranged from 1.480–2.858 ms and 1.748–6.286 ms. The first-completion spread across cold keys had median/range 1.821 ms (0.987–1.880) for 100 B and 2.113 ms (1.164–2.267) for 1 KiB. Last-completion spread was 0.852 ms (0.645–0.964) and 1.782 ms (1.119–1.787), respectively.

Broker resource medians (observed range) were: at 100 B, CPU 0.83 s (0.83–0.89), RSS average 33.81 MiB (33.66–33.93) and maximum 34.36 MiB (34.27–34.54), and data-directory average 0.415 MiB (0.407–0.416) and maximum 0.455 MiB (0.414–0.467). At 1 KiB, CPU was 1.00 s (0.93–1.03), RSS average 49.24 MiB (48.66–49.73) and maximum 50.45 MiB (50.37–50.64), and data-directory average 3.879 MiB (3.767–3.929) and maximum 3.941 MiB (3.904–4.198).

**Built-in checks and limits:** all six cases completed 96/96 acknowledgements with no redelivery, verified exact per-key delivery and completion order, and observed no same-key processing overlap. The client-side maximum processing concurrency observed was two messages. The host remained quiet under the process check and exclusive lock. The resource sampler runs at 100 ms intervals; this short probe supplied only a few samples per case. The three repetitions are descriptive, not a confidence interval or stability test. The p99 ranges, especially for the 1 KiB case, show run-to-run variation; there is no probe-specific stability threshold to classify them as stable. The result says unrelated keys progressed in this preloaded trace, but it does not measure backlog age, live-ingress admission, selector candidates scanned, or the source of request latency. It does not attribute cost to candidate scanning or establish an optimization effect. Runtime tests were not run because no broker code or behavior changed; the probe's built-in invariants are the relevant runtime evidence for this research update.

## Disposition

The backlog item remains open. This first controlled baseline quantifies one preloaded hot/cold distribution at two payload sizes and shows cold-key progress while the hot backlog remains. It does not cover offered-rate distributions, changing hot keys, backlog age, local-versus-cluster effects, candidate-scan counts, retries, expiry, or recovery, so it does not complete the representative workload or policy-evaluation criteria. No tech-debt item is added: this probe does not show that candidate scanning is harmful or attribute measured latency or CPU to it. Revisit that assessment when selector instrumentation or a controlled candidate comparison supplies such evidence. No ADR is warranted because no runtime policy or ordering consequence was selected. No implementation refactor is indicated by this research-only update.
