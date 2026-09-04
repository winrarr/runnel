# Benchmark framework

## Concurrent local workflows

The benchmark commands are safe to run concurrently through the repository's
isolation runner:

```text
just isolated bench-container-smoke
just isolated bench-cluster-smoke
just isolated bench-cluster-container-smoke
```

Each run receives a unique Cargo target directory, temporary directory, output
directory, and Docker network. Use the normal `just bench-*` commands for
single-run investigation or authoritative performance measurements; use
`just isolated bench-*` when the goal is to keep independent development runs
from sharing local state.

The repeatable local container benchmark is:

```text
just bench-container
```

It builds `runnel:bench`, starts one broker container, applies explicit CPU and memory limits, runs durable publish, concurrent publish, consume-and-acknowledge, publish/consume/acknowledge round-trip, and restart-recovery scenarios for 100-byte and 1-KiB payloads, then writes a JSON result under `benchmark-results/`.

The limits and workload can be changed without editing the runner:

```text
python3 scripts/benchmarks/run.py \
  --image runnel:bench \
  --cpus 2 \
  --memory 1g \
  --messages 10000 \
  --concurrency 8 \
  --payload-sizes 100,1024
```

Every raw runner emits the same version-2 result envelope. It records a unique run ID, exact command, full source and host provenance, explicit status and timestamps, workload and resource limits, target/runtime metadata, startup time, scenario-scoped cgroup CPU time, CPU efficiency, sampled resources, throughput, p50/p99/p99.9/max latency, logical payload throughput, and bounded deltas from the broker's `/metrics` endpoint where available. The single-node result stores its target under `backends.runnel`, just like clustered and comparison results.

Use `--scenarios` to run only the named scenarios when a benchmark consumer needs a smaller, explicit workload. The accepted names are `durable_publish`, `concurrent_publish`, `consume_ack`, `publish_consume_ack_roundtrip`, and `restart_recovery`; the default runs all of them. The native comparison adapter selects only `durable_publish` and `consume_ack`, because those are the scenarios it retains from the Runnel result.

This is an end-to-end benchmark of the current development protocol. It is not yet a fair Kafka, Redpanda, or NATS JetStream comparison: those brokers require adapters that express equivalent acknowledgement, replication, ordering, and delivery guarantees. The comparison work belongs in the benchmark backlog. Do not compare raw numbers across brokers until the adapter semantics and environment are recorded in the result.

The short `just bench-container-smoke` recipe is used by CI to verify that the image can start, accept workload traffic under limits, expose metrics, and recover an unacknowledged message. It is a workflow check, not a performance gate.

## Clustered baseline

Run the real three-node clustered baseline with:

```text
just bench-cluster
```

The runner defaults to native broker processes with independent durable directories and exercises the public protocol through multiple nodes. It measures durable publish, non-grouped consume/acknowledge, a bounded slow-consumer backlog drain, sequential shared-consumer delivery, parallel shared-consumer delivery, restart recovery, and retained-data recovery for the selected payload sizes. Each result records the node count, acknowledgement timeout, quorum durability boundary, protocol boundary, workload, throughput, p50/p99/p99.9 latency, aggregate and per-node broker CPU time, resident memory, and on-disk storage samples. Results use the same `backends` shape as the single-node container and comparison runners, so they can be normalized into the existing history dashboard.

The opt-in `publish_batch` scenario measures the clustered public `publish_batch` protocol path, which is not part of the default workload. Setup creates the stream and publishes the warmup records outside the measured interval. Measured requests contain up to 32 records by default, rotate persistent clients across the cluster nodes, and validate one published outcome and contiguous offset for every input record. Set `--batch-size` from 1 through the protocol's 1,024-record limit. The result counts records for throughput and uses one latency sample per batch request, recording `batch_size`, batch count, outcome validation, setup exclusion, and the latency scope in scenario metadata. This is a clustered batching baseline, not evidence that the current engine commits a batch atomically or that it performs one consensus append per request; compare only runs with matching batch size, payload, message count, topology, runtime, and resource limits.

Run the same three-node workload with one bounded Docker container per broker:

```text
just bench-cluster-container
```

This selects `--runtime container`, applies the `--cpus` and `--memory` limits to each broker, keeps the benchmark client on the host, and connects the brokers over a private Docker network. The containerized and native modes share the workload and result schema, but their numbers are separate measurements because process scheduling, networking, filesystem mounts, and resource boundaries differ. Use `--runtime container` directly with `cluster.py` to select a custom image, workload, or resource budget. The three-node container run is a Runnel cluster baseline, not a cross-product ranking; the competitor adapter remains a separate suite.

The clustered runner uses the broker's 30-second acknowledgement-timeout default. This keeps a constrained benchmark host from interpreting scheduling or quorum delay as a delivery failure during throughput measurements. Override it with `--ack-timeout-ms` when intentionally measuring redelivery behavior.

The slow-consumer scenario preloads a finite stream, polls one message at a time, waits for the configured processing delay, and then acknowledges it. Its request-latency samples cover only the broker poll and acknowledgement requests; its drain throughput and resource interval include the intentional delay. The default delay is 10 milliseconds and is required to be shorter than the acknowledgement timeout, so the scenario measures a slow but successful consumer rather than deliberate redelivery. Set the delay with `--slow-consumer-delay-ms`; the bounded message count and payload size remain the workload limits. This is evidence about behavior under a slow consumer, not proof that the current broker applies a configurable backpressure policy.

The retained-data recovery scenario is named `cluster_retained_recovery`. It preloads a separate stream with 2,048 records by default, which is above the current local 1,024-record tail-index boundary, then excludes that setup from the measured interval. The measured interval restarts one node, waits for readiness, polls the earliest retained record at offset 0, verifies its payload, and acknowledges it. Set the retained history size with `--retained-messages`; values must be at least 1,025. The scenario runs once for the first selected payload size and records `retained_messages` and `retained_logical_payload_bytes` in its existing v2 scenario metadata. Its request sample is the restart-ready-to-replay-acknowledgement interval, while the scenario resource sample covers that same interval. This measures recovery and cold replay with a known retained-data size; it does not measure retention cleanup, disk-pressure admission, batching, or prove that recovery cost is bounded. Compare runs only when retained count, payload size, topology, durability, runtime, and resource limits match.

The opt-in `retained_hot_path` scenario measures the append path after retained state already exists. It preloads exactly `--retained-messages` records into a fresh stream, excludes that setup, and then publishes the selected `--messages` through persistent public clients on the same stream while checking contiguous post-preload offsets. Select it explicitly, for example:

```text
python3 scripts/benchmarks/cluster.py \
  --scenarios retained_hot_path \
  --messages 1000 \
  --retained-messages 2048 \
  --payload-sizes 100 \
  --output benchmark-results/retained-hot-path.json
```

The result is a normal schema-v2 scenario record named `cluster_retained_hot_path`; its latency and throughput cover only measured durable publish round trips after the preload, while metadata records the retained count and logical payload bytes. This is a diagnostic hot-path baseline, not an optimization claim. It does not measure consume/replay, restart recovery, retention cleanup, disk-pressure admission, storage amplification, or behavior beyond the selected retained-history sizes. Compare only matching retained count, payload, measured message count, topology, durability, runtime, resource limits, and source/build conditions.

The opt-in `leader_failure_recovery` scenario is a bounded fault baseline for one three-node quorum. Select it explicitly with a run-scoped output and log directory:

```text
run_dir="$(mktemp -d -t runnel-leader-failure-XXXXXX)"
python3 scripts/benchmarks/cluster.py \
  --build \
  --scenarios leader_failure_recovery \
  --messages 1 \
  --payload-sizes 100 \
  --leader-failure-timeout-seconds 60 \
  --output "$run_dir/result.json" \
  --log-dir "$run_dir/logs"
```

The setup creates a stream and commits one record through node 1, then excludes that work from measurement. The current static clustered implementation starts node 1 with `--bootstrap` before the other nodes and uses it to initialize the metadata group; because the provisional public protocol forwards follower requests and exposes no leader identity, the scenario records this justified bootstrap assumption instead of claiming to detect a leader ID. It stops node 1, retries public publish/poll/ack requests through both surviving nodes until a replacement can serve, restarts node 1 on its run-scoped port and durable directory, and verifies publish/poll/ack through the restarted endpoint. Retried publishes use stable `request_id` values so an ambiguous response can be retried without intentionally creating a second record.

The default fault budget is 60 seconds and the maximum is 300 seconds. The scenario runs once for the first selected payload size and records the failed and surviving node IDs, leader-selection basis, replacement-serving observation, verified offsets, request attempts, restart-ready time, scenario resource samples, and `/metrics` delta. Its fixed three-record sequence is deliberately separate from the general sustained `--messages` workload setting. Because one node is restarted during the measured interval, its metric counters can reset; the result marks that condition as expected. The single latency sample spans stop through survivor failover and restarted-node acknowledgement; this is a reliability/recovery measurement, not a throughput comparison or evidence of a runtime performance improvement. The bounded scope excludes network partitions, storage loss, membership changes, two-node failures, repeated stable tail-latency measurements, and cross-engine comparisons. Use `--skip-recovery` to omit this scenario along with the other restart/recovery probes.

The opt-in `follower_failure_recovery` scenario uses the same bounded public-protocol sequence but stops node 2, a non-bootstrap follower, before survivor publish/consume/acknowledgement and same-node restart. It records `failure_state: follower_process_stop`, the selected failed node, and the fact that leader identity was not needed for this probe. Comparing the leader and follower cases separates leader-election behavior from ordinary replica restart behavior; neither is a complete partition, storage-loss, or multi-failure matrix.

For a rerunnable workload and fault matrix, use:

```text
just bench-cluster-matrix
```

`matrix.py` expands scenarios, payload sizes, relevant concurrency and slow-consumer delay values, retained-history sizes, runtimes, and repetitions into independent sequential `cluster.py` invocations. Every case receives a unique result and broker-log directory under the selected artifacts directory. The default matrix covers durable publish, consume/acknowledge, slow-consumer drain, restart/replay, retained-history recovery, and both leader and follower process-stop probes. Select `retained_hot_path` explicitly to expand each `--retained-message-values` entry into a separate post-preload publish case; this keeps retained-state hot-path coverage visible without changing the existing default matrix. Use `--keep-going` to retain later cases after a failure; the command still exits nonzero and records failed or timed-out cases in the matrix envelope. `--case-timeout-seconds` is an outer bound, while each fault scenario keeps its own bounded recovery timeout. Matrix cases are diagnostic coverage and are not combined into an authoritative performance claim.

The matrix accepts `--runtimes process,container`, `--cpus`, and `--memory` for explicit resource dimensions. Container cases enforce per-broker Docker limits. Add `--native-resource-scope` on Linux to place native broker and client processes in the same bounded systemd user scope. Cluster resource samples include aggregate and per-node CPU, resident memory, and on-disk storage bytes; storage scans are throttled between scenario boundaries so they do not turn the sampler into a hot-path observer. Cases with different dimensions are intentionally not aggregated into a performance ranking; normalize and aggregate matching raw case results when repeated evidence is needed.

For a small lifecycle check of the matrix orchestration:

```text
just bench-cluster-matrix-smoke
just isolated bench-cluster-matrix-smoke
```

The opt-in `peer_forwarding` scenario targets the topology-free forwarding pool. Select it explicitly so the existing clustered entrypoint keeps its established workload:

```text
python3 scripts/benchmarks/cluster.py \
  --build \
  --scenarios peer_forwarding \
  --messages 256 \
  --warmup 16 \
  --payload-sizes 100 \
  --peer-forwarding-concurrency 8 \
  --peer-response-delay-ms 5 \
  --peer-forwarding-timeout-seconds 60 \
  --output benchmark-results/peer-forwarding.json
```

The setup creates the stream and publishes warmup records through node 1; that work is excluded from measurement. Each measured publish uses one persistent public client per worker on node 2, the non-bootstrap ingress node, and therefore exercises the broker's internal `Forward` request to the data-group leader. The default eight workers exceed the current four shared forwarding permits (five pooled connections minus one reserved control connection), making pool wait visible when peer responses are delayed. The benchmark validates that all measured offsets are present and contiguous, but it does not expose offsets as a public product guarantee.

`--peer-response-delay-ms` enables a run-scoped native TCP proxy on every peer address. The proxy forwards the real framed peer protocol and delays only `Forward` responses, leaving Raft control responses on the same transport but outside the injected delay; it is deliberately a bounded perturbation for response-delay and saturation experiments, not a production topology. A zero delay keeps direct peer connections. The proxy is native-process-only because container peers cannot reach the host loopback proxy. Keep the delay small enough for the cluster's acknowledgement and request timeouts. The focused scenario has a bounded wall-clock budget from `--peer-forwarding-timeout-seconds` (default 60 seconds, maximum 300); individual protocol requests retain the broker's 30-second timeout.

The result uses the normal schema-v2 envelope and records the selected scenarios, message and warmup counts, payload sizes, forwarding concurrency, response delay, timeout, runtime, resource limits, full source revision, host provenance, and public-protocol durability boundary. Its `cluster_peer_forwarding` record reports throughput, logical payload throughput, p50/p99/p99.9/maximum follower round-trip latency, aggregate and per-node CPU/memory samples, and `/metrics` deltas. The scenario metadata identifies the forwarding ingress, operation, setup and latency boundaries, delay, concurrency, and saturation interpretation. When enabled, raw backend metadata at `backends.runnel-cluster.peer_response_proxy` additionally reports proxy connections, framed requests/responses, delayed responses, and per-node listen/target ports. These counters include cluster startup and setup traffic; use the scenario latency and metric deltas for measured comparisons.

This probe establishes a repeatable forwarding and overload baseline; it is not evidence of a runtime performance improvement. Compare only runs with the same native runtime, topology, payload, warmup, message count, forwarding concurrency, response delay, timeout, resource budget, and source/build conditions. The delayed forwarding responses still include the target's normal quorum work, so a result does not isolate pool wait from consensus or target-processing cost. The public clustered benchmark remains unchanged unless `peer_forwarding` is selected in `--scenarios`.

`--skip-recovery` skips restart and failure-recovery scenarios, including the retained-data recovery, leader-failure, and follower-failure probes; it does not skip the independent `retained_hot_path` publish probe. The cluster's temporary durable directories, generated stream names, native ports, process/container resources, and container network are run-scoped. Supply distinct output and log paths when invoking the script directly; use the isolation runner when independent process-heavy workflows overlap.

The scheduled GitHub Actions history uses 200 messages per clustered scenario by default, independently of the native comparison workload. The retained-data probe remains at its separate 2,048-record default unless `--retained-messages` is overridden. This keeps repeated recovery and quorum measurements within the workflow time budget on the workflow runner's constrained CPU allocation while retaining enough traffic to compare the cluster scenarios. Increase the `cluster_messages` input for a larger manual run when investigating sustained-load behavior.

For a quick lifecycle check:

```text
just bench-cluster-smoke
```

For a container lifecycle check, use `just bench-cluster-container-smoke`. Neither smoke workflow is a performance gate. Host scheduling, background processes, filesystem, kernel state, Docker networking, and container resource enforcement can materially affect the numbers. Keep the host and workload metadata with any result used for comparison.

## Profiling

Capture Linux CPU call-graph samples while the cluster is under a sustained publish/consume/acknowledge workload:

```text
just profile-cluster
```

The profile workflow writes one `perf.data` file and one `perf report --stdio` text report per broker process, plus broker logs and a JSON manifest, under `benchmark-results/profile-*/`. Use the reports to distinguish time spent in protocol parsing, serialization, consensus, locks, storage, and scheduling. The workload is deliberately representative rather than a synthetic microbenchmark; change its duration, worker count, payload size, and sampling frequency through the script options when investigating a hypothesis. `perf` permissions and kernel configuration are host prerequisites, so this workflow remains local and optional.

For internal stage timing without `perf`:

```text
just profile-cluster-instrumented
```

The `instrumentation` Cargo feature compiles timing guards into the broker and `RUST_LOG=runnel::timing=trace` records their durations in each node's broker log. The workflow summarizes p50, p99, and maximum microseconds for protocol handling, lock waits, storage, quorum operations, state-machine application, and peer RPCs in `profile.json`. Peer RPCs are split into connect, write, and read stages. The profile workload performs one publish, poll, and acknowledgement per completed message, so stage counts can be interpreted alongside the recorded workload count. The normal build has no timing guards, and the timing feature should not be used for uncontaminated performance comparisons.

## First-pass broker comparison

Run the native-tool comparison with:

```text
just bench-compare
```

The runner starts each selected broker in isolation on a temporary Docker network, applies the same per-container broker and client CPU/memory limits, creates one stream/topic, publishes 10,000 messages, consumes them, records image identifiers and scenario-scoped resource measurements, and writes a JSON result under `benchmark-results/compare-<timestamp>.json`. The default payload sizes are 100 bytes and 1 KiB. Container names, data directories, and the Docker network are run-scoped, so separate comparison invocations can overlap without name collisions; cleanup removes the containers, temporary data, and network on success or failure. Broker readiness is bounded at 45 seconds, with each Docker readiness probe bounded at 10 seconds; measured native commands retain a 180-second timeout.

Each raw result is self-describing: `run_id` identifies the artifact, `command` records the exact invocation, `source` records the full revision and CI workflow identity, and `environment` records the host, platform, processor, Python version, and CPU count. The per-backend records retain the pinned broker image and resolved image identifier; the measurement-client image and acknowledgement/replication boundary are documented in the backend metadata and command implementation. This provenance must travel with any numbers used for comparison.

The raw result also contains a machine-readable `comparison_guardrail` with `apples_to_apples: false` and `ranking_eligible: false`. Every backend has `semantic_metadata` for its acknowledgement boundary, replication topology, measurement boundary, client identity, and measured scenario classes. Each scenario carries `metadata.comparison_class`: `publish-only`, `consume-with-ack`, or `consume-without-ack`. The harness validates these declarations and fails before writing a result when a boundary or classification is missing or inconsistent. This keeps the native measurements useful as engineering baselines while making their non-equivalence explicit to downstream tooling.

The pinned images are Apache Kafka `4.3.1`, Redpanda `v26.2.1`, NATS Server `2.14.5-alpine`, and `nats-box` `0.19.7`. The Runnel image is built by the `just` recipe. Redpanda's development mode needs more than a 1 GiB cgroup, so the shared default is 2 CPUs and 2 GiB; pass `--cpus` and `--memory` to change it.

This is intentionally a first baseline built around native benchmark clients. Runnel and JetStream report durable publish latency; Kafka and Redpanda use Kafka's native producer performance client, whose latency includes its configured client batching, and their native consumer performance client reports fetch throughput without application-level acknowledgement. The JSON records these boundaries and marks the output as non-equivalent. Do not present the single-node output as a final cross-product ranking until a common client workload and equivalent consumer acknowledgement path exist.

The comparison entrypoint is kept stable at `compare.py`; its shared lifecycle and resource cleanup, semantic result policy, backend execution, and CLI/result-envelope responsibilities have focused private ownership in `compare_lifecycle.py`, `compare_results.py`, `compare_backends.py`, and `compare_cli.py`. This is a maintainability extraction only: it does not make the native workloads equivalent or change the comparison guardrails.

The bounded three-node slice measures only replicated durable publish for the three external competitors:

```text
python3 scripts/benchmarks/compare.py \
  --nodes 3 \
  --backends kafka,redpanda,nats \
  --messages 1000 \
  --payload-sizes 100 \
  --cpus 2 \
  --memory 2g \
  --client-cpus 2 \
  --client-memory 2g
```

Run the replicated three-node competitor baseline with:

```text
just bench-compare-cluster
```

This records one publish scenario per payload size for Kafka, Redpanda, and NATS JetStream with replication factor three. It is deliberately a separate `cluster-comparison` history suite: the current Runnel cluster runner measures the public protocol and consumer acknowledgement paths, while this first competitor slice has only equivalent durable-publish adapters. The weekly competitor workflow repeats and aggregates this suite independently from the Runnel optimization history.

`--nodes 3` starts three Kafka KRaft brokers/controllers, three Redpanda brokers, or three NATS servers with JetStream clustering. Kafka topics use one partition, replication factor 3, `min.insync.replicas=3`, `acks=all`, and producer idempotence. JetStream streams use file storage and `--replicas=3`; the native synchronous publisher measures the PubAck boundary. Each broker container receives the broker limits and the short-lived native client receives the client limits, so a three-node run consumes up to three times the per-container broker budget plus the client budget. The result keeps per-node resource summaries and aggregate CPU/memory fields.

The three-node mode rejects Runnel because this comparison runner has no distributed Runnel adapter, and it omits consumers because Kafka and Redpanda's native consumer performance client does not perform application-level acknowledgements. It therefore establishes a useful RF=3 publish baseline, not a complete end-to-end or failure-tolerance comparison. The broker modes also differ in their exact persistence and acknowledgement implementation: `acks=all`/`min.insync.replicas=3` and synchronous JetStream PubAck are recorded as client-visible boundaries, not a claim that every broker performs identical filesystem flushes. Fault injection, common client code, partitioning/concurrency parity, and replicated consume/ack remain follow-up work.

Examples:

```text
python3 scripts/benchmarks/compare.py --backends kafka,redpanda --messages 1000 --payload-sizes 100 --cpus 2 --memory 2g
python3 scripts/benchmarks/compare.py --backends nats --messages 10000 --payload-sizes 1024 --cpus 2 --memory 2g
```

## History and dashboard

Normalize a comparison result and generate local history data:

```text
python3 scripts/benchmarks/normalize.py \
  --input benchmark-results/compare-<timestamp>.json \
  --output benchmark-results/normalized.json
python3 scripts/benchmarks/build_history.py \
  --runs benchmark-results \
  --output benchmark-results/site
```

The normalized schema intentionally excludes native tool logs. It retains the version-2 run envelope, workload, limits, image identifiers, semantic boundaries, scenario resource and server-metric deltas, source revision, workflow provenance, and measured points. `build_history.py` aggregates these records into `site/data.json` on the generated `benchmark-history` branch. The hand-authored HTML, CSS, and JavaScript in `docs/benchmarks/` are served directly by GitHub Pages and read that public history data from the raw GitHub URL. The longer Runnel-only history is the primary optimization series; native and three-node competitor records are kept as separate suites. Invalid or unrelated JSON files are skipped.

The dashboard uses the dedicated Runnel suite as the primary optimization history. Older Runnel points recorded by the native-comparison workflow remain available as a separate history, because their measurement boundary can differ. Charts keep benchmark suite, backend, operation, and payload size in separate visual series, so selecting all sizes or suites does not connect unrelated measurements or hide which point belongs to which workload. Raw run medians are shown as dots, while a five-run rolling median makes the direction of change easier to see; repetition ranges are shown as a band when available.

## Local performance evidence

The policy for deciding whether a change needs a benchmark, interpreting stable or inconclusive results, diagnosing outliers, and reporting findings lives in [docs/benchmarking.md](../../docs/benchmarking.md). This README documents the harnesses, workload semantics, options, result schema, and history generation that support that policy.

Use `just bench-pr-local` for the authoritative same-host current-versus-`origin/main` comparison. When a complete comparison is inconclusive, use `just bench-pr-local-until-stable` to retain attempts while retrying the controlled workflow. Use `just bench-pr-local-quick` only for diagnostics; it is not performance evidence.

The generated Markdown report and raw JSON artifacts remain under `benchmark-results/pr-local/`. Preserve them with the handoff so the exact revision, workload, resources, repetition, stability result, and measurement boundaries remain reviewable.
