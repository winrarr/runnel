# Container benchmarks

## Concurrent local workflows

The benchmark commands are safe to run concurrently through the repository's
isolation runner:

```text
just isolated bench-container-smoke
just isolated bench-cluster-smoke
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

The result records the image, source revision, host information, resource limits, startup time, scenario-scoped cgroup CPU time, CPU efficiency, sampled container memory, workload parameters, throughput, and p50/p99/p99.9/max latency where a scenario has per-message latency.

Use `--scenarios` to run only the named scenarios when a benchmark consumer needs a smaller, explicit workload. The accepted names are `durable_publish`, `concurrent_publish`, `consume_ack`, `publish_consume_ack_roundtrip`, and `restart_recovery`; the default runs all of them. The native comparison adapter selects only `durable_publish` and `consume_ack`, because those are the scenarios it retains from the Runnel result.

This is an end-to-end benchmark of the current development protocol. It is not yet a fair Kafka, Redpanda, or NATS JetStream comparison: those brokers require adapters that express equivalent acknowledgement, replication, ordering, and delivery guarantees. The comparison work belongs in the benchmark backlog. Do not compare raw numbers across brokers until the adapter semantics and environment are recorded in the result.

The short `just bench-container-smoke` recipe is used by CI to verify that the image can start, accept workload traffic under limits, expose metrics, and recover an unacknowledged message. It is a workflow check, not a performance gate.

## Clustered baseline

Run the real three-node clustered baseline with:

```text
just bench-cluster
```

The runner builds the release broker, starts three processes with independent durable directories, and exercises the public protocol through multiple nodes. It measures durable publish, non-grouped consume/acknowledge, a bounded slow-consumer backlog drain, sequential shared-consumer delivery, parallel shared-consumer delivery, and restart recovery for the selected payload sizes. Each result records the node count, acknowledgement timeout, quorum durability boundary, protocol boundary, workload, throughput, p50/p99/p99.9 latency, aggregate process CPU time, and resident memory samples. Results use the same `backends` shape as the container and comparison runners, so they can be normalized into the existing history dashboard.

The clustered runner uses the broker's 30-second acknowledgement-timeout default. This keeps a constrained benchmark host from interpreting scheduling or quorum delay as a delivery failure during throughput measurements. Override it with `--ack-timeout-ms` when intentionally measuring redelivery behavior.

The slow-consumer scenario preloads a finite stream, polls one message at a time, waits for the configured processing delay, and then acknowledges it. Its request-latency samples cover only the broker poll and acknowledgement requests; its drain throughput and resource interval include the intentional delay. The default delay is 10 milliseconds and is required to be shorter than the acknowledgement timeout, so the scenario measures a slow but successful consumer rather than deliberate redelivery. Set the delay with `--slow-consumer-delay-ms`; the bounded message count and payload size remain the workload limits. This is evidence about behavior under a slow consumer, not proof that the current broker applies a configurable backpressure policy.

The scheduled GitHub Actions history uses 200 messages per clustered scenario by default, independently of the native comparison workload. This keeps repeated recovery and quorum measurements within the workflow time budget on the workflow runner's constrained CPU allocation while retaining enough traffic to compare the cluster scenarios. Increase the `cluster_messages` input for a larger manual run when investigating sustained-load behavior.

For a quick lifecycle check:

```text
just bench-cluster-smoke
```

This is not a performance gate. Host scheduling, background processes, filesystem, and kernel state can materially affect the numbers. Keep the host and workload metadata with any result used for comparison.

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

The pinned images are Apache Kafka `4.3.1`, Redpanda `v26.2.1`, NATS Server `2.14.5-alpine`, and `nats-box` `0.19.7`. The Runnel image is built by the `just` recipe. Redpanda's development mode needs more than a 1 GiB cgroup, so the shared default is 2 CPUs and 2 GiB; pass `--cpus` and `--memory` to change it.

This is intentionally a first baseline built around native benchmark clients. Runnel and JetStream report durable publish latency; Kafka and Redpanda use Kafka's native producer performance client, whose latency includes its configured client batching, and their native consumer performance client reports fetch throughput without application-level acknowledgement. The JSON records these boundaries. Do not present the single-node output as a final cross-product ranking until a common client workload and equivalent consumer acknowledgement path exist.

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

The normalized schema intentionally excludes native tool logs. It retains workload, limits, image identifiers, semantic boundaries, scenario resource samples, source revision, workflow provenance, and measured points. `build_history.py` aggregates these records into `site/data.json` on the generated `benchmark-history` branch. The hand-authored HTML, CSS, and JavaScript in `docs/benchmarks/` are served directly by GitHub Pages and read that public history data from the raw GitHub URL. The longer Runnel-only history is the primary optimization series; native and three-node competitor records are kept as separate suites. Invalid or unrelated JSON files are skipped.

The dashboard uses the dedicated Runnel suite as the primary optimization history. Older Runnel points recorded by the native-comparison workflow remain available as a separate history, because their measurement boundary can differ. Charts keep benchmark suite, backend, operation, and payload size in separate visual series, so selecting all sizes or suites does not connect unrelated measurements or hide which point belongs to which workload. Raw run medians are shown as dots, while a five-run rolling median makes the direction of change easier to see; repetition ranges are shown as a band when available.

## Local performance evidence

The policy for deciding whether a change needs a benchmark, interpreting stable or inconclusive results, diagnosing outliers, and reporting findings lives in [docs/benchmarking.md](../../docs/benchmarking.md). This README documents the harnesses, workload semantics, options, result schema, and history generation that support that policy.

Use `just bench-pr-local` for the authoritative same-host current-versus-`origin/main` comparison. When a complete comparison is inconclusive, use `just bench-pr-local-until-stable` to retain attempts while retrying the controlled workflow. Use `just bench-pr-local-quick` only for diagnostics; it is not performance evidence.

The generated Markdown report and raw JSON artifacts remain under `benchmark-results/pr-local/`. Preserve them with the handoff so the exact revision, workload, resources, repetition, stability result, and measurement boundaries remain reviewable.
