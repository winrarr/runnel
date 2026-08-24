# Testing and local operation

The canonical local end-to-end check is:

```text
just smoke
```

This is a real broker test, not a mock or an in-process shortcut. It builds the server and CLI, allocates temporary local ports and storage, starts `runnel`, uses `runnelctl` to create streams, publish, consume, acknowledge, and share work between members of a consumer, restarts the broker, verifies redelivery and durable consumer state, checks readiness and metrics, and removes its temporary data.

Run it whenever changing storage, delivery, protocol, process startup, shutdown, or deployment behavior. CI invokes the same recipe.

## Concurrent local workflows

Use the isolation runner when more than one process-heavy workflow needs to run at once:

```text
just isolated
just isolated cluster-test
just isolated bench-container-smoke
```

Every invocation receives a unique Cargo target directory, temporary-file directory, and benchmark artifact directory. The smoke and cluster workflows already allocate ephemeral loopback ports; container benchmarks additionally use unique container names and private Docker networks. Successful benchmark artifacts remain under `benchmark-results/isolated/<run-id>/`, while failed runs retain their temporary build and process state so the failure can be inspected. Use only the named workflows shown by `python3 scripts/isolated.py --help`; arbitrary commands may use resources that cannot be isolated automatically.

## Interactive local walkthrough

Start the broker:

```text
just run
```

In a second terminal, use the development CLI:

```text
cargo run -q -p runnel-cli -- create-stream playground
cargo run -q -p runnel-cli -- publish playground "hello from runnel"
cargo run -q -p runnel-cli -- consume playground local-worker
cargo run -q -p runnel-cli -- ack playground local-worker <offset-from-consume>
```

To exercise the shared-consumer path locally, publish at least two messages and run the following from separate terminals. Use the delivery token printed by each consume response when acknowledging:

```text
cargo run -q -p runnel-cli -- consume playground workers --member worker-a
cargo run -q -p runnel-cli -- consume playground workers --member worker-b
cargo run -q -p runnel-cli -- ack playground workers <offset> --member worker-a --delivery-token <token>
```

The two members share work under `workers`; a different consumer name receives an independent copy. Both local and clustered grouped paths serialize messages with the same key and reject stale delivery tokens after redelivery. For a real three-node process test, run `just cluster-test`; it also verifies grouped delivery through follower forwarding, reassignment after a node failure, and clustered dead-letter recovery after the configured attempt limit.

The broker uses `./data` by default, listens on `127.0.0.1:4222`, and serves readiness and metrics on `127.0.0.1:8080`. Stop it with SIGINT or SIGTERM. Use a new stream and consumer name, or remove local development data deliberately, when an old checkpoint would make the expected offset unclear.

To exercise retry and dead-letter behavior, start a local broker with a short timeout and a limit:

~~~
cargo run -q -p runnel-server -- --data-dir ./data --ack-timeout-ms 50 --max-delivery-attempts 2
cargo run -q -p runnel-cli -- publish jobs poison
cargo run -q -p runnel-cli -- consume jobs retry-worker
# wait for the timeout, then consume again
cargo run -q -p runnel-cli -- consume jobs retry-worker
# after the second timeout, the record is available on jobs.dead-letter
cargo run -q -p runnel-cli -- consume jobs.dead-letter dead-letter-inspector
~~~

The dead-letter stream preserves the original key and payload. The current move is at least once across the source checkpoint and dead-letter log, so crash recovery may expose a duplicate dead-letter record.

The clustered grouped-consumer policy uses the same command-line settings:

```text
cargo run -q -p runnel-server -- --engine raft --node-id 1 --cluster-name local --data-dir ./data-1 --peer 1=127.0.0.1:7101 --peer 2=127.0.0.1:7102 --peer 3=127.0.0.1:7103 --bootstrap --ack-timeout-ms 50 --max-delivery-attempts 2
```

The three-node process test exercises both grouped and non-grouped clustered paths through the public protocol. Their dead-letter transitions are committed with source progress in the stream data group.

## Verification layers

The Criterion suite includes durable publish, legacy publish/poll/ack, two-member shared-consumer, and keyed shared-consumer baselines. Interpret every result with its durability mode, message size, membership, and ordering-key distribution.

- `just test` runs workspace unit, integration, and benchmark-target tests.
- `just doc-test` runs Rust documentation tests.
- `just verify` runs formatting, Clippy, Rust tests, ShellCheck, benchmark-script tests, and a workspace build.
- `just smoke` exercises the running process and CLI across a restart.
- `just cluster-test` starts three real Raft-backed broker processes and verifies quorum replication, grouped and non-grouped delivery through follower forwarding, grouped reassignment after node failure, clustered retry limits and dead-letter recovery, follower restart, leader election, post-failure recovery, consensus-log compaction, empty replacement-node snapshot recovery, an interrupted snapshot transfer retry, and recovery metrics through the public protocol.
- `just bench` runs the Criterion durable publish, publish/poll/ack, shared-consumer, keyed-ordering, and local concurrency-scaling benchmarks; interpret every result with its durability mode and workload.
- `just bench-container` builds a Runnel image, applies explicit Docker CPU and memory limits, and runs repeatable end-to-end publish, concurrent publish, consume/acknowledge, round-trip, and restart-recovery scenarios for 100-byte and 1-KiB payloads. Results are written as ignored JSON artifacts under `benchmark-results/` and include scenario-scoped cgroup CPU time, CPU efficiency, memory samples, and p50/p99/p99.9 latency where applicable.
- `just bench-container-smoke` runs the same container path with a small workload for CI. It verifies the benchmark harness and container lifecycle; it is not a performance gate.
- `just bench-cluster` builds the release broker and measures a real three-node static Raft cluster through the public protocol. It covers durable publish, non-grouped consume/acknowledge, sequential shared-consumer delivery, parallel shared-consumer delivery, and restart recovery for 100-byte and 1-KiB payloads. Results are written as `cluster-*.json` under the ignored `benchmark-results/` directory and include aggregate broker CPU time and resident memory samples. The harness uses the broker's 30-second acknowledgement timeout by default; pass `--ack-timeout-ms` to measure a different retry window.
- `just bench-cluster-smoke` runs the same clustered harness with a small workload and skips recovery. It verifies startup, quorum routing, delivery, and result generation; it is not a performance gate.
- `just profile-cluster` builds the release broker, runs a sustained clustered publish/consume/acknowledge workload, and captures per-node Linux `perf` call-graph samples plus text reports under `benchmark-results/profile-*/`. It requires `perf` and suitable kernel profiling permissions; profiling is intentionally optional and is not part of correctness CI.
- `just profile-cluster-instrumented` builds with the opt-in `instrumentation` feature, enables `runnel::timing` logs, and records internal stage timing summaries without attaching `perf`. Peer RPC timing is split into connection, write, and read stages. The profile workload performs one publish, poll, and acknowledgement per completed message. The default build compiles these timing paths out; use `--features instrumentation` with `profile.py` directly when both internal timings and Linux `perf` are available.
- `just bench-compare` builds Runnel and runs one first-pass native-tool comparison against the pinned Kafka, Redpanda, and NATS JetStream images. It uses isolated single-node, replication-factor-one containers with explicit CPU and memory limits and writes a machine-readable comparison artifact. Each measured publish or consume scenario records its own broker CPU time and memory interval so the dashboard can show CPU efficiency and memory at the selected workload.
- `just bench-compare-cluster` runs the three-node RF=3 durable-publish comparison for Kafka, Redpanda, and NATS JetStream with explicit CPU and memory limits. It is a publish-only first slice because equivalent replicated consume/acknowledgement adapters do not yet exist; each result records its topology and measurement boundary.
- `python3 scripts/benchmarks/pr_report.py --input benchmark-results/pr.json --output benchmark-results/pr-comment.md` renders a short Runnel pull-request benchmark artifact as a compact Markdown report. It is intended for CI comments and local inspection, not as a merge gate.
- `just bench-dashboard` generates local benchmark history data from comparison JSON files under `benchmark-results/`; the static dashboard source is in `docs/benchmarks/`.
- `just bench-test` runs the benchmark normalization and dashboard tests.
- `just ci` runs verification, the smoke test, the container build, and the container benchmark smoke check.

When a test depends on crash behavior, use a real process and persistent temporary storage. Keep mocks and unit tests for local domain logic, not as substitutes for process, filesystem, or protocol coverage.

## Container benchmark interpretation

The container benchmark measures the current Runnel development protocol with the selected broker image and Docker resource limits. The comparison harness now provides a first-pass cross-broker baseline using each product's native benchmark client. It deliberately records the semantic boundary: Runnel measures its current request/response protocol with durable publish and consume acknowledgement; NATS measures synchronous JetStream publish and explicit durable-consumer acknowledgement; Kafka and Redpanda measure Kafka producer publish with `acks=all` plus consumer fetch throughput without per-record application acknowledgement. CPU efficiency is reported as messages per broker CPU-second, while memory is reported as scenario peak memory; neither should be interpreted without the workload and latency charts. These results are useful for discovering orders of magnitude and resource costs, but they are not a final apples-to-apples claim. Record image versions, host, storage, CPU and memory limits, workload, batching, and failure state for every comparison.

The comparison command is:

```text
just bench-compare
```

For a smaller or selected run:

```text
python3 scripts/benchmarks/compare.py \
  --backends runnel,kafka,redpanda,nats \
  --messages 10000 \
  --payload-sizes 100,1024 \
  --cpus 2 \
  --memory 2g
```

The current pinned images are Apache Kafka `4.3.1`, Redpanda `v26.2.1`, NATS Server `2.14.5-alpine`, and `nats-box` `0.19.7`. Redpanda's development image cannot reliably start under the shared 1 GiB default, so the comparison defaults to 2 GiB. The image identifiers and limits are copied into each JSON result.

## Automatic benchmark history

`.github/workflows/benchmarks.yml` runs the longer Runnel-only single-node and three-node history suites on pushes to `main`, daily, and manually. `.github/workflows/benchmark-competitors.yml` runs the separate native single-node and three-node RF=3 competitor comparisons weekly or manually. `.github/workflows/benchmark-pr.yml` runs a short Runnel-only benchmark for pull requests, while `.github/workflows/benchmark-pr-comment.yml` uses a trusted workflow to turn the artifact into an informational PR comment. Runnel history is the primary optimization signal; competitor suites remain separate ranking evidence. History runs normalize each repetition, aggregate by median while retaining min/max observed values, preserve raw and aggregate results as artifacts, and append generated `site/data.json` to the `benchmark-history` branch. The dashboard separates suites, displays repetition counts and ranges, and compares each run with the previous compatible workload, resource, broker-image, and measurement configuration. Set `messages`, `cluster_comparison_messages`, `cluster_messages`, and `repetitions` when dispatching the applicable workflow manually.

GitHub Pages is configured to publish `/docs` from `main`. The benchmark workflow does not need Pages deployment permissions or a Pages environment. The data branch is generated output; change the benchmark scripts and dashboard assets rather than editing it manually.
