# Testing and local operation

The canonical local end-to-end check is:

```text
just smoke
```

This is a real broker test, not a mock or an in-process shortcut. It builds the server and CLI, allocates temporary local ports and storage, starts `runnel`, uses `runnelctl` to create a stream, publish, consume, and acknowledge messages, restarts the broker, verifies redelivery and durable consumer state, checks readiness and metrics, and removes its temporary data.

Run it whenever changing storage, delivery, protocol, process startup, shutdown, or deployment behavior. CI invokes the same recipe.

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

The broker uses `./data` by default, listens on `127.0.0.1:4222`, and serves readiness and metrics on `127.0.0.1:8080`. Stop it with SIGINT or SIGTERM. Use a new stream and consumer name, or remove local development data deliberately, when an old checkpoint would make the expected offset unclear.

## Verification layers

- `just test` runs workspace unit, integration, and benchmark-target tests.
- `just doc-test` runs Rust documentation tests.
- `just verify` runs formatting, Clippy, Rust tests, ShellCheck, benchmark-script tests, and a workspace build.
- `just smoke` exercises the running process and CLI across a restart.
- `just cluster-test` starts three real Raft-backed broker processes and verifies quorum replication, follower restart, leader election, post-failure recovery, consensus-log compaction, empty replacement-node snapshot recovery, an interrupted snapshot transfer retry, and recovery metrics through the public protocol.
- `just bench` runs the Criterion durable publish and publish/poll/ack benchmarks; interpret every result with its durability mode and workload.
- `just bench-container` builds a Runnel image, applies explicit Docker CPU and memory limits, and runs repeatable end-to-end publish, concurrent publish, consume/acknowledge, round-trip, and restart-recovery scenarios for 100-byte and 1-KiB payloads. Results are written as ignored JSON artifacts under `benchmark-results/` and include resource samples and p50/p99/p99.9 latency where applicable.
- `just bench-container-smoke` runs the same container path with a small workload for CI. It verifies the benchmark harness and container lifecycle; it is not a performance gate.
- `just bench-compare` builds Runnel and runs the first-pass native-tool comparison against the pinned Kafka, Redpanda, and NATS JetStream images. It uses isolated single-node, replication-factor-one containers with explicit CPU and memory limits and writes a machine-readable comparison artifact.
- `just bench-dashboard` generates a local static dashboard from comparison JSON files under `benchmark-results/`.
- `just bench-test` runs the benchmark normalization and dashboard tests.
- `just ci` runs verification, the smoke test, the container build, and the container benchmark smoke check.

When a test depends on crash behavior, use a real process and persistent temporary storage. Keep mocks and unit tests for local domain logic, not as substitutes for process, filesystem, or protocol coverage.

## Container benchmark interpretation

The container benchmark measures the current Runnel development protocol with the selected broker image and Docker resource limits. The comparison harness now provides a first-pass cross-broker baseline using each product's native benchmark client. It deliberately records the semantic boundary: Runnel measures its current request/response protocol with durable publish and consume acknowledgement; NATS measures synchronous JetStream publish and explicit durable-consumer acknowledgement; Kafka and Redpanda measure Kafka producer publish with `acks=all` plus consumer fetch throughput without per-record application acknowledgement. These results are useful for discovering orders of magnitude and resource costs, but they are not a final apples-to-apples claim. Record image versions, host, storage, CPU and memory limits, workload, batching, and failure state for every comparison.

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

The current pinned images are Apache Kafka `4.3.1`, Redpanda `v26.2.1`, NATS Server `2.12.14-alpine`, and `nats-box` `0.19.7`. Redpanda's development image cannot reliably start under the shared 1 GiB default, so the comparison defaults to 2 GiB. The image identifiers and limits are copied into each JSON result.

## Automatic benchmark history

`.github/workflows/benchmarks.yml` runs the comparison for pushes to `main`, weekly scheduled runs, and manual runs. It preserves the raw comparison output as a workflow artifact, normalizes stable measurements with commit and runner provenance, and appends the normalized record plus `site/data.json` to the generated `benchmark-history` branch. `.github/workflows/benchmark-pages.yml` publishes only the static dashboard code to the separate `benchmark-pages` branch when the generator changes; the dashboard reads the public history data directly from GitHub.

GitHub Pages is configured to publish the root of the `benchmark-pages` branch. The benchmark workflow does not need Pages deployment permissions or a Pages environment. The data branch is generated output; change the benchmark scripts and workflows rather than editing it manually.
