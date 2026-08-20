# Runnel

Runnel is a Rust message broker focused on low latency, predictable resource usage, durable delivery, and simple operation. It is designed to feel closer to starting a local infrastructure tool than operating a distributed event platform.

This repository currently provides the first vertical slice:

- a single-node broker process;
- durable append-only stream storage;
- multiple independent durable consumers;
- at-least-once polling and acknowledgements;
- redelivery after acknowledgement timeout or broker restart;
- health and basic Prometheus-compatible metrics;
- a small development CLI;
- an early three-node Multi-Raft development backend with any-node client routing;
- Docker and Kubernetes starting points.

The workspace also contains `runnel-engine`, the shared semantic engine contract, and `runnel-raft`, an early static Multi-Raft backend. `--engine raft` enables versioned durable Raft/state-machine files, framed TCP peer transport, topology-free client forwarding, and a three-node development cluster. The backend is not yet production-complete: replicated metadata, dynamic membership, security policy, and broader failure semantics remain unfinished.

Consumer groups, retention, batching, compression, authentication, TLS, and clustering are planned product work. The current line-delimited JSON protocol is a development protocol and is not yet a compatibility promise.

## Quick start

Start the broker in one terminal:

    cargo run -p runnel-server -- --data-dir ./data

Use the CLI in another terminal:

    cargo run -p runnel-cli -- create-stream events
    cargo run -p runnel-cli -- publish events "hello from runnel"
    cargo run -p runnel-cli -- consume events worker
    cargo run -p runnel-cli -- ack events worker 0

The broker listens on 127.0.0.1:4222. Health endpoints and metrics listen on 127.0.0.1:8080:

    curl http://127.0.0.1:8080/health/live
    curl http://127.0.0.1:8080/health/ready
    curl http://127.0.0.1:8080/metrics

To demonstrate restart recovery, publish a message, consume it without acknowledging, stop and restart the broker with the same data directory, then consume with the same consumer name. The message is delivered again because the checkpoint did not advance.

## Development

The supported development environment is Linux. The repository uses a Cargo workspace and just as its canonical command runner:

    cargo install --locked just
    just verify

Useful workflows:

    just run
    just smoke
    just cluster-test
    just bench
    just bench-container
    just bench-compare
    just bench-dashboard
    just bench-test
    just ci

The existing scripts/verify.sh command remains as a thin compatibility wrapper around just verify.

The test suite includes core persistence and recovery tests, wire-format round-trip tests, and a network-level test that starts the real broker process and verifies acknowledgement state across restart. `just smoke` is the canonical local end-to-end test: it starts the broker itself and uses `runnelctl` to publish, consume, acknowledge, restart, and verify recovery. The benchmark suite measures durable publish and publish/poll/ack paths; benchmark results must always be interpreted with their durability settings and workload. See [docs/testing.md](docs/testing.md) for the interactive walkthrough and test layers.

`just bench-container` builds a resource-limited Runnel container and runs repeatable end-to-end workload scenarios for 100-byte and 1-KiB messages. It writes machine-readable results under the ignored `benchmark-results/` directory. See [scripts/benchmarks/README.md](scripts/benchmarks/README.md) for workload semantics and comparison limitations.

`just bench-compare` builds Runnel and runs an isolated first-pass native-tool comparison against pinned Kafka, Redpanda, and NATS JetStream containers. It uses a 2 CPU/2 GiB broker and client budget by default because Redpanda's development container needs more than a 1 GiB cgroup. Results are written under the ignored `benchmark-results/` directory and must be read with the recorded measurement boundaries; this is an engineering baseline, not a final apples-to-apples claim.

`just bench-dashboard` generates a local static dashboard from JSON results under `benchmark-results/`. The GitHub Actions benchmark workflow normalizes successful runs, keeps the raw result as an artifact, appends history to a generated branch, and publishes the dashboard through GitHub Pages.

The current protocol accepts one JSON request per TCP line. For example:

    {"op":"publish","stream":"events","payload":"hello","request_id":"optional-stable-id"}
    {"op":"poll","stream":"events","consumer":"worker"}
    {"op":"ack","stream":"events","consumer":"worker","offset":0}

See [docs/architecture.md](docs/architecture.md) for the current boundaries, [docs/design/distributed-architecture-options.md](docs/design/distributed-architecture-options.md) for the multi-node alternatives, [docs/design/multi-raft-implementation-plan.md](docs/design/multi-raft-implementation-plan.md) for the proposed first clustered plan, [docs/backlog.md](docs/backlog.md) for intended next outcomes, and [docs/tech-debt.md](docs/tech-debt.md) for known implementation shortcuts. Repository operating guidance lives in AGENTS.md.

For dependency auditing, install cargo-audit and run:

    cargo install --locked cargo-audit
    just audit

## Docker

Build and run a local image:

    docker build -t runnel:dev .
    docker volume create runnel-data
    docker run --rm -p 4222:4222 -p 8080:8080 -v runnel-data:/var/lib/runnel runnel:dev

The Kubernetes manifest in deploy/kubernetes/runnel.yaml starts a three-node static Multi-Raft development cluster with independent persistent volumes. It does not provide TLS, authentication, upgrades, or production policy by itself; see [deploy/kubernetes/README.md](deploy/kubernetes/README.md).
