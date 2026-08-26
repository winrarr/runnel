# Runnel

Runnel is a Rust message broker focused on low latency, predictable resource usage, durable delivery, and simple operation. It is designed to feel closer to starting a local infrastructure tool than operating a distributed event platform.

The initial product is aimed at small engineering teams that need durable background work and application-event streams, want a genuinely useful single-node deployment, and need a credible path to highly available and larger deployments without carrying avoidable broker-topology complexity in application code. See [docs/product-fit.md](docs/product-fit.md) for the intended audience, workloads, product boundaries, and evidence still required.

This repository currently provides the first vertical slice:

- a single-node broker process;
- durable append-only stream storage;
- multiple independent durable consumers;
- shared consumers that distribute work between named members;
- at-least-once polling and acknowledgements;
- per-key ordering and stale-delivery rejection for local and clustered shared consumers;
- retry attempt tracking and optional dead-letter streams in local and clustered grouped delivery;
- redelivery after acknowledgement timeout or broker restart;
- health and basic Prometheus-compatible metrics;
- a small development CLI;
- a reusable async client with persistent connections and bounded request timeouts;
- an early three-node Multi-Raft development backend with any-node client routing;
- Docker and Kubernetes starting points.

The workspace also contains `runnel-engine`, the shared semantic engine contract, and `runnel-raft`, an early static Multi-Raft backend. `--engine raft` enables versioned durable Raft/state-machine files, framed TCP peer transport, topology-free client forwarding, replicated shared-consumer ownership, clustered attempt limits and dead-letter streams, and a three-node development cluster. The backend is not yet production-complete: dynamic membership, scalable placement, backoff and dead-letter provenance, security policy, and broader failure semantics remain unfinished.

The workspace includes reusable `runnel-client` and `runnel-test-support` crates. The client provides persistent sequential request/response transport with bounded connection, write, and response timeouts for the provisional protocol; the test-support crate contains storage- and topology-independent assertions for the Engine contract.

Retention, batching, compression, authentication, and TLS remain planned product work. The current line-delimited JSON protocol is a development protocol and is not yet a compatibility promise. Retry limits and dead-letter streams are broker-wide configuration for local and clustered delivery; the early static Raft backend provides replicated ownership, expiry fencing, and dead-letter recovery without exposing its internal consumer state.

## Quick start

Start the broker in one terminal:

    cargo run -p runnel-server -- --data-dir ./data

Use the CLI in another terminal:

    cargo run -p runnel-cli -- create-stream events
    cargo run -p runnel-cli -- publish events "hello from runnel"
    cargo run -p runnel-cli -- consume events worker
    cargo run -p runnel-cli -- ack events worker 0

To share work between local worker processes, use one consumer name and a distinct member name for each worker. The grouped consume response includes a delivery token that must be supplied when acknowledging:

    cargo run -p runnel-cli -- consume jobs workers --member worker-a
    cargo run -p runnel-cli -- ack jobs workers 0 --member worker-a --delivery-token <token-from-consume>

The broker listens on 127.0.0.1:4222. Health endpoints and metrics listen on 127.0.0.1:8080:

    curl http://127.0.0.1:8080/health/live
    curl http://127.0.0.1:8080/health/ready
    curl http://127.0.0.1:8080/metrics

To demonstrate restart recovery, publish a message, consume it without acknowledging, stop and restart the broker with the same data directory, then consume with the same consumer name. The message is delivered again because the checkpoint did not advance.

When max-delivery-attempts is set, a message that reaches the limit is copied to the stream's .dead-letter stream with its key and payload preserved. The acknowledgement timeout controls when the next attempt becomes eligible. The clustered path commits the dead-letter record and source progress in the same replicated data-group operation; the local path remains at least once across the source checkpoint and dead-letter log, so operators should tolerate duplicate dead-letter records after a local crash.

## Development

The supported development environment is Linux. The repository uses a Cargo workspace and just as its canonical command runner:

    cargo install --locked just
    just verify

Useful workflows:

    just run
    just smoke
    just isolated
    just isolated cluster-test
    just isolated cluster-replacement-test
    just isolated bench-container-smoke
    just cluster-test
    just cluster-replacement-test
    just bench
    just bench-container
    just bench-cluster
    just bench-pr-local
    just bench-pr-local-quick
    just profile-cluster
    just profile-cluster-instrumented
    just bench-compare
    just bench-compare-cluster
    just bench-dashboard
    just bench-test
    just ci

The existing scripts/verify.sh command remains as a thin compatibility wrapper around just verify.

Contributions use Conventional Commits because pull-request titles become the
release-facing subjects after squash merges. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the format; GitHub Actions enforces it on pull requests and new commits to
`main`.

When multiple local processes, containers, or test suites need to run at the same time, use `just isolated <workflow>`. Each invocation gets its own Cargo target directory, temporary-file directory, benchmark artifact directory, and workflow-specific Docker resources. The supported workflows are listed by `python3 scripts/isolated.py --help`; failed runs retain their temporary state for diagnosis, while successful build state is removed and benchmark results remain under `benchmark-results/isolated/`. This is intentionally a named-workflow interface rather than a wrapper for arbitrary commands whose ports or external state are unknown.

The test suite includes core persistence and recovery tests, wire-format round-trip tests, and a network-level test that starts the real broker process and verifies acknowledgement state across restart. `just smoke` is the canonical local end-to-end test: it starts the broker itself and uses `runnelctl` to publish, consume, acknowledge, restart, and verify recovery. The benchmark suite measures durable publish and publish/poll/ack paths; benchmark results must always be interpreted with their durability settings and workload. See [docs/testing.md](docs/testing.md) for the interactive walkthrough and test layers.

`just bench-container` builds a resource-limited Runnel container and runs repeatable end-to-end workload scenarios for 100-byte and 1-KiB messages. It writes machine-readable results under the ignored `benchmark-results/` directory. See [scripts/benchmarks/README.md](scripts/benchmarks/README.md) for workload semantics and comparison limitations.

`just bench-cluster` builds the release broker and runs durable publish, non-grouped delivery, shared-consumer delivery, and restart-recovery scenarios against a real three-node cluster. `just profile-cluster` captures optional Linux `perf` call graphs for the broker processes under sustained traffic. Both workflows write ignored artifacts under `benchmark-results/`; profiling requires suitable Linux kernel permissions.

`just profile-cluster-instrumented` enables the opt-in Rust timing feature and records stage timing summaries for protocol handling, lock waits, storage, Raft quorum operations, state-machine application, and peer RPCs. The default build does not compile these timing calls. It is useful on hosts where `perf` is unavailable or restricted; combine `--features instrumentation` with the normal profile command when both internal timings and `perf` are available.

Evaluate benchmark requirements case by case. For a change intended to improve throughput, latency, or tail latency—or one that changes a hot path with a plausible runtime effect—run `just bench-pr-local`. It waits for the exclusive local benchmark lock, compares the current commit with `origin/main` on the same host inside the same 2-CPU/2-GiB Linux systemd user scope, alternates paired runs, and writes a report under `benchmark-results/pr-local/`. The authoritative command exits nonzero unless the throughput and p99 ranges become stable, so only a stable report supports an optimization claim. Performance-neutral changes need not run it unless runtime impact is plausible. `just bench-pr-local-quick` uses a shared lock and an explicit diagnostic override for one pair; do not use it as evidence. Full cluster, container, competitor, and profiling benchmarks use the exclusive lock. The local benchmark skips restart recovery by default to keep feedback practical; the longer clustered history benchmark includes recovery separately. Use the `just` recipes rather than invoking controllers directly so concurrent local benchmark sessions cannot contend for host resources.

`just bench-compare` builds Runnel and runs an isolated first-pass native-tool comparison against pinned Kafka, Redpanda, and NATS JetStream containers. It uses a 2 CPU/2 GiB broker and client budget by default because Redpanda's development container needs more than a 1 GiB cgroup. Results are written under the ignored `benchmark-results/` directory and must be read with the recorded measurement boundaries; this is an engineering baseline, not a final apples-to-apples claim. Missing pinned benchmark images are pulled automatically.

`just bench-compare-cluster` runs the verified first three-node RF=3 publish comparison against Kafka, Redpanda, and NATS JetStream with explicit resource limits. `just bench-dashboard` generates local benchmark history data from JSON results under `benchmark-results/`. GitHub Actions keeps the longer Runnel-only history suite on pushes to `main` and daily, and runs competitor comparisons weekly or manually; it does not run noisy PR benchmark jobs. Use same-host `just bench-pr-local` results in performance-sensitive pull requests; Runnel history is the primary long-term signal for evaluating optimizations, while competitor series are separate ranking evidence. Each history suite is aggregated by median while retaining observed ranges and compatible-run comparisons, and the generated data is appended to the `benchmark-history` branch. GitHub Pages serves the hand-authored dashboard in `docs/benchmarks/` from `main`; the dashboard reads the public history data directly from that branch.

The current protocol accepts one JSON request per TCP line. For example:

    {"op":"publish","stream":"events","payload":"hello","request_id":"optional-stable-id"}
    {"op":"poll","stream":"events","consumer":"worker"}
    {"op":"ack","stream":"events","consumer":"worker","offset":0}

Grouped delivery uses `poll_group` and `ack_group` requests with a consumer name, member name, and delivery token. These are provisional development-protocol operations.

Message responses include a delivery attempt while retry state is being tracked. Dead-letter records are available on the source stream's .dead-letter stream and preserve the original key and payload.

See [docs/product-fit.md](docs/product-fit.md) for the initial audience and product boundaries, [docs/architecture.md](docs/architecture.md) for the current technical boundaries, [docs/research/README.md](docs/research/README.md) for source-backed investigations, [docs/design/multi-raft-implementation-plan.md](docs/design/multi-raft-implementation-plan.md) for the proposed first clustered plan, [docs/backlog.md](docs/backlog.md) for intended next outcomes, and [docs/tech-debt.md](docs/tech-debt.md) for known implementation shortcuts. Repository operating guidance lives in AGENTS.md.

For dependency auditing, install cargo-audit and run:

    cargo install --locked cargo-audit
    just audit

## Docker

Build and run a local image:

    docker build -t runnel:dev .
    docker volume create runnel-data
    docker run --rm -p 4222:4222 -p 8080:8080 -v runnel-data:/var/lib/runnel runnel:dev

The Kubernetes manifest in deploy/kubernetes/runnel.yaml starts a three-node static Multi-Raft development cluster with independent persistent volumes. It does not provide TLS, authentication, upgrades, or production policy by itself; see [deploy/kubernetes/README.md](deploy/kubernetes/README.md).
