# Runnel

Runnel is a Rust message broker focused on low latency, predictable resource usage, durable delivery, and simple operation. It is designed to feel closer to starting a local infrastructure tool than operating a distributed event platform.

The initial product is aimed at small engineering teams that need durable background work and application-event streams, want a genuinely useful single-node deployment, and need a credible path to highly available and larger deployments without carrying avoidable broker-topology complexity in application code. See [docs/product-fit.md](docs/product-fit.md) for the intended audience, workloads, product boundaries, and evidence still required.

This repository currently provides the first vertical slice:

- a single-node broker process;
- durable append-only stream storage;
- multiple independent durable consumers;
- shared consumers that distribute work between named members;
- at-least-once polling and acknowledgements;
- explicit one-record logical-offset replay that leaves ordinary progress unchanged;
- per-key ordering and stale-delivery rejection for local and clustered shared consumers;
- retry attempt tracking and optional dead-letter streams in local and clustered grouped delivery;
- redelivery after acknowledgement timeout or broker restart;
- health and basic Prometheus-compatible metrics;
- a small development CLI;
- a reusable async client with persistent connections and bounded request timeouts;
- an early three-node Multi-Raft development backend with any-node client routing;
- Docker and Kubernetes starting points.

The workspace also contains `runnel-engine`, the shared semantic engine contract, and `runnel-raft`, an early static Multi-Raft backend. `--engine raft` enables versioned durable Raft/state-machine files, TLS-protected peer transport, topology-free client forwarding, replicated shared-consumer ownership, per-consumer retry delay, attempt limits and dead-letter streams, and a three-node development cluster. The backend is not yet production-complete: dynamic membership, scalable placement, backoff and dead-letter provenance, replica-consistent application security policy, production deployment automation, and broader failure semantics remain unfinished.

The workspace includes reusable `runnel-client` and `runnel-test-support` crates. The client provides persistent sequential request/response transport with bounded connection, write, response-size, and response timeouts for negotiated protocol v2; the test-support crate contains storage- and topology-independent assertions for the Engine contract.

Retention and compression remain planned product work. Bounded publish and consume batches preserve per-record outcomes without batch atomicity. Static-cluster peer traffic uses mutual TLS with explicit per-node credentials. The application listener supports negotiated v2, TLS 1.3, and bearer-token authorization for the local engine; its default loopback development mode is plaintext. `runnelctl` does not yet expose client trust-root or credential-file options, and application security configuration is rejected with the Raft engine until replica policy consistency is implemented. The separate HTTP health and metrics listener remains cleartext and unauthenticated. Protocol v2 is the only supported application wire protocol; prior Runnel wire versions are not a compatibility target. Broker-wide retry settings remain the fallback for local and clustered delivery; `configure_consumer` and `inspect_consumer` provide durable per-consumer acknowledgement timeouts, attempt limits, and fixed retry delays while preserving expiry fencing and dead-letter recovery without exposing internal consumer state.

## Quick start

Start the broker in one terminal:

    cargo run -p runnel-server -- --data-dir ./data

Use the CLI in another terminal:

    cargo run -p runnel-cli -- create-stream events
    cargo run -p runnel-cli -- publish events "hello from runnel"
    cargo run -p runnel-cli -- consume events worker
    cargo run -p runnel-cli -- ack events worker 0
    cargo run -p runnel-cli -- replay events worker 0

To share work between local worker processes, use one consumer name and a distinct member name for each worker. The grouped consume response includes a delivery token that must be supplied when acknowledging:

    cargo run -p runnel-cli -- consume jobs workers --member worker-a
    cargo run -p runnel-cli -- ack jobs workers 0 --member worker-a --delivery-token <token-from-consume>

Configure and inspect a durable policy for one stream and consumer pair:

    cargo run -p runnel-cli -- configure-consumer events worker 5000 --max-delivery-attempts 5 --retry-delay-ms 1000
    cargo run -p runnel-cli -- inspect-consumer events worker

Both commands return a `consumer_policy` JSON response. For these values, it includes:

    {
      "type": "consumer_policy",
      "stream": "events",
      "consumer": "worker",
      "version": 1,
      "configured": true,
      "ack_timeout_ms": 5000,
      "max_delivery_attempts": 5,
      "retry_delay_ms": 1000
    }

`version` advances when any value changes; repeating the same configuration is idempotent. An unconfigured consumer reports `configured: false` and `version: 0`, with policy values showing the broker-wide timeout and attempt-limit fallback and a zero retry delay. Omitting `--max-delivery-attempts` when configuring sets that consumer's attempt limit to `null` (no limit), rather than inheriting the broker-wide attempt limit. The timeout and retry delay are in milliseconds; each accepts zero through seven days, independently. A specified attempt limit must be positive. Configuration applies only to an existing stream and the named consumer; other consumers on the stream are unaffected. After a lease expires, the retry delay starts when a poll or stale acknowledgement first durably observes that expiry. While waiting, the offset's ordering key stays reserved; unrelated eligible work can proceed. See [ADR 0027](docs/decisions/0027-consumer-scoped-retry-policy.md) and [ADR 0033](docs/decisions/0033-fixed-consumer-retry-delay.md) for the policy semantics.

The broker listens on 127.0.0.1:4222. Health endpoints and metrics listen on 127.0.0.1:8080:

    curl http://127.0.0.1:8080/health/live
    curl http://127.0.0.1:8080/health/ready
    curl http://127.0.0.1:8080/metrics

To demonstrate restart recovery, publish a message, consume it without acknowledging, stop and restart the broker with the same data directory, then consume with the same consumer name. The message is delivered again because the checkpoint did not advance.

When max-delivery-attempts is set, a message that reaches the limit is copied to a derived dead-letter stream with its key and payload preserved. The name uses the .dead-letter suffix, with a bounded hashed fallback for long source names. The acknowledgement timeout controls when the next attempt becomes eligible. The clustered path commits the dead-letter record and source progress in the same replicated data-group operation. New local moves preserve a stable internal identity so a retry or reopen can reuse a completed target append before advancing source progress. The local operation still spans two durable records; legacy records and incomplete I/O failure coverage mean operators should continue tolerating duplicates. See [TD-017](docs/tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records).

## Development

The supported development environment is Linux. The repository uses a Cargo workspace and just as its canonical command runner:

    cargo install --locked just
    just verify

Useful workflows:

    just run
    just smoke
    just product-fit
    just product-fit --workload background_work
    just isolated
    just isolated cluster-test
    just isolated cluster-replacement-test
    just isolated bench-container-smoke
    just isolated bench-cluster-smoke
    just isolated bench-cluster-peer-forwarding-smoke
    just isolated bench-cluster-peer-forwarding-container-smoke
    just isolated bench-cluster-matrix-smoke
    just isolated bench-cluster-container-smoke
    just cluster-test
    just cluster-replacement-test
    just bench
    just bench-container
    just bench-container-smoke
    just bench-cluster
    just bench-cluster-smoke
    just bench-cluster-peer-forwarding-smoke
    just bench-cluster-peer-forwarding-container-smoke
    just bench-cluster-matrix
    just bench-cluster-matrix-smoke
    just bench-cluster-container
    just bench-cluster-container-smoke
    just bench-pr-local
    just bench-pr-local-until-stable
    just bench-pr-local-quick
    just profile-cluster
    just profile-cluster-instrumented
    just bench-compare
    just bench-compare-cluster
    just bench-dashboard
    just bench-test
    just ci

The existing scripts/verify.sh command remains as a thin compatibility wrapper around just verify.

`just bench-compare-cluster` builds the Runnel image and runs an opt-in three-node durable-publish comparison for Runnel, Kafka, Redpanda, and JetStream. It is an engineering baseline, not a cross-product ranking; Runnel's host-side benchmark client is not cgroup-limited by the configured client budget.

The required pull-request CI gate is a two-branch DAG: `Verify` and `Integration`
run in parallel and both must pass. `Verify` owns the real three-node
`cluster_smoke` test; `Integration` owns the process smoke and container smoke
workflows and reuses one prebuilt image for its single-node and three-node
container checks, including the peer-forwarding socket-census smoke.

Contributions use Conventional Commits because pull-request titles become the
release-facing subjects after squash merges. See [AGENTS.md](AGENTS.md) for the
format; GitHub Actions enforces it on pull requests and new commits to `main`.

When multiple local processes, containers, or test suites need to run at the same time, use `just isolated <workflow>`. Each invocation gets its own Cargo target directory, temporary-file directory, benchmark artifact directory, and workflow-specific Docker resources. The supported workflows are listed by `python3 scripts/isolated.py --help`; failed runs retain their temporary state for diagnosis, while successful build state is removed and benchmark results remain under `benchmark-results/isolated/`. This is intentionally a named-workflow interface rather than a wrapper for arbitrary commands whose ports or external state are unknown.

The test suite includes core persistence and recovery tests, wire-format round-trip tests, and a network-level test that starts the real broker process and verifies acknowledgement state across restart. `just smoke` is the canonical local end-to-end test: it starts the broker itself and uses `runnelctl` to publish, consume, acknowledge, restart, and verify recovery. `just product-fit` runs the two pre-registered local reference workloads through the public protocol and writes an evidence package under the ignored `benchmark-results/product-fit/` directory; use `--workload` to run only one. It is an opt-in evidence workflow, not a production SLO or CI gate. See [docs/testing.md](docs/testing.md) for the interactive walkthrough and test layers.

Benchmark workflows and interpretation are documented in [docs/benchmarking.md](docs/benchmarking.md). See [scripts/benchmarks/README.md](scripts/benchmarks/README.md) for harness semantics and comparison limitations.

The TCP application listener negotiates v2 with the `RNLN` preface and Protobuf
frames. Use `runnelctl` for the current development operations:

    cargo run -p runnel-cli -- create-stream events
    cargo run -p runnel-cli -- publish events hello
    cargo run -p runnel-cli -- consume events worker
    cargo run -p runnel-cli -- ack events worker 0
    cargo run -p runnel-cli -- replay events worker 0
    cargo run -p runnel-cli -- configure-consumer events worker 5000 --max-delivery-attempts 5 --retry-delay-ms 1000
    cargo run -p runnel-cli -- inspect-consumer events worker

Grouped delivery uses `consume` with `--member` and `ack` with both
`--member` and `--delivery-token`. The CLI currently connects to the default
loopback development listener; it does not yet configure TLS trust roots or a
bearer credential for a secured listener. See
[ADR 0031](docs/decisions/0031-protocol-v2-contract.md) for the wire contract
and [ADR 0035](docs/decisions/0035-first-application-client-security.md) for
application listener security behavior and its remaining boundaries.

For a local-engine application listener on a non-loopback address, provide
`--app-tls-cert <path>`, `--app-tls-key <path>`, and
`--credential-policy <path>` together. Startup validates these runtime files
before binding and fails closed on invalid configuration. The Raft engine
rejects application TLS/authentication settings until policy consistency across
replicas is implemented. The separate HTTP listener is not protected by these
application credentials. The explicitly named
`--insecure-development-listen` flag is for isolated development only.

Protocol frame limits default to 1 MiB for requests (`--max-request-bytes`) and
65 MiB for responses (`--max-response-bytes`). The request limit can be raised
up to 64 MiB or lowered to at least 1 KiB; the response limit can be lowered to
at least 1 KiB. These limits are negotiated per connection, and a response
that cannot fit is rejected before the operation has an effect.

Replay reads exactly one inclusive logical offset and does not create an
ordinary delivery or advance the consumer checkpoint. Its response has no
delivery token; an unavailable offset returns `history_unavailable` with the
available offset range. Replay sessions, time selectors, retention floors,
and replay acknowledgements are not implemented yet.

Message responses include a delivery attempt while retry state is being tracked. Consumer policies are durable and pinned per delivery; unconfigured consumers use the broker-wide fallback. Dead-letter records are available on the source stream's derived dead-letter stream and preserve the original key and payload.

See [docs/product-fit.md](docs/product-fit.md) for the initial audience and product boundaries, [docs/architecture.md](docs/architecture.md) for the current technical boundaries, [docs/benchmarking.md](docs/benchmarking.md) for benchmark policy, [docs/research/README.md](docs/research/README.md) for source-backed investigations, [docs/design/multi-raft-implementation-plan.md](docs/design/multi-raft-implementation-plan.md) for the first clustered plan, [docs/backlog.md](docs/backlog.md) for intended next outcomes, and [docs/tech-debt.md](docs/tech-debt.md) for known implementation shortcuts. Repository operating guidance lives in AGENTS.md.

For dependency auditing, install cargo-audit and run:

    cargo install --locked cargo-audit
    just audit

## Docker

Build and run a local image:

    docker build -t runnel:dev .
    docker volume create runnel-data
    docker run --rm -p 127.0.0.1:4222:4222 -v runnel-data:/var/lib/runnel runnel:dev --listen 0.0.0.0:4222 --insecure-development-listen

The image defaults to loopback listeners. This local-only invocation opens the
broker inside the container for Docker port forwarding while binding the host
port to loopback. It explicitly enables unauthenticated plaintext for isolated
local development; do not expose it to an untrusted network. The HTTP health
and metrics listener remains separate, unauthenticated, and unexposed here.

The Kubernetes manifest in deploy/kubernetes/runnel.yaml starts a three-node static Multi-Raft development cluster with independent persistent volumes. The manifest supplies peer TLS file paths but requires operator-provided per-pod credentials. It explicitly enables unauthenticated plaintext on the broker Service for isolated development. It is not suitable for production or untrusted networks and does not provide application or HTTP security; see [deploy/kubernetes/README.md](deploy/kubernetes/README.md).
