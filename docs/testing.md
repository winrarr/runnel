# Testing and local operation

The canonical local end-to-end check is:

```text
just smoke
```

This is a real broker test, not a mock or an in-process shortcut. It builds the server and CLI, allocates temporary local ports and storage, starts `runnel`, and uses `runnelctl` to create streams, publish, consume, acknowledge, replay an acknowledged record without changing ordinary consumer progress, and share work between members of a consumer. It also configures and inspects a consumer retry policy. After restarting the broker, it verifies redelivery and durable consumer state, including the policy values and version, checks readiness and metrics, and removes its temporary data.

Run it whenever changing storage, delivery, protocol, process startup, shutdown, or deployment behavior. CI invokes the same recipe.

## Product-fit reference workloads

Run the opt-in reference workload harness after building the broker:

```text
just product-fit
just product-fit --workload background_work
```

The harness uses a real local broker and the negotiated v2 Protobuf protocol. It
loads the pre-registered manifest at
`docs/research/product-fit-manifests/local-reference.json` and writes an
immutable run package under `benchmark-results/product-fit/<run-id>/` (which is
ignored by Git): manifest, request transcript, message ledger, restart-separated
Prometheus snapshots, resource samples, latency distributions, budget checks,
and broker logs. The command exits non-zero when an automated check fails.
These are representative engineering budgets, not product SLOs; an intended
user must still complete the worksheet in the product-fit validation note.

## Concurrent local workflows

Use the isolation runner when more than one process-heavy workflow needs to run at once:

```text
just isolated
just isolated cluster-test
just isolated cluster-replacement-test
just isolated bench-container-smoke
just isolated bench-cluster-matrix-smoke
just isolated bench-cluster-container-smoke
just isolated bench-cluster-peer-forwarding-smoke
just isolated bench-cluster-peer-forwarding-container-smoke
```

Every invocation receives a unique Cargo target directory, temporary-file directory, and benchmark artifact directory. The smoke and cluster workflows already allocate ephemeral loopback ports; container benchmarks additionally use unique container names and private Docker networks. `bench-cluster-peer-forwarding-smoke` and `bench-cluster-peer-forwarding-container-smoke` are focused checks of the opt-in forwarding scenario with two streams, eight persistent follower clients, 20 total measured publishes, and two warmup messages per stream. The container variant also requires direct per-node established socket endpoint counts after setup warmup and after measured forwarding; it validates all three node samples and their procfs provenance before returning success. Successful benchmark artifacts remain under `benchmark-results/isolated/<run-id>/`, while failed runs retain their temporary build and process state so the failure can be inspected. Use only the named workflows shown by `python3 scripts/isolated.py --help`; arbitrary commands may use resources that cannot be isolated automatically.

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

The two members share work under `workers`; a different consumer name receives an independent copy. Both local and clustered grouped paths serialize messages with the same key and reject stale delivery tokens after redelivery. For a real three-node process test, run `just cluster-test`; it also verifies grouped delivery through follower forwarding, reassignment after a node failure, and clustered dead-letter recovery after the configured attempt limit. The experimental empty-replica snapshot replacement test is separate: run `just cluster-replacement-test` when explicitly investigating that recovery boundary.

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

The clustered grouped-consumer policy uses the same retry and attempt-limit
settings. Each Raft process also needs its own peer certificate and key plus
the explicit cluster trust bundle. Peer sockets require TLS 1.3; the public
client and HTTP listeners remain separate plaintext boundaries. For example,
start node 1 with protected credential files already provisioned at these
paths:

```text
cargo run -q -p runnel-server -- --engine raft --node-id 1 --cluster-name local --data-dir ./data-1 --peer-listen 127.0.0.1:7101 --cluster-node 1=127.0.0.1:7101 --cluster-node 2=127.0.0.1:7102 --cluster-node 3=127.0.0.1:7103 --peer-trust-bundle /run/runnel/peer-tls/ca.pem --peer-cert-chain /run/runnel/peer-tls/node-1.crt --peer-private-key /run/runnel/peer-tls/node-1.key --bootstrap --ack-timeout-ms 50 --max-delivery-attempts 2
```

Provision each other node with a distinct leaf certificate/key whose SAN
matches its configured node ID and the exact `local` cluster name. Do not put
private key contents in command arguments or the broker data directory. See
[ADR 0032](decisions/0032-static-cluster-peer-mutual-tls.md) and the
[Kubernetes peer credential guidance](../deploy/kubernetes/README.md) for
identity, cutover, and restart-based trust-rotation details.

The three-node process test exercises both grouped and non-grouped clustered paths through the public protocol. Their dead-letter transitions are committed with source progress in the stream data group.

## Verification layers

The Criterion suite includes durable publish, legacy publish/poll/ack, two-member shared-consumer, keyed shared-consumer, and local concurrency-scaling baselines. Interpret every result with its durability mode, message size, membership, and ordering-key distribution.

- `just test` runs workspace unit, integration, and benchmark-target tests.
- `just doc-test` runs Rust documentation tests.
- `just verify` runs formatting, Clippy, default-feature Rust tests (including the real-process `cluster_smoke` test), ShellCheck, benchmark-script tests, and a workspace build.
- `just integration` runs the isolated process smoke test, focused multi-stream peer-forwarding process smoke, Docker image setup, single-node container benchmark-smoke, three-node container benchmark-smoke, and three-node peer-forwarding container census smoke. The clustered process recovery test is owned by `just verify`, so it is not run a second time here. A caller may provide `CARGO_TARGET_DIR` for process smoke builds; temporary process, data, image, and benchmark resources remain isolated. CI prebuilds one `runnel:dev` image with reusable Docker layers and reuses it for all three container smoke workflows.
- `just smoke` exercises the running process and CLI across a restart.
- `just product-fit` runs the pre-registered local background-work and event/replay workloads and records an evidence package; it is intentionally opt-in and is not a CI gate.
- `just cluster-test` starts three real Raft-backed broker processes and verifies quorum replication, grouped and non-grouped delivery through follower forwarding, reassignment after node failure, retry limits, dead-letter recovery, follower restart, leader election, post-failure recovery, and recovery metrics through the public protocol.
- `just cluster-replacement-test` explicitly enables the test-only permissive recovery feature and runs the experimental empty replacement-node snapshot recovery and interrupted snapshot transfer checks.
- `just bench-cluster-peer-forwarding-smoke` runs the bounded multi-stream follower-forwarding scenario against three real broker processes; it is a correctness/lifecycle smoke, not performance evidence.
- `just bench-cluster-peer-forwarding-container-smoke` runs the same bounded forwarding workload in three broker containers and fails unless both settled-boundary samples contain available non-negative socket counts for all three nodes with direct procfs provenance; it is a diagnostic lifecycle check, not performance evidence.
- `just bench-test` runs the Python script test suite, including benchmark normalization and dashboard tests.
- `just ci` runs `just verify` and `just integration`; integration exercises the isolated process smoke, peer-forwarding process smoke, and single-node, three-node, and three-node peer-forwarding container smoke workflows, building an image unless a prebuilt integration image is supplied.

## Pull-request CI path selection

The required CI and security workflows trigger on every pull request so their required check contexts are always reported. Do not add workflow-level `paths` or `paths-ignore` filters to them: GitHub leaves checks pending when the entire workflow is skipped. Instead, `.github/workflows/ci.yml` and `.github/workflows/security.yml` use `dorny/paths-filter` with the shared rules in `.github/ci-paths.yaml` to conditionally run their required jobs. A skipped required job reports success under its existing check name. Conventional Commit title and commit-subject checks also run on every pull request because they do not depend on changed file paths.

Changes to Rust crates, Cargo manifests or lockfile, scripts, build configuration, or relevant workflow files run the affected verification or integration jobs. Dependency files and security workflow changes run `audit`. Documentation and research-only pull requests still run path detection and pull-request title validation, while unaffected runtime and audit jobs skip. If path detection fails, the dependent jobs run as a safe fallback. A daily scheduled run executes the full verification and integration jobs against the latest default-branch revision. There are no checks triggered by pushes to the default branch; passing required checks on the exact pull-request head are the merge gate. Scheduled or manually dispatched security audits remain unconditional.

Benchmark workflows, applicability, interpretation, and required handoff evidence are documented in [benchmarking.md](benchmarking.md). Workload semantics, comparison boundaries, and harness-specific options are documented in [scripts/benchmarks/README.md](../scripts/benchmarks/README.md).

## Merge evidence classes

Classify each independently reviewable pull request by its primary intended outcome before deciding what evidence is required. Add secondary tags when a change has another material concern, such as `hot-path`, `storage/recovery`, `public-contract`, `breaking`, `security`, or `deployment`. If a pull request contains independent outcomes, split it when practical; otherwise satisfy the evidence requirements for every applicable class.

This matrix sets the minimum evidence for the change's main claim. It does not relax the global requirements for safety invariants, crash recovery, default branch checks, required CI for affected paths, pull-request delivery, or worker cleanup.

| Primary class | Evidence required before merge | Benchmark treatment |
| --- | --- | --- |
| Performance optimization | Identify the changed runtime path, expected effect, non-effects, workload, and resource dimensions. Run the authoritative comparison when the path is covered, or document why a targeted benchmark is infeasible or disproportionate. | Required for a quantified performance claim when feasible. If the result remains inconclusive, report the reason and do not claim a measured improvement; merge only when the change has independently strong correctness, resource, or operational value and the implementation evidence justifies accepting the uncertainty. Otherwise revise, rerun, or defer. |
| Correctness, reliability, operability, or resource safety | Reproduce the issue end to end when practical, then add focused behavior, failure, recovery, bound, timeout, shutdown, metrics, or real-process tests appropriate to the risk. Persistence, acknowledgement, redelivery, and recovery changes require crash/recovery coverage. Network changes require the real server process. | Use benchmarks as a diagnostic regression check when runtime impact is plausible. They are not normally a stability gate for a correctness or operability change. |
| Design or research | Compare relevant competitor or reference designs and primary research, explain the differences that matter to Runnel, record alternatives and unresolved risks, and place the result in `docs/research/` or `docs/design/`. Foundational accepted choices also require an ADR. | Not required for a design-only change. Benchmark an implementation only when it changes a runtime path. |
| Public contract or schema change | Test the intended current protocol/API/schema behavior and any interoperability required between current components. Backward compatibility with prior Runnel versions is not required; do not add old-version compatibility or migration checks solely to preserve prior behavior. Document intentional breaking changes and keep ambiguous outcomes and current public guarantees explicit. | Required only when the contract change also affects a performance-sensitive path. |
| Tooling, CI, test, or benchmark infrastructure | Verify deterministic behavior, failure reporting, reproducibility, provenance, generated output, and the affected workflow. Update user-facing commands and documentation when the interface changes. | Required only when the tooling change changes broker runtime behavior or resource use. |
| Maintenance, security, dependency, deployment, or documentation | Run the relevant audit, dependency, configuration, deployment, link, formatting, or focused validation. For security and deployment changes, test the affected boundary rather than relying on compilation alone. | Required only when the change plausibly alters runtime behavior or resource cost. |

Every handoff must name the primary class and secondary tags, state expected effects and non-effects, identify evidence and coverage gaps, and recommend `merge`, `revise`, `rerun`, or `defer`. Performance evidence follows [benchmarking.md](benchmarking.md), including the exact revision, baseline, workload, resources, repetitions, stability result, directional medians, and outlier diagnostics when available. A worker reports the same fields even when benchmarking is not required or could not be completed; the orchestrator must relay every delegated report.

When a test depends on crash behavior, use a real process and persistent temporary storage. Keep mocks and unit tests for local domain logic, not as substitutes for process, filesystem, or protocol coverage.
