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
just isolated cluster-replacement-test
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

The clustered grouped-consumer policy uses the same command-line settings:

```text
cargo run -q -p runnel-server -- --engine raft --node-id 1 --cluster-name local --data-dir ./data-1 --peer 1=127.0.0.1:7101 --peer 2=127.0.0.1:7102 --peer 3=127.0.0.1:7103 --bootstrap --ack-timeout-ms 50 --max-delivery-attempts 2
```

The three-node process test exercises both grouped and non-grouped clustered paths through the public protocol. Their dead-letter transitions are committed with source progress in the stream data group.

## Verification layers

The Criterion suite includes durable publish, legacy publish/poll/ack, two-member shared-consumer, keyed shared-consumer, and local concurrency-scaling baselines. Interpret every result with its durability mode, message size, membership, and ordering-key distribution.

- `just test` runs workspace unit, integration, and benchmark-target tests.
- `just doc-test` runs Rust documentation tests.
- `just verify` runs formatting, Clippy, default-feature Rust tests, ShellCheck, benchmark-script tests, and a workspace build.
- `just integration` runs separate isolated smoke, clustered recovery, Docker image, and container benchmark-smoke steps. A caller may provide `CARGO_TARGET_DIR` to reuse compilation across the sequential smoke and cluster checks; temporary process, data, image, and benchmark resources remain isolated. CI also prebuilds the image with reusable Docker layers and skips the duplicate local image build.
- `just smoke` exercises the running process and CLI across a restart.
- `just cluster-test` starts three real Raft-backed broker processes and verifies quorum replication, grouped and non-grouped delivery through follower forwarding, reassignment after node failure, retry limits, dead-letter recovery, follower restart, leader election, post-failure recovery, and recovery metrics through the public protocol.
- `just cluster-replacement-test` explicitly enables the test-only permissive recovery feature and runs the experimental empty replacement-node snapshot recovery and interrupted snapshot transfer checks.
- `just bench-test` runs the benchmark normalization and dashboard tests.
- `just ci` runs verification, the smoke test, the container build, and the container benchmark smoke check.

Benchmark workflows, applicability, interpretation, and required handoff evidence are documented in [benchmarking.md](benchmarking.md). Workload semantics, comparison boundaries, and harness-specific options are documented in [scripts/benchmarks/README.md](../scripts/benchmarks/README.md).

## Merge evidence classes

Classify each independently reviewable pull request by its primary intended outcome before deciding what evidence is required. Add secondary tags when a change has another material concern, such as `hot-path`, `storage/recovery`, `public-contract`, `breaking`, `security`, or `deployment`. If a pull request contains independent outcomes, split it when practical; otherwise satisfy the evidence requirements for every applicable class.

This matrix sets the minimum evidence for the change's main claim. It does not relax the global requirements for safety invariants, crash recovery, default branch checks, required CI, pull-request delivery, or worker cleanup.

| Primary class | Evidence required before merge | Benchmark treatment |
| --- | --- | --- |
| Performance optimization | Identify the changed runtime path, expected effect, non-effects, workload, and resource dimensions. Run the authoritative comparison when the path is covered, or document why a targeted benchmark is infeasible or disproportionate. | Required for a quantified performance claim when feasible. If the result remains inconclusive, report the reason and do not claim a measured improvement; merge only when the change has independently strong correctness, resource, or operational value and the implementation evidence justifies accepting the uncertainty. Otherwise revise, rerun, or defer. |
| Correctness, reliability, operability, or resource safety | Reproduce the issue end to end when practical, then add focused behavior, failure, recovery, bound, timeout, shutdown, metrics, or real-process tests appropriate to the risk. Persistence, acknowledgement, redelivery, and recovery changes require crash/recovery coverage. Network changes require the real server process. | Use benchmarks as a diagnostic regression check when runtime impact is plausible. They are not normally a stability gate for a correctness or operability change. |
| Design or research | Compare relevant competitor or reference designs and primary research, explain the differences that matter to Runnel, record alternatives and unresolved risks, and place the result in `docs/research/` or `docs/design/`. Foundational accepted choices also require an ADR. | Not required for a design-only change. Benchmark an implementation only when it changes a runtime path. |
| Contract, compatibility, or migration | Add or update protocol/API/schema compatibility tests, interoperability fixtures, upgrade or migration checks, and explicit breaking-change or rollback details. Keep ambiguous outcomes and public guarantees explicit. | Required only when the contract change also affects a performance-sensitive path. |
| Tooling, CI, test, or benchmark infrastructure | Verify deterministic behavior, failure reporting, reproducibility, provenance, generated output, and the affected workflow. Update user-facing commands and documentation when the interface changes. | Required only when the tooling change changes broker runtime behavior or resource use. |
| Maintenance, security, dependency, deployment, or documentation | Run the relevant audit, dependency, configuration, deployment, link, formatting, or focused validation. For security and deployment changes, test the affected boundary rather than relying on compilation alone. | Required only when the change plausibly alters runtime behavior or resource cost. |

Every handoff must name the primary class and secondary tags, state expected effects and non-effects, identify evidence and coverage gaps, and recommend `merge`, `revise`, `rerun`, or `defer`. Performance evidence follows [benchmarking.md](benchmarking.md), including the exact revision, baseline, workload, resources, repetitions, stability result, directional medians, and outlier diagnostics when available. A worker reports the same fields even when benchmarking is not required or could not be completed; the orchestrator must relay every delegated report.

When a test depends on crash behavior, use a real process and persistent temporary storage. Keep mocks and unit tests for local domain logic, not as substitutes for process, filesystem, or protocol coverage.
