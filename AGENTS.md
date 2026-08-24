# Runnel project guide

Runnel is a Rust message broker intended to offer durable streams, low operational overhead, and a credible path from a single node to a distributed system. The current repository contains intentionally small single-node and early static-cluster vertical slices, not the complete product described in the product brief.

## Repository map

- crates/runnel-protocol: provisional line-delimited JSON request and response types. This is the boundary for future language clients.
- crates/runnel-engine: topology-free broker engine contract and shared messaging outcomes.
- crates/runnel-test-support: reusable engine-level contract assertions for local and future distributed implementations.
- crates/runnel-core: local broker engine, append-only durable stream log, consumer checkpoints, acknowledgements, and recovery.
- crates/runnel-raft: OpenRaft adapter with durable local storage, framed TCP peer transport, and the early static-cluster backend.
- crates/runnel-server: runnel broker process, TCP protocol server, health endpoints, and Prometheus-compatible metrics.
- crates/runnel-cli: runnelctl, a small development client for the current protocol.
- crates/runnel-core/benches: Criterion benchmarks for durable publish, legacy publish/poll/ack, and shared-consumer delivery paths.
- crates/runnel-server/tests: network-level protocol and restart tests.
- scripts/benchmarks/run.py: resource-limited container benchmark runner with machine-readable results.
- scripts/benchmarks/cluster.py: real three-node clustered benchmark runner with machine-readable results.
- scripts/benchmarks/profile.py: optional Linux `perf` workflow for clustered CPU hotspot profiles.
- scripts/benchmarks/compare.py: first-pass single-node and three-node native-tool comparison runner for Runnel, Kafka, Redpanda, and JetStream.
- scripts/benchmarks/pr_report.py: renders the short Runnel pull-request benchmark artifact as a Markdown report.
- scripts/benchmarks/aggregate.py: median aggregation and observed-range summaries for repeated benchmark runs.
- scripts/isolated.py: canonical isolated workflow runner for concurrent local tests and benchmarks.
- scripts/benchmarks/normalize.py: strips raw tool output and adds provenance for durable benchmark history.
- scripts/benchmarks/build_history.py: aggregates normalized benchmark runs into generated history data.
- docs/benchmarks/: hand-authored static benchmark dashboard served by GitHub Pages.
- scripts/benchmarks/README.md: benchmark scope, semantics, and comparison guidance.
- docs/architecture.md: current data flow and boundaries.
- docs/design/: active architecture explorations, alternatives, and proposed implementation plans that are not accepted decisions.
- docs/research/: source-backed investigations, competitor comparisons, and measured evidence that inform design without becoming decisions by themselves.
- docs/decisions/: consequential decisions that should not be rediscovered from code.
- docs/backlog.md: explicitly intended product outcomes that are not implemented yet.
- docs/tech-debt.md: known implementation shortcuts, their impact, and retirement conditions.
- docs/testing.md: canonical local, interactive, integration, and benchmark workflows.
- deploy/kubernetes/: illustrative single-node and three-node StatefulSet deployments.
- justfile: canonical Linux development command interface.
- scripts/smoke.sh: repeatable broker/CLI/restart smoke test.
- scripts/verify.sh: compatibility wrapper around just verify.
- .codex/skills/parallel-worktrees/SKILL.md: repository workflow for disjoint delegated work, isolated tests, and benchmark resource separation.

## Sources of truth and boundaries

Rust code and tests define current behavior. The wire protocol is intentionally provisional until compatibility policy is decided. Do not expose storage paths, offsets, or physical layout as public concepts beyond what the current protocol already requires.

The semantic engine contract is owned by runnel-engine. Reusable behavior assertions for that contract belong in runnel-test-support and must not depend on storage or topology. The local durable log format is owned by runnel-core. Changes to either require focused tests and a decision record when they affect compatibility, crash behavior, or future engine boundaries. Keep local file I/O and consumer-state persistence inside runnel-core; keep transport concerns in runnel-server and client ergonomics in runnel-cli. Consensus-specific code must stay behind a distributed-engine adapter and must not become part of the public protocol model.

The current implementation serializes broker operations behind one in-process lock. Treat that as a vertical-slice limitation, not as a target performance architecture. Benchmark before replacing it, and preserve the public model while changing internals. The local and early clustered engines have an initial shared-consumer path with transient members, out-of-order durable acknowledgements, per-key delivery gates, fenced delivery tokens, expiry-based redelivery, persisted attempt limits, and optional dead-letter streams; backoff, provenance, and final policy semantics remain future work.

## Engineering rules

- Preserve at-least-once behavior: an acknowledgement advances durable consumer state only after the state update succeeds.
- Do not claim stronger durability or ordering guarantees than the implementation and tests establish.
- Make ambiguous outcomes explicit in protocol responses rather than silently retrying operations.
- Keep stream and consumer names validated before they become filesystem paths.
- Prefer small domain types and explicit state transitions over transport-specific logic in the core.
- Add a focused crash/recovery test before changing persistence, acknowledgement, or redelivery behavior.
- Treat benchmarks as part of performance work; include the durability mode and workload in every result.
- Treat anything that could improve throughput or latency as worth considering. Evaluate allocation, copying, lock scope, batching, I/O, scheduling, transport, and encoding effects when making changes, while preserving correctness, bounded resource use, and predictable tail latency. Benchmark material assumptions instead of optimizing on intuition alone.
- Treat `just` recipes, development scripts, and CLI flags as part of the developer-facing interface. When changing a test, benchmark, or operational workflow, expose useful workload, wait, timeout, retry, isolation, and output controls when they have a real use; keep defaults sensible, document them, and test them. If a task requires a repeatable script or workaround, consider whether that behavior is useful enough to promote into the normal user-facing interface as a recipe, script, or CLI option. Prefer explicit options over hard-coded values, without adding speculative configuration.
- Keep network behavior covered by tests that start the real server process.
- Keep the pinned development toolchain separate from compatibility policy; do not infer a supported compiler floor from the pinned version.
- Use Conventional Commits for project commits and pull-request titles. Keep the type meaningful, use a scope when it clarifies ownership, and mark breaking changes explicitly with `!` and migration details.

## Canonical commands

Linux development uses `just` as the canonical command runner. Install it once with:

    cargo install --locked just

Run these from the repository root:

- just verify runs formatting, Clippy, all-target tests, documentation tests, ShellCheck, benchmark-script tests, and a workspace build.
- just ci runs verification, the real broker smoke test, the Docker build, and the container benchmark smoke check.
- just run starts a local broker with data in ./data.
- just smoke starts a real broker and uses runnelctl to exercise publish, consume, acknowledgement, restart recovery, readiness, and metrics with temporary state.
- just isolated runs the default workspace test with a unique Cargo target, temporary directory, and benchmark artifact directory; pass a supported workflow such as `just isolated cluster-test` or `just isolated bench-container-smoke` for concurrent work.
- just cluster-test starts three real broker processes, exercises quorum replication, follower restart, leader failure, and recovery through the public protocol.
- just bench runs the Criterion performance benchmarks, including shared-consumer delivery and keyed-ordering baselines.
- just bench-container builds and benchmarks the broker image with explicit CPU and memory limits.
- just bench-container-smoke exercises the container benchmark path with a small workload for CI.
- just bench-cluster runs the real three-node clustered performance baseline.
- just bench-cluster-smoke exercises the clustered benchmark lifecycle with a small workload.
- just profile-cluster captures optional Linux `perf` samples and reports for all clustered broker processes.
- just profile-cluster-instrumented builds the opt-in Rust timing instrumentation and records internal stage timings without requiring `perf` permissions.
- just bench-compare builds Runnel and runs the documented first-pass comparison against Kafka, Redpanda, and JetStream.
- just bench-compare-cluster runs the documented RF=3 durable-publish comparison against three-node Kafka, Redpanda, and JetStream clusters.
- just bench-dashboard builds local history data from JSON files under benchmark-results/.
- just bench-test runs the benchmark normalization and dashboard tests.
- python3 scripts/benchmarks/pr_report.py renders a pull-request benchmark JSON artifact as a Markdown report.
- just audit runs cargo-audit when it is installed.
- ./scripts/verify.sh is a thin compatibility wrapper for just verify.

Use `just isolated <workflow>` when running process-heavy tests or benchmarks concurrently. The runner owns a unique Cargo target directory, temporary-file directory, benchmark artifact directory, and workflow-specific Docker image or network. It only supports named workflows because arbitrary commands may still bind fixed ports or use external state that the runner cannot identify.

Do not add a second task runner. Keep README commands and CI wired to just recipes. If the command graph changes, update AGENTS.md, README.md, justfile, and .github/workflows/ together.

## Verification and automation

The required CI path is .github/workflows/ci.yml. It runs the pinned toolchain checks, the real network integration test, and the container smoke build. .github/workflows/security.yml audits the dependency lockfile on pull requests and weekly. Dependabot keeps Cargo and GitHub Actions dependencies visible for review.

.github/workflows/benchmarks.yml runs the longer Runnel-only single-node and three-node history suites on pushes to `main`, daily, and manually. `.github/workflows/benchmark-competitors.yml` runs the separate native and three-node competitor comparisons weekly or manually. `.github/workflows/benchmark-pr.yml` produces a short, read-only Runnel artifact for pull requests, and `.github/workflows/benchmark-pr-comment.yml` runs the same short workload against the default branch and turns both results into an informational PR comment with relative deltas. The Runnel history is the primary optimization signal; competitor suites are separate ranking evidence. The history workflows keep raw and aggregated results as artifacts and append generated data to the `benchmark-history` branch. GitHub Pages serves the hand-authored `docs/benchmarks/` directory from `main` and reads the public history data at runtime. Treat `benchmark-history` as generated output; change the scripts, dashboard assets, and workflows rather than editing that branch manually.

## Knowledge routing

Put implementation behavior in code and tests, current boundaries in docs/architecture.md, source-backed investigations in docs/research/, unsettled alternatives and implementation proposals in docs/design/, durable accepted rationale in a dated decision record, external or user-mandated guardrails in docs/constraints.md, intended unfinished outcomes in docs/backlog.md, known implementation shortcuts in docs/tech-debt.md, verification workflows in docs/testing.md, and operational deployment guidance beside its deployment artifact. Put workflow changes in justfile and CI changes in .github/workflows. Remove stale guidance instead of appending exceptions.

When parallel delegated work is authorized, follow .codex/skills/parallel-worktrees/SKILL.md. Keep task ownership disjoint, isolate process and container resources, and treat concurrent performance measurements as exploratory unless CPU, storage, and workload interference are controlled.
