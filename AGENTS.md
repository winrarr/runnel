# Runnel project guide

Runnel is a Rust message broker intended to offer durable streams, low operational overhead, and a credible path from a single node to a distributed system. The current repository contains intentionally small single-node and early static-cluster vertical slices, not the complete product described in the product brief.

## Repository map

- crates/runnel-protocol: provisional line-delimited JSON request and response types. This is the boundary for future language clients.
- crates/runnel-client: reusable async persistent client for the provisional protocol.
- crates/runnel-engine: topology-free broker engine contract and shared messaging outcomes.
- crates/runnel-test-support: reusable engine-level contract assertions for local and future distributed implementations.
- crates/runnel-core: local broker engine, append-only durable stream log, consumer checkpoints, acknowledgements, and recovery.
- crates/runnel-raft: OpenRaft adapter with durable local storage, framed TCP peer transport, and the early static-cluster backend.
- crates/runnel-server: runnel broker process, TCP protocol server, health endpoints, and Prometheus-compatible metrics.
- crates/runnel-cli: runnelctl, a small development client for the current protocol.
- crates/runnel-core/benches: Criterion benchmarks for durable publish, legacy publish/poll/ack, and shared-consumer delivery paths.
- crates/runnel-server/tests: network-level protocol and restart tests.
- scripts/benchmarks/run.py: resource-limited container benchmark runner with machine-readable results.
- scripts/benchmarks/runtime.py: shared Docker lifecycle and sampled-container primitives for benchmark runners.
- scripts/benchmarks/cluster.py: real three-node clustered benchmark runner with native-process or bounded-container runtimes and machine-readable results.
- scripts/benchmarks/pr_local.py: same-host current-vs-default-branch clustered benchmark and Markdown PR report generator.
- scripts/benchmarks/resource_scope.py: Linux systemd user-scope wrapper for explicit native benchmark CPU and memory limits.
- scripts/benchmarks/profile.py: optional Linux `perf` workflow for clustered CPU hotspot profiles.
- scripts/benchmarks/compare.py: first-pass single-node and three-node native-tool comparison runner for Runnel, Kafka, Redpanda, and JetStream.
- scripts/benchmarks/pr_report.py: renders clustered and single-node Runnel pull-request benchmark artifacts as Markdown reports.
- scripts/benchmarks/aggregate.py: median aggregation and observed-range summaries for repeated benchmark runs.
- scripts/isolated.py: canonical isolated workflow runner for concurrent local tests and benchmarks.
- scripts/benchmarks/normalize.py: strips raw tool output and adds provenance for durable benchmark history.
- scripts/benchmarks/build_history.py: aggregates normalized benchmark runs into generated history data.
- docs/benchmarks/: hand-authored static benchmark dashboard served by GitHub Pages.
- scripts/benchmarks/README.md: benchmark scope, semantics, and comparison guidance.
- docs/architecture.md: current data flow and boundaries.
- docs/benchmarking.md: canonical benchmark applicability, interpretation, and handoff evidence policy.
- docs/product-fit.md: initial audience, representative workloads, product promise, non-goals, and validation needs.
- docs/design/: active architecture explorations, alternatives, and implementation plans; accepted choices belong in docs/decisions/.
- docs/research/: source-backed investigations, competitor comparisons, and measured evidence that inform design without becoming decisions by themselves.
- docs/decisions/: consequential decisions that should not be rediscovered from code.
- docs/backlog.md: explicitly intended product outcomes that are not implemented yet.
- docs/tech-debt.md: known implementation shortcuts, their impact, and retirement conditions.
- docs/testing.md: canonical local, interactive, integration, and test workflows.
- deploy/kubernetes/: illustrative single-node and three-node StatefulSet deployments.
- justfile: canonical Linux development command interface.
- scripts/smoke.sh: repeatable broker/CLI/restart smoke test.
- scripts/verify.sh: compatibility wrapper around just verify.
- .codex/skills/parallel-worktrees/SKILL.md: repository workflow for delegated work, coordinated refactors, isolated tests, and benchmark resource separation.

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
- Before merging, classify each independently reviewable change by one primary evidence class and optional secondary tags, then satisfy the applicable gate in [docs/testing.md](docs/testing.md). Use [docs/benchmarking.md](docs/benchmarking.md) for benchmark execution, interpretation, and reporting. Classification does not relax safety, compatibility, default-branch, CI, pull-request, or cleanup requirements.
- Every handoff must state expected effects and non-effects, evidence and coverage gaps, and an evidence-based recommendation to merge, revise, rerun, or defer. Coordinators must collect and relay these fields for every delegated worker, including blocked or inconclusive work.
- Treat anything that could improve throughput or latency as worth considering. Evaluate allocation, copying, lock scope, batching, I/O, scheduling, transport, and encoding effects when making changes, while preserving correctness, bounded resource use, and predictable tail latency. Benchmark material assumptions instead of optimizing on intuition alone.
- For any non-trivial design that could change broker semantics, storage, replication, ordering, recovery, or operational safety, compare relevant competitor or reference designs and primary research before implementation. Record direct sources, the differences that matter to Runnel, alternatives considered, hypotheses, and unresolved risks in `docs/research/` or `docs/design/`, and capture the accepted consequence in an ADR before treating the change as foundational.
- Treat refactoring as part of normal engineering judgment. While working in a subsystem, proactively identify structural improvements that would better support current or likely requirements. Agents may propose or implement a substantial or higher-risk refactor when the architectural benefit warrants it; do not reject a sound evolution solely because it exceeds the immediate feature scope. If the refactor is not timely, record a focused tech-debt item with its goal, rationale, constraints, and verifiable retirement criteria.
- Keep ADRs aligned with the accepted current state. Agents may revise existing ADRs when decisions or assumptions evolve; retain historical context only when it explains an important consequence, rejected alternative, migration, or compatibility constraint. Replace stale guidance instead of layering contradictory exceptions.
- Treat `just` recipes, development scripts, and CLI flags as part of the developer-facing interface. When changing a test, benchmark, or operational workflow, expose useful workload, wait, timeout, retry, isolation, and output controls when they have a real use; keep defaults sensible, document them, and test them. If a task requires a repeatable script or workaround, consider whether that behavior is useful enough to promote into the normal user-facing interface as a recipe, script, or CLI option. Prefer explicit options over hard-coded values, without adding speculative configuration.
- Keep network behavior covered by tests that start the real server process.
- Keep the pinned development toolchain separate from compatibility policy; do not infer a supported compiler floor from the pinned version.
- Use Conventional Commits for project commits and pull-request titles. Keep the type meaningful, use a scope when it clarifies ownership, and mark breaking changes explicitly with `!` and migration details.
- Deliver every independently reviewable change through its own pull request on a non-`main` branch. Never push directly to `main` or bypass repository rulesets and required checks.

## Change-run baseline and coordination

Every agent change run must check the default branch before doing work. Human
contributors must follow the same rule
for every change run, as described in `CONTRIBUTING.md`, so local work does not
silently start from stale project state:

Every agent, including the coordinator and delegated subagents, must read this
`AGENTS.md` before starting work. Coordinators must state that requirement
explicitly in worker prompts and must not rely on automatic project-instruction
loading.

- At the beginning of every change run—including reviews, documentation or
  configuration changes, coordination, and delegated work—run `git fetch origin
  main`, record `git rev-parse origin/main`, and inspect the latest `ci.yml` run
  for that SHA when GitHub access is available. Treat that commit as the task
  baseline; do not assume the local `main` checkout is current.
- When work is delegated to disjoint worktrees, the coordinator gives every
  worker the same baseline revision and requires the same starting-branch
  check. Workers report their baseline and whether the branch was refreshed.
  Do not make independent branches chase unrelated merges solely to satisfy
  branch age.
- Required pull-request checks still must pass before merging. Independently
  reviewable pull requests may be merged in parallel. Coordinate or serialize
  changes that overlap in files, shared contracts, dependencies, generated
  output, or integration behavior; coordinated architectural refactors may
  intentionally overlap when they follow domain responsibility and have an
  explicit integration plan.

When checking a remote baseline, compare the `headSha`, `status`, and
`conclusion` from
`gh run list --workflow ci.yml --branch main --limit 1 --json headSha,status,conclusion,url`
with the revision reported by `git rev-parse origin/main`; a successful older
run does not prove that the newest commit has passed.

## Canonical commands

Linux development uses `just` as the canonical command runner. Install it once with:

    cargo install --locked just

Run these from the repository root:

- just verify runs formatting, Clippy, all-target tests, documentation tests, ShellCheck, benchmark-script tests, and a workspace build.
- just ci runs verification and the full local integration sequence.
- just integration runs the same separate isolated smoke, clustered, Docker, single-node container-smoke, and three-node container-smoke sequence used by the CI integration job; callers may provide `CARGO_TARGET_DIR` to reuse compilation across the sequential smoke and cluster checks, and CI prebuilds the image with reusable Docker layers.
- just run starts a local broker with data in ./data.
- just smoke starts a real broker and uses runnelctl to exercise publish, consume, acknowledgement, restart recovery, readiness, and metrics with temporary state.
- just isolated runs the default workspace test with a unique Cargo target, temporary directory, and benchmark artifact directory; pass a supported workflow such as `just isolated cluster-test`, `just isolated cluster-replacement-test`, `just isolated bench-container-smoke`, or `just isolated bench-cluster-container-smoke` for concurrent work. An explicitly supplied `CARGO_TARGET_DIR` is for sequential workflows only.
- just cluster-test starts three real broker processes, exercises quorum replication, follower restart, leader failure, and recovery through the public protocol.
- just cluster-replacement-test runs the opt-in snapshot replacement experiment that depends on the test-only permissive recovery feature.
- just bench, just bench-container, just bench-cluster, and just bench-cluster-container run the documented local, single-node container, native clustered, and containerized clustered benchmark suites.
- just bench-container-smoke, just bench-cluster-smoke, and just bench-cluster-container-smoke exercise small benchmark lifecycles for CI and diagnostics.
- just bench-pr-local runs the authoritative current-versus-`origin/main` comparison; just bench-pr-local-until-stable retries complete inconclusive comparisons; just bench-pr-local-quick is diagnostic only. See [docs/benchmarking.md](docs/benchmarking.md).
- just profile-cluster captures optional Linux `perf` samples and reports for all clustered broker processes.
- just profile-cluster-instrumented builds the opt-in Rust timing instrumentation and records internal stage timings without requiring `perf` permissions; it uses the exclusive host benchmark lock.
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

The required CI path is .github/workflows/ci.yml. It runs the pinned toolchain checks, the supported real network integration test, and the container smoke build. The test-only replacement-recovery experiment is not part of the required CI path; run it explicitly with `just cluster-replacement-test` when investigating that recovery boundary. .github/workflows/security.yml audits the dependency lockfile on pull requests and weekly. Dependabot keeps Cargo and GitHub Actions dependencies visible for review.

.github/workflows/benchmarks.yml runs the longer Runnel-only single-node and three-node history suites daily and manually; it does not run on every `main` push. `.github/workflows/benchmark-competitors.yml` runs the separate native and three-node competitor comparisons weekly or manually. Hosted PR benchmark workflows are intentionally absent because shared runners are too noisy to establish optimization evidence. See [docs/benchmarking.md](docs/benchmarking.md) for local evidence and comparison policy. The history workflows keep raw and aggregated results as artifacts and append generated data to the `benchmark-history` branch. GitHub Pages serves the hand-authored `docs/benchmarks/` directory from `main` and reads the public history data at runtime. Treat `benchmark-history` as generated output; change the scripts, dashboard assets, and workflows rather than editing that branch manually.

## Knowledge routing

Put implementation behavior in code and tests, the initial audience and product boundaries in docs/product-fit.md, current technical boundaries in docs/architecture.md, source-backed investigations in docs/research/, unsettled alternatives and implementation proposals in docs/design/, durable accepted rationale in a dated decision record, external or user-mandated guardrails in docs/constraints.md, intended unfinished outcomes in docs/backlog.md, known implementation shortcuts in docs/tech-debt.md, verification workflows in docs/testing.md, benchmark evidence policy in docs/benchmarking.md, and operational deployment guidance beside its deployment artifact. Put workflow changes in justfile and CI changes in .github/workflows. Remove stale guidance instead of appending exceptions.

When parallel delegated work is authorized, follow .codex/skills/parallel-worktrees/SKILL.md. Divide independent work by domain responsibility, but allow coordinated refactors to overlap where that reflects the architecture rather than an artificial file boundary; identify an integration owner and reconcile shared changes before merging. Isolate process and container resources, and treat concurrent performance measurements as exploratory unless CPU, storage, and workload interference are controlled. Before removing a delegated worktree, explicitly stop or close its worker, including any nested workers, and verify that the worker and its owned processes and containers are gone. A clean Git status is not sufficient; preserve committed work and do not remove an active or dirty worktree.
