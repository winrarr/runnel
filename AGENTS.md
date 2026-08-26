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
- docs/product-fit.md: initial audience, representative workloads, product promise, non-goals, and validation needs.
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
- Evaluate benchmark requirements case by case. A change whose goal is to improve throughput, latency, or tail latency, or that changes a hot path with a plausible runtime effect, requires an authoritative `just bench-pr-local` run after committing. The helper compares the current revision with `origin/main` on the same host inside an explicit Linux CPU/memory scope and exits nonzero unless paired throughput and p99 measurements meet the stability thresholds. Rerun or investigate the environment until the result is stable before claiming or accepting a performance improvement. Performance-neutral changes need not run this benchmark unless their behavior plausibly affects runtime cost. `just bench-pr-local-quick` and explicit `--allow-inconclusive` runs are diagnostics only; hosted PR benchmarks are not proof.
- Treat anything that could improve throughput or latency as worth considering. Evaluate allocation, copying, lock scope, batching, I/O, scheduling, transport, and encoding effects when making changes, while preserving correctness, bounded resource use, and predictable tail latency. Benchmark material assumptions instead of optimizing on intuition alone.
- When a failure or performance limitation could arise from an architectural design choice, investigate relevant competitor designs and primary research before changing broker semantics, storage, replication, ordering, or recovery behavior. Record the evidence, alternatives, and unresolved risks in `docs/research/` or `docs/design/`, and capture the accepted consequence in an ADR before treating the change as foundational.
- Treat `just` recipes, development scripts, and CLI flags as part of the developer-facing interface. When changing a test, benchmark, or operational workflow, expose useful workload, wait, timeout, retry, isolation, and output controls when they have a real use; keep defaults sensible, document them, and test them. If a task requires a repeatable script or workaround, consider whether that behavior is useful enough to promote into the normal user-facing interface as a recipe, script, or CLI option. Prefer explicit options over hard-coded values, without adding speculative configuration.
- Keep network behavior covered by tests that start the real server process.
- Keep the pinned development toolchain separate from compatibility policy; do not infer a supported compiler floor from the pinned version.
- Use Conventional Commits for project commits and pull-request titles. Keep the type meaningful, use a scope when it clarifies ownership, and mark breaking changes explicitly with `!` and migration details.
- Deliver every independently reviewable change through its own pull request on a non-`main` branch. Never push directly to `main` or bypass repository rulesets and required checks.

## Change-run baseline and coordination

Every agent change run must check the default branch before doing work and
again before handing work off. Human contributors must follow the same rule
for every change run, as described in `CONTRIBUTING.md`, so local work does not
silently start from or finish against stale project state:

- At the beginning of every change run—including reviews, documentation or
  configuration changes, coordination, and delegated work—run `git fetch origin
  main`, record `git rev-parse origin/main`, and inspect the latest `ci.yml` run
  for that SHA when GitHub access is available. Treat that commit as the task
  baseline; do not assume the local `main` checkout is current.
- Before declaring the change run complete or opening or updating a pull
  request, fetch `origin/main` again and repeat the check. Inspect commits
  added since the recorded baseline. If `main` changed in owned paths, shared
  contracts, generated files, dependencies, or integration behavior, update the
  branch and rerun relevant checks. A disjoint branch may remain based on the
  earlier baseline when it is cleanly mergeable; record both revisions in the
  handoff.
- When work is delegated to disjoint worktrees, the coordinator gives every
  worker the same baseline revision and requires the same start/end checks.
  Workers report their baseline, the newest `origin/main` revision, and
  whether an update was needed. Do not make independent branches chase
  unrelated merges solely to satisfy branch age.
- Before merging, check the newest `origin/main` revision and its default-branch
  CI status again. Required pull-request checks still must pass, and the CI run
  on `main` after a merge is the final check of the combined state.

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
- just integration runs the same smoke, clustered, Docker, and container-smoke sequence used by the CI integration job.
- just run starts a local broker with data in ./data.
- just smoke starts a real broker and uses runnelctl to exercise publish, consume, acknowledgement, restart recovery, readiness, and metrics with temporary state.
- just isolated runs the default workspace test with a unique Cargo target, temporary directory, and benchmark artifact directory; pass a supported workflow such as `just isolated cluster-test`, `just isolated cluster-replacement-test`, `just isolated bench-container-smoke`, or `just isolated bench-cluster-smoke` for concurrent work.
- just cluster-test starts three real broker processes, exercises quorum replication, follower restart, leader failure, and recovery through the public protocol.
- just cluster-replacement-test runs the opt-in snapshot replacement experiment that depends on the test-only permissive recovery feature.
- just bench runs the authoritative Criterion performance benchmarks, including shared-consumer delivery and keyed-ordering baselines, under the exclusive host benchmark lock.
- just bench-container builds and benchmarks the broker image with explicit CPU and memory limits.
- just bench-container-smoke exercises the container benchmark path with a small workload for CI.
- just bench-cluster runs the real three-node clustered performance baseline.
- just bench-cluster-smoke exercises the clustered benchmark lifecycle with a small workload.
- just bench-pr-local waits for the exclusive host benchmark lock, then benchmarks the current commit and `origin/main` on the same host under the same default 2-CPU/2-GiB Linux systemd user scope, repeats paired three-node results until stability or a seven-pair maximum, writes paste-ready Markdown under `benchmark-results/pr-local/`, and exits nonzero unless the report is stable.
- just bench-pr-local-quick uses a shared lock with other diagnostic benchmark workflows and performs one paired run with an explicit diagnostic override; it must not be used to support an optimization claim.
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

.github/workflows/benchmarks.yml runs the longer Runnel-only single-node and three-node history suites on pushes to `main`, daily, and manually. `.github/workflows/benchmark-competitors.yml` runs the separate native and three-node competitor comparisons weekly or manually. Hosted PR benchmark workflows are intentionally absent because shared runners are too noisy to establish optimization evidence. For performance-sensitive pull requests, the same-host `just bench-pr-local` report is the primary optimization evidence. The Runnel history is the primary long-term optimization signal; competitor suites are separate ranking evidence. The history workflows keep raw and aggregated results as artifacts and append generated data to the `benchmark-history` branch. GitHub Pages serves the hand-authored `docs/benchmarks/` directory from `main` and reads the public history data at runtime. Treat `benchmark-history` as generated output; change the scripts, dashboard assets, and workflows rather than editing that branch manually.

## Knowledge routing

Put implementation behavior in code and tests, the initial audience and product boundaries in docs/product-fit.md, current technical boundaries in docs/architecture.md, source-backed investigations in docs/research/, unsettled alternatives and implementation proposals in docs/design/, durable accepted rationale in a dated decision record, external or user-mandated guardrails in docs/constraints.md, intended unfinished outcomes in docs/backlog.md, known implementation shortcuts in docs/tech-debt.md, verification workflows in docs/testing.md, and operational deployment guidance beside its deployment artifact. Put workflow changes in justfile and CI changes in .github/workflows. Remove stale guidance instead of appending exceptions.

When parallel delegated work is authorized, follow .codex/skills/parallel-worktrees/SKILL.md. Keep task ownership disjoint, isolate process and container resources, and treat concurrent performance measurements as exploratory unless CPU, storage, and workload interference are controlled.
