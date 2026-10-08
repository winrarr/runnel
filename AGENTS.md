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
- scripts/benchmarks/matrix.py: sequential clustered workload/fault matrix runner with per-case artifacts and machine-readable results.
- scripts/benchmarks/pr_local.py: same-host current-vs-default-branch clustered benchmark and Markdown PR report generator.
- scripts/benchmarks/resource_scope.py: Linux systemd user-scope wrapper for explicit native benchmark CPU and memory limits.
- scripts/benchmarks/profile.py: optional Linux `perf` workflow for clustered CPU hotspot profiles.
- scripts/benchmarks/compare.py: first-pass single-node and three-node native-tool comparison runner for Runnel, Kafka, Redpanda, and JetStream.
- scripts/benchmarks/pr_report.py: renders clustered and single-node Runnel pull-request benchmark artifacts as Markdown reports.
- scripts/benchmarks/aggregate.py: median aggregation and observed-range summaries for repeated benchmark runs.
- scripts/isolated.py: canonical isolated workflow runner for concurrent local tests and benchmarks.
- scripts/benchmarks/normalize.py: strips raw tool output and adds provenance for durable benchmark history.
- scripts/benchmarks/build_history.py: aggregates normalized benchmark runs into generated history data.
- scripts/product_fit.py: repeatable local reference-workload harness that records product-fit evidence packages.
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
- .codex/skills/parallel-worktrees/SKILL.md and WORKER.md: repository workflow for parallel isolated changes, pull request handoffs, coordinated refactors, isolated tests, and benchmark resource separation.

## Sources of truth and boundaries

Rust code and tests define current behavior. The wire protocol is provisional, and Runnel has no backward-compatibility requirement. Evolve or replace the protocol when the intended design warrants it. Do not expose storage paths, offsets, or physical layout as public concepts beyond what the current protocol needs.

Runnel is pre-release and has no external deployments. Backward compatibility with prior Runnel releases, clients, wire protocols, or persisted formats is not a product goal. Prefer one current protocol and storage model; do not retain obsolete paths, fallback readers, dual-stack behavior, shims, or upgrade work solely to support older versions. Revise accepted decisions and planning records when they impose compatibility work that no longer serves a product outcome. Preserve durability and crash guarantees for the currently supported behavior, and fail clearly on unsupported old state rather than silently opening it as empty or deleting it. Reassess this policy if external deployments begin.

Future-work documentation—including design, research, backlog, and tech-debt records—is advisory planning material, not current behavior, executable instructions, or an accepted commitment. Do not follow proposed APIs, invariants, steps, or acceptance criteria blindly: inspect code and tests, current ADRs, constraints, and the task request, and distinguish observed facts, hypotheses, target outcomes, and accepted decisions. An unresolved choice is a prompt for design work, not a blanket reason to defer an authorized outcome. Resolve necessary choices from the evidence and project goals, then record consequential decisions in an ADR before treating them as foundational. Seek a second opinion only when a genuine owner-level trade-off remains or the task does not authorize the materially different product commitment.

The semantic engine contract is owned by runnel-engine. Reusable behavior assertions for that contract belong in runnel-test-support and must not depend on storage or topology. The local durable log format is owned by runnel-core. Changes to either require focused tests and a decision record when they alter current public behavior, crash behavior, or future engine boundaries. Keep local file I/O and consumer-state persistence inside runnel-core; keep transport concerns in runnel-server and client ergonomics in runnel-cli. Consensus-specific code must stay behind a distributed-engine adapter and must not become part of the public protocol model.

The local engine uses per-stream locks and bounded blocking-storage execution; clustered operations follow per-group replicated state transitions. These are vertical-slice implementations, not a final performance architecture. Benchmark material changes to concurrency and persistence. Evolve the public model when the intended design calls for it; do not preserve it solely for backward compatibility. The local and early clustered engines have an initial shared-consumer path with transient members, out-of-order durable acknowledgements, per-key delivery gates, fenced delivery tokens, expiry-based redelivery, persisted attempt limits, and optional dead-letter streams; backoff, provenance, and final policy semantics remain future work.

Backlog and tech-debt registers are inventories, not execution queues. Select work using the task request, dependencies, risk, and learning value. Early correctness work, reusable verification, and long-horizon research are intentional investments. Exploratory documents should explain concrete approaches and tradeoffs without turning hypothetical APIs, file layouts, or implementation steps into requirements.

## Engineering rules

- When an authorized outcome requires a new behavior or architecture choice, investigate viable options and select the one best supported by project goals and evidence. Do not ask for guidance merely because the choice changes behavior or lacks an existing ADR. Record the rationale, alternatives, and residual risks; seek a second opinion only when materially different product commitments remain comparably supported, authorization is insufficient, or evidence is insufficient to choose responsibly.
- Preserve at-least-once behavior: an acknowledgement advances durable consumer state only after the state update succeeds.
- Do not claim stronger durability or ordering guarantees than the implementation and tests establish.
- Make ambiguous outcomes explicit in protocol responses rather than silently retrying operations.
- Keep stream and consumer names validated before they become filesystem paths.
- Prefer small domain types and explicit state transitions over transport-specific logic in the core.
- Add a focused crash/recovery test before changing persistence, acknowledgement, or redelivery behavior.
- Before merging, classify each independently reviewable change by one primary evidence class and optional secondary tags, then satisfy the applicable gate in [docs/testing.md](docs/testing.md). Use [docs/benchmarking.md](docs/benchmarking.md) for benchmark execution, interpretation, and reporting. Classification does not relax safety, current behavior guarantees, default-branch, CI, pull-request, or cleanup requirements.
- Every handoff must state the outcome and expected effects and non-effects; primary evidence class and optional secondary tags; verification commands and results, gaps, and relevant end-to-end coverage; unresolved risks, including blocked or inconclusive evidence; refactor and planning-record assessment, including any no-update rationale; and an evidence-based recommendation to merge, revise, rerun, or defer.
- Treat anything that could improve throughput or latency as worth considering. Evaluate allocation, copying, lock scope, batching, I/O, scheduling, transport, and encoding effects when making changes, while preserving correctness, bounded resource use, and predictable tail latency. Benchmark material assumptions instead of optimizing on intuition alone.
- For any non-trivial design that could change broker semantics, storage, replication, ordering, recovery, or operational safety, compare relevant competitor or reference designs and primary research before implementation. Record direct sources, the differences that matter to Runnel, alternatives considered, hypotheses, and unresolved risks in `docs/research/` or `docs/design/`, and capture the accepted consequence in an ADR before treating the change as foundational.
- When research or review surfaces a Runnel-relevant finding, assess whether it is reasonably implementable in the near future given the evidence, project goals, dependencies, and risks. If so, create or update a backlog item for an intended outcome that is not implemented, or a tech-debt item for a current shortcut. State the goal, rationale, constraints, and verifiable acceptance or retirement criteria, and record supporting evidence with a link to related research when applicable. Keep speculative or longer-horizon findings in research or design and state why they are deferred. Give substantive findings an explicit disposition even when no tracker update is warranted.
- Treat refactoring as normal judgment: inspect touched code and its surroundings for clarity, safety, performance, and changeability, then make the cleanest change that advances the outcome. Risk alone is not a reason to defer; use proportionate evidence and report residual risks. Before crossing an ownership boundary, document the goal, rationale, scope, effects, evidence, and risks, and agree on an integration plan. Track worthwhile out-of-scope improvements as focused tech debt in the same change, with a goal, rationale, constraints, and verifiable retirement criteria; task narrowness alone is no reason to omit them. A no-update decision requires inspection to find no concrete improvement worth recording; explain it in the handoff. Keep backlog and tech-debt records aligned with material outcome changes.
- Keep ADRs aligned with the accepted current state. Agents may revise existing ADRs when decisions or assumptions evolve; retain historical context only when it explains an important consequence, rejected alternative, migration, or compatibility constraint. Replace stale guidance instead of layering contradictory exceptions.
- Treat `just` recipes, development scripts, and CLI flags as part of the developer-facing interface. When changing a test, benchmark, or operational workflow, expose useful workload, wait, timeout, retry, isolation, and output controls when they have a real use; keep defaults sensible, document them, and test them. If a task requires a repeatable script or workaround, consider whether that behavior is useful enough to promote into the normal user-facing interface as a recipe, script, or CLI option. Prefer explicit options over hard-coded values, without adding speculative configuration.
- Use unit and topology-free engine-contract tests for broad, deterministic coverage of normal behavior, boundaries, and failure cases; keep that coverage in required verification.
- Keep always-on CI end-to-end coverage small and representative. Add focused real-process tests for specific network, crash/recovery, or integration risks that lower-level tests cannot establish, and run the relevant case when that boundary changes. Keep unrelated and combinatorial process scenarios in targeted or opt-in verification rather than adding them to every pull request's required CI. This scope rule does not waive real-server coverage for network behavior or focused crash/recovery evidence when required by the change.
- Keep the pinned development toolchain separate from compatibility policy; do not infer a supported compiler floor from the pinned version.
- Use Conventional Commits for project commits and pull-request titles, with a meaningful lowercase type and a scope when it clarifies ownership. Squash merges make the PR title the release-facing commit subject. Mark breaking changes with `!` and include migration details.
- Deliver every independently reviewable change through its own pull request on a non-`main` branch. Never push directly to `main` or bypass repository rulesets and required checks.

## Change-run baseline

Every contributor must read and follow this file before starting a change. Every change run—including reviews, documentation or configuration changes—uses one recorded baseline from the default branch. The run lead establishes and shares that baseline and its verification state; all participants use the supplied baseline instead of repeating run-level checks. For an individual change, the person doing the work leads the run. Scheduled default-branch checks are periodic health evidence and may lag the current revision; record their revision and state, but use passing required checks on the exact pull-request head as the merge gate. Do not wait for a new default-branch run before starting or merging work.

Do not update a task or PR branch just because the default branch advanced or paths overlap. Merge ready PRs against the current default after review and exact-head required checks pass. Update a branch only to resolve an actual merge conflict or meet a concrete integration need, such as overlapping worker changes that cannot be independently reviewed or verified. Record the reason and scope; for parallel work, agree on the integration plan before updating. Keep the update minimal and rerun affected checks. A repository gate alone is not a reason to update; report its exact blocker.

For parallel worktrees, follow `.codex/skills/parallel-worktrees/SKILL.md` for the workflow. Shared engineering and evidence policies remain in this file.

## Canonical commands

Linux development uses `just` as the canonical command runner. Install it once with:

    cargo install --locked just

Run these from the repository root:

- just verify runs formatting, Clippy, all-target tests, documentation tests, ShellCheck, benchmark-script tests, and a workspace build.
- just ci runs verification and the full local integration sequence.
- just integration runs the isolated process smoke, focused multi-stream peer-forwarding process smoke, Docker image setup, single-node container-smoke, three-node container-smoke, and three-node peer-forwarding container census smoke used by the CI integration job; the `cluster_smoke` process recovery test is owned by `just verify` and is not duplicated here. Callers may provide `CARGO_TARGET_DIR` for process smoke builds, and CI prebuilds one `runnel:dev` image with reusable Docker layers for all three container workflows.
- just run starts a local broker with data in ./data.
- just smoke starts a real broker and uses runnelctl to exercise publish, consume, acknowledgement, restart recovery, readiness, and metrics with temporary state.
- just product-fit builds the broker and runs the pre-registered local background-work and event/replay workloads, writing an opt-in evidence package under benchmark-results/product-fit/.
- just isolated runs the default workspace test with a unique Cargo target, temporary directory, and benchmark artifact directory; pass a supported workflow such as `just isolated cluster-test`, `just isolated cluster-replacement-test`, `just isolated bench-container-smoke`, `just isolated bench-cluster-container-smoke`, `just isolated bench-cluster-peer-forwarding-smoke`, or `just isolated bench-cluster-peer-forwarding-container-smoke` for concurrent work. An explicitly supplied `CARGO_TARGET_DIR` is for sequential workflows only.
- just cluster-test starts three real broker processes, exercises quorum replication, follower restart, leader failure, and recovery through the public protocol.
- just cluster-replacement-test runs the opt-in snapshot replacement experiment that depends on the test-only permissive recovery feature.
- just bench, just bench-container, just bench-cluster, just bench-cluster-container, and just bench-cluster-matrix run the documented local, single-node container, native clustered, containerized clustered, and clustered matrix benchmark suites.
- just bench-container-smoke, just bench-cluster-smoke, just bench-cluster-peer-forwarding-smoke, just bench-cluster-matrix-smoke, just bench-cluster-container-smoke, and just bench-cluster-peer-forwarding-container-smoke exercise small benchmark lifecycles for CI and diagnostics. The peer-forwarding smokes are also available through the corresponding `just isolated` workflows for concurrent local work; the container variant requires Linux and Docker and gates on direct per-node socket counts at both settled scenario boundaries.
- just bench-pr-local runs the authoritative current-versus-`origin/main` comparison; just bench-pr-local-until-stable retries complete inconclusive comparisons; just bench-pr-local-quick is diagnostic only. See [docs/benchmarking.md](docs/benchmarking.md).
- just profile-cluster captures optional Linux `perf` samples and reports for all clustered broker processes.
- just profile-cluster-instrumented builds the opt-in Rust timing instrumentation and records internal stage timings without requiring `perf` permissions; it uses the exclusive host benchmark lock.
- just bench-compare builds Runnel and runs the documented first-pass comparison against Kafka, Redpanda, and JetStream.
- just bench-compare-cluster builds the Runnel image and runs the documented RF=3 durable-publish comparison against Runnel, Kafka, Redpanda, and JetStream clusters; results remain experimental and non-ranking.
- just bench-dashboard builds local history data from JSON files under benchmark-results/.
- just bench-test runs the benchmark normalization and dashboard tests.
- python3 scripts/benchmarks/pr_report.py renders a pull-request benchmark JSON artifact as a Markdown report.
- just audit runs cargo-audit when it is installed.
- ./scripts/verify.sh is a thin compatibility wrapper for just verify.

Use `just isolated <workflow>` when running process-heavy tests or benchmarks concurrently. The runner owns a unique Cargo target directory, temporary-file directory, benchmark artifact directory, and workflow-specific Docker image or network. It only supports named workflows because arbitrary commands may still bind fixed ports or use external state that the runner cannot identify.

Do not add a second task runner. Keep README commands and CI wired to just recipes. If the command graph changes, update AGENTS.md, README.md, justfile, and .github/workflows/ together.

## Verification and automation

The required CI path is .github/workflows/ci.yml. It runs the pinned toolchain checks, the supported real network integration test, and the container smoke build daily on the default branch. On pull requests, `dorny/paths-filter` uses `.github/ci-paths.yaml` to run Verify and integration/container checks only when their inputs change; their named required check contexts remain present and report success when skipped. The workflow-level triggers remain unconditional for pull requests so required statuses are not left pending. There are no checks triggered by pushes to the default branch; passing required checks on the exact pull-request head are the merge gate. `.github/workflows/security.yml` applies the same path filtering to its required `audit` check for pull requests and continues to audit on schedule or manual dispatch. The test-only replacement-recovery experiment is not part of the required CI path; run it explicitly with `just cluster-replacement-test` when investigating that recovery boundary. Dependabot keeps Cargo and GitHub Actions dependencies visible for review.

.github/workflows/benchmarks.yml runs the longer Runnel-only single-node and three-node history suites daily and manually; it does not run on every `main` push. `.github/workflows/benchmark-competitors.yml` runs the separate native and three-node competitor comparisons weekly or manually. Hosted PR benchmark workflows are intentionally absent because shared runners are too noisy to establish optimization evidence. See [docs/benchmarking.md](docs/benchmarking.md) for local evidence and comparison policy. The history workflows keep raw and aggregated results as artifacts and append generated data to the `benchmark-history` branch. GitHub Pages serves the hand-authored `docs/benchmarks/` directory from `main` and reads the public history data at runtime. Treat `benchmark-history` as generated output; change the scripts, dashboard assets, and workflows rather than editing that branch manually.

## Knowledge routing

Put implementation behavior in code and tests, the initial audience and product boundaries in docs/product-fit.md, current technical boundaries in docs/architecture.md, source-backed investigations in docs/research/, unsettled alternatives and implementation proposals in docs/design/, durable accepted rationale in a dated decision record, external or user-mandated guardrails in docs/constraints.md, intended unfinished outcomes in docs/backlog.md, known implementation shortcuts in docs/tech-debt.md, verification workflows in docs/testing.md, benchmark evidence policy in docs/benchmarking.md, and operational deployment guidance beside its deployment artifact. Put workflow changes in justfile and CI changes in .github/workflows. Remove stale guidance instead of appending exceptions.

Parallel worktree orchestration follows .codex/skills/parallel-worktrees/SKILL.md. Explicitly coordinated architectural efforts may overlap when that reflects the architecture rather than an artificial file boundary; identify an integration owner and reconcile shared changes before merging. Treat concurrent performance measurements as exploratory unless CPU, storage, and workload interference are controlled.
