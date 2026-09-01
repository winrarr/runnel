---
name: parallel-worktrees
description: Coordinate independent coding and benchmark tasks in isolated Git worktrees with explicit file ownership and resource isolation. Use when parallel delegated work is authorized and tests or containers must not interfere.
---

# Parallel worktrees

Use this workflow only when the user has authorized parallel delegation. Parallelism is valuable only when tasks have disjoint responsibilities and isolated execution resources.

The root `AGENTS.md` change-run baseline is mandatory for every run. This skill
adds the same start and handoff checks to delegated workers; it does not limit
the rule to parallel work.

## Before spawning

- Identify the immediate local task and keep it on the critical path.
- Split work by responsibility and file ownership. Do not assign two workers overlapping source, test, workflow, or generated-output paths.
- Establish a committed baseline revision. Do not assume that uncommitted edits in the main worktree are visible in another worktree; if they matter, create a clearly identified local baseline or provide an explicit patch.
- Before spawning, fetch `origin/main`, record its revision, and inspect the latest `ci.yml` run for that SHA when GitHub access is available. Give every worker the baseline revision, its owned paths, its expected result, and the instruction not to revert unrelated work.
- Before spawning, give the user a short summary of each proposed worker's feature or outcome and primary evidence class, such as performance, correctness, reliability, or benchmark infrastructure. For performance-sensitive work, include an evidence-based expected direction and rough magnitude of change, or explicitly say that no direct performance change is expected or that the magnitude is unknown; do not invent precision.
- Tell every worker, including nested subagents, to read the repository root `AGENTS.md` before starting. Do not assume the delegated environment loads project instructions automatically.
- Give workers the recorded starting baseline and its default-branch CI result; they do not need to repeat that check before handoff. A newer `main` commit requires an update only when it is known to overlap the worker's paths or shared contracts, dependencies, generated output, or integration behavior; disjoint work may remain based on the original baseline when it is cleanly mergeable.

## Worktree and branch isolation

- Create one worktree outside the repository directory per task, with one branch per task.
- Keep the main worktree for integration and verification; workers must not edit it directly.
- Workers must inspect status and diff before committing and stage only their owned paths.
- Prefer one focused commit and one pull request per independently reviewable improvement.
- Do not rebase or update a pending branch solely because another disjoint pull request changed the base. Recheck the newest `origin/main` revision after merges and update the branch when the changed commits overlap or affect shared behavior; rerun relevant checks after any update.

## Test and benchmark isolation

Prefer the repository's executable isolation runner for supported workflows:

```text
just isolated
just isolated cluster-test
just isolated bench-cluster-smoke
just isolated bench-container-smoke
```

The runner creates a unique Cargo target directory, temporary-file directory,
benchmark artifact directory, and workflow-specific Docker resources. Use the
named workflows listed by `python3 scripts/isolated.py --help`; do not turn
manual port selection into a prerequisite for normal local verification.

For workflows not covered by the runner, every concurrent process-level test
or benchmark must have unique resources:

- allocate unique broker, HTTP, and peer ports rather than relying on fixed ports;
- use a unique data directory, temporary directory, benchmark output path, Docker project/network, container name, and volume name;
- use a unique `CARGO_TARGET_DIR` when concurrent builds would contend on artifacts or locks;
- bound CPU, memory, and worker counts explicitly; use separate CPU sets when comparing performance in parallel;
- ensure each workload cleans up child processes, containers, sockets, and temporary data on success and failure.

Shared Cargo registries are normally acceptable as caches, but shared target directories, generated benchmark files, and mutable broker data are not. Do not use a shared `benchmark-results/` path for concurrent writers.

Parallel runs are suitable for exploratory correctness checks and rough optimization feedback. Host CPU scheduling, disk bandwidth, page cache, and kernel socket resources are still shared, so authoritative latency or throughput comparisons must run sequentially or with explicitly isolated CPU and storage resources.

For any worker task that can affect throughput, latency, CPU, memory, batching,
I/O, or scheduling, follow [docs/benchmarking.md](../../../docs/benchmarking.md)
and first determine whether the standard benchmark meaningfully covers the PR's
changes. Run the canonical local benchmark before claiming an improvement. If
the standard benchmark does not meaningfully cover the PR, evaluate whether a
focused targeted benchmark would be relevant and feasible with reasonable
effort and controlled resources; when it is, the worker must run it before
claiming an improvement or recommending that an optimization PR merge. Use
`just bench-pr-local` after committing; if it is inconclusive, use
`just bench-pr-local-until-stable` to retry complete authoritative comparisons.
A one-pair command such as `just bench-pr-local-quick` is an explicit
diagnostic only. Never treat a hosted PR benchmark or concurrent worker
measurement as proof of a performance change. Do not claim or merge an
optimization from an inconclusive authoritative result; investigate or rerun
it under the same controlled conditions rather than selecting a favorable
sample. If no targeted benchmark is feasible, record the concrete blocker and
coverage gap in the handoff; do not make a performance claim or recommend
merging solely as an optimization until the changed path has appropriate
evidence.

## Worker instructions

Tell each worker to:

- stay inside its assigned worktree and write scope;
- read the repository root `AGENTS.md` before editing and follow its change-run baseline and handoff requirements;
- for non-trivial architectural changes, follow `AGENTS.md`'s requirement to compare relevant competitor or reference designs and primary research, and include the sources, differences, alternatives, hypotheses, and unresolved risks in the handoff;
- classify the work by one primary evidence class and optional secondary tags, follow the applicable gate in [docs/testing.md](../../../docs/testing.md), and do not use a classification to waive global safety, baseline, CI, pull-request, or cleanup requirements;
- use the repository's canonical `just` commands and existing benchmark harnesses;
- record the exact revision, workload, resource limits, isolation settings, and commands;
- for performance-sensitive work, determine whether the standard benchmark meaningfully covers the PR, run the local benchmark sequentially with a fixed CPU/memory budget, and record the actual repetition count, stability thresholds, and stable status; if standard coverage is insufficient, run a focused targeted benchmark when it is relevant and feasible, or record why no such benchmark can be run; treat an inconclusive authoritative run as unfinished evidence;
- distinguish a confirmed improvement from noise, a blocked run, and an inconclusive result;
- report changed files, correctness and crash-recovery considerations, test results, and remaining risks. Follow [docs/testing.md](../../../docs/testing.md) for the class-specific merge gate and [docs/benchmarking.md](../../../docs/benchmarking.md) for expected effects, non-performance improvements, benchmark applicability, exact findings, repetition and stability results, directional medians, outlier diagnostics, and the evidence-based recommendation to merge, revise, rerun, or defer. Include blocked or inconclusive results rather than omitting them;
- report the recorded baseline revision and whether the branch was refreshed;
- commit the result on its task branch when code is ready, or leave a clearly documented uncommitted patch when it is not.

When coordinating delegated work, the orchestrator must collect each worker's
expected effects, non-performance improvements, benchmark applicability and
findings, and recommendation and include them in the final status, review, or
pull-request handoff. A worker's missing, blocked, or inconclusive benchmark
report remains an explicit unresolved result; it must not be silently collapsed
into the orchestrator's own summary.

## Integration

Review each branch independently before integration. Check the diff against the recorded baseline, rerun focused tests in a clean worktree, and run the repository verification path. Do not merge an optimization solely because a microbenchmark improved: preserve durability, ordering, timeout, ambiguous-outcome, bounded-resource, and recovery guarantees.

Merge independently reviewable disjoint pull requests in parallel once their required pull-request checks pass. Coordinate or serialize changes that overlap in files, shared contracts, dependencies, generated output, or integration behavior. Never bypass required checks to compensate for a flaky test; diagnose whether the failure is in the implementation, test harness, environment, or resource isolation.

## Cleanup and handoff

After a worker is complete or cancelled, explicitly stop or close the worker and any nested workers before cleanup. Then confirm that the workers and their owned processes and containers are gone, preserve committed work and benchmark artifacts needed for review, and remove only clean temporary worktrees. A clean Git status is not sufficient. Do not delete a worktree containing uncommitted user changes or belonging to a worker that is still active; do not terminate unrelated processes.

The final handoff should state which branches or pull requests were integrated, which remain open or blocked, the exact verification status, and any unresolved resource or benchmark reliability issue.
