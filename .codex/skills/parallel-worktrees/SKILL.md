---
name: parallel-worktrees
description: Coordinate independent coding and benchmark tasks in isolated Git worktrees with explicit file ownership and resource isolation. Use when parallel delegated work is authorized and tests or containers must not interfere.
---

# Parallel worktrees

Use this workflow only when the user has authorized parallel delegation. Parallelism is valuable only when tasks have disjoint responsibilities and isolated execution resources.

## Before spawning

- Identify the immediate local task and keep it on the critical path.
- Split work by responsibility and file ownership. Do not assign two workers overlapping source, test, workflow, or generated-output paths.
- Establish a committed baseline revision. Do not assume that uncommitted edits in the main worktree are visible in another worktree; if they matter, create a clearly identified local baseline or provide an explicit patch.
- Give every worker the baseline revision, its owned paths, its expected result, and the instruction not to revert unrelated work.

## Worktree and branch isolation

- Create one worktree outside the repository directory per task, with one branch per task.
- Keep the main worktree for integration and verification; workers must not edit it directly.
- Workers must inspect status and diff before committing and stage only their owned paths.
- Prefer one focused commit and one pull request per independently reviewable improvement.
- Rebase or update a pending branch after another pull request changes the base, then rerun required checks before merging.

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

## Worker instructions

Tell each worker to:

- stay inside its assigned worktree and write scope;
- use the repository's canonical `just` commands and existing benchmark harnesses;
- record the exact revision, workload, resource limits, isolation settings, and commands;
- distinguish a confirmed improvement from noise, a blocked run, and an inconclusive result;
- report changed files, correctness and crash-recovery considerations, test results, benchmark results, and remaining risks;
- commit the result on its task branch when code is ready, or leave a clearly documented uncommitted patch when it is not.

## Integration

Review each branch independently before integration. Check the diff against the baseline, rerun focused tests in a clean worktree, and run the repository verification path. Do not merge an optimization solely because a microbenchmark improved: preserve durability, ordering, timeout, ambiguous-outcome, bounded-resource, and recovery guarantees.

Merge pull requests one at a time. After each merge, update remaining branches and rerun required checks because the base revision and required status checks have changed. Never bypass required checks to compensate for a flaky test; diagnose whether the failure is in the implementation, test harness, environment, or resource isolation.

## Cleanup and handoff

After a worker is complete, confirm its processes and containers are gone, preserve committed work and benchmark artifacts needed for review, and remove only clean temporary worktrees. Do not delete a worktree containing uncommitted user changes.

The final handoff should state which branches or pull requests were integrated, which remain open or blocked, the exact verification status, and any unresolved resource or benchmark reliability issue.
