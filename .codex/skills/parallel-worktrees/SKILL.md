---
name: parallel-worktrees
description: Coordinate authorized fixed or rolling pools of independent coding, refactor, and benchmark tasks in isolated Git worktrees with explicit responsibility and resource isolation.
---

# Parallel worktrees

Use this workflow only when the user has authorized parallel delegation. Parallelism is valuable when tasks have clear responsibilities and isolated execution resources; coordinated architectural refactors may intentionally overlap when that better matches the domain.

The root `AGENTS.md` change-run baseline is mandatory for every run. This skill
adds the same start and handoff checks to delegated workers; it does not limit
the rule to parallel work.

## Before spawning

- Identify the immediate local task and keep it on the critical path.
- Split independent work by responsibility, file ownership, and an explicit domain boundary. The refactoring and backlog/tech-debt policy is defined once in the repository root `AGENTS.md`; workers and coordinators must follow that policy. For an explicitly coordinated architectural refactor, overlapping paths are allowed when they reflect the domain; name an integration owner, explain the overlap, and define how shared changes will be reconciled.
- Establish a committed baseline before spawning. Fetch `origin/main`, record its revision, and inspect the latest `ci.yml` run for that SHA when GitHub access is available. If uncommitted edits matter, create a clearly identified local baseline or explicit patch.
- Give every worker the baseline revision, owned paths, expected result, and instruction not to revert unrelated work.
- Before spawning, give the user a short summary of each proposed worker's feature or outcome and primary evidence class, such as performance, correctness, reliability, or benchmark infrastructure. For performance-sensitive work, include a best-effort expectation of the likely direction and rough magnitude of change when possible, or explicitly say that no direct performance change is expected or that the magnitude is unclear. Label estimates as expectations rather than measured results; do not invent precision.
- A newer `main` commit requires a worker update when it overlaps the worker's paths or shared contracts, dependencies, generated output, or integration behavior; independent work may remain on the recorded baseline when it is cleanly mergeable.
- Treat implicit worktree allocation as a serialized critical section. Do not issue concurrent spawn or resume calls until each prior worker's worktree identity has been validated, unless all worktrees were explicitly provisioned beforehand.
- After each worker is provisioned and before it edits, verify `git worktree list --porcelain` and record a task-to-path-to-branch mapping. The coordinator worktree must remain on the default branch; every worker path must be distinct, outside the repository root, on its assigned branch, and at the recorded baseline. If any check fails, interrupt the worker before editing, preserve any patch, and provision a replacement worktree; do not switch branches inside a shared or ambiguous worktree.

## Worktree and branch isolation

- Create one worktree outside the repository directory per task, with one branch per task.
- Keep the main worktree for integration and verification; workers must not edit it directly.
- The first worker instruction must require a pre-edit identity report containing `pwd`, `git rev-parse --show-toplevel`, `git branch --show-current`, and `git rev-parse HEAD`. A worker must stop and notify the coordinator if the reported top-level is the coordinator repository, the branch is not its assigned branch, or the revision is not the supplied baseline.
- A resumed or restarted worker must receive a newly validated dedicated worktree unless its previous worktree is known to be isolated and is explicitly revalidated before editing.
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

- read the repository root `AGENTS.md` and this skill before editing; if spawning nested workers, pass the same requirement through; stay inside the assigned worktree and write scope; do not revert unrelated work;
- follow the single-source refactoring and planning-record policy in `AGENTS.md`, including within-domain refactors, cross-boundary tech debt, and handoff reporting;
- for non-trivial architectural changes, follow `AGENTS.md`'s requirement to compare relevant competitor or reference designs and primary research, and include the sources, differences, alternatives, hypotheses, and unresolved risks in the handoff;
- classify the work by one primary evidence class and optional secondary tags, follow the applicable gate in [docs/testing.md](../../../docs/testing.md), and do not use a classification to waive global safety, baseline, CI, pull-request, or cleanup requirements;
- use the repository's canonical `just` commands and existing benchmark harnesses;
- apply the backlog and tech-debt update requirements in `AGENTS.md`, and include the resulting update or explicit no-update rationale in the handoff;
- record the exact revision, workload, resource limits, isolation settings, and commands;
- for performance-sensitive work, follow the benchmark policy above, run the applicable benchmark before claiming an improvement, and report the exact workload, resources, repetitions, stability thresholds, and result; treat inconclusive evidence as unfinished;
- distinguish a confirmed improvement from noise, a blocked run, and an inconclusive result;
- report changed files, expected effects and non-effects, correctness and crash-recovery considerations, evidence, coverage gaps, and an evidence-based recommendation to merge, revise, rerun, or defer. Include blocked or inconclusive results rather than omitting them;
- report the recorded baseline revision and whether the branch was refreshed;
- commit the result on its task branch when code is ready, or leave a clearly documented uncommitted patch when it is not.

When coordinating delegated work, the orchestrator must collect each worker's
expected effects and non-effects, evidence and coverage gaps, recommendation,
baseline, and refactor/planning assessment. Preserve blocked or inconclusive
results in the final status and do not turn an omitted planning update into an
untracked coordinator follow-up.

## Lifecycle and invocation modes

Interpret these concise requests as predefined coordination modes:

- “run parallel-worktrees with N subagents”: start a fixed pool of exactly N
  workers and do not start replacements.
- “run parallel-worktrees with N subagents and rolling pool”: start up to N
  workers and replace a worker only after its recommended PR actually merges.
  Worker completion, PR creation, or green checks do not trigger replacement.

For both modes, N is the requested initial and maximum worker count unless the
user gives a different concurrency limit. Record the count, mode, baseline,
task-to-worktree mapping, and stop condition. In rolling mode, a slot may remain
unfilled while a recommended PR awaits checks or merge, or while work is deferred.

After spawning, retain worker identifiers and keep the run active until every
requested worker reaches a final status or is explicitly cancelled. Use grouped,
non-busy-polling waits, check open PRs periodically, and report each completion
with its branch or PR, result, evidence, gaps, and recommendation. Report blocked
or inconclusive work explicitly.

When a worker recommends merge, review its branch and merge only after required
checks pass. In rolling mode, refresh the default-branch baseline and start one
replacement from a newly validated worktree after the actual merge, unless the
user has asked to stop. When a worker does not recommend merge, report the branch,
evidence, gaps, and recommendation immediately; do not replace it automatically.

A stop request prevents all replacements. Let already-started workers reach a
final status when practical and process their recommendations. Auto-merge remains
opt-in; if explicitly requested but unsupported by the repository, leave the PR
open and report that limitation.

## Integration

Review each branch independently before integration. Check the diff against the recorded baseline, rerun focused tests in a clean worktree, and run the repository verification path. Do not merge an optimization solely because a microbenchmark improved: preserve durability, ordering, timeout, ambiguous-outcome, bounded-resource, and recovery guarantees.

Merge independently reviewable pull requests in parallel once their required pull-request checks pass. Coordinate or serialize changes that overlap in files, shared contracts, dependencies, generated output, or integration behavior; overlapping architectural refactors require integration review and must not be merged independently just because their pull requests are individually green. Never bypass required checks to compensate for a flaky test; diagnose whether the failure is in the implementation, test harness, environment, or resource isolation.

- If delegation dirties the coordinator worktree or mixes task files, stop the affected workers before changing branches. Preserve the mixed state in a recoverable stash or explicit patch, restore the coordinator worktree to the default branch, and re-home only verified task files into dedicated worktrees. Never reset or discard the mixed state to repair allocation.

## Cleanup and handoff

After a worker is complete or cancelled, explicitly stop or close the worker and any nested workers before cleanup. Then confirm that the workers and their owned processes and containers are gone, preserve committed work and benchmark artifacts needed for review, and remove only clean temporary worktrees. A clean Git status is not sufficient. Do not delete a worktree containing uncommitted user changes or belonging to a worker that is still active; do not terminate unrelated processes.

The final handoff should state which branches or pull requests were integrated, which remain open or blocked, the exact verification status, and any unresolved resource or benchmark reliability issue.
