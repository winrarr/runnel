---
name: parallel-worktrees
description: Coordinate authorized fixed or rolling pools of independent coding, refactor, and benchmark tasks in isolated Git worktrees with explicit responsibility and resource isolation.
---

# Parallel worktrees

Use this workflow only when the user has authorized parallel delegation. Parallelism is valuable when tasks have clear responsibilities and isolated execution resources; coordinated architectural refactors may intentionally overlap when that better matches the domain.

At the beginning of each run, the orchestrator performs the single baseline
check required by `AGENTS.md`: fetch `origin/main`, record its revision, and
inspect the latest `ci.yml` run when GitHub access is available. Compare that
run's `headSha`, `status`, and `conclusion` with the fetched revision; an older
successful run is not evidence that the current baseline passed. Share the
revision and CI state with every assignment. Do not ask each assignee to repeat
the run-level fetch or CI lookup; each assignee reports worktree identity and
confirms that its `HEAD` matches the supplied baseline. Inspect the run with
`gh run list --workflow ci.yml --branch main --limit 1 --json headSha,status,conclusion,url`.

## Before spawning

- Identify the immediate local task and keep it on the critical path.
- For a backlog or tech-debt run, select independent, still-unfinished items from
  `docs/backlog.md` and `docs/tech-debt.md`. Confirm the current code and tests
  still support each item before assigning it; planning text is not an accepted
  implementation contract by itself.
- Split independent work by responsibility, file ownership, and an explicit domain boundary. The refactoring and backlog/tech-debt policy is defined once in the repository root `AGENTS.md`; ensure every assignment follows that policy. For an explicitly coordinated architectural refactor, overlapping paths are allowed when they reflect the domain; name an integration owner, explain the overlap, and define how shared changes will be reconciled.
- Use the committed baseline established at the start of the run. If relevant uncommitted edits matter, create a clearly identified local baseline or explicit patch.
- Give every assignee the baseline revision, owned paths, expected result, and instruction not to revert unrelated work.
- Before spawning, give the user a short summary of each proposed worker's feature or outcome and primary evidence class, such as performance, correctness, reliability, or benchmark infrastructure. For performance-sensitive work, include a best-effort expectation of the likely direction and rough magnitude of change when possible, or explicitly say that no direct performance change is expected or that the magnitude is unclear. Label estimates as expectations rather than measured results; do not invent precision.
- Update an assignment when a newer `main` commit overlaps its paths or shared contracts, dependencies, generated output, or integration behavior; independent work may remain on the recorded baseline when it is cleanly mergeable.
- Treat implicit worktree allocation as a serialized critical section. Do not issue concurrent spawn or resume calls until each prior worker's worktree identity has been validated, unless all worktrees were explicitly provisioned beforehand.
- After each assignee is provisioned and before editing starts, verify `git worktree list --porcelain` and record a task-to-path-to-branch mapping. Keep the orchestrator worktree on the default branch; every task path must be distinct, outside the repository root, on its assigned branch, and at the recorded baseline. If any check fails, stop the assignee before editing, preserve any patch, and provision a replacement worktree; do not switch branches inside a shared or ambiguous worktree.

## Worktree and branch isolation

- Create one worktree outside the repository directory per task, with one branch per task.
- Keep the main worktree for integration and verification; do not assign edits there.
- Require each assignee's first instruction to request a pre-edit identity report containing `pwd`, `git rev-parse --show-toplevel`, `git branch --show-current`, and `git rev-parse HEAD`. Stop work if the reported top-level is the orchestrator repository, the branch is not assigned to that task, or the revision is not the supplied baseline.
- Before resuming or restarting an assignee, provision a newly validated dedicated worktree unless its previous worktree is known to be isolated and is explicitly revalidated before editing.
- Require assignees to inspect status and diff before committing and stage only their owned paths.
- Prefer one focused commit and one pull request per independently reviewable improvement.
- Do not rebase or update a pending branch solely because another disjoint pull request changed the base. Recheck the newest `origin/main` revision after merges and update the branch when the changed commits overlap or affect shared behavior; rerun relevant checks after any update.

## Test and benchmark isolation

Require the repository's executable isolation runner for supported workflows:

```text
just isolated
just isolated cluster-test
just isolated bench-cluster-smoke
just isolated bench-container-smoke
```

The runner creates a unique Cargo target directory, temporary-file directory,
benchmark artifact directory, and workflow-specific Docker resources. Use the
named workflows listed by `python3 scripts/isolated.py --help`. Do not require
manual port selection as a prerequisite for normal local verification.

For workflows not covered by the runner, require assignments to give each
concurrent process-level test or benchmark unique resources, including:

- allocate unique broker, HTTP, and peer ports rather than relying on fixed ports;
- use a unique data directory, temporary directory, benchmark output path, Docker project/network, container name, and volume name;
- use a unique `CARGO_TARGET_DIR` when concurrent builds would contend on artifacts or locks;
- bound CPU, memory, and worker counts explicitly; use separate CPU sets when comparing performance in parallel;
- ensure each workload cleans up child processes, containers, sockets, and temporary data on success and failure.

Shared Cargo registries are normally acceptable as caches, but shared target directories, generated benchmark files, and mutable broker data are not. Do not use a shared `benchmark-results/` path for concurrent writers.

Keep at most two assigned work items in flight whose acceptance or merge recommendation requires benchmark evidence. Count work whose performance claim or plausible material performance effect needs measurement; keep its slot occupied while required benchmark evidence or benchmark-related revisions remain outstanding. Do not count correctness or safety work when no material performance effect is expected and no performance claim is made. For that work, use the applicable focused tests and end-to-end verification instead. Fill other pool slots with independent work that does not need benchmark evidence when available. Change this cap only at the user's explicit direction.

Parallel runs are suitable for exploratory correctness checks and rough optimization feedback. Host CPU scheduling, disk bandwidth, page cache, and kernel socket resources are still shared, so do not treat concurrent results as authoritative latency or throughput comparisons unless CPU and storage resources are explicitly isolated. Schedule authoritative comparisons sequentially otherwise.

For any task that can affect throughput, latency, CPU, memory, batching,
I/O, or scheduling, follow [docs/benchmarking.md](../../../docs/benchmarking.md)
and determine whether the standard benchmark meaningfully covers the PR's
changes. If the change has no material performance effect expected and makes
no performance claim, use the applicable correctness, safety, and
end-to-end checks without requiring a benchmark. Otherwise, require the
assignee to run the canonical local benchmark before an improvement claim. If
the standard benchmark does not meaningfully cover the PR, evaluate whether a
focused targeted benchmark would be relevant and feasible with reasonable
effort and controlled resources; when it is, require it before recommending
that an optimization PR merge. Require authoritative comparisons to use
`just bench-pr-local` after committing and, if inconclusive,
`just bench-pr-local-until-stable` to retry complete comparisons. Treat a
one-pair command such as `just bench-pr-local-quick` as diagnostic only. Never
treat a hosted PR benchmark or concurrent task measurement as proof of a
performance change. Do not claim or merge an optimization from an inconclusive
authoritative result; investigate or rerun it under the same controlled
conditions rather than selecting a favorable sample. If no targeted benchmark
is feasible, require the handoff to record the concrete blocker and coverage
gap; do not recommend merging solely as an optimization until the changed path
has appropriate evidence.

## Assignment and handoff protocol

The orchestrator tells each assignee to read the repository root `AGENTS.md`,
this skill, and [WORKER.md](WORKER.md). The orchestrator does not rely on
automatic instruction loading. Nested delegation is disabled unless the
orchestrator explicitly authorizes it; any authorized nested task receives the
same worktree, ownership, and identity checks.

Each assignment states the goal, acceptance criteria, owned paths, supplied
baseline revision and CI state, task-to-worktree mapping, expected evidence
class, resource and isolation constraints, and coordination boundaries. The
orchestrator requires a pre-edit worktree identity report and stops work if it
does not match the mapping or supplied baseline. Assignments prohibit reverting
unrelated changes.

If an assignee proposes work across its assigned paths or another task's
ownership boundary, the orchestrator obtains the goal, rationale, affected
scope, expected effects and non-effects, evidence, risks, and recommendation.
Before authorizing an agreed expansion, update the assignment, ownership
mapping, and integration plan. Escalate uncertain boundary changes to the user.

`WORKER.md` is the single assignee-facing checklist for implementation,
testing and end-to-end assessment, evidence, planning and refactor assessment,
pull requests, and handoff. Keep shared engineering policy in `AGENTS.md` and
orchestration lifecycle policy in this skill; do not create conflicting copies.

The orchestrator collects expected effects and non-effects, evidence and
coverage gaps, recommendation, supplied baseline, and refactor/planning
assessment from every handoff. Preserve blocked or inconclusive results in the
final status, and do not turn an omitted planning update into an untracked
follow-up.

## Lifecycle and invocation modes

Interpret these concise requests as predefined coordination modes:

- “run parallel-worktrees with N subagents”: start a fixed pool of exactly N
  workers and do not start replacements. If fewer eligible tasks exist, start
  one worker per available task and report the shortage.
- “run parallel-worktrees with N subagents and rolling pool”: fill N initial
  worker slots (or all available eligible tasks when fewer exist), maintain at
  most N assigned workers, and replace a slot only after the orchestrator
  recommends that worker's PR for merge and the PR actually merges. Every
  replacement is another eligible item from the backlog or tech-debt records.
  Worker completion, PR creation, a worker's recommendation, or green checks do
  not trigger replacement.

For both modes, N is the requested initial and maximum worker count unless the
user gives a different concurrency limit. Record the count, mode, baseline,
task-to-worktree mapping, and stop condition. In rolling mode, a slot remains
unfilled while its worker's PR awaits checks or merge, or after the orchestrator
does not recommend merging that item.

After spawning, retain worker identifiers. Use grouped, non-busy-polling waits,
check open PRs periodically while workers or PRs are pending, and report each
completion with its branch and PR, result, evidence, gaps, and recommendation.
Report blocked or inconclusive work explicitly.

Assign each task to its owner for creation and maintenance of exactly one PR.
Review the diff against the recorded baseline, the assignee's evidence and
planning assessment, required checks, mergeability, and relevant repository
gates before making the final recommendation. If recommending merge, either
wait for required checks and merge when they pass, or enable auto-merge while
checks are pending when the repository supports it. Auto-merge does not bypass
required checks. In rolling mode, refresh the default-branch baseline and start
exactly one replacement only after the actual merge, never merely because
auto-merge was enabled, unless a stop condition has been reached.

If the orchestrator does not recommend merging a work item—including when it
recommends revise, rerun, defer, or records blocked/inconclusive evidence—leave
its PR and worktree in place, increment the distinct non-merge work-item count,
and immediately update the user with the PR, branch/worktree, evidence, gaps,
and recommendation so the user can direct follow-up. Do not replace that slot.

In rolling mode, continue until the user tells the orchestrator to stop starting
workers or five distinct work items have received a final non-merge
recommendation. Once either condition is reached, start no replacements. Let
already-started workers reach final status when practical and continue
processing their PRs, including merging PRs the orchestrator recommends when
their required checks pass.

If the user asks to call it a day after the remaining workers finish and requests
auto-merge, enable it for open PRs the orchestrator recommends merging. If the
repository does not support auto-merge, leave those PRs open, report the
limitation, and finish without starting replacements. Auto-merge never replaces
the orchestrator's review or merge recommendation.

## Integration

Review each branch independently before integration. Check the diff against the recorded baseline, rerun focused tests in a clean worktree, and run the repository verification path. Do not merge an optimization solely because a microbenchmark improved: preserve durability, ordering, timeout, ambiguous-outcome, bounded-resource, and recovery guarantees.

Merge independently reviewable pull requests in parallel once their required pull-request checks pass, or enable auto-merge after the orchestrator's recommendation while checks are pending. Coordinate or serialize changes that overlap in files, shared contracts, dependencies, generated output, or integration behavior; overlapping architectural refactors require integration review and must not be merged independently just because their pull requests are individually green. Never bypass required checks to compensate for a flaky test; diagnose whether the failure is in the implementation, test harness, environment, or resource isolation.

- If delegation dirties the orchestrator worktree or mixes task files, stop the affected assignees before changing branches. Preserve the mixed state in a recoverable stash or explicit patch, restore the orchestrator worktree to the default branch, and re-home only verified task files into dedicated worktrees. Never reset or discard the mixed state to repair allocation.

## Cleanup and handoff

Before cleanup, explicitly close the completed or cancelled assignee and any
authorized nested assignees. For a non-merge recommendation, retain the PR and
worktree for user-directed follow-up. Otherwise confirm that the assignees and
their owned processes and containers are gone, preserve committed work and
benchmark artifacts needed for review, and remove only clean temporary
worktrees. A clean Git status is not sufficient. Do not delete a worktree
containing uncommitted changes or one that still belongs to an active assignee;
do not terminate unrelated processes.

The final handoff should state which branches or pull requests were integrated, which remain open or blocked, the exact verification status, and any unresolved resource or benchmark reliability issue.
