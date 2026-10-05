---
name: parallel-worktrees
description: Coordinate authorized fixed or rolling pools of independent coding, refactor, and benchmark tasks in isolated Git worktrees.
---

# Parallel worktrees

Use this workflow only when the user authorizes parallel delegation. Read the
repository root `AGENTS.md` and follow its shared engineering, evidence, and
pull-request policies.

## Orchestration

At the start of a run, the orchestrator performs the single baseline check
required by `AGENTS.md`: fetch `origin/main`, record its revision, and inspect
the latest `ci.yml` run on `main` when GitHub access is available. Record its
`headSha`, `status`, and `conclusion`, noting that a default-branch run may lag
the current revision and is periodic health evidence. Passing required checks
on the exact pull-request head are the merge gate; do not wait for a new
default-branch run. Share the revision and CI state with workers.
Workers confirm their worktree `HEAD` matches the supplied revision; they do
not repeat the run-level fetch or CI check. Inspect the run with:

```text
gh run list --workflow ci.yml --branch main --limit 1 --json headSha,status,conclusion,url
```

Do not repeat the baseline CI lookup during the run. Required checks must pass on each PR's exact final head before merge. Keep task branches at their supplied baseline as `main` advances; follow `AGENTS.md` for branch-update exceptions. Merge ready PRs against the current base after review and exact-head checks pass; report other repository blockers instead of syncing automatically.

Use the task request, dependencies, risk, and learning value to select work
from `docs/backlog.md` and `docs/tech-debt.md`. Confirm an item still applies
to the current code. Split work only where outcomes can be independently
owned, verified, and reviewed; carry forward the source item's goal,
acceptance criteria, and dependencies. Assign the work and let workers choose
how to deliver and verify it within `AGENTS.md` and the applicable testing
and benchmarking policies. Do not delegate onward unless explicitly
authorized.
Base assignments on the recorded commit. If relevant uncommitted changes are
needed, make them an explicit patch or establish and share a clear local
baseline.

Workers update backlog or tech-debt records in the same PR when their outcome
changes them. The orchestrator checks that merged outcomes and tracker status
remain aligned. Keep the active task on the critical path, and give each worker
the goal, relevant source item, owned paths, supplied baseline, and coordination
boundaries. Do not assign edits in the orchestrator's repository worktree.
Resolve proposed work across ownership or shared boundaries under `AGENTS.md`,
and update the ownership map and integration plan before expanding a task.
Ensure workers read the repository `AGENTS.md`, this skill, and `WORKER.md`;
do not rely on automatic instruction loading.

## Worktree and resource isolation

Provision one dedicated branch and worktree per task, outside the repository
directory. Before editing, require each worker to report `pwd`,
`git rev-parse --show-toplevel`, `git branch --show-current`, and
`git rev-parse HEAD`. Validate that the path and branch match the assignment
and that `HEAD` matches the supplied baseline. Record the task-to-path-to-branch
mapping; do not start another implicit worktree allocation until the prior one
is validated. Revalidate an existing worktree before resuming a worker.

Use `just isolated <workflow>` for supported concurrent process tests and
benchmarks. Give other concurrent work unique ports, data and output paths,
containers, and build targets where needed. Do not share mutable broker data,
benchmark output paths, or Cargo target directories. Limit concurrent tasks
requiring authoritative host benchmarks to two, and run those benchmarks with
the host otherwise idle. Treat measurements made during contention as
exploratory. Follow `docs/benchmarking.md` for authoritative comparison and
reporting requirements.

If worktree allocation is wrong or task changes become mixed, stop affected
workers and preserve the changes before re-homing verified work. Never reset
or discard mixed work to repair allocation. Clean up only inactive workers and
confirm their processes and containers have stopped before removing clean
temporary worktrees. Retain uncommitted work and work needed for follow-up;
never terminate unrelated processes.

## Fixed and rolling pools

- “Run parallel-worktrees with N subagents” starts up to N workers and does
  not replace them. If fewer eligible tasks exist, use the available tasks
  and report the shortage.
- “Run parallel-worktrees with N subagents and rolling pool” keeps at most N
  tasks assigned, filling up to N initial slots with eligible work, and starts
  one replacement only after a worker's PR is reviewed, recommended for merge,
  and merged. A worker finishing, opening a PR, or getting green checks does
  not by itself free the slot. Keep the rolling-pool run active until the user
  explicitly asks to stop it. After each reviewed-and-merged PR, select the
  next eligible backlog or tech-debt task and fill the available slot. If no
  task can responsibly be identified, report why the pool is empty and leave
  the run open and idle; resume selection when eligible work becomes
  available. An empty eligible queue is not completion.

Record the requested mode and concurrency. In rolling mode, leave a slot
unfilled while a PR awaits review or merge, or when its outcome is not
recommended for merge. If the user asks to stop replacements, start none;
continue resolving already-started work when practical, then leave the run open
and idle until the user explicitly stops it. A non-merge recommendation leaves
its slot open and is not a run-completion condition, regardless of how many
such recommendations have occurred. Preserve open PRs and worktrees for
user-directed follow-up.
For each non-merge recommendation, promptly report the PR, branch/worktree,
evidence gaps, and recommendation so the user can direct follow-up; leave the
slot open.
If the user asks to call it a day after remaining workers finish and requests
auto-merge, enable it only for PRs recommended for merge; if unsupported, leave
them open and report that limitation.

## Review and merge

Workers own implementation, their evidence approach, testing, and PR readiness.
They hand off only when the PR is ready for review and all required checks pass
on its exact final head. The orchestrator reviews the diff, acceptance outcome,
evidence and stated gaps, tracker updates, mergeability, and applicable
repository gates. Request revisions when a required check or gate fails, or
when the outcome or claims lack support. Do not repeat green checks by default.

The orchestrator owns the merge decision. Merge a PR when review is complete and exact-head required checks pass. Auto-merge may be enabled after recommending merge when supported; it must not bypass checks. Coordinate genuinely coupled changes before handoff, follow `AGENTS.md` for branch updates, and report repository blockers instead of syncing automatically.

For a rolling pool, give each replacement the commit and passing PR checks from
the preceding merge as its baseline; do not run a separate post-merge CI check.
Existing tasks keep their supplied baseline, with branch updates governed by
`AGENTS.md`.

Use [`WORKER.md`](WORKER.md) as the worker checklist. Keep shared engineering
policy in `AGENTS.md` and orchestration policy here. The final status reports
merged and open PRs, evidence and coverage gaps, recommendations, verification
state, tracker disposition, and unresolved resource or benchmark concerns.
