# Worker assignment and pull request handoff

Use this guide for every task delegated through the parallel-worktrees skill.
It supplements the repository root `AGENTS.md` and `SKILL.md`; it does not
override either one. Confirm the assignment's owner, scope, baseline, and
acceptance criteria. Ask the coordinator only when a missing detail materially
changes the work; otherwise state a safe assumption and proceed.

## Before editing

- Read the repository root [`AGENTS.md`](../../../AGENTS.md),
  [`SKILL.md`](SKILL.md), and this guide.
- Confirm the assigned worktree identity and report `pwd`,
  `git rev-parse --show-toplevel`, `git branch --show-current`, and
  `git rev-parse HEAD`. Stop if the path, branch, or revision differs from the
  assignment.
- Run `git fetch origin main`, record `git rev-parse origin/main`, and compare
  the latest `ci.yml` run's `headSha`, `status`, and `conclusion` with that
  revision when GitHub access is available. Treat the matching revision as the
  baseline. Report if and why the assigned branch needs refreshing.
- Check `git status` and inspect the current code, tests, decisions, and nearby
  design or planning records. Treat backlog and tech-debt text as guidance to
  validate, not as accepted APIs or behavior.
- Keep edits inside the assigned worktree and scope. Do not discard, rewrite,
  or stage unrelated work. Coordinate before changing shared contracts,
  dependencies, generated files, or integration behavior.
- Identify the primary evidence class, applicable gate in
  [`docs/testing.md`](../../../docs/testing.md), relevant acceptance criteria,
  and verification commands before implementation.

## Implementation and verification

- Inspect the touched code and its immediate surroundings. Make safe,
  appropriately scoped refactors; record broader concrete debt and update
  backlog or tech-debt outcomes as required by `AGENTS.md`. State why no update
  is warranted when inspection finds no concrete follow-up.
- For non-trivial changes to semantics, storage, replication, ordering,
  recovery, or operational safety, compare relevant reference designs and
  primary research before implementation, following `AGENTS.md`.
- Use focused tests for changed behavior and the canonical `just` commands.
  Add crash/recovery coverage before changing persistence, acknowledgement, or
  redelivery behavior. Keep network behavior covered by tests that start the
  real server process.
- Assess end-to-end coverage explicitly. Identify which process, network,
  restart, cluster, or deployment journey exercises the changed acceptance
  criteria, and check that the test asserts the behavior at issue. Run the
  end-to-end command required by the applicable testing gate when the change
  affects that path. Do not present unit or engine-contract tests as
  end-to-end coverage. If no existing end-to-end test covers the behavior, add
  one when in scope or report the concrete gap and why it remains.
- For documentation-only work, state why runtime tests do not apply and run
  applicable document checks, such as `git diff --check`.
- If a check fails, inspect its output and diagnose the cause before rerunning.
  Distinguish a confirmed fix from a transient failure, inconclusive run, or
  unresolved failure; report the commands and each relevant result.
- Use `just isolated <workflow>` for supported process-heavy tests or
  benchmarks run concurrently. Give other concurrent workflows unique
  processes, ports, data, output paths, and build targets as required by
  `SKILL.md`.
- For performance-sensitive work, follow
  [`docs/benchmarking.md`](../../../docs/benchmarking.md) and the benchmark
  rules in `SKILL.md`. Record the exact revision, workload, resources,
  isolation, repetitions, commands, and artifacts. Do not claim an improvement
  from an inconclusive or uncontrolled comparison.

## Pull request and handoff

- Before committing, inspect the complete diff, run `git diff --check`, and
  stage only assigned files. Use a Conventional Commit on the assigned branch.
- Push the branch and open exactly one pull request for the assignment. Use a
  draft PR for incomplete or blocked work. Do not merge the PR or enable
  auto-merge; the coordinator owns review and integration.
- Include a concise handoff in the PR description with:
  - goal, changed files, expected effects, and non-effects;
  - primary evidence class and any secondary evidence tags;
  - baseline revision, matching baseline CI status, and whether the branch was
    refreshed;
  - commands and results, focused and end-to-end coverage assessment, and any
    test or benchmark artifacts;
  - correctness, failure, and recovery considerations; evidence gaps and
    unresolved risks;
  - refactor and backlog/tech-debt assessment, including any updates or an
    explicit no-update rationale;
  - a recommendation to merge, revise, rerun, or defer, with reasons.
- Tell the coordinator the PR URL, branch and worktree, check status, and final
  head revision and recommendation. Verify required PR checks, including the
  relevant end-to-end job, against that exact head; report pending checks as
  pending and inspect failure logs before recommending a rerun. Report blocked
  or inconclusive work as such. Keep the worktree and branch available for
  review and requested revisions; do not remove them or stop nested workers
  until the coordinator closes the work.
