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
- Check `git status` and inspect the current code, tests, decisions, and nearby
  design or planning records. Treat backlog and tech-debt text as guidance to
  validate, not as accepted APIs or behavior.
- Keep edits inside the assigned worktree. Include same-change planning-record
  updates required by `AGENTS.md`. Do not discard, rewrite, or stage unrelated
  work. If a better solution crosses your assigned scope, another worker's
  ownership, or a shared boundary, send the coordinator a proposal with its
  goal, rationale, affected scope, expected effects and non-effects, evidence,
  risks, and recommendation. Do not silently shrink the design or edit across
  the boundary. If you and the coordinator agree the change is best, update
  task ownership and the integration plan, then proceed. If either of you is
  unsure, the coordinator escalates the scope decision to the user before the
  boundary changes.
- Identify the primary evidence class, applicable gate in
  [`docs/testing.md`](../../../docs/testing.md), relevant acceptance criteria,
  and verification commands before implementation.

## Implementation and verification

- Inspect the touched code and its immediate surroundings. Aim for the clean,
  maintainable, performant design that best advances the assignment; do not
  keep a needed change artificially small or preserve old behavior solely to
  avoid churn. Backward compatibility is not a Runnel requirement; make
  deliberate breaking changes when they improve the intended design and
  remove obsolete compatibility paths. When the better design needs a
  cohesive refactor across owned areas or shared boundaries, use the proposal
  and agreement process above rather than shrinking the solution. Assess
  material risks with proportionate evidence, and follow the refactor and
  planning-record policy in `AGENTS.md`, including its no-update rationale.
- For non-trivial changes to semantics, storage, replication, ordering,
  recovery, or operational safety, compare relevant reference designs and
  primary research before implementation, following `AGENTS.md`.
- Keep accepted decisions and current architecture documentation aligned when
  the change alters them; update the affected ADR or architecture document as
  appropriate rather than changing records that do not describe the new state.
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
- If a check or PR workflow fails, inspect its logs and assess whether the
  cause is the change, a test or workflow defect, or the environment. Fix
  relevant issues and rerun the affected checks; do not rerun blindly.
  Distinguish a confirmed fix from a transient failure, inconclusive run, or
  unresolved blocker, and report the commands and results.
- Use `just isolated <workflow>` for supported process-heavy tests or
  benchmarks run concurrently. Give other concurrent workflows unique
  processes, ports, data, output paths, and build targets as required by
  `SKILL.md`.
- For performance-sensitive work, follow
  [`docs/benchmarking.md`](../../../docs/benchmarking.md) and the benchmark
  rules in `SKILL.md`. Record the exact revision, workload, resources,
  isolation, repetitions, commands, and artifacts. Do not claim an improvement
  from an inconclusive or uncontrolled comparison. For a correctness or safety
  improvement that you believe has no material performance effect, tell the
  coordinator why and propose the focused and end-to-end tests that cover it;
  the coordinator decides whether benchmark evidence is unnecessary. If the
  coordinator requires a benchmark, assess the result against the intended
  effect and coordinate its timing so no other tests, benchmarks, or
  resource-heavy workloads run on the host during an authoritative comparison.

## Pull request and handoff

- Before committing, inspect the complete diff, run `git diff --check`, and
  stage only files in the agreed scope, including required planning-record
  updates. Use a Conventional Commit for both the commit and PR title.
- Push the branch and open exactly one pull request for the assignment. Keep
  it as a draft while implementation or verification is in progress. Monitor
  its workflows, assess and address failures, and update the PR title and
  description to reflect the current work and evidence as they change; the PR
  description does not need a revision history. Mark it ready and hand it to
  the coordinator only when you consider the assignment complete and the PR
  ready for independent review. If a blocker prevents readiness, keep the PR
  in draft and send the coordinator a progress update with the evidence and
  blocker. Do not merge the PR or enable auto-merge; the coordinator owns
  review and integration.
- Include a concise handoff in the PR description with:
  - goal, changed files, expected effects, and non-effects;
  - primary evidence class and any secondary evidence tags;
  - supplied baseline revision and whether the branch was refreshed;
  - commands and results, focused and end-to-end coverage assessment, and any
    test or benchmark artifacts;
  - correctness, failure, and recovery considerations; evidence gaps and
    unresolved risks;
  - refactor and backlog/tech-debt assessment, including any updates or an
    explicit no-update rationale;
  - a recommendation to merge, revise, rerun, or defer, with reasons.
- At handoff, tell the coordinator the PR URL, branch and worktree, check
  status, final head revision, and recommendation. Verify required PR checks,
  including the relevant end-to-end job, against that exact head. The
  coordinator is the reviewer and will send any requested revisions directly;
  do not wait for review comments. Keep the worktree and branch available
  until the coordinator closes the work, and do not stop authorized nested
  workers before then.
