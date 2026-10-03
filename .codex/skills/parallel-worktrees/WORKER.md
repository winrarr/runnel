# Worker assignment and pull request handoff

This checklist covers delegated tasks. Follow the repository root `AGENTS.md` and [`SKILL.md`](SKILL.md), which own shared engineering and parallel-worktree policy. Confirm the assignment's owner, scope, baseline, and acceptance criteria. Ask only when a missing detail materially changes the work; otherwise state a safe assumption and proceed.

## Before editing

- Read `AGENTS.md`, [`SKILL.md`](SKILL.md), and this checklist.
- Verify `pwd`, `git rev-parse --show-toplevel`, `git branch --show-current`, and `git rev-parse HEAD` match the assignment. Stop if they do not.
- Check `git status`; inspect the implementation, tests, decisions, and nearby planning records. Treat backlog and tech-debt items as guidance to validate, and preserve unrelated work.
- Keep the supplied baseline; follow `AGENTS.md` before updating the branch.
- Choose the primary evidence class, applicable gate in [`docs/testing.md`](../../../docs/testing.md), and verification approach for the acceptance criteria.
- Keep edits within the assignment. For work that crosses an ownership or shared boundary, propose its goal, rationale, scope, expected effects and non-effects, evidence, risks, and recommendation to the coordinator. Proceed only after agreement and an updated ownership and integration plan; ask the user if the scope remains unclear.

## Implementation and verification

- Follow `AGENTS.md` for design, refactoring, research, decisions, and planning records. Follow `docs/testing.md` and `docs/benchmarking.md` for evidence.
- Assess end-to-end coverage against the acceptance criteria and run the applicable gate. Add coverage when in scope or report the concrete gap. For documentation-only work, explain why runtime tests do not apply and run applicable document checks.
- Use `just isolated <workflow>` for supported concurrent process tests and benchmarks; isolate resources for other concurrent workflows as required by [`SKILL.md`](SKILL.md).
- For performance work, report the revision, workload, resources, isolation, repetitions, commands, artifacts, and evidence limits. Do not claim improvement from inconclusive or uncontrolled results.
- When a check fails, inspect its logs, diagnose the cause, fix relevant issues, and rerun affected checks. Report transient failures and unresolved blockers.

## Pull request and handoff

- Before committing, inspect the complete diff, run `git diff --check`, and stage only agreed paths, including required planning-record updates. Use Conventional Commits for the commit and PR title.
- If the assignment produces a change, push the branch and open one draft PR. Keep it in draft while work or checks remain; monitor and resolve relevant failures and keep its title and description current. Mark it ready only when the assignment is complete, independently reviewable, and required checks pass on its exact final head. If a blocker remains, keep it in draft and report the evidence and blocker. If no change is warranted, report the evidence and disposition to the coordinator without opening an empty PR. Do not merge or enable auto-merge; the coordinator owns integration.
- Include the handoff required by `AGENTS.md` in the PR description: goal and files, effects and non-effects, evidence class and any secondary tags, commands and results, relevant end-to-end coverage, gaps and risks, refactor and planning-record assessment, and recommendation. State the supplied baseline and any justified branch update with its reason and scope.
- Tell the coordinator the PR URL, branch, worktree, final revision, exact-head check status (including the relevant end-to-end job), evidence gaps, and recommendation. Keep the branch and worktree available until the coordinator closes the task, including any explicitly authorized nested work.
