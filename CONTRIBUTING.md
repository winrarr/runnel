# Contributing to Runnel

Runnel uses [Conventional Commits](https://www.conventionalcommits.org/) for
commits that are intended to become part of the project history. This keeps
release summaries and future changelog generation understandable without
requiring every change to be rediscovered manually.

Use this shape:

    <type>[optional scope][!]: short description

Examples:

    feat(protocol): support binary payloads
    fix(storage): reject a corrupt record checksum
    perf(core): reduce durable publish copying
    docs: explain clustered recovery
    refactor!: remove the provisional JSON protocol

Use a meaningful lowercase type such as `feat`, `fix`, `perf`, `docs`, `test`,
`refactor`, `build`, `ci`, `chore`, `security`, or `revert`. Mark a breaking
change with `!` and explain the migration in the body or a `BREAKING CHANGE:`
footer. Keep the subject specific and concise.

Pull-request titles follow the same format because Runnel uses squash merges;
the title becomes the release-facing commit subject. Existing history predates
this rule and is not rewritten.

The check is enforced by GitHub Actions for pull-request titles and new
commits pushed to `main`. Automated dependency updates use a Conventional
Commit prefix through Dependabot's commit-message configuration.

## Branches and pull requests

Create a non-`main` branch for each independently reviewable change and deliver
it through a separate pull request. Direct pushes to `main` and bypassing
repository rulesets or required checks are not allowed.

Pull-request branches do not need to be rebased solely because `main` advanced.
Before starting and before opening or updating a pull request, fetch `origin/main`
and check the latest default-branch CI run. Update the branch and rerun relevant
checks when changes overlap in owned paths, shared contracts, generated files,
dependencies, or integration behavior; otherwise a cleanly mergeable disjoint
branch may proceed from its recorded baseline.
