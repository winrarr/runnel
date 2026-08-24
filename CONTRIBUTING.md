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

Run the local check with:

    just check-commits
    just check-commits 'origin/main..HEAD'

The check is also enforced for pull-request titles and new commits pushed to
`main`. Automated dependency updates should use a Conventional Commit prefix
through Dependabot's commit-message configuration.
