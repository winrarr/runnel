# ADR 0002: Use just for Linux development commands

- Status: accepted
- Date: 2026-08-19

## Decision

Use justfile recipes as the canonical local development command interface. Keep Cargo as the build and dependency authority, and keep scripts only for workflows that need more shell control.

## Rationale

Runnel supports Linux development only, and the repository needs a readable command layer for verification, smoke tests, benchmarks, local operation, audits, and container work. just is purpose-built for command running, is already available in the supported environment, and keeps the command graph explicit without introducing another build language.

Make is not needed for target selection or incremental builds because Cargo owns those concerns. Task would add another external tool without a concrete benefit for this Linux-only Rust repository. Adding both a task runner and parallel shell recipes would create competing sources of truth.

## Consequences

- Contributors need just installed; the README documents the installation command.
- CI installs the same runner before invoking the canonical recipes.
- scripts/verify.sh remains only as a thin compatibility wrapper and must not grow an independent check list.
- Workflow changes belong in justfile; CI and documentation should invoke those recipes rather than duplicate their commands.

