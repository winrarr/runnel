# ADR 0011: Do not publish a formal Rust MSRV

- Status: accepted
- Date: 2026-08-20

## Context

Runnel is currently a broker application distributed as a binary or container. The repository does not publish general-purpose Rust libraries for downstream applications, and the project has one primary developer. Maintaining a separate compiler compatibility floor adds dependency and CI constraints without protecting a current product contract.

ADR 0004 previously declared Rust 1.88 as the MSRV to support its selected OpenRaft dependency graph. That policy is no longer aligned with the product's distribution model.

## Decision

Remove the workspace `rust-version` declarations and the dedicated MSRV CI check. Pin Rust 1.97.1 for local development, CI, and container builds so the supported build environment remains explicit and reproducible.

Runnel makes no formal promise that older Rust toolchains can compile the source. Revisit the policy before publishing reusable crates or accepting downstream source-build compatibility as a product requirement.

## Consequences

- Dependency and language choices are not constrained by an artificially maintained compiler floor.
- The pinned toolchain remains consistent across local development, CI, and Docker builds.
- Source builds with older compilers are unsupported unless they happen to work.
- Runtime compatibility remains defined by the published binary and container targets, not by the Rust compiler version used to build them.
