# ADR 0003: Use a CLI-driven process smoke test for local verification

- Status: accepted
- Date: 2026-08-19

## Decision

`just smoke`, implemented by `scripts/smoke.sh`, is the canonical local end-to-end verification path. It starts the real broker process, drives it through `runnelctl`, exercises durable publish, consume, acknowledgement, restart recovery, readiness, and metrics, and cleans up temporary state. CI runs this same recipe.

## Rationale

The important first behavior crosses process, TCP, protocol, filesystem, and shutdown boundaries. An in-process test would not prove that an agent or developer can start the broker and use the client surface. A single deterministic command also gives local development and CI the same acceptance path.

## Consequences

- Changes to startup, storage, delivery, protocol, or shutdown should keep `just smoke` passing.
- The smoke test intentionally uses the development CLI and is not a compatibility promise for the eventual client ecosystem.
- The supported Linux development environment needs the tools used by the script, including Rust, just, curl, Python 3, ripgrep, and ShellCheck for verification.
- More extensive failure and workload scenarios belong in focused integration tests and benchmarks rather than making the smoke test indefinitely large.
