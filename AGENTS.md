# Runnel project guide

Runnel is a Rust message broker intended to offer durable streams, low operational overhead, and a credible path from a single node to a distributed system. The current repository contains intentionally small single-node and early static-cluster vertical slices, not the complete product described in the product brief.

## Repository map

- crates/runnel-protocol: provisional line-delimited JSON request and response types. This is the boundary for future language clients.
- crates/runnel-engine: topology-free broker engine contract and shared messaging outcomes.
- crates/runnel-core: local broker engine, append-only durable stream log, consumer checkpoints, acknowledgements, and recovery.
- crates/runnel-raft: OpenRaft adapter with durable local storage, framed TCP peer transport, and the early static-cluster backend.
- crates/runnel-server: runnel broker process, TCP protocol server, health endpoints, and Prometheus-compatible metrics.
- crates/runnel-cli: runnelctl, a small development client for the current protocol.
- crates/runnel-core/benches: Criterion benchmarks for durable publish and publish/poll/ack paths.
- crates/runnel-server/tests: network-level protocol and restart tests.
- scripts/benchmarks/run.py: resource-limited container benchmark runner with machine-readable results.
- scripts/benchmarks/compare.py: first-pass native-tool comparison runner for Runnel, Kafka, Redpanda, and JetStream.
- scripts/benchmarks/README.md: benchmark scope, semantics, and comparison guidance.
- docs/architecture.md: current data flow and boundaries.
- docs/design/: active architecture explorations, alternatives, and proposed implementation plans that are not accepted decisions.
- docs/decisions/: consequential decisions that should not be rediscovered from code.
- docs/backlog.md: explicitly intended product outcomes that are not implemented yet.
- docs/tech-debt.md: known implementation shortcuts, their impact, and retirement conditions.
- docs/testing.md: canonical local, interactive, integration, and benchmark workflows.
- deploy/kubernetes/: illustrative single-node and three-node StatefulSet deployments.
- justfile: canonical Linux development command interface.
- scripts/smoke.sh: repeatable broker/CLI/restart smoke test.
- scripts/verify.sh: compatibility wrapper around just verify.

## Sources of truth and boundaries

Rust code and tests define current behavior. The wire protocol is intentionally provisional until compatibility policy is decided. Do not expose storage paths, offsets, or physical layout as public concepts beyond what the current protocol already requires.

The semantic engine contract is owned by runnel-engine. The local durable log format is owned by runnel-core. Changes to either require focused tests and a decision record when they affect compatibility, crash behavior, or future engine boundaries. Keep local file I/O and consumer-state persistence inside runnel-core; keep transport concerns in runnel-server and client ergonomics in runnel-cli. Consensus-specific code must stay behind a distributed-engine adapter and must not become part of the public protocol model.

The current implementation serializes broker operations behind one in-process lock. Treat that as a vertical-slice limitation, not as a target performance architecture. Benchmark before replacing it, and preserve the public model while changing internals.

## Engineering rules

- Preserve at-least-once behavior: an acknowledgement advances durable consumer state only after the state update succeeds.
- Do not claim stronger durability or ordering guarantees than the implementation and tests establish.
- Make ambiguous outcomes explicit in protocol responses rather than silently retrying operations.
- Keep stream and consumer names validated before they become filesystem paths.
- Prefer small domain types and explicit state transitions over transport-specific logic in the core.
- Add a focused crash/recovery test before changing persistence, acknowledgement, or redelivery behavior.
- Treat benchmarks as part of performance work; include the durability mode and workload in every result.
- Treat anything that could improve throughput or latency as worth considering. Evaluate allocation, copying, lock scope, batching, I/O, scheduling, transport, and encoding effects when making changes, while preserving correctness, bounded resource use, and predictable tail latency. Benchmark material assumptions instead of optimizing on intuition alone.
- Keep network behavior covered by tests that start the real server process.
- Keep the minimum supported Rust version checked separately from the pinned development toolchain.

## Canonical commands

Linux development uses `just` as the canonical command runner. Install it once with:

    cargo install --locked just

Run these from the repository root:

- just verify runs formatting, Clippy, all-target tests, documentation tests, ShellCheck, and a workspace build.
- just ci runs verification, the real broker smoke test, the Docker build, and the container benchmark smoke check.
- just run starts a local broker with data in ./data.
- just smoke starts a real broker and uses runnelctl to exercise publish, consume, acknowledgement, restart recovery, readiness, and metrics with temporary state.
- just cluster-test starts three real broker processes, exercises quorum replication, follower restart, leader failure, and recovery through the public protocol.
- just bench runs the Criterion performance benchmarks.
- just bench-container builds and benchmarks the broker image with explicit CPU and memory limits.
- just bench-container-smoke exercises the container benchmark path with a small workload for CI.
- just bench-compare builds Runnel and runs the documented first-pass comparison against Kafka, Redpanda, and JetStream.
- just audit runs cargo-audit when it is installed.
- ./scripts/verify.sh is a thin compatibility wrapper for just verify.

Do not add a second task runner. Keep README commands and CI wired to just recipes. If the command graph changes, update AGENTS.md, README.md, justfile, and .github/workflows/ together.

## Verification and automation

The required CI path is .github/workflows/ci.yml. It runs the pinned toolchain checks, the minimum supported Rust check, the real network integration test, and the container smoke build. .github/workflows/security.yml audits the dependency lockfile on pull requests and weekly. Dependabot keeps Cargo and GitHub Actions dependencies visible for review.

## Knowledge routing

Put implementation behavior in code and tests, current boundaries in docs/architecture.md, unsettled alternatives in docs/design/, durable accepted rationale in a dated decision record, external or user-mandated guardrails in docs/constraints.md, intended unfinished outcomes in docs/backlog.md, known implementation shortcuts in docs/tech-debt.md, verification workflows in docs/testing.md, and operational deployment guidance beside its deployment artifact. Put workflow changes in justfile and CI changes in .github/workflows. Remove stale guidance instead of appending exceptions.
