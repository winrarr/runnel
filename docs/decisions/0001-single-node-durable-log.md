# ADR 0001: Start with a single-node durable log

- Status: accepted
- Date: 2026-08-19

## Decision

The initial implementation uses one self-contained broker process, an append-only file per stream, and one durable committed offset per consumer.

## Rationale

This is the smallest design that can exercise the required first recovery path: start the broker, publish durably, consume, acknowledge, stop, restart, and observe the correct checkpoint behavior. It keeps storage and delivery invariants visible while leaving the transport and domain boundaries open for future segmentation and clustering.

## Consequences

- The current implementation is not a high-throughput production architecture and must not be presented as one.
- The initial process-wide lock has been replaced by per-stream ownership and bounded blocking-storage execution; the single-node durability boundary remains local.
- The log format and state files need explicit compatibility/versioning before long-lived upgrades are supported.
- Shared consumers and early clustered ownership now have their own decisions in ADRs 0013 and 0015. Public consumer intent remains independent of local file layout.
