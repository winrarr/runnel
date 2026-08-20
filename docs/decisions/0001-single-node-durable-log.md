# ADR 0001: Start with a single-node durable log

- Status: accepted
- Date: 2026-08-19

## Decision

The initial implementation uses one self-contained broker process, an append-only file per stream, and one durable committed offset per consumer.

## Rationale

This is the smallest design that can exercise the required first recovery path: start the broker, publish durably, consume, acknowledge, stop, restart, and observe the correct checkpoint behavior. It keeps storage and delivery invariants visible while leaving the transport and domain boundaries open for future segmentation and clustering.

## Consequences

- The current implementation is not a high-throughput production architecture and must not be presented as one.
- A process-wide lock limits concurrency until measured work justifies finer-grained ownership.
- The log format and state files need explicit compatibility/versioning before long-lived upgrades are supported.
- Consumer groups and distributed ownership are future work; the public consumer intent should remain independent of local file layout.

