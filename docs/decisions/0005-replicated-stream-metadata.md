# ADR 0005: Represent stream metadata in replicated broker state

- Status: superseded by [ADR 0006](0006-separate-metadata-and-data-groups.md)
- Date: 2026-08-19

## Decision

The current clustered slice will represent each stream with durable metadata containing a stable stream identity, a stable data-group identity, and a lifecycle state. The metadata and stream records are applied through the same static Raft group for now.

Stream creation remains idempotent. Publishing may create a stream on first use, matching the local engine's existing behavior; both paths produce the same deterministic identities. The current slice serves only the `Active` lifecycle, while the state model reserves lifecycle transitions for future reconciled creation, movement, and deletion.

State-machine and snapshot formats are versioned. The broker accepts the previous stream representation during recovery and writes the new metadata-aware representation thereafter.

## Rationale

Stable metadata is the smallest change that makes stream identity independent from a local file or process while preserving the current one-group implementation. It gives future placement, group management, and lifecycle work a durable domain model without prematurely implementing a separate metadata group or cross-group transaction.

## Consequences

- Stream names remain the public address; internal identities are not exposed through the client protocol.
- The current implementation does not yet provide a separate metadata group, multi-group creation protocol, or partial lifecycle reconciliation.
- Deterministic identities are sufficient while streams cannot be renamed; a future rename or deletion design must define identity reuse and history explicitly.
- Format migration and snapshot compatibility are now part of the clustered storage contract.

## References

- [Clustered metadata backlog](../backlog.md)
- [Multi-Raft implementation plan](../design/multi-raft-implementation-plan.md)
