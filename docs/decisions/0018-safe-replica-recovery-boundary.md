# ADR 0018: Keep permissive empty-replica recovery test-only

- Status: accepted
- Date: 2026-08-24

## Decision

The default Runnel broker build does not enable OpenRaft's `loosen-follower-log-revert` feature and does not claim that an operator may erase a replica's local state and restart it with the same voter identity.

The snapshot-based empty-replica scenario remains an explicit test-only configuration. Its integration test enables the OpenRaft feature deliberately, verifies that broker child-process exits fail the test, and records the scenario as experimental recovery evidence rather than a production replacement guarantee.

Normal supported behavior remains process restart with preserved durable state. Safe replacement of a replica whose state is missing or inconsistent requires a future rejoin/recovery lifecycle with explicit identity, progress, serving, fencing, and membership semantics.

## Rationale

OpenRaft documents follower log rollback as an unexpected or special-case condition and warns that erasing a node can lead to panic or data loss in later elections. Kafka and Redpanda treat replica liveness, committed visibility, recovery, and reconfiguration as explicit parts of the replicated-log design. Enabling permissive rollback in the production binary would hide a recovery boundary that has not yet been designed or verified.

The test-only feature keeps a valuable snapshot-transfer experiment while making its risk visible. The process-health assertion is required because a passing client assertion is not evidence that all clustered nodes remained alive.

## Consequences

- The existing empty-replica snapshot test is not a production compatibility or availability promise.
- Future replacement work must preserve acknowledged data and consumer state under a documented quorum and membership model.
- A later decision may replace the test-only path with learner-based recovery, controlled reconfiguration, or another design after benchmarking and failure testing.
- ADR 0007 remains valid for snapshot format and transfer mechanics, but its empty-node recovery implication is scoped by this decision.

## References

- [Raft follower recovery and replacement research](../research/raft-recovery-and-replacement.md)
- [ADR 0007: Snapshot-based replica recovery](0007-snapshot-based-replica-recovery.md)
- [ADR 0004: Use Multi-Raft as the first distributed engine](0004-multi-raft-first-distributed-engine.md)
