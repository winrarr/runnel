# Raft follower recovery and replacement

- Status: exploratory
- Last reviewed: 2026-08-24
- Scope: the early static Multi-Raft backend, process crashes, restart, and replacement of a node whose local state is missing or inconsistent.

## Why this note exists

The clustered integration harness previously treated a `Child` handle as proof that a broker was still running. Persistent test logs exposed an OpenRaft panic during an otherwise passing local recovery run: `follower log reversion is not allowed`. The harness was therefore capable of reporting success after a broker process had exited.

This is both a verification defect and an architectural warning. The immediate harness correction is independent of the broker's recovery design. The recovery design must not be changed merely to make the test green.

## Evidence

OpenRaft documents follower log reversion as a normally unexpected condition. Its `loosen-follower-log-revert` feature permits rollback for testing or special scenarios, but is disabled by default. Its FAQ specifically warns that erasing one node's data and waiting for a leader to replicate it again can panic the leader and can create data-loss scenarios after a subsequent leader failure. OpenRaft also describes monotonic log pointers and the need to persist committed progress and recover snapshots without moving applied state backward.

Kafka's replication design treats each partition as a leader with followers, exposes only committed messages, tracks replica liveness through the in-sync replica set, and permits crashed brokers to recover without requiring all data to remain intact. Its recovery documentation validates and truncates an incomplete or corrupt tail before serving the log.

Redpanda uses a Raft group for each topic partition, requires a majority of replicas to persist an acknowledged record, and treats recovery and reconfiguration as explicit cluster operations. Its documentation and engineering discussion call out ordered replication paths, learner recovery, bounded recovery work, and avoiding concurrent replication paths that can reorder requests and cause truncation or redelivery.

These systems do not make “start an empty process with the old voter identity” the ordinary replacement contract. They separate normal restart with durable local state from controlled recovery or reconfiguration of a missing replica.

## Implications for Runnel

- An acknowledged record must remain protected by quorum and leader-election invariants even when a follower is unavailable or has lost local state.
- An empty or inconsistent replica must not silently become a normal voter merely because it has the same configured node ID.
- Snapshot transfer is a useful recovery mechanism, but snapshot installation needs an explicit replica incarnation, serving boundary, and promotion/rejoin policy.
- The default broker build must not enable OpenRaft's permissive follower-log rollback feature as a substitute for a safe replacement lifecycle.
- Integration tests must fail immediately when a broker process exits, and failure artifacts must preserve the broker logs long enough to distinguish a harness failure, storage failure, transport failure, and consensus invariant violation.

## Candidate directions to evaluate

1. Preserve local Raft state across ordinary process restart and test only crash/restart semantics in the default clustered path.
2. Add a controlled replacement lifecycle in which a missing replica is recovered as a non-voting participant, receives validated state, catches up, and is promoted only after the cluster confirms its identity and progress.
3. Keep empty-replica recovery as an explicitly opt-in test scenario using OpenRaft's test feature, with tests proving that committed messages and consumer state remain intact while the original leader stays available.
4. If the current storage adapter is found to return inconsistent log state, fix its persistence and recovery invariants rather than enabling rollback. In particular, verify append, truncate, purge, snapshot installation, committed-log persistence, and restart ordering with fault injection.

The recommended immediate boundary is the combination of (1) and (3): ordinary production behavior remains conservative, while the snapshot experiment remains available as a clearly labeled test. Direction (2) is the production path to evaluate next.

## Sources

- [OpenRaft feature flags](https://docs.rs/openraft/latest/openraft/docs/feature_flags/)
- [OpenRaft FAQ: lost data and follower replacement](https://docs.rs/openraft/latest/openraft/docs/faq/)
- [OpenRaft log pointers and committed progress](https://docs.rs/openraft/latest/openraft/docs/data/log_pointers/)
- [OpenRaft log replication](https://docs.rs/openraft/latest/openraft/docs/protocol/replication/log_replication/)
- [OpenRaft change log](https://github.com/databendlabs/openraft/blob/main/change-log.md)
- [Apache Kafka replication design](https://kafka.apache.org/42/design/design/)
- [Apache Kafka log recovery](https://kafka.apache.org/10/implementation/log/)
- [Redpanda architecture](https://docs.redpanda.com/streaming/24.2/get-started/architecture/)
- [Redpanda Raft group reconfiguration](https://docs.redpanda.com/streaming/25.1/manage/raft-group-reconfiguration/)
- [Redpanda replication implementation discussion](https://www.redpanda.com/blog/simplifying-raft-replication-in-redpanda)
