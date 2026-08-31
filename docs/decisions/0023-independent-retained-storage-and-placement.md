# ADR 0023: Separate retained storage and placement identity

- Status: accepted; implementation deferred
- Date: 2026-08-31
- Baseline: `origin/main` `1fded36ca96d110f91d6f358fa206f6086e8f245`
- Scope: the retained-state growth and scalable-placement outcomes
- Related: [TD-002](../tech-debt.md#td-002-one-file-and-a-startup-scan-per-local-stream), [TD-008](../tech-debt.md#td-008-distributed-raft-backend-is-an-early-static-cluster-implementation), [TD-009](../tech-debt.md#td-009-snapshots-rewrite-the-complete-materialized-group-state), [TD-010](../tech-debt.md#td-010-clustered-state-materializes-complete-retained-history), and the [retained-state](../backlog.md#make-retained-state-growth-independent-of-the-hot-path) and [placement](../backlog.md#make-placement-scale-independently-of-stream-identity) outcomes

## Context

The current vertical slice uses one local log per stream and one static
clustered data group per stream. The clustered state machine also materializes
complete retained message history. This is a useful correctness baseline, but
it makes the public stream identity the accidental unit of storage, consensus,
recovery, and placement. It also makes retained history part of the cost of
materializing state and building snapshots.

The existing [retention and disk-pressure plan](../design/retention-disk-pressure-plan.md)
defines the retention floor, immutable segment cleanup, and capacity-admission
questions. This decision addresses the adjacent identity boundary: which unit
is allowed to grow, move, split, replicate, and be recovered. It does not
define a public partition API or a retention policy.

## Evidence and differences that matter

These references solve related problems, not Runnel's exact delivery contract.
Their behavior is evidence for boundaries and tradeoffs, not implementation or
compatibility evidence for Runnel.

| Reference | Directly relevant design | Difference that matters to Runnel |
| --- | --- | --- |
| [Apache Kafka topics and partitions](https://kafka.apache.org/documentation/#intro_topics) and [topic log configuration](https://kafka.apache.org/42/configuration/topic-configs/) | A partition is an ordered append-only log and the unit of parallelism, with immutable segments and sparse offset indexes. Retention is applied per partition and deletes whole old segments. | Fixed partitions make capacity and consumer parallelism a creation-time choice. Runnel needs hidden units that can be assigned and moved without exposing partitions, while preserving logical stream offsets and acknowledgement state. |
| [Apache Pulsar bundles](https://pulsar.apache.org/docs/4.2.x/concepts-broker-load-balancing-concepts/) and [scalable topics](https://pulsar.apache.org/docs/5.0.x/concepts-scalable-topics/) | Bundles are a middle-layer assignment unit for many topics. Range-based scalable topics split and merge key ranges and retain per-key ordering across those changes. | This is the closest placement precedent: stream/topic identity stays logical while a range or bundle is the movable unit. Runnel must additionally fence delivery owners and preserve durable consumer progress during a move. |
| [TiKV terminology and placement](https://tikv.org/docs/7.1/reference/architecture/terminology/) and [replication/rebalancing](https://tikv.org/docs/5.1/concepts/explore-tikv-features/replication-and-rebalancing/) | Continuous key ranges are independently replicated Raft groups. A placement driver moves, splits, and merges ranges; a new replica catches up before the old replica is removed. | Separating placement scheduling from the Raft group gives Runnel a path to capacity-aware balancing. TiKV's transactions and key/value model do not establish Runnel's stream replay, at-least-once, or consumer-group semantics. |
| [Apache BookKeeper protocol](https://bookkeeper.apache.org/docs/development/protocol/) | A ledger is a segment of a larger log. Ensemble, write quorum, and acknowledgement quorum are explicit; ensemble changes create new fragments, and fencing prevents concurrent writers. | Extent-level replica changes and a durable writer epoch are useful alternatives to one Raft group per stream. Their quorum and read-repair semantics would need a separate Runnel engine experiment. |
| [The Raft paper](https://raft.github.io/raft.pdf) | A consensus group applies one ordered command log with one leader at a time; snapshots preserve state-machine state and membership while old log entries are compacted. | Raft remains a suitable first replication boundary per hidden unit, but its consensus log must not become the retained message history. A snapshot must be able to recover metadata and references to retained data without serializing all history. |

The common lesson is to separate at least four identities: the public stream,
the ordering domain, the movable placement unit, and the physical replica or
segment. The references do not prove that one particular mapping, unit count,
or quorum policy is right for Runnel.

## Decision

Runnel will use a hidden, two-level storage and placement model for future
implementations:

```text
stream + ordering intent
  -> committed placement map (unit identity, range/assignment, epoch)
    -> physical replica set and consensus group
      -> immutable retained-data segments plus bounded indexes and manifest
```

The following boundaries are accepted:

1. **Logical identity is stable.** The public stream name, logical record
   offsets, consumers, acknowledgements, replay intent, and delivery outcomes
   remain the application model. A client must not learn a node, Raft group,
   partition, segment, or placement map in order to use a stream.
2. **The placement unit is internal and movable.** A unit may contain many
   streams, or a key-range portion of one stream when its ordering contract
   permits that. It, not the stream name, is the unit of replica placement,
   leadership, migration, and resource accounting. The initial one-data-group
   per-stream cluster may remain the compatibility baseline; it is not the
   target identity model.
3. **Retained payloads are segmented state.** New records append to a bounded
   active segment. Sealed segments, a versioned manifest, and indexes provide
   historical reads and retention cleanup. Replicated state carries the
   manifest generation, logical retained floor, consumer checkpoints, delivery
   fencing state, and producer/request deduplication facts; it must not require
   copying all retained payloads for each ordinary apply, checkpoint, or
   snapshot. The selected durability point still waits for the required local
   and/or replica durable writes before reporting success.
4. **Placement changes are committed transitions.** A placement map entry has
   an epoch. A move or split first prepares the target replica/unit and proves
   the required data and state boundary, then commits the new map and epoch.
   The old owner drains or rejects work at the cutover boundary; stale routes,
   writers, and delivery owners receive an explicit retry/fencing outcome.
   Hashing directly from a stream name or using `hash(key) % N` after changing
   `N` is not a safe migration protocol.
5. **Ordering limits scale.** Stream-wide FIFO remains one ordering domain and
   therefore cannot be split without changing its contract. Key-scoped FIFO
   may use range units: a key stays in one range, and a split moves a range to
   children only at a fenced logical boundary. Cross-unit polling, replay,
   consumer progress, and grouped acknowledgements need an explicit semantic
   design before a stream is split in production.
6. **Consensus and retained history remain separate.** The initial clustered
   implementation may use one Multi-Raft group per placement unit. A future
   sequenced-quorum, ledger, or other engine may use a different replica
   boundary behind the same semantic engine contract. Compacting a consensus
   log must never delete retained records still named by the active manifest or
   resurrect records below the durable retention floor.

The metadata group owns the committed placement map, unit lineage, replica
configuration, and cutover epochs. It does not carry every retained message.
The exact map encoding, assignment algorithm, number of virtual units,
replication engine, segment size, and online split policy remain deferred.

## Alternatives considered

- **Keep one file and one Raft group per stream.** Simplest recovery and the
  current implementation, but startup, snapshots, group overhead, hot-stream
  capacity, and placement cardinality all grow with stream identity.
- **Fixed Kafka-like partitions per stream.** Provides familiar parallelism and
  ordering, but requires an upfront partition count and makes naive resizing
  remap keys. It also exposes partition-count and consumer-assignment questions
  that Runnel deliberately keeps internal.
- **Direct consistent hashing to brokers.** Low metadata overhead, but a node
  change can remap live writers without a durable cutover, does not naturally
  encode replica/failure-domain policy, and cannot fence an old owner by itself.
- **A single global Raft group.** Easy to reason about but makes one consensus
  leader, retained state, snapshots, and recovery the cluster-wide bottleneck.
- **BookKeeper-like ledgers with a small metadata consensus plane.** Attractive
  for immutable extents, quorum flexibility, and independent storage/ordering,
  but introduces a new fencing, committed-watermark, repair, and read-placement
  protocol. Keep it as a future comparison, not as an implicit change to the
  current engine.

## Accepted consequences and compatibility boundary

This decision accepts the cost of a durable placement-control plane and a
future migration protocol in exchange for not freezing public stream names to
physical scale. It also accepts that a hot stream requiring total FIFO remains
a deliberate single-ordering-domain limit.

There is no public protocol change and no claim that the current static cluster
can rebalance. Existing local and clustered formats remain authoritative until
a versioned migration is implemented. Any migration must preserve logical
offsets, consumer checkpoints, delivery attempts/fencing, and producer retry
identity; bind manifests to stream, unit, cluster, and node identities; and
fail closed on mismatches in the spirit of [ADR 0019](0019-clustered-storage-identity.md).
It must also keep the replacement boundary in [ADR 0018](0018-safe-replica-recovery-boundary.md): copying a directory or matching a node ID is not sufficient to make a replica authoritative.

The operational costs are real: placement metadata, segment lineage, replica
catch-up, migration bandwidth, temporary double storage, backpressure during
cutover, and higher-cardinality metrics. Those costs must be bounded and
observable; this ADR does not claim a performance improvement.

## Hypotheses and unresolved risks

- **H1:** appending and acknowledging a record can remain bounded by the active
  segment and bounded metadata rather than total retained payload bytes;
  manifest-based snapshots will reduce recovery work without weakening
  acknowledged durability.
- **H2:** a pool of virtual units will distribute uniform and skewed workloads
  better than one group per stream while keeping idle group, timer, file, and
  metadata overhead acceptable.
- **H3:** range lineage plus an epoch-fenced cutover can preserve per-key FIFO
  and at-least-once delivery through split/move and process failure.
- **H4:** sharing a unit among cold streams will reduce control-plane overhead
  without allowing one hot stream or slow consumer to starve its neighbors.

The main unresolved risks are the cross-unit consumer/replay model; a global
logical offset or per-domain cursor design; crash recovery between segment
publication and metadata commit; cleanup while deliveries are active; target
replica catch-up and stale-owner fencing; placement-map scale and bootstrap;
heterogeneous disk capacity and failure domains; and migration of current
per-stream groups without losing uncertain publish outcomes.

## Next experiment and evidence gate

No runtime benchmark is required for this documentation-only ADR. Per
[benchmarking guidance](../benchmarking.md), an implementation that changes a
hot path must first map the path to the standard suite and run a focused
experiment when the standard suite misses it. The existing PR comparison does
not exercise large retained histories, placement moves, or split/cutover
failures, so it cannot by itself establish this decision's hypotheses.

The next experiment should compare the current clustered materialization with
a non-public segmented/manifest prototype under controlled, sequential
resources:

1. Predeclare at least three retained-history sizes spanning two orders of
   magnitude (for example, 10^4, 10^5, and 10^6 records), use 100-byte and
   1-KiB records, and test both uniform streams and one hot ordering domain
   plus many cold streams. Measure publish, poll, and acknowledgement
   p50/p99/p99.9, throughput, allocations/RSS, index and physical bytes,
   startup/recovery time, and replay seek time.
2. On three real broker processes, exercise a unit move and a key-range split
   while publishing and polling. Kill the old owner before and after the
   cutover and restart a target replica. Verify no uncommitted record is
   visible, no acknowledged progress moves backward, stale owners are fenced,
   and replay can traverse the manifest lineage without a silent gap.
3. Run the comparison sequentially under the documented benchmark lock and a
   fixed 2-CPU/2-GiB budget, retain raw results, and report the exact
   repetition count and stability result. Use `just bench-pr-local` after the
   runtime path exists; add a targeted harness for retained-size and
   move/split coverage rather than treating an unrelated stable result as
   evidence.

The design is ready for implementation only when the prototype demonstrates
that ordinary append/checkpoint/snapshot work does not copy all retained
payloads, the semantic/fault checks pass, and the measured resource and tail
latency tradeoffs are explicitly accepted. Until then, this is an architectural
boundary and a hypothesis, not implementation evidence.

## References

- [Current architecture](../architecture.md)
- [Retention and disk-pressure implementation plan](../design/retention-disk-pressure-plan.md)
- [Multi-Raft implementation plan](../design/multi-raft-implementation-plan.md)
- [ADR 0004: first distributed engine](0004-multi-raft-first-distributed-engine.md)
- [ADR 0006: separate metadata and stream data groups](0006-separate-metadata-and-data-groups.md)
- [ADR 0018: safe replica recovery boundary](0018-safe-replica-recovery-boundary.md)
- [ADR 0019: clustered storage identity](0019-clustered-storage-identity.md)
- [ADR 0020: stable optimization evidence](0020-stable-optimization-evidence.md)
