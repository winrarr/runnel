# Cluster replication-progress telemetry

Status: source-backed research for the early static Multi-Raft cluster; no telemetry design accepted

Last reviewed: 2026-10-02

## Scope and conclusion

This note examines how to expose leadership and replication progress for the current three-node clustered backend while keeping metric cardinality and scrape work bounded. It is based on source inspection at baseline `8c7efc3ea043fd9f04abc1698c173c333bd9117b` and primary project and dependency documentation. No runtime measurements or performance tests were run for this note.

The OpenRaft version pinned in [`Cargo.lock`](../../Cargo.lock) is 0.9.25. Its node metrics already contain useful leadership, log, quorum-ack-age, and leader-to-peer replication state. Runnel currently uses `current_leader()` for decisions and health but does not consume the `Raft::metrics()` watch receiver or export per-group replication progress. Existing metrics aggregate process-level broker and snapshot signals. This confirms the open outcome in [Make the clustered deployment operable](../backlog.md#make-the-clustered-deployment-operable); no backlog correction or ADR is warranted by this evidence.

**Recommendation (inference):** make the first replication view process-level and fixed-cardinality. Aggregate per-group state and leader counts, plus per-peer replication-lag summaries and quorum-ack age, without stream or Raft-group labels. Keep an on-demand, access-controlled diagnostic view as a possible later way to identify an individual group. Treat all samples as the local node's latest OpenRaft view, attach freshness/availability semantics, and never use these metrics alone as a write-safety or readiness guarantee.

## Current evidence in Runnel

- The clustered backend creates one metadata Raft group and a data group for each stream. The initial layout uses the same static three-node membership for every group ([architecture](../architecture.md#current-data-model), [group manager](../../crates/runnel-raft/src/group_manager.rs#L184-L215)). The manager keeps its locally opened groups in a `BTreeMap`; the count can grow with streams and is not capped by telemetry policy ([group manager](../../crates/runnel-raft/src/group_manager.rs#L90-L110)).
- `RaftGroup` retains an OpenRaft handle. Current application code calls `current_leader()` in cluster routing/health paths; a source search finds no direct subscription to `Raft::metrics()` ([RaftGroup](../../crates/runnel-raft/src/engine.rs#L372-L378), [cluster health](../../crates/runnel-raft/src/engine.rs#L1207-L1228)). OpenRaft documents that `current_leader()` is based on its metrics system, but it is a routing hint and can be stale; that is not an explicit exported replication view.
- Clustered `/metrics` currently emits process/broker counters and histograms plus process-aggregated snapshot build, install, failure, byte, chunk, and in-progress signals ([scrape path](../../crates/runnel-server/src/observability.rs#L344-L383), [snapshot metric rendering](../../crates/runnel-server/src/observability.rs#L843-L854)). `GroupManager::snapshot_metrics()` copies the local group list and folds its state-machine counters ([aggregation](../../crates/runnel-raft/src/group_manager.rs#L637-L668)). When bounded engine health fails, `/metrics` omits engine-derived values instead of emitting zeroes as if they were fresh.
- Cluster health requires initialized metadata and a known metadata-group leader, then aggregates data-group health. It does not establish that every data group has a leader, that every follower is caught up, or that a current quorum is reachable. The illustrative Kubernetes readiness comment explicitly says the probe is not local-leader, replication-lag, quorum-margin, or disk-capacity validation ([health path](../../crates/runnel-raft/src/engine.rs#L1207-L1228), [probe](../../deploy/kubernetes/runnel.yaml#L123-L136)).
- Existing real-process recovery coverage asserts snapshot transfer/install counters after replacement recovery ([cluster smoke test](../../crates/runnel-server/tests/cluster_smoke.rs#L1540-L1603)). It does not assert elected-leader identity or follower replication lag. The current backlog describes leadership and replication progress as incomplete, consistent with these observations.

These are source and test-coverage observations, not live measurements of how much lag or scrape overhead occurs under a workload.

## Signals available from pinned OpenRaft

The [OpenRaft 0.9.25 metrics module](https://docs.rs/openraft/0.9.25/openraft/metrics/index.html) recommends `Raft::metrics()` as an observability input and says its watch channel holds only the latest state, not every transition. The pinned [`RaftMetrics` source](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/metrics/raft_metrics.rs) exposes:

| Signal | Coverage and meaning | Limitation for Runnel |
|---|---|---|
| Node `state`, `current_leader`, term, and membership configuration | Current local role and the leader/member view for one Raft group. | It is per group and latest-state only. A scrape can miss intermediate leader changes. `current_leader = None` may be election/startup state rather than a sustained outage. The metrics struct has no source-update timestamp, and a reported leader is not a fresh quorum check. |
| `last_log_index`, `last_applied`, snapshot and purge positions | Local log/apply and compaction position for one group. | Indices from different groups are independent and must not be summed or compared across groups. Index distance is entries, not bytes, time, or application records. |
| `replication` map | Present only when this node is leader; maps each replication target to its matched log ID. This permits a per-group, per-peer entry-distance estimate against that same group's last log index. | Followers do not expose their leader's peer map. It is a latest view, and entry lag does not reveal payload bytes, replication RPC latency, or why a follower is behind. |
| `millis_since_quorum_ack` | Present for a leader after a quorum has acknowledged a committed `AppendEntries` request; absent when not leader or not yet acknowledged. OpenRaft describes it as a likelihood signal for loss of synchronization. | It is not a quorum-margin guarantee, a commit-latency histogram, or proof that the next write can commit. Absence must remain distinguishable from zero. |

OpenRaft's [`Raft::metrics()` API](https://docs.rs/openraft/0.9.25/openraft/raft/struct.Raft.html#method.metrics) provides the receiver, and its [observation FAQ](https://docs.rs/openraft/0.9.25/openraft/docs/faq/index.html#observation-and-management) recommends subscribing to changes for node-state observation. Since the channel coalesces state, exact transition counts require observing updates rather than polling infrequently; a gauge alone cannot reconstruct missed transitions.

## Candidate bounded metric shape

The three-node static membership gives a small, known peer set, but the number of stream groups can grow. Therefore prefer a constant set of aggregate series per node and peer over one series per stream/group.

| Candidate summary | Useful diagnosis | Cost and coverage limits |
|---|---|---|
| Counts of registered groups by local role and known leader ID; count with no known leader | Shows leadership distribution and groups with no current leader in the locally observed set. | Reading group state is `O(G)` for `G` local groups. Leader ID labels are bounded by configured membership today. Counts describe this process's opened groups, not a strongly consistent cluster-wide inventory. |
| Per configured peer: max `last_log_index - peer_matched_index` and sampled counts of groups in fixed lag ranges or above a configured entry threshold | Reveals whether the node's leaders see a peer falling behind and whether lag is isolated or broad. | Only groups led locally contribute. Compute differences inside each group; do not aggregate raw indices. Define how an unmatched `None` position is represented before implementation. Entry count is not byte backlog or time-to-catch-up. The ranges and threshold need workload evidence. |
| Per local leader: maximum `millis_since_quorum_ack`, count above a configured age threshold, and count without a quorum acknowledgement | Helps spot leaders whose latest metrics show aging quorum contact. | Only meaningful for leader groups. Use OpenRaft's absent state separately from zero. A scrape samples current state and must not claim synchronous quorum availability. |
| Existing aggregate snapshot transfer counters, with an explicit availability/freshness contract | Connects a lag episode to snapshot recovery activity already measured by Runnel. | These are lifecycle totals, not bytes remaining or per-peer replication progress. Existing scrape behavior omits engine-derived samples when engine health times out. |

Keep labels to a small fixed vocabulary: local role, leader node ID where needed, and configured peer ID. Avoid stream names, consumer names, group IDs, terms, log indices, and error strings as metric labels. An on-demand diagnostic response could include a selected group's identity and full positions, but it would have a different cost, access-control, pagination, and stream-name disclosure review from `/metrics`.

## Alternatives and reference designs

1. **Per-group Prometheus series.** Rejected for the initial static-cluster view (recommendation): a stream label makes time-series count proportional to user-created streams and leaves retired identities in monitoring history. It offers easy attribution but makes metric-storage cost depend on workload cardinality.
2. **Aggregate on every scrape.** Mechanically simple and returns the latest in-memory views, but adds work proportional to local group count and configured peers. Runnel's current scrape already performs a bounded health check across groups and a second group walk for snapshot totals. Future aggregation should reuse an existing pass or prove its additional scrape cost at representative group counts rather than introduce a third walk. Current group-count and scrape-cost bounds have not been measured.
3. **Manager-owned latest-state cache.** A coalesced observer can make scrapes constant-time after initialization and expose cache age, but adds lifecycle, synchronization, and state proportional to groups. One task per group would also add task count proportional to groups. Compare this only if measured scrape cost makes the direct aggregation unacceptable; preserve a clear maximum staleness and omit unavailable samples.
4. **Detailed on-demand diagnostics.** Useful for identifying the offending stream after an aggregate alert. It can be paginated and run less often, but increases endpoint surface, scrape/operator complexity, and the chance of disclosing stream names. Runnel's current operational security is incomplete, and `/metrics` is not a place to add high-cardinality identity by convenience.

Apache Kafka's [first-party monitoring guide](https://kafka.apache.org/43/operations/monitoring/) pairs cluster-wide under-replicated counts with maximum and per-follower lag. The transferable idea is to provide both a cheap fleet signal and a peer-specific signal; Kafka partitions and Runnel's independent Raft groups do not have identical semantics. NATS' [first-party monitoring guide](https://docs.nats.io/learn/monitoring/monitoring-endpoints) uses per-node on-demand snapshots, and warns that full JetStream detail can be slow at large entity counts and should be filtered/paged. Prometheus' [instrumentation guidance](https://prometheus.io/docs/practices/instrumentation/#do-not-overuse-labels) notes that each label set adds a time series and recommends moving high-cardinality analysis out of monitoring. Together these references support separating low-cardinality frequent metrics from costly identity-rich diagnostics rather than scraping every object continuously.

## Recommendation, disposition, and evidence gates

**Recommendation (inference):** keep this outcome in the existing [clustered deployment-operability backlog item](../backlog.md#make-the-clustered-deployment-operable). No tracker edit is needed: its acceptance criterion explicitly calls for cluster health, leadership, and replication-progress metrics, and the current-progress paragraph says those signals remain incomplete. Before implementation, confirm exact `RaftMetrics` semantics against pinned 0.9.25 and choose whether a direct combined pass or a cache meets the current group-count and scrape-latency envelope. Start with fixed-cardinality process aggregates; keep readiness independent until the health contract is separately designed and tested. No ADR is appropriate until an implementation/design choice is accepted.

Useful implementation evidence would include:

- a real three-process test that observes leadership and a follower's lag during a controlled replication interruption, then verifies lag falls after rejoin and existing acknowledged state remains correct;
- assertions that no-leader, unknown/stale metrics, and `millis_since_quorum_ack = None` do not render as healthy zero values;
- a cardinality test demonstrating series count is independent of stream count, with only configured peer IDs represented;
- measured scrape latency and CPU across increasing group counts, including concurrent readiness/metrics scrapes, to decide whether the direct pass remains acceptable or a bounded-age cache is required.

No performance claim follows from this research. Health and recovery semantics must remain covered by real broker processes; any material scrape-cost or cache design should record workload, group count, resources, sample age, and artifacts under the project's benchmark evidence policy.

## Refactor and planning-record assessment

The inspected code has a clear ownership split: Raft state stays in `runnel-raft`, HTTP metric formatting stays in `runnel-server`, and the public engine health model does not carry topology. Preserve these boundaries when implementing telemetry. The existing separate group traversals in health and snapshot aggregation are a concrete cost to account for, but no isolated code or register change is justified without a group-scale cost measurement. The current backlog item already names the outcome and its acceptance criterion; this note supplies the evidence and bounds needed to implement it later. No runtime refactor, tech-debt update, backlog change, or ADR is included here.
